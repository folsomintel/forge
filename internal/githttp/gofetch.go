package githttp

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
)

// The fork-free fetch fast path - the read-side twin of goreceive.
//
// Two shapes are served entirely in Go:
//
//  1. INCREMENTAL (agent fan-out): a follower fetches want=W have=H where
//     the W..H delta arrived as fast-path pushes. Our stored receive packs
//     ARE that delta: at receive time we record (newOID -> pack, oldOID,
//     externals); at fetch time we walk wants back through those edges to
//     the client's haves and re-emit the union as one full-object pack.
//     No pack-objects fork, no negotiation, no materialization.
//
//  2. CLONE PASSTHROUGH: right after maintenance a repo is exactly one
//     consolidated gc pack whose closure covers every ref. A clone of that
//     repo IS that pack - stream it straight from disk or the bucket,
//     without materializing the repo or forking git.
//
// Soundness rule for (1): the client must end up with closure(W). Each
// chain pack was connectivity-verified at receive against pack+store, so
// the only risk is an object the SERVER had but the CLIENT does not (e.g.
// a blob shared by content from another branch). Every external object a
// chain pack referenced must therefore be PROVEN client-reachable: it is
// a have, a chain link, or it is found by a bounded walk of a have's own
// tree. Anything unproven falls back to git - which is always correct.

// goFetchMaxChain / goFetchMaxBytes bound the incremental union.
const (
	goFetchMaxChain = 16
	goFetchMaxBytes = 4 << 20
	// goFetchBFSCap bounds the client-closure proof walk (cat-file reads).
	goFetchBFSCap = 768
	// transitionCap bounds the in-memory transition window.
	transitionCap = 8192
)

// GoFetchStats mirrors GoReceiveStats for the fetch fast path.
type GoFetchStats struct {
	Eligible    int64            `json:"eligible"`
	CloneStream int64            `json:"clone_stream"`
	FellBack    int64            `json:"fell_back"`
	FellBackBy  map[string]int64 `json:"fell_back_by,omitempty"`
}

type goFetchCounters struct {
	eligible    atomic.Int64
	cloneStream atomic.Int64
	fellBack    atomic.Int64

	fbArgs      atomic.Int64 // unsupported argument (filter/shallow/...)
	fbCloneMiss atomic.Int64 // clone shape but repo not single-gc-pack-covered
	fbChainMiss atomic.Int64 // wants don't resolve to haves in the window
	fbLineage   atomic.Int64 // external object not provably client-reachable
	fbPackRead  atomic.Int64 // stored pack unreadable
	fbTags      atomic.Int64 // include-tag with tags present (incremental)
}

func (c *goFetchCounters) snapshot() GoFetchStats {
	by := map[string]int64{}
	for name, v := range map[string]*atomic.Int64{
		"args": &c.fbArgs, "clone_miss": &c.fbCloneMiss, "chain_miss": &c.fbChainMiss,
		"lineage": &c.fbLineage, "pack_read": &c.fbPackRead, "tags": &c.fbTags,
	} {
		if n := v.Load(); n > 0 {
			by[name] = n
		}
	}
	return GoFetchStats{
		Eligible: c.eligible.Load(), CloneStream: c.cloneStream.Load(),
		FellBack: c.fellBack.Load(), FellBackBy: by,
	}
}

// GoFetchStatsSnapshot reports fetch fast-path decision counts.
func (h *Handler) GoFetchStatsSnapshot() GoFetchStats { return h.goFetchCtr.snapshot() }

// transition is one recorded push edge: newOID was created by pack, whose
// parent state was oldOID, referencing externals outside the pack.
type transition struct {
	pack      string
	old       string
	externals []string
}

// transitions is a bounded FIFO map keyed by repo+"\x00"+newOID.
type transitionMap struct {
	mu    sync.Mutex
	m     map[string]transition
	order []string
}

func (t *transitionMap) put(key string, tr transition) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string]transition{}
	}
	if _, exists := t.m[key]; !exists {
		t.order = append(t.order, key)
	}
	t.m[key] = tr
	for len(t.order) > transitionCap {
		delete(t.m, t.order[0])
		t.order = t.order[1:]
	}
}

func (t *transitionMap) get(key string) (transition, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tr, ok := t.m[key]
	return tr, ok
}

// recordTransitions captures push edges for the fetch fast path. Pushes
// with huge external sets are not recorded (the lineage proof would not
// be attempted anyway).
func (h *Handler) recordTransitions(repo, pack string, cmds []pushCmd, externals []string) {
	if len(externals) > 512 {
		return
	}
	for _, c := range cmds {
		h.transitions.put(repo+"\x00"+c.new, transition{pack: pack, old: c.old, externals: externals})
	}
}

// fetchArgs is the parsed, eligibility-checked v2 fetch request.
type fetchArgs struct {
	wants      []string
	haves      map[string]bool
	done       bool
	includeTag bool
}

// parseFetchArgs returns ok=false for any argument the fast path does not
// model (shallow, filter, want-ref, ...) - those requests belong to git.
func parseFetchArgs(lines []string) (fetchArgs, bool) {
	fa := fetchArgs{haves: map[string]bool{}}
	inArgs := false
	for _, l := range lines {
		l = strings.TrimSuffix(l, "\n")
		if !inArgs {
			// Capability lines before the delim (agent=..., object-format).
			if l == "command=fetch" || strings.HasPrefix(l, "agent=") || l == "object-format=sha1" {
				continue
			}
			inArgs = true // parsePktLines drops the delim; first arg begins here
		}
		switch {
		case strings.HasPrefix(l, "want "):
			oid := strings.TrimPrefix(l, "want ")
			if len(oid) != 40 {
				return fa, false
			}
			fa.wants = append(fa.wants, oid)
		case strings.HasPrefix(l, "have "):
			oid := strings.TrimPrefix(l, "have ")
			if len(oid) != 40 {
				return fa, false
			}
			fa.haves[oid] = true
		case l == "done":
			fa.done = true
		case l == "thin-pack", l == "ofs-delta", l == "no-progress":
			// Harmless to us: we always send a full (non-thin) pack.
		case l == "wait-for-done":
			// v2: the server must NOT send "ready" and must wait for the
			// client's "done" before the pack. Our response always sends
			// ready+pack, so we cannot honor it - hand these to git.
			return fa, false
		case l == "include-tag":
			fa.includeTag = true
		default:
			return fa, false // shallow/deepen/filter/want-ref/sideband-all/...
		}
	}
	return fa, len(fa.wants) > 0
}

// goFetch serves a v2 fetch command in Go, or returns false for git.
func (h *Handler) goFetch(w http.ResponseWriter, r *http.Request, repo, ns string, lines []string) bool {
	if !h.GoFetch || ns != "" {
		return false
	}
	fail := func(c *atomic.Int64) bool {
		c.Add(1)
		h.goFetchCtr.fellBack.Add(1)
		return false
	}
	fa, ok := parseFetchArgs(lines)
	if !ok {
		return fail(&h.goFetchCtr.fbArgs)
	}
	ctx := r.Context()

	if len(fa.haves) == 0 {
		if !fa.done {
			return fail(&h.goFetchCtr.fbCloneMiss)
		}
		return h.goFetchClone(ctx, w, repo, fa)
	}
	return h.goFetchIncremental(ctx, w, repo, fa)
}

// --- clone passthrough ---

// visibleTips returns the tip OIDs of the repo's normal-view refs (the
// ephemeral refs/namespaces/* refs are hidden), and whether any hidden ref
// exists. It is the fetch fast path's visibility gate: git refuses a `want`
// that is not reachable from an advertised ref (allowAnySHA1InWant off), so
// serving an arbitrary OID just because we recorded a transition for it, or
// streaming a pack whose closure includes hidden ephemeral objects, would
// leak private data the forked path never exposes.
func (h *Handler) visibleTips(ctx context.Context, repo string) (tips map[string]bool, hasHidden bool, err error) {
	refs, err := h.DB.ListRefs(ctx, repo)
	if err != nil {
		return nil, false, err
	}
	tips = make(map[string]bool, len(refs))
	for _, ref := range refs {
		if strings.HasPrefix(ref.Name, "refs/namespaces/") {
			hasHidden = true
			continue
		}
		tips[ref.Target] = true
	}
	return tips, hasHidden, nil
}

// goFetchClone streams the repo's single consolidated pack when it covers
// every want: the cold-clone case right after maintenance.
func (h *Handler) goFetchClone(ctx context.Context, w http.ResponseWriter, repo string, fa fetchArgs) bool {
	fail := func(c *atomic.Int64) bool {
		c.Add(1)
		h.goFetchCtr.fellBack.Add(1)
		return false
	}
	// The gc pack packs the closure of EVERY ref, including hidden
	// refs/namespaces/* (ephemeral PR scratch) so they survive compaction.
	// Streaming it whole to a normal-view clone would leak those objects, so
	// bail whenever the repo has any hidden ref - git serves those clones.
	_, hasHidden, err := h.visibleTips(ctx, repo)
	if err != nil || hasHidden {
		return fail(&h.goFetchCtr.fbCloneMiss)
	}
	// A repo with exactly one self-owned pack always contains the full closure
	// of every ref: a second push would add a second pack (len>1 bails), and
	// consolidation produces a single pack. So the single-pack invariant - not
	// the pack's Source label - is what makes whole-pack streaming sound (and
	// every want is still verified against the idx below). Source-agnostic so a
	// recovered replica, whose pack list is rebuilt from the bucket, also
	// streams. BlobRepo != "" (a fork sharing the owner's blobs) still bails.
	packs, err := h.DB.ListPacks(ctx, repo)
	if err != nil || len(packs) != 1 || packs[0].BlobRepo != "" {
		return fail(&h.goFetchCtr.fbCloneMiss)
	}
	name := packs[0].Name
	idx, err := h.readPackAux(ctx, repo, name+".idx")
	if err != nil {
		return fail(&h.goFetchCtr.fbPackRead)
	}
	for _, want := range fa.wants {
		if !idxContains(idx, want) {
			return fail(&h.goFetchCtr.fbCloneMiss) // ref moved since gc
		}
	}
	// Stream: local pack file when materialized, else straight from the
	// store. Never materializes, never forks.
	var src io.ReadCloser
	if dir, ok := h.Cache.DirIfMaterialized(repo); ok {
		if f, err := os.Open(filepath.Join(dir, "objects", "pack", name+".pack")); err == nil {
			src = f
		}
	}
	if src == nil {
		rc, err := h.Blobs.Get(ctx, repo, name+".pack")
		if err != nil {
			return fail(&h.goFetchCtr.fbPackRead)
		}
		src = rc
	}
	defer src.Close()
	h.goFetchCtr.eligible.Add(1)
	h.goFetchCtr.cloneStream.Add(1)
	writeFetchResponse(w, nil, src)
	return true
}

// --- incremental chain ---

func (h *Handler) goFetchIncremental(ctx context.Context, w http.ResponseWriter, repo string, fa fetchArgs) bool {
	fail := func(c *atomic.Int64) bool {
		c.Add(1)
		h.goFetchCtr.fellBack.Add(1)
		return false
	}

	// Visibility gate: every want must currently be a visible ref tip.
	// transitions is a raw oid->pack map with no ref/visibility scope and no
	// eviction on delete/force-push, so without this a client could fetch an
	// ephemeral tip by oid, or resurrect a force-pushed-away/deleted object
	// still in the transition window. git refuses such wants; so do we.
	tips, _, err := h.visibleTips(ctx, repo)
	if err != nil {
		return fail(&h.goFetchCtr.fbChainMiss)
	}
	for _, want := range fa.wants {
		if !tips[want] {
			return fail(&h.goFetchCtr.fbChainMiss)
		}
	}

	// Walk each want back through recorded push edges until a have.
	packNames := []string{}
	seenPack := map[string]bool{}
	links := map[string]bool{} // every chain oid (client will have all of them)
	terminals := map[string]bool{}
	var externals []string
	for _, want := range fa.wants {
		cur := want
		for hop := 0; ; hop++ {
			if fa.haves[cur] {
				terminals[cur] = true
				break
			}
			if hop >= goFetchMaxChain {
				return fail(&h.goFetchCtr.fbChainMiss)
			}
			tr, ok := h.transitions.get(repo + "\x00" + cur)
			if !ok {
				return fail(&h.goFetchCtr.fbChainMiss)
			}
			links[cur] = true
			if !seenPack[tr.pack] {
				seenPack[tr.pack] = true
				packNames = append(packNames, tr.pack)
				externals = append(externals, tr.externals...)
			}
			if tr.old == zeroOID {
				// Chain bottoms out at branch creation: the client's haves
				// were never reached, so we cannot prove coverage.
				return fail(&h.goFetchCtr.fbChainMiss)
			}
			cur = tr.old
		}
	}
	if len(packNames) == 0 {
		// Everything wanted is already had (up-to-date fetch): an empty
		// pack response is valid.
		h.goFetchCtr.eligible.Add(1)
		writeFetchResponse(w, ackLines(terminals, fa.done), strings.NewReader(emptyPack()))
		return true
	}

	// include-tag: a tag pointing INTO these packs would be omitted; stay
	// out of that business whenever the repo has tags at all.
	if fa.includeTag {
		if refs, err := h.DB.ListRefs(ctx, repo); err != nil {
			return fail(&h.goFetchCtr.fbPackRead)
		} else {
			for _, ref := range refs {
				if strings.HasPrefix(ref.Name, "refs/tags/") {
					return fail(&h.goFetchCtr.fbTags)
				}
			}
		}
	}

	// Load + parse the chain packs (stored packs are self-contained).
	var objs []ingest.PackObject
	seenObj := map[string]bool{}
	total := 0
	for _, name := range packNames {
		data, err := h.readPackAux(ctx, repo, name+".pack")
		if err != nil {
			return fail(&h.goFetchCtr.fbPackRead)
		}
		total += len(data)
		if total > goFetchMaxBytes {
			return fail(&h.goFetchCtr.fbChainMiss)
		}
		p, _, err := ingest.ReadPackThin(data, nil)
		if err != nil {
			return fail(&h.goFetchCtr.fbPackRead)
		}
		for _, o := range p.Objects {
			if !seenObj[o.OID] {
				seenObj[o.OID] = true
				objs = append(objs, o)
			}
		}
	}

	// The soundness proof: every external must be client-reachable.
	// Allowed for free: haves, chain links (client receives them), and
	// objects present in the union itself. The rest must be found by a
	// bounded walk of a terminal have's own tree.
	var unproven []string
	for _, e := range externals {
		if fa.haves[e] || links[e] || seenObj[e] {
			continue
		}
		unproven = append(unproven, e)
	}
	if len(unproven) > 0 {
		if !h.proveInClientClosure(ctx, repo, terminals, unproven) {
			return fail(&h.goFetchCtr.fbLineage)
		}
	}

	h.goFetchCtr.eligible.Add(1)
	writeFetchResponse(w, ackLines(terminals, fa.done), strings.NewReader(string(ingest.WritePack(objs))))
	return true
}

// proveInClientClosure verifies that every oid in need is reachable from
// one of the client's proven-had commits, via a bounded breadth-first walk
// of those commits' trees through the cat-file pool. Early exit once all
// are found; over the cap means unproven, never unsound.
func (h *Handler) proveInClientClosure(ctx context.Context, repo string, terminals map[string]bool, need []string) bool {
	dir, err := h.Cache.MaterializeServe(ctx, repo)
	if err != nil {
		return false
	}
	pending := map[string]bool{}
	for _, e := range need {
		pending[e] = true
	}
	var queue []string
	for t := range terminals {
		// The have itself and its root tree seed the walk.
		delete(pending, t)
		data, err := h.Cache.BlobContents(repo, dir, t)
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(data), "\n") {
			if tree, ok := strings.CutPrefix(line, "tree "); ok {
				queue = append(queue, strings.TrimSpace(tree))
			}
			if line == "" {
				break
			}
		}
	}
	visited := 0
	seen := map[string]bool{}
	for len(queue) > 0 && len(pending) > 0 {
		oid := queue[0]
		queue = queue[1:]
		if seen[oid] {
			continue
		}
		seen[oid] = true
		delete(pending, oid)
		if len(pending) == 0 {
			return true
		}
		visited++
		if visited > goFetchBFSCap {
			return false
		}
		typ, _, err := h.Cache.ObjectInfo(repo, dir, oid)
		if err != nil || typ != "tree" {
			continue // blobs terminate; missing objects just don't help
		}
		data, err := h.Cache.BlobContents(repo, dir, oid)
		if err != nil {
			continue
		}
		queue = append(queue, treeEntryOIDs(data)...)
	}
	return len(pending) == 0
}

// treeEntryOIDs parses raw tree bytes: "<mode> <name>\x00<20-byte oid>"*.
func treeEntryOIDs(data []byte) []string {
	var out []string
	for len(data) > 0 {
		nul := -1
		for i, b := range data {
			if b == 0 {
				nul = i
				break
			}
		}
		if nul < 0 || nul+21 > len(data) {
			return out
		}
		out = append(out, hex.EncodeToString(data[nul+1:nul+21]))
		data = data[nul+21:]
	}
	return out
}

// readPackAux reads a pack or idx blob: local cache file first, then the
// store, then the WAL tail (inline pack not yet flushed).
func (h *Handler) readPackAux(ctx context.Context, repo, name string) ([]byte, error) {
	if dir, ok := h.Cache.DirIfMaterialized(repo); ok {
		if data, err := os.ReadFile(filepath.Join(dir, "objects", "pack", name)); err == nil {
			return data, nil
		}
	}
	read := func() ([]byte, error) {
		rc, err := h.Blobs.Get(ctx, repo, name)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(io.LimitReader(rc, goFetchMaxBytes+1))
	}
	data, err := read()
	if err != nil {
		if rec, ok := h.DB.(interface {
			RecoverInlinePack(context.Context, string, string) bool
		}); ok && rec.RecoverInlinePack(ctx, repo, strings.TrimSuffix(strings.TrimSuffix(name, ".pack"), ".idx")) {
			data, err = read()
		}
	}
	return data, err
}

// --- protocol v2 response writing ---

// ackLines builds the acknowledgments section for a no-done round: ACK
// the terminals we matched, then ready (the pack follows immediately).
// A done request needs no acknowledgments section at all.
func ackLines(terminals map[string]bool, done bool) []string {
	if done {
		return nil
	}
	var lines []string
	lines = append(lines, "acknowledgments\n")
	for t := range terminals {
		lines = append(lines, "ACK "+t+"\n")
	}
	lines = append(lines, "ready\n")
	return lines
}

// writeFetchResponse emits the v2 fetch sections: optional
// acknowledgments (ending ready), then the packfile section with the pack
// bytes multiplexed on sideband channel 1.
func writeFetchResponse(w http.ResponseWriter, ack []string, pack io.Reader) {
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	var out strings.Builder
	for _, l := range ack {
		pktf(&out, "%s", l)
	}
	if len(ack) > 0 {
		out.WriteString("0001") // delim between sections
	}
	pktf(&out, "packfile\n")
	io.WriteString(w, out.String())

	buf := make([]byte, 32<<10)
	for {
		n, err := pack.Read(buf)
		if n > 0 {
			fmt.Fprintf(w, "%04x\x01%s", n+5, buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			// A mid-stream store/read error must NOT look like a clean end:
			// emit a sideband-3 (error) pkt so the client aborts with a
			// transport error it can retry, instead of a well-framed truncated
			// pack it reports as corruption. No trailing flush - the aborted
			// stream is the signal.
			slog.Warn("gofetch: pack stream error", "err", err)
			pktf(w, "\x03pack stream error\n")
			return
		}
	}
	io.WriteString(w, "0000")
}

// emptyPack is a valid zero-object pack (client is already current).
func emptyPack() string {
	return string(ingest.WritePack(nil))
}

// idxContains reports whether a v2 pack idx lists oid (binary search over
// the sorted OID table).
func idxContains(idx []byte, oid string) bool {
	raw, err := hex.DecodeString(oid)
	if err != nil || len(raw) != 20 {
		return false
	}
	// v2 idx: 4 magic + 4 version + 256*4 fanout, then N*20 OIDs.
	const tableOff = 8 + 256*4
	if len(idx) < tableOff {
		return false
	}
	count := int(binary.BigEndian.Uint32(idx[8+255*4 : 8+256*4]))
	if len(idx) < tableOff+count*20 {
		return false
	}
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		entry := idx[tableOff+mid*20 : tableOff+mid*20+20]
		switch cmp := compareBytes(raw, entry); {
		case cmp == 0:
			return true
		case cmp < 0:
			hi = mid
		default:
			lo = mid + 1
		}
	}
	return false
}

func compareBytes(a, b []byte) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

var _ = repodb.ZeroOID // keep repodb imported for future use
