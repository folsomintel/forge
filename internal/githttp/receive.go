package githttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

// The fork-free receive fast path. A normal push forks git receive-pack +
// index-pack + the hook; on a tiny agent commit that is ~45ms of process
// startup for ~1ms of real work (see docs/fork-elimination.md). This path
// ingests the common case - a small push, including the delta/thin packs
// every real `git push` sends - entirely in Go: parse the pkt-line
// commands, verify + delta-resolve the pack (ingest.ReadPackThin, which
// also fixes thin packs into self-contained ones), prove connectivity
// against the pack + store, generate the idx, persist through the same
// WAL CAS the hook uses, and write report-status. Anything unusual
// (unresolvable bases, oversize, ref-only deletes, odd capabilities)
// returns handled=false and the caller falls back to forking git - which
// is always correct. We only ACK a push after proving we did exactly what
// git would have.

// GoReceiveStats counts fast-path decisions since boot. FellBackBy breaks
// the fallbacks down by cause, so "the fast path isn't engaging" is
// diagnosable from telemetry alone.
type GoReceiveStats struct {
	Eligible     int64            `json:"eligible"`      // served entirely in Go
	FellBack     int64            `json:"fell_back"`     // handed to git
	Rejected     int64            `json:"rejected"`      // CAS-rejected in Go (ng)
	StorageErred int64            `json:"storage_erred"` // fell back because the store errored
	FellBackBy   map[string]int64 `json:"fell_back_by,omitempty"`
}

type goReceiveCounters struct {
	eligible   atomic.Int64
	fellBack   atomic.Int64
	rejected   atomic.Int64
	storageErr atomic.Int64

	// fallback causes
	fbOversize     atomic.Int64 // body exceeded the buffer cap
	fbParse        atomic.Int64 // malformed pkt-lines
	fbRefDelete    atomic.Int64 // ref deletion (git handles those)
	fbPack         atomic.Int64 // unresolvable pack (bad bytes, missing base)
	fbConnectivity atomic.Int64 // object closure not provable
	fbIdx          atomic.Int64 // idx-unrepresentable
	fbCASFallback  atomic.Int64 // non-CAS UpdateRefs error
}

func (c *goReceiveCounters) snapshot() GoReceiveStats {
	by := map[string]int64{}
	for name, v := range map[string]*atomic.Int64{
		"oversize": &c.fbOversize, "parse": &c.fbParse, "ref_delete": &c.fbRefDelete,
		"pack": &c.fbPack, "connectivity": &c.fbConnectivity, "idx": &c.fbIdx,
		"storage": &c.storageErr, "update_refs": &c.fbCASFallback,
	} {
		if n := v.Load(); n > 0 {
			by[name] = n
		}
	}
	return GoReceiveStats{
		Eligible: c.eligible.Load(), FellBack: c.fellBack.Load(),
		Rejected: c.rejected.Load(), StorageErred: c.storageErr.Load(),
		FellBackBy: by,
	}
}

// goReceiveMaxBytes caps the buffered body the fast path will consider; a
// larger push falls back to streaming git. Agent commits are far under it.
const goReceiveMaxBytes = 2 << 20 // 2 MiB

type pushCmd struct {
	old, new, ref string
}

// tryGoReceive attempts to serve a receive-pack POST entirely in Go from
// an already-buffered body. Returns handled=true only when the push was
// fully processed (ACKed or ref-rejected) in Go; false means the caller
// must fall back to git with the same body. It never returns true after a
// non-CAS failure, so a fallback can never double-apply.
//
// It holds NO repo lock and materializes NOTHING up front: a
// self-contained push (the storm/agent shape) touches only the store and
// the DB, so it can never wait behind a repack or hydration. The local
// cache repo is materialized lazily, only when a thin-pack base or a
// connectivity lookup actually needs it.
func (h *Handler) tryGoReceive(w http.ResponseWriter, r *http.Request, repo, namespace, pusher string, body []byte) bool {
	cmds, packOff, sideband, ok := parsePktCommands(body)
	if !ok || len(cmds) == 0 || packOff >= len(body) {
		h.goRecv.fbParse.Add(1)
		return false // malformed, or ref-only (delete): let git handle it
	}
	// Every command must create/update to a real OID present in this pack.
	// (Deletes -> git; they carry no pack and want the same report plumbing.)
	for _, c := range cmds {
		if c.new == zeroOID {
			h.goRecv.fbRefDelete.Add(1)
			return false
		}
	}
	// The repo must exist BEFORE we write any blob or pack row. authorize()
	// only checks existence on the anon-public path, so an org-wide write
	// token would otherwise let this path Put blobs + insert pack rows under
	// an arbitrary/just-deleted prefix that nothing ever sweeps (the
	// broken-tenant leak). git's own path 404s cleanly; match it.
	if _, err := h.DB.GetRepo(r.Context(), repo); err != nil {
		h.goRecv.fbPack.Add(1)
		return false
	}
	// Oversize guard parity: a push git would reject via receive.maxInputSize
	// must not sneak in through the fast path.
	if h.MaxPushBytes > 0 && int64(len(body)-packOff) > h.MaxPushBytes {
		h.goRecv.fbOversize.Add(1)
		return false
	}

	// Lazy materialization: resolved at most once, on first need.
	var dir string
	var dirErr error
	var dirOnce sync.Once
	lazyDir := func() (string, error) {
		dirOnce.Do(func() { dir, dirErr = h.Cache.MaterializeServe(r.Context(), repo) })
		return dir, dirErr
	}

	// Parse and resolve the pack, including thin-pack REF_DELTA bases
	// pulled from the local cache repo. A thin pack comes back FIXED
	// (bases appended, trailer recomputed) so what we store is always
	// self-contained.
	pack, packBytes, err := ingest.ReadPackThin(body[packOff:], func(oid string) (string, []byte, error) {
		d, err := lazyDir()
		if err != nil {
			return "", nil, err
		}
		typ, _, err := h.Cache.ObjectInfo(repo, d, oid)
		if err != nil {
			return "", nil, err
		}
		data, err := h.Cache.BlobContents(repo, d, oid)
		return typ, data, err
	})
	if err != nil {
		h.goRecv.fbPack.Add(1)
		return false // unresolvable base, corrupt bytes, or over caps -> git
	}

	byOID := make(map[string]ingest.PackObject, len(pack.Objects))
	for _, o := range pack.Objects {
		byOID[o.OID] = o
	}
	// Connectivity: every object reachable from each new tip must be in the
	// pack or already in the store. Any gap -> fall back; we only proceed
	// when we can prove full closure ourselves.
	connOK, externals := h.connectivityOK(r.Context(), repo, lazyDir, cmds, byOID)
	if !connOK {
		h.goRecv.fbConnectivity.Add(1)
		return false
	}

	// Committed to the fast path from here. Persist the verified pack + a
	// generated idx, record it, then CAS the refs - the same durability
	// order as the hook path.
	idx, err := ingest.WriteIdxV2(pack)
	if err != nil {
		h.goRecv.fbIdx.Add(1)
		return false // idx-unrepresentable (e.g. >2GiB offset) -> git
	}
	name := "pack-" + hex.EncodeToString(pack.Trailer[:])
	ctx := r.Context()

	updates := make([]repodb.RefUpdate, len(cmds))
	for i, c := range cmds {
		ref := c.ref
		if namespace != "" {
			ref = "refs/namespaces/" + namespace + "/" + ref
		}
		updates[i] = repodb.RefUpdate{Name: ref, Old: c.old, New: c.new}
	}
	events := webhook.PushEventsFor(repo, updates, pusher)

	// Small packs ride INSIDE the WAL entry: refs and data become durable
	// in one conditional PUT, and the group commit amortizes that PUT
	// across every concurrent push on the repo. Larger packs take the
	// classic path: pack+idx to the store first (in parallel), then CAS.
	type inlineCommitter interface {
		UpdateRefsWithPack(ctx context.Context, repoID string, updates []repodb.RefUpdate, events []repodb.Event, pack *repodb.InlinePack) error
	}
	if ic, ok := h.DB.(inlineCommitter); ok && len(packBytes) <= repodb.InlinePackMax {
		err = ic.UpdateRefsWithPack(ctx, repo, updates, events, &repodb.InlinePack{Name: name, Data: packBytes})
		if err == nil {
			// Best-effort local install so same-node reads skip the store -
			// even into a not-yet-materialized dir (avoids the next
			// materialize racing the async flush into a negative GET).
			installPack(h.Cache.RepoDir(repo), name, packBytes, idx)
		}
	} else {
		// A store failure here is NOT a normal "unusual push" fallback: git
		// will hit the same storage and fail too. Count and log it distinctly
		// so an outage does not hide inside an elevated fell_back number.
		// Pack and idx ship concurrently - one store round trip, not two.
		putErrs := make(chan error, 2)
		go func() { putErrs <- h.Blobs.Put(ctx, repo, name+".pack", bytes.NewReader(packBytes)) }()
		go func() { putErrs <- h.Blobs.Put(ctx, repo, name+".idx", bytes.NewReader(idx)) }()
		for i := 0; i < 2; i++ {
			if err := <-putErrs; err != nil {
				h.storageFallback(repo, "put pack/idx", err)
				return false
			}
		}
		if err := h.DB.AddPacks(ctx, repo, []repodb.Pack{{Name: name, SizeBytes: int64(len(packBytes)), Source: "receive"}}); err != nil {
			h.storageFallback(repo, "add packs", err)
			return false
		}
		// Best-effort local install, only when the repo is already materialized
		// (never materialize just for this; the next read hydrates from the store).
		if d, ok := h.Cache.DirIfMaterialized(repo); ok {
			installPack(d, name, packBytes, idx)
		}
		err = h.DB.UpdateRefs(ctx, repo, updates, events)
	}
	if err != nil {
		if errors.Is(err, repodb.ErrCASFailed) {
			h.goRecv.rejected.Add(1)
			writeReportStatus(w, sideband, cmds, err)
			return true // a real rejection - git would do the same
		}
		// Transactional: nothing applied. Safe to fall back to git.
		h.goRecv.fbCASFallback.Add(1)
		return false
	}
	h.goRecv.eligible.Add(1)
	h.recordTransitions(repo, name, cmds, externals)
	// A workload served entirely by the fast path adds one pack per push and
	// would never cross the consolidation threshold on its own - so clone
	// passthrough (which needs a single gc pack) could never engage. Nudge
	// maintenance like the hook path does.
	if h.NudgeMaintain != nil {
		h.NudgeMaintain(repo)
	}
	writeReportStatus(w, sideband, cmds, nil)
	return true
}

// storageFallback records that the fast path fell back because the store
// errored (not because the push was unusual). git will fail on the same
// store; surfacing this separately turns an S3 outage into a visible signal.
func (h *Handler) storageFallback(repo, op string, err error) {
	h.goRecv.storageErr.Add(1)
	slog.Warn("goreceive: store error, falling back to git", "repo", repo, "op", op, "err", err)
}

// connectivityOK walks each new tip's object closure. Objects in the pack
// are parsed and their references enqueued; objects already in the store
// are accepted without recursion (the store's closure is self-consistent).
// A referenced object in neither place fails the check. The cache repo is
// materialized (via lazyDir) only if an out-of-pack lookup is needed.
// The returned externals list (out-of-pack objects the closure touched)
// feeds the fetch fast path's transition record.
func (h *Handler) connectivityOK(ctx context.Context, repo string, lazyDir func() (string, error), cmds []pushCmd, byOID map[string]ingest.PackObject) (bool, []string) {
	seen := map[string]bool{}
	var externals []string
	var queue []string
	for _, c := range cmds {
		queue = append(queue, c.new)
	}
	for len(queue) > 0 {
		oid := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if oid == "" || oid == zeroOID || seen[oid] {
			continue
		}
		seen[oid] = true

		obj, inPack := byOID[oid]
		if !inPack {
			// Must already be present in the store; don't recurse into it.
			dir, err := lazyDir()
			if err != nil {
				return false, nil
			}
			if _, _, err := h.Cache.ObjectInfo(repo, dir, oid); err != nil {
				return false, nil
			}
			externals = append(externals, oid)
			continue
		}
		refs, err := referencedOIDs(obj)
		if err != nil {
			return false, nil
		}
		queue = append(queue, refs...)
	}
	return true, externals
}

// referencedOIDs returns the OIDs a commit/tree/tag object points at.
// Blobs reference nothing.
func referencedOIDs(o ingest.PackObject) ([]string, error) {
	switch o.Type {
	case "blob":
		return nil, nil
	case "commit":
		return commitRefs(o.Data)
	case "tag":
		return tagRefs(o.Data), nil
	case "tree":
		return treeRefs(o.Data)
	}
	return nil, fmt.Errorf("unknown object type %q", o.Type)
}

func commitRefs(data []byte) ([]string, error) {
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			break // headers end at the blank line before the message
		}
		if t, ok := strings.CutPrefix(line, "tree "); ok {
			out = append(out, strings.TrimSpace(t))
		} else if p, ok := strings.CutPrefix(line, "parent "); ok {
			out = append(out, strings.TrimSpace(p))
		}
	}
	return out, nil
}

func tagRefs(data []byte) []string {
	for _, line := range strings.Split(string(data), "\n") {
		if o, ok := strings.CutPrefix(line, "object "); ok {
			return []string{strings.TrimSpace(o)}
		}
		if line == "" {
			break
		}
	}
	return nil
}

// treeRefs parses git's tree format: repeated "<mode> <name>\x00<20 raw
// oid bytes>".
func treeRefs(data []byte) ([]string, error) {
	var out []string
	for len(data) > 0 {
		nul := bytes.IndexByte(data, 0)
		if nul < 0 || nul+1+20 > len(data) {
			return nil, fmt.Errorf("malformed tree entry")
		}
		oid := hex.EncodeToString(data[nul+1 : nul+1+20])
		out = append(out, oid)
		data = data[nul+1+20:]
	}
	return out, nil
}

// installPack drops the pack+idx into the cache repo so upload-pack serves
// it without a store round-trip. Best-effort: a miss just costs the next
// materialize a fetch. .pack before .idx (never discoverable via idx first).
func installPack(dir, name string, packBytes, idx []byte) {
	packDir := filepath.Join(dir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return
	}
	// Atomic tmp+rename, .pack before .idx: git only discovers a pack via
	// its .idx, so a torn write can never leave an idx pointing at a
	// half-written pack that a concurrent cat-file/upload-pack mmaps.
	if repocache.WriteFileAtomic(filepath.Join(packDir, name+".pack"), packBytes) != nil {
		return
	}
	repocache.WriteFileAtomic(filepath.Join(packDir, name+".idx"), idx)
}

const zeroOID = "0000000000000000000000000000000000000000"

// parsePktCommands reads the receive-pack command list: pkt-lines of
// "<old> <new> <ref>[\x00capabilities]" terminated by a flush-pkt, after
// which the packfile begins. Returns the commands, the offset where the
// pack starts, whether the client negotiated side-band-64k, and ok.
func parsePktCommands(body []byte) (cmds []pushCmd, packOff int, sideband bool, ok bool) {
	i := 0
	first := true
	for {
		if i+4 > len(body) {
			return nil, 0, false, false
		}
		n, err := hexPkt(body[i : i+4])
		if err != nil {
			return nil, 0, false, false
		}
		if n == 0 { // flush-pkt: commands end, pack follows
			return cmds, i + 4, sideband, true
		}
		if n < 4 || i+n > len(body) {
			return nil, 0, false, false
		}
		payload := string(body[i+4 : i+n])
		i += n
		if first {
			if nul := strings.IndexByte(payload, 0); nul >= 0 {
				caps := payload[nul+1:]
				sideband = strings.Contains(caps, "side-band-64k")
				payload = payload[:nul]
			}
			first = false
		}
		f := strings.Fields(strings.TrimRight(payload, "\n"))
		if len(f) != 3 {
			return nil, 0, false, false
		}
		cmds = append(cmds, pushCmd{old: f[0], new: f[1], ref: f[2]})
	}
}

func hexPkt(b []byte) (int, error) {
	n, err := strconv.ParseInt(string(b), 16, 32)
	return int(n), err
}

// writeReportStatus emits the receive-pack result: "unpack ok" then a
// per-ref ok/ng, flush. Under side-band-64k it rides data channel 1.
func writeReportStatus(w io.Writer, sideband bool, cmds []pushCmd, casErr error) {
	var rep bytes.Buffer
	writePkt(&rep, "unpack ok\n")
	for _, c := range cmds {
		if casErr != nil {
			writePkt(&rep, fmt.Sprintf("ng %s fetch first\n", c.ref))
		} else {
			writePkt(&rep, fmt.Sprintf("ok %s\n", c.ref))
		}
	}
	rep.WriteString("0000") // flush inside the report stream

	if !sideband {
		w.Write(rep.Bytes())
		return
	}
	// Side-band: the whole report stream is data on channel 1, then an
	// outer flush closes the response.
	var out bytes.Buffer
	writePkt(&out, "\x01"+rep.String())
	out.WriteString("0000")
	w.Write(out.Bytes())
}

func writePkt(b *bytes.Buffer, s string) {
	fmt.Fprintf(b, "%04x%s", len(s)+4, s)
}
