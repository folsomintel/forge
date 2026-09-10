package githttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/folsomintel/forge/internal/repodb"
)

// Fork-free ref advertisement. The advertisement is the hottest git
// operation there is - every fetch/push/poll starts with one - and it is
// pure ref data we already hold in SQLite. Serving it in Go removes a git
// fork AND a materialization from every info/refs, which is most of the
// request volume an agent workload generates.
//
// Capability strings are CALIBRATED, not hardcoded: once per process we
// run the installed git against a template repo (with the same config our
// cache repos use) and lift its capability sets for v0 upload-pack, v2
// upload-pack, and receive-pack. Whatever git would advertise on this box,
// we advertise - drift is impossible. The POST side stays split: pushes
// land in tryGoReceive or git; v2 ls-refs and bundle-uri are answered in
// Go; fetch negotiation still forks git.

const goAgent = "agent=forge/goadv"

// v2CommandMaxBytes bounds the buffered upload-pack POST we inspect for
// v2 metadata commands; a bigger body (a real fetch negotiation) streams
// to git with the prefix replayed.
const v2CommandMaxBytes = 64 << 10

type advCaps struct {
	uploadV0  string   // upload-pack v0 capability string (sans agent/symref)
	receiveV0 string   // receive-pack capability string (sans agent)
	uploadV2  []string // v2 capability lines (sans agent)
}

var (
	capsOnce sync.Once
	caps     advCaps
)

// calibrateCaps builds a throwaway bare repo configured exactly like a
// cache repo and asks git what it would advertise. Falls back to
// conservative baked-in sets if git is unavailable (tests without git).
func calibrateCaps() advCaps {
	fallback := advCaps{
		uploadV0: "multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not deepen-relative no-progress include-tag multi_ack_detailed no-done filter object-format=sha1",
		receiveV0: "report-status report-status-v2 delete-refs side-band-64k quiet atomic ofs-delta " +
			"push-options object-format=sha1",
		uploadV2: []string{"version 2", "ls-refs=unborn", "fetch=shallow wait-for-done filter", "server-option", "object-format=sha1"},
	}
	dir, err := os.MkdirTemp("", "forge-caps-*")
	if err != nil {
		return fallback
	}
	defer os.RemoveAll(dir)
	for _, d := range []string{"objects/pack", "objects/info", "refs/heads"} {
		os.MkdirAll(filepath.Join(dir, d), 0o755)
	}
	// Same config as cache repos so conditional caps (filter, push-options)
	// calibrate identically. bundles.conf intentionally absent.
	cfg := "[core]\n\trepositoryformatversion = 0\n\tbare = true\n" +
		"[receive]\n\tadvertisePushOptions = true\n" +
		"[uploadpack]\n\tallowFilter = true\n"
	os.WriteFile(filepath.Join(dir, "config"), []byte(cfg), 0o644)
	os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644)

	run := func(service string, env ...string) ([]string, error) {
		bin := strings.TrimPrefix(service, "git-")
		cmd := exec.Command("git", bin, "--stateless-rpc", "--advertise-refs", dir)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.Output()
		if err != nil {
			return nil, err
		}
		return parsePktLines(out)
	}
	stripTokens := func(capstr string) string {
		var keep []string
		for _, tok := range strings.Fields(capstr) {
			if strings.HasPrefix(tok, "agent=") || strings.HasPrefix(tok, "session-id=") ||
				strings.HasPrefix(tok, "symref=") {
				continue
			}
			keep = append(keep, tok)
		}
		return strings.Join(keep, " ")
	}
	v0caps := func(lines []string) (string, bool) {
		// First line: "<oid> <ref>\x00<caps>".
		if len(lines) == 0 {
			return "", false
		}
		_, capstr, ok := strings.Cut(lines[0], "\x00")
		if !ok {
			return "", false
		}
		return stripTokens(strings.TrimSuffix(capstr, "\n")), true
	}

	out := advCaps{}
	if lines, err := run("git-upload-pack"); err == nil {
		if c, ok := v0caps(lines); ok {
			out.uploadV0 = c
		}
	}
	if lines, err := run("git-receive-pack"); err == nil {
		if c, ok := v0caps(lines); ok {
			out.receiveV0 = c
		}
	}
	if lines, err := run("git-upload-pack", "GIT_PROTOCOL=version=2"); err == nil {
		for _, l := range lines {
			l = strings.TrimSuffix(l, "\n")
			if strings.HasPrefix(l, "agent=") || strings.HasPrefix(l, "session-id=") {
				continue
			}
			out.uploadV2 = append(out.uploadV2, l)
		}
	}
	if out.uploadV0 == "" {
		out.uploadV0 = fallback.uploadV0
	}
	if out.receiveV0 == "" {
		out.receiveV0 = fallback.receiveV0
	}
	if len(out.uploadV2) == 0 {
		out.uploadV2 = fallback.uploadV2
	}
	return out
}

func advCapsFor() advCaps {
	capsOnce.Do(func() { caps = calibrateCaps() })
	return caps
}

// parsePktLines splits a pkt-line stream into payload strings (flush and
// delim packets are skipped; parsing stops at stream end).
func parsePktLines(data []byte) ([]string, error) {
	var out []string
	i := 0
	for i+4 <= len(data) {
		n, err := hexPkt(data[i : i+4])
		if err != nil {
			return nil, err
		}
		if n == 0 || n == 1 { // flush / delim
			i += 4
			continue
		}
		if n < 4 || i+n > len(data) {
			return nil, fmt.Errorf("bad pkt length %d", n)
		}
		out = append(out, string(data[i+4:i+n]))
		i += n
	}
	return out, nil
}

func pktf(w io.Writer, format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	fmt.Fprintf(w, "%04x%s", len(s)+4, s)
}

func flushPkt(w io.Writer) { io.WriteString(w, "0000") }

// advRefs assembles the (namespace-filtered, sorted) ref list for the
// advertisement, straight from the index.
func (h *Handler) advRefs(ctx context.Context, repo, ns string) ([]repodb.Ref, error) {
	refs, err := h.DB.ListRefs(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := refs[:0]
	prefix := ""
	if ns != "" {
		prefix = "refs/namespaces/" + ns + "/"
	}
	for _, r := range refs {
		if ns == "" {
			if strings.HasPrefix(r.Name, "refs/namespaces/") {
				continue // hidden in the normal view (transfer.hideRefs parity)
			}
			out = append(out, r)
			continue
		}
		if rest, ok := strings.CutPrefix(r.Name, prefix); ok {
			out = append(out, repodb.Ref{Name: rest, Target: r.Target})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// maxPeeledEntries bounds the process-wide peel cache. Keys are content
// addresses so entries never go stale, but a server that serves millions of
// distinct tags over its lifetime would otherwise grow the map without
// limit. On overflow we clear it wholesale (cheap, self-healing: the cache
// is a pure optimization the protocol tolerates missing) rather than track
// an LRU we'd never tune.
const maxPeeledEntries = 50000

// storePeeled caches a peel result under the entry cap.
func (h *Handler) storePeeled(oid, peeled string) {
	if h.peeledN.Load() >= maxPeeledEntries {
		h.peeled.Range(func(k, _ any) bool { h.peeled.Delete(k); return true })
		h.peeledN.Store(0)
	}
	if _, loaded := h.peeled.LoadOrStore(oid, peeled); !loaded {
		h.peeledN.Add(1)
	}
}

// peel resolves an annotated tag chain to its underlying object. Tags are
// immutable, so results cache forever. Returns "" when the target is not a
// tag or the repo isn't materialized (peeled lines are an optimization the
// protocol tolerates missing).
func (h *Handler) peel(repo, oid string) string {
	if v, ok := h.peeled.Load(oid); ok {
		return v.(string)
	}
	dir, ok := h.Cache.DirIfMaterialized(repo)
	if !ok {
		return ""
	}
	cur := oid
	for depth := 0; depth < 10; depth++ {
		typ, _, err := h.Cache.ObjectInfo(repo, dir, cur)
		if err != nil {
			return ""
		}
		if typ != "tag" {
			if cur == oid {
				h.storePeeled(oid, "") // not a tag; remember that too
				return ""
			}
			h.storePeeled(oid, cur)
			return cur
		}
		data, err := h.Cache.BlobContents(repo, dir, cur)
		if err != nil {
			return ""
		}
		next := tagRefs(data)
		if len(next) == 0 {
			return ""
		}
		cur = next[0]
	}
	return ""
}

// goInfoRefs serves GET /info/refs entirely in Go. Returns false when the
// caller should fork git instead (escape hatch on).
func (h *Handler) goInfoRefs(w http.ResponseWriter, r *http.Request, service, repo, ns string) bool {
	if h.ForkAdvertisement {
		return false
	}
	ctx := r.Context()
	repoRow, err := h.DB.GetRepo(ctx, repo)
	if err != nil {
		http.Error(w, "repository not found", http.StatusNotFound)
		return true
	}
	refs, err := h.advRefs(ctx, repo, ns)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return true
	}
	c := advCapsFor()

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")
	var b bytes.Buffer
	pktf(&b, "# service=%s\n", service)
	flushPkt(&b)

	if service == "git-upload-pack" && strings.Contains(r.Header.Get("Git-Protocol"), "version=2") {
		// v2 capability advertisement: no refs here; the client asks via
		// ls-refs. bundle-uri joins the set when this repo advertises one.
		for _, line := range c.uploadV2 {
			pktf(&b, "%s\n", line)
		}
		pktf(&b, "%s\n", goAgent)
		if dir, ok := h.Cache.DirIfMaterialized(repo); ok {
			if _, err := os.Stat(filepath.Join(dir, "bundles.conf")); err == nil {
				pktf(&b, "bundle-uri\n")
			}
		}
		flushPkt(&b)
		w.Write(b.Bytes())
		return true
	}

	headTarget := ""
	for _, ref := range refs {
		if ref.Name == "refs/heads/"+repoRow.DefaultBranch {
			headTarget = ref.Target
		}
	}

	first := true
	writeRef := func(oid, name, caps string) {
		if caps != "" {
			pktf(&b, "%s %s\x00%s\n", oid, name, caps)
		} else {
			pktf(&b, "%s %s\n", oid, name)
		}
	}
	switch service {
	case "git-receive-pack":
		capstr := c.receiveV0 + " " + goAgent
		for _, ref := range refs {
			if first {
				writeRef(ref.Target, ref.Name, capstr)
				first = false
			} else {
				writeRef(ref.Target, ref.Name, "")
			}
		}
		if first {
			writeRef(zeroOID, "capabilities^{}", capstr)
		}
	case "git-upload-pack":
		capstr := c.uploadV0
		if headTarget != "" {
			capstr += " symref=HEAD:refs/heads/" + repoRow.DefaultBranch
		}
		capstr += " " + goAgent
		if headTarget != "" {
			writeRef(headTarget, "HEAD", capstr)
			first = false
		}
		for _, ref := range refs {
			if first {
				writeRef(ref.Target, ref.Name, capstr)
				first = false
			} else {
				writeRef(ref.Target, ref.Name, "")
			}
			if strings.HasPrefix(ref.Name, "refs/tags/") {
				if p := h.peel(repo, ref.Target); p != "" {
					writeRef(p, ref.Name+"^{}", "")
				}
			}
		}
		if first {
			writeRef(zeroOID, "capabilities^{}", capstr)
		}
	}
	flushPkt(&b)
	w.Write(b.Bytes())
	return true
}

// goUploadPackCommand intercepts protocol-v2 upload-pack commands that are
// pure ref/metadata reads (ls-refs, bundle-uri) and answers them in Go.
// Returns handled=false for anything else (fetch, object-info) - the
// caller replays the body to git.
func (h *Handler) goUploadPackCommand(w http.ResponseWriter, r *http.Request, repo, ns string, body []byte) bool {
	if h.ForkAdvertisement {
		return false
	}
	lines, err := parsePktLines(body)
	if err != nil || len(lines) == 0 {
		return false
	}
	cmd := strings.TrimSuffix(lines[0], "\n")
	switch cmd {
	case "command=ls-refs":
		return h.goLsRefs(w, r, repo, ns, lines[1:])
	case "command=bundle-uri":
		return h.goBundleURI(w, repo)
	case "command=fetch":
		return h.goFetch(w, r, repo, ns, lines)
	}
	return false
}

func (h *Handler) goLsRefs(w http.ResponseWriter, r *http.Request, repo, ns string, args []string) bool {
	ctx := r.Context()
	repoRow, err := h.DB.GetRepo(ctx, repo)
	if err != nil {
		return false
	}
	refs, err := h.advRefs(ctx, repo, ns)
	if err != nil {
		return false
	}
	var prefixes []string
	wantPeel, wantSymrefs, wantUnborn := false, false, false
	for _, a := range args {
		a = strings.TrimSuffix(a, "\n")
		switch {
		case a == "peel":
			wantPeel = true
		case a == "symrefs":
			wantSymrefs = true
		case a == "unborn":
			wantUnborn = true
		case strings.HasPrefix(a, "ref-prefix "):
			prefixes = append(prefixes, strings.TrimPrefix(a, "ref-prefix "))
		}
	}
	match := func(name string) bool {
		if len(prefixes) == 0 {
			return true
		}
		for _, p := range prefixes {
			if strings.HasPrefix(name, p) {
				return true
			}
		}
		return false
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	var b bytes.Buffer
	headRef := "refs/heads/" + repoRow.DefaultBranch
	headTarget := ""
	for _, ref := range refs {
		if ref.Name == headRef {
			headTarget = ref.Target
		}
	}
	if match("HEAD") {
		switch {
		case headTarget != "":
			line := headTarget + " HEAD"
			if wantSymrefs {
				line += " symref-target:" + headRef
			}
			pktf(&b, "%s\n", line)
		case wantUnborn:
			pktf(&b, "unborn HEAD symref-target:%s\n", headRef)
		}
	}
	for _, ref := range refs {
		if !match(ref.Name) {
			continue
		}
		line := ref.Target + " " + ref.Name
		if wantPeel && strings.HasPrefix(ref.Name, "refs/tags/") {
			if p := h.peel(repo, ref.Target); p != "" {
				line += " peeled:" + p
			}
		}
		pktf(&b, "%s\n", line)
	}
	flushPkt(&b)
	w.Write(b.Bytes())
	return true
}

// goBundleURI answers command=bundle-uri from the repo's bundles.conf (the
// same file git would read). Emits the whole chain in creationToken order
// with mode=all so git bootstraps from the full and only pulls newer
// incrementals on later fetches. No bundles advertised = an empty valid list.
func (h *Handler) goBundleURI(w http.ResponseWriter, repo string) bool {
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	var b bytes.Buffer
	if dir, ok := h.Cache.DirIfMaterialized(repo); ok {
		if data, err := os.ReadFile(filepath.Join(dir, "bundles.conf")); err == nil {
			pktf(&b, "bundle.version=1\n")
			mode := "any"
			if strings.Contains(string(data), "mode = all") {
				mode = "all"
			}
			pktf(&b, "bundle.mode=%s\n", mode)
			// Parse the git-config-style file: each [bundle "<id>"] section
			// carries a uri and (for chains) a creationToken.
			id := "clone"
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if sect, ok := strings.CutPrefix(line, `[bundle "`); ok {
					id = strings.TrimSuffix(sect, `"]`)
					continue
				}
				if uri, ok := strings.CutPrefix(line, "uri = "); ok {
					pktf(&b, "bundle.%s.uri=%s\n", id, uri)
				}
				if tok, ok := strings.CutPrefix(line, "creationToken = "); ok {
					pktf(&b, "bundle.%s.creationToken=%s\n", id, tok)
				}
			}
		}
	}
	flushPkt(&b)
	w.Write(b.Bytes())
	return true
}
