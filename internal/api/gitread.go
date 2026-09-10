package api

import (
	"container/heap"
	"context"
	"strings"

	"github.com/folsomintel/forge/internal/gitcmd"
)

// Fork-free REST reads: the same treatment the git wire paths got, applied
// to the API surface. Tree listings, file resolution, and commit walks go
// through the pooled cat-file daemon + Go object parsers instead of
// forking `git ls-tree` / `git log` per request - under many concurrent
// readers there is no fork stampede, no fork-gate queueing, and no
// write-lock acquisition (MaterializeServe never convoys reads).
//
// Every function here returns ok=false on ANY anomaly (unmaterialized
// repo, mid-swap object miss, parse error) and the caller falls back to
// the fork path, which is always correct.

// readDir returns the repo's cache dir for pool reads without taking the
// write lock; stale-but-serving is fine for reads (CAS arbitrates writes).
func (s *Server) readDir(ctx context.Context, repoID string) (string, bool) {
	dir, err := s.Cache.MaterializeServe(ctx, repoID)
	if err != nil {
		return "", false
	}
	return dir, true
}

// commitTree resolves a commit (or tag chain) oid to its root tree.
func (s *Server) commitTree(repoID, dir, oid string) (string, bool) {
	for depth := 0; depth < 10; depth++ {
		typ, _, err := s.Cache.ObjectInfo(repoID, dir, oid)
		if err != nil {
			return "", false
		}
		data, err := s.Cache.BlobContents(repoID, dir, oid)
		if err != nil {
			return "", false
		}
		switch typ {
		case "commit":
			cm, err := gitcmd.ParseCommitRaw(oid, data)
			if err != nil {
				return "", false
			}
			return cm.Tree, true
		case "tag":
			next := ""
			for _, line := range strings.Split(string(data), "\n") {
				if o, ok := strings.CutPrefix(line, "object "); ok {
					next = strings.TrimSpace(o)
					break
				}
				if line == "" {
					break
				}
			}
			if next == "" {
				return "", false
			}
			oid = next
		case "tree":
			return oid, true
		default:
			return "", false
		}
	}
	return "", false
}

// resolveTreePath walks path components from a root tree. found=false
// with ok=true is a GENUINE not-found (the walk itself succeeded);
// ok=false means machinery failure (caller falls back to forking git).
func (s *Server) resolveTreePath(repoID, dir, rootTree, path string) (entry gitcmd.TreeEntry, found, ok bool) {
	cur := gitcmd.TreeEntry{Mode: "040000", Type: "tree", SHA: rootTree, Path: ""}
	if path == "" {
		return cur, true, true
	}
	for _, comp := range strings.Split(path, "/") {
		if cur.Type != "tree" {
			return cur, false, true // descends through a non-directory: not found
		}
		data, err := s.Cache.BlobContents(repoID, dir, cur.SHA)
		if err != nil {
			return cur, false, false
		}
		entries, err := gitcmd.ParseTreeRaw(data)
		if err != nil {
			return cur, false, false
		}
		next, hit := gitcmd.TreeEntry{}, false
		for _, e := range entries {
			if e.Path == comp {
				next, hit = e, true
				break
			}
		}
		if !hit {
			return cur, false, true
		}
		cur = next
	}
	cur.Path = path
	return cur, true, true
}

// listTree returns a directory's entries with blob sizes (via the pool;
// tree/submodule entries keep size 0, matching ls-tree -l).
func (s *Server) listTree(repoID, dir, treeOID, prefix string) ([]gitcmd.TreeEntry, bool) {
	data, err := s.Cache.BlobContents(repoID, dir, treeOID)
	if err != nil {
		return nil, false
	}
	entries, err := gitcmd.ParseTreeRaw(data)
	if err != nil {
		return nil, false
	}
	for i := range entries {
		if prefix != "" {
			entries[i].Path = prefix + "/" + entries[i].Path
		}
		if entries[i].Type == "blob" {
			_, size, err := s.Cache.ObjectInfo(repoID, dir, entries[i].SHA)
			if err != nil {
				return nil, false
			}
			entries[i].Size = size
		}
	}
	return entries, true
}

// --- commit walk (fork-free `git log`) ---

// commitHeap orders by committer date descending (git log's traversal
// order), tie-broken by insertion order for determinism.
type walkItem struct {
	meta gitcmd.CommitMeta
	ord  int
}
type commitHeap []walkItem

func (h commitHeap) Len() int { return len(h) }
func (h commitHeap) Less(i, j int) bool {
	if h[i].meta.CommitterTS != h[j].meta.CommitterTS {
		return h[i].meta.CommitterTS > h[j].meta.CommitterTS
	}
	return h[i].ord < h[j].ord
}
func (h commitHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *commitHeap) Push(x any)   { *h = append(*h, x.(walkItem)) }
func (h *commitHeap) Pop() any     { old := *h; n := len(old); it := old[n-1]; *h = old[:n-1]; return it }

// logWalk lists up to limit commits reachable from tip, newest first by
// committer date - the same order `git log` produces. ok=false on any
// read/parse anomaly (caller forks git).
func (s *Server) logWalk(repoID, dir, tip string, limit int) ([]gitcmd.Commit, bool) {
	read := func(oid string) (gitcmd.CommitMeta, bool) {
		data, err := s.Cache.BlobContents(repoID, dir, oid)
		if err != nil {
			return gitcmd.CommitMeta{}, false
		}
		cm, err := gitcmd.ParseCommitRaw(oid, data)
		if err != nil {
			return gitcmd.CommitMeta{}, false
		}
		return cm, true
	}
	first, ok := read(tip)
	if !ok {
		return nil, false
	}
	h := &commitHeap{}
	heap.Init(h)
	ord := 0
	seen := map[string]bool{tip: true}
	heap.Push(h, walkItem{meta: first, ord: ord})

	var out []gitcmd.Commit
	reads := 0
	for h.Len() > 0 && len(out) < limit {
		it := heap.Pop(h).(walkItem)
		out = append(out, it.meta.Commit)
		for _, p := range it.meta.Parents {
			if seen[p] {
				continue
			}
			seen[p] = true
			reads++
			if reads > limit*8+256 {
				return nil, false // pathological topology: let git handle it
			}
			pm, ok := read(p)
			if !ok {
				return nil, false
			}
			ord++
			heap.Push(h, walkItem{meta: pm, ord: ord})
		}
	}
	return out, true
}
