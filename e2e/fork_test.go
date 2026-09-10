package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Zero-copy forks: instant regardless of size; parent blobs shared until
// the fork's first consolidation; parent never mutated.
func TestInstantFork(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("template")
	writeFile(t, work, "lib.py", "def f(): pass\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "lib")
	e.git(work, "push", "-q")

	t0 := time.Now()
	e.mustAPI("POST", "/api/repos/template/fork", map[string]any{"id": "sandbox-1"}, http.StatusCreated)
	forkMS := time.Since(t0).Milliseconds()
	if forkMS > 500 {
		t.Fatalf("fork took %dms; supposed to be instant", forkMS)
	}

	// Fork clones identically, immediately.
	clone := filepath.Join(e.dir, "fork-clone")
	e.git(e.dir, "clone", e.remote("sandbox-1", ""), clone)
	if got := e.git(clone, "show", "HEAD:lib.py"); got != "def f(): pass\n" {
		t.Fatalf("fork content: %q", got)
	}

	// Diverge the fork; parent stays untouched.
	writeFile(t, clone, "own.txt", "fork only\n")
	e.git(clone, "add", "-A")
	e.git(clone, "commit", "-qm", "fork work")
	e.git(clone, "push", "-q")
	_, parentCommits := e.apiList("GET", "/api/repos/template/commits")
	if len(parentCommits) != 2 {
		t.Fatalf("parent gained commits from fork: %d", len(parentCommits))
	}

	// Fork's consolidation migrates it fully onto its own prefix; parent
	// blobs and clones still work afterwards.
	e.mustAPI("POST", "/api/repos/sandbox-1/maintenance", nil, http.StatusOK)
	packs, _ := e.srv.DB.ListPacks(t.Context(), "sandbox-1")
	if len(packs) != 1 || packs[0].BlobRepo != "" {
		t.Fatalf("fork not self-owned after compact: %+v", packs)
	}
	if err := os.RemoveAll(filepath.Join(e.dir, "server", "cache")); err != nil {
		t.Fatal(err)
	}
	e.git(e.dir, "clone", e.remote("sandbox-1", ""), filepath.Join(e.dir, "fork-cold"))
	e.git(e.dir, "clone", e.remote("template", ""), filepath.Join(e.dir, "parent-cold"))

	// Fork-of-fork points at the ORIGINAL blob owner (no chains).
	e.mustAPI("POST", "/api/repos/template/fork", map[string]any{"id": "sandbox-2"}, http.StatusCreated)
	e.mustAPI("POST", "/api/repos/sandbox-2/fork", map[string]any{"id": "sandbox-3"}, http.StatusCreated)
	p3, _ := e.srv.DB.ListPacks(t.Context(), "sandbox-3")
	for _, p := range p3 {
		if p.BlobRepo != "template" {
			t.Fatalf("grandchild blob_repo should be template: %+v", p)
		}
	}
	e.assertRepoIntegrity("template")
}

// Regression: compacting the PARENT must not delete blobs a fork still
// shares. Before the dependents check, parent consolidation + sweep reaped
// its own superseded blobs, orphaning any fork that hadn't yet consolidated
// onto its own prefix (cold materialize -> permanent ErrNotFound).
func TestForkSurvivesParentCompaction(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("base")
	writeFile(t, work, "a.py", "print(1)\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "a")
	e.git(work, "push", "-q")

	// Fork; the fork shares the parent's blobs (no fork consolidation).
	e.mustAPI("POST", "/api/repos/base/fork", map[string]any{"id": "child"}, http.StatusCreated)

	// Push more to the parent so it has multiple packs, then compact the
	// PARENT (minPacks<=1 forces it). This supersedes the shared blob.
	writeFile(t, work, "b.py", "print(2)\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "b")
	e.git(work, "push", "-q")
	e.mustAPI("POST", "/api/repos/base/maintenance", nil, http.StatusOK)

	// Cold-materialize the fork from the store: its shared blobs must still
	// exist despite the parent's compaction.
	if err := os.RemoveAll(filepath.Join(e.dir, "server", "cache")); err != nil {
		t.Fatal(err)
	}
	clone := filepath.Join(e.dir, "child-cold")
	e.git(e.dir, "clone", e.remote("child", ""), clone)
	if got := e.git(clone, "show", "HEAD:a.py"); got != "print(1)\n" {
		t.Fatalf("fork lost shared blob after parent compaction: %q", got)
	}
}
