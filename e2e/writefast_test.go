package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The Go-native commit builder must produce trees real git accepts under
// --strict, including the classic sort trap: a directory "a" sorts as
// "a/", i.e. AFTER "a.txt" and BEFORE "a0.txt". Wrong order = fsck error.
func TestFastWriteTreeSortAndFsck(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("fw")

	// One multi-file commit exercising the sort trap + nesting + modes.
	e.mustAPI("POST", "/api/repos/fw/commits", map[string]any{
		"message": "adversarial",
		"changes": []map[string]any{
			{"path": "a.txt", "content": b64("dot\n")},
			{"path": "a/inner.txt", "content": b64("nested\n")},
			{"path": "a0.txt", "content": b64("zero\n")},
			{"path": "a/b/deep.txt", "content": b64("deep\n")},
			{"path": "run.sh", "content": b64("#!/bin/sh\n"), "mode": "100755"},
			{"path": "link", "content": b64("a.txt"), "mode": "120000"},
		},
	}, http.StatusCreated)

	// Single-file PUT on top (separate fast-path commit).
	e.mustAPI("PUT", "/api/repos/fw/contents/a/second.txt", map[string]any{
		"message": "second", "content": b64("two\n"),
	}, http.StatusCreated)

	// Delete that prunes a subtree ("a/b" empties).
	e.mustAPI("DELETE", "/api/repos/fw/contents/a/b/deep.txt", map[string]any{
		"message": "prune",
	}, http.StatusCreated)

	// Prove the fast path engaged: inline-committed packs are recorded by
	// the WAL apply with Source "receive"; the fork path writes "api".
	packs, _ := e.srv.DB.ListPacks(t.Context(), "fw")
	inline := 0
	for _, p := range packs {
		if p.Source == "receive" {
			inline++
		}
	}
	if inline < 3 {
		t.Fatalf("fast write path not engaging: pack sources %+v", packs)
	}

	// Real git must accept every object under --strict.
	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("fw", ""), clone)
	if out := e.git(clone, "fsck", "--strict", "--no-dangling"); strings.TrimSpace(out) != "" {
		t.Fatalf("fsck rejected fast-write objects: %s", out)
	}
	// Content + mode round-trips.
	got, err := os.ReadFile(filepath.Join(clone, "a", "inner.txt"))
	if err != nil || string(got) != "nested\n" {
		t.Fatalf("nested content: %q %v", got, err)
	}
	info, err := os.Stat(filepath.Join(clone, "run.sh"))
	if err != nil || info.Mode()&0o100 == 0 {
		t.Fatalf("exec bit lost: %v %v", info, err)
	}
	if target, err := os.Readlink(filepath.Join(clone, "link")); err != nil || target != "a.txt" {
		t.Fatalf("symlink: %q %v", target, err)
	}
	if _, err := os.Stat(filepath.Join(clone, "a", "b")); !os.IsNotExist(err) {
		t.Fatal("emptied subtree not pruned")
	}
	// git's own sort check: the root tree lists a.txt < a < a0.txt.
	lsout := e.git(clone, "ls-tree", "--name-only", "HEAD")
	names := strings.Fields(lsout)
	idx := map[string]int{}
	for i, n := range names {
		idx[n] = i
	}
	if !(idx["a.txt"] < idx["a"] && idx["a"] < idx["a0.txt"]) {
		t.Fatalf("tree order wrong: %v", names)
	}

	// And git interop both directions: a git push on top of API commits.
	e.git(clone, "config", "user.email", "t@example.com")
	e.git(clone, "config", "user.name", "t")
	os.WriteFile(filepath.Join(clone, "gitfile.txt"), []byte("via git\n"), 0o644)
	e.git(clone, "add", ".")
	e.git(clone, "commit", "-q", "-m", "git on top")
	e.git(clone, "push", "-q", "origin", "HEAD:main")
	got2 := e.mustAPI("GET", "/api/repos/fw/contents/gitfile.txt", nil, http.StatusOK)
	if got2["content"] != b64("via git\n") {
		t.Fatalf("git-on-top content: %v", got2)
	}
}

// Security: the Go write builder must not accept paths git's index would
// reject, nor let author fields inject commit headers. These are blocked
// at the API boundary so fast and fork paths behave identically.
func TestFastWriteRejectsMaliciousInput(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("sec")
	e.mustAPI("PUT", "/api/repos/sec/contents/seed.txt", map[string]any{
		"message": "seed", "content": b64("x\n"),
	}, http.StatusCreated)

	// .git-family + control-byte paths rejected (single-file + multi-file).
	for _, p := range []string{".git/hooks/pre-commit", ".git/config", "a/.git/x", ".GIT/x", ".git./x"} {
		e.mustAPI("PUT", "/api/repos/sec/contents/"+p, map[string]any{
			"message": "evil", "content": b64("#!/bin/sh\n"),
		}, http.StatusBadRequest)
	}
	e.mustAPI("POST", "/api/repos/sec/commits", map[string]any{
		"message": "evil",
		"changes": []map[string]any{{"path": ".git/hooks/post-checkout", "content": b64("evil\n")}},
	}, http.StatusBadRequest)
	// NUL byte in a JSON path (can't traverse a URL, but a body can carry it)
	// would corrupt the tree object - must be rejected, not stored.
	e.mustAPI("POST", "/api/repos/sec/commits", map[string]any{
		"message": "evil",
		"changes": []map[string]any{{"path": "bad\u0000name.txt", "content": b64("x\n")}},
	}, http.StatusBadRequest)

	// Newline in author name must not inject a fake parent header.
	out := e.mustAPI("PUT", "/api/repos/sec/contents/ok.txt", map[string]any{
		"message": "legit",
		"content": b64("data\n"),
		"author":  map[string]any{"name": "evil\nparent 0000000000000000000000000000000000000000", "email": "e@e"},
	}, http.StatusCreated)
	commit, _ := out["commit"].(map[string]any)
	parents, _ := commit["parents"].([]any)
	// Exactly one real parent (the seed commit), not two.
	if len(parents) != 1 {
		t.Fatalf("author-newline injected a parent: parents=%v", parents)
	}

	// The whole thing still clones clean under fsck.
	clone := filepath.Join(e.dir, "secclone")
	e.git(e.dir, "clone", e.remote("sec", ""), clone)
	if out := e.git(clone, "fsck", "--strict"); strings.TrimSpace(out) != "" {
		t.Fatalf("fsck: %s", out)
	}
	if _, err := os.Stat(filepath.Join(clone, ".git", "hooks", "pre-commit")); err == nil {
		t.Fatal("evil hook was planted")
	}
}
