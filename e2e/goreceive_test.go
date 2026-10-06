package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/folsomintel/forge/internal/config"
)

// The load-test finding: real `git push` sends thin/delta packs, so the
// fast path must handle them, not just the no-delta first push. This test
// drives real git through GoReceive and asserts the incremental pushes -
// the hot path - are served in Go, and that what it stored is a clone-able
// repo.
func TestGoReceiveHandlesRealGitPushes(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.GoReceive = true })
	e.createRepo("fast")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("fast", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")

	// Push 1: brand-new objects (no deltas possible against the server).
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("line one\nline two\nline three\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "first")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	// Pushes 2..4: incremental edits - git sends thin packs whose tree
	// deltas reference objects only the server has. Before delta support,
	// every one of these fell back (eligible stuck at the initial pushes).
	for i, content := range []string{
		"line one\nline two\nline three\nline four\n",
		"line one\nCHANGED two\nline three\nline four\n",
		"line one\nCHANGED two\nline three\nline four\nline five\n",
	} {
		os.WriteFile(filepath.Join(work, "a.txt"), []byte(content), 0o644)
		e.git(work, "add", ".")
		e.git(work, "commit", "-q", "-m", "edit")
		e.git(work, "push", "-q", "origin", "HEAD:main")
		_ = i
	}

	stats := e.srv.GitHTTP.GoReceiveStats()
	if stats.Eligible < 3 {
		t.Fatalf("goreceive not engaging on real pushes: eligible=%d fell_back=%d by=%v",
			stats.Eligible, stats.FellBack, stats.FellBackBy)
	}

	// What the fast path stored must be a complete, consistent repo.
	verify := filepath.Join(e.dir, "verify")
	e.git(e.dir, "clone", e.remote("fast", ""), verify)
	got, err := os.ReadFile(filepath.Join(verify, "a.txt"))
	if err != nil {
		t.Fatalf("clone after goreceive pushes: %v", err)
	}
	want := "line one\nCHANGED two\nline three\nline four\nline five\n"
	if string(got) != want {
		t.Fatalf("content after goreceive pushes = %q, want %q", got, want)
	}
	if out := e.git(verify, "fsck", "--strict"); out != "" {
		t.Logf("fsck output: %s", out)
	}
}

// Branch cleanup (an all-delete push carries no pack) is served in Go; a
// delete of the default branch still goes to git, which refuses it.
func TestGoReceiveDeleteOnlyPush(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.GoReceive = true })
	e.createRepo("del")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("del", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("a\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main", "HEAD:refs/heads/scratch-1", "HEAD:refs/heads/scratch-2")

	before := e.srv.GitHTTP.GoReceiveStats()
	e.git(work, "push", "-q", "origin", "--delete", "scratch-1", "scratch-2")
	after := e.srv.GitHTTP.GoReceiveStats()
	if after.Eligible != before.Eligible+1 || after.FellBack != before.FellBack {
		t.Fatalf("delete-only push left the fast path: before=%+v after=%+v", before, after)
	}
	if out := e.git(work, "ls-remote", "origin", "refs/heads/scratch-*"); out != "" {
		t.Fatalf("branches survived delete: %q", out)
	}

	before = after
	if _, err := e.gitErr(work, "push", "-q", "origin", "--delete", "main"); err == nil {
		t.Logf("git accepted deleting the default branch (bare repo)")
	}
	after = e.srv.GitHTTP.GoReceiveStats()
	if after.Eligible != before.Eligible || after.FellBackBy["ref_delete"] <= before.FellBackBy["ref_delete"] {
		t.Fatalf("default-branch delete must go to git: before=%+v after=%+v", before, after)
	}
}
