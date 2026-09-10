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
