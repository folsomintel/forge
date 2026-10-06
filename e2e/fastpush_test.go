package e2e

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
)

// The load-test finding: real `git push` sends thin/delta packs, so the
// fast path must handle them, not just the no-delta first push. This test
// drives real git through FastPush and asserts the incremental pushes -
// the hot path - are served in Go, and that what it stored is a clone-able
// repo.
func TestFastPushHandlesRealGitPushes(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.FastPush = true })
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

	stats := e.srv.GitHTTP.FastPushStats()
	if stats.Eligible < 3 {
		t.Fatalf("fast push not engaging on real pushes: eligible=%d fell_back=%d by=%v",
			stats.Eligible, stats.FellBack, stats.FellBackBy)
	}

	// What the fast path stored must be a complete, consistent repo.
	verify := filepath.Join(e.dir, "verify")
	e.git(e.dir, "clone", e.remote("fast", ""), verify)
	got, err := os.ReadFile(filepath.Join(verify, "a.txt"))
	if err != nil {
		t.Fatalf("clone after fast-path pushes: %v", err)
	}
	want := "line one\nCHANGED two\nline three\nline four\nline five\n"
	if string(got) != want {
		t.Fatalf("content after fast-path pushes = %q, want %q", got, want)
	}
	if out := e.git(verify, "fsck", "--strict"); out != "" {
		t.Logf("fsck output: %s", out)
	}
}

// Branch cleanup (an all-delete push carries no pack) is served in Go; a
// delete of the default branch still goes to git, which refuses it.
func TestFastPushDeleteOnlyPush(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.FastPush = true })
	e.createRepo("del")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("del", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("a\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main", "HEAD:refs/heads/scratch-1", "HEAD:refs/heads/scratch-2")

	before := e.srv.GitHTTP.FastPushStats()
	e.git(work, "push", "-q", "origin", "--delete", "scratch-1", "scratch-2")
	after := e.srv.GitHTTP.FastPushStats()
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
	after = e.srv.GitHTTP.FastPushStats()
	if after.Eligible != before.Eligible || after.FellBackBy["ref_delete"] <= before.FellBackBy["ref_delete"] {
		t.Fatalf("default-branch delete must go to git: before=%+v after=%+v", before, after)
	}
}

// A crafted push with a ref name git refuses must never be stored: ref
// names become cache file paths, and refs/heads/../../x once escaped the
// repo directory. The normal view also may not write hidden namespace refs.
func TestFastPushRejectsFunnyAndHiddenRefs(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.FastPush = true })
	e.createRepo("funny")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("funny", ""), work)
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("a\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	// Raw receive-pack request (real git clients refuse to send these).
	blob := strings.TrimSpace(e.git(work, "hash-object", "-w", "a.txt"))
	pack := exec.Command("git", "pack-objects", "-q", "--stdout")
	pack.Dir = work
	pack.Stdin = strings.NewReader(blob + "\n")
	packBytes, err := pack.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"refs/heads/../../../escaped", "refs/heads/.hidden", "HEAD"} {
		line := fmt.Sprintf("%s %s %s\x00report-status\n", strings.Repeat("0", 40), blob, ref)
		body := append([]byte(fmt.Sprintf("%04x%s0000", len(line)+4, line)), packBytes...)
		req, _ := http.NewRequest("POST", e.base+"/funny.git/git-receive-pack", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+e.token)
		req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(out), "ok "+ref) {
			t.Errorf("push of %q was accepted: %q", ref, out)
		}
	}
	if out := e.git(work, "ls-remote", "origin"); strings.Contains(out, "escaped") || strings.Contains(out, ".hidden") {
		t.Fatalf("funny ref stored:\n%s", out)
	}
	// A read materializes the cache; nothing may land outside refs/.
	e.git(e.dir, "clone", "-q", e.remote("funny", ""), filepath.Join(e.dir, "verify"))
	filepath.WalkDir(e.dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Name() == "escaped" {
			t.Errorf("ref name escaped the repo dir: %s", path)
		}
		return nil
	})

	// The normal view must not create hidden (ephemeral namespace) refs.
	if out, err := e.gitErr(work, "push", "origin", "HEAD:refs/namespaces/x/refs/heads/sneaky"); err == nil {
		t.Fatalf("normal-view push into refs/namespaces succeeded:\n%s", out)
	}
	if out := e.git(work, "ls-remote", e.remote("funny", "+ephemeral")); strings.Contains(out, "sneaky") {
		t.Fatalf("hidden ref created from the normal view:\n%s", out)
	}
	if by := e.srv.GitHTTP.FastPushStats().FellBackBy; by["ref_name"] < 4 {
		t.Fatalf("funny/hidden refs should fall back on ref_name: %v", by)
	}
}
