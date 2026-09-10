package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestCloneCommitPushRoundtrip(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")

	// Second commit, push, fresh clone sees both.
	writeFile(t, work, "src/main.go", "package main\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "second")
	e.git(work, "push", "-q")

	clone2 := filepath.Join(e.dir, "clone2")
	e.git(e.dir, "clone", e.remote("demo", ""), clone2)
	if got := e.git(clone2, "log", "--format=%s"); !strings.Contains(got, "second") || !strings.Contains(got, "initial") {
		t.Fatalf("fresh clone missing history: %q", got)
	}
	if b, _ := os.ReadFile(filepath.Join(clone2, "src/main.go")); string(b) != "package main\n" {
		t.Fatalf("file content mismatch: %q", b)
	}
}

func TestCacheWipeRebuildsFromStore(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")
	e.git(work, "tag", "-a", "v1", "-m", "release")
	e.git(work, "push", "-q", "origin", "v1")

	// Nuke the cache: everything must rebuild from pack store + DB.
	if err := os.RemoveAll(filepath.Join(e.dir, "server", "cache")); err != nil {
		t.Fatal(err)
	}
	clone := filepath.Join(e.dir, "rebuilt")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	if got := e.git(clone, "tag", "-l"); !strings.Contains(got, "v1") {
		t.Fatalf("tag lost after cache wipe: %q", got)
	}
	if b, _ := os.ReadFile(filepath.Join(clone, "README.md")); string(b) != "hello\n" {
		t.Fatalf("content lost after cache wipe: %q", b)
	}
	e.assertRepoIntegrity("demo")
}

func TestConcurrentPushesOneWinner(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	// Two clones at the same base commit racing to push main.
	dirs := make([]string, 2)
	for i := range dirs {
		dirs[i] = filepath.Join(e.dir, "racer", string(rune('a'+i)))
		e.git(e.dir, "clone", e.remote("demo", ""), dirs[i])
		writeFile(t, dirs[i], "race.txt", strings.Repeat(string(rune('a'+i)), 4)+"\n")
		e.git(dirs[i], "add", "-A")
		e.git(dirs[i], "commit", "-qm", "race")
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range dirs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = e.gitErr(dirs[i], "push", "-q", "origin", "main")
		}(i)
	}
	wg.Wait()
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("want exactly 1 rejected push, got %d rejected", failed)
	}
}

func TestPartialClone(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")
	clone := filepath.Join(e.dir, "partial")
	e.git(e.dir, "clone", "--filter=blob:none", e.remote("demo", ""), clone)
	if b, _ := os.ReadFile(filepath.Join(clone, "README.md")); string(b) != "hello\n" {
		t.Fatalf("partial clone content: %q", b)
	}
}

func TestShallowClone(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")
	writeFile(t, work, "2.txt", "2\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "second")
	e.git(work, "push", "-q")

	clone := filepath.Join(e.dir, "shallow")
	e.git(e.dir, "clone", "--depth=1", e.remote("demo", ""), clone)
	if got := strings.TrimSpace(e.git(clone, "rev-list", "--count", "HEAD")); got != "1" {
		t.Fatalf("shallow clone depth: %s commits", got)
	}
}

func TestRepoScopedTokenIsolation(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")
	e.seedRepo("other")

	scoped := e.mintToken("git:read git:write", "demo")
	host := strings.TrimPrefix(e.base, "http://")
	okDir := filepath.Join(e.dir, "scoped-ok")
	e.git(e.dir, "clone", "http://t:"+scoped+"@"+host+"/demo.git", okDir)
	if out, err := e.gitErr(e.dir, "clone", "http://t:"+scoped+"@"+host+"/other.git", filepath.Join(e.dir, "scoped-bad")); err == nil {
		t.Fatalf("repo-scoped token cloned another repo: %s", out)
	}
}

func TestUsageEndpoint(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")
	u := e.mustAPI("GET", "/api/usage", nil, 200)
	if u["repos"].(float64) != 1 || u["pack_bytes"].(float64) <= 0 {
		t.Fatalf("usage: %v", u)
	}
	if u["disk_total_bytes"].(float64) <= 0 {
		t.Fatalf("disk stats missing: %v", u)
	}
}
