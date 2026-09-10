package e2e

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
)

// Bundle-uri: compaction publishes a clone bundle; the repo advertises a
// signed capability URL; opted-in clients bootstrap from it.
func TestBundleURICloneOffload(t *testing.T) {
	t.Parallel()
	var base string
	e := startServerWith(t, func(cfg *config.Config) { cfg.PublicURL = "PLACEHOLDER" })
	base = e.base
	_ = base
	// PublicURL had to be set before Build; re-point manager (test-only).
	e.srv.Maintain.PublicURL = e.base

	work := e.seedRepo("demo")
	writeFile(t, work, "b.txt", "bundle me\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "second")
	e.git(work, "push", "-q")

	e.mustAPI("POST", "/api/repos/demo/maintenance", nil, http.StatusOK)

	// The advertisement config exists and carries a signed URI.
	conf, err := os.ReadFile(filepath.Join(e.dir, "server", "cache", "demo.git", "bundles.conf"))
	if err != nil {
		t.Fatalf("bundles.conf missing after compact: %v", err)
	}
	line := ""
	for _, l := range strings.Split(string(conf), "\n") {
		if strings.Contains(l, "uri = ") {
			line = strings.TrimSpace(strings.SplitN(l, "= ", 2)[1])
		}
	}
	if line == "" {
		t.Fatalf("no uri in bundles.conf: %s", conf)
	}

	// The capability URL serves a real bundle without bearer auth.
	resp, err := http.Get(line)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	bundleFile := filepath.Join(e.dir, "dl.bundle")
	bf, _ := os.Create(bundleFile)
	io.Copy(bf, resp.Body)
	bf.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("bundle fetch: %d", resp.StatusCode)
	}

	// Tampered signature rejected.
	if resp2, err := http.Get(strings.Replace(line, "sig=", "sig=00", 1)); err == nil {
		resp2.Body.Close()
		if resp2.StatusCode != http.StatusForbidden {
			t.Fatalf("tampered sig accepted: %d", resp2.StatusCode)
		}
	}

	// An opted-in client clones successfully with the advertisement live.
	clone := filepath.Join(e.dir, "via-bundle")
	e.git(e.dir, "clone", "-c", "transfer.bundleURI=true", e.remote("demo", ""), clone)
	if got := e.git(clone, "log", "--format=%s", "-1"); !strings.Contains(got, "second") {
		t.Fatalf("bundle-assisted clone content: %q", got)
	}
	// The synthesized bundle must be a valid v2 bundle in git's own eyes
	// (verify requires a repo context).
	e.git(clone, "bundle", "verify", bundleFile)
}

// Bundle chains: a second push + maintenance publishes an INCREMENTAL bundle
// on top of the full, both advertised in creationToken order with mode=all.
func TestBundleChainIncremental(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.PublicURL = "PLACEHOLDER" })
	e.srv.Maintain.PublicURL = e.base

	work := e.seedRepo("chain")
	writeFile(t, work, "a.txt", "one\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "first")
	e.git(work, "push", "-q")
	e.mustAPI("POST", "/api/repos/chain/maintenance", nil, http.StatusOK)

	confPath := filepath.Join(e.dir, "server", "cache", "chain.git", "bundles.conf")
	conf1, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("bundles.conf missing after first compact: %v", err)
	}
	if !strings.Contains(string(conf1), "mode = all") {
		t.Fatalf("chain must advertise mode=all:\n%s", conf1)
	}
	if n := strings.Count(string(conf1), "creationToken = "); n != 1 {
		t.Fatalf("want 1 bundle after first push, got %d:\n%s", n, conf1)
	}

	// A second push, then maintenance: the tips moved, so an incremental is cut.
	writeFile(t, work, "b.txt", "two\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "second")
	e.git(work, "push", "-q")
	e.mustAPI("POST", "/api/repos/chain/maintenance", nil, http.StatusOK)

	conf2, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("bundles.conf missing after second compact: %v", err)
	}
	if n := strings.Count(string(conf2), "creationToken = "); n != 2 {
		t.Fatalf("want 2 chain members after second push, got %d:\n%s", n, conf2)
	}

	// Download every advertised member; each must be a valid git bundle
	// (the incremental carries prerequisite lines).
	var uris []string
	for _, l := range strings.Split(string(conf2), "\n") {
		if uri, ok := strings.CutPrefix(strings.TrimSpace(l), "uri = "); ok {
			uris = append(uris, uri)
		}
	}
	if len(uris) != 2 {
		t.Fatalf("want 2 uris, got %d", len(uris))
	}
	for i, u := range uris {
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		bf := filepath.Join(e.dir, "chain-"+string(rune('0'+i))+".bundle")
		f, _ := os.Create(bf)
		io.Copy(f, resp.Body)
		f.Close()
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("member %d fetch: %d", i, resp.StatusCode)
		}
		e.git(work, "bundle", "verify", bf)
	}

	// A bundle-uri clone assembles the whole chain and lands both commits.
	clone := filepath.Join(e.dir, "via-chain")
	e.git(e.dir, "clone", "-c", "transfer.bundleURI=true", e.remote("chain", ""), clone)
	if got := e.git(clone, "log", "--format=%s"); !strings.Contains(got, "first") || !strings.Contains(got, "second") {
		t.Fatalf("chain-assisted clone missing commits: %q", got)
	}
}
