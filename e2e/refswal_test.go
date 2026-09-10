package e2e

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/server"
)

// E: the bucket is the truth for refs. Every ref transaction lands as a
// conditional-PUT WAL entry before the index; a fresh machine with an
// empty DB pointed at the same store resurrects everything.
func TestRefsWALBucketResurrection(t *testing.T) {
	e := startServer(t)
	work := e.seedRepo("demo")

	// A few more moves: extra branch, a tag, one more commit on main.
	writeFile(t, work, "x.txt", "x\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "second")
	e.git(work, "push", "-q", "origin", "main")
	e.git(work, "push", "-q", "origin", "main:refs/heads/side")
	e.git(work, "tag", "v1")
	e.git(work, "push", "-q", "origin", "v1")

	// Webhook config and an LFS index row: both must survive resurrection
	// (webhooks mirror to the bucket; LFS rebuilds from lfs/ blobs).
	e.mustAPI("POST", "/api/repos/demo/webhooks",
		map[string]any{"url": "https://example.com/hook", "events": []string{"push"}},
		http.StatusCreated)
	lfsOID := strings.Repeat("ab", 32)
	if err := e.srv.Blobs.Put(t.Context(), "demo", "lfs/"+lfsOID,
		strings.NewReader("big file bytes")); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.DB.AddLFSObject(t.Context(), "demo",
		repodb.LFSObject{OID: lfsOID, Size: 14}); err != nil {
		t.Fatal(err)
	}

	before := e.git(work, "ls-remote", e.remote("demo", ""))

	// WAL entries (or a snapshot) must exist in the store.
	walDir := filepath.Join(e.dir, "server", "packs", "demo", "refs")
	if _, err := os.Stat(walDir); err != nil {
		t.Fatalf("no refs/ WAL data in store: %v", err)
	}

	// Disaster: brand-new data dir + empty DB, same blob store. Recovery
	// runs inside server.Build.
	dir2 := t.TempDir()
	cfg2 := config.Config{
		DataDir:             filepath.Join(dir2, "server"),
		DBPath:              filepath.Join(dir2, "server", "forge.db"),
		StoreKind:           "local",
		StorePath:           filepath.Join(e.dir, "server", "packs"), // same store
		SelfPath:            forgedBin,
		WebhookAllowPrivate: true,
	}
	srv2, err := server.Build(cfg2)
	if err != nil {
		t.Fatalf("build recovery server: %v", err)
	}
	defer srv2.Close()

	// No key reseed: client keys mirror to _forge/keys.json and recovery
	// restores them, so the pre-disaster token keeps working.

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv2.Mux}
	go hs.Serve(ln)
	defer hs.Close()
	remote2 := "http://t:" + e.token + "@" + ln.Addr().String() + "/demo.git"

	after := e.git(work, "ls-remote", remote2)
	norm := func(s string) string {
		var keep []string
		for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
			if !strings.Contains(l, "HEAD") {
				keep = append(keep, l)
			}
		}
		return strings.Join(keep, "\n")
	}
	if norm(before) != norm(after) {
		t.Fatalf("resurrected refs differ:\nbefore:\n%s\nafter:\n%s", before, after)
	}

	// And the data actually serves: full clone + fsck from the ashes.
	cl := filepath.Join(dir2, "clone")
	e.git(e.dir, "clone", "-q", remote2, cl)
	e.git(cl, "fsck", "--strict", "--no-dangling")

	// Webhook config and LFS index came back too.
	hooks, err := srv2.DB.ListWebhooks(context.Background(), "demo")
	if err != nil || len(hooks) != 1 || hooks[0].URL != "https://example.com/hook" {
		t.Fatalf("webhooks not resurrected: %v %v", hooks, err)
	}
	if hooks[0].Secret == "" {
		t.Fatal("webhook secret lost in resurrection")
	}
	obj, err := srv2.DB.GetLFSObject(context.Background(), "demo", lfsOID)
	if err != nil || obj.Size != 14 {
		t.Fatalf("lfs index not resurrected: %v %v", obj, err)
	}
}

// Losing the conditional PUT must surface as a CAS conflict, not a hang or
// a corrupt sequence: simulate the race by pre-planting the next WAL seq.
func TestRefsWALSequenceContention(t *testing.T) {
	e := startServer(t)
	work := e.seedRepo("demo")

	// Plant a foreign entry at the next sequence (2): as if another writer
	// (split-brain) advanced the WAL. Content is a no-op update batch.
	foreign := filepath.Join(e.dir, "server", "packs", "demo", "refs", "wal",
		"0000000000000002.json")
	if err := os.MkdirAll(filepath.Dir(foreign), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreign, []byte(`{"seq":2,"updates":[],"ts":0}`), 0o644); err != nil {
		t.Fatal(err)
	}

	// The next push re-syncs past the foreign entry and still lands.
	writeFile(t, work, "y.txt", "y\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "after-contention")
	e.git(work, "push", "-q", "origin", "main")
	e.assertRepoIntegrity("demo")
}
