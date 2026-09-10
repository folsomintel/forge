package e2e

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/server"
)

// Remote placement (walgit remote reader): a read replica serves a repo's
// object reads straight from the bucket in blocks, keeping only the pack .idx
// locally - so it serves a repo whose pack data it never downloaded. The
// primary holds full packs and does the maintenance.
func TestRemotePlacementReplicaServesFromBucket(t *testing.T) {
	// Primary: push a >1 MiB (incompressible) file so the gc pack clears the
	// remote-serving floor, then consolidate. BuildHistoryPack publishes the
	// blobless commits+trees pack for the replica to serve history locally.
	e := startServerWith(t, func(cfg *config.Config) { cfg.BuildHistoryPack = true })
	work := e.seedRepo("big")
	big := make([]byte, 2<<20)
	x := uint32(12345)
	for i := range big {
		x = x*1664525 + 1013904223
		big[i] = byte(x >> 24)
	}
	if err := os.WriteFile(filepath.Join(work, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "big blob")
	e.git(work, "push", "-q", "origin", "main")
	headSHA := strings.Fields(e.git(work, "rev-parse", "HEAD"))[0]
	e.mustAPI("POST", "/api/repos/big/maintenance", nil, http.StatusOK)

	// The primary published a blobless history pack to the bucket.
	if rc, err := e.srv.Blobs.Get(t.Context(), "big", "meta/history.pack"); err != nil {
		t.Fatalf("history pack not published: %v", err)
	} else {
		rc.Close()
	}

	// Replica: fresh data dir, SAME bucket, remote placement on.
	dir2 := t.TempDir()
	cfg2 := config.Config{
		DataDir:              filepath.Join(dir2, "server"),
		DBPath:               filepath.Join(dir2, "server", "forge.db"),
		StoreKind:            "local",
		StorePath:            filepath.Join(e.dir, "server", "packs"), // same store
		SelfPath:             forgedBin,
		Replica:              true,
		RemotePlacementBytes: 1,       // any repo with packs is "big"
		BlockCacheBytes:      8 << 20, // small block LRU
		GoFetch:              true,    // remote placement serves clones via gofetch
		WebhookAllowPrivate:  true,
	}
	srv2, err := server.Build(cfg2)
	if err != nil {
		t.Fatalf("build replica: %v", err)
	}
	defer srv2.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv2.Mux}
	go hs.Serve(ln)
	defer hs.Close()
	base := "http://" + ln.Addr().String()

	// Read the big file THROUGH the replica's API. This materializes the repo
	// in remote mode (idx-only) and serves the blob from the bucket via
	// packstore.
	got := replicaContents(t, base, e.token, "big", "big.bin")
	if !equalBytes(got, big) {
		t.Fatalf("replica served wrong content: got %d bytes, want %d", len(got), len(big))
	}

	// The replica must NOT have the gc .pack on local disk - only its .idx and
	// the .remote size sidecar.
	packDir := filepath.Join(dir2, "server", "cache", "big.git", "objects", "pack")
	ents, _ := os.ReadDir(packDir)
	var hasIdx, hasSidecar, hasPack bool
	for _, en := range ents {
		switch {
		case strings.HasSuffix(en.Name(), ".pack"):
			hasPack = true
		case strings.HasSuffix(en.Name(), ".idx"):
			hasIdx = true
		case strings.HasSuffix(en.Name(), ".remote"):
			hasSidecar = true
		}
	}
	if hasPack {
		t.Fatalf("replica downloaded the .pack; remote placement not in effect")
	}
	if !hasIdx || !hasSidecar {
		t.Fatalf("replica missing idx/sidecar (idx=%v sidecar=%v)", hasIdx, hasSidecar)
	}

	// A smaller file (commit/tree reads too) also resolves via the remote pack.
	small := replicaContents(t, base, e.token, "big", "README.md")
	if len(small) == 0 {
		t.Fatalf("replica failed to serve small file from remote pack")
	}

	// Phase 3: the blobless history pack is local on the replica, so commits +
	// trees are served without touching the bucket.
	histDir := filepath.Join(dir2, "server", "cache", "big.git", "meta-history")
	if _, err := os.Stat(filepath.Join(histDir, "history.pack")); err != nil {
		t.Fatalf("replica did not hydrate the local history pack: %v", err)
	}
	if _, err := os.Stat(filepath.Join(histDir, "history.idx")); err != nil {
		t.Fatalf("replica missing local history idx: %v", err)
	}
	// A commit read is served (from the local history pack).
	cReq, _ := http.NewRequest("GET", base+"/api/repos/big/commits/"+headSHA, nil)
	cReq.Header.Set("Authorization", "Bearer "+e.token)
	cResp, err := http.DefaultClient.Do(cReq)
	if err != nil {
		t.Fatal(err)
	}
	cResp.Body.Close()
	if cResp.StatusCode != 200 {
		t.Fatalf("replica commit read: HTTP %d", cResp.StatusCode)
	}

	// And a clone from the replica works (gofetch streams the gc pack from the
	// bucket; no local .pack needed).
	clone := filepath.Join(dir2, "cloned")
	remote := "http://t:" + e.token + "@" + ln.Addr().String() + "/big.git"
	e.git(dir2, "clone", "-q", remote, clone)
	cloned, err := os.ReadFile(filepath.Join(clone, "big.bin"))
	if err != nil || !equalBytes(cloned, big) {
		t.Fatalf("clone from replica missing/incorrect big.bin: err=%v", err)
	}
}

func replicaContents(t *testing.T, base, token, repo, path string) []byte {
	t.Helper()
	req, _ := http.NewRequest("GET", base+"/api/repos/"+repo+"/contents/"+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("contents %s: HTTP %d: %s", path, resp.StatusCode, body)
	}
	var out struct {
		Content string `json:"content"`
	}
	json.Unmarshal(body, &out)
	data, err := base64.StdEncoding.DecodeString(out.Content)
	if err != nil {
		t.Fatalf("decode content: %v", err)
	}
	return data
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
