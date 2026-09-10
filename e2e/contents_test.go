package e2e

import (
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestContentsCreateReadUpdateDelete(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo") // empty repo: first PUT is the root commit

	out := e.mustAPI("PUT", "/api/repos/demo/contents/docs/hi.txt", map[string]any{
		"message": "add hi", "content": b64("hello api\n"),
	}, http.StatusCreated)
	if out["commit"] == nil {
		t.Fatalf("no commit in response: %v", out)
	}

	// Read back as JSON.
	got := e.mustAPI("GET", "/api/repos/demo/contents/docs/hi.txt", nil, http.StatusOK)
	if got["type"] != "file" || got["content"] != b64("hello api\n") {
		t.Fatalf("contents get: %v", got)
	}
	fileSHA := got["sha"].(string)

	// Directory listing.
	dir := e.mustAPI("GET", "/api/repos/demo/contents/docs", nil, http.StatusOK)
	if dir["type"] != "dir" {
		t.Fatalf("expected dir listing: %v", dir)
	}

	// Update with correct file CAS.
	e.mustAPI("PUT", "/api/repos/demo/contents/docs/hi.txt", map[string]any{
		"message": "update", "content": b64("v2\n"), "sha": fileSHA,
	}, http.StatusCreated)

	// Update with the now-stale sha → 409.
	e.mustAPI("PUT", "/api/repos/demo/contents/docs/hi.txt", map[string]any{
		"message": "stale", "content": b64("v3\n"), "sha": fileSHA,
	}, http.StatusConflict)

	// Identical content is an idempotent no-op: re-PUTting the same bytes
	// succeeds (the desired state is already met) rather than 409-ing.
	e.mustAPI("PUT", "/api/repos/demo/contents/docs/hi.txt", map[string]any{
		"message": "noop", "content": b64("v2\n"),
	}, http.StatusCreated)

	// Delete, then it's gone.
	e.mustAPI("DELETE", "/api/repos/demo/contents/docs/hi.txt", map[string]any{"message": "rm"}, http.StatusCreated)
	e.mustAPI("GET", "/api/repos/demo/contents/docs/hi.txt", nil, http.StatusNotFound)

	// The whole history is visible to a plain git clone.
	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	if got := e.git(clone, "log", "--format=%s"); !contains(got, "add hi", "update", "rm") {
		t.Fatalf("API commits missing from clone: %q", got)
	}
	if _, err := os.Stat(filepath.Join(clone, "docs/hi.txt")); !os.IsNotExist(err) {
		t.Fatal("deleted file still present in clone")
	}
}

func TestContentsRawAndExecutableMode(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo")
	e.mustAPI("PUT", "/api/repos/demo/contents/run.sh", map[string]any{
		"message": "add script", "content": b64("#!/bin/sh\necho hi\n"), "mode": "100755",
	}, http.StatusCreated)

	req, _ := http.NewRequest("GET", e.base+"/api/repos/demo/raw/run.sh", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 64)
	n, _ := resp.Body.Read(buf)
	if string(buf[:n]) != "#!/bin/sh\necho hi\n" {
		t.Fatalf("raw content: %q", buf[:n])
	}

	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	info, err := os.Stat(filepath.Join(clone, "run.sh"))
	if err != nil || info.Mode()&0o111 == 0 {
		t.Fatalf("script not executable in clone: %v %v", info, err)
	}
}

func contains(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// TestRawFileETag304 verifies conditional reads: a raw file carries a
// strong ETag, and a matching If-None-Match returns 304 with no body -
// the cheap-poll path for agents (and the replica-freshness mechanism).
func TestRawFileETag304(t *testing.T) {
	e := startServer(t)
	e.createRepo("demo")
	e.mustAPI("PUT", "/api/repos/demo/contents/f.txt", map[string]any{
		"message": "add", "content": b64("hello\n"),
	}, http.StatusCreated)

	req, _ := http.NewRequest("GET", e.base+"/api/repos/demo/raw/f.txt", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	etag := resp.Header.Get("ETag")
	resp.Body.Close()
	if etag == "" {
		t.Fatal("no ETag on raw file")
	}
	if cc := resp.Header.Get("Cache-Control"); !contains(cc, "immutable") {
		t.Fatalf("expected immutable Cache-Control, got %q", cc)
	}

	req2, _ := http.NewRequest("GET", e.base+"/api/repos/demo/raw/f.txt", nil)
	req2.Header.Set("Authorization", "Bearer "+e.token)
	req2.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match: got %d, want 304", resp2.StatusCode)
	}
}
