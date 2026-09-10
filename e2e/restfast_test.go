package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fork-free commit walk must produce git log's exact order, including
// across merges (committer-date traversal).
func TestCommitsOrderMatchesGitLogWithMerge(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("order")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("order", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")

	os.WriteFile(filepath.Join(work, "a.txt"), []byte("a\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "checkout", "-q", "-b", "feat")
	os.WriteFile(filepath.Join(work, "b.txt"), []byte("b\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "feat 1")
	e.git(work, "checkout", "-q", "main")
	os.WriteFile(filepath.Join(work, "c.txt"), []byte("c\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "main 1")
	e.git(work, "merge", "-q", "--no-ff", "-m", "merge feat", "feat")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	want := strings.Fields(e.git(work, "log", "--format=%H", "main"))

	var got []struct {
		SHA string `json:"sha"`
	}
	req, _ := http.NewRequest("GET", e.base+"/api/repos/order/commits?limit=50", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("API returned %d commits, git log has %d", len(got), len(want))
	}
	for i := range want {
		if got[i].SHA != want[i] {
			t.Fatalf("order diverges at %d: api=%s git=%s", i, got[i].SHA, want[i])
		}
	}
}

// Conditional GETs: a poller's repeat request costs a 304; a write moves
// the repo's change token and the next GET is a fresh 200.
func TestConditionalGet304(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("etag")
	e.mustAPI("PUT", "/api/repos/etag/contents/f.txt", map[string]any{
		"message": "seed", "content": b64("v1\n"),
	}, http.StatusCreated)

	get := func(inm string) (*http.Response, string) {
		req, _ := http.NewRequest("GET", e.base+"/api/repos/etag/contents/f.txt", nil)
		req.Header.Set("Authorization", "Bearer "+e.token)
		if inm != "" {
			req.Header.Set("If-None-Match", inm)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res, string(body)
	}

	res1, body1 := get("")
	if res1.StatusCode != 200 || res1.Header.Get("ETag") == "" {
		t.Fatalf("first GET: status %d etag %q", res1.StatusCode, res1.Header.Get("ETag"))
	}
	etag := res1.Header.Get("ETag")

	res2, _ := get(etag)
	if res2.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional GET: status %d, want 304", res2.StatusCode)
	}

	// A write invalidates: same If-None-Match now yields fresh content.
	e.mustAPI("PUT", "/api/repos/etag/contents/f.txt", map[string]any{
		"message": "bump", "content": b64("v2\n"),
	}, http.StatusCreated)
	res3, body3 := get(etag)
	if res3.StatusCode != 200 {
		t.Fatalf("post-write GET: status %d, want 200", res3.StatusCode)
	}
	if res3.Header.Get("ETag") == etag {
		t.Fatal("ETag did not change after a write")
	}
	if body3 == body1 || !strings.Contains(body3, b64("v2\n")) {
		t.Fatalf("stale content after write: %q", body3)
	}
}
