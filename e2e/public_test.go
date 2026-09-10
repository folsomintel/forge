package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Public repos: anonymous clone + anonymous read API; writes and private
// repos stay locked. The read API doubles as the public explore surface.
func TestPublicRepos(t *testing.T) {
	e := startServer(t)
	e.seedRepo("open")
	e.seedRepo("closed")
	e.mustAPI("PATCH", "/api/repos/open", map[string]any{"public": true}, http.StatusOK)

	host := strings.TrimPrefix(e.base, "http://")

	// Anonymous clone of the public repo works; private repo refuses.
	if out, err := e.gitErr(e.dir, "clone", "-q", "http://"+host+"/open.git", e.dir+"/anon-open"); err != nil {
		t.Fatalf("anon clone of public repo failed: %v\n%s", err, out)
	}
	if out, err := e.gitErr(e.dir, "clone", "-q", "http://"+host+"/closed.git", e.dir+"/anon-closed"); err == nil {
		probe, _ := http.Get(e.base + "/closed.git/info/refs?service=git-upload-pack")
		status := 0
		if probe != nil {
			status = probe.StatusCode
			probe.Body.Close()
		}
		repo, rerr := e.srv.DB.GetRepo(t.Context(), "closed")
		t.Fatalf("anon clone of private repo succeeded (probe info/refs=%d, repo=%+v err=%v):\n%s", status, repo, rerr, out)
	}

	// Anonymous read API on the public repo; 401 on the private one.
	resp, err := http.Get(e.base + "/api/repos/open/contents/README.md")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("anon contents on public repo: %v %v", resp.StatusCode, err)
	}
	var c struct {
		Content string `json:"content"`
	}
	json.NewDecoder(resp.Body).Decode(&c)
	resp.Body.Close()
	if c.Content == "" {
		t.Fatal("anon contents empty")
	}
	resp, _ = http.Get(e.base + "/api/repos/closed/contents/README.md")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("anon contents on private repo = %d, want 401", resp.StatusCode)
	}

	// Anonymous push to the public repo must fail.
	work := e.dir + "/anon-open"
	writeFile(t, work, "evil.txt", "nope\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "evil")
	if _, err := e.gitErr(work, "push", "-q", "origin", "main"); err == nil {
		t.Fatal("anonymous push to public repo succeeded")
	}

	// Listing repos anonymously stays locked (no id param, org-wide data).
	resp, _ = http.Get(e.base + "/api/repos")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("anon repo list = %d, want 401", resp.StatusCode)
	}
}
