package e2e

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Offline import: bundle URL in, indexed out-of-band, refs land atomically.
func TestOfflineBundleImport(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("origin-repo")
	writeFile(t, work, "big.txt", strings.Repeat("data\n", 2000))
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "bulk")
	e.git(work, "tag", "v1")
	e.git(work, "push", "-q", "--tags", "origin", "main")

	// A real bundle, served over HTTP like a migration source would be.
	bundlePath := filepath.Join(e.dir, "migrate.bundle")
	e.git(work, "bundle", "create", bundlePath, "--all")
	fs := httptest.NewServer(http.FileServer(http.Dir(e.dir)))
	defer fs.Close()

	e.createRepo("imported")
	out := e.mustAPI("POST", "/api/repos/imported/import", map[string]any{
		"url": fs.URL + "/migrate.bundle",
	}, http.StatusAccepted)
	if out["status"] != "running" {
		t.Fatalf("import kickoff: %v", out)
	}
	waitFor(t, "import completion", 30*time.Second, func() bool {
		st := e.mustAPI("GET", "/api/repos/imported/import", nil, http.StatusOK)
		if st["status"] == "error" {
			t.Fatalf("import errored: %v", st)
		}
		return st["status"] == "done"
	})

	clone := filepath.Join(e.dir, "imported-clone")
	e.git(e.dir, "clone", e.remote("imported", ""), clone)
	if got := e.git(clone, "show", "HEAD:big.txt"); len(got) != 5*2000 {
		t.Fatalf("imported content size: %d", len(got))
	}
	if tags := e.git(clone, "tag", "-l"); !strings.Contains(tags, "v1") {
		t.Fatalf("imported tags: %q", tags)
	}

	// Re-import into a repo whose refs exist -> conflict, atomically nothing.
	e.mustAPI("POST", "/api/repos/imported/import", map[string]any{
		"url": fs.URL + "/migrate.bundle",
	}, http.StatusAccepted)
	waitFor(t, "conflicting import to fail", 30*time.Second, func() bool {
		st := e.mustAPI("GET", "/api/repos/imported/import", nil, http.StatusOK)
		return st["status"] == "error" && strings.Contains(st["error"].(string), "existing refs")
	})
	e.assertRepoIntegrity("imported")
}
