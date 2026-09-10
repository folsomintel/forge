package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommitsListGetDiff(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")
	writeFile(t, work, "a.txt", "aaa\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "add a")
	e.git(work, "push", "-q")

	status, commits := e.apiList("GET", "/api/repos/demo/commits?limit=10")
	if status != http.StatusOK || len(commits) != 2 {
		t.Fatalf("list commits: %d %v", status, commits)
	}
	if commits[0]["message"] != "add a" {
		t.Fatalf("newest first expected: %v", commits[0])
	}
	sha := commits[0]["sha"].(string)

	got := e.mustAPI("GET", "/api/repos/demo/commits/"+sha, nil, http.StatusOK)
	if got["message"] != "add a" || len(got["parents"].([]any)) != 1 {
		t.Fatalf("get commit: %v", got)
	}

	req, _ := http.NewRequest("GET", e.base+"/api/repos/demo/commits/"+sha+"/diff", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	patch, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(patch), "+aaa") {
		t.Fatalf("commit diff: %q", patch)
	}
}

func TestCommitFromDiff(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	diff := `diff --git a/README.md b/README.md
--- a/README.md
+++ b/README.md
@@ -1 +1,2 @@
 hello
+from a diff
diff --git a/new.txt b/new.txt
new file mode 100644
--- /dev/null
+++ b/new.txt
@@ -0,0 +1 @@
+created
`
	out := e.mustAPI("POST", "/api/repos/demo/commits/from-diff", map[string]any{
		"branch": "main", "message": "apply patch", "diff": diff,
	}, http.StatusCreated)
	if out["commit"] == nil {
		t.Fatalf("no commit: %v", out)
	}

	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	if got := e.git(clone, "show", "HEAD:new.txt"); got != "created\n" {
		t.Fatalf("new.txt: %q", got)
	}
	if got := e.git(clone, "show", "HEAD:README.md"); got != "hello\nfrom a diff\n" {
		t.Fatalf("README.md: %q", got)
	}

	// A diff against content that no longer matches → 422.
	e.mustAPI("POST", "/api/repos/demo/commits/from-diff", map[string]any{
		"branch": "main", "message": "stale", "diff": diff,
	}, http.StatusUnprocessableEntity)
}

func TestCompareDiff(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")
	e.git(work, "checkout", "-qb", "feature")
	writeFile(t, work, "feat.txt", "feature work\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "feature commit")
	e.git(work, "push", "-q", "origin", "feature")

	req, _ := http.NewRequest("GET", e.base+"/api/repos/demo/diff?base=main&head=feature", nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	patch, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(patch), "+feature work") {
		t.Fatalf("compare diff: %q", patch)
	}
}

// TestCommitsPaginationCursor: paging with limit + X-Next-Cursor walks the
// entire history exactly once (no duplicates, no gaps).
func TestCommitsPaginationCursor(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("paged")
	const n = 7
	for i := 0; i < n; i++ {
		writeFile(t, work, "f.txt", strings.Repeat("x", i+1)+"\n")
		e.git(work, "add", "-A")
		e.git(work, "commit", "-qm", "c")
	}
	e.git(work, "push", "-q")

	// Actual history length (seedRepo adds an initial commit on top of ours).
	_, all := e.apiList("GET", "/api/repos/paged/commits?limit=100")
	total := len(all)

	seen := map[string]bool{}
	after := ""
	pages := 0
	for {
		url := "/api/repos/paged/commits?limit=3"
		if after != "" {
			url += "&after=" + after
		}
		req, _ := http.NewRequest("GET", e.base+url, nil)
		req.Header.Set("Authorization", "Bearer "+e.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var commits []map[string]any
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		decodeJSON(t, body, &commits)
		for _, c := range commits {
			sha := c["sha"].(string)
			if seen[sha] {
				t.Fatalf("duplicate commit across pages: %s", sha)
			}
			seen[sha] = true
		}
		after = resp.Header.Get("X-Next-Cursor")
		pages++
		if after == "" {
			break
		}
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("walked %d commits, want %d (gap or overlap)", len(seen), total)
	}
}

func decodeJSON(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decode: %v (%s)", err, data)
	}
}
