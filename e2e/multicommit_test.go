package e2e

import (
	"net/http"
	"testing"
)

// A fresh repo lists its root as an empty dir (not 404), and a multi-file
// commit lands several changes atomically with a working expected_head CAS.
func TestMultiFileCommit(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("multi")

	// Empty repo: root contents is a browsable empty directory, not 404.
	root := e.mustAPI("GET", "/api/repos/multi/contents", nil, http.StatusOK)
	if root["type"] != "dir" {
		t.Fatalf("empty repo root should be a dir: %v", root)
	}

	// One commit, three files.
	out := e.mustAPI("POST", "/api/repos/multi/commits", map[string]any{
		"message": "seed",
		"changes": []map[string]any{
			{"path": "a.txt", "content": b64("A\n")},
			{"path": "dir/b.txt", "content": b64("B\n")},
			{"path": "dir/c.txt", "content": b64("C\n")},
		},
	}, http.StatusCreated)
	commit, _ := out["commit"].(map[string]any)
	head, _ := commit["sha"].(string)
	if head == "" {
		t.Fatalf("no commit sha: %v", out)
	}

	// All three files are present.
	for path, want := range map[string]string{"a.txt": "A\n", "dir/b.txt": "B\n", "dir/c.txt": "C\n"} {
		got := e.mustAPI("GET", "/api/repos/multi/contents/"+path, nil, http.StatusOK)
		if got["content"] != b64(want) {
			t.Fatalf("%s = %v, want %q", path, got["content"], want)
		}
	}

	// A stale expected_head is rejected.
	e.mustAPI("POST", "/api/repos/multi/commits", map[string]any{
		"message":       "stale",
		"expected_head": "0000000000000000000000000000000000000000",
		"changes":       []map[string]any{{"path": "a.txt", "content": b64("A2\n")}},
	}, http.StatusConflict)

	// The current head passes, and can add + delete in one commit.
	e.mustAPI("POST", "/api/repos/multi/commits", map[string]any{
		"message":       "edit",
		"expected_head": head,
		"changes": []map[string]any{
			{"path": "a.txt", "content": b64("A2\n")},
			{"path": "dir/b.txt", "delete": true},
		},
	}, http.StatusCreated)
	got := e.mustAPI("GET", "/api/repos/multi/contents/a.txt", nil, http.StatusOK)
	if got["content"] != b64("A2\n") {
		t.Fatalf("a.txt not updated: %v", got)
	}
	e.mustAPI("GET", "/api/repos/multi/contents/dir/b.txt", nil, http.StatusNotFound)
}
