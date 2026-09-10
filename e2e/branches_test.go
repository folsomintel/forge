package e2e

import (
	"net/http"
	"strings"
	"testing"
)

func TestBranchesCRUD(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	main := e.mustAPI("GET", "/api/repos/demo/branches/main", nil, http.StatusOK)
	sha := main["sha"].(string)

	created := e.mustAPI("POST", "/api/repos/demo/branches", map[string]any{
		"name": "feature", "from": "main",
	}, http.StatusCreated)
	if created["sha"] != sha {
		t.Fatalf("branch from main should share tip: %v", created)
	}

	// Duplicate → 409.
	e.mustAPI("POST", "/api/repos/demo/branches", map[string]any{"name": "feature"}, http.StatusConflict)

	status, branches := e.apiList("GET", "/api/repos/demo/branches")
	if status != http.StatusOK || len(branches) != 2 {
		t.Fatalf("list branches: %d %v", status, branches)
	}

	// Default branch is protected from deletion; feature is not.
	e.mustAPI("DELETE", "/api/repos/demo/branches/main", nil, http.StatusUnprocessableEntity)
	e.mustAPI("DELETE", "/api/repos/demo/branches/feature", nil, http.StatusNoContent)
	e.mustAPI("GET", "/api/repos/demo/branches/feature", nil, http.StatusNotFound)
}

func TestGitRefsAPI(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")
	main := e.mustAPI("GET", "/api/repos/demo/branches/main", nil, http.StatusOK)
	sha := main["sha"].(string)

	// Create a tag ref pointing at an existing commit.
	e.mustAPI("POST", "/api/repos/demo/git/refs", map[string]any{
		"ref": "refs/tags/v1", "sha": sha,
	}, http.StatusCreated)

	// Pointing a ref at a hash the repo doesn't contain must fail:
	// reachability, not hash existence, gates access.
	e.mustAPI("POST", "/api/repos/demo/git/refs", map[string]any{
		"ref": "refs/tags/evil", "sha": strings.Repeat("d", 40),
	}, http.StatusUnprocessableEntity)

	// Invalid names rejected.
	e.mustAPI("POST", "/api/repos/demo/git/refs", map[string]any{
		"ref": "refs/heads/bad..name", "sha": sha,
	}, http.StatusBadRequest)

	status, refs := e.apiList("GET", "/api/repos/demo/git/refs?prefix=refs/tags/")
	if status != http.StatusOK || len(refs) != 1 {
		t.Fatalf("list refs: %d %v", status, refs)
	}

	e.mustAPI("DELETE", "/api/repos/demo/git/refs/refs/tags/v1", nil, http.StatusNoContent)
}

func TestGitDataBlobAndTree(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	root := e.mustAPI("GET", "/api/repos/demo/contents", nil, http.StatusOK)
	entries := root["entries"].([]any)
	first := entries[0].(map[string]any)
	blobSHA := first["sha"].(string)

	blob := e.mustAPI("GET", "/api/repos/demo/git/blobs/"+blobSHA, nil, http.StatusOK)
	if blob["content"] != b64("hello\n") {
		t.Fatalf("blob content: %v", blob)
	}

	commit := e.mustAPI("GET", "/api/repos/demo/commits/main", nil, http.StatusOK)
	tree := e.mustAPI("GET", "/api/repos/demo/git/trees/"+commit["tree"].(string)+"?recursive=true", nil, http.StatusOK)
	if len(tree["tree"].([]any)) == 0 {
		t.Fatalf("tree listing empty: %v", tree)
	}
}
