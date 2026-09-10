package e2e

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// The core agent workflow: push attempts to the ephemeral namespace,
// invisible to normal clones, then promote the winner to a real branch.
func TestEphemeralBranchLifecycle(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")

	// Push an attempt into the ephemeral view.
	e.git(work, "checkout", "-qb", "attempt-1")
	writeFile(t, work, "solution.txt", "attempt one\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "attempt 1")
	e.git(work, "push", "-q", e.remote("demo", "+ephemeral"), "attempt-1")

	// Invisible to a normal clone.
	normal := filepath.Join(e.dir, "normal")
	e.git(e.dir, "clone", e.remote("demo", ""), normal)
	if got := e.git(normal, "branch", "-a"); strings.Contains(got, "attempt-1") {
		t.Fatalf("ephemeral branch leaked into normal clone: %q", got)
	}

	// Visible through the ephemeral view.
	eph := filepath.Join(e.dir, "eph")
	e.git(e.dir, "clone", e.remote("demo", "+ephemeral"), eph)
	if got := e.git(eph, "branch", "-a"); !strings.Contains(got, "attempt-1") {
		t.Fatalf("ephemeral clone missing branch: %q", got)
	}

	// Listed via the API only with ephemeral=true.
	_, normalBranches := e.apiList("GET", "/api/repos/demo/branches")
	for _, b := range normalBranches {
		if b["name"] == "attempt-1" {
			t.Fatalf("ephemeral branch in normal listing: %v", normalBranches)
		}
	}
	_, ephBranches := e.apiList("GET", "/api/repos/demo/branches?ephemeral=true")
	if len(ephBranches) != 1 || ephBranches[0]["name"] != "attempt-1" {
		t.Fatalf("ephemeral listing: %v", ephBranches)
	}

	// Promote: ephemeral → real branch. Pure ref CAS.
	promoted := e.mustAPI("POST", "/api/repos/demo/branches", map[string]any{
		"name": "solution", "from": "attempt-1", "from_ephemeral": true,
	}, http.StatusCreated)
	if promoted["sha"] != ephBranches[0]["sha"] {
		t.Fatalf("promotion changed sha: %v vs %v", promoted, ephBranches[0])
	}

	// The promoted branch is now in a normal clone.
	e.git(normal, "fetch", "-q", "origin")
	if got := e.git(normal, "show", "origin/solution:solution.txt"); got != "attempt one\n" {
		t.Fatalf("promoted content: %q", got)
	}

	// Clean up the ephemeral attempt.
	e.mustAPI("DELETE", "/api/repos/demo/branches/attempt-1?ephemeral=true", nil, http.StatusNoContent)
	_, after := e.apiList("GET", "/api/repos/demo/branches?ephemeral=true")
	if len(after) != 0 {
		t.Fatalf("ephemeral branch not deleted: %v", after)
	}
}

func TestEphemeralAPIWrites(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	// Create an ephemeral branch from main via API, commit to it via API.
	e.mustAPI("POST", "/api/repos/demo/branches", map[string]any{
		"name": "scratch", "from": "main", "ephemeral": true,
	}, http.StatusCreated)
	e.mustAPI("PUT", "/api/repos/demo/contents/notes.txt", map[string]any{
		"message": "scratch note", "content": b64("wip\n"), "branch": "scratch", "ephemeral": true,
	}, http.StatusCreated)

	// Commits visible via API on the ephemeral ref.
	status, commits := e.apiList("GET", "/api/repos/demo/commits?ref=scratch&ephemeral=true")
	if status != http.StatusOK || len(commits) != 2 || commits[0]["message"] != "scratch note" {
		t.Fatalf("ephemeral commits: %d %v", status, commits)
	}

	// Main is untouched.
	_, mainCommits := e.apiList("GET", "/api/repos/demo/commits?ref=main")
	if len(mainCommits) != 1 {
		t.Fatalf("main polluted by ephemeral write: %v", mainCommits)
	}
}
