package e2e

import (
	"net/http"
	"testing"
)

// seedDiverged pushes: main has base + main-side commit; feature branches
// from base with its own commit touching a different file.
func seedDiverged(t *testing.T, e *env, conflicting bool) (work string) {
	work = e.seedRepo("demo")
	e.git(work, "checkout", "-qb", "feature")
	if conflicting {
		writeFile(t, work, "README.md", "feature version\n")
	} else {
		writeFile(t, work, "feature.txt", "feature\n")
	}
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "feature work")
	e.git(work, "push", "-q", "origin", "feature")

	e.git(work, "checkout", "-q", "main")
	if conflicting {
		writeFile(t, work, "README.md", "main version\n")
	} else {
		writeFile(t, work, "main.txt", "main\n")
	}
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "main work")
	e.git(work, "push", "-q", "origin", "main")
	return work
}

func TestMergeFastForward(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")
	e.git(work, "checkout", "-qb", "feature")
	writeFile(t, work, "f.txt", "f\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "feature")
	e.git(work, "push", "-q", "origin", "feature")

	out := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature", "strategy": "ff-preferred",
	}, http.StatusOK)
	if out["status"] != "merged" || out["fast_forward"] != true {
		t.Fatalf("ff merge: %v", out)
	}
	feature := e.mustAPI("GET", "/api/repos/demo/branches/feature", nil, http.StatusOK)
	main := e.mustAPI("GET", "/api/repos/demo/branches/main", nil, http.StatusOK)
	if main["sha"] != feature["sha"] {
		t.Fatalf("main not fast-forwarded: %v vs %v", main, feature)
	}

	// Merging again → up to date.
	again := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature",
	}, http.StatusOK)
	if again["status"] != "up_to_date" {
		t.Fatalf("expected up_to_date: %v", again)
	}
}

func TestMergeCommitAndSquash(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	seedDiverged(t, e, false)

	// Preview first: mergeable, nothing committed.
	preview := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature", "preview": true,
	}, http.StatusOK)
	if preview["status"] != "mergeable" {
		t.Fatalf("preview: %v", preview)
	}
	_, before := e.apiList("GET", "/api/repos/demo/commits?ref=main")

	// Real merge commit: two parents.
	merged := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature", "message": "Merge feature",
	}, http.StatusOK)
	if merged["status"] != "merged" || merged["fast_forward"] == true {
		t.Fatalf("merge: %v", merged)
	}
	commit := e.mustAPI("GET", "/api/repos/demo/commits/"+merged["sha"].(string), nil, http.StatusOK)
	if len(commit["parents"].([]any)) != 2 {
		t.Fatalf("merge commit parents: %v", commit)
	}
	_, after := e.apiList("GET", "/api/repos/demo/commits?ref=main")
	if len(after) != len(before)+2 { // merge commit + feature commit now reachable
		t.Fatalf("history after merge: %d → %d", len(before), len(after))
	}

	// Squash the same feature into a fresh branch: single parent.
	e.mustAPI("POST", "/api/repos/demo/branches", map[string]any{"name": "sq", "from": "main"}, http.StatusCreated)
	e.git(seedFeature2(t, e), "push", "-q", "origin", "feature2")
	squashed := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "sq", "head": "feature2", "strategy": "squash", "message": "Squash feature2",
	}, http.StatusOK)
	sq := e.mustAPI("GET", "/api/repos/demo/commits/"+squashed["sha"].(string), nil, http.StatusOK)
	if len(sq["parents"].([]any)) != 1 {
		t.Fatalf("squash parents: %v", sq)
	}
	e.assertRepoIntegrity("demo")
}

// seedFeature2 adds another divergent branch to the demo repo.
func seedFeature2(t *testing.T, e *env) string {
	work := t.TempDir()
	e.git(e.dir, "clone", e.remote("demo", ""), work)
	e.git(work, "checkout", "-qb", "feature2")
	writeFile(t, work, "f2.txt", "f2\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "feature2 work")
	return work
}

func TestMergeConflictsReported(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	seedDiverged(t, e, true)

	// Preview reports the conflict without touching anything.
	preview := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature", "preview": true,
	}, http.StatusOK)
	if preview["status"] != "conflicts" {
		t.Fatalf("preview: %v", preview)
	}

	out := e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature",
	}, http.StatusConflict)
	files := out["conflicts"].([]any)
	if len(files) != 1 || files[0] != "README.md" {
		t.Fatalf("conflict files: %v", out)
	}

	// ff-only on diverged branches → 409.
	e.mustAPI("POST", "/api/repos/demo/merge", map[string]any{
		"base": "main", "head": "feature", "strategy": "ff-only",
	}, http.StatusConflict)
}
