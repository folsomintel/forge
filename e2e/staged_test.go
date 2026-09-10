package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Staged tee push: the wire pack uploads to staged/ during the transfer and
// a trailer match turns the ack-path upload into a server-side copy. From
// the outside the contract is: pushes land intact (first push non-thin ->
// copy path; later pushes thin -> fallback), and no staged/ debris survives
// a completed push.
func TestStagedPushLeavesNoDebris(t *testing.T) {
	e := startServer(t)
	work := e.seedRepo("demo") // first push: non-thin, copy path

	// Second push: thin pack against existing objects, fallback path.
	writeFile(t, work, "b.txt", strings.Repeat("beta\n", 2000))
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "two")
	e.git(work, "push", "-q", "origin", "main")

	// Both pushes durable and correct.
	e.assertRepoIntegrity("demo")

	// No staged/ leftovers in the store after completed pushes.
	stagedDir := filepath.Join(e.dir, "server", "packs", "demo", "staged")
	if entries, err := os.ReadDir(stagedDir); err == nil && len(entries) > 0 {
		names := []string{}
		for _, en := range entries {
			names = append(names, en.Name())
		}
		t.Fatalf("staged debris left after pushes: %v", names)
	}
}
