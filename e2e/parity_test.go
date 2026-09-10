package e2e

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Parity strategy: git itself is the oracle. We drive an identical, seeded
// random operation sequence against forge and against a plain bare repo
// (the reference implementation of "a git remote"), and require identical
// observable state after every push: the full ref advertisement (ls-remote
// --symref) and, at the end, the complete object closure and a strict fsck.
//
// The wire protocol needs no parity testing - forge execs real git for it.
// What this catches is state divergence: materialization bugs, ref CAS
// bugs, pack pipeline losing objects.
func TestStateParityRandomOps(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo")

	oracle := filepath.Join(e.dir, "oracle.git")
	e.git(e.dir, "init", "-q", "--bare", oracle)
	e.git(oracle, "symbolic-ref", "HEAD", "refs/heads/main")

	seed := time.Now().UnixNano()
	if s := os.Getenv("PARITY_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("parity seed: %d (re-run with PARITY_SEED=%d)", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "init", "-q", work)
	e.git(work, "remote", "add", "forge", e.remote("demo", ""))
	e.git(work, "remote", "add", "oracle", oracle)
	e.git(work, "checkout", "-qb", "main")
	writeFile(t, work, "README.md", "seed\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "seed")

	branches := []string{"main"}
	current := "main"
	tagN := 0

	pushBoth := func() {
		t.Helper()
		// Force + prune mirrors the local ref state (incl. deletions) to both.
		refspecs := []string{"--prune", "--force", "refs/heads/*:refs/heads/*", "refs/tags/*:refs/tags/*"}
		e.git(work, append([]string{"push", "-q", "forge"}, refspecs...)...)
		e.git(work, append([]string{"push", "-q", "oracle"}, refspecs...)...)
	}
	assertSameRefs := func(step string) {
		t.Helper()
		f := sortedLines(e.git(work, "ls-remote", "--symref", "forge"))
		o := sortedLines(e.git(work, "ls-remote", "--symref", "oracle"))
		if f != o {
			t.Fatalf("ref advertisement diverged after %s (seed %d)\n--- forge ---\n%s\n--- oracle ---\n%s", step, seed, f, o)
		}
	}
	pushBoth()
	assertSameRefs("seed")

	const ops = 30
	for i := 0; i < ops; i++ {
		op := rng.Intn(6)
		var step string
		switch op {
		case 0, 1: // plain commit (weighted: most common op)
			writeFile(t, work, fmt.Sprintf("f%d.txt", rng.Intn(8)), fmt.Sprintf("content %d\n", rng.Int63()))
			e.git(work, "add", "-A")
			e.git(work, "commit", "-qm", fmt.Sprintf("commit %d", i), "--allow-empty")
			step = "commit"
		case 2: // new branch off current
			name := fmt.Sprintf("b%d", i)
			e.git(work, "checkout", "-qb", name)
			branches = append(branches, name)
			current = name
			step = "branch " + name
		case 3: // switch to a random branch
			current = branches[rng.Intn(len(branches))]
			e.git(work, "checkout", "-q", current)
			step = "switch " + current
		case 4: // annotated tag
			tagN++
			e.git(work, "tag", "-a", fmt.Sprintf("t%d", tagN), "-m", "tag")
			step = "tag"
		case 5: // amend + force-push, or delete a branch
			if len(branches) > 1 && rng.Intn(2) == 0 {
				victim := ""
				for _, b := range branches {
					if b != "main" && b != current {
						victim = b
						break
					}
				}
				if victim != "" {
					e.git(work, "branch", "-qD", victim)
					next := branches[:0]
					for _, b := range branches {
						if b != victim {
							next = append(next, b)
						}
					}
					branches = next
					step = "delete " + victim
					break
				}
			}
			e.git(work, "commit", "-q", "--amend", "-m", fmt.Sprintf("amended %d", i), "--allow-empty")
			step = "amend"
		}
		pushBoth()
		assertSameRefs(fmt.Sprintf("op %d (%s)", i, step))
	}

	// Compact forge, then verify parity still holds: compaction must be
	// invisible to every observable (refs unchanged, closure identical).
	e.mustAPI("POST", "/api/repos/demo/maintenance", nil, 200)
	assertSameRefs("compaction")

	// Final deep check: mirror-clone both sides; object closures must match
	// and both must pass a strict fsck.
	forgeClone := filepath.Join(e.dir, "final-forge.git")
	oracleClone := filepath.Join(e.dir, "final-oracle.git")
	e.git(e.dir, "clone", "-q", "--mirror", e.remote("demo", ""), forgeClone)
	e.git(e.dir, "clone", "-q", "--mirror", oracle, oracleClone)

	fObjs := sortedLines(e.git(forgeClone, "rev-list", "--all", "--objects"))
	oObjs := sortedLines(e.git(oracleClone, "rev-list", "--all", "--objects"))
	if fObjs != oObjs {
		t.Fatalf("object closures diverged (seed %d): forge %d lines vs oracle %d lines",
			seed, strings.Count(fObjs, "\n"), strings.Count(oObjs, "\n"))
	}
	e.git(forgeClone, "fsck", "--strict", "--no-dangling")
	e.assertRepoIntegrity("demo")
}

// sortedLines normalizes multi-line output for comparison.
func sortedLines(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
