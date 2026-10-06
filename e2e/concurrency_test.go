package e2e

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/config"
)

// Same-repo push storm with the post-push maintenance nudge firing mid-way:
// the Cursor lesson (lock the ref transaction, never the transfer) plus D's
// staged tee and the internal maintenance lifecycle, all colliding on one
// repo. Regression guard for convoy/deadlock bugs.
func TestSameRepoParallelPushStorm(t *testing.T) {
	e := startServerWith(t, func(c *config.Config) {
		c.MaintainMinPacks = 5 // nudge kicks in mid-storm
	})
	e.seedRepo("demo")

	const workers, each = 6, 4
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				work := filepath.Join(e.dir, fmt.Sprintf("storm-%d-%d", w, i))
				if _, err := e.gitErr(e.dir, "init", "-q", work); err != nil {
					errs <- err
					return
				}
				writeFile(t, work, "f.txt", fmt.Sprintf("w%d i%d\n", w, i))
				for _, args := range [][]string{
					{"add", "."}, {"commit", "-qm", "x"},
					{"push", "-q", e.remote("demo", ""), fmt.Sprintf("HEAD:refs/heads/storm-%d-%d", w, i)},
				} {
					if out, err := e.gitErr(work, args...); err != nil {
						errs <- fmt.Errorf("git %v: %v\n%s", args, err, out)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Let any nudged run finish (its blob deletes race the assertion
	// otherwise), then consolidate once more: packs below the nudge
	// threshold legitimately remain after a storm. Generous timeout:
	// consolidation is CPU-bound and slow on loaded shared CI runners.
	waitFor(t, "maintenance quiescence", 60*time.Second, func() bool {
		return e.srv.Maintain.Idle("demo")
	})
	if _, err := e.srv.Maintain.Run(t.Context(), "demo", 1); err != nil {
		t.Fatalf("final consolidation: %v", err)
	}
	if packs, _ := e.srv.DB.ListPacks(t.Context(), "demo"); len(packs) != 1 {
		t.Fatalf("packs after final consolidation = %d, want 1", len(packs))
	}
	e.assertRepoIntegrity("demo")
}
