package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/config"
)

func TestCompactionConsolidatesPacks(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")

	// Several pushes → several per-push packs.
	for i := 0; i < 5; i++ {
		writeFile(t, work, "f.txt", strings.Repeat("x", i+1)+"\n")
		e.git(work, "add", "-A")
		e.git(work, "commit", "-qm", "c")
		e.git(work, "push", "-q")
	}
	// An ephemeral branch that must survive compaction.
	e.git(work, "checkout", "-qb", "attempt")
	writeFile(t, work, "eph.txt", "ephemeral data\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "eph")
	e.git(work, "push", "-q", e.remote("demo", "+ephemeral"), "attempt")

	// Plant an orphan pack blob (a rejected push's debris), aged past grace.
	packDir := filepath.Join(e.dir, "server", "packs", "demo")
	orphan := filepath.Join(packDir, "pack-"+strings.Repeat("0", 40)+".pack")
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(orphan, old, old)

	before, _ := e.srv.DB.ListPacks(t.Context(), "demo")
	if len(before) < 6 {
		t.Fatalf("expected many per-push packs, got %d", len(before))
	}

	out := e.mustAPI("POST", "/api/repos/demo/maintenance", nil, http.StatusOK)
	if out["packs_after"].(float64) != 1 {
		t.Fatalf("compact result: %v", out)
	}
	if out["orphans_swept"].(float64) < 1 {
		t.Fatalf("orphan not swept: %v", out)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan blob still in store")
	}

	after, _ := e.srv.DB.ListPacks(t.Context(), "demo")
	if len(after) != 1 || after[0].Source != "gc" {
		t.Fatalf("pack list after compact: %+v", after)
	}
	// Store holds exactly one .pack (+idx, optional .bitmap, meta/ aux).
	entries, _ := os.ReadDir(packDir)
	packs, idxs := 0, 0
	for _, en := range entries {
		if strings.HasSuffix(en.Name(), ".pack") {
			packs++
		}
		if strings.HasSuffix(en.Name(), ".idx") {
			idxs++
		}
	}
	if packs != 1 || idxs != 1 {
		t.Fatalf("store should hold one pack+idx, has: %v", entries)
	}

	// Everything still clones — from a wiped cache, so purely store+DB.
	if err := os.RemoveAll(filepath.Join(e.dir, "server", "cache")); err != nil {
		t.Fatal(err)
	}
	clone := filepath.Join(e.dir, "after")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	if got := e.git(clone, "log", "--format=%s", "-1"); !strings.Contains(got, "c") {
		t.Fatalf("history after compact: %q", got)
	}
	eph := filepath.Join(e.dir, "after-eph")
	e.git(e.dir, "clone", e.remote("demo", "+ephemeral"), eph)
	if got := e.git(eph, "show", "origin/attempt:eph.txt"); got != "ephemeral data\n" {
		t.Fatalf("ephemeral lost in compaction: %q", got)
	}
	e.assertRepoIntegrity("demo")
}

func TestCompactionDropsUnreachableHistory(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	work := e.seedRepo("demo")

	// Rewrite history: the old commit becomes unreachable.
	writeFile(t, work, "big.txt", strings.Repeat("z", 4096))
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "heavy")
	e.git(work, "push", "-q")
	e.git(work, "reset", "-q", "--hard", "HEAD~1")
	writeFile(t, work, "small.txt", "s\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "rewritten")
	e.git(work, "push", "-qf")

	e.mustAPI("POST", "/api/repos/demo/maintenance", nil, http.StatusOK)

	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("demo", ""), clone)
	log := e.git(clone, "log", "--format=%s")
	if strings.Contains(log, "heavy") {
		t.Fatalf("unreachable commit survived: %q", log)
	}
	if !strings.Contains(log, "rewritten") {
		t.Fatalf("reachable history lost: %q", log)
	}
	e.assertRepoIntegrity("demo")
}

func TestBackgroundCompactionWorker(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) {
		cfg.MaintainMinPacks = 2
		cfg.MaintainInterval = 200 * time.Millisecond
	})
	work := e.seedRepo("demo")
	for i := 0; i < 4; i++ {
		writeFile(t, work, "f.txt", strings.Repeat("y", i+1)+"\n")
		e.git(work, "add", "-A")
		e.git(work, "commit", "-qm", "c")
		e.git(work, "push", "-q")
	}
	waitFor(t, "background compaction", 15*time.Second, func() bool {
		packs, _ := e.srv.DB.ListPacks(t.Context(), "demo")
		return len(packs) == 1
	})
	e.assertRepoIntegrity("demo")
}
