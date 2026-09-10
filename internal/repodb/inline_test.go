package repodb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func inlineOID(i int) string { return fmt.Sprintf("%040d", i+1) }

// Inline packs: refs and data land in one conditional PUT; blobs flush
// asynchronously; the index rows appear atomically with the refs.
func TestInlinePackCommit(t *testing.T) {
	w, store := newTestWAL(t, time.Millisecond)
	ctx := context.Background()

	const n = 24
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pack := &InlinePack{
				Name: fmt.Sprintf("pack-%040d", i),
				Data: []byte(fmt.Sprintf("packdata-%d", i)),
				Idx:  []byte(fmt.Sprintf("idxdata-%d", i)),
			}
			u := RefUpdate{Name: fmt.Sprintf("refs/heads/w%d", i), Old: ZeroOID, New: inlineOID(i)}
			if err := w.UpdateRefsWithPack(ctx, "r", []RefUpdate{u}, nil, pack); err != nil {
				t.Errorf("inline commit %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	refs, _ := w.ListRefs(ctx, "r")
	packs, _ := w.ListPacks(ctx, "r")
	if len(refs) != n || len(packs) != n {
		t.Fatalf("got %d refs, %d pack rows; want %d each", len(refs), len(packs), n)
	}
	// Blobs flush asynchronously; they must all appear.
	deadline := time.Now().Add(5 * time.Second)
	for {
		blobs, _ := store.List(ctx, "r", "pack-")
		if len(blobs) >= 2*n || time.Now().After(deadline) {
			if len(blobs) < 2*n {
				t.Fatalf("only %d pack blobs flushed, want %d", len(blobs), 2*n)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Flushed content matches what was inlined.
	rc, err := store.Get(ctx, "r", fmt.Sprintf("pack-%040d.pack", 3))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "packdata-3" {
		t.Fatalf("flushed pack content = %q", data)
	}
}

// The disaster case: machine dies after the entry PUT but before ANY blob
// flush. A fresh index recovering from the bucket alone must resurrect the
// refs, the pack rows, AND rewrite the standalone blobs from the entries.
func TestInlinePackRecoveryFromEntriesAlone(t *testing.T) {
	w, store := newTestWAL(t, 0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		pack := &InlinePack{
			Name: fmt.Sprintf("pack-%040d", 100+i),
			Data: []byte(fmt.Sprintf("survives-%d", i)),
			Idx:  []byte("idx"),
		}
		u := RefUpdate{Name: fmt.Sprintf("refs/heads/b%d", i), Old: ZeroOID, New: inlineOID(i)}
		if err := w.UpdateRefsWithPack(ctx, "r", []RefUpdate{u}, nil, pack); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate crash-before-flush: delete every standalone pack blob.
	blobs, _ := store.List(ctx, "r", "pack-")
	for _, b := range blobs {
		store.Delete(ctx, "r", b.Name)
	}

	// Fresh volume: empty index, same bucket.
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sq2.Close()
	w2 := NewWAL(sq2, store)
	if _, err := w2.RecoverRepos(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	refs, _ := w2.ListRefs(ctx, "r")
	if len(refs) != 3 {
		t.Fatalf("recovered %d refs, want 3", len(refs))
	}
	packs, _ := w2.ListPacks(ctx, "r")
	byName := map[string]bool{}
	for _, p := range packs {
		byName[p.Name] = true
	}
	for i := 0; i < 3; i++ {
		if !byName[fmt.Sprintf("pack-%040d", 100+i)] {
			t.Fatalf("pack row %d missing after recovery: %v", i, packs)
		}
	}
	// Replay must have rewritten the blobs from the entries.
	rc, err := store.Get(ctx, "r", fmt.Sprintf("pack-%040d.pack", 101))
	if err != nil {
		t.Fatalf("blob not re-flushed on recovery: %v", err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "survives-1" {
		t.Fatalf("recovered blob content = %q", data)
	}
}

// failPackPuts wraps the store, failing standalone pack-blob writes - the
// flusher can never succeed, so snapshot pruning must hold the entries.
type failPackPuts struct {
	*memStore
	mu   sync.Mutex
	fail bool
}

func (s *failPackPuts) Put(ctx context.Context, repo, name string, r io.Reader) error {
	s.mu.Lock()
	f := s.fail
	s.mu.Unlock()
	if f && strings.HasPrefix(name, "pack-") {
		return fmt.Errorf("injected put failure for %s", name)
	}
	return s.memStore.Put(ctx, repo, name, r)
}

func TestInlineFlushBarsPrune(t *testing.T) {
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	store := &failPackPuts{memStore: newMemStore(0), fail: true}
	w := NewWAL(sq, store)
	ctx := context.Background()
	if err := w.CreateRepo(ctx, "r", "main"); err != nil {
		t.Fatal(err)
	}

	// Enough sequential inline commits to cross the snapshot threshold.
	for i := 0; i < snapEvery+2; i++ {
		pack := &InlinePack{Name: fmt.Sprintf("pack-%040d", i), Data: []byte("d"), Idx: []byte("i")}
		u := RefUpdate{Name: fmt.Sprintf("refs/heads/s%d", i), Old: ZeroOID, New: inlineOID(i)}
		if err := w.UpdateRefsWithPack(ctx, "r", []RefUpdate{u}, nil, pack); err != nil {
			t.Fatal(err)
		}
	}
	// Let the async snapshot + flush attempts settle.
	time.Sleep(300 * time.Millisecond)

	// Every entry must still be present: nothing was flushable, so nothing
	// was prunable - the entries are the only durable copy of the packs.
	entries, _ := store.List(ctx, "r", walPrefix)
	if len(entries) < snapEvery {
		t.Fatalf("entries pruned while unflushed: %d left, want >= %d", len(entries), snapEvery)
	}

	// Heal the store; the next snapshot catch-up flushes and prunes.
	store.mu.Lock()
	store.fail = false
	store.mu.Unlock()
	r := w.repo("r")
	w.snapshot("r", r)
	blobs, _ := store.List(ctx, "r", "pack-")
	if len(blobs) < 2*(snapEvery+2) {
		t.Fatalf("catch-up flushed %d blobs, want %d", len(blobs), 2*(snapEvery+2))
	}
	entries, _ = store.List(ctx, "r", walPrefix)
	if len(entries) != 0 {
		t.Fatalf("healed snapshot should prune all entries, %d left", len(entries))
	}
	// And the refs replay identically from snapshot alone on a fresh index.
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sq2.Close()
	w2 := NewWAL(sq2, store)
	if _, err := w2.RecoverRepos(ctx); err != nil {
		t.Fatal(err)
	}
	refs, _ := w2.ListRefs(ctx, "r")
	if len(refs) != snapEvery+2 {
		t.Fatalf("recovered %d refs, want %d", len(refs), snapEvery+2)
	}
}

var _ = bytes.MinRead // keep bytes imported if edits drop other uses

// stubIdx is a deterministic non-empty idx generator for tests (the real
// one lives in ingest, which repodb cannot import). Flush/prune/recovery
// logic only cares that a non-empty .idx blob lands.
func stubIdx(pack []byte) ([]byte, error) { return append([]byte("IDX"), pack...), nil }

// Production shape: inline packs carry NO idx (RegenIdx rebuilds it). A
// disaster restore with every standalone blob gone must resurrect refs,
// pack rows, AND re-flush both .pack and .idx from the entries.
func TestInlineIdxlessRecovery(t *testing.T) {
	w, store := newTestWAL(t, 0)
	w.RegenIdx = stubIdx
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		pack := &InlinePack{Name: fmt.Sprintf("pack-%040d", 200+i), Data: []byte(fmt.Sprintf("body-%d", i))} // no Idx
		u := RefUpdate{Name: fmt.Sprintf("refs/heads/p%d", i), Old: ZeroOID, New: inlineOID(i)}
		if err := w.UpdateRefsWithPack(ctx, "r", []RefUpdate{u}, nil, pack); err != nil {
			t.Fatal(err)
		}
	}
	// Async flush lands both blobs.
	deadline := time.Now().Add(5 * time.Second)
	for {
		b, _ := store.List(ctx, "r", "pack-")
		if len(b) >= 8 || time.Now().After(deadline) {
			if len(b) < 8 {
				t.Fatalf("idxless flush produced %d blobs, want 8", len(b))
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Crash-before-flush: wipe every standalone blob.
	blobs, _ := store.List(ctx, "r", "pack-")
	for _, b := range blobs {
		store.Delete(ctx, "r", b.Name)
	}
	// Fresh index, RegenIdx wired (the ordering fix guarantees this in prod).
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sq2.Close()
	w2 := NewWAL(sq2, store)
	w2.RegenIdx = stubIdx
	if _, err := w2.RecoverRepos(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if refs, _ := w2.ListRefs(ctx, "r"); len(refs) != 4 {
		t.Fatalf("recovered %d refs, want 4", len(refs))
	}
	// Both .pack and .idx re-flushed from the entries.
	for i := 0; i < 4; i++ {
		for _, ext := range []string{".pack", ".idx"} {
			if _, err := store.Get(ctx, "r", fmt.Sprintf("pack-%040d%s", 200+i, ext)); err != nil {
				t.Fatalf("blob %d%s not re-flushed on recovery: %v", 200+i, ext, err)
			}
		}
	}
}

// Prune must never delete an entry whose inline packs are not confirmed
// durable - even with no persisted watermark and RegenIdx present.
func TestInlinePruneConfirmsIdxlessFlush(t *testing.T) {
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	store := &failPackPuts{memStore: newMemStore(0), fail: true}
	w := NewWAL(sq, store)
	w.RegenIdx = stubIdx
	ctx := context.Background()
	if err := w.CreateRepo(ctx, "r", "main"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < snapEvery+2; i++ {
		pack := &InlinePack{Name: fmt.Sprintf("pack-%040d", i), Data: []byte("d")} // no Idx
		u := RefUpdate{Name: fmt.Sprintf("refs/heads/s%d", i), Old: ZeroOID, New: inlineOID(i)}
		if err := w.UpdateRefsWithPack(ctx, "r", []RefUpdate{u}, nil, pack); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	entries, _ := store.List(ctx, "r", walPrefix)
	if len(entries) < snapEvery {
		t.Fatalf("entries pruned while unflushed: %d left", len(entries))
	}
	// Heal; the next snapshot confirm-flushes then prunes.
	store.mu.Lock()
	store.fail = false
	store.mu.Unlock()
	w.snapshot("r", w.repo("r"))
	blobs, _ := store.List(ctx, "r", "pack-")
	if len(blobs) < 2*(snapEvery+2) {
		t.Fatalf("catch-up flushed %d blobs, want %d", len(blobs), 2*(snapEvery+2))
	}
	if e, _ := store.List(ctx, "r", walPrefix); len(e) != 0 {
		t.Fatalf("healed snapshot should prune all entries, %d left", len(e))
	}
}
