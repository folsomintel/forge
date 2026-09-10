package repodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
)

// memStore is an in-memory blobstore with injectable conditional-PUT
// latency - it stands in for Tigris (~10ms same-region) so the group
// commit multiplier is measurable in-process.
type memStore struct {
	mu      sync.Mutex
	m       map[string][]byte
	latency time.Duration
	puts    atomic.Int64 // conditional PUTs that succeeded (= WAL entries)
}

func newMemStore(latency time.Duration) *memStore {
	return &memStore{m: map[string][]byte{}, latency: latency}
}

func (s *memStore) key(repo, name string) string { return repo + "/" + name }

func (s *memStore) Put(ctx context.Context, repo, name string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.m[s.key(repo, name)] = data
	s.mu.Unlock()
	return nil
}

func (s *memStore) PutIfAbsent(ctx context.Context, repo, name string, data []byte) error {
	time.Sleep(s.latency)
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(repo, name)
	if _, ok := s.m[k]; ok {
		return blobstore.ErrExists
	}
	s.m[k] = append([]byte(nil), data...)
	s.puts.Add(1)
	return nil
}

func (s *memStore) Get(ctx context.Context, repo, name string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.m[s.key(repo, name)]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", blobstore.ErrNotFound, repo, name)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memStore) GetRange(ctx context.Context, repo, name string, off, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.m[s.key(repo, name)]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", blobstore.ErrNotFound, repo, name)
	}
	if off > int64(len(data)) {
		off = int64(len(data))
	}
	end := off + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return io.NopCloser(bytes.NewReader(data[off:end])), nil
}

func (s *memStore) Delete(ctx context.Context, repo, name string) error {
	s.mu.Lock()
	delete(s.m, s.key(repo, name))
	s.mu.Unlock()
	return nil
}

func (s *memStore) Copy(ctx context.Context, repo, src, dst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.m[s.key(repo, src)]
	if !ok {
		return blobstore.ErrNotFound
	}
	s.m[s.key(repo, dst)] = data
	return nil
}

func (s *memStore) List(ctx context.Context, repo, prefix string) ([]blobstore.BlobInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []blobstore.BlobInfo
	for k, v := range s.m {
		if rest, ok := strings.CutPrefix(k, repo+"/"); ok && strings.HasPrefix(rest, prefix) {
			out = append(out, blobstore.BlobInfo{Name: rest, Size: int64(len(v)), ModTime: time.Now()})
		}
	}
	return out, nil
}

func (s *memStore) Prefixes(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	for k := range s.m {
		if i := strings.IndexByte(k, '/'); i > 0 {
			seen[k[:i]] = true
		}
	}
	var out []string
	for p := range seen {
		out = append(out, p)
	}
	return out, nil
}

func (s *memStore) Presign(ctx context.Context, repo, name string, exp time.Duration) (string, error) {
	return "", blobstore.ErrNoPresign
}

func newTestWAL(t *testing.T, latency time.Duration) (*WAL, *memStore) {
	t.Helper()
	sq, err := OpenSQLite(filepath.Join(t.TempDir(), "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	store := newMemStore(latency)
	w := NewWAL(sq, store)
	if err := w.CreateRepo(context.Background(), "r", "main"); err != nil {
		t.Fatal(err)
	}
	return w, store
}

const testOID = "1111111111111111111111111111111111111111"

// Correctness under concurrency: distinct refs all land, batches shrink
// the entry count, and the refs replay to an identical state.
func TestGroupCommitCorrectness(t *testing.T) {
	w, store := newTestWAL(t, 2*time.Millisecond)
	ctx := context.Background()

	const workers, each = 16, 10
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				u := RefUpdate{Name: fmt.Sprintf("refs/heads/w%d-%d", g, i), Old: ZeroOID, New: testOID}
				if err := w.UpdateRefs(ctx, "r", []RefUpdate{u}, nil); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	refs, err := w.ListRefs(ctx, "r")
	if err != nil || len(refs) != workers*each {
		t.Fatalf("want %d refs, got %d (err %v)", workers*each, len(refs), err)
	}
	entries := store.puts.Load()
	if entries >= workers*each {
		t.Fatalf("no batching happened: %d entries for %d txs", entries, workers*each)
	}
	t.Logf("%d txs -> %d WAL entries (avg batch %.1f)",
		workers*each, entries, float64(workers*each)/float64(entries))

	// Replay fidelity: a fresh index rebuilt from the store must match.
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "forge2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sq2.Close()
	w2 := NewWAL(sq2, store)
	if _, err := w2.RecoverRepos(ctx); err != nil {
		t.Fatal(err)
	}
	refs2, _ := w2.ListRefs(ctx, "r")
	if len(refs2) != len(refs) {
		t.Fatalf("replay mismatch: %d vs %d refs", len(refs2), len(refs))
	}
}

// CAS conflicts fail individually without poisoning their batch.
func TestGroupCommitCASConflictsAreIndividual(t *testing.T) {
	w, _ := newTestWAL(t, 2*time.Millisecond)
	ctx := context.Background()

	const workers = 12
	var wg sync.WaitGroup
	var won, lost atomic.Int64
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Everyone races to create the SAME ref...
			u := RefUpdate{Name: "refs/heads/contested", Old: ZeroOID, New: testOID}
			err := w.UpdateRefs(ctx, "r", []RefUpdate{u}, nil)
			switch {
			case err == nil:
				won.Add(1)
			case errors.Is(err, ErrCASFailed):
				lost.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
			// ...and everyone also creates their own ref, which must land
			// even when sharing a batch with the losers above.
			own := RefUpdate{Name: fmt.Sprintf("refs/heads/own-%d", g), Old: ZeroOID, New: testOID}
			if err := w.UpdateRefs(ctx, "r", []RefUpdate{own}, nil); err != nil {
				t.Errorf("own ref failed: %v", err)
			}
		}(g)
	}
	wg.Wait()
	if won.Load() != 1 || lost.Load() != workers-1 {
		t.Fatalf("contested ref: won=%d lost=%d (want 1/%d)", won.Load(), lost.Load(), workers-1)
	}
	refs, _ := w.ListRefs(ctx, "r")
	if len(refs) != workers+1 {
		t.Fatalf("want %d refs, got %d", workers+1, len(refs))
	}
}

// Throughput under Tigris-like PUT latency: with group commit as the only
// mode, assert the batching multiplier directly - entries must be a small
// fraction of transactions, and throughput far beyond 1/PUT-RTT.
func TestGroupCommitThroughput(t *testing.T) {
	const latency = 10 * time.Millisecond // Tigris same-region conditional PUT
	const workers, each = 32, 8

	w, store := newTestWAL(t, latency)
	ctx := context.Background()
	var wg sync.WaitGroup
	t0 := time.Now()
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				u := RefUpdate{Name: fmt.Sprintf("refs/heads/w%d-%d", g, i), Old: ZeroOID, New: testOID}
				if err := w.UpdateRefs(ctx, "r", []RefUpdate{u}, nil); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(t0)
	txs := workers * each
	tps := float64(txs) / elapsed.Seconds()
	entries := store.puts.Load()
	serialFloor := float64(time.Second / latency) // ~1/PUT-RTT, the pre-group ceiling
	t.Logf("%d txs in %v: %.0f tx/s, %d entries (avg batch %.1f)",
		txs, elapsed.Round(time.Millisecond), tps, entries, float64(txs)/float64(entries))
	if tps < serialFloor*3 {
		t.Fatalf("group commit not batching: %.0f tx/s (serial ceiling ~%.0f)", tps, serialFloor)
	}
	if entries*4 > int64(txs) {
		t.Fatalf("batches too small: %d entries for %d txs", entries, txs)
	}
}

// Pre-WAL repos (refs in the index, nothing in the bucket) get their
// bucket truth seeded by Backfill - the Litestream retirement migration.
func TestBackfillMigratesPreWALRepos(t *testing.T) {
	w, store := newTestWAL(t, 0)
	ctx := context.Background()

	// Simulate a legacy repo: index rows exist, bucket has no WAL data.
	if err := w.SQLite.CreateRepo(ctx, "legacy", "main"); err != nil {
		t.Fatal(err)
	}
	if err := w.SQLite.ApplyWAL(ctx, "legacy",
		[]RefUpdate{{Name: "refs/heads/main", Old: ZeroOID, New: testOID}}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}

	if err := w.Backfill(ctx); err != nil {
		t.Fatal(err)
	}

	// Disaster: fresh index + same bucket resurrects the legacy repo.
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sq2.Close()
	w2 := NewWAL(sq2, store)
	if _, err := w2.RecoverRepos(ctx); err != nil {
		t.Fatal(err)
	}
	refs, err := w2.ListRefs(ctx, "legacy")
	if err != nil || len(refs) != 1 || refs[0].Target != testOID {
		t.Fatalf("legacy repo did not resurrect: %v %v", refs, err)
	}
	repo, err := w2.GetRepo(ctx, "legacy")
	if err != nil || repo.DefaultBranch != "main" {
		t.Fatalf("default branch lost: %v %v", repo, err)
	}
}

// TestRefreshIndexReplicaFreshness is the multi-machine correctness gate:
// a follower sharing the bucket but with its own index serves stale after a
// write on the primary, and becomes current only via RefreshIndex - proving
// freshness-on-read for horizontal replicas.
func TestRefreshIndexReplicaFreshness(t *testing.T) {
	ctx := context.Background()
	primary, store := newTestWAL(t, 0)

	// Replica: separate SQLite index, SAME bucket. Recover to sync it to the
	// primary's current state (repo exists, no refs yet).
	sq2, err := OpenSQLite(filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq2.Close() })
	replica := NewWAL(sq2, store)
	if _, err := replica.RecoverRepos(ctx); err != nil {
		t.Fatal(err)
	}
	if refs, _ := replica.ListRefs(ctx, "r"); len(refs) != 0 {
		t.Fatalf("replica should start with 0 refs, got %d", len(refs))
	}

	// Primary commits a ref (durable in the shared bucket WAL).
	u := RefUpdate{Name: "refs/heads/main", Old: ZeroOID, New: testOID}
	if err := primary.UpdateRefs(ctx, "r", []RefUpdate{u}, nil); err != nil {
		t.Fatal(err)
	}

	// The replica's index is stale until it refreshes.
	advanced, err := replica.RefreshIndex(ctx, "r")
	if err != nil {
		t.Fatalf("RefreshIndex: %v", err)
	}
	if !advanced {
		t.Fatal("expected RefreshIndex to advance after a primary write")
	}
	refs, _ := replica.ListRefs(ctx, "r")
	if len(refs) != 1 || refs[0].Name != "refs/heads/main" || refs[0].Target != testOID {
		t.Fatalf("replica did not catch up: %+v", refs)
	}

	// A second refresh with no new writes is a cheap no-op (not advanced).
	advanced, err = replica.RefreshIndex(ctx, "r")
	if err != nil {
		t.Fatal(err)
	}
	if advanced {
		t.Fatal("RefreshIndex advanced with no new writes")
	}
}
