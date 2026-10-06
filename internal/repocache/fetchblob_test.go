package repocache

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/folsomintel/forge/internal/blobstore"
)

type countingStore struct {
	blobstore.Store
	gets atomic.Int64
}

func (s *countingStore) Get(ctx context.Context, repoID, name string) (io.ReadCloser, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, repoID, name)
}

// A hydrate-only artifact the store lacks is probed once per TTL, not on
// every materialization; it is fetched as soon as it exists after expiry,
// and packs are never negative-cached.
func TestFetchBlobNegativeCachesAbsentArtifacts(t *testing.T) {
	ctx := context.Background()
	local, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := &countingStore{Store: local}
	c := &Cache{Blobs: store}
	dest := filepath.Join(t.TempDir(), "commit-graph")

	for range 3 {
		if err := c.FetchBlob(ctx, "r", "meta/commit-graph", dest); !errors.Is(err, blobstore.ErrNotFound) {
			t.Fatalf("FetchBlob missing artifact: %v", err)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Fatalf("store GETs for a missing artifact = %d, want 1", n)
	}

	// Once the entry expires, a newly derived artifact is fetched.
	if err := local.Put(ctx, "r", "meta/commit-graph", strings.NewReader("cg")); err != nil {
		t.Fatal(err)
	}
	c.absentArtifacts.Range(func(k, _ any) bool { c.absentArtifacts.Delete(k); return true })
	if err := c.FetchBlob(ctx, "r", "meta/commit-graph", dest); err != nil {
		t.Fatalf("FetchBlob after the artifact appeared: %v", err)
	}

	store.gets.Store(0)
	for range 2 {
		c.FetchBlob(ctx, "r", "pack-abc.pack", filepath.Join(t.TempDir(), "p"))
	}
	if n := store.gets.Load(); n != 2 {
		t.Fatalf("store GETs for a missing pack = %d, want 2 (never cached)", n)
	}
}
