package packstore

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/blobstore"
)

// deterministic pseudo-random bytes (no rand import: keep tests reproducible).
func blob(n int) []byte {
	b := make([]byte, n)
	x := uint32(2166136261)
	for i := range b {
		x = x*16777619 + uint32(i)
		b[i] = byte(x >> 13)
	}
	return b
}

func TestReaderMatchesFullReadAcrossBlocks(t *testing.T) {
	store, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// 2.5 blocks so reads cross boundaries and the tail block is partial.
	data := blob(BlockSize*2 + BlockSize/2)
	if err := store.Put(ctx, "repo", "pack-x.pack", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	cache := NewCache(64 << 20)
	r := Open(ctx, store, cache, "repo", "pack-x.pack", int64(len(data)))
	if r.Size() != int64(len(data)) {
		t.Fatalf("size = %d, want %d", r.Size(), len(data))
	}

	// A spread of offsets/lengths, including block edges and the tail.
	cases := []struct{ off, ln int }{
		{0, 10},
		{BlockSize - 5, 10},     // spans block 0->1
		{BlockSize, 1},          // exact boundary
		{BlockSize*2 - 1, 3},    // spans block 1->2 (tail)
		{0, len(data)},          // whole thing
		{len(data) - 7, 7},      // exact end
		{BlockSize + 123, 5000}, // mid, multi-block
	}
	for _, c := range cases {
		p := make([]byte, c.ln)
		n, err := r.ReadAt(p, int64(c.off))
		want := data[c.off:min(c.off+c.ln, len(data))]
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt(%d,%d): %v", c.off, c.ln, err)
		}
		if !bytes.Equal(p[:n], want) {
			t.Fatalf("ReadAt(%d,%d) mismatch: got %d bytes", c.off, c.ln, n)
		}
	}

	// Reading at/after EOF yields io.EOF.
	if n, err := r.ReadAt(make([]byte, 4), int64(len(data))); err != io.EOF || n != 0 {
		t.Fatalf("ReadAt at EOF = (%d,%v), want (0,EOF)", n, err)
	}

	// Second pass is served from cache (hits climb, misses stop).
	_, missBefore := cache.Stats()
	p := make([]byte, 100)
	r.ReadAt(p, 0)
	_, missAfter := cache.Stats()
	if missAfter != missBefore {
		t.Fatalf("expected cache hit on re-read; misses went %d -> %d", missBefore, missAfter)
	}
}

func TestCacheEvictsUnderBound(t *testing.T) {
	store, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := blob(BlockSize * 8)
	store.Put(ctx, "repo", "pack-y.pack", bytes.NewReader(data))

	// Cache holds ~2 blocks; a sweep over 8 blocks must evict and stay bounded.
	cache := NewCache(BlockSize * 2)
	r := Open(ctx, store, cache, "repo", "pack-y.pack", int64(len(data)))
	one := make([]byte, 16)
	for i := int64(0); i < 8; i++ {
		if _, err := r.ReadAt(one, i*BlockSize); err != nil && err != io.EOF {
			t.Fatal(err)
		}
	}
	cache.mu.Lock()
	cur, max := cache.cur, cache.max
	cache.mu.Unlock()
	if cur > max {
		t.Fatalf("cache exceeded bound: cur=%d max=%d", cur, max)
	}

	// Content still reads correctly after eviction churn.
	p := make([]byte, 1000)
	n, _ := r.ReadAt(p, BlockSize*7+10)
	if !bytes.Equal(p[:n], data[BlockSize*7+10:min(BlockSize*7+10+1000, len(data))]) {
		t.Fatal("post-eviction read mismatch")
	}
}

func TestConcurrentReadsAreSafe(t *testing.T) {
	store, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := blob(BlockSize * 4)
	store.Put(ctx, "repo", "pack-z.pack", bytes.NewReader(data))

	cache := NewCache(BlockSize * 2) // small enough to force concurrent eviction
	r := Open(ctx, store, cache, "repo", "pack-z.pack", int64(len(data)))

	done := make(chan bool, 16)
	for g := 0; g < 16; g++ {
		go func(g int) {
			p := make([]byte, 777)
			for i := 0; i < 50; i++ {
				off := int64((g*131 + i*997) % (len(data) - 777))
				n, err := r.ReadAt(p, off)
				if err != nil && err != io.EOF {
					t.Errorf("read: %v", err)
					break
				}
				if !bytes.Equal(p[:n], data[off:int(off)+n]) {
					t.Errorf("concurrent read mismatch at %d", off)
					break
				}
			}
			done <- true
		}(g)
	}
	for i := 0; i < 16; i++ {
		<-done
	}
}

func TestReaderMissingBlobErrors(t *testing.T) {
	store, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := Open(context.Background(), store, NewCache(BlockSize), "repo", "nope.pack", 4096)
	if _, err := r.ReadAt(make([]byte, 10), 0); err == nil || strings.Contains(err.Error(), "EOF") {
		t.Fatalf("want a fetch error for a missing blob, got %v", err)
	}
}
