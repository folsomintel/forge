// Package packstore reads pack data straight from the blob store in fixed
// blocks through a process-wide byte-bounded LRU, so a repo whose packs
// exceed local disk can still be served: only the pack .idx stays local, and
// object bytes are paged in on demand. This is the foundation the history
// pack (blob-on-demand) builds on.
//
// A Reader is an io.ReaderAt over one remote blob. Blocks are 1 MiB, aligned,
// and shared across all Readers of the same blob via the Cache. Pack data is
// immutable (content-addressed names), so a cached block never goes stale.
package packstore

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// BlockSize is the paging granularity: big enough to amortize per-request
// latency (an S3 GET is ~tens of ms regardless of size up to ~MBs), small
// enough that a random object read pulls little waste.
const BlockSize = 1 << 20 // 1 MiB

// RangeReader is the slice of blobstore.Store packstore needs. Kept local to
// avoid a package dependency (and to make Readers trivial to fake in tests).
type RangeReader interface {
	GetRange(ctx context.Context, repoID, name string, off, length int64) (io.ReadCloser, error)
}

// Cache is a process-wide, byte-bounded LRU of pack blocks, shared by all
// Readers. Safe for concurrent use.
type Cache struct {
	mu   sync.Mutex
	max  int64
	cur  int64
	ll   *list.List               // front = most recently used
	m    map[string]*list.Element // key -> element(*cacheEntry)
	miss uint64
	hit  uint64
}

type cacheEntry struct {
	key  string
	data []byte
}

// NewCache bounds the block cache to maxBytes (min one block).
func NewCache(maxBytes int64) *Cache {
	if maxBytes < BlockSize {
		maxBytes = BlockSize
	}
	return &Cache{max: maxBytes, ll: list.New(), m: map[string]*list.Element{}}
}

func (c *Cache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[key]; ok {
		c.ll.MoveToFront(el)
		c.hit++
		return el.Value.(*cacheEntry).data, true
	}
	c.miss++
	return nil, false
}

func (c *Cache) put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; ok {
		return // immutable content; first writer wins
	}
	el := c.ll.PushFront(&cacheEntry{key: key, data: data})
	c.m[key] = el
	c.cur += int64(len(data))
	for c.cur > c.max && c.ll.Len() > 1 {
		back := c.ll.Back()
		if back == nil {
			break
		}
		ent := c.ll.Remove(back).(*cacheEntry)
		delete(c.m, ent.key)
		c.cur -= int64(len(ent.data))
	}
}

// Stats returns cumulative hit/miss counts (for telemetry/tests).
func (c *Cache) Stats() (hits, misses uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hit, c.miss
}

// Reader is an io.ReaderAt over a single remote blob, paging BlockSize chunks
// through the shared Cache. Not tied to a goroutine; safe for concurrent
// ReadAt (the underlying git/idx access pattern is random reads).
type Reader struct {
	ctx   context.Context
	store RangeReader
	repo  string
	name  string
	size  int64
	cache *Cache
}

// Open returns a Reader for the blob (repo,name) of the given size. size must
// be the true object size (from the pack row / idx); ReadAt clamps to it.
func Open(ctx context.Context, store RangeReader, cache *Cache, repo, name string, size int64) *Reader {
	return &Reader{ctx: ctx, store: store, repo: repo, name: name, size: size, cache: cache}
}

// Size is the blob's total size.
func (r *Reader) Size() int64 { return r.size }

func (r *Reader) blockKey(idx int64) string {
	return r.repo + "\x00" + r.name + "\x00" + strconv.FormatInt(idx, 10)
}

// block returns block idx (aligned BlockSize), fetching+caching on miss.
func (r *Reader) block(idx int64) ([]byte, error) {
	key := r.blockKey(idx)
	if b, ok := r.cache.get(key); ok {
		return b, nil
	}
	off := idx * BlockSize
	if off >= r.size {
		return nil, io.EOF
	}
	length := int64(BlockSize)
	if off+length > r.size {
		length = r.size - off
	}
	rc, err := r.store.GetRange(r.ctx, r.repo, r.name, off, length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	buf := make([]byte, length)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, fmt.Errorf("packstore: short block read %s@%d: %w", r.name, off, err)
	}
	r.cache.put(key, buf)
	return buf, nil
}

// ReadAt implements io.ReaderAt: fills p from off, spanning blocks as needed.
// Returns io.EOF when (and only when) it reads fewer than len(p) bytes because
// it hit the end of the blob, matching the io.ReaderAt contract.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("packstore: negative offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) {
		abs := off + int64(n)
		if abs >= r.size {
			return n, io.EOF
		}
		idx := abs / BlockSize
		blk, err := r.block(idx)
		if err != nil {
			return n, err
		}
		start := abs - idx*BlockSize
		n += copy(p[n:], blk[start:])
	}
	return n, nil
}
