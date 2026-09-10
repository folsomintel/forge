package repocache

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Cache eviction: the cache dir is a disposable materialization of
// store+DB, so under disk pressure we delete least-recently-used repos and
// let them rebuild on next touch (the Sourcegraph gitserver model). Eviction
// never touches the blob store or the metadata DB.

// evictMinIdle protects just-served repos from the eviction/serve race:
// the stale-serve path returns a dir without holding the lock for the
// whole request, so never evict anything touched this recently.
const evictMinIdle = time.Minute

// DiskUsage reports the filesystem holding the cache dir.
func (c *Cache) DiskUsage() (total, free uint64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(c.Dir, &st); err != nil {
		return 0, 0
	}
	bs := uint64(st.Bsize)
	return uint64(st.Blocks) * bs, uint64(st.Bavail) * bs
}

// RunEviction evicts LRU cache repos when disk usage crosses highPct,
// until it is back under lowPct.
func (c *Cache) RunEviction(ctx context.Context, interval time.Duration, highPct, lowPct int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.evictTick(highPct, lowPct)
		}
	}
}

func (c *Cache) usedPct() int {
	total, free := c.DiskUsage()
	if total == 0 {
		return 0
	}
	return int((total - free) * 100 / total)
}

func (c *Cache) evictTick(highPct, lowPct int) {
	if c.usedPct() < highPct {
		return
	}
	entries, err := os.ReadDir(c.Dir)
	if err != nil {
		return
	}
	type candidate struct {
		repoID string
		at     time.Time
	}
	var candidates []candidate
	for _, e := range entries {
		if !e.IsDir() || !strings.HasSuffix(e.Name(), ".git") {
			continue
		}
		repoID := strings.TrimSuffix(e.Name(), ".git")
		at := time.Time{}
		if v, ok := c.lastAccess.Load(repoID); ok {
			at = v.(time.Time)
		} else if info, err := e.Info(); err == nil {
			at = info.ModTime()
		}
		if time.Since(at) < evictMinIdle {
			continue
		}
		candidates = append(candidates, candidate{repoID, at})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].at.Before(candidates[j].at) })

	for _, cand := range candidates {
		if c.usedPct() <= lowPct {
			return
		}
		lock := c.Lock(cand.repoID)
		if !lock.TryLock() {
			continue // in use right now; next round
		}
		c.Cats.Kill(cand.repoID)
		err := os.RemoveAll(filepath.Join(c.Dir, cand.repoID+".git"))
		c.Invalidate(cand.repoID)
		lock.Unlock()
		if err != nil {
			slog.Error("evict cache repo", "repo", cand.repoID, "err", err)
			continue
		}
		c.lastAccess.Delete(cand.repoID)
		slog.Info("evicted cache repo", "repo", cand.repoID, "idle_since", cand.at)
	}
	if pct := c.usedPct(); pct > highPct {
		slog.Warn("disk still above high watermark after eviction", "used_pct", pct)
	}
}
