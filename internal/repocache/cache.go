// Package repocache materializes cache repos. A cache repo is a disposable
// bare repo on local disk assembled from the two sources of truth: the pack
// list in the metadata DB (blobs fetched from the blob store) and the ref
// rows. Any cache repo can be deleted at any time and rebuilt from scratch.
package repocache

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/packstore"
	"github.com/folsomintel/forge/internal/repodb"
)

// cacheConfig disables everything that would let git mutate or GC the cache
// behind our back. unpackLimit=1 forces every push to stay a pack (the store
// has no concept of loose objects).
const cacheConfig = `[core]
	repositoryformatversion = 0
	bare = true
[gc]
	auto = 0
	autoDetach = false
[receive]
	autogc = false
	unpackLimit = 1
	advertisePushOptions = true
[transfer]
	unpackLimit = 1
[uploadpack]
	allowFilter = true
[include]
	path = bundles.conf
`

type Cache struct {
	Dir   string
	DB    repodb.DB
	Blobs blobstore.Store

	// Replica marks a read-only follower: it refreshes its index from the
	// bucket WAL before serving a read, so a push committed on another
	// machine is visible here. The primary/single-machine writer leaves this
	// false and is current by construction.
	Replica bool

	// Cats pools cat-file daemons for API object reads.
	Cats gitcmd.Pool

	// Remote placement (walgit remote reader): when RemoteBytes > 0 and a
	// repo's total pack bytes exceed it, syncPacks fetches only the .idx of
	// large packs (leaving a .remote size sidecar) and object reads are served
	// straight from the bucket in blocks via Blocks, so a repo bigger than
	// local disk still serves reads. Blocks nil (default) disables it.
	Blocks      *packstore.Cache
	RemoteBytes int64

	// idxCache caches parsed pack indexes (packName -> *packstore.Index).
	// Pack names are content-addressed, so an entry never goes stale.
	idxCache sync.Map
	// remoteSets caches the per-repo set of remotely-served packs (repoID ->
	// *remoteSet, or a nil-valued entry meaning "none"). Invalidated on
	// Invalidate/Drop.
	remoteSets sync.Map

	// Hydrate, when set (by the maintenance pipeline at wiring time), places
	// derived artifacts and advertisements into a fresh materialization.
	// Best-effort: it must never fail a materialization.
	Hydrate func(ctx context.Context, repoID, dir string)

	mu    sync.Mutex
	locks map[string]*sync.RWMutex

	// Materialize fast path: skip the whole refs/packs sync when a repo's
	// per-repo change token is unchanged since it was last synced. Per-repo
	// (not a single global token) so a push to one repo does not force every
	// other repo to re-materialize on its next read.
	syncMu   sync.Mutex
	syncedAt map[string]int64 // repoID -> RepoChangeToken it was last synced at

	lastAccess sync.Map // repoID -> time.Time (per-process; dir mtime is the fallback)
}

func New(dir string, db repodb.DB, blobs blobstore.Store) *Cache {
	return &Cache{Dir: dir, DB: db, Blobs: blobs,
		locks: map[string]*sync.RWMutex{}, syncedAt: map[string]int64{}}
}

// upToDate reports whether repoID's cache is already synced at its current
// per-repo change token. A write to another repo leaves this token unmoved,
// so the fast path survives it.
func (c *Cache) upToDate(ctx context.Context, repoID string) bool {
	tok, err := c.DB.RepoChangeToken(ctx, repoID)
	if err != nil {
		return false
	}
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	last, ok := c.syncedAt[repoID]
	return ok && last == tok
}

// markSyncedAt records that repoID's cache reflects state as of tok. The
// token MUST be sampled BEFORE the sync reads refs/packs: a lock-free push
// (goreceive / API writes hold no repo lock) can bump the token during the
// sync, and storing the post-sync token would mark the cache fresh at a
// value newer than what it actually contains - pinning stale refs until the
// next write. Storing the pre-sync token means such a push leaves
// syncedAt < current, so the next read re-syncs.
func (c *Cache) markSyncedAt(repoID string, tok int64) {
	c.syncMu.Lock()
	c.syncedAt[repoID] = tok
	c.syncMu.Unlock()
}

// isMaterialized reports whether dir holds a real cache repo. Sentinel is
// the config file, NOT objects/: the push fast paths pre-install pack files
// into objects/pack of an unmaterialized dir, so objects/ can exist before
// the skeleton (config/HEAD/refs) does.
func isMaterialized(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "config"))
	return err == nil
}

// Invalidate drops the fast-path flag after a write bypassed the cache.
func (c *Cache) Invalidate(repoID string) {
	c.syncMu.Lock()
	delete(c.syncedAt, repoID)
	c.syncMu.Unlock()
	c.remoteSets.Delete(repoID)
}

// MaterializeServe is Materialize for the wire-protocol path: when the
// repo's write lock is contended (in-flight pushes hold readers), it serves
// from the existing cache rather than convoying behind them - a slightly
// stale ref advertisement is harmless because the DB CAS arbitrates every
// ref move (and git clients negotiate from whatever is advertised).
func (c *Cache) MaterializeServe(ctx context.Context, repoID string) (string, error) {
	dir := c.RepoDir(repoID)
	// Replica freshness gate: pull any WAL tail written by another machine
	// into the local index before deciding the cache is up to date. Advancing
	// the index moves the local change token, so upToDate then re-materializes
	// with the fresh refs. Cheap when already current (one small bucket read).
	if c.Replica {
		c.refreshIndex(ctx, repoID)
	}
	if c.upToDate(ctx, repoID) {
		if isMaterialized(dir) {
			c.touch(repoID)
			return dir, nil
		}
	}
	lock := c.Lock(repoID)
	if lock.TryLock() {
		defer lock.Unlock()
		return c.Materialize(ctx, repoID)
	}
	if isMaterialized(dir) {
		c.touch(repoID)
		return dir, nil // stale-but-serving beats convoying
	}
	lock.Lock()
	defer lock.Unlock()
	return c.Materialize(ctx, repoID)
}

// refreshIndex asks the WAL-backed DB to catch the local index up to the
// bucket (replica read path). Best-effort: a refresh error degrades to
// serving the current index, never fails the read. No-op if the DB does
// not implement the freshness capability (e.g. a plain SQLite index).
func (c *Cache) refreshIndex(ctx context.Context, repoID string) {
	f, ok := c.DB.(interface {
		RefreshIndex(context.Context, string) (bool, error)
	})
	if !ok {
		return
	}
	if advanced, err := f.RefreshIndex(ctx, repoID); err != nil {
		slog.Warn("replica refresh", "repo", repoID, "err", err)
	} else if advanced {
		c.Invalidate(repoID) // force a re-materialize with the fresh refs
	}
}

// Lock returns the per-repo lock. Pushes hold it exclusively for their whole
// git exec (correctness over throughput on day one); fetches hold it shared.
func (c *Cache) Lock(repoID string) *sync.RWMutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locks[repoID]
	if !ok {
		l = &sync.RWMutex{}
		c.locks[repoID] = l
	}
	return l
}

func (c *Cache) RepoDir(repoID string) string {
	return filepath.Join(c.Dir, repoID+".git")
}

// DirIfMaterialized returns the repo's cache path only when it is already
// materialized locally - no sync, no locks, no side effects. For callers
// that treat the local copy as a pure optimization.
func (c *Cache) DirIfMaterialized(repoID string) (string, bool) {
	dir := c.RepoDir(repoID)
	if !isMaterialized(dir) {
		return "", false
	}
	return dir, true
}

// ObjectInfo reads type and size. For a remote-placed repo it resolves from
// the bucket via packstore (the big .pack isn't local); otherwise it uses the
// pooled cat-file daemon. Normal repos have no remote set and pay nothing.
func (c *Cache) ObjectInfo(repoID, dir, oid string) (string, int64, error) {
	if rs := c.remoteSetFor(repoID, dir); rs != nil {
		if typ, data, ok, err := rs.object(oid); ok || err != nil {
			if err != nil {
				return "", 0, err
			}
			return typ, int64(len(data)), nil
		}
	}
	return c.Cats.ObjectInfo(repoID, dir, oid)
}

// BlobContents reads a blob, from the bucket via packstore for a remote-placed
// repo, else via the pooled cat-file daemon.
func (c *Cache) BlobContents(repoID, dir, oid string) ([]byte, error) {
	if rs := c.remoteSetFor(repoID, dir); rs != nil {
		if _, data, ok, err := rs.object(oid); ok || err != nil {
			return data, err
		}
	}
	return c.Cats.BlobContents(repoID, dir, oid)
}

// Materialize brings the cache repo for repoID in sync with the metadata
// store and returns its path. Caller must hold the repo lock (write).
func (c *Cache) Materialize(ctx context.Context, repoID string) (string, error) {
	dir := c.RepoDir(repoID)
	if c.upToDate(ctx, repoID) {
		if isMaterialized(dir) {
			c.touch(repoID)
			return dir, nil
		}
	}
	// Sample the change token BEFORE reading refs/packs, so a concurrent
	// lock-free push that lands mid-sync is not masked (see markSyncedAt).
	tok, tokErr := c.DB.RepoChangeToken(ctx, repoID)
	repo, err := c.DB.GetRepo(ctx, repoID)
	if err != nil {
		return "", err
	}
	if err := c.ensureSkeleton(dir); err != nil {
		return "", err
	}
	if err := c.syncPacks(ctx, repoID, dir); err != nil {
		return "", err
	}
	if err := c.SyncRefs(ctx, repoID, dir); err != nil {
		return "", err
	}
	if err := WriteFileAtomic(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/"+repo.DefaultBranch+"\n")); err != nil {
		return "", err
	}
	if c.Hydrate != nil {
		c.Hydrate(ctx, repoID, dir)
	}
	c.touch(repoID)
	if tokErr == nil {
		c.markSyncedAt(repoID, tok)
	}
	return dir, nil
}

// Drop removes the cache repo (used on repo delete; also safe as a repair).
func (c *Cache) Drop(repoID string) error {
	c.Invalidate(repoID)
	c.Cats.Kill(repoID)
	return os.RemoveAll(c.RepoDir(repoID))
}

func (c *Cache) ensureSkeleton(dir string) error {
	// Sentinel is the config file, NOT objects/: the push fast paths
	// pre-install pack files into unmaterialized dirs, so objects/ can
	// exist before the repo skeleton does.
	if _, err := os.Stat(filepath.Join(dir, "config")); err == nil {
		return nil
	}
	for _, d := range []string{"objects/pack", "objects/info", "refs/heads", "refs/tags"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return err
		}
	}
	if err := WriteFileAtomic(filepath.Join(dir, "config"), []byte(cacheConfig)); err != nil {
		return err
	}
	return WriteFileAtomic(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"))
}

// syncPacks downloads any pack in the metadata list that the cache is
// missing. Extra local packs (e.g. freshly pushed, or orphaned by a rejected
// push) are left alone - unreferenced packs are harmless to git.
// remotePackFloor: in remote-placement mode, only packs larger than this are
// served from the bucket (their .pack is left un-materialized); smaller packs
// stay local so tiny receive packs and the common case pay nothing.
const remotePackFloor = 1 << 20 // 1 MiB

func (c *Cache) syncPacks(ctx context.Context, repoID, dir string) error {
	packs, err := c.DB.ListPacks(ctx, repoID)
	if err != nil {
		return err
	}
	packDir := filepath.Join(dir, "objects", "pack")

	// Remote placement: when a repo's total pack bytes exceed RemoteBytes,
	// large packs are served from the bucket in blocks (see remote.go) - only
	// their .idx is kept local. Restricted to REPLICAS: a read-only follower
	// never runs maintenance or accepts pushes, so it can safely skip the big
	// .pack. The primary (which repacks and forks git) always holds full packs,
	// so the shared cache dir never has to be both full and idx-only at once.
	var total int64
	for _, p := range packs {
		total += p.SizeBytes
	}
	remote := c.Replica && c.Blocks != nil && c.RemoteBytes > 0 && total > c.RemoteBytes

	var missing []repodb.Pack // packs to hydrate FULLY (.pack + .idx)
	var idxOnly []repodb.Pack // remote packs to hydrate .idx-only (+ sidecar)
	hadRemote := false
	for _, p := range packs {
		servedRemotely := remote && p.SizeBytes > remotePackFloor
		idxPath := filepath.Join(packDir, p.Name+".idx")
		if servedRemotely {
			hadRemote = true
			// A remote pack must NOT keep its .pack locally (it may have been
			// full before the repo crossed the threshold).
			os.Remove(filepath.Join(packDir, p.Name+".pack"))
			if _, err := os.Stat(idxPath); err != nil {
				idxOnly = append(idxOnly, p)
			} else {
				c.writeRemoteSidecar(packDir, p) // idx already local; ensure sidecar
			}
			continue
		}
		_, packErr := os.Stat(filepath.Join(packDir, p.Name+".pack"))
		_, idxErr := os.Stat(idxPath)
		if packErr != nil || idxErr != nil { // both must be present; a torn install re-hydrates
			missing = append(missing, p)
		}
	}
	if len(idxOnly) > 0 {
		if err := c.hydrateIdxOnly(ctx, repoID, packDir, idxOnly); err != nil {
			return err
		}
	}
	if remote {
		c.hydrateHistoryPack(ctx, repoID, dir) // best-effort; absent = serve history from the remote pack
	}
	// The pack layout may have changed (a repo crossed the threshold, or a new
	// remote pack landed); drop the cached remote serving set so reads rebuild.
	if hadRemote || len(idxOnly) > 0 {
		c.remoteSets.Delete(repoID)
	}
	if len(missing) == 0 {
		return nil
	}
	// Hydrate packs concurrently (bounded); within one pack, .pack lands
	// before .idx - git only notices a pack via its .idx.
	sem := make(chan struct{}, 4)
	errs := make(chan error, len(missing))
	for _, p := range missing {
		sem <- struct{}{}
		go func(p repodb.Pack) {
			defer func() { <-sem }()
			blobRepo := p.BlobRepo
			if blobRepo == "" {
				blobRepo = repoID
			}
			for _, ext := range []string{".pack", ".idx"} {
				err := c.FetchBlob(ctx, blobRepo, p.Name+ext, filepath.Join(packDir, p.Name+ext))
				if err != nil {
					// Inline-pack race: the entry is durable but the async
					// blob flush hasn't landed yet - recover it from the WAL
					// tail and retry once.
					if rec, ok := c.DB.(interface {
						RecoverInlinePack(context.Context, string, string) bool
					}); ok && rec.RecoverInlinePack(ctx, blobRepo, p.Name) {
						err = c.FetchBlob(ctx, blobRepo, p.Name+ext, filepath.Join(packDir, p.Name+ext))
					}
				}
				if err != nil {
					errs <- fmt.Errorf("hydrate %s%s: %w", p.Name, ext, err)
					return
				}
			}
			// Bitmaps exist only for gc packs; missing is normal.
			if p.Source == "gc" {
				c.FetchBlob(ctx, blobRepo, p.Name+".bitmap", filepath.Join(packDir, p.Name+".bitmap"))
			}
			errs <- nil
		}(p)
	}
	for range missing {
		if err := <-errs; err != nil {
			return err
		}
	}
	// A multi-pack-index requires every pack's .pack locally; skip it when any
	// pack is served remotely (remote repos don't use git plumbing for reads).
	if !hadRemote {
		writeMidx(ctx, dir, len(packs))
	}
	return nil
}

// writeMidx builds a multi-pack-index over the repo's packs so object
// lookups are one binary search across all packs instead of a linear scan
// of every .idx - the "avoid O(n) index lookups across hundreds of
// packfiles" optimization (Cursor's git-at-any-scale; also stock Git
// guidance). Pointless for a single pack, best-effort, and git reads
// correctly without it (a stale or missing midx just costs a fallback
// scan), so failures are swallowed.
func writeMidx(ctx context.Context, dir string, packCount int) {
	if packCount < 2 {
		return
	}
	cmd := exec.CommandContext(ctx, "git", "-C", dir, "multi-pack-index", "write")
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	if out, err := cmd.CombinedOutput(); err != nil {
		slog.Debug("multi-pack-index write", "dir", dir, "err", err, "out", string(out))
	}
}

// FetchBlob downloads one store blob to dest atomically (tmp+rename).
func (c *Cache) FetchBlob(ctx context.Context, repoID, name, dest string) error {
	rc, err := c.Blobs.Get(ctx, repoID, name)
	if err != nil {
		return err
	}
	defer rc.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, rc); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dest)
}

// SyncRefs makes the cache's loose refs exactly mirror the DB. Writes are
// atomic per-ref (tmp+rename) so a concurrent reader sees old or new, never
// a torn ref. Loose refs (not packed-refs) so git peels annotated tags itself.
func (c *Cache) SyncRefs(ctx context.Context, repoID, dir string) error {
	want := map[string]string{}
	refs, err := c.DB.ListRefs(ctx, repoID)
	if err != nil {
		return err
	}
	for _, r := range refs {
		want[r.Name] = r.Target
	}

	refRoot := filepath.Join(dir, "refs")
	// Delete stale loose refs.
	err = filepath.WalkDir(refRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if strings.HasSuffix(name, ".lock") {
			return nil
		}
		if _, ok := want[name]; !ok {
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// packed-refs could shadow deleted refs; the cache never uses it.
	if err := os.Remove(filepath.Join(dir, "packed-refs")); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Write current refs.
	for name, target := range want {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if cur, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(cur)) == target {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := WriteFileAtomic(path, []byte(target+"\n")); err != nil {
			return err
		}
	}
	return nil
}

// WriteFileAtomic writes via tmp+rename in the destination directory.
func WriteFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func (c *Cache) touch(repoID string) { c.lastAccess.Store(repoID, time.Now()) }
