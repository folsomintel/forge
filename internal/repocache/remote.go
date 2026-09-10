package repocache

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/folsomintel/forge/internal/packstore"
	"github.com/folsomintel/forge/internal/repodb"
)

// Remote-placement serving (the walgit remote reader): a repo whose large
// packs exceed RemoteBytes keeps only their .idx local (plus a "<pack>.remote"
// sidecar holding the pack's byte size); object reads are served straight from
// the bucket in blocks via packstore, so a repo bigger than local disk still
// serves reads. Enabled only when Blocks and RemoteBytes are set.

const remoteSidecarExt = ".remote"

// writeRemoteSidecar records a remotely-served pack's size next to its .idx,
// so the serving path knows the blob size without a DB or store round trip.
func (c *Cache) writeRemoteSidecar(packDir string, p repodb.Pack) {
	_ = WriteFileAtomic(filepath.Join(packDir, p.Name+remoteSidecarExt), []byte(strconv.FormatInt(p.SizeBytes, 10)))
}

// hydrateIdxOnly fetches just the .idx of each remote pack (concurrently) and
// writes its size sidecar. The big .pack is never downloaded.
func (c *Cache) hydrateIdxOnly(ctx context.Context, repoID, packDir string, packs []repodb.Pack) error {
	sem := make(chan struct{}, 4)
	errs := make(chan error, len(packs))
	for _, p := range packs {
		sem <- struct{}{}
		go func(p repodb.Pack) {
			defer func() { <-sem }()
			blobRepo := p.BlobRepo
			if blobRepo == "" {
				blobRepo = repoID
			}
			if err := c.FetchBlob(ctx, blobRepo, p.Name+".idx", filepath.Join(packDir, p.Name+".idx")); err != nil {
				errs <- err
				return
			}
			c.writeRemoteSidecar(packDir, p)
			errs <- nil
		}(p)
	}
	for range packs {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

// Blob keys of the primary-published history pack (mirror of
// maintain.historyPackBlob/historyIdxBlob; repocache can't import maintain).
const (
	historyPackKey = "meta/history.pack"
	historyIdxKey  = "meta/history.idx"
)

// hydrateHistoryPack fetches the blobless history pack (commits+trees) into
// <dir>/meta-history/ so a remote replica serves history locally. Best-effort:
// a repo may not publish one (BuildHistory off on the primary), in which case
// history is served from the remote gc pack like any other object.
func (c *Cache) hydrateHistoryPack(ctx context.Context, repoID, dir string) {
	base := filepath.Join(dir, historyDir)
	if err := os.MkdirAll(base, 0o755); err != nil {
		return
	}
	// Always refresh (this runs only when the repo changed, i.e. syncPacks was
	// not on its up-to-date fast path): the history blob is republished each
	// maintenance cycle. FetchBlob is atomic (temp+rename). .pack before .idx
	// (the serving path gates on the .idx).
	if err := c.FetchBlob(ctx, repoID, historyPackKey, filepath.Join(base, "history.pack")); err != nil {
		os.Remove(filepath.Join(base, "history.idx")) // no history published; don't serve a stale one
		os.Remove(filepath.Join(base, "history.pack"))
		return
	}
	c.FetchBlob(ctx, repoID, historyIdxKey, filepath.Join(base, "history.idx"))
}

// remoteEntry pairs one remotely-served pack's index with its block reader.
type remoteEntry struct {
	blobRepo string
	name     string
	idx      *packstore.Index
	reader   *packstore.ObjectReader
}

// remoteSet is the cached set of a repo's remotely-served packs.
type remoteSet struct {
	entries []remoteEntry
}

// remoteSetFor returns the repo's remote serving set (nil if the repo has no
// remotely-served packs). Cached until the pack layout changes (syncPacks
// clears it). Building parses each remote pack's local .idx (cached) and wires
// a block reader over its bucket .pack.
func (c *Cache) remoteSetFor(repoID, dir string) *remoteSet {
	if v, ok := c.remoteSets.Load(repoID); ok {
		if v == nil {
			return nil
		}
		return v.(*remoteSet)
	}
	rs := c.buildRemoteSet(repoID, dir)
	if rs == nil {
		c.remoteSets.Store(repoID, (*remoteSet)(nil))
		return nil
	}
	c.remoteSets.Store(repoID, rs)
	return rs
}

func (c *Cache) buildRemoteSet(repoID, dir string) *remoteSet {
	if c.Blocks == nil {
		return nil
	}
	packDir := filepath.Join(dir, "objects", "pack")
	ents, err := os.ReadDir(packDir)
	if err != nil {
		return nil
	}
	var rs remoteSet
	// History pack (Phase 3): commits+trees kept LOCAL, tried first, so
	// refs/log/tree/web-UI reads never touch the bucket - only blob reads fall
	// through to the remote gc pack below. Best-effort: absent = fall back to
	// serving history from the remote pack too.
	if hp := c.openHistoryPack(dir); hp != nil {
		rs.entries = append(rs.entries, *hp)
	}
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), remoteSidecarExt)
		if !ok {
			continue
		}
		sizeRaw, err := os.ReadFile(filepath.Join(packDir, e.Name()))
		if err != nil {
			continue
		}
		size, err := strconv.ParseInt(strings.TrimSpace(string(sizeRaw)), 10, 64)
		if err != nil || size <= 0 {
			continue
		}
		idx, err := c.parseIdx(packDir, name)
		if err != nil {
			continue
		}
		// blob prefix: a fork's packs live under the owner's prefix. The
		// sidecar sits with the .idx; the .pack blob may be owned elsewhere.
		blobRepo := repoID
		reader := packstore.Open(context.Background(), c.Blobs, c.Blocks, blobRepo, name+".pack", size)
		rs.entries = append(rs.entries, remoteEntry{
			blobRepo: blobRepo, name: name, idx: idx,
			reader: packstore.NewObjectReader(reader, reader.Size(), idx),
		})
	}
	if len(rs.entries) == 0 {
		return nil
	}
	return &rs
}

// historyDir is where a remote replica keeps the local blobless history pack
// (commits+trees). Kept OUT of objects/pack so a stray git process never sees
// a pack whose gc sibling's data is absent.
const historyDir = "meta-history"

// openHistoryPack returns an ObjectReader over the local history pack, or nil
// if it isn't present. The pack (commits+trees only) is small, so it's read
// wholly into memory (a bytes.Reader io.ReaderAt) - no long-lived fd to leak
// across remote-set rebuilds.
func (c *Cache) openHistoryPack(dir string) *remoteEntry {
	base := filepath.Join(dir, historyDir)
	idxBytes, err := os.ReadFile(filepath.Join(base, "history.idx"))
	if err != nil {
		return nil
	}
	idx, err := packstore.ParseIndex(idxBytes)
	if err != nil {
		return nil
	}
	packBytes, err := os.ReadFile(filepath.Join(base, "history.pack"))
	if err != nil {
		return nil
	}
	return &remoteEntry{
		name:   "history",
		idx:    idx,
		reader: packstore.NewObjectReader(bytes.NewReader(packBytes), int64(len(packBytes)), idx),
	}
}

// parseIdx returns the parsed (immutable, content-addressed) index for a pack,
// caching it across reads.
func (c *Cache) parseIdx(packDir, name string) (*packstore.Index, error) {
	if v, ok := c.idxCache.Load(name); ok {
		return v.(*packstore.Index), nil
	}
	data, err := os.ReadFile(filepath.Join(packDir, name+".idx"))
	if err != nil {
		return nil, err
	}
	idx, err := packstore.ParseIndex(data)
	if err != nil {
		return nil, err
	}
	c.idxCache.Store(name, idx)
	return idx, nil
}

// remoteObject serves an object from the repo's remotely-served packs, or
// ok=false if the repo has none / the object isn't in them.
func (rs *remoteSet) object(oid string) (typ string, data []byte, ok bool, err error) {
	for i := range rs.entries {
		if _, hit := rs.entries[i].idx.Offset(oid); hit {
			t, d, e := rs.entries[i].reader.Object(oid)
			return t, d, true, e
		}
	}
	return "", nil, false, nil
}
