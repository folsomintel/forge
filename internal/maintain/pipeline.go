// Package maintain is the background maintenance pipeline:
//
//	consolidate  rewrite the per-push packs into one gc pack (+ bitmap),
//	             swapped into the pack list atomically. GC here is a pure
//	             read-write-swap of immutable blobs with the DB as arbiter -
//	             never mtime pruning, never git deleting on its own.
//	derive       produce artifacts (artifacts.go) from the consolidated state.
//	advertise    refresh read-path advertisements (bundle capability URL).
//	sweep        delete store blobs orphaned by rejected pushes, after a
//	             grace period.
//
// Objects unreachable from every ref are dropped by consolidation, same as
// git gc. Maintenance is an internal lifecycle concern: it runs from the
// periodic worker and from post-write nudges (import completion, pushes
// crossing the pack threshold) - never as a customer-facing operation.
package maintain

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
)

const orphanGrace = time.Hour

// nudgeTimeout bounds a background maintenance run; a full repack of a
// monorepo-sized repo runs ~12 minutes, so leave generous headroom.
const nudgeTimeout = time.Hour

type Pipeline struct {
	Cache *repocache.Cache
	DB    repodb.DB
	Blobs blobstore.Store

	// Bundle-uri clone offload: when PublicURL is set, maintenance publishes
	// a full-clone bundle and materialization advertises a signed URL to it.
	PublicURL    string
	BundleSecret []byte

	// MinPacks is the pack-count threshold for worker scans and push nudges.
	MinPacks int

	// BuildHistory publishes a blobless commits+trees "history pack" each
	// maintenance run, for remote-placement replicas to serve history locally
	// (walgit Phase 3). Off by default; enable on instances whose replicas use
	// remote placement.
	BuildHistory bool

	inflight sync.Map // repoID -> struct{}: single-flight for async runs
}

type Report struct {
	Before    int      `json:"packs_before"`
	After     int      `json:"packs_after"`
	Swept     int      `json:"orphans_swept"`
	NewSize   int64    `json:"new_pack_bytes"`
	Artifacts []string `json:"artifacts,omitempty"`
}

// Run executes the pipeline for one repo. minPacks <= 1 forces consolidation
// regardless of pack count; larger values make it threshold-gated.
func (p *Pipeline) Run(ctx context.Context, repoID string, minPacks int) (*Report, error) {
	// Single-flight per repo: two concurrent runs would build duplicate
	// packs, and it is also what makes the lock-free build window below
	// safe - only pushes (which are additive) can then run alongside a
	// build, never a second consolidation. A run already in flight makes
	// this call a no-op.
	if _, busy := p.inflight.LoadOrStore(repoID, struct{}{}); busy {
		return &Report{}, nil
	}
	defer p.inflight.Delete(repoID)

	lock := p.Cache.Lock(repoID)
	lock.Lock()
	held := true
	defer func() {
		if held {
			lock.Unlock()
		}
	}()

	dir, err := p.Cache.Materialize(ctx, repoID)
	if err != nil {
		return nil, err
	}
	packs, err := p.DB.ListPacks(ctx, repoID)
	if err != nil {
		return nil, err
	}
	refs, err := p.DB.ListRefs(ctx, repoID)
	if err != nil {
		return nil, err
	}
	res := &Report{Before: len(packs), After: len(packs)}
	st := &RepoState{RepoID: repoID, Dir: dir, Refs: refs}

	// Force mode also (re)builds accelerators for a repo that has a single
	// pack but no bitmap yet.
	force := minPacks <= 1
	needsAccel := false
	if force && len(packs) == 1 {
		_, err := os.Stat(filepath.Join(dir, "objects", "pack", packs[0].Name+".bitmap"))
		needsAccel = err != nil
	}
	if len(refs) > 0 && (len(packs) >= max(minPacks, 2) || needsAccel || (force && len(packs) > 1)) {
		// The expensive part - the rev walk, pack-objects, and the blob
		// uploads - runs WITHOUT the repo lock so concurrent fetches (and
		// pushes) are not blocked behind a big repack. Packs are immutable
		// and only added concurrently, so the snapshot closure stays fully
		// readable throughout; only the atomic pack-list swap needs the lock.
		lock.Unlock()
		held = false
		built, berr := p.buildConsolidated(ctx, st, res)
		lock.Lock()
		held = true
		if berr != nil {
			return nil, berr
		}
		if built != nil {
			if err := p.swapConsolidated(ctx, st, packs, built, res); err != nil {
				return nil, err
			}
			res.After = 1
			res.Artifacts = p.deriveAll(ctx, st)
		}
	}

	p.advertise(ctx, repoID, dir)

	swept, err := p.sweepOrphans(ctx, repoID)
	if err != nil {
		slog.Error("orphan sweep", "repo", repoID, "err", err)
	}
	res.Swept = swept
	return res, nil
}

// Nudge forces one asynchronous maintenance run for the repo (single-flight;
// a duplicate nudge while one is running is dropped). Used after imports and
// wherever a repo should become optimally readable right now.
func (p *Pipeline) Nudge(repoID string) {
	p.runAsync(repoID, 1)
}

// NudgeIfNeeded checks the pack count and, if it crossed the threshold,
// kicks an asynchronous threshold-gated run. Cheap enough to call after
// every push; the periodic worker remains the safety net.
func (p *Pipeline) NudgeIfNeeded(ctx context.Context, repoID string) {
	if p.MinPacks <= 0 {
		return
	}
	packs, err := p.DB.ListPacks(ctx, repoID)
	if err != nil || len(packs) < p.MinPacks {
		return
	}
	p.runAsync(repoID, p.MinPacks)
}

func (p *Pipeline) runAsync(repoID string, minPacks int) {
	// Run self-guards with the same inflight map, so a duplicate nudge while
	// a run is in flight is dropped there (returns a no-op report).
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), nudgeTimeout)
		defer cancel()
		res, err := p.Run(ctx, repoID, minPacks)
		if err != nil {
			slog.Error("maintain (nudged)", "repo", repoID, "err", err)
			return
		}
		slog.Info("maintained (nudged)", "repo", repoID, "before", res.Before,
			"after", res.After, "artifacts", res.Artifacts, "swept", res.Swept)
	}()
}

// builtPack is a consolidated pack staged in qdir and already uploaded to
// the store, awaiting the atomic pack-list swap.
type builtPack struct {
	qdir string
	name string // "pack-<hash>"
	hash string
	exts []string
}

// buildConsolidated packs the full reachable closure (all refs incl.
// ephemeral namespaces) via a rev walk so pack-objects emits a reachability
// bitmap, then uploads pack+idx+bitmap. It runs WITHOUT the repo lock: it
// only reads immutable packs and writes uniquely-named staging/blobs, so a
// concurrent push cannot disturb it. Returns nil when nothing is reachable.
func (p *Pipeline) buildConsolidated(ctx context.Context, st *RepoState, res *Report) (*builtPack, error) {
	qdir, err := os.MkdirTemp(st.Dir, "compact-*")
	if err != nil {
		return nil, err
	}
	x := &gitcmd.Exec{Ctx: ctx, Dir: st.Dir}
	var revs strings.Builder
	for _, r := range st.Refs {
		revs.WriteString(r.Target + "\n")
	}
	base := filepath.Join(qdir, "pack")
	out, err := x.RunIn(strings.NewReader(revs.String()),
		"pack-objects", "-q", "--revs", "--non-empty", "--write-bitmap-index", base)
	if err != nil {
		os.RemoveAll(qdir)
		return nil, err
	}
	hash := strings.TrimSpace(out)
	if hash == "" {
		os.RemoveAll(qdir)
		return nil, nil // nothing reachable
	}
	name := "pack-" + hash
	st.GCPack = name

	// Upload before the swap (readers must never see a pack list whose
	// blobs aren't durable); pack/idx/bitmap ship concurrently.
	exts := []string{".pack", ".idx", ".bitmap"}
	errs := make(chan error, len(exts))
	for _, ext := range exts {
		go func(ext string) {
			f, err := os.Open(base + "-" + hash + ext)
			if err != nil {
				if ext == ".bitmap" {
					errs <- nil // git may skip bitmap generation; not fatal
					return
				}
				errs <- err
				return
			}
			defer f.Close()
			if ext == ".pack" {
				if info, err := f.Stat(); err == nil {
					res.NewSize = info.Size()
				}
			}
			errs <- p.Blobs.Put(ctx, st.RepoID, name+ext, f)
		}(ext)
	}
	for range exts {
		if err := <-errs; err != nil {
			os.RemoveAll(qdir)
			return nil, fmt.Errorf("store %s: %w", name, err)
		}
	}
	return &builtPack{qdir: qdir, name: name, hash: hash, exts: exts}, nil
}

// swapConsolidated installs the built pack as the repo's sole pack: swap the
// pack list, move the blobs into the cache, then delete the superseded ones.
// Caller holds the repo lock. Concurrent pushes only ADD packs, so every
// pack in oldPacks still exists and none of the new push's packs are removed.
func (p *Pipeline) swapConsolidated(ctx context.Context, st *RepoState, oldPacks []repodb.Pack, built *builtPack, res *Report) error {
	defer os.RemoveAll(built.qdir)
	name, hash, exts := built.name, built.hash, built.exts

	// The gc pack contains closure(st.Refs) as snapshotted before the
	// unlocked build. If a ref moved since (a force-push resurrecting an old
	// commit, an ephemeral PR head) it may now reach an object that lives
	// ONLY in a pack we're about to delete and is NOT in the gc pack -
	// deleting oldNames would then strand it (repo corruption). Bail unless
	// the ref set is byte-identical to the snapshot; the next cycle retries.
	// Also re-check existence: DELETE /repos ran during the unlocked window.
	if _, err := p.DB.GetRepo(ctx, st.RepoID); err != nil {
		return nil // repo deleted mid-build; drop the built pack (swept later)
	}
	cur, err := p.DB.ListRefs(ctx, st.RepoID)
	if err != nil {
		return err
	}
	if !sameRefs(cur, st.Refs) {
		slog.Info("consolidate: refs moved during build, retrying next cycle", "repo", st.RepoID)
		return nil
	}

	// Never delete a name we just (re)inserted.
	oldNames := []string{}
	ownedOld := []string{}
	for _, op := range oldPacks {
		if op.Name == name {
			continue
		}
		oldNames = append(oldNames, op.Name)
		if op.BlobRepo == "" {
			ownedOld = append(ownedOld, op.Name) // shared fork blobs stay with their owner
		}
	}
	if err := p.DB.ReplacePacks(ctx, st.RepoID, oldNames, []repodb.Pack{{Name: name, SizeBytes: res.NewSize, Source: "gc"}}); err != nil {
		return err
	}

	// Update the cache, then delete superseded blobs (post-commit only).
	packDir := filepath.Join(st.Dir, "objects", "pack")
	for _, ext := range exts {
		if err := os.Rename(filepath.Join(built.qdir, "pack-"+hash+ext), filepath.Join(packDir, name+ext)); err != nil && ext != ".bitmap" {
			return err
		}
	}
	for _, old := range oldNames {
		for _, ext := range exts {
			os.Remove(filepath.Join(packDir, old+ext))
		}
	}
	for _, old := range ownedOld {
		// A zero-copy fork may still reference this blob (blob_repo=us);
		// deleting it would orphan the fork. Leave shared blobs for the fork
		// to inherit until it consolidates onto its own prefix.
		if ref, err := p.DB.BlobReferenced(ctx, st.RepoID, old); err != nil {
			slog.Error("blob dependents check", "repo", st.RepoID, "pack", old, "err", err)
			continue
		} else if ref {
			continue
		}
		for _, ext := range exts {
			if err := p.Blobs.Delete(ctx, st.RepoID, old+ext); err != nil {
				slog.Error("delete superseded pack", "repo", st.RepoID, "pack", old, "err", err)
			}
		}
	}
	return nil
}

// sweepOrphans deletes store pack blobs that are not in the pack list and
// are older than the grace period - the debris of rejected pushes. Grace
// protects in-flight pushes (their packs upload before the ref CAS).
func (p *Pipeline) sweepOrphans(ctx context.Context, repoID string) (int, error) {
	blobs, err := p.Blobs.List(ctx, repoID, "")
	if err != nil {
		return 0, err
	}
	packs, err := p.DB.ListPacks(ctx, repoID)
	if err != nil {
		return 0, err
	}
	known := map[string]bool{}
	for _, pk := range packs {
		known[pk.Name] = true
	}
	swept := 0
	cutoff := time.Now().Add(-orphanGrace)
	for _, b := range blobs {
		// Abandoned staged wire packs (client aborts, standalone-hook
		// pushes) age out here as the backstop behind the stager's TTL.
		if strings.HasPrefix(b.Name, "staged/") {
			if b.ModTime.Before(cutoff) {
				if err := p.Blobs.Delete(ctx, repoID, b.Name); err != nil {
					return swept, err
				}
				swept++
			}
			continue
		}
		if !strings.HasPrefix(b.Name, "pack-") {
			continue // artifacts and LFS objects are not ours to sweep here
		}
		name := b.Name
		for _, ext := range []string{".pack", ".idx", ".bitmap"} {
			name = strings.TrimSuffix(name, ext)
		}
		if known[name] || !b.ModTime.Before(cutoff) {
			continue
		}
		// Never reap a blob a fork still points at (blob_repo=repoID).
		if ref, err := p.DB.BlobReferenced(ctx, repoID, name); err != nil {
			return swept, err
		} else if ref {
			continue
		}
		if err := p.Blobs.Delete(ctx, repoID, b.Name); err != nil {
			return swept, err
		}
		swept++
	}
	return swept, nil
}

// Worker is the periodic maintainer: scans for repos whose pack count
// crossed the threshold and runs the pipeline for each.
func (p *Pipeline) Worker(ctx context.Context, interval time.Duration) {
	minPacks := max(p.MinPacks, 2)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			repos, err := p.DB.ReposNeedingCompaction(ctx, minPacks)
			if err != nil {
				slog.Error("maintenance scan", "err", err)
				continue
			}
			for _, repo := range repos {
				res, err := p.Run(ctx, repo, minPacks)
				if err != nil {
					slog.Error("maintain", "repo", repo, "err", err)
					continue
				}
				slog.Info("maintained", "repo", repo, "before", res.Before, "after", res.After,
					"artifacts", res.Artifacts, "swept", res.Swept)
			}
		}
	}
}

// sameRefs reports whether two ref lists are identical as name->target
// maps (order-independent). Used to abort a consolidation whose gc pack was
// built from a now-stale ref snapshot.
func sameRefs(a, b []repodb.Ref) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]string, len(a))
	for _, r := range a {
		m[r.Name] = r.Target
	}
	for _, r := range b {
		if m[r.Name] != r.Target {
			return false
		}
	}
	return true
}
