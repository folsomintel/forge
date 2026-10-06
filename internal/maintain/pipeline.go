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

// deleteParallelism bounds concurrent store DELETEs of superseded blobs.
const deleteParallelism = 8

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

	// MinInterval paces threshold-gated runs per repo (see pace.go); 0
	// disables pacing. QuietAfter lets the worker consolidate a repo below
	// MinPacks once it has been push-quiet that long; 0 disables.
	MinInterval time.Duration
	QuietAfter  time.Duration

	// BuildHistory publishes a blobless commits+trees "history pack" each
	// maintenance run, for remote-placement replicas to serve history locally
	// (walgit Phase 3). Off by default; enable on instances whose replicas use
	// remote placement.
	BuildHistory bool

	inflight sync.Map // repoID -> struct{}: single-flight for async runs
	pace     sync.Map // repoID -> *repoPace

	// afterBuild, when set (tests only), runs between the unlocked build
	// and the swap - the window in which concurrent pushes move refs.
	afterBuild func()
}

type Report struct {
	Before    int      `json:"packs_before"`
	After     int      `json:"packs_after"`
	Swept     int      `json:"orphans_swept"`
	NewSize   int64    `json:"new_pack_bytes"`
	Artifacts []string `json:"artifacts,omitempty"`
}

// Run executes the pipeline for one repo. minPacks <= 1 forces consolidation
// regardless of pack count; larger values make it threshold-gated and
// subject to per-repo pacing (a paced call is a no-op).
func (p *Pipeline) Run(ctx context.Context, repoID string, minPacks int) (*Report, error) {
	// Single-flight per repo: two concurrent runs would build duplicate
	// packs, and it is also what makes the lock-free build window below
	// safe - only pushes (which are additive) can then run alongside a
	// build, never a second consolidation. A run already in flight makes
	// this call a no-op.
	if _, busy := p.inflight.LoadOrStore(repoID, struct{}{}); busy {
		return &Report{}, nil
	}
	defer func() {
		p.inflight.Delete(repoID)
		// Pushes that landed during this run had their nudges dropped
		// (single-flight); re-evaluate once for them, or their packs would
		// wait for the next push or worker tick.
		if p.takePending(repoID) {
			p.NudgeIfNeeded(context.Background(), repoID)
		}
	}()
	if minPacks > 1 && p.paced(repoID) {
		return &Report{}, nil
	}
	start := time.Now()
	res, err := p.run(ctx, repoID, minPacks)
	p.ran(repoID, start, err)
	return res, err
}

func (p *Pipeline) run(ctx context.Context, repoID string, minPacks int) (*Report, error) {
	// The write lock is held only to snapshot and materialize: it waits out
	// in-flight git-path pushes (they hold the read lock), so the snapshot
	// includes them. Packs, then refs: a push records its pack before moving
	// a ref, so a snapshot ref whose pack missed the pack snapshot points
	// into a post-snapshot pack, which the swap keeps. Materialize runs
	// AFTER the snapshot so everything the snapshot refs reach is local for
	// the build.
	lock := p.Cache.Lock(repoID)
	lock.Lock()
	packs, refs, dir, err := p.snapshot(ctx, repoID)
	lock.Unlock()
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
		built, err := p.buildConsolidated(ctx, st, res)
		if err != nil {
			return nil, err
		}
		if built != nil {
			defer os.RemoveAll(built.qdir)
			if p.afterBuild != nil {
				p.afterBuild()
			}
			superseded, ok, err := p.swapConsolidated(ctx, st, packs, built, res)
			if err != nil {
				return nil, err
			}
			if ok {
				// gc pack + whatever pushes added since the snapshot.
				if after, err := p.DB.ListPacks(ctx, repoID); err == nil {
					res.After = len(after)
				}
				// Everything below runs unlocked. Superseded blobs are out of
				// the pack list, so nothing new will fetch them; derive reads
				// only the gc pack (closure(st.Refs)) and advertise/sweep only
				// write atomically-renamed files or store blobs.
				p.deleteSuperseded(ctx, repoID, superseded)
				res.Artifacts = p.deriveAll(ctx, st)
			}
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

// snapshot reads the pack list and refs, then materializes. Caller holds
// the repo write lock.
func (p *Pipeline) snapshot(ctx context.Context, repoID string) ([]repodb.Pack, []repodb.Ref, string, error) {
	packs, err := p.DB.ListPacks(ctx, repoID)
	if err != nil {
		return nil, nil, "", err
	}
	refs, err := p.DB.ListRefs(ctx, repoID)
	if err != nil {
		return nil, nil, "", err
	}
	dir, err := p.Cache.Materialize(ctx, repoID)
	return packs, refs, dir, err
}

// Nudge forces one asynchronous maintenance run for the repo (single-flight;
// a duplicate nudge while one is running is dropped). Used after imports and
// wherever a repo should become optimally readable right now.
func (p *Pipeline) Nudge(repoID string) {
	p.runAsync(repoID, 1)
}

// NudgeIfNeeded checks the pack count and, if it crossed the threshold,
// kicks an asynchronous threshold-gated run. Called after every push, so it
// consults the pacing window and a cached per-repo pack estimate first and
// lists packs only when a run could actually start; the periodic worker
// remains the safety net.
func (p *Pipeline) NudgeIfNeeded(ctx context.Context, repoID string) {
	if p.MinPacks <= 0 || !p.mayNeed(repoID) {
		return
	}
	packs, err := p.DB.ListPacks(ctx, repoID)
	if err != nil {
		return
	}
	p.observed(repoID, len(packs))
	if len(packs) < p.MinPacks {
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

// swapConsolidated installs the built pack in place of the snapshot's packs:
// prove every current ref stays covered, swap the pack list, move the blobs
// into the cache. Only the swap itself holds the repo write lock; deleting
// the superseded blobs (returned: the removed packs whose blobs this repo
// owns) is the caller's job, after the lock is released. ok=false with no
// error means the swap was skipped; the next cycle retries.
//
// Concurrent pushes only ADD packs, so every pack in oldPacks still exists
// and packs added after the snapshot are never removed: after the swap the
// pack list is gc + post-snapshot packs. The gc pack holds closure(st.Refs);
// a ref that moved since (a force-push, an ephemeral PR head) may reach
// objects outside it, which is safe only if they are in the post-snapshot
// packs - otherwise deleting oldNames would strand them (repo corruption).
// coverage (coverage.go) proves that before anything is committed.
func (p *Pipeline) swapConsolidated(ctx context.Context, st *RepoState, oldPacks []repodb.Pack, built *builtPack, res *Report) (superseded []string, ok bool, err error) {
	name, hash, exts := built.name, built.hash, built.exts

	// Re-check existence: DELETE /repos ran during the unlocked window.
	if _, err := p.DB.GetRepo(ctx, st.RepoID); err != nil {
		return nil, false, nil // repo deleted mid-build; drop the built pack (swept later)
	}

	// Never delete a name we just (re)inserted.
	old := map[string]bool{}
	oldNames := []string{}
	ownedOld := []string{}
	for _, op := range oldPacks {
		if op.Name == name {
			continue
		}
		old[op.Name] = true
		oldNames = append(oldNames, op.Name)
		if op.BlobRepo == "" {
			ownedOld = append(ownedOld, op.Name) // shared fork blobs stay with their owner
		}
	}

	cov, err := p.newCoverage(st, built, old)
	if err != nil {
		return nil, false, err
	}
	defer cov.close()
	// The bulk of the proof (refs that moved during the build) runs
	// unlocked; under the lock only refs that moved again since need it.
	if err := cov.check(ctx); err != nil {
		slog.Info("consolidate: moved ref not covered, retrying next cycle", "repo", st.RepoID, "err", err)
		return nil, false, nil
	}

	lock := p.Cache.Lock(st.RepoID)
	lock.Lock()
	locked := time.Now()
	defer func() {
		lock.Unlock()
		slog.Info("consolidate: swap", "repo", st.RepoID, "lock_held", time.Since(locked),
			"swapped", ok, "superseded", len(oldNames))
	}()
	// The write lock excludes git-path pushes and materializations; the
	// pack fence excludes in-flight fast-path pushes, which may be waiting
	// on this lock while holding the fence - so never block on it here.
	fence := p.Cache.PackFence(st.RepoID)
	if !tryLockFor(fence, fenceWait) {
		slog.Info("consolidate: pushes in flight, retrying next cycle", "repo", st.RepoID)
		return nil, false, nil
	}
	defer fence.Unlock()
	// Fast-path pushes that committed since the build: re-prove now.
	if err := cov.check(ctx); err != nil {
		slog.Info("consolidate: moved ref not covered, retrying next cycle", "repo", st.RepoID, "err", err)
		return nil, false, nil
	}
	if err := p.DB.ReplacePacks(ctx, st.RepoID, oldNames, []repodb.Pack{{Name: name, SizeBytes: res.NewSize, Source: "gc"}}); err != nil {
		return nil, false, err
	}

	// Update the cache (post-commit only). The multi-pack-index covers the
	// superseded packs; drop it rather than leave lookups probing them.
	packDir := filepath.Join(st.Dir, "objects", "pack")
	for _, ext := range exts {
		if err := os.Rename(filepath.Join(built.qdir, "pack-"+hash+ext), filepath.Join(packDir, name+ext)); err != nil && ext != ".bitmap" {
			return nil, false, err
		}
	}
	for _, o := range oldNames {
		for _, ext := range exts {
			os.Remove(filepath.Join(packDir, o+ext))
		}
	}
	os.Remove(filepath.Join(packDir, "multi-pack-index"))
	return ownedOld, true, nil
}

// fenceWait bounds how long a swap waits for in-flight fast-path pushes.
var fenceWait = 2 * time.Second

// tryLockFor takes l exclusively within d, without queueing as a writer
// (a queued writer would stall new readers behind it).
func tryLockFor(l *sync.RWMutex, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if l.TryLock() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// deleteSuperseded deletes the store blobs of packs a committed swap took
// out of the pack list - bounded-parallel and without the repo lock. The
// local copies are already gone and nothing new resolves these names, so
// this is pure garbage collection; a failed delete is logged and left for
// sweepOrphans.
func (p *Pipeline) deleteSuperseded(ctx context.Context, repoID string, names []string) {
	sem := make(chan struct{}, deleteParallelism)
	var wg sync.WaitGroup
	for _, old := range names {
		sem <- struct{}{}
		wg.Add(1)
		go func(old string) {
			defer func() { <-sem; wg.Done() }()
			// A zero-copy fork may still reference this blob (blob_repo=us);
			// deleting it would orphan the fork. Leave shared blobs for the
			// fork to inherit until it consolidates onto its own prefix.
			if ref, err := p.DB.BlobReferenced(ctx, repoID, old); err != nil {
				slog.Error("blob dependents check", "repo", repoID, "pack", old, "err", err)
				return
			} else if ref {
				return
			}
			for _, ext := range []string{".pack", ".idx", ".bitmap"} {
				if err := p.Blobs.Delete(ctx, repoID, old+ext); err != nil {
					slog.Error("delete superseded pack", "repo", repoID, "pack", old, "err", err)
				}
			}
		}(old)
	}
	wg.Wait()
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
// crossed the threshold - or that went quiet with more than one pack - and
// runs the pipeline for each (paced repos are skipped until their window
// passes).
func (p *Pipeline) Worker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.scan(ctx)
		}
	}
}

// scan is one worker tick.
func (p *Pipeline) scan(ctx context.Context) {
	minPacks := max(p.MinPacks, 2)
	repos, err := p.DB.ReposNeedingCompaction(ctx, minPacks)
	if err != nil {
		slog.Error("maintenance scan", "err", err)
		return
	}
	due := map[string]int{}
	for _, repo := range repos {
		due[repo] = minPacks
	}
	// Quiet pass: a repo below the threshold that has stopped receiving
	// packs still converges to a single gc pack (clone passthrough needs
	// one), once per quiet spell.
	if p.QuietAfter > 0 && minPacks > 2 {
		multi, err := p.DB.ReposNeedingCompaction(ctx, 2)
		if err != nil {
			slog.Error("maintenance scan", "err", err)
		}
		for _, repo := range multi {
			if _, ok := due[repo]; ok || p.paced(repo) {
				continue
			}
			packs, err := p.DB.ListPacks(ctx, repo)
			if err != nil || len(packs) < 2 {
				continue
			}
			if p.quiet(repo, packs) {
				due[repo] = 2
			}
		}
	}
	for repo, threshold := range due {
		if p.paced(repo) {
			continue
		}
		res, err := p.Run(ctx, repo, threshold)
		if err != nil {
			slog.Error("maintain", "repo", repo, "err", err)
			continue
		}
		slog.Info("maintained", "repo", repo, "before", res.Before, "after", res.After,
			"artifacts", res.Artifacts, "swept", res.Swept)
	}
}
