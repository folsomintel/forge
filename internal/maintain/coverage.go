package maintain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

// Swap coverage: may a consolidation swap commit although refs moved while
// the gc pack was being built?
//
// After the swap the pack list is G (the gc pack, = closure(S) for the
// snapshot refs S) plus N (every pack added after the snapshot; the swap
// only removes snapshot packs). Every current ref tip t must have
// closure(t) within G+N:
//
//   - t unchanged since S, or deleted: trivially fine.
//   - t moved: rev-list --objects t --not S, run against a scratch repo
//     whose object store is exactly G+N, walks every object reachable from
//     t but not from S and fails on the first one absent. Objects reachable
//     from S are in G by construction, so success proves closure(t) within
//     G+N. This is git's own connectivity check (check_connected), aimed at
//     the post-swap pack set instead of the live one.
//
// The common agent shape - force-push a new commit on top of a base that
// some ref in S reaches - walks only the pushed objects. A tip that needs
// an object only a superseded pack holds (a resurrected commit, a push
// whose client still believed in a since-rewritten ref) fails the check and
// the swap is skipped, exactly as before.
type coverage struct {
	p      *Pipeline
	st     *RepoState
	dir    string            // scratch repo: G + N only
	packs  string            // dir/objects/pack
	gc     string            // G's pack name
	old    map[string]bool   // snapshot packs the swap removes
	linked map[string]bool   // N packs present in the scratch repo
	snap   map[string]string // snapshot ref -> target
	proven map[string]bool   // tips already proven covered
}

func (p *Pipeline) newCoverage(st *RepoState, built *builtPack, old map[string]bool) (*coverage, error) {
	root, err := filepath.Abs(st.Dir)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(root, "verify-*")
	if err != nil {
		return nil, err
	}
	c := &coverage{p: p, st: st, dir: dir, packs: filepath.Join(dir, "objects", "pack"),
		gc: built.name, old: old, linked: map[string]bool{},
		snap: tipsOf(st.Refs), proven: map[string]bool{}}
	fail := func(err error) (*coverage, error) {
		c.close()
		return nil, err
	}
	for _, d := range []string{"objects/pack", "objects/info", "refs"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return fail(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644); err != nil {
		return fail(err)
	}
	qdir, err := filepath.Abs(built.qdir)
	if err != nil {
		return fail(err)
	}
	for _, ext := range []string{".pack", ".idx"} {
		if err := os.Symlink(filepath.Join(qdir, "pack-"+built.hash+ext), filepath.Join(c.packs, built.name+ext)); err != nil {
			return fail(err)
		}
	}
	return c, nil
}

func (c *coverage) close() { os.RemoveAll(c.dir) }

// check proves every current ref covered by G+N, or returns why not. Tips
// proven by an earlier call stay proven: N only grows.
func (c *coverage) check(ctx context.Context) error {
	refs, err := c.p.DB.ListRefs(ctx, c.st.RepoID)
	if err != nil {
		return err
	}
	var tips []string
	for _, r := range refs {
		if r.Target != c.snap[r.Name] && !c.proven[r.Target] {
			tips = append(tips, r.Target)
		}
	}
	if len(tips) == 0 {
		return nil
	}
	// Packs AFTER refs: a push records its pack before moving the ref, so
	// every pack a tip above needs is in this listing.
	packs, err := c.p.DB.ListPacks(ctx, c.st.RepoID)
	if err != nil {
		return err
	}
	for _, pk := range packs {
		if c.old[pk.Name] || pk.Name == c.gc || c.linked[pk.Name] {
			continue
		}
		if err := c.link(ctx, pk); err != nil {
			return fmt.Errorf("stage %s: %w", pk.Name, err)
		}
		c.linked[pk.Name] = true
	}
	var in strings.Builder
	for _, t := range tips {
		in.WriteString(t + "\n")
	}
	for _, t := range c.snap {
		in.WriteString("^" + t + "\n")
	}
	// GIT_DIR/GIT_OBJECT_DIRECTORY pin the scratch store: the scratch dir
	// sits inside the cache repo, and discovery must never fall through to
	// it (it still holds the superseded packs).
	x := &gitcmd.Exec{Ctx: ctx, Dir: c.dir, Env: []string{
		"GIT_DIR=" + c.dir, "GIT_OBJECT_DIRECTORY=" + filepath.Join(c.dir, "objects")}}
	if _, err := x.RunIn(strings.NewReader(in.String()), "rev-list", "--objects", "--quiet", "--stdin"); err != nil {
		return err
	}
	for _, t := range tips {
		c.proven[t] = true
	}
	return nil
}

// link places one post-snapshot pack into the scratch store: a symlink to
// the cache copy when it is local (pushes install theirs), else a download.
func (c *coverage) link(ctx context.Context, pk repodb.Pack) error {
	local, err := filepath.Abs(filepath.Join(c.st.Dir, "objects", "pack", pk.Name))
	if err != nil {
		return err
	}
	_, perr := os.Stat(local + ".pack")
	_, ierr := os.Stat(local + ".idx")
	if perr == nil && ierr == nil {
		for _, ext := range []string{".pack", ".idx"} {
			if err := os.Symlink(local+ext, filepath.Join(c.packs, pk.Name+ext)); err != nil {
				return err
			}
		}
		return nil
	}
	blobRepo := pk.BlobRepo
	if blobRepo == "" {
		blobRepo = c.st.RepoID
	}
	for _, ext := range []string{".pack", ".idx"} { // .idx last: git finds packs by it
		if err := c.p.Cache.FetchBlob(ctx, blobRepo, pk.Name+ext, filepath.Join(c.packs, pk.Name+ext)); err != nil {
			return err
		}
	}
	return nil
}
