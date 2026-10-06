package maintain

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
)

// fixture is a primary with no HTTP front: a client work repo whose commits
// are "pushed" the way the fast push path does it - a self-contained pack (externals
// allowed, thin bases not) put to the store, a pack row, then the ref CAS.
type fixture struct {
	t     *testing.T
	ctx   context.Context
	dir   string
	db    repodb.DB
	blobs blobstore.Store
	p     *Pipeline
	work  string
	omit  map[string]bool // objects push leaves out (as a client that believes the server has them)
}

const repo = "demo"

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	sq, err := repodb.OpenSQLite(filepath.Join(dir, "forge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sq.Close() })
	blobs, err := blobstore.NewLocal(filepath.Join(dir, "packs"))
	if err != nil {
		t.Fatal(err)
	}
	db := repodb.NewWAL(sq, blobs)
	f := &fixture{t: t, ctx: context.Background(), dir: dir, db: db, blobs: blobs,
		work: filepath.Join(dir, "work")}
	f.p = &Pipeline{Cache: repocache.New(filepath.Join(dir, "cache"), db, blobs), DB: db, Blobs: blobs}
	if err := db.CreateRepo(f.ctx, repo, "main"); err != nil {
		t.Fatal(err)
	}
	f.git(dir, "init", "-q", "-b", "main", f.work)
	return f
}

func (f *fixture) git(dir string, args ...string) string {
	f.t.Helper()
	return f.gitIn(dir, nil, args...)
}

func (f *fixture) gitIn(dir string, stdin *strings.Reader, args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	if stdin != nil {
		cmd.Stdin = stdin
	}
	out, err := cmd.Output()
	if err != nil {
		f.t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

// commit writes files in the work repo and commits them; returns the oid.
func (f *fixture) commit(msg string, files map[string]string) string {
	f.t.Helper()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(f.work, name), []byte(body), 0o644); err != nil {
			f.t.Fatal(err)
		}
	}
	f.git(f.work, "add", "-A")
	f.git(f.work, "commit", "-qm", msg)
	return f.git(f.work, "rev-parse", "HEAD")
}

// push stores a pack of tip's objects minus everything reachable from
// exclude, records it, and CASes ref to tip.
func (f *fixture) push(ref, tip string, exclude ...string) string {
	f.t.Helper()
	revs := tip + "\n"
	for _, e := range exclude {
		revs += "^" + e + "\n"
	}
	var objs strings.Builder
	for _, line := range strings.Split(f.gitIn(f.work, strings.NewReader(revs), "rev-list", "--objects", "--stdin"), "\n") {
		if oid, _, _ := strings.Cut(line, " "); oid != "" && !f.omit[oid] {
			objs.WriteString(oid + "\n")
		}
	}
	base := filepath.Join(f.t.TempDir(), "p")
	hash := f.gitIn(f.work, strings.NewReader(objs.String()), "pack-objects", "-q", base)
	name := "pack-" + hash
	var size int64
	for _, ext := range []string{".pack", ".idx"} {
		fh, err := os.Open(base + "-" + hash + ext)
		if err != nil {
			f.t.Fatal(err)
		}
		if ext == ".pack" {
			st, _ := fh.Stat()
			size = st.Size()
		}
		err = f.blobs.Put(f.ctx, repo, name+ext, fh)
		fh.Close()
		if err != nil {
			f.t.Fatal(err)
		}
	}
	if err := f.db.AddPacks(f.ctx, repo, []repodb.Pack{{Name: name, SizeBytes: size, Source: "receive"}}); err != nil {
		f.t.Fatal(err)
	}
	f.setRef(ref, tip)
	return name
}

func (f *fixture) setRef(ref, target string) {
	f.t.Helper()
	old := repodb.ZeroOID
	refs, _ := f.db.ListRefs(f.ctx, repo)
	for _, r := range refs {
		if r.Name == ref {
			old = r.Target
		}
	}
	if err := f.db.UpdateRefs(f.ctx, repo, []repodb.RefUpdate{{Name: ref, Old: old, New: target}}, nil); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) packNames() map[string]bool {
	f.t.Helper()
	packs, err := f.db.ListPacks(f.ctx, repo)
	if err != nil {
		f.t.Fatal(err)
	}
	m := map[string]bool{}
	for _, p := range packs {
		m[p.Name] = true
	}
	return m
}

// assertIntact materializes the repo from store+DB alone into a fresh cache
// and proves every ref's full closure is present.
func (f *fixture) assertIntact() {
	f.t.Helper()
	c := repocache.New(filepath.Join(f.t.TempDir(), "cache"), f.db, f.blobs)
	dir, err := c.Materialize(f.ctx, repo)
	if err != nil {
		f.t.Fatalf("materialize: %v", err)
	}
	f.git(dir, "fsck", "--connectivity-only", "--no-dangling")
}

// A ref force-pushed while the gc pack is being built no longer livelocks
// consolidation: the new tip's pack is kept beside the gc pack and its
// externals are in the gc pack.
func TestSwapSucceedsDespiteConcurrentRefMove(t *testing.T) {
	f := newFixture(t)
	c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
	p0 := f.push("refs/heads/main", c0)
	f.git(f.work, "checkout", "-qb", "a")
	a1 := f.commit("a1", map[string]string{"a.txt": "a1\n"})
	p1 := f.push("refs/heads/a", a1, c0)
	a2 := f.commit("a2", map[string]string{"a.txt": "a2\n"})
	p2 := f.push("refs/heads/a", a2, a1)

	var a3, p3 string
	f.p.afterBuild = func() {
		// The agent shape: rewrite the branch on top of the base.
		f.git(f.work, "reset", "-q", "--hard", c0)
		a3 = f.commit("a3", map[string]string{"a.txt": "a3\n"})
		p3 = f.push("refs/heads/a", a3, c0)
	}
	res, err := f.p.Run(f.ctx, repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Before != 3 || res.After != 2 {
		t.Fatalf("report %+v, want before=3 after=2 (gc + concurrent push)", res)
	}
	got := f.packNames()
	for _, gone := range []string{p0, p1, p2} {
		if got[gone] {
			t.Fatalf("superseded pack %s still listed: %v", gone, got)
		}
		if _, err := os.Stat(filepath.Join(f.dir, "packs", repo, gone+".pack")); !os.IsNotExist(err) {
			t.Fatalf("superseded blob %s not deleted", gone)
		}
	}
	if !got[p3] || len(got) != 2 {
		t.Fatalf("pack list after swap: %v (want gc + %s)", got, p3)
	}
	refs, _ := f.db.ListRefs(f.ctx, repo)
	for _, r := range refs {
		if r.Name == "refs/heads/a" && r.Target != a3 {
			t.Fatalf("ref a = %s, want %s", r.Target, a3)
		}
	}
	f.assertIntact()
}

// A ref moved during the build to a tip that needs an object only a
// superseded pack holds must not be swapped (it would strand the object).
func TestSwapBailsWhenMovedRefNeedsSupersededObject(t *testing.T) {
	for _, tc := range []struct {
		name string
		// moved builds the new tip off the now-unreachable u1 and returns
		// it; its pack excludes u1's closure.
		moved func(f *fixture, u1 string) string
	}{
		{"missing parent", func(f *fixture, u1 string) string {
			f.git(f.work, "checkout", "-q", "-B", "v", u1)
			return f.commit("v1", map[string]string{"v.txt": "v\n"})
		}},
		{"missing blob", func(f *fixture, u1 string) string {
			// A root commit (no parent) whose tree keeps u1's blob; the
			// pusher omits the blob, believing the server still has it.
			f.git(f.work, "checkout", "-q", "--orphan", "w", u1)
			f.omit = map[string]bool{f.git(f.work, "rev-parse", u1+":secret.txt"): true}
			return f.commit("w1", map[string]string{"w.txt": "w\n"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
			f.push("refs/heads/main", c0)
			f.git(f.work, "checkout", "-qb", "u")
			u1 := f.commit("u1", map[string]string{"secret.txt": "only in a superseded pack\n"})
			f.push("refs/heads/u", u1, c0)
			// Branch deleted: u1 and its blob are now unreachable, so the
			// gc pack will not contain them.
			if err := f.db.UpdateRefs(f.ctx, repo, []repodb.RefUpdate{{Name: "refs/heads/u", Old: u1, New: repodb.ZeroOID}}, nil); err != nil {
				t.Fatal(err)
			}
			before := f.packNames()

			var vp string
			f.p.afterBuild = func() {
				tip := tc.moved(f, u1)
				vp = f.push("refs/heads/moved", tip, u1)
			}
			res, err := f.p.Run(f.ctx, repo, 1)
			if err != nil {
				t.Fatal(err)
			}
			got := f.packNames()
			for name := range before {
				if !got[name] {
					t.Fatalf("pack %s removed despite uncovered ref (%+v)", name, res)
				}
			}
			if !got[vp] || len(got) != len(before)+1 {
				t.Fatalf("pack list changed: %v", got)
			}
			f.assertIntact()
		})
	}
}

// lockProbe records whether the repo write lock was held during store
// DELETEs.
type lockProbe struct {
	blobstore.Store
	lock          *sync.RWMutex
	deletes, held atomic.Int32
}

func (s *lockProbe) Delete(ctx context.Context, repoID, name string) error {
	if s.lock.TryLock() {
		s.lock.Unlock()
	} else {
		s.held.Add(1)
	}
	s.deletes.Add(1)
	return s.Store.Delete(ctx, repoID, name)
}

func TestSupersededDeletesRunOutsideLock(t *testing.T) {
	f := newFixture(t)
	probe := &lockProbe{Store: f.blobs, lock: f.p.Cache.Lock(repo)}
	f.p.Blobs = probe
	c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
	f.push("refs/heads/main", c0)
	prev := c0
	for i := range 4 {
		c := f.commit("c", map[string]string{"f.txt": strings.Repeat("x", i+1)})
		f.push("refs/heads/main", c, prev)
		prev = c
	}
	res, err := f.p.Run(f.ctx, repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.After != 1 {
		t.Fatalf("report %+v", res)
	}
	if probe.deletes.Load() < 5*3 {
		t.Fatalf("expected superseded deletes, got %d", probe.deletes.Load())
	}
	if n := probe.held.Load(); n != 0 {
		t.Fatalf("%d deletes ran under the repo write lock", n)
	}
	f.assertIntact()
}

func TestPacingSkipsThresholdRunsWithinInterval(t *testing.T) {
	f := newFixture(t)
	f.p.MinInterval = time.Hour
	f.p.MinPacks = 2
	c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
	f.push("refs/heads/main", c0)
	c1 := f.commit("c1", map[string]string{"f.txt": "1\n"})
	f.push("refs/heads/main", c1, c0)

	if res, err := f.p.Run(f.ctx, repo, 2); err != nil || res.After != 1 {
		t.Fatalf("first run: %+v %v", res, err)
	}
	c2 := f.commit("c2", map[string]string{"f.txt": "2\n"})
	f.push("refs/heads/main", c2, c1)
	c3 := f.commit("c3", map[string]string{"f.txt": "3\n"})
	f.push("refs/heads/main", c3, c2)

	if !f.p.paced(repo) {
		t.Fatal("repo not paced right after a run")
	}
	if f.p.mayNeed(repo) {
		t.Fatal("nudge pre-check passed while paced")
	}
	res, err := f.p.Run(f.ctx, repo, 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Before != 0 || len(f.packNames()) != 3 {
		t.Fatalf("threshold run inside the pacing window was not skipped: %+v", res)
	}
	// Forced runs (imports, the ops endpoint) are never paced.
	if res, err := f.p.Run(f.ctx, repo, 1); err != nil || res.After != 1 {
		t.Fatalf("forced run: %+v %v", res, err)
	}
	f.assertIntact()
}

func TestNudgeEstimateAvoidsPackListing(t *testing.T) {
	p := &Pipeline{MinPacks: 3}
	if !p.mayNeed(repo) {
		t.Fatal("unknown count must list packs")
	}
	p.observed(repo, 1)
	if p.mayNeed(repo) { // est 2
		t.Fatal("listed packs below the threshold")
	}
	if !p.mayNeed(repo) { // est 3
		t.Fatal("estimate reached the threshold but no listing")
	}
}

// A repo below the (raised) threshold that stops receiving pushes still
// converges to one gc pack via the worker's quiet pass.
func TestWorkerConsolidatesQuietRepo(t *testing.T) {
	f := newFixture(t)
	f.p.MinPacks = 64
	f.p.MinInterval = time.Hour
	f.p.QuietAfter = time.Nanosecond
	c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
	f.push("refs/heads/main", c0)
	c1 := f.commit("c1", map[string]string{"f.txt": "1\n"})
	f.push("refs/heads/main", c1, c0)

	f.p.scan(f.ctx)
	if n := len(f.packNames()); n != 1 {
		t.Fatalf("quiet repo not consolidated: %d packs", n)
	}
	f.assertIntact()
}

// An in-flight fast-path push holds the pack fence from its connectivity
// proof through its ref commit; the swap must not replace the pack list
// under it (it skips the cycle instead of blocking), and swaps normally
// once the push is done.
func TestSwapWaitsOutInFlightFastPush(t *testing.T) {
	f := newFixture(t)
	c0 := f.commit("base", map[string]string{"base.txt": "base\n"})
	f.push("refs/heads/main", c0)
	c1 := f.commit("c1", map[string]string{"base.txt": "c1\n"})
	f.push("refs/heads/main", c1, c0)

	defer func(d time.Duration) { fenceWait = d }(fenceWait)
	fenceWait = 50 * time.Millisecond
	fence := f.p.Cache.PackFence(repo)
	f.p.afterBuild = func() { fence.RLock() }
	res, err := f.p.Run(f.ctx, repo, 1)
	fence.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if res.After != res.Before || len(f.packNames()) != 2 {
		t.Fatalf("swapped under an in-flight push: %+v packs=%v", res, f.packNames())
	}

	f.p.afterBuild = nil
	if _, err := f.p.Run(f.ctx, repo, 1); err != nil {
		t.Fatal(err)
	}
	if n := len(f.packNames()); n != 1 {
		t.Fatalf("packs after the push finished = %d, want 1", n)
	}
	f.assertIntact()
}
