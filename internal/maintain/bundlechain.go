package maintain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

// Bundle chains (the walgit/Cursor pattern, extending our single-bundle
// offload): instead of one full-clone bundle, maintenance publishes a full
// bundle periodically plus small incremental "catchup" bundles between fulls.
// Clients using git's bundle-uri with mode=all + creationToken fetch the full
// once, then only the newer incrementals on later operations - repeat fetches
// stop hitting server compute, and clone/fetch bytes come from the bucket.
//
// Layout under the repo prefix:
//
//	meta/bundles/<token>.bundle   one bundle (full or incremental)
//	meta/bundles/list.json        ordered manifest (source of truth)
//
// A full is synthesized cheaply from the gc pack (header + io.Copy, no
// repack). An incremental is a real `git bundle create <tips> --not
// <prev-tips>` so its prerequisite lines let git apply it on top of the
// chain so far.

// BundlePrefix is the store key prefix for chain bundle blobs (read by the
// signed download endpoint in githttp).
const BundlePrefix = bundlePrefix

const (
	bundlePrefix       = "meta/bundles/"
	bundleManifest     = "meta/bundles/list.json"
	bundleFullInterval = 7 * 24 * time.Hour // cut a fresh full at most weekly
	bundleKeepFulls    = 2                  // fulls to retain (older swept)
	bundleMaxChain     = 32                 // incrementals since last full before forcing a full
)

// bundleEntry is one member of the chain. Tips is the full ref state at cut
// time: it seeds the next incremental's exclude set and detects "no change".
type bundleEntry struct {
	Name  string            `json:"name"`  // "<token>.bundle"
	Token int64             `json:"token"` // creationToken (unix seconds, strictly increasing)
	Full  bool              `json:"full"`
	Tips  map[string]string `json:"tips"` // ref name -> oid
}

type bundleList struct {
	Entries []bundleEntry `json:"entries"` // ascending token order
}

func (l bundleList) last() *bundleEntry {
	if len(l.Entries) == 0 {
		return nil
	}
	return &l.Entries[len(l.Entries)-1]
}

func (l bundleList) incrementalsSinceFull() int {
	n := 0
	for i := len(l.Entries) - 1; i >= 0; i-- {
		if l.Entries[i].Full {
			break
		}
		n++
	}
	return n
}

func (l bundleList) newestFullAge() (time.Duration, bool) {
	for i := len(l.Entries) - 1; i >= 0; i-- {
		if l.Entries[i].Full {
			return time.Since(time.Unix(l.Entries[i].Token, 0)), true
		}
	}
	return 0, false
}

func tipsOf(refs []repodb.Ref) map[string]string {
	m := make(map[string]string, len(refs))
	for _, r := range refs {
		m[r.Name] = r.Target
	}
	return m
}

func sameTips(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// --- the artifact ---

type bundleChain struct{}

func (bundleChain) Kind() string { return "bundle" }

func (bundleChain) Derive(ctx context.Context, p *Pipeline, st *RepoState) error {
	if p.PublicURL == "" || len(p.BundleSecret) == 0 || st.GCPack == "" {
		return nil // not advertising bundles on this instance
	}
	list := p.loadBundleList(ctx, st.RepoID) // best-effort; empty on miss
	tips := tipsOf(st.Refs)

	age, haveFull := list.newestFullAge()
	needFull := !haveFull || age >= bundleFullInterval || list.incrementalsSinceFull() >= bundleMaxChain

	// Nothing to publish if an incremental would be empty (refs unchanged
	// since the last bundle) and we don't owe a fresh full.
	if !needFull {
		if prev := list.last(); prev != nil && sameTips(prev.Tips, tips) {
			return nil
		}
	}

	token := time.Now().Unix()
	if prev := list.last(); prev != nil && token <= prev.Token {
		token = prev.Token + 1 // creationToken must strictly increase
	}
	name := fmt.Sprintf("%d.bundle", token)

	var err error
	if needFull {
		err = p.putFullBundle(ctx, st, name)
	} else {
		err = p.putIncrementalBundle(ctx, st, name, list.last().Tips)
	}
	if err != nil {
		return err
	}

	list.Entries = append(list.Entries, bundleEntry{Name: name, Token: token, Full: needFull, Tips: tips})
	list = p.pruneBundles(ctx, st.RepoID, list)

	if err := p.saveBundleList(ctx, st.RepoID, list); err != nil {
		return err
	}
	// One-time migration cleanup: retire the pre-chain single-bundle blobs
	// (meta/clone.bundle + its marker) now that this repo has a chain. Delete
	// is idempotent, so this is a harmless no-op once they're gone.
	p.Blobs.Delete(ctx, st.RepoID, "meta/clone.bundle")
	p.Blobs.Delete(ctx, st.RepoID, "meta/bundle.ok")
	// Cache the manifest locally as the existence proof + advertise source,
	// and force a fresh advertisement.
	if data, mErr := json.Marshal(list); mErr == nil {
		os.WriteFile(filepath.Join(st.Dir, localBundleManifest), data, 0o644)
	}
	os.Remove(filepath.Join(st.Dir, "bundles.conf"))
	return nil
}

func (bundleChain) Hydrate(ctx context.Context, p *Pipeline, repoID, dir string) {
	// The advertisement (bundles.conf) is written by the advertise stage.
}

// putFullBundle synthesizes a self-contained full bundle from the gc pack:
// header + refs + the pack bytes verbatim (no repack, no prerequisites).
func (p *Pipeline) putFullBundle(ctx context.Context, st *RepoState, name string) error {
	packPath := filepath.Join(st.Dir, "objects", "pack", st.GCPack+".pack")
	pack, err := os.Open(packPath)
	if err != nil {
		return err
	}
	defer pack.Close()

	tmp, err := os.CreateTemp(st.Dir, "bundle-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	var header strings.Builder
	header.WriteString("# v2 git bundle\n")
	refs := append([]repodb.Ref(nil), st.Refs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })
	for _, r := range refs {
		fmt.Fprintf(&header, "%s %s\n", r.Target, r.Name)
	}
	header.WriteString("\n")
	if _, err := tmp.WriteString(header.String()); err != nil {
		return err
	}
	if _, err := io.Copy(tmp, pack); err != nil {
		return err
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		return err
	}
	return p.Blobs.Put(ctx, st.RepoID, bundlePrefix+name, tmp)
}

// putIncrementalBundle builds a real delta bundle: objects reachable from the
// current tips but not from prevTips. git writes the prerequisite (-<oid>)
// header lines so a client that already applied the earlier chain can apply
// this on top. Ref names come from our DB (all start with "refs/", so none
// can be read as an option); negatives are hex oids.
func (p *Pipeline) putIncrementalBundle(ctx context.Context, st *RepoState, name string, prevTips map[string]string) error {
	tmp, err := os.CreateTemp(st.Dir, "bundle-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	x := &gitcmd.Exec{Ctx: ctx, Dir: st.Dir}

	// `git bundle create <refname>` resolves ref NAMES against the cache
	// repo's own refs, which lag the DB (the DB is truth; materialize doesn't
	// rewrite refs once the repo exists). Sync the refs we're about to bundle
	// to DB truth first, or the positive tip can resolve to a stale/older oid
	// and git refuses an "empty" bundle. One batched update-ref.
	var refIn strings.Builder
	for _, r := range st.Refs {
		fmt.Fprintf(&refIn, "update %s %s\n", r.Name, r.Target)
	}
	if _, err := x.RunIn(strings.NewReader(refIn.String()), "update-ref", "--stdin"); err != nil {
		return fmt.Errorf("sync refs before bundle: %w", err)
	}

	// Positive ref names FIRST, then `--not <negatives>`: git's --not negates
	// EVERYTHING after it, so negatives must come last or the positive tips
	// get excluded too (an empty bundle). Ref names are DB-sourced and all
	// start with "refs/", so none can be read as an option; negatives are hex.
	args := []string{"bundle", "create", tmpPath}
	for _, r := range st.Refs {
		args = append(args, r.Name)
	}
	seen := map[string]bool{}
	var negs []string
	for _, oid := range prevTips {
		if oid != repodb.ZeroOID && !seen[oid] {
			seen[oid] = true
			negs = append(negs, oid)
		}
	}
	if len(negs) > 0 {
		args = append(args, "--not")
		args = append(args, negs...)
	}

	if _, err := x.Run(args...); err != nil {
		return fmt.Errorf("git bundle create: %w", err)
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return p.Blobs.Put(ctx, st.RepoID, bundlePrefix+name, f)
}

// pruneBundles keeps the last bundleKeepFulls fulls and every entry newer
// than the oldest retained full, deleting swept bundle blobs. Best-effort:
// a delete failure just leaves a dead blob (swept next time).
func (p *Pipeline) pruneBundles(ctx context.Context, repoID string, list bundleList) bundleList {
	fullIdx := []int{}
	for i, e := range list.Entries {
		if e.Full {
			fullIdx = append(fullIdx, i)
		}
	}
	if len(fullIdx) <= bundleKeepFulls {
		return list
	}
	cut := fullIdx[len(fullIdx)-bundleKeepFulls] // first index to keep
	for _, e := range list.Entries[:cut] {
		p.Blobs.Delete(ctx, repoID, bundlePrefix+e.Name)
	}
	list.Entries = append([]bundleEntry(nil), list.Entries[cut:]...)
	return list
}

func (p *Pipeline) loadBundleList(ctx context.Context, repoID string) bundleList {
	var list bundleList
	rc, err := p.Blobs.Get(ctx, repoID, bundleManifest)
	if err != nil {
		return list
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, 4<<20))
	if err != nil {
		return bundleList{}
	}
	json.Unmarshal(data, &list)
	sort.Slice(list.Entries, func(i, j int) bool { return list.Entries[i].Token < list.Entries[j].Token })
	return list
}

func (p *Pipeline) saveBundleList(ctx context.Context, repoID string, list bundleList) error {
	data, err := json.Marshal(list)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "bundlelist-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Seek(0, 0)
	err = p.Blobs.Put(ctx, repoID, bundleManifest, tmp)
	tmp.Close()
	return err
}
