package maintain

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

// Terminology (see ARCHITECTURE, "The derived-data flywheel"):
//
//   truth      packs + refs (bucket + DB). Never derived, never regenerable.
//   artifact   a redundant, regenerable representation of truth that makes
//              a common read cheap. Stored under "derived/" (plus the
//              bitmap, which rides its pack's basename).
//   derive     produce an artifact from the current repo state.
//   hydrate    place stored artifacts into a fresh materialization.
//   maintain   the background pipeline: consolidate -> derive -> advertise
//              -> sweep.
//
// To add an artifact: implement Artifact, append to Artifacts. Derivation
// runs after consolidation with the RepoState in hand; hydration is
// best-effort and must never fail a materialization.

type RepoState struct {
	RepoID string
	Dir    string
	Refs   []repodb.Ref
	GCPack string // name of the consolidated pack ("pack-<hash>")
}

type Artifact interface {
	Kind() string
	Derive(ctx context.Context, p *Pipeline, st *RepoState) error
	Hydrate(ctx context.Context, p *Pipeline, repoID, dir string)
}

// Artifacts is the registry, in derivation order (independent; run
// concurrently by the pipeline). The bundle chain lives in bundlechain.go.
var Artifacts = []Artifact{commitGraph{}, bundleChain{}, historyPack{}}

// Hydrate places stored artifacts and advertisements into a fresh
// materialization; it is installed as repocache.Cache.Hydrate at wiring
// time. Best-effort by contract.
func (p *Pipeline) Hydrate(ctx context.Context, repoID, dir string) {
	for _, a := range Artifacts {
		a.Hydrate(ctx, p, repoID, dir)
	}
	p.advertise(ctx, repoID, dir)
}

// --- commit-graph: log/merge-base/ancestry at scale ---

type commitGraph struct{}

func (commitGraph) Kind() string { return "commit-graph" }

const commitGraphBlob = "meta/commit-graph"

func (commitGraph) Derive(ctx context.Context, p *Pipeline, st *RepoState) error {
	x := &gitcmd.Exec{Ctx: ctx, Dir: st.Dir}
	if _, err := x.Run("commit-graph", "write", "--reachable"); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(st.Dir, "objects", "info", "commit-graph"))
	if err != nil {
		return err
	}
	defer f.Close()
	return p.Blobs.Put(ctx, st.RepoID, commitGraphBlob, f)
}

func (commitGraph) Hydrate(ctx context.Context, p *Pipeline, repoID, dir string) {
	dest := filepath.Join(dir, "objects", "info", "commit-graph")
	if _, err := os.Stat(dest); err != nil {
		p.Cache.FetchBlob(ctx, repoID, commitGraphBlob, dest)
	}
}

// deriveAll runs every artifact concurrently; failures degrade (the read
// paths fall back), so they log rather than abort maintenance.
func (p *Pipeline) deriveAll(ctx context.Context, st *RepoState) []string {
	type res struct {
		kind string
		err  error
	}
	ch := make(chan res, len(Artifacts))
	for _, a := range Artifacts {
		go func(a Artifact) { ch <- res{a.Kind(), a.Derive(ctx, p, st)} }(a)
	}
	var derived []string
	for range Artifacts {
		r := <-ch
		if r.err != nil {
			slog.Warn("derive artifact", "kind", r.kind, "repo", st.RepoID, "err", r.err)
			continue
		}
		derived = append(derived, r.kind)
	}
	sort.Strings(derived)
	return derived
}
