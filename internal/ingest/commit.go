package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

var ErrNothingToCommit = errors.New("nothing to commit")

type Sig struct {
	Name  string
	Email string
}

func (s Sig) orDefault() Sig {
	if s.Name == "" {
		s.Name = "forge"
	}
	if s.Email == "" {
		s.Email = "forge@localhost"
	}
	return s
}

// CommitSpec is one API-driven write. Build runs arbitrary plumbing inside a
// quarantine (new objects never touch the cache repo's object dir until they
// are durable in the blob store) and returns the tree to commit.
type CommitSpec struct {
	RepoID  string
	Ref     string   // full ref name to CAS (already namespaced if ephemeral)
	OldOID  string   // expected current target; repodb.ZeroOID for "must not exist"
	Parents []string // parent commits; empty for a root commit
	Message string
	Author  Sig
	Pusher  string // subject for the push event
	// Build receives an Exec wired to the quarantine env. Return the tree
	// OID to commit. If AllowEmpty is false and the tree equals the first
	// parent's tree, the op fails with ErrNothingToCommit.
	Build      func(x *gitcmd.Exec) (tree string, err error)
	AllowEmpty bool
}

// Commit runs the whole pipeline under the repo write lock: quarantine build
// → commit-tree → pack new objects → upload to store → record packs → CAS
// ref → install pack into cache. Returns the commit.
func (s *Service) Commit(ctx context.Context, spec CommitSpec) (*gitcmd.Commit, error) {
	lock := s.Cache.Lock(spec.RepoID)
	lock.Lock()
	defer lock.Unlock()

	dir, err := s.Cache.Materialize(ctx, spec.RepoID)
	if err != nil {
		return nil, err
	}

	qdir, err := os.MkdirTemp(dir, "quarantine-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(qdir)
	if err := os.Mkdir(filepath.Join(qdir, "pack"), 0o755); err != nil {
		return nil, err
	}

	author := spec.Author.orDefault()
	x := &gitcmd.Exec{Ctx: ctx, Dir: dir, Env: []string{
		"GIT_OBJECT_DIRECTORY=" + qdir,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(dir, "objects"),
		"GIT_INDEX_FILE=" + filepath.Join(qdir, "index"),
		"GIT_AUTHOR_NAME=" + author.Name,
		"GIT_AUTHOR_EMAIL=" + author.Email,
		"GIT_COMMITTER_NAME=" + author.Name,
		"GIT_COMMITTER_EMAIL=" + author.Email,
	}}

	tree, err := spec.Build(x)
	if err != nil {
		return nil, err
	}
	tree = strings.TrimSpace(tree)

	if !spec.AllowEmpty && len(spec.Parents) > 0 {
		parentTree, err := x.Run("rev-parse", spec.Parents[0]+"^{tree}")
		if err == nil && strings.TrimSpace(parentTree) == tree {
			return nil, ErrNothingToCommit
		}
	}

	args := []string{"commit-tree", tree, "-m", spec.Message}
	for _, p := range spec.Parents {
		args = append(args, "-p", p)
	}
	out, err := x.Run(args...)
	if err != nil {
		return nil, err
	}
	commitOID := strings.TrimSpace(out)

	if err := s.publish(ctx, x, spec.RepoID, qdir, commitOID); err != nil {
		return nil, err
	}

	old := spec.OldOID
	if old == "" {
		old = repodb.ZeroOID
	}
	update := repodb.RefUpdate{Name: spec.Ref, Old: old, New: commitOID}
	events := []repodb.Event{webhook.PushEventFor(spec.RepoID, update, spec.Pusher)}
	if err := s.DB.UpdateRefs(ctx, spec.RepoID, []repodb.RefUpdate{update}, events); err != nil {
		return nil, err
	}
	// Cache refs now lag the DB; next materialize fixes them. Do it eagerly
	// since we hold the lock and it's cheap.
	if err := s.Cache.SyncRefs(ctx, spec.RepoID, dir); err != nil {
		return nil, err
	}
	return x.GetCommit(commitOID)
}

// publish packs every object reachable from commitOID but not from current
// refs, uploads pack+idx to the store, records them, and installs them into
// the cache repo so serving needs no store round-trip.
func (s *Service) publish(ctx context.Context, x *gitcmd.Exec, repoID, qdir, commitOID string) error {
	refs, err := s.DB.ListRefs(ctx, repoID)
	if err != nil {
		return err
	}
	// Feed the revs on stdin, not argv: a repo with thousands of refs would
	// blow past ARGV_MAX. "^<ref>" is the per-rev form of --not.
	var revs strings.Builder
	revs.WriteString(commitOID + "\n")
	for _, r := range refs {
		revs.WriteString("^" + r.Target + "\n")
	}
	objects, err := x.RunIn(strings.NewReader(revs.String()), "rev-list", "--objects", "--stdin")
	if err != nil {
		return err
	}
	var oids strings.Builder
	for _, line := range strings.Split(objects, "\n") {
		if oid, _, _ := strings.Cut(line, " "); len(oid) >= 40 {
			oids.WriteString(oid + "\n")
		}
	}
	if oids.Len() == 0 {
		return nil // pure ref move; nothing new to store
	}

	base := filepath.Join(qdir, "pack", "pack")
	out, err := x.RunIn(strings.NewReader(oids.String()), "pack-objects", "-q", base)
	if err != nil {
		return err
	}
	hash := strings.TrimSpace(out)
	name := "pack-" + hash

	var size int64
	for _, ext := range []string{".pack", ".idx"} {
		path := base + "-" + hash + ext
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		if ext == ".pack" {
			if info, err := f.Stat(); err == nil {
				size = info.Size()
			}
		}
		err = s.Blobs.Put(ctx, repoID, name+ext, f)
		f.Close()
		if err != nil {
			return fmt.Errorf("store %s%s: %w", name, ext, err)
		}
	}
	if err := s.DB.AddPacks(ctx, repoID, []repodb.Pack{{Name: name, SizeBytes: size, Source: "api"}}); err != nil {
		return err
	}
	// Install into the cache (.pack before .idx).
	packDir := filepath.Join(s.Cache.RepoDir(repoID), "objects", "pack")
	for _, ext := range []string{".pack", ".idx"} {
		if err := os.Rename(base+"-"+hash+ext, filepath.Join(packDir, name+ext)); err != nil {
			return err
		}
	}
	return nil
}
