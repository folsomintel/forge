package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
)

var errApply = errors.New("apply failed")

func (s *Server) registerCommits(api huma.API) {
	huma.Register(api, op("listCommits", "GET", "/api/repos/{id}/commits", auth.ScopeGitRead,
		"List commits, newest first"),
		func(ctx context.Context, in *struct {
			ID        string `path:"id"`
			Ref       string `query:"ref"`
			Path      string `query:"path"`
			Limit     int    `query:"limit" minimum:"1" maximum:"100" default:"30"`
			After     string `query:"after" doc:"Cursor: commit SHA from a prior page's X-Next-Cursor"`
			Ephemeral bool   `query:"ephemeral"`
		}) (*struct {
			Next string `header:"X-Next-Cursor"`
			Body []gitcmd.Commit
		}, error) {
			start := in.Ref
			if in.After != "" {
				start = in.After // continue the walk from the cursor
			}
			oid, _, err := s.resolveRev(ctx, in.ID, start, in.Ephemeral)
			if err != nil {
				return nil, huma.Error404NotFound("ref not found")
			}
			limit := in.Limit
			if limit == 0 {
				limit = 30
			}
			// Over-fetch by one: the extra commit is the next page's cursor,
			// not part of this page (so no duplicate, no gap).
			var commits []gitcmd.Commit
			served := false
			if in.Path == "" && len(oid) == 40 && isHex(oid) {
				// Fork-free fast path: walk commits via the cat-file pool
				// (path-filtered listings need diff machinery - git's job;
				// short revs too, so output SHAs stay full-length).
				if dir, ok := s.readDir(ctx, in.ID); ok {
					if walked, ok := s.logWalk(in.ID, dir, oid, limit+1); ok {
						commits, served = walked, true
					}
				}
			}
			if !served {
				x, release, err := s.exec(ctx, in.ID)
				if err != nil {
					return nil, err
				}
				defer release()
				commits, err = x.Log(oid, in.Path, limit+1)
				if err != nil {
					return nil, huma.Error404NotFound("ref not found")
				}
			}
			next := ""
			if len(commits) > limit {
				next = commits[limit].SHA
				commits = commits[:limit]
			}
			if commits == nil {
				commits = []gitcmd.Commit{}
			}
			return &struct {
				Next string `header:"X-Next-Cursor"`
				Body []gitcmd.Commit
			}{Next: next, Body: commits}, nil
		})

	huma.Register(api, op("getCommit", "GET", "/api/repos/{id}/commits/{sha}", auth.ScopeGitRead,
		"Get a commit"),
		func(ctx context.Context, in *struct {
			ID  string `path:"id"`
			SHA string `path:"sha"`
		}) (*struct{ Body gitcmd.Commit }, error) {
			// Fork-free for a full SHA that is a commit (short SHAs and ref
			// names keep the fork path so the output SHA stays canonical).
			if dir, ok := s.readDir(ctx, in.ID); ok && len(in.SHA) == 40 && isHex(in.SHA) {
				if typ, _, err := s.Cache.ObjectInfo(in.ID, dir, in.SHA); err == nil && typ == "commit" {
					if walked, ok := s.logWalk(in.ID, dir, in.SHA, 1); ok && len(walked) == 1 {
						return &struct{ Body gitcmd.Commit }{walked[0]}, nil
					}
				}
			}
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			defer release()
			commit, err := x.GetCommit(in.SHA)
			if err != nil {
				return nil, huma.Error404NotFound("commit not found")
			}
			return &struct{ Body gitcmd.Commit }{*commit}, nil
		})

	huma.Register(api, op("getCommitDiff", "GET", "/api/repos/{id}/commits/{sha}/diff", auth.ScopeGitRead,
		"Get a commit's unified diff"),
		func(ctx context.Context, in *struct {
			ID  string `path:"id"`
			SHA string `path:"sha"`
		}) (*huma.StreamResponse, error) {
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			if _, err := x.ObjectType(in.SHA); err != nil {
				release()
				return nil, huma.Error404NotFound("commit not found")
			}
			sha := in.SHA
			pub := s.repoPublic(ctx, in.ID)
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				defer release()
				if serveImmutable(hc, sha, pub) {
					return // a commit's diff is fixed by its OID
				}
				hc.SetHeader("Content-Type", "text/plain; charset=utf-8")
				x.RunStream(hc.BodyWriter(), "diff-tree", "-p", "--root", "--no-commit-id", "--end-of-options", sha)
			}}, nil
		})

	huma.Register(api, op("compareDiff", "GET", "/api/repos/{id}/diff", auth.ScopeGitRead,
		"Three-dot compare between two revisions"),
		func(ctx context.Context, in *struct {
			ID            string `path:"id"`
			Base          string `query:"base" required:"true"`
			Head          string `query:"head" required:"true"`
			EphemeralHead bool   `query:"ephemeral_head"`
		}) (*huma.StreamResponse, error) {
			base, _, err := s.resolveRev(ctx, in.ID, in.Base, false)
			if err != nil {
				return nil, huma.Error404NotFound("base not found")
			}
			head, _, err := s.resolveRev(ctx, in.ID, in.Head, in.EphemeralHead)
			if err != nil {
				return nil, huma.Error404NotFound("head not found")
			}
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			pub := s.repoPublic(ctx, in.ID)
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				defer release()
				// Both endpoints are resolved to immutable OIDs, so the diff
				// between them is fixed.
				if serveImmutable(hc, base+".."+head, pub) {
					return
				}
				hc.SetHeader("Content-Type", "text/plain; charset=utf-8")
				x.RunStream(hc.BodyWriter(), "diff", base+"..."+head)
			}}, nil
		})

	fromDiffOp := op("commitFromDiff", "POST", "/api/repos/{id}/commits/from-diff", auth.ScopeGitWrite,
		"Apply a unified diff as one commit")
	fromDiffOp.Description = "The one-request alternative to the blob/tree/commit/ref dance."
	fromDiffOp.DefaultStatus = http.StatusCreated
	huma.Register(api, fromDiffOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body struct {
			Branch    string `json:"branch,omitempty" doc:"Default branch if empty"`
			Ephemeral bool   `json:"ephemeral,omitempty"`
			Message   string `json:"message"`
			Diff      string `json:"diff" doc:"Unified diff text"`
			Author    Author `json:"author,omitzero"`
		}
	}) (*struct{ Body FromDiffResult }, error) {
		if in.Body.Diff == "" {
			return nil, huma.Error400BadRequest("diff required")
		}
		if in.Body.Message == "" {
			return nil, huma.Error400BadRequest("message required")
		}
		repo, err := s.DB.GetRepo(ctx, in.ID)
		if err != nil {
			return nil, huma.Error404NotFound("repository not found")
		}
		branch := in.Body.Branch
		if branch == "" {
			branch = repo.DefaultBranch
		}
		tip, _, _ := s.resolveRev(ctx, in.ID, branch, in.Body.Ephemeral)

		diff := in.Body.Diff
		spec := ingest.CommitSpec{
			RepoID:  in.ID,
			Ref:     repodb.BranchRef(branch, in.Body.Ephemeral),
			OldOID:  tip,
			Message: in.Body.Message,
			Author:  ingest.Sig{Name: in.Body.Author.Name, Email: in.Body.Author.Email},
			Build: func(x *gitcmd.Exec) (string, error) {
				if tip != "" {
					if _, err := x.Run("read-tree", tip); err != nil {
						return "", err
					}
				} else if _, err := x.Run("read-tree", "--empty"); err != nil {
					return "", err
				}
				if _, err := x.RunIn(strings.NewReader(diff), "apply", "--cached", "--whitespace=nowarn", "-"); err != nil {
					return "", errApply
				}
				return x.Run("write-tree")
			},
		}
		if tip != "" {
			spec.Parents = []string{tip}
		}
		commit, err := s.writeCommit(ctx, spec)
		switch {
		case errors.Is(err, errApply):
			return nil, huma.Error422UnprocessableEntity("diff does not apply to the current branch tip")
		case errors.Is(err, ingest.ErrNothingToCommit):
			return nil, huma.Error409Conflict("nothing to commit: diff is empty against the branch tip")
		case isCAS(err):
			return nil, huma.Error409Conflict("branch moved concurrently - retry")
		case err != nil:
			return nil, internalErr("commit from diff", err)
		}
		return &struct{ Body FromDiffResult }{FromDiffResult{Commit: *commit, Branch: branch, Ephemeral: in.Body.Ephemeral}}, nil
	})
}

type FromDiffResult struct {
	Commit    gitcmd.Commit `json:"commit"`
	Branch    string        `json:"branch"`
	Ephemeral bool          `json:"ephemeral"`
}
