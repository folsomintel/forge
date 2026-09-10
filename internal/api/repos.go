package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

type repoParam struct {
	ID string `path:"id" doc:"Repository id"`
}

func (s *Server) registerRepos(api huma.API) {
	createOp := op("createRepo", "POST", "/api/repos", auth.ScopeRepoWrite, "Create a repository")
	createOp.DefaultStatus = http.StatusCreated
	huma.Register(api, createOp, func(ctx context.Context, in *struct {
		Body struct {
			ID            string `json:"id" doc:"Letters, digits, . _ - (max 100 chars)"`
			DefaultBranch string `json:"default_branch,omitempty"`
			Public        bool   `json:"public,omitempty" doc:"Anonymous read access (clone + read API)"`
		}
	}) (*struct{ Body repodb.Repo }, error) {
		if !repodb.ValidRepoID(in.Body.ID) {
			return nil, huma.Error400BadRequest("invalid repo id: use letters, digits, . _ - (max 100 chars)")
		}
		branch := in.Body.DefaultBranch
		if branch == "" {
			branch = "main"
		}
		if err := s.DB.CreateRepo(ctx, in.Body.ID, branch); err != nil {
			if errors.Is(err, repodb.ErrExists) {
				return nil, huma.Error409Conflict("repository already exists")
			}
			return nil, internalErr("create repo", err)
		}
		if in.Body.Public {
			if err := s.DB.SetRepoPublic(ctx, in.Body.ID, true); err != nil {
				return nil, internalErr("set visibility", err)
			}
		}
		repo, err := s.DB.GetRepo(ctx, in.Body.ID)
		if err != nil {
			return nil, internalErr("get repo", err)
		}
		s.audit(ctx, "repo.create", in.Body.ID, visibility(in.Body.Public))
		return &struct{ Body repodb.Repo }{*repo}, nil
	})

	huma.Register(api, op("listRepos", "GET", "/api/repos", auth.ScopeOrgRead, "List repositories"),
		func(ctx context.Context, in *struct {
			Limit int    `query:"limit" minimum:"1" maximum:"1000" default:"100"`
			After string `query:"after" doc:"Cursor: repo id from a prior page's X-Next-Cursor"`
		}) (*struct {
			Next string `header:"X-Next-Cursor"`
			Body []repodb.Repo
		}, error) {
			limit := in.Limit
			if limit == 0 {
				limit = 100
			}
			repos, err := s.DB.ListReposPage(ctx, in.After, limit+1) // over-fetch for the cursor
			if err != nil {
				return nil, internalErr("list repos", err)
			}
			next := ""
			if len(repos) > limit {
				next = repos[limit-1].ID
				repos = repos[:limit]
			}
			if repos == nil {
				repos = []repodb.Repo{}
			}
			return &struct {
				Next string `header:"X-Next-Cursor"`
				Body []repodb.Repo
			}{Next: next, Body: repos}, nil
		})

	huma.Register(api, op("getRepo", "GET", "/api/repos/{id}", auth.ScopeOrgRead, "Get a repository"),
		func(ctx context.Context, in *repoParam) (*struct{ Body repodb.Repo }, error) {
			repo, err := s.DB.GetRepo(ctx, in.ID)
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("repository not found")
			}
			if err != nil {
				return nil, internalErr("get repo", err)
			}
			return &struct{ Body repodb.Repo }{*repo}, nil
		})

	deleteOp := op("deleteRepo", "DELETE", "/api/repos/{id}", auth.ScopeRepoWrite, "Delete a repository")
	deleteOp.DefaultStatus = http.StatusNoContent
	huma.Register(api, deleteOp, func(ctx context.Context, in *repoParam) (*struct{}, error) {
		lock := s.Cache.Lock(in.ID)
		lock.Lock()
		defer lock.Unlock()
		if err := s.DB.DeleteRepo(ctx, in.ID); err != nil {
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("repository not found")
			}
			return nil, internalErr("delete repo", err)
		}
		if err := s.Cache.Drop(in.ID); err != nil {
			slog.Error("drop cache repo", "repo", in.ID, "err", err)
		}
		s.audit(ctx, "repo.delete", in.ID, "")
		return nil, nil
	})

	huma.Register(api, op("updateRepo", "PATCH", "/api/repos/{id}", auth.ScopeRepoWrite,
		"Update repository settings (visibility)"),
		func(ctx context.Context, in *struct {
			ID   string `path:"id"`
			Body struct {
				Public *bool `json:"public,omitempty" doc:"Anonymous read access (clone + read API)"`
			}
		}) (*struct{ Body repodb.Repo }, error) {
			if in.Body.Public != nil {
				if err := s.DB.SetRepoPublic(ctx, in.ID, *in.Body.Public); err != nil {
					if errors.Is(err, repodb.ErrNotFound) {
						return nil, huma.Error404NotFound("repository not found")
					}
					return nil, internalErr("set visibility", err)
				}
			}
			repo, err := s.DB.GetRepo(ctx, in.ID)
			if err != nil {
				return nil, internalErr("get repo", err)
			}
			if in.Body.Public != nil {
				s.audit(ctx, "repo.visibility", in.ID, visibility(*in.Body.Public))
			}
			return &struct{ Body repodb.Repo }{*repo}, nil
		})

	forkOp := op("forkRepo", "POST", "/api/repos/{id}/fork", auth.ScopeRepoWrite,
		"Fork a repository (zero-copy)")
	forkOp.Description = "Milliseconds regardless of size: refs are copied, immutable packs are shared. " +
		"The fork's first maintenance run rewrites its data under its own storage prefix."
	forkOp.DefaultStatus = http.StatusCreated
	huma.Register(api, forkOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body struct {
			ID string `json:"id" doc:"New repository id"`
		}
	}) (*struct{ Body repodb.Repo }, error) {
		if !repodb.ValidRepoID(in.Body.ID) {
			return nil, huma.Error400BadRequest("invalid repo id")
		}
		err := s.DB.ForkRepo(ctx, in.ID, in.Body.ID)
		switch {
		case errors.Is(err, repodb.ErrNotFound):
			return nil, huma.Error404NotFound("repository not found")
		case errors.Is(err, repodb.ErrExists):
			return nil, huma.Error409Conflict("target repository already exists")
		case err != nil:
			return nil, internalErr("fork", err)
		}
		repo, err := s.DB.GetRepo(ctx, in.Body.ID)
		if err != nil {
			return nil, internalErr("get fork", err)
		}
		s.audit(ctx, "repo.fork", in.Body.ID, "from "+in.ID)
		return &struct{ Body repodb.Repo }{*repo}, nil
	})
}

func visibility(public bool) string {
	if public {
		return "public"
	}
	return "private"
}
