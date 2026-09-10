package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

type Branch struct {
	Name      string `json:"name"`
	SHA       string `json:"sha"`
	Ephemeral bool   `json:"ephemeral"`
	Default   bool   `json:"default,omitempty"`
}

func (s *Server) registerBranches(api huma.API) {
	huma.Register(api, op("listBranches", "GET", "/api/repos/{id}/branches", auth.ScopeGitRead,
		"List branches"),
		func(ctx context.Context, in *struct {
			ID        string `path:"id"`
			Ephemeral bool   `query:"ephemeral"`
		}) (*struct{ Body []Branch }, error) {
			repo, err := s.DB.GetRepo(ctx, in.ID)
			if err != nil {
				return nil, huma.Error404NotFound("repository not found")
			}
			refs, err := s.DB.ListRefs(ctx, in.ID)
			if err != nil {
				return nil, internalErr("list refs", err)
			}
			out := []Branch{}
			for _, ref := range refs {
				name, eph := repodb.SplitRef(ref.Name)
				short, isBranch := strings.CutPrefix(name, "refs/heads/")
				if !isBranch || eph != in.Ephemeral {
					continue
				}
				out = append(out, Branch{Name: short, SHA: ref.Target, Ephemeral: eph,
					Default: !eph && short == repo.DefaultBranch})
			}
			return &struct{ Body []Branch }{out}, nil
		})

	huma.Register(api, op("getBranch", "GET", "/api/repos/{id}/branches/{branch}", auth.ScopeGitRead,
		"Get a branch"),
		func(ctx context.Context, in *struct {
			ID        string `path:"id"`
			Branch    string `path:"branch"`
			Ephemeral bool   `query:"ephemeral"`
		}) (*struct{ Body Branch }, error) {
			oid, _, err := s.resolveRev(ctx, in.ID, in.Branch, in.Ephemeral)
			if err != nil {
				return nil, huma.Error404NotFound("branch not found")
			}
			return &struct{ Body Branch }{Branch{Name: in.Branch, SHA: oid, Ephemeral: in.Ephemeral}}, nil
		})

	createOp := op("createBranch", "POST", "/api/repos/{id}/branches", auth.ScopeGitWrite,
		"Create a branch (also promotes ephemeral branches)")
	createOp.Description = "Pure ref CAS; no objects are copied. Promote an agent's ephemeral attempt " +
		"by setting from_ephemeral: true and ephemeral: false."
	createOp.DefaultStatus = http.StatusCreated
	huma.Register(api, createOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body struct {
			Name          string `json:"name"`
			From          string `json:"from,omitempty" doc:"Branch name or commit sha; default branch tip if empty"`
			FromEphemeral bool   `json:"from_ephemeral,omitempty"`
			Ephemeral     bool   `json:"ephemeral,omitempty"`
		}
	}) (*struct{ Body Branch }, error) {
		if in.Body.Name == "" || !validRefName("refs/heads/"+in.Body.Name) {
			return nil, huma.Error400BadRequest("invalid branch name")
		}
		oid, _, err := s.resolveRev(ctx, in.ID, in.Body.From, in.Body.FromEphemeral)
		if err != nil {
			return nil, huma.Error404NotFound("base not found")
		}
		if err := s.objectExists(ctx, in.ID, oid); err != nil {
			return nil, err
		}
		u := repodb.RefUpdate{Name: repodb.BranchRef(in.Body.Name, in.Body.Ephemeral), Old: repodb.ZeroOID, New: oid}
		if err := s.casRef(ctx, in.ID, u); err != nil {
			if isCAS(err) {
				return nil, huma.Error409Conflict("branch already exists")
			}
			return nil, refErr(err)
		}
		return &struct{ Body Branch }{Branch{Name: in.Body.Name, SHA: oid, Ephemeral: in.Body.Ephemeral}}, nil
	})

	delOp := op("deleteBranch", "DELETE", "/api/repos/{id}/branches/{branch}", auth.ScopeGitWrite,
		"Delete a branch")
	delOp.DefaultStatus = http.StatusNoContent
	huma.Register(api, delOp, func(ctx context.Context, in *struct {
		ID        string `path:"id"`
		Branch    string `path:"branch"`
		Ephemeral bool   `query:"ephemeral"`
	}) (*struct{}, error) {
		repo, err := s.DB.GetRepo(ctx, in.ID)
		if err != nil {
			return nil, huma.Error404NotFound("repository not found")
		}
		if !in.Ephemeral && in.Branch == repo.DefaultBranch {
			return nil, huma.Error422UnprocessableEntity("cannot delete the default branch")
		}
		cur, _, err := s.resolveRev(ctx, in.ID, in.Branch, in.Ephemeral)
		if err != nil {
			return nil, huma.Error404NotFound("branch not found")
		}
		u := repodb.RefUpdate{Name: repodb.BranchRef(in.Branch, in.Ephemeral), Old: cur, New: repodb.ZeroOID}
		if err := s.casRef(ctx, in.ID, u); err != nil {
			return nil, refErr(err)
		}
		return nil, nil
	})
}
