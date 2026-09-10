package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
)

type MergeRequest struct {
	Base          string `json:"base" doc:"Target branch"`
	BaseEphemeral bool   `json:"base_ephemeral,omitempty"`
	Head          string `json:"head" doc:"Branch or sha being merged"`
	HeadEphemeral bool   `json:"head_ephemeral,omitempty"`
	Strategy      string `json:"strategy,omitempty" enum:",merge,squash,ff-only,ff-preferred" doc:"Default merge (no-ff commit)"`
	Message       string `json:"message,omitempty"`
	Preview       bool   `json:"preview,omitempty" doc:"Dry run - report mergeability, commit nothing"`
	Author        Author `json:"author,omitzero"`
}

type MergeResult struct {
	Status      string   `json:"status" enum:"merged,mergeable,conflicts,up_to_date"`
	SHA         string   `json:"sha,omitempty"`
	FastForward bool     `json:"fast_forward,omitempty"`
	Conflicts   []string `json:"conflicts,omitempty"`
}

type conflictErr struct{ files []string }

func (e *conflictErr) Error() string { return "merge conflicts" }

func (s *Server) registerMerge(api huma.API) {
	mergeOp := op("merge", "POST", "/api/repos/{id}/merge", auth.ScopeGitWrite, "Server-side merge")
	mergeOp.Errors = []int{404, 409, 422}
	huma.Register(api, mergeOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body MergeRequest
	}) (*mergeOut, error) {
		return s.merge(ctx, in.ID, in.Body)
	})
}

// mergeOut carries a dynamic status: 200 for merged/mergeable/up_to_date,
// 409 with the conflict list.
type mergeOut struct {
	Status int
	Body   MergeResult
}

func (s *Server) merge(ctx context.Context, repoID string, req MergeRequest) (*mergeOut, error) {
	switch req.Strategy {
	case "":
		req.Strategy = "merge"
	case "merge", "squash", "ff-only", "ff-preferred":
	default:
		return nil, huma.Error400BadRequest("strategy must be merge, squash, ff-only, or ff-preferred")
	}

	baseTip, baseRef, err := s.resolveRev(ctx, repoID, req.Base, req.BaseEphemeral)
	if err != nil || !strings.Contains(baseRef, "refs/heads/") {
		return nil, huma.Error404NotFound("base branch not found")
	}
	headOID, _, err := s.resolveRev(ctx, repoID, req.Head, req.HeadEphemeral)
	if err != nil {
		return nil, huma.Error404NotFound("head not found")
	}

	x, release, err := s.exec(ctx, repoID)
	if err != nil {
		return nil, err
	}
	headIsAncestor, err1 := isAncestor(x, headOID, baseTip)
	ffPossible, err2 := isAncestor(x, baseTip, headOID)
	release()
	if err1 != nil || err2 != nil {
		return nil, huma.Error422UnprocessableEntity("base or head is not a commit in this repository")
	}

	if headIsAncestor {
		return &mergeOut{Status: 200, Body: MergeResult{Status: "up_to_date", SHA: baseTip}}, nil
	}
	if req.Message == "" {
		req.Message = fmt.Sprintf("Merge %s into %s", req.Head, req.Base)
	}

	switch {
	case ffPossible && (req.Strategy == "ff-only" || req.Strategy == "ff-preferred"):
		if req.Preview {
			return &mergeOut{Status: 200, Body: MergeResult{Status: "mergeable", FastForward: true}}, nil
		}
		u := repodb.RefUpdate{Name: baseRef, Old: baseTip, New: headOID}
		if err := s.casRef(ctx, repoID, u); err != nil {
			return nil, refErr(err)
		}
		return &mergeOut{Status: 200, Body: MergeResult{Status: "merged", SHA: headOID, FastForward: true}}, nil
	case !ffPossible && req.Strategy == "ff-only":
		return nil, huma.Error409Conflict("not fast-forwardable")
	}

	if req.Preview {
		return s.previewMerge(ctx, repoID, baseTip, headOID)
	}

	parents := []string{baseTip, headOID}
	if req.Strategy == "squash" {
		parents = []string{baseTip}
	}
	spec := ingest.CommitSpec{
		RepoID:     repoID,
		Ref:        baseRef,
		OldOID:     baseTip,
		Parents:    parents,
		Message:    req.Message,
		Author:     ingest.Sig{Name: req.Author.Name, Email: req.Author.Email},
		AllowEmpty: true, // a merge commit with an unchanged tree is still meaningful
		Build: func(x *gitcmd.Exec) (string, error) {
			return mergeTree(x, baseTip, headOID)
		},
	}
	commit, err := s.writeCommit(ctx, spec)
	var conflicts *conflictErr
	switch {
	case errors.As(err, &conflicts):
		return &mergeOut{Status: 409, Body: MergeResult{Status: "conflicts", Conflicts: conflicts.files}}, nil
	case isCAS(err):
		return nil, huma.Error409Conflict("base branch moved concurrently - retry")
	case err != nil:
		return nil, internalErr("merge", err)
	}
	return &mergeOut{Status: 200, Body: MergeResult{Status: "merged", SHA: commit.SHA}}, nil
}

// previewMerge runs merge-tree with a throwaway object dir so preview
// objects never pollute the cache repo.
func (s *Server) previewMerge(ctx context.Context, repoID, baseTip, headOID string) (*mergeOut, error) {
	x, release, err := s.exec(ctx, repoID)
	if err != nil {
		return nil, err
	}
	defer release()
	qdir, err := os.MkdirTemp("", "forge-preview-*")
	if err != nil {
		return nil, internalErr("preview", err)
	}
	defer os.RemoveAll(qdir)
	px := &gitcmd.Exec{Ctx: ctx, Dir: x.Dir, Env: []string{
		"GIT_OBJECT_DIRECTORY=" + qdir,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(x.Dir, "objects"),
	}}
	_, err = mergeTree(px, baseTip, headOID)
	var conflicts *conflictErr
	switch {
	case errors.As(err, &conflicts):
		return &mergeOut{Status: 200, Body: MergeResult{Status: "conflicts", Conflicts: conflicts.files}}, nil
	case err != nil:
		return nil, internalErr("preview merge", err)
	}
	return &mergeOut{Status: 200, Body: MergeResult{Status: "mergeable"}}, nil
}

// mergeTree runs `git merge-tree --write-tree` and returns the merged tree,
// or a *conflictErr listing conflicted paths.
func mergeTree(x *gitcmd.Exec, base, head string) (string, error) {
	out, code, err := x.RunCode(nil, "merge-tree", "--write-tree", "--name-only", "--no-messages", base, head)
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	switch code {
	case 0:
		return strings.TrimSpace(lines[0]), nil
	case 1:
		files := []string{}
		for _, l := range lines[1:] {
			if l == "" {
				break // informational section separator
			}
			files = append(files, l)
		}
		return "", &conflictErr{files: files}
	default:
		return "", fmt.Errorf("merge-tree exit %d: %s", code, out)
	}
}

func isAncestor(x *gitcmd.Exec, a, b string) (bool, error) {
	_, code, err := x.RunCode(nil, "merge-base", "--is-ancestor", a, b)
	if err != nil {
		return false, err
	}
	if code != 0 && code != 1 {
		return false, fmt.Errorf("merge-base exit %d", code)
	}
	return code == 0, nil
}
