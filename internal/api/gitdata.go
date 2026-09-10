package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

type Blob struct {
	SHA      string `json:"sha"`
	Size     int    `json:"size"`
	Encoding string `json:"encoding"`
	Content  string `json:"content" doc:"Base64-encoded blob content"`
}

func (s *Server) registerGitData(api huma.API) {
	huma.Register(api, op("getBlob", "GET", "/api/repos/{id}/git/blobs/{sha}", auth.ScopeGitRead,
		"Get a blob (base64)"),
		func(ctx context.Context, in *struct {
			ID  string `path:"id"`
			SHA string `path:"sha"`
		}) (*struct{ Body Blob }, error) {
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			defer release()
			if t, _, err := s.Cache.ObjectInfo(in.ID, x.Dir, in.SHA); err != nil || t != "blob" {
				return nil, huma.Error404NotFound("blob not found")
			}
			content, err := s.Cache.BlobContents(in.ID, x.Dir, in.SHA)
			if err != nil {
				return nil, internalErr("cat-file", err)
			}
			return &struct{ Body Blob }{Blob{
				SHA: in.SHA, Size: len(content),
				Encoding: "base64", Content: base64.StdEncoding.EncodeToString(content),
			}}, nil
		})

	huma.Register(api, op("getRawBlob", "GET", "/api/repos/{id}/git/blobs/{sha}/raw", auth.ScopeGitRead,
		"Stream a blob's raw bytes"),
		func(ctx context.Context, in *struct {
			ID  string `path:"id"`
			SHA string `path:"sha"`
		}) (*huma.StreamResponse, error) {
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			if t, err := x.ObjectType(in.SHA); err != nil || t != "blob" {
				release()
				return nil, huma.Error404NotFound("blob not found")
			}
			sha := in.SHA
			pub := s.repoPublic(ctx, in.ID)
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				defer release()
				if serveImmutable(hc, sha, pub) {
					return // blob is content-addressed; client already has it
				}
				hc.SetHeader("Content-Type", "application/octet-stream")
				x.RunStream(hc.BodyWriter(), "cat-file", "blob", "--end-of-options", sha)
			}}, nil
		})

	huma.Register(api, op("getTree", "GET", "/api/repos/{id}/git/trees/{sha}", auth.ScopeGitRead,
		"Get a tree listing"),
		func(ctx context.Context, in *struct {
			ID        string `path:"id"`
			SHA       string `path:"sha"`
			Recursive bool   `query:"recursive"`
		}) (*struct{ Body TreeOut }, error) {
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			defer release()
			entries, err := x.LsTree(in.SHA, "", in.Recursive)
			if err != nil {
				return nil, huma.Error404NotFound("tree not found")
			}
			return &struct{ Body TreeOut }{TreeOut{SHA: in.SHA, Tree: entries}}, nil
		})

	huma.Register(api, op("listRefs", "GET", "/api/repos/{id}/git/refs", auth.ScopeGitRead, "List refs"),
		func(ctx context.Context, in *struct {
			ID     string `path:"id"`
			Prefix string `query:"prefix" example:"refs/heads/"`
		}) (*struct{ Body []repodb.Ref }, error) {
			refs, err := s.DB.ListRefs(ctx, in.ID)
			if err != nil {
				return nil, internalErr("list refs", err)
			}
			out := []repodb.Ref{}
			for _, ref := range refs {
				if in.Prefix == "" || strings.HasPrefix(ref.Name, in.Prefix) {
					out = append(out, ref)
				}
			}
			return &struct{ Body []repodb.Ref }{out}, nil
		})

	createOp := op("createRef", "POST", "/api/repos/{id}/git/refs", auth.ScopeGitWrite,
		"Create a ref pointing at an existing object")
	createOp.DefaultStatus = http.StatusCreated
	huma.Register(api, createOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body struct {
			Ref string `json:"ref" example:"refs/tags/v1"`
			SHA string `json:"sha"`
		}
	}) (*struct{ Body repodb.Ref }, error) {
		if !validRefName(in.Body.Ref) {
			return nil, huma.Error400BadRequest("invalid ref name")
		}
		if err := s.objectExists(ctx, in.ID, in.Body.SHA); err != nil {
			return nil, err
		}
		u := repodb.RefUpdate{Name: in.Body.Ref, Old: repodb.ZeroOID, New: in.Body.SHA}
		if err := s.casRef(ctx, in.ID, u); err != nil {
			return nil, refErr(err)
		}
		return &struct{ Body repodb.Ref }{repodb.Ref{Name: in.Body.Ref, Target: in.Body.SHA}}, nil
	})

	huma.Register(api, op("updateRef", "PATCH", "/api/repos/{id}/git/refs/{ref...}", auth.ScopeGitWrite,
		"Move a ref (compare-and-swap)"),
		func(ctx context.Context, in *struct {
			ID   string `path:"id"`
			Ref  string `path:"ref"`
			Body struct {
				SHA    string `json:"sha"`
				OldSHA string `json:"old_sha,omitempty" doc:"Expected current target; defaults to the ref's current value"`
			}
		}) (*struct{ Body repodb.Ref }, error) {
			if err := s.objectExists(ctx, in.ID, in.Body.SHA); err != nil {
				return nil, err
			}
			old := in.Body.OldSHA
			if old == "" {
				cur, _, err := s.resolveRev(ctx, in.ID, in.Ref, false)
				if err != nil {
					return nil, huma.Error404NotFound("ref not found")
				}
				old = cur
			}
			u := repodb.RefUpdate{Name: in.Ref, Old: old, New: in.Body.SHA}
			if err := s.casRef(ctx, in.ID, u); err != nil {
				return nil, refErr(err)
			}
			return &struct{ Body repodb.Ref }{repodb.Ref{Name: in.Ref, Target: in.Body.SHA}}, nil
		})

	delOp := op("deleteRef", "DELETE", "/api/repos/{id}/git/refs/{ref...}", auth.ScopeGitWrite, "Delete a ref")
	delOp.DefaultStatus = http.StatusNoContent
	huma.Register(api, delOp, func(ctx context.Context, in *struct {
		ID  string `path:"id"`
		Ref string `path:"ref"`
	}) (*struct{}, error) {
		cur, _, err := s.resolveRev(ctx, in.ID, in.Ref, false)
		if err != nil {
			return nil, huma.Error404NotFound("ref not found")
		}
		u := repodb.RefUpdate{Name: in.Ref, Old: cur, New: repodb.ZeroOID}
		if err := s.casRef(ctx, in.ID, u); err != nil {
			return nil, refErr(err)
		}
		return nil, nil
	})
}

type TreeOut struct {
	SHA  string             `json:"sha"`
	Tree []gitcmd.TreeEntry `json:"tree"`
}

// objectExists verifies the target object is in the repo before a ref op —
// reachability, not hash existence, gates access.
func (s *Server) objectExists(ctx context.Context, repoID, sha string) error {
	if len(sha) != 40 || !isHex(sha) {
		return huma.Error400BadRequest("sha must be 40 hex chars")
	}
	x, release, err := s.exec(ctx, repoID)
	if err != nil {
		return err
	}
	defer release()
	if _, err := x.ObjectType(sha); err != nil {
		return huma.Error422UnprocessableEntity("object does not exist in this repository")
	}
	return nil
}

func refErr(err error) error {
	switch {
	case isCAS(err):
		return huma.Error409Conflict("ref changed concurrently - retry")
	case errors.Is(err, repodb.ErrNotFound):
		return huma.Error404NotFound("repository not found")
	default:
		return internalErr("ref update", err)
	}
}

// validRefName is a conservative subset of git-check-ref-format.
func validRefName(name string) bool {
	if !strings.HasPrefix(name, "refs/") || strings.HasSuffix(name, "/") ||
		strings.HasSuffix(name, ".lock") || strings.Contains(name, "..") ||
		strings.Contains(name, "//") || strings.Contains(name, "@{") {
		return false
	}
	for _, c := range name {
		if c < 0x20 || c == 0x7f || strings.ContainsRune(" ~^:?*[\\", c) {
			return false
		}
	}
	return true
}
