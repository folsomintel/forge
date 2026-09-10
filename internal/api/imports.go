package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

func (s *Server) registerImports(api huma.API) {
	importOp := op("startImport", "POST", "/api/repos/{id}/import", auth.ScopeGitWrite,
		"Import a bundle into an empty-ish repo, asynchronously")
	importOp.Description = "Downloads a self-contained v2 bundle from the URL and indexes it " +
		"out-of-band - the migration path for large repos. Poll GET .../import for status. " +
		"Fails if any bundle ref already exists in the repo."
	importOp.DefaultStatus = http.StatusAccepted
	huma.Register(api, importOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body struct {
			URL string `json:"url" format:"uri" doc:"HTTPS URL of a v2 git bundle"`
		}
	}) (*struct{ Body repodb.Import }, error) {
		if _, err := s.DB.GetRepo(ctx, in.ID); err != nil {
			return nil, huma.Error404NotFound("repository not found")
		}
		if !strings.HasPrefix(in.Body.URL, "http://") && !strings.HasPrefix(in.Body.URL, "https://") {
			return nil, huma.Error400BadRequest("url must be http(s)")
		}
		if cur, err := s.DB.GetImport(ctx, in.ID); err == nil && cur.Status == "running" {
			return nil, huma.Error409Conflict("an import is already running for this repository")
		}
		if err := s.DB.SetImport(ctx, in.ID, "running", "", 0); err != nil {
			return nil, internalErr("import", err)
		}
		go s.Ingest.StartImport(in.ID, in.Body.URL)
		im, _ := s.DB.GetImport(ctx, in.ID)
		return &struct{ Body repodb.Import }{*im}, nil
	})

	huma.Register(api, op("getImport", "GET", "/api/repos/{id}/import", auth.ScopeGitRead,
		"Import status"),
		func(ctx context.Context, in *repoParam) (*struct{ Body repodb.Import }, error) {
			im, err := s.DB.GetImport(ctx, in.ID)
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("no import for this repository")
			}
			if err != nil {
				return nil, internalErr("import status", err)
			}
			return &struct{ Body repodb.Import }{*im}, nil
		})
}
