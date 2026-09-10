package api

import (
	"context"
	"errors"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/maintain"
	"github.com/folsomintel/forge/internal/repodb"
)

// Maintenance is an internal lifecycle concern (worker + post-write nudges);
// customers never think about packs. The endpoint survives as a hidden
// operational lever - reachable for ops and e2e tests, absent from the
// OpenAPI spec and the generated SDK.
func (s *Server) registerMaintenance(api huma.API) {
	maintOp := op("maintainRepo", "POST", "/api/repos/{id}/maintenance", auth.ScopeRepoWrite,
		"Force one maintenance run (consolidate, derive, advertise, sweep)")
	maintOp.Hidden = true
	huma.Register(api, maintOp,
		func(ctx context.Context, in *repoParam) (*struct{ Body maintain.Report }, error) {
			res, err := s.Maintain.Run(ctx, in.ID, 1)
			if errors.Is(err, repodb.ErrNotFound) {
				return nil, huma.Error404NotFound("repository not found")
			}
			if err != nil {
				return nil, internalErr("maintain", err)
			}
			return &struct{ Body maintain.Report }{*res}, nil
		})
}
