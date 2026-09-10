package api

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

type UsageOut struct {
	Repos          int   `json:"repos"`
	PackBytes      int64 `json:"pack_bytes"`
	LFSBytes       int64 `json:"lfs_bytes"`
	DiskTotalBytes int64 `json:"disk_total_bytes"`
	DiskFreeBytes  int64 `json:"disk_free_bytes"`
	// ActiveTransfers counts in-flight git transfers; the control plane
	// polls it to drain a machine before restart/recreate.
	ActiveTransfers int64 `json:"active_transfers"`
	// WAL exposes group-commit telemetry when the backing DB is the WAL.
	WAL *repodb.WALStats `json:"wal,omitempty"`
	// GoReceive exposes fork-free receive fast-path decision counts.
	GoReceive *GoReceiveOut `json:"go_receive,omitempty"`
	GoFetch   *GoFetchOut   `json:"go_fetch,omitempty"`
}

type GoFetchOut struct {
	Eligible    int64            `json:"eligible"`     // served entirely in Go
	CloneStream int64            `json:"clone_stream"` // clones streamed straight from the store
	FellBack    int64            `json:"fell_back"`    // handed to git
	FellBackBy  map[string]int64 `json:"fell_back_by,omitempty" doc:"Fallback counts by cause"`
}

type GoReceiveOut struct {
	Eligible     int64            `json:"eligible"`
	FellBack     int64            `json:"fell_back"`
	Rejected     int64            `json:"rejected"`
	StorageErred int64            `json:"storage_erred,omitempty"`
	FellBackBy   map[string]int64 `json:"fell_back_by,omitempty" doc:"Fallback counts by cause"`
}

type KeyOut struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func (s *Server) registerInstance(api huma.API) {
	huma.Register(api, op("listAudit", "GET", "/api/audit", auth.ScopeOrgRead,
		"Operational audit trail (repo lifecycle, keys, webhooks)"),
		func(ctx context.Context, in *struct {
			Action string `query:"action" doc:"Exact action filter, e.g. repo.create"`
			Q      string `query:"q" doc:"Substring match on actor, target, detail"`
			Limit  int    `query:"limit"`
		}) (*struct{ Body []repodb.AuditEntry }, error) {
			entries, err := s.DB.ListAudit(ctx, in.Action, in.Q, in.Limit)
			if err != nil {
				return nil, internalErr("audit", err)
			}
			return &struct{ Body []repodb.AuditEntry }{entries}, nil
		})

	huma.Register(api, op("listKeys", "GET", "/api/keys", auth.ScopeOrgRead,
		"Registered signing keys (public keys accepted for JWT auth)"),
		func(ctx context.Context, _ *struct{}) (*struct{ Body []KeyOut }, error) {
			keys, err := s.DB.ListKeys(ctx)
			if err != nil {
				return nil, internalErr("keys", err)
			}
			out := []KeyOut{}
			for _, k := range keys {
				out = append(out, KeyOut{ID: k.ID, Name: k.Name})
			}
			return &struct{ Body []KeyOut }{out}, nil
		})

	delKeyOp := op("deleteKey", "DELETE", "/api/keys/{id}", auth.ScopeRepoWrite,
		"Revoke a registered signing key")
	delKeyOp.DefaultStatus = http.StatusNoContent
	huma.Register(api, delKeyOp, func(ctx context.Context, in *struct {
		ID int64 `path:"id"`
	}) (*struct{}, error) {
		existed, err := s.DB.DeleteKey(ctx, in.ID)
		if err != nil {
			return nil, internalErr("delete key", err)
		}
		if !existed {
			return nil, huma.Error404NotFound("key not found")
		}
		// Revoke immediately in-process; don't let the verifier's key cache
		// keep honoring tokens signed by the just-deleted key for its TTL.
		s.Auth.Invalidate()
		return nil, nil
	})

	huma.Register(api, op("getUsage", "GET", "/api/usage", auth.ScopeOrgRead,
		"Instance usage aggregates and disk headroom"),
		func(ctx context.Context, _ *struct{}) (*struct{ Body UsageOut }, error) {
			repos, packBytes, lfsBytes, err := s.DB.Usage(ctx)
			if err != nil {
				return nil, internalErr("usage", err)
			}
			total, free := s.Cache.DiskUsage()
			active := int64(0)
			if s.ActiveTransfers != nil {
				active = s.ActiveTransfers()
			}
			out := UsageOut{
				Repos: repos, PackBytes: packBytes, LFSBytes: lfsBytes,
				DiskTotalBytes: int64(total), DiskFreeBytes: int64(free),
				ActiveTransfers: active,
			}
			if ws, ok := s.DB.(interface{ Stats() repodb.WALStats }); ok {
				st := ws.Stats()
				out.WAL = &st
			}
			if s.GoReceiveStats != nil {
				gr := s.GoReceiveStats()
				out.GoReceive = &gr
			}
			if s.GoFetchStats != nil {
				gf := s.GoFetchStats()
				out.GoFetch = &gf
			}
			return &struct{ Body UsageOut }{out}, nil
		})
}
