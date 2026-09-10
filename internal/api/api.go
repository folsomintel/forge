// Package api is the REST surface, served through Huma: handlers are typed
// operations and the OpenAPI spec is generated from the code — it cannot
// drift from the implementation. The git wire protocol and LFS live in
// githttp on the same mux and are deliberately outside the spec.
package api

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/events"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/maintain"
	"github.com/folsomintel/forge/internal/ratelimit"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

type Server struct {
	DB       repodb.DB
	Auth     *auth.Verifier
	Cache    *repocache.Cache
	Ingest   *ingest.Service
	Maintain *maintain.Pipeline
	Limit    *ratelimit.Limiter
	// ActiveTransfers reports in-flight git transfers (githttp), for the
	// drain-before-restart signal in /api/usage.
	ActiveTransfers func() int64
	// GoReceiveStats, when set, reports fork-free receive fast-path
	// decision counts for /api/usage, including the fallback-cause
	// breakdown that makes a non-engaging fast path diagnosable.
	GoReceiveStats func() GoReceiveOut
	// GoFetchStats, when set, reports the fork-free fetch counters.
	GoFetchStats func() GoFetchOut
	// Events, when set, enables the SSE ref-event streams.
	Events *events.Hub
}

// Register mounts the typed API on the mux and returns the generated spec.
func (s *Server) Register(mux *http.ServeMux) *huma.OpenAPI {
	registry := huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	config := huma.Config{
		OpenAPI: &huma.OpenAPI{
			OpenAPI:    "3.1.0",
			Info:       &huma.Info{Title: "forge", Version: "0.2.0"},
			Components: &huma.Components{Schemas: registry},
		},
		OpenAPIPath: "/openapi",
		DocsPath:    "/docs",
		Formats:     huma.DefaultFormats,
	}
	config.Info.Description = "Headless git hosting backed by object storage. " +
		"Auth: self-signed JWTs (ES256/RS256) against registered public keys; " +
		"scopes git:read, git:write (write implies read), repo:write, org:read. " +
		"The git smart HTTP protocol lives at /{repo}.git (ephemeral view: /{repo}+ephemeral.git) " +
		"and Git LFS at /{repo}.git/info/lfs/objects/batch — outside this spec."
	config.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"bearer": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
	}

	api := humago.New(mux, config)
	api.UseMiddleware(func(hc huma.Context, next func(huma.Context)) {
		op := hc.Operation()
		scope, _ := op.Metadata["scope"].(string)
		if scope == "" {
			next(hc)
			return
		}
		claims, err := s.Auth.VerifyAuthorization(hc.Context(), hc.Header("Authorization"))
		if err != nil || !claims.Allow(scope, hc.Param("id")) {
			// Public repos answer read-scoped operations anonymously: the
			// whole read API becomes the public explore surface.
			if readScope(scope) {
				if id := hc.Param("id"); id != "" {
					if repo, rerr := s.DB.GetRepo(hc.Context(), id); rerr == nil && repo.Public {
						subject := "anon:" + hc.Header("Fly-Client-IP")
						if !s.Limit.Allow(subject) {
							hc.SetHeader("Retry-After", "1")
							huma.WriteErr(api, hc, http.StatusTooManyRequests, "rate limit exceeded - retry after a moment")
							return
						}
						next(hc)
						return
					}
				}
			}
			huma.WriteErr(api, hc, http.StatusUnauthorized, "authentication required")
			return
		}
		if !s.Limit.Allow(claims.Subject) {
			hc.SetHeader("Retry-After", "1")
			huma.WriteErr(api, hc, http.StatusTooManyRequests, "rate limit exceeded - retry after a moment")
			return
		}
		next(huma.WithValue(hc, claimsKey{}, claims))
	})

	// Conditional GETs for pollers: the ref-derived read ops carry an ETag
	// keyed by the repo's change token, so an agent polling
	// contents/commits/branches pays a 304 (auth + one token read) instead
	// of a tree walk. OPT-IN, not opt-out: the token only moves on
	// refs/packs/metadata changes, so applying it to import-status or
	// webhook-delivery reads (whose state changes with no token bump) would
	// pin a poller to a permanent 304. The boot id keys the token to this
	// process so a restart can never alias a stale ETag.
	bootID := make([]byte, 6)
	rand.Read(bootID)
	api.UseMiddleware(func(hc huma.Context, next func(huma.Context)) {
		id := hc.Param("id")
		// Replicas must run the handler (its freshness gate advances the
		// local index); a 304 here would pin the client to pre-push data.
		if hc.Method() != http.MethodGet || id == "" || s.Cache.Replica || !etagEligible(hc.URL().Path) {
			next(hc)
			return
		}
		rev, err := s.DB.RepoChangeToken(hc.Context(), id)
		if err != nil {
			next(hc)
			return
		}
		sum := fnv.New64a()
		u := hc.URL()
		io.WriteString(sum, u.RequestURI())
		etag := fmt.Sprintf(`W/"%x-%d-%x"`, bootID, rev, sum.Sum64())
		hc.SetHeader("ETag", etag) // RFC 9110: a 304 SHOULD carry the ETag too
		if hc.Header("If-None-Match") == etag {
			hc.SetStatus(http.StatusNotModified)
			return
		}
		next(hc)
	})

	s.registerRepos(api)
	s.registerMaintenance(api)
	s.registerImports(api)
	s.registerInstance(api)
	s.registerContents(api)
	s.registerGitData(api)
	s.registerBranches(api)
	s.registerCommits(api)
	s.registerMerge(api)
	s.registerWebhooks(api)
	s.registerEvents(api)
	return api.OpenAPI()
}

// op builds the common operation skeleton; scope drives the auth middleware.
func op(id, method, path, scope, summary string) huma.Operation {
	tag := "repos"
	if rest, ok := strings.CutPrefix(path, "/api/repos/{id}/"); ok {
		tag = strings.SplitN(rest, "/", 2)[0]
	}
	return huma.Operation{
		OperationID: id, Method: method, Path: path, Summary: summary,
		Metadata: map[string]any{"scope": scope},
		Security: []map[string][]string{{"bearer": {}}},
		Tags:     []string{tag},
	}
}

// readScope marks operations safe for anonymous access on public repos.
func readScope(scope string) bool {
	return scope == auth.ScopeGitRead || scope == auth.ScopeOrgRead
}

// etagEligible marks the read paths whose response is a pure function of
// the repo's ref/pack state (what RepoChangeToken tracks). Only these get
// conditional-GET handling; import status, webhook deliveries, usage, and
// the /raw + /events streams are deliberately excluded.
func etagEligible(path string) bool {
	for _, seg := range []string{"/contents", "/commits", "/branches", "/tags", "/tree", "/git/", "/diff"} {
		if strings.Contains(path, seg) {
			return true
		}
	}
	return false
}

type claimsKey struct{}

func subject(ctx context.Context) string {
	if c, ok := ctx.Value(claimsKey{}).(*auth.Claims); ok {
		return c.Subject
	}
	return ""
}

// --- shared repo helpers (transport-free) ---

// exec materializes the repo and returns an Exec holding the read lock.
// MaterializeServe never queues readers behind a repack or another
// materialization (stale-but-serving; the DB CAS arbitrates writes), so a
// burst of API readers cannot convoy on the repo write lock.
func (s *Server) exec(ctx context.Context, repoID string) (*gitcmd.Exec, func(), error) {
	dir, err := s.Cache.MaterializeServe(ctx, repoID)
	if errors.Is(err, repodb.ErrNotFound) {
		return nil, nil, huma.Error404NotFound("repository not found")
	}
	if err != nil {
		slog.Error("materialize", "repo", repoID, "err", err)
		return nil, nil, huma.Error500InternalServerError("internal error")
	}
	lock := s.Cache.Lock(repoID)
	lock.RLock()
	return &gitcmd.Exec{Ctx: ctx, Dir: dir}, lock.RUnlock, nil
}

// writeCommit runs a CommitSpec (ingest takes the repo write lock); the
// push event commits transactionally with the ref CAS.
func (s *Server) writeCommit(ctx context.Context, spec ingest.CommitSpec) (*gitcmd.Commit, error) {
	spec.Pusher = subject(ctx)
	return s.Ingest.Commit(ctx, spec)
}

// casRef updates a single ref (no new objects); outbox event included.
func (s *Server) casRef(ctx context.Context, repoID string, u repodb.RefUpdate) error {
	events := []repodb.Event{webhook.PushEventFor(repoID, u, subject(ctx))}
	return s.DB.UpdateRefs(ctx, repoID, []repodb.RefUpdate{u}, events)
}

// resolveRev resolves branch/tag/full-ref/oid using the DB as ref truth.
func (s *Server) resolveRev(ctx context.Context, repoID, rev string, ephemeral bool) (oid, fullRef string, err error) {
	if rev == "" {
		repo, err := s.DB.GetRepo(ctx, repoID)
		if err != nil {
			return "", "", err
		}
		rev = repo.DefaultBranch
	}
	refs, err := s.DB.ListRefs(ctx, repoID)
	if err != nil {
		return "", "", err
	}
	byName := map[string]string{}
	for _, ref := range refs {
		byName[ref.Name] = ref.Target
	}
	for _, cand := range []string{rev, repodb.BranchRef(rev, ephemeral), repodb.TagRef(rev)} {
		if target, ok := byName[cand]; ok {
			return target, cand, nil
		}
	}
	if len(rev) >= 4 && len(rev) <= 64 && isHex(rev) {
		return rev, "", nil
	}
	return "", "", repodb.ErrNotFound
}

func isHex(s string) bool {
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// --- shared error mapping ---

func isCAS(err error) bool { return errors.Is(err, repodb.ErrCASFailed) }

// audit records an operational event with the caller as actor;
// best-effort by design.
func (s *Server) audit(ctx context.Context, action, target, detail string) {
	actor := subject(ctx)
	if actor == "" {
		actor = "unknown"
	}
	if err := s.DB.AddAudit(ctx, repodb.AuditEntry{
		Actor: actor, Action: action, Target: target, Detail: detail,
	}); err != nil {
		slog.Warn("audit", "action", action, "err", err)
	}
}

func internalErr(what string, err error) error {
	slog.Error(what, "err", err)
	return huma.Error500InternalServerError("internal error")
}

// serveImmutable sets a strong ETag + long immutable Cache-Control for
// content addressed by an immutable id (a blob/commit OID: the content
// cannot change without the id changing). If the client already has this
// exact object (If-None-Match), it writes a 304 and returns true so the
// caller streams no body — turning an agent's repeated read into a ~free
// revalidation. Only safe on genuinely content-addressed responses.
// serveImmutable sets an OID-keyed ETag + long cache on an immutable blob
// response, returning true when it answered 304. public gates the cache
// scope: a private repo's bytes must be marked `private` (never `public`,
// which authorizes a shared proxy to store and re-serve them to a client
// that never authenticated), and Vary: Authorization keeps even a private
// cache from serving one token's response to another.
func serveImmutable(hc huma.Context, oid string, public bool) bool {
	etag := `"` + oid + `"`
	hc.SetHeader("ETag", etag)
	hc.SetHeader("Vary", "Authorization")
	if public {
		hc.SetHeader("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		hc.SetHeader("Cache-Control", "private, max-age=31536000, immutable")
	}
	if inm := hc.Header("If-None-Match"); inm == etag || inm == oid {
		hc.SetStatus(http.StatusNotModified)
		return true
	}
	return false
}

// repoPublic reports whether a repo is public (best-effort: a lookup error
// is treated as private, the safe default for cache-scope decisions).
func (s *Server) repoPublic(ctx context.Context, id string) bool {
	repo, err := s.DB.GetRepo(ctx, id)
	return err == nil && repo.Public
}
