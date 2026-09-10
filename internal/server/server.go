// Package server assembles the components into a runnable service; used by
// both `forged serve` and the e2e test suite.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/folsomintel/forge/internal/api"
	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/events"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/githttp"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/maintain"
	"github.com/folsomintel/forge/internal/netguard"
	"github.com/folsomintel/forge/internal/packstore"
	"github.com/folsomintel/forge/internal/ratelimit"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

type Server struct {
	Cfg        config.Config
	DB         repodb.DB
	Blobs      blobstore.Store
	Cache      *repocache.Cache
	Ingest     *ingest.Service
	Maintain   *maintain.Pipeline
	Auth       *auth.Verifier
	Mux        *http.ServeMux
	Worker     *webhook.Worker
	Stager     *ingest.Stager
	HookSocket string
	HookToken  string           // per-boot secret the pre-receive hook must present on the socket
	GitHTTP    *githttp.Handler // shared git-service path (HTTP + SSH transports)
	Events     *events.Hub      // ref-event bus (SSE); closed on drain
	draining   atomic.Bool
}

// SetDraining flips readiness off so the edge stops routing new requests
// before a graceful shutdown drains the in-flight ones.
func (s *Server) SetDraining(v bool) {
	s.draining.Store(v)
	// End every SSE stream so graceful shutdown never waits on idle
	// event subscribers.
	if v && s.Events != nil {
		s.Events.Close()
	}
}

// ActiveTransfers is the count of in-flight git transfers (drain gate).
func (s *Server) ActiveTransfers() int64 {
	if s.GitHTTP == nil {
		return 0
	}
	return s.GitHTTP.ActiveTransfers()
}

func OpenStores(cfg config.Config) (*repodb.WAL, blobstore.Store, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, err
	}
	db, err := repodb.OpenSQLite(cfg.DBPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open metadata db: %w", err)
	}
	var blobs blobstore.Store
	switch cfg.StoreKind {
	case "local":
		blobs, err = blobstore.NewLocal(cfg.StorePath)
	case "s3":
		blobs, err = blobstore.NewS3(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3UseSSL)
	default:
		err = fmt.Errorf("unknown FORGE_STORE %q (want local or s3)", cfg.StoreKind)
	}
	if err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("open blob store: %w", err)
	}
	return repodb.NewWAL(db, blobs), blobs, nil
}

const preReceiveScript = `#!/bin/sh
exec "$FORGE_SELF" hook pre-receive
`

// Build wires everything up. SelfPath must point at a forged binary (the
// pre-receive hook re-invokes it); empty means the current executable.
func Build(cfg config.Config) (*Server, error) {
	db, blobs, err := OpenStores(cfg)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.HooksDir(), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.CacheDir(), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(cfg.HooksDir(), "pre-receive"), []byte(preReceiveScript), 0o755); err != nil {
		return nil, err
	}
	self := cfg.SelfPath
	if self == "" {
		if self, err = os.Executable(); err != nil {
			return nil, err
		}
	}

	bundleSecret, err := loadOrCreateSecret(filepath.Join(cfg.DataDir, "bundle-secret"))
	if err != nil {
		return nil, err
	}

	// RegenIdx MUST be wired before RecoverRepos: recovery replays inline
	// WAL entries whose idx is derived from the pack, and flushPack needs
	// RegenIdx to write the standalone .idx. Wiring it afterwards made the
	// disaster-restore path silently fail to materialize every inline pack.
	db.RegenIdx = func(pack []byte) ([]byte, error) {
		p, _, err := ingest.ReadPackThin(pack, nil)
		if err != nil {
			return nil, err
		}
		return ingest.WriteIdxV2(p)
	}

	// Empty index + populated bucket = fresh volume after a disaster (or a
	// recreate/migration - this IS the restore path): resurrect everything
	// from the bucket before serving.
	if repos, err := db.ListRepos(context.Background()); err == nil && len(repos) == 0 {
		if n, err := db.RecoverRepos(context.Background()); err != nil {
			slog.Warn("refs-wal recovery", "err", err)
		} else if n > 0 {
			slog.Info("refs-wal: recovered repos from bucket", "count", n)
		}
	}
	// Seed bucket truth for repos that predate the WAL (idempotent).
	go func() {
		if err := db.Backfill(context.Background()); err != nil {
			slog.Warn("refs-wal backfill", "err", err)
		}
	}()

	// One process-wide bound on forked git subprocesses (API fallbacks,
	// maintenance, merges) - same cap as the wire paths' fork gate.
	n := cfg.MaxGitForks
	if n <= 0 {
		n = max(4, 2*runtime.NumCPU())
	}
	gitcmd.SetForkGate(n)

	cache := repocache.New(cfg.CacheDir(), db, blobs)
	cache.Replica = cfg.Replica // followers refresh from the WAL before serving reads
	if cfg.RemotePlacementBytes > 0 {
		// Remote reader: serve big repos' pack data from the bucket in blocks.
		// Requires the Go-native serve paths - a forked git upload-pack on a
		// repo whose .pack isn't local would abort ("repository corruption") -
		// so force GoFetch on. Remote placement only applies to replicas.
		cache.RemoteBytes = cfg.RemotePlacementBytes
		cache.Blocks = packstore.NewCache(cfg.BlockCacheBytes)
		if !cfg.GoFetch {
			cfg.GoFetch = true
			slog.Info("remote placement forces GoFetch on (Go-native clone serving)")
		}
		slog.Info("remote pack placement enabled", "threshold_bytes", cfg.RemotePlacementBytes, "block_cache_bytes", cfg.BlockCacheBytes)
	}
	pipeline := &maintain.Pipeline{
		Cache: cache, DB: db, Blobs: blobs,
		PublicURL:    strings.TrimSuffix(cfg.PublicURL, "/"),
		BundleSecret: bundleSecret,
		MinPacks:     cfg.MaintainMinPacks,
		BuildHistory: cfg.BuildHistoryPack,
	}
	cache.Hydrate = pipeline.Hydrate
	if cfg.WebhookAllowPrivate {
		// This flag disables the SSRF guard for BOTH webhook delivery and
		// import downloads - safe only for dev/self-host. Make it impossible
		// to run multi-tenant with it on by accident and not notice.
		slog.Warn("SSRF GUARD DISABLED: FORGE_WEBHOOK_ALLOW_PRIVATE=true - webhooks and imports can reach private/reserved addresses; never use in a multi-tenant deployment")
	}
	svc := &ingest.Service{
		Cache: cache, DB: db, Blobs: blobs,
		ImportClient: netguard.Client(2*time.Hour, cfg.WebhookAllowPrivate, cfg.EgressProxy),
		Maintain:     pipeline,
	}
	verifier := &auth.Verifier{DB: db}
	mux := http.NewServeMux()

	var stager *ingest.Stager
	if !cfg.NoStagedPush {
		stager = &ingest.Stager{Blobs: blobs}
	}
	srv := &Server{
		Cfg: cfg, DB: db, Blobs: blobs, Cache: cache,
		Ingest: svc, Maintain: pipeline, Auth: verifier, Mux: mux, Stager: stager,
	}
	// Per-boot secret the pre-receive hook presents on the socket (defense in
	// depth over the 0o600 permission). Generated fresh each boot, passed to
	// git (and thus the hook) via env, never written to disk.
	tokBuf := make([]byte, 32)
	rand.Read(tokBuf)
	srv.HookToken = hex.EncodeToString(tokBuf)
	if err := srv.startHookSocket(); err != nil {
		return nil, fmt.Errorf("hook socket: %w", err)
	}
	hookEnv := append(cfg.Env(),
		"FORGE_HOOK_SOCKET="+srv.HookSocket,
		"FORGE_HOOK_TOKEN="+srv.HookToken,
	)

	apiLimit := ratelimit.New(cfg.RateAPI, int(cfg.RateAPI*4)+1)
	gitLimit := ratelimit.New(cfg.RateGit, int(cfg.RateGit*4)+1)
	gh := &githttp.Handler{
		Cache: cache, DB: db, Blobs: blobs, Bundles: pipeline, Stager: stager, Auth: verifier,
		HooksDir: cfg.HooksDir(), SelfPath: self, HookEnv: hookEnv, Limit: gitLimit,
		MaxPushBytes: cfg.MaxPushBytes, GoReceive: cfg.GoReceive,
		MaxGitForks: cfg.MaxGitForks, ForkAdvertisement: cfg.ForkAdvertisement,
		GoFetch: cfg.GoFetch,
		NudgeMaintain: func(repo string) {
			pipeline.NudgeIfNeeded(context.Background(), repo)
		},
	}
	gh.Register(mux)
	gh.RegisterLFS(mux)
	srv.GitHTTP = gh
	// Ref-event bus: every durable ref transaction (all push paths funnel
	// through this WAL) fans out to SSE subscribers.
	hub := events.NewHub()
	srv.Events = hub
	db.OnCommit = func(repoID string, seq, ts int64, updates []repodb.RefUpdate) {
		evs := make([]events.Event, len(updates))
		for i, u := range updates {
			evs[i] = events.Event{Repo: repoID, Seq: seq, Ref: u.Name, Old: u.Old, New: u.New, TS: ts}
		}
		hub.Publish(evs...)
	}
	goRecvStats := func() api.GoReceiveOut {
		s := gh.GoReceiveStats()
		return api.GoReceiveOut{
			Eligible: s.Eligible, FellBack: s.FellBack, Rejected: s.Rejected,
			StorageErred: s.StorageErred, FellBackBy: s.FellBackBy,
		}
	}
	goFetchStats := func() api.GoFetchOut {
		s := gh.GoFetchStatsSnapshot()
		return api.GoFetchOut{
			Eligible: s.Eligible, CloneStream: s.CloneStream,
			FellBack: s.FellBack, FellBackBy: s.FellBackBy,
		}
	}
	(&api.Server{DB: db, Auth: verifier, Cache: cache, Ingest: svc,
		Maintain: pipeline, Limit: apiLimit, ActiveTransfers: gh.ActiveTransfers,
		GoReceiveStats: goRecvStats, GoFetchStats: goFetchStats, Events: hub}).Register(mux)
	// Liveness: the process is up. Never fails while we can answer, so a
	// draining machine still reports healthz ok (it is alive, just closing).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	// Readiness: safe to route traffic here. 503 while draining (so the edge
	// stops sending new requests before shutdown) and if the object store -
	// the source of truth - is unreachable.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if srv.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if _, err := blobs.List(ctx, "_forge", ""); err != nil { // instance-global prefix
			http.Error(w, "storage unreachable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})

	srv.Worker = webhook.NewWorker(db, webhook.Options{
		AllowPrivate: cfg.WebhookAllowPrivate,
		ProxyURL:     cfg.EgressProxy,
	})
	return srv, nil
}

// Start runs the background workers until ctx is done. A replica is a
// read-only follower: it must NOT run maintenance (consolidate/sweep) or it
// would delete the primary's packs as "orphans" - only the primary mutates
// the bucket.
func (s *Server) Start(ctx context.Context) {
	if s.Cfg.Replica {
		go s.Cache.RunEviction(ctx, time.Minute, s.Cfg.CacheHighPct, s.Cfg.CacheLowPct)
		return
	}
	go s.Worker.Run(ctx)
	if s.Cfg.MaintainInterval > 0 {
		go s.Maintain.Worker(ctx, s.Cfg.MaintainInterval)
	}
	go s.Cache.RunEviction(ctx, time.Minute, s.Cfg.CacheHighPct, s.Cfg.CacheLowPct)
}

func (s *Server) Close() error { return s.DB.Close() }

// loadOrCreateSecret persists a random 32-byte secret across restarts.
func loadOrCreateSecret(path string) ([]byte, error) {
	if b, err := os.ReadFile(path); err == nil && len(b) >= 32 {
		return b, nil
	}
	b := make([]byte, 32)
	rand.Read(b)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, err
	}
	return b, nil
}
