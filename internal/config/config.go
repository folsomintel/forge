// Package config loads server configuration from FORGE_* environment
// variables. The same env is inherited by git subprocesses, which is how the
// pre-receive hook (re-invoked as `forged hook pre-receive`) finds the
// metadata DB and pack store without any extra plumbing.
package config

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	Addr    string // listen address
	SSHAddr string // git-over-SSH listen address ("" = disabled)
	Replica bool   // read-only follower: refresh index from the WAL before serving reads
	DataDir string // cache repos, hooks, default locations for db/packs
	DBPath  string // sqlite metadata db

	StoreKind string // "local" or "s3"
	StorePath string // local pack store root (StoreKind=local)
	SelfPath  string // forged binary path for hooks ("" = current executable)

	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool

	// WebhookAllowPrivate disables the SSRF guard on webhook deliveries
	// (loopback/private/link-local targets). Dev/self-host convenience;
	// keep false anywhere multi-tenant.
	WebhookAllowPrivate bool
	// EgressProxy routes webhook deliveries through a CONNECT proxy
	// (e.g. Stripe Smokescreen) for network-layer egress policy.
	EgressProxy string

	// MaintainMinPacks triggers maintenance (consolidate/derive/advertise/
	// sweep) when a repo's pack count reaches this many - both for the
	// periodic worker scan and for post-push nudges. MaintainInterval is
	// the worker cadence; 0 disables the periodic worker (nudges and the
	// hidden ops endpoint still work).
	MaintainMinPacks int
	MaintainInterval time.Duration

	// Per-subject request budgets (requests/second; 0 disables). Git ops
	// are budgeted separately from REST calls - a clone costs more than a
	// JSON read.
	RateAPI float64
	RateGit float64

	// Cache eviction watermarks (percent disk used on the cache volume).
	CacheHighPct int
	CacheLowPct  int

	// Remote placement (walgit remote reader): when a repo's total pack bytes
	// exceed RemotePlacementBytes (>0), its large packs are served from the
	// bucket in blocks instead of materialized locally, so a repo bigger than
	// local disk still serves reads. 0 = disabled. BlockCacheBytes bounds the
	// process-wide pack-block LRU.
	RemotePlacementBytes int64
	BlockCacheBytes      int64
	// BuildHistoryPack publishes a blobless history pack each maintenance run
	// (Phase 3); enable on primaries whose replicas use remote placement.
	BuildHistoryPack bool

	// PublicURL is this instance's externally reachable base URL; enables
	// bundle-uri clone offload when set.
	PublicURL string

	// MaxPushBytes rejects receive-pack inputs above this size with a loud
	// client-visible error (receive.maxInputSize) instead of letting an
	// oversized push OOM the machine. 0 = unlimited; offline import is the
	// documented path for monster migrations. The control plane sets this
	// to the tenant's memory size.
	MaxPushBytes int64

	// GoReceive enables the fork-free receive fast path (small non-delta
	// pushes ingested in Go; everything else falls back to git).
	GoReceive bool

	// MaxGitForks bounds concurrent forked git processes so a client
	// stampede sheds load (503) instead of OOMing a small machine.
	// 0 = max(4, 2*NumCPU).
	MaxGitForks int

	// ForkAdvertisement restores the fork-git info/refs path (escape hatch
	// for the Go-native advertisement).
	ForkAdvertisement bool

	// GoFetch enables the fork-free fetch fast path (incremental chains
	// from stored receive packs; clone passthrough of consolidated packs).
	GoFetch bool

	// NoStagedPush disables teeing incoming wire packs to the blob store
	// during the transfer (which turns most big-push uploads into
	// server-side copies). Zero value = staging on.
	NoStagedPush bool
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func FromEnv() Config {
	data := env("FORGE_DATA_DIR", "./data")
	return Config{
		Addr:        env("FORGE_ADDR", "127.0.0.1:8347"),
		SSHAddr:     os.Getenv("FORGE_SSH_ADDR"),
		Replica:     os.Getenv("FORGE_REPLICA") == "true",
		DataDir:     data,
		DBPath:      env("FORGE_DB", filepath.Join(data, "forge.db")),
		StoreKind:   env("FORGE_STORE", "local"),
		StorePath:   env("FORGE_STORE_PATH", filepath.Join(data, "packs")),
		SelfPath:    os.Getenv("FORGE_SELF_BIN"),
		S3Endpoint:  os.Getenv("FORGE_S3_ENDPOINT"),
		S3Bucket:    os.Getenv("FORGE_S3_BUCKET"),
		S3AccessKey: os.Getenv("FORGE_S3_ACCESS_KEY"),
		S3SecretKey: os.Getenv("FORGE_S3_SECRET_KEY"),
		S3UseSSL:    os.Getenv("FORGE_S3_USE_SSL") == "true",

		WebhookAllowPrivate: os.Getenv("FORGE_WEBHOOK_ALLOW_PRIVATE") == "true",
		EgressProxy:         os.Getenv("FORGE_EGRESS_PROXY"),

		// New names, with fallback to the pre-rename env keys still set on
		// deployed machines.
		MaintainMinPacks: envInt("FORGE_MAINTAIN_MIN_PACKS", envInt("FORGE_COMPACT_MIN_PACKS", 10)),
		MaintainInterval: envDuration("FORGE_MAINTAIN_INTERVAL", envDuration("FORGE_COMPACT_INTERVAL", time.Minute)),

		RateAPI: envFloat("FORGE_RATE_API", 50),
		RateGit: envFloat("FORGE_RATE_GIT", 10),

		RemotePlacementBytes: envInt64("FORGE_REMOTE_PLACEMENT_BYTES", 0),
		BlockCacheBytes:      envInt64("FORGE_BLOCK_CACHE_BYTES", 256<<20),
		BuildHistoryPack:     env("FORGE_BUILD_HISTORY_PACK", "") == "true",
		CacheHighPct:         envInt("FORGE_CACHE_HIGH_PCT", 85),
		CacheLowPct:          envInt("FORGE_CACHE_LOW_PCT", 70),

		PublicURL: os.Getenv("FORGE_PUBLIC_URL"),

		NoStagedPush:      os.Getenv("FORGE_STAGED_PUSH") == "false",
		GoReceive:         os.Getenv("FORGE_GORECEIVE") == "true",
		MaxGitForks:       envInt("FORGE_MAX_GIT_FORKS", 0),
		ForkAdvertisement: os.Getenv("FORGE_FORK_ADVERTISEMENT") == "true",
		GoFetch:           os.Getenv("FORGE_GOFETCH") == "true",
		MaxPushBytes:      envInt64("FORGE_MAX_PUSH_BYTES", 0),
	}
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if raw := os.Getenv(key); raw != "" {
		if v, err := time.ParseDuration(raw); err == nil {
			return v
		}
	}
	return def
}

func (c Config) CacheDir() string { return filepath.Join(c.DataDir, "cache") }
func (c Config) HooksDir() string { return filepath.Join(c.DataDir, "hooks") }

// Env encodes the config for subprocesses (the pre-receive hook re-derives
// its config from these). Explicit, so a programmatically-configured server
// (tests) behaves identically to an env-configured one.
func (c Config) Env() []string {
	ssl := "false"
	if c.S3UseSSL {
		ssl = "true"
	}
	return []string{
		"FORGE_DATA_DIR=" + c.DataDir,
		"FORGE_DB=" + c.DBPath,
		"FORGE_STORE=" + c.StoreKind,
		"FORGE_STORE_PATH=" + c.StorePath,
		"FORGE_S3_ENDPOINT=" + c.S3Endpoint,
		"FORGE_S3_BUCKET=" + c.S3Bucket,
		"FORGE_S3_ACCESS_KEY=" + c.S3AccessKey,
		"FORGE_S3_SECRET_KEY=" + c.S3SecretKey,
		"FORGE_S3_USE_SSL=" + ssl,
	}
}

func envInt64(key string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(key), 10, 64); err == nil {
		return v
	}
	return def
}
