// Package repodb is the transactional metadata store: repos, refs, the
// pack list, webhooks, and client public keys. Together with the blob
// store it is the source of truth - cache repos on disk are disposable
// materializations of it.
package repodb

import (
	"context"
	"errors"
	"regexp"
	"time"
)

const ZeroOID = "0000000000000000000000000000000000000000"

var (
	ErrNotFound  = errors.New("not found")
	ErrExists    = errors.New("already exists")
	ErrCASFailed = errors.New("ref update conflict")
)

var repoIDRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$`)

// ValidRepoID rejects anything that could escape a path or confuse a URL.
func ValidRepoID(id string) bool {
	return repoIDRe.MatchString(id) && id != ".." && id != "api"
}

type Repo struct {
	ID            string    `json:"id"`
	DefaultBranch string    `json:"default_branch"`
	Public        bool      `json:"public"`
	CreatedAt     time.Time `json:"created_at"`
}

type Ref struct {
	Name   string `json:"name"`
	Target string `json:"target"`
}

// RefUpdate is a compare-and-swap: Old is the expected current target
// (ZeroOID = ref must not exist), New is the desired target (ZeroOID =
// delete). A batch is applied atomically: all updates or none.
type RefUpdate struct {
	Name string
	Old  string
	New  string
}

type Pack struct {
	Name      string // "pack-<hash>" (no extension)
	SizeBytes int64
	Source    string // "receive" | "gc" | "import" | "fork"
	CreatedAt time.Time
	// BlobRepo is the repo prefix whose store holds this pack's blobs; ""
	// means self. Forks share the parent's immutable blobs until their
	// first consolidation rewrites everything under their own prefix.
	BlobRepo string
}

type Key struct {
	ID           int64
	Name         string
	PublicKeyPEM string
	// Scopes granted when this key authenticates over SSH. Empty means the
	// full historical grant (git:read git:write repo:write org:read), so
	// keys registered before per-key scopes keep working.
	Scopes []string
}

type Webhook struct {
	ID        string    `json:"id"`
	RepoID    string    `json:"-"`
	URL       string    `json:"url"`
	Secret    string    `json:"-"`
	Events    []string  `json:"events"` // e.g. ["push"]; "*" matches all
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

func (w *Webhook) Matches(event string) bool {
	if !w.Active {
		return false
	}
	for _, e := range w.Events {
		if e == event || e == "*" {
			return true
		}
	}
	return false
}

type Delivery struct {
	ID            string    `json:"id"`
	WebhookID     string    `json:"webhook_id"`
	Event         string    `json:"event"`
	Payload       string    `json:"-"`
	Status        string    `json:"status"` // pending | ok | failed
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastError     string    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`

	// Joined from the webhook for the delivery worker.
	URL    string `json:"-"`
	Secret string `json:"-"`
}

type LFSObject struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

type Import struct {
	RepoID     string     `json:"repo_id"`
	Status     string     `json:"status" enum:"running,done,error"`
	Error      string     `json:"error,omitempty"`
	Refs       int        `json:"refs"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// Event is an outbox entry paired with a ref update in UpdateRefs.
type Event struct {
	Name    string // e.g. "push"
	Payload []byte
}

// DB is the full metadata store, composed of role interfaces so callers
// can depend on (and alternate implementations can provide) exactly the
// slice they need - e.g. a bucket-backed RefStore with SQLite as index.
type DB interface {
	RepoStore
	RefStore
	PackIndex
	ImportStore
	KeyStore
	WebhookStore
	LFSIndex
	AuditStore

	// Usage returns billing-grade aggregates: repo count, pack bytes, LFS bytes.
	Usage(ctx context.Context) (repos int, packBytes, lfsBytes int64, err error)

	// ChangeToken returns a value that changes whenever any other connection
	// commits a write - the cache-invalidation signal for materialization.
	ChangeToken(ctx context.Context) (int64, error)

	// RepoChangeToken is ChangeToken narrowed to one repo: it moves only when
	// that repo's refs/packs/metadata change, so one repo's write no longer
	// invalidates every other repo's materialize fast path.
	RepoChangeToken(ctx context.Context, repoID string) (int64, error)

	Close() error
}

type RepoStore interface {
	CreateRepo(ctx context.Context, id, defaultBranch string) error
	GetRepo(ctx context.Context, id string) (*Repo, error)
	ListRepos(ctx context.Context) ([]Repo, error)
	// ListReposPage is keyset pagination: repos with id > after, up to limit
	// (0 = unbounded). The last row's id is the next page's cursor.
	ListReposPage(ctx context.Context, after string, limit int) ([]Repo, error)
	DeleteRepo(ctx context.Context, id string) error
	// ForkRepo creates dst as a zero-copy fork of src: refs copied
	// (ephemeral namespaces excluded), pack rows shared via BlobRepo.
	ForkRepo(ctx context.Context, srcID, dstID string) error
	// SetRepoPublic flips anonymous read access for a repo.
	SetRepoPublic(ctx context.Context, id string, public bool) error
}

type RefStore interface {
	ListRefs(ctx context.Context, repoID string) ([]Ref, error)
	// UpdateRefs applies the batch atomically; returns ErrCASFailed if any
	// update's Old doesn't match the current state. events (nil, or one per
	// update) are enqueued to matching webhooks in the SAME transaction -
	// the transactional outbox: a committed ref move implies its event row.
	UpdateRefs(ctx context.Context, repoID string, updates []RefUpdate, events []Event) error
}

type PackIndex interface {
	ListPacks(ctx context.Context, repoID string) ([]Pack, error)
	AddPacks(ctx context.Context, repoID string, packs []Pack) error
	// ReplacePacks atomically swaps old pack rows for new ones (the
	// consolidation stage of maintenance).
	ReplacePacks(ctx context.Context, repoID string, oldNames []string, newPacks []Pack) error
	// ReposNeedingCompaction lists repos whose pack count is >= minPacks.
	ReposNeedingCompaction(ctx context.Context, minPacks int) ([]string, error)
	// BlobReferenced reports whether any repo (a zero-copy fork) still points
	// at a pack blob physically owned by ownerRepo. Guards deletion: shared
	// fork blobs must never be swept while a dependent references them.
	BlobReferenced(ctx context.Context, ownerRepo, packName string) (bool, error)
}

type ImportStore interface {
	SetImport(ctx context.Context, repoID, status, errMsg string, refs int) error
	GetImport(ctx context.Context, repoID string) (*Import, error)
}

type KeyStore interface {
	AddKey(ctx context.Context, name, publicKeyPEM string, scopes []string) (int64, error)
	ListKeys(ctx context.Context) ([]Key, error)
	// DeleteKey revokes a registered key by id; existed is false if no such
	// key. It is the only way to retire a compromised or stale credential.
	DeleteKey(ctx context.Context, id int64) (existed bool, err error)
}

type WebhookStore interface {
	CreateWebhook(ctx context.Context, w *Webhook) error
	ListWebhooks(ctx context.Context, repoID string) ([]Webhook, error)
	GetWebhook(ctx context.Context, repoID, id string) (*Webhook, error)
	UpdateWebhook(ctx context.Context, w *Webhook) error
	DeleteWebhook(ctx context.Context, repoID, id string) error

	// EnqueueEvent creates one pending delivery per active webhook on the
	// repo whose event list matches. Safe from any process (hook or server).
	EnqueueEvent(ctx context.Context, repoID, event string, payload []byte) error
	DueDeliveries(ctx context.Context, now time.Time, limit int) ([]Delivery, error)
	MarkDelivery(ctx context.Context, id, status string, attempts int, nextAttempt time.Time, lastErr string) error
	ListDeliveries(ctx context.Context, repoID, webhookID string, limit int) ([]Delivery, error)
	// PruneDeliveries removes settled (ok/failed) deliveries created before
	// the cutoff; returns how many were removed.
	PruneDeliveries(ctx context.Context, before time.Time) (int64, error)
}

// AuditStore is the tenant's operational trail: repo lifecycle, key
// registration, webhook changes. Ops trail, not truth - it lives in the
// index only.
type AuditStore interface {
	AddAudit(ctx context.Context, e AuditEntry) error
	ListAudit(ctx context.Context, action, q string, limit int) ([]AuditEntry, error)
}

type AuditEntry struct {
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	Target string    `json:"target"`
	Detail string    `json:"detail,omitempty"`
}

type LFSIndex interface {
	AddLFSObject(ctx context.Context, repoID string, obj LFSObject) error
	GetLFSObject(ctx context.Context, repoID, oid string) (*LFSObject, error)
}
