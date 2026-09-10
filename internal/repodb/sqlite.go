package repodb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// SQLite implements DB. WAL mode + busy_timeout make it safe for the
// two-process access pattern (server + pre-receive hook).
type SQLite struct {
	db *sql.DB
	// verConn is a dedicated connection: PRAGMA data_version is relative to
	// a connection and changes when any OTHER connection commits - including
	// the pre-receive hook process.
	verConn *sql.Conn
	verMu   sync.Mutex

	// repoRev is a per-repo change counter (repoID -> *atomic.Int64) bumped
	// by every mutation that touches a repo's refs/packs/metadata. It backs
	// the cache's materialize fast path so a write to one repo no longer
	// invalidates every other repo's cache. In-memory by design: it resets
	// with the process, in lockstep with the cache's own synced state, and
	// every mutation funnels through these SQLite methods so it cannot miss.
	repoRev sync.Map
}

const schema = `
CREATE TABLE IF NOT EXISTS repos (
	id             TEXT PRIMARY KEY,
	default_branch TEXT NOT NULL,
	created_at     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS refs (
	repo_id TEXT NOT NULL,
	name    TEXT NOT NULL,
	target  TEXT NOT NULL,
	PRIMARY KEY (repo_id, name)
);
CREATE TABLE IF NOT EXISTS packs (
	repo_id    TEXT NOT NULL,
	name       TEXT NOT NULL,
	size_bytes INTEGER NOT NULL,
	source     TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (repo_id, name)
);
CREATE TABLE IF NOT EXISTS keys (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	name           TEXT NOT NULL,
	public_key_pem TEXT NOT NULL,
	created_at     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS webhooks (
	id         TEXT PRIMARY KEY,
	repo_id    TEXT NOT NULL,
	url        TEXT NOT NULL,
	secret     TEXT NOT NULL,
	events     TEXT NOT NULL,
	active     INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
	id              TEXT PRIMARY KEY,
	webhook_id      TEXT NOT NULL,
	repo_id         TEXT NOT NULL,
	event           TEXT NOT NULL,
	payload         TEXT NOT NULL,
	status          TEXT NOT NULL,
	attempts        INTEGER NOT NULL,
	next_attempt_at INTEGER NOT NULL,
	last_error      TEXT NOT NULL,
	created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries (status, next_attempt_at);
CREATE TABLE IF NOT EXISTS imports (
	repo_id     TEXT PRIMARY KEY,
	status      TEXT NOT NULL,
	error       TEXT NOT NULL DEFAULT '',
	refs        INTEGER NOT NULL DEFAULT 0,
	started_at  INTEGER NOT NULL,
	finished_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit_log (
	id      INTEGER PRIMARY KEY AUTOINCREMENT,
	at      INTEGER NOT NULL,
	actor   TEXT NOT NULL,
	action  TEXT NOT NULL,
	target  TEXT NOT NULL,
	detail  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_at ON audit_log (at DESC);
CREATE TABLE IF NOT EXISTS wal_state (
	repo_id TEXT PRIMARY KEY,
	seq     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS lfs_objects (
	repo_id    TEXT NOT NULL,
	oid        TEXT NOT NULL,
	size       INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (repo_id, oid)
);
`

func OpenSQLite(path string) (*SQLite, error) {
	// _txlock=immediate: every transaction takes the write lock up front, so
	// concurrent writers queue on busy_timeout instead of hitting the
	// deferred-tx upgrade path, which returns SQLITE_BUSY without waiting.
	// synchronous(NORMAL): this DB is a disposable index of the bucket -
	// refs and events live in WAL entries and replay on the next sync, so
	// a power-loss losing the last checkpoint window heals itself. FULL's
	// per-commit fsync (brutal on macOS) buys nothing here.
	dsn := fmt.Sprintf("file:%s?_txlock=immediate&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// modernc sqlite serializes writes per connection; keep the pool small so
	// SQLITE_BUSY windows stay short.
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	// Additive migrations for pre-existing tenant DBs; duplicate-column
	// errors mean already applied.
	if _, err := db.Exec(`ALTER TABLE packs ADD COLUMN blob_repo TEXT NOT NULL DEFAULT ''`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("migrate packs.blob_repo: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE repos ADD COLUMN public INTEGER NOT NULL DEFAULT 0`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("migrate repos.public: %w", err)
	}
	// Per-key SSH scopes; empty means the full historical grant (see
	// AuthorizeSSHKey), so existing keys keep working.
	if _, err := db.Exec(`ALTER TABLE keys ADD COLUMN scopes TEXT NOT NULL DEFAULT ''`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("migrate keys.scopes: %w", err)
	}
	// Inline-pack flush watermark: entries <= flushed_seq have had their
	// embedded packs written as standalone blobs. -1 = repo has never
	// carried an inline pack (all history is pack-free entries).
	if _, err := db.Exec(`ALTER TABLE wal_state ADD COLUMN flushed_seq INTEGER NOT NULL DEFAULT -1`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column") {
		db.Close()
		return nil, fmt.Errorf("migrate wal_state.flushed_seq: %w", err)
	}
	s := &SQLite{db: db}
	if conn, err := db.Conn(context.Background()); err == nil {
		s.verConn = conn
	}
	return s, nil
}

func (s *SQLite) ChangeToken(ctx context.Context) (int64, error) {
	s.verMu.Lock()
	defer s.verMu.Unlock()
	if s.verConn == nil {
		return 0, fmt.Errorf("no version connection")
	}
	var v int64
	err := s.verConn.QueryRowContext(ctx, `PRAGMA data_version`).Scan(&v)
	return v, err
}

// RepoChangeToken returns a value that changes whenever repoID's refs,
// packs, or metadata change - and, unlike ChangeToken, is unmoved by writes
// to other repos. Cheap (a single atomic load).
func (s *SQLite) RepoChangeToken(ctx context.Context, repoID string) (int64, error) {
	return s.repoCounter(repoID).Load(), nil
}

func (s *SQLite) repoCounter(repoID string) *atomic.Int64 {
	v, _ := s.repoRev.LoadOrStore(repoID, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// bumpRev records a mutation of repoID. Called on the success path of every
// method that changes a repo's refs, packs, or metadata.
func (s *SQLite) bumpRev(repoID string) { s.repoCounter(repoID).Add(1) }

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) CreateRepo(ctx context.Context, id, defaultBranch string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO repos (id, default_branch, created_at) VALUES (?, ?, ?)`,
		id, defaultBranch, time.Now().Unix())
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return ErrExists
	}
	if err == nil {
		s.bumpRev(id)
	}
	return err
}

func (s *SQLite) GetRepo(ctx context.Context, id string) (*Repo, error) {
	var r Repo
	var created int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, default_branch, created_at, public FROM repos WHERE id = ?`, id).
		Scan(&r.ID, &r.DefaultBranch, &created, &r.Public)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.CreatedAt = time.Unix(created, 0).UTC()
	return &r, nil
}

func (s *SQLite) ListRepos(ctx context.Context) ([]Repo, error) {
	return s.ListReposPage(ctx, "", 0)
}

// ListReposPage returns repos with id > after (keyset pagination), up to
// limit (0 = no limit). Ordered by id so the last row's id is the next
// page's cursor.
func (s *SQLite) ListReposPage(ctx context.Context, after string, limit int) ([]Repo, error) {
	query := `SELECT id, default_branch, created_at, public FROM repos WHERE id > ? ORDER BY id`
	args := []any{after}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Repo
	for rows.Next() {
		var r Repo
		var created int64
		if err := rows.Scan(&r.ID, &r.DefaultBranch, &created, &r.Public); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(created, 0).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *SQLite) DeleteRepo(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM repos WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	// Every repo-scoped table, or a delete leaks rows that outlive the repo
	// (and, for imports, block a same-id repo from ever importing again).
	for _, q := range []string{
		`DELETE FROM refs WHERE repo_id = ?`,
		`DELETE FROM packs WHERE repo_id = ?`,
		`DELETE FROM webhooks WHERE repo_id = ?`,
		`DELETE FROM deliveries WHERE repo_id = ?`,
		`DELETE FROM imports WHERE repo_id = ?`,
		`DELETE FROM wal_state WHERE repo_id = ?`,
		`DELETE FROM lfs_objects WHERE repo_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLite) SetRepoPublic(ctx context.Context, id string, public bool) error {
	res, err := s.db.ExecContext(ctx, `UPDATE repos SET public = ? WHERE id = ?`, boolInt(public), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	s.bumpRev(id)
	return nil
}

func (s *SQLite) ListRefs(ctx context.Context, repoID string) ([]Ref, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, target FROM refs WHERE repo_id = ? ORDER BY name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Ref
	for rows.Next() {
		var r Ref
		if err := rows.Scan(&r.Name, &r.Target); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func listWebhooksTx(ctx context.Context, tx *sql.Tx, repoID string) ([]Webhook, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE repo_id = ?`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *w)
	}
	return out, rows.Err()
}

func (s *SQLite) ReplacePacks(ctx context.Context, repoID string, oldNames []string, newPacks []Pack) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, name := range oldNames {
		res, err := tx.ExecContext(ctx, `DELETE FROM packs WHERE repo_id = ? AND name = ?`, repoID, name)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("pack list changed concurrently: %s missing", name)
		}
	}
	for _, p := range newPacks {
		// UPSERT, not DO NOTHING: a consolidated pack can be byte-identical
		// to an existing pack (an initial full-closure push repacked to the
		// same bytes -> same name); the row must ADOPT the new source/size
		// or the "one gc pack after consolidation" invariant silently breaks.
		// blob_repo resets to '' too: a consolidated pack's blob is written
		// under THIS repo's prefix, so a collision with an inherited fork row
		// (blob_repo=owner) must stop pointing at the owner - else reads
		// resolve to the wrong prefix and the owner can never prune.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO packs (repo_id, name, size_bytes, source, created_at, blob_repo) VALUES (?, ?, ?, ?, ?, '')
			 ON CONFLICT (repo_id, name) DO UPDATE SET size_bytes = excluded.size_bytes, source = excluded.source, blob_repo = ''`,
			repoID, p.Name, p.SizeBytes, p.Source, time.Now().Unix()); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.bumpRev(repoID)
	return nil
}

func (s *SQLite) ReposNeedingCompaction(ctx context.Context, minPacks int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT repo_id FROM packs GROUP BY repo_id HAVING COUNT(*) >= ?`, minPacks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *SQLite) PruneDeliveries(ctx context.Context, before time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM deliveries WHERE status != 'pending' AND created_at < ?`, before.Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLite) ListPacks(ctx context.Context, repoID string) ([]Pack, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, size_bytes, source, created_at, blob_repo FROM packs WHERE repo_id = ? ORDER BY created_at, name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pack
	for rows.Next() {
		var p Pack
		var created int64
		if err := rows.Scan(&p.Name, &p.SizeBytes, &p.Source, &created, &p.BlobRepo); err != nil {
			return nil, err
		}
		p.CreatedAt = time.Unix(created, 0).UTC()
		out = append(out, p)
	}
	return out, rows.Err()
}

// BlobReferenced reports whether a fork points at a blob owned by
// ownerRepo (fork rows carry blob_repo = the owner's prefix). The owner's
// own rows carry blob_repo=” so they never match - this counts dependents
// only.
func (s *SQLite) BlobReferenced(ctx context.Context, ownerRepo, packName string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM packs WHERE blob_repo = ? AND name = ?)`,
		ownerRepo, packName).Scan(&exists)
	return exists, err
}

func (s *SQLite) AddPacks(ctx context.Context, repoID string, packs []Pack) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range packs {
		// Idempotent: re-pushing identical objects can produce the same pack name.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO packs (repo_id, name, size_bytes, source, created_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT DO NOTHING`,
			repoID, p.Name, p.SizeBytes, p.Source, time.Now().Unix()); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.bumpRev(repoID)
	return nil
}

func (s *SQLite) Usage(ctx context.Context) (int, int64, int64, error) {
	var repos int
	var packBytes, lfsBytes int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repos`).Scan(&repos); err != nil {
		return 0, 0, 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM packs`).Scan(&packBytes); err != nil {
		return 0, 0, 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size), 0) FROM lfs_objects`).Scan(&lfsBytes); err != nil {
		return 0, 0, 0, err
	}
	return repos, packBytes, lfsBytes, nil
}

func (s *SQLite) SetImport(ctx context.Context, repoID, status, errMsg string, refs int) error {
	now := time.Now().Unix()
	finished := int64(0)
	if status == "done" || status == "error" {
		finished = now
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO imports (repo_id, status, error, refs, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (repo_id) DO UPDATE SET status = ?, error = ?, refs = ?, finished_at = ?`,
		repoID, status, errMsg, refs, now, finished,
		status, errMsg, refs, finished)
	return err
}

func (s *SQLite) GetImport(ctx context.Context, repoID string) (*Import, error) {
	var im Import
	var started, finished int64
	err := s.db.QueryRowContext(ctx,
		`SELECT repo_id, status, error, refs, started_at, finished_at FROM imports WHERE repo_id = ?`,
		repoID).Scan(&im.RepoID, &im.Status, &im.Error, &im.Refs, &started, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	im.StartedAt = time.Unix(started, 0).UTC()
	if finished > 0 {
		t := time.Unix(finished, 0).UTC()
		im.FinishedAt = &t
	}
	return &im, nil
}

func (s *SQLite) ForkRepo(ctx context.Context, srcID, dstID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var branch string
	if err := tx.QueryRowContext(ctx,
		`SELECT default_branch FROM repos WHERE id = ?`, srcID).Scan(&branch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO repos (id, default_branch, created_at) VALUES (?, ?, ?)`,
		dstID, branch, time.Now().Unix()); err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return ErrExists
		}
		return err
	}
	// Refs: everything except ephemeral namespaces (scratch is not heritage).
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO refs (repo_id, name, target)
		 SELECT ?, name, target FROM refs WHERE repo_id = ? AND name NOT LIKE 'refs/namespaces/%'`,
		dstID, srcID); err != nil {
		return err
	}
	// Packs: shared blobs - BlobRepo resolves to the prefix that really
	// holds them (which may itself already be a grandparent).
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO packs (repo_id, name, size_bytes, source, created_at, blob_repo)
		 SELECT ?, name, size_bytes, 'fork', ?, CASE WHEN blob_repo = '' THEN ? ELSE blob_repo END
		 FROM packs WHERE repo_id = ?`,
		dstID, time.Now().Unix(), srcID, srcID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.bumpRev(dstID)
	return nil
}

func (s *SQLite) AddKey(ctx context.Context, name, publicKeyPEM string, scopes []string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO keys (name, public_key_pem, scopes, created_at) VALUES (?, ?, ?, ?)`,
		name, publicKeyPEM, strings.Join(scopes, " "), time.Now().Unix())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// DeleteKey revokes a registered key by id.
func (s *SQLite) DeleteKey(ctx context.Context, id int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM keys WHERE id = ?`, id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// RestoreKey reinserts a key with its identity intact (bucket recovery);
// idempotent on id.
func (s *SQLite) RestoreKey(ctx context.Context, k Key) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO keys (id, name, public_key_pem, scopes, created_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (id) DO NOTHING`,
		k.ID, k.Name, k.PublicKeyPEM, strings.Join(k.Scopes, " "), time.Now().Unix())
	return err
}

func (s *SQLite) ListKeys(ctx context.Context) ([]Key, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, public_key_pem, scopes FROM keys`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var k Key
		var scopes string
		if err := rows.Scan(&k.ID, &k.Name, &k.PublicKeyPEM, &scopes); err != nil {
			return nil, err
		}
		if scopes != "" {
			k.Scopes = strings.Fields(scopes)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// --- ref-WAL support (repodb.WAL wraps SQLite; see wal.go) ---

// WALSeq returns the last WAL sequence applied to this index for the repo.
func (s *SQLite) WALSeq(ctx context.Context, repoID string) (int64, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx,
		`SELECT seq FROM wal_state WHERE repo_id = ?`, repoID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

// ApplyWAL applies a committed WAL entry to the index: ref moves land
// WITHOUT CAS checks (the bucket entry already won), events enqueue to
// matching webhooks, pack rows (inline-pack entries) are recorded, and the
// repo's applied seq advances - one transaction. Replay after a crash can
// re-enqueue an event (at-least-once delivery; receivers re-fetch by
// contract) and re-insert a pack row (ON CONFLICT DO NOTHING).
func (s *SQLite) ApplyWAL(ctx context.Context, repoID string, updates []RefUpdate, events []Event, packs []Pack, seq int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range packs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO packs (repo_id, name, size_bytes, source, created_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT DO NOTHING`,
			repoID, p.Name, p.SizeBytes, p.Source, time.Now().Unix()); err != nil {
			return err
		}
	}
	for _, u := range updates {
		switch {
		case u.New == ZeroOID:
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM refs WHERE repo_id = ? AND name = ?`, repoID, u.Name); err != nil {
				return err
			}
		default:
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO refs (repo_id, name, target) VALUES (?, ?, ?)
				 ON CONFLICT (repo_id, name) DO UPDATE SET target = ?`,
				repoID, u.Name, u.New, u.New); err != nil {
				return err
			}
		}
	}
	if events != nil {
		hooks, err := listWebhooksTx(ctx, tx, repoID)
		if err != nil {
			return err
		}
		now := time.Now().Unix()
		for _, ev := range events {
			if ev.Name == "" {
				continue
			}
			for _, w := range hooks {
				if !w.Matches(ev.Name) {
					continue
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO deliveries (id, webhook_id, repo_id, event, payload, status, attempts, next_attempt_at, last_error, created_at)
					 VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, '', ?)`,
					NewID(), w.ID, repoID, ev.Name, string(ev.Payload), now, now); err != nil {
					return err
				}
			}
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO wal_state (repo_id, seq) VALUES (?, ?)
		 ON CONFLICT (repo_id) DO UPDATE SET seq = ?`, repoID, seq, seq); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.bumpRev(repoID)
	return nil
}

// ApplySnapshot replaces the repo's entire ref set with the snapshot state
// (used when the WAL tail below the snapshot has been pruned).
func (s *SQLite) ApplySnapshot(ctx context.Context, repoID string, refs []Ref, seq int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM refs WHERE repo_id = ?`, repoID); err != nil {
		return err
	}
	for _, r := range refs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO refs (repo_id, name, target) VALUES (?, ?, ?)`,
			repoID, r.Name, r.Target); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO wal_state (repo_id, seq) VALUES (?, ?)
		 ON CONFLICT (repo_id) DO UPDATE SET seq = ?`, repoID, seq, seq); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.bumpRev(repoID)
	return nil
}

// --- audit log: who did what, when (index state; ops trail, not truth) ---

func (s *SQLite) AddAudit(ctx context.Context, e AuditEntry) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_log (at, actor, action, target, detail) VALUES (?, ?, ?, ?, ?)`,
		time.Now().Unix(), e.Actor, e.Action, e.Target, e.Detail)
	return err
}

// ListAudit returns entries newest-first. action filters exactly; q
// substring-matches actor, target, and detail.
func (s *SQLite) ListAudit(ctx context.Context, action, q string, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT at, actor, action, target, detail FROM audit_log WHERE 1=1`
	args := []any{}
	if action != "" {
		query += ` AND action = ?`
		args = append(args, action)
	}
	if q != "" {
		query += ` AND (actor LIKE ? OR target LIKE ? OR detail LIKE ?)`
		pat := "%" + q + "%"
		args = append(args, pat, pat, pat)
	}
	query += ` ORDER BY at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuditEntry{}
	for rows.Next() {
		var e AuditEntry
		var at int64
		if err := rows.Scan(&at, &e.Actor, &e.Action, &e.Target, &e.Detail); err != nil {
			return nil, err
		}
		e.At = time.Unix(at, 0).UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}
