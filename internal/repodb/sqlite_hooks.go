package repodb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// NewID returns a 16-byte random hex id (webhooks, deliveries).
func NewID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *SQLite) CreateWebhook(ctx context.Context, w *Webhook) error {
	if w.ID == "" {
		w.ID = NewID()
	}
	w.CreatedAt = time.Now().UTC().Truncate(time.Second)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO webhooks (id, repo_id, url, secret, events, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.RepoID, w.URL, w.Secret, strings.Join(w.Events, " "), boolInt(w.Active), w.CreatedAt.Unix())
	return err
}

// RestoreWebhook reinserts a webhook with its identity intact (bucket
// recovery path); idempotent on id.
func (s *SQLite) RestoreWebhook(ctx context.Context, w *Webhook) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO webhooks (id, repo_id, url, secret, events, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (id) DO NOTHING`,
		w.ID, w.RepoID, w.URL, w.Secret, strings.Join(w.Events, " "), boolInt(w.Active), w.CreatedAt.Unix())
	return err
}

func scanWebhook(row interface{ Scan(...any) error }) (*Webhook, error) {
	var w Webhook
	var events string
	var active int
	var created int64
	if err := row.Scan(&w.ID, &w.RepoID, &w.URL, &w.Secret, &events, &active, &created); err != nil {
		return nil, err
	}
	w.Events = strings.Fields(events)
	w.Active = active != 0
	w.CreatedAt = time.Unix(created, 0).UTC()
	return &w, nil
}

const webhookCols = `id, repo_id, url, secret, events, active, created_at`

func (s *SQLite) ListWebhooks(ctx context.Context, repoID string) ([]Webhook, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE repo_id = ? ORDER BY created_at, id`, repoID)
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

func (s *SQLite) GetWebhook(ctx context.Context, repoID, id string) (*Webhook, error) {
	w, err := scanWebhook(s.db.QueryRowContext(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE repo_id = ? AND id = ?`, repoID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return w, err
}

func (s *SQLite) UpdateWebhook(ctx context.Context, w *Webhook) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE webhooks SET url = ?, secret = ?, events = ?, active = ? WHERE repo_id = ? AND id = ?`,
		w.URL, w.Secret, strings.Join(w.Events, " "), boolInt(w.Active), w.RepoID, w.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) DeleteWebhook(ctx context.Context, repoID, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM webhooks WHERE repo_id = ? AND id = ?`, repoID, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) EnqueueEvent(ctx context.Context, repoID, event string, payload []byte) error {
	hooks, err := s.ListWebhooks(ctx, repoID)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, w := range hooks {
		if !w.Matches(event) {
			continue
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO deliveries (id, webhook_id, repo_id, event, payload, status, attempts, next_attempt_at, last_error, created_at)
			 VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, '', ?)`,
			NewID(), w.ID, repoID, event, string(payload), now, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) DueDeliveries(ctx context.Context, now time.Time, limit int) ([]Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT d.id, d.webhook_id, d.event, d.payload, d.status, d.attempts, d.next_attempt_at, d.last_error, d.created_at, w.url, w.secret
		 FROM deliveries d JOIN webhooks w ON w.id = d.webhook_id
		 WHERE d.status = 'pending' AND d.next_attempt_at <= ?
		 ORDER BY d.next_attempt_at LIMIT ?`, now.Unix(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveries(rows, true)
}

func (s *SQLite) MarkDelivery(ctx context.Context, id, status string, attempts int, nextAttempt time.Time, lastErr string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = ?, attempts = ?, next_attempt_at = ?, last_error = ? WHERE id = ?`,
		status, attempts, nextAttempt.Unix(), lastErr, id)
	return err
}

func (s *SQLite) ListDeliveries(ctx context.Context, repoID, webhookID string, limit int) ([]Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, webhook_id, event, payload, status, attempts, next_attempt_at, last_error, created_at
		 FROM deliveries WHERE repo_id = ? AND webhook_id = ? ORDER BY created_at DESC, id LIMIT ?`,
		repoID, webhookID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDeliveries(rows, false)
}

func scanDeliveries(rows *sql.Rows, withHook bool) ([]Delivery, error) {
	var out []Delivery
	for rows.Next() {
		var d Delivery
		var next, created int64
		dest := []any{&d.ID, &d.WebhookID, &d.Event, &d.Payload, &d.Status, &d.Attempts, &next, &d.LastError, &created}
		if withHook {
			dest = append(dest, &d.URL, &d.Secret)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		d.NextAttemptAt = time.Unix(next, 0).UTC()
		d.CreatedAt = time.Unix(created, 0).UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *SQLite) AddLFSObject(ctx context.Context, repoID string, obj LFSObject) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO lfs_objects (repo_id, oid, size, created_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		repoID, obj.OID, obj.Size, time.Now().Unix())
	return err
}

func (s *SQLite) GetLFSObject(ctx context.Context, repoID, oid string) (*LFSObject, error) {
	var obj LFSObject
	err := s.db.QueryRowContext(ctx,
		`SELECT oid, size FROM lfs_objects WHERE repo_id = ? AND oid = ?`, repoID, oid).
		Scan(&obj.OID, &obj.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
