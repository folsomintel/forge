// Package webhook builds event payloads and delivers them: HMAC-SHA256
// signed POSTs with retries and backoff, driven off the deliveries table so
// enqueueing works from any process (server or pre-receive hook).
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/folsomintel/forge/internal/netguard"
	"github.com/folsomintel/forge/internal/repodb"
)

// PushEvent mirrors the shape agents actually need: the ref that moved and
// the before/after commit range. The payload is a hint - before→after via
// the API is the truth (the GitHub lesson: receivers re-fetch).
type PushEvent struct {
	Repo      string `json:"repo"`
	Ref       string `json:"ref"` // stripped name, e.g. refs/heads/main
	Ephemeral bool   `json:"ephemeral"`
	Before    string `json:"before"`
	After     string `json:"after"`
	Created   bool   `json:"created"`
	Deleted   bool   `json:"deleted"`
	Pusher    string `json:"pusher,omitempty"`
}

// PushEventFor builds the outbox event for a ref update, to be passed to
// repodb.UpdateRefs so it commits in the same transaction as the ref move.
func PushEventFor(repoID string, u repodb.RefUpdate, pusher string) repodb.Event {
	name, eph := repodb.SplitRef(u.Name)
	payload, _ := json.Marshal(PushEvent{
		Repo: repoID, Ref: name, Ephemeral: eph,
		Before: u.Old, After: u.New,
		Created: u.Old == repodb.ZeroOID, Deleted: u.New == repodb.ZeroOID,
		Pusher: pusher,
	})
	return repodb.Event{Name: "push", Payload: payload}
}

// PushEventsFor maps a batch of updates to their outbox events.
func PushEventsFor(repoID string, updates []repodb.RefUpdate, pusher string) []repodb.Event {
	events := make([]repodb.Event, len(updates))
	for i, u := range updates {
		events[i] = PushEventFor(repoID, u, pusher)
	}
	return events
}

// Sign returns the X-Forge-Signature-256 header value for a payload.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

var backoff = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour}

const maxAttempts = 5

type Worker struct {
	DB     repodb.DB
	Client *http.Client
}

// Options configures delivery egress. AllowPrivate disables the SSRF guard
// (dev/tests). ProxyURL routes deliveries through a CONNECT proxy such as
// Stripe Smokescreen for network-layer policy on top of the in-process guard.
type Options struct {
	AllowPrivate bool
	ProxyURL     string
}

func NewWorker(m repodb.DB, opts Options) *Worker {
	return &Worker{DB: m, Client: netguard.Client(10*time.Second, opts.AllowPrivate, opts.ProxyURL)}
}

const deliveryRetention = 30 * 24 * time.Hour

// Run polls for due deliveries until ctx is done, and prunes settled
// deliveries past retention hourly.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	prune := time.NewTicker(time.Hour)
	defer prune.Stop()
	w.Prune(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Tick(ctx)
		case <-prune.C:
			w.Prune(ctx)
		}
	}
}

// Prune removes settled deliveries older than the retention window.
func (w *Worker) Prune(ctx context.Context) {
	n, err := w.DB.PruneDeliveries(ctx, time.Now().Add(-deliveryRetention))
	if err != nil {
		slog.Error("webhook: prune deliveries", "err", err)
		return
	}
	if n > 0 {
		slog.Info("webhook: pruned deliveries", "count", n)
	}
}

// Tick processes one batch of due deliveries (exported for tests).
// Deliveries run concurrently (bounded) so one slow endpoint eating its
// full timeout can't head-of-line block everyone else's events.
func (w *Worker) Tick(ctx context.Context) {
	due, err := w.DB.DueDeliveries(ctx, time.Now(), 20)
	if err != nil {
		slog.Error("webhook: list due deliveries", "err", err)
		return
	}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for _, d := range due {
		wg.Add(1)
		sem <- struct{}{}
		go func(d repodb.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			w.deliver(ctx, d)
		}(d)
	}
	wg.Wait()
}

func (w *Worker) deliver(ctx context.Context, d repodb.Delivery) {
	err := w.post(ctx, d)
	attempts := d.Attempts + 1
	switch {
	case err == nil:
		w.mark(ctx, d.ID, "ok", attempts, time.Time{}, "")
	case attempts >= maxAttempts:
		w.mark(ctx, d.ID, "failed", attempts, time.Time{}, err.Error())
	default:
		w.mark(ctx, d.ID, "pending", attempts, time.Now().Add(backoff[min(attempts-1, len(backoff)-1)]), err.Error())
	}
}

func (w *Worker) mark(ctx context.Context, id, status string, attempts int, next time.Time, lastErr string) {
	if err := w.DB.MarkDelivery(ctx, id, status, attempts, next, lastErr); err != nil {
		slog.Error("webhook: mark delivery", "id", id, "err", err)
	}
}

func (w *Worker) post(ctx context.Context, d repodb.Delivery) error {
	body := []byte(d.Payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "forge-webhook")
	req.Header.Set("X-Forge-Event", d.Event)
	req.Header.Set("X-Forge-Delivery", d.ID)
	req.Header.Set("X-Forge-Signature-256", Sign(d.Secret, body))
	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("endpoint returned %s", resp.Status)
	}
	return nil
}
