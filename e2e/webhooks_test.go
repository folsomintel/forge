package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/config"

	"github.com/folsomintel/forge/internal/webhook"
)

type hookRecorder struct {
	mu       sync.Mutex
	requests []recordedHook
	fail     bool
}

type recordedHook struct {
	event, delivery, signature string
	body                       []byte
}

func (h *hookRecorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.requests = append(h.requests, recordedHook{
			event:     r.Header.Get("X-Forge-Event"),
			delivery:  r.Header.Get("X-Forge-Delivery"),
			signature: r.Header.Get("X-Forge-Signature-256"),
			body:      body,
		})
		fail := h.fail
		h.mu.Unlock()
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *hookRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

func (h *hookRecorder) last() recordedHook {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests[len(h.requests)-1]
}

func TestWebhookDeliveryOnGitPush(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	rec := &hookRecorder{}
	receiver := httptest.NewServer(rec.handler())
	defer receiver.Close()

	e.createRepo("demo")
	created := e.mustAPI("POST", "/api/repos/demo/webhooks", map[string]any{
		"url": receiver.URL, "events": []string{"push"},
	}, http.StatusCreated)
	secret := created["secret"].(string)

	e.seedRepoPush(t, "demo") // git push over smart HTTP → hook enqueues

	waitFor(t, "webhook delivery", 10*time.Second, func() bool { return rec.count() >= 1 })
	got := rec.last()
	if got.event != "push" || got.delivery == "" {
		t.Fatalf("headers: %+v", got)
	}
	if got.signature != webhook.Sign(secret, got.body) {
		t.Fatalf("bad HMAC signature: %s", got.signature)
	}
	var payload webhook.PushEvent
	if err := json.Unmarshal(got.body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Repo != "demo" || payload.Ref != "refs/heads/main" || !payload.Created || payload.Pusher != "e2e" {
		t.Fatalf("payload: %+v", payload)
	}

	// API-driven commits fire too.
	e.mustAPI("PUT", "/api/repos/demo/contents/x.txt", map[string]any{
		"message": "via api", "content": b64("x\n"),
	}, http.StatusCreated)
	waitFor(t, "api webhook delivery", 10*time.Second, func() bool { return rec.count() >= 2 })
}

// seedRepoPush pushes a first commit to an existing repo.
func (e *env) seedRepoPush(t *testing.T, repo string) {
	work := t.TempDir()
	e.git(e.dir, "clone", e.remote(repo, ""), work)
	writeFile(t, work, "README.md", "hello\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "initial")
	e.git(work, "push", "-q", "origin", "HEAD:refs/heads/main")
}

func TestWebhookRetryAndDeliveryLog(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	rec := &hookRecorder{fail: true}
	receiver := httptest.NewServer(rec.handler())
	defer receiver.Close()

	e.createRepo("demo")
	created := e.mustAPI("POST", "/api/repos/demo/webhooks", map[string]any{"url": receiver.URL}, http.StatusCreated)
	hookID := created["id"].(string)
	e.seedRepoPush(t, "demo")

	// First attempt fails; the delivery log shows a pending retry.
	waitFor(t, "failed attempt recorded", 10*time.Second, func() bool {
		_, deliveries := e.apiList("GET", "/api/repos/demo/webhooks/"+hookID+"/deliveries")
		return len(deliveries) >= 1 && deliveries[0]["attempts"].(float64) >= 1 &&
			deliveries[0]["status"] == "pending" && deliveries[0]["last_error"] != ""
	})

	// Recover the receiver and force the retry due now (test-only nudge),
	// then the worker delivers on its next tick.
	rec.mu.Lock()
	rec.fail = false
	rec.mu.Unlock()
	_, deliveries := e.apiList("GET", "/api/repos/demo/webhooks/"+hookID+"/deliveries")
	id := deliveries[0]["id"].(string)
	attempts := int(deliveries[0]["attempts"].(float64))
	if err := e.srv.DB.MarkDelivery(t.Context(), id, "pending", attempts, time.Now(), "nudged"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "delivery after recovery", 10*time.Second, func() bool {
		_, ds := e.apiList("GET", "/api/repos/demo/webhooks/"+hookID+"/deliveries")
		return len(ds) >= 1 && ds[0]["status"] == "ok"
	})
}

// With the SSRF guard on (the production default), deliveries to private
// or reserved addresses are blocked at dial time and the reason lands in
// the delivery log.
func TestWebhookSSRFGuardBlocksPrivateTargets(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.WebhookAllowPrivate = false })
	rec := &hookRecorder{}
	receiver := httptest.NewServer(rec.handler()) // loopback → must be blocked
	defer receiver.Close()

	e.createRepo("demo")
	created := e.mustAPI("POST", "/api/repos/demo/webhooks", map[string]any{"url": receiver.URL}, http.StatusCreated)
	hookID := created["id"].(string)
	e.seedRepoPush(t, "demo")

	waitFor(t, "blocked delivery recorded", 10*time.Second, func() bool {
		_, ds := e.apiList("GET", "/api/repos/demo/webhooks/"+hookID+"/deliveries")
		return len(ds) >= 1 && ds[0]["attempts"].(float64) >= 1 &&
			strings.Contains(ds[0]["last_error"].(string), "blocked")
	})
	if rec.count() != 0 {
		t.Fatalf("SSRF guard let %d requests through to a loopback target", rec.count())
	}
}

func TestWebhookCRUD(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo")

	created := e.mustAPI("POST", "/api/repos/demo/webhooks", map[string]any{
		"url": "https://example.com/hook",
	}, http.StatusCreated)
	id := created["id"].(string)
	if created["secret"] == "" {
		t.Fatal("secret not generated")
	}

	got := e.mustAPI("GET", "/api/repos/demo/webhooks/"+id, nil, http.StatusOK)
	if _, hasSecret := got["secret"]; hasSecret {
		t.Fatalf("secret leaked on GET: %v", got)
	}

	updated := e.mustAPI("PATCH", "/api/repos/demo/webhooks/"+id, map[string]any{
		"active": false, "events": []string{"*"},
	}, http.StatusOK)
	if updated["active"] != false {
		t.Fatalf("patch: %v", updated)
	}

	e.mustAPI("POST", "/api/repos/demo/webhooks", map[string]any{"url": "ftp://nope"}, http.StatusBadRequest)
	e.mustAPI("DELETE", "/api/repos/demo/webhooks/"+id, nil, http.StatusNoContent)
	e.mustAPI("GET", "/api/repos/demo/webhooks/"+id, nil, http.StatusNotFound)
}
