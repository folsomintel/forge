// Package githttp serves the git smart HTTP protocol by shelling out to
// real git against materialized cache repos. The wire protocol is the one
// part of git we never reimplement.
package githttp

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/maintain"
	"github.com/folsomintel/forge/internal/ratelimit"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
)

type Handler struct {
	Cache    *repocache.Cache
	DB       repodb.DB
	Blobs    blobstore.Store
	Bundles  *maintain.Pipeline // capability-URL verification for bundle downloads
	Stager   *ingest.Stager     // staged tee push (nil disables)
	Auth     *auth.Verifier
	HooksDir string
	SelfPath string   // absolute path to the forged binary, re-invoked as the pre-receive hook
	HookEnv  []string // explicit config env for hook subprocesses
	Limit    *ratelimit.Limiter

	// MaxPushBytes rejects packs larger than this via receive.maxInputSize:
	// a LOUD client-visible error instead of an OOM-killed machine when a
	// push exceeds what the memory tier can index. 0 = unlimited. Monster
	// migrations go through offline import, which is unaffected.
	MaxPushBytes int64

	// active counts in-flight upload-pack/receive-pack bodies; the control
	// plane reads it (via /api/usage) to drain before restarts.
	active atomic.Int64

	// GoReceive enables the fork-free receive fast path (env
	// FORGE_GORECEIVE). Off by default: it falls back to git on anything it
	// cannot prove, but it writes repo truth, so it earns its way on.
	GoReceive bool
	goRecv    goReceiveCounters

	// MaxGitForks bounds concurrent forked git processes (advertisement +
	// service fallback). A synchronized 64-writer stampede once OOM-killed
	// a 256MB machine purely on advertisement forks; with the gate, excess
	// requests queue briefly and then get 503 + Retry-After instead of
	// taking the box down. 0 = max(4, 2*NumCPU). The in-Go fast path is
	// not gated - it forks nothing.
	MaxGitForks int
	gateOnce    sync.Once
	forkGate    chan struct{}

	// ForkAdvertisement restores the fork-git info/refs path (escape hatch
	// for the Go-native advertisement; FORGE_FORK_ADVERTISEMENT=true).
	ForkAdvertisement bool
	peeled            sync.Map     // tag oid -> peeled oid ("" = not a tag); tags are immutable
	peeledN           atomic.Int64 // approximate live entries in peeled, for the cap

	// GoFetch enables the fork-free fetch fast path (incremental chains
	// from stored receive packs + clone passthrough of consolidated
	// packs; env FORGE_GOFETCH). Everything unprovable falls back to git.
	GoFetch     bool
	goFetchCtr  goFetchCounters
	transitions transitionMap

	// NudgeMaintain, when set, kicks a threshold-gated maintenance run after
	// a fast-path push (the fork/hook path calls NudgeIfNeeded; the fast
	// path has no hook, so it must nudge itself or never consolidate).
	NudgeMaintain func(repo string)
}

// ActiveTransfers reports in-flight git transfer requests.
func (h *Handler) ActiveTransfers() int64 { return h.active.Load() }

// GoReceiveStats reports fast-path decision counts (eligible/fell-back/rejected).
func (h *Handler) GoReceiveStats() GoReceiveStats { return h.goRecv.snapshot() }

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /{repo}/info/refs", h.infoRefs)
	mux.HandleFunc("POST /{repo}/git-upload-pack", func(w http.ResponseWriter, r *http.Request) {
		h.service(w, r, "git-upload-pack")
	})
	mux.HandleFunc("POST /{repo}/git-receive-pack", func(w http.ResponseWriter, r *http.Request) {
		h.service(w, r, "git-receive-pack")
	})
	// Signed capability URL, no bearer auth: it is only ever disclosed
	// inside an authenticated ref advertisement. The chain member is named by
	// ?name=<token>.bundle (a query param, not a path segment, so the route
	// stays 2-segment and can't collide with /{repo}/info/refs).
	mux.HandleFunc("GET /bundles/{repo}", h.bundle)
}

// validBundleName accepts only "<digits>.bundle" - the chain member naming.
// Keeps the {name} path segment from being anything but a bundle blob (no
// traversal into other meta/ objects; the store rejects "/" and ".." too).
func validBundleName(name string) bool {
	base, ok := strings.CutSuffix(name, ".bundle")
	if !ok || base == "" {
		return false
	}
	for i := 0; i < len(base); i++ {
		if base[i] < '0' || base[i] > '9' {
			return false
		}
	}
	return true
}

func (h *Handler) bundle(w http.ResponseWriter, r *http.Request) {
	repo := r.PathValue("repo")
	name := r.URL.Query().Get("name")
	// Defense in depth alongside the signature check: reject a malformed repo
	// id / bundle name before VerifyBundleSig / Blobs.Get. The HMAC already
	// binds the URL to this repo+name; this keeps the store ingress consistent
	// with the other wire paths.
	if !repodb.ValidRepoID(repo) || !validBundleName(name) {
		http.Error(w, "invalid or expired bundle link", http.StatusForbidden)
		return
	}
	exp, _ := strconv.ParseInt(r.URL.Query().Get("exp"), 10, 64)
	if !h.Bundles.VerifyBundleSig(repo, name, exp, r.URL.Query().Get("sig")) {
		http.Error(w, "invalid or expired bundle link", http.StatusForbidden)
		return
	}
	rc, err := h.Blobs.Get(r.Context(), repo, maintain.BundlePrefix+name)
	if err != nil {
		http.Error(w, "bundle not found", http.StatusNotFound)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	io.Copy(w, rc)
}

// parseTarget splits "{repo}[+ephemeral][.git]" into repo id and the git
// ref namespace to serve under ("" = the normal view).
func parseTarget(r *http.Request) (repo, namespace string) {
	repo = strings.TrimSuffix(r.PathValue("repo"), ".git")
	if base, ok := strings.CutSuffix(repo, "+ephemeral"); ok {
		return base, repodb.Namespace
	}
	return repo, ""
}

func scopeFor(service string) string {
	if service == "git-receive-pack" {
		return auth.ScopeGitWrite
	}
	return auth.ScopeGitRead
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, service, repo string) (*auth.Claims, bool) {
	// Defense in depth: reject a malformed repo id before it reaches
	// materialize/the store. The DB-existence gate already stops traversal
	// today, but this keeps the wire ingress from silently relying on that
	// invariant if a downstream path ever touches the FS before GetRepo.
	if !repodb.ValidRepoID(repo) {
		http.Error(w, "repository not found", http.StatusNotFound)
		return nil, false
	}
	claims, err := h.Auth.FromRequest(r)
	if err != nil || !claims.Allow(scopeFor(service), repo) {
		// Public repos serve reads (clone/fetch) anonymously; writes and
		// private repos still demand credentials.
		if service == "git-upload-pack" {
			if rp, rerr := h.DB.GetRepo(r.Context(), repo); rerr == nil && rp.Public {
				anon := &auth.Claims{Subject: anonSubject(r)}
				if !h.Limit.Allow(anon.Subject) {
					w.Header().Set("Retry-After", "1")
					http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
					return nil, false
				}
				return anon, true
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="forge"`)
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return nil, false
	}
	if !h.Limit.Allow(claims.Subject) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return nil, false
	}
	return claims, true
}

// infoRefs handles the ref advertisement for both services.
func (h *Handler) infoRefs(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	if service != "git-upload-pack" && service != "git-receive-pack" {
		http.Error(w, "smart HTTP only", http.StatusForbidden)
		return
	}
	repo, ns := parseTarget(r)
	claims, ok := h.authorize(w, r, service, repo)
	if !ok {
		return
	}

	// Fork-free advertisement: served straight from the ref index - no
	// materialization, no git process. This is the hottest request in an
	// agent workload (every fetch/poll/push starts here).
	if h.goInfoRefs(w, r, service, repo, ns) {
		return
	}

	// Escape hatch: the forked path (FORGE_FORK_ADVERTISEMENT).
	dir, ok := h.materialize(w, r, repo)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache")
	// Smart-HTTP framing: "# service=..." pkt-line + flush, then the raw
	// advertisement from git (valid for protocol v0 and v2 clients alike).
	prefix := "# service=" + service + "\n"
	fmt.Fprintf(w, "%04x%s0000", len(prefix)+4, prefix)

	h.runGit(w, r, dir, repo, ns, service, claims.Subject, nil, nil, "--advertise-refs")
}

// service handles the POST body for upload-pack / receive-pack.
func (h *Handler) service(w http.ResponseWriter, r *http.Request, service string) {
	h.active.Add(1)
	defer h.active.Add(-1)
	repo, ns := parseTarget(r)
	claims, ok := h.authorize(w, r, service, repo)
	if !ok {
		return
	}

	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "bad gzip body", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	}

	// Fork-free fast paths first: they touch only the store and the DB, so
	// they need NO repo lock and NO materialization - a self-contained push
	// or a v2 metadata read never waits behind a repack or hydration.
	if service == "git-receive-pack" && h.GoReceive {
		buf, full, err := readUpTo(body, goReceiveMaxBytes)
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		if full {
			w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))
			w.Header().Set("Cache-Control", "no-cache")
			if h.tryGoReceive(w, r, repo, ns, claims.Subject, buf) {
				return
			}
			// Fell back: hand the buffered body to git unchanged.
			h.goRecv.fellBack.Add(1)
			body = bytes.NewReader(buf)
		} else {
			// Too big for the fast path: replay the prefix, keep streaming.
			h.goRecv.fellBack.Add(1)
			h.goRecv.fbOversize.Add(1)
			body = io.MultiReader(bytes.NewReader(buf), body)
		}
	}
	if service == "git-upload-pack" {
		// Protocol-v2 metadata commands (ls-refs, bundle-uri) are pure ref
		// reads - answer them from the index without forking git. This is
		// what every `git fetch` (v2) sends first, and what agents poll.
		buf, full, err := readUpTo(body, v2CommandMaxBytes)
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		if full && h.goUploadPackCommand(w, r, repo, ns, buf) {
			return
		}
		body = io.MultiReader(bytes.NewReader(buf), body)
	}

	// Fallback: fork git against the materialized cache repo. Pushes run
	// CONCURRENTLY per repo (quarantines are isolated, the DB CAS is the
	// arbiter); only materialization takes the write lock. This is the
	// Cursor lesson: lock the ref transaction, never the transfer.
	lock := h.Cache.Lock(repo)
	dir, ok := h.materialize(w, r, repo)
	if !ok {
		return
	}
	lock.RLock()
	defer lock.RUnlock()

	// Staged tee: upload the wire pack to the store WHILE the client is
	// still sending it; the hook turns a trailer match into a server-side
	// copy instead of a second upload (ingest.Stager).
	var stagedEnv []string
	if service == "git-receive-pack" && h.Stager != nil {
		id, sw := h.Stager.Begin(repo)
		teed, cancel := teeReceivePack(body, sw)
		defer cancel()
		body = teed
		stagedEnv = []string{"FORGE_STAGED_ID=" + id}
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-result", service))
	w.Header().Set("Cache-Control", "no-cache")
	h.runGit(w, r, dir, repo, ns, service, claims.Subject, body, stagedEnv)
}

// materialize uses the serve-path variant: never convoys behind in-flight
// transfers; may serve a slightly stale advertisement (CAS arbitrates).
func (h *Handler) materialize(w http.ResponseWriter, r *http.Request, repo string) (string, bool) {
	dir, err := h.Cache.MaterializeServe(r.Context(), repo)
	if errors.Is(err, repodb.ErrNotFound) {
		http.Error(w, "repository not found", http.StatusNotFound)
		return "", false
	}
	if err != nil {
		slog.Error("materialize failed", "repo", repo, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return "", false
	}
	return dir, true
}

// acquireFork blocks until a fork slot is free (bounding concurrent git
// processes across BOTH the HTTP and SSH transports - a stampede on either
// can OOM a small box). It returns a release func and true on success, or
// false if the context is cancelled or the 15s queue budget elapses. The
// caller renders load-shed however its transport requires.
func (h *Handler) acquireFork(ctx context.Context) (release func(), ok bool) {
	h.gateOnce.Do(func() {
		n := h.MaxGitForks
		if n <= 0 {
			n = max(4, 2*runtime.NumCPU())
		}
		h.forkGate = make(chan struct{}, n)
	})
	select {
	case h.forkGate <- struct{}{}:
		return func() { <-h.forkGate }, true
	case <-ctx.Done():
		return func() {}, false
	case <-time.After(15 * time.Second):
		return func() {}, false
	}
}

// runGit executes the git service against the cache repo, wiring the request
// body to stdin and the response to stdout.
func (h *Handler) runGit(w http.ResponseWriter, r *http.Request, dir, repo, namespace, service, pusher string, stdin io.Reader, extraEnv []string, extraArgs ...string) {
	// Bound concurrent forks: each git process (plus its forged hook) costs
	// real memory, and a synchronized stampede of advertisements once took
	// a 256MB machine down. Queue briefly, then shed load LOUDLY.
	release, ok := h.acquireFork(r.Context())
	if !ok {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "server busy - retry", http.StatusServiceUnavailable)
		return
	}
	defer release()

	bin := strings.TrimPrefix(service, "git-") // upload-pack | receive-pack
	args := []string{}
	if namespace == "" {
		// Normal view: namespaced (ephemeral) refs are invisible.
		args = append(args, "-c", "transfer.hideRefs=refs/namespaces")
	}
	if service == "git-receive-pack" {
		args = append(args, "-c", "core.hooksPath="+h.HooksDir)
		if h.MaxPushBytes > 0 {
			args = append(args, "-c", fmt.Sprintf("receive.maxInputSize=%d", h.MaxPushBytes))
		}
	}
	args = append(args, bin, "--stateless-rpc")
	args = append(args, extraArgs...)
	args = append(args, dir)

	cmd := exec.CommandContext(r.Context(), "git", args...)
	// BaseEnv strips credential-bearing vars (S3 keys, admin token) from the
	// child. Only receive-pack's hook needs S3 creds for its direct-apply
	// fallback, so HookEnv (which re-adds them) is appended for that service
	// alone - upload-pack and advertisement inherit no secrets.
	cmd.Env = append(gitcmd.BaseEnv(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_PROTOCOL="+r.Header.Get("Git-Protocol"),
		"FORGE_REPO_ID="+repo,
		"FORGE_SELF="+h.SelfPath,
		"FORGE_PUSHER="+pusher,
	)
	if service == "git-receive-pack" {
		cmd.Env = append(cmd.Env, h.HookEnv...)
	}
	cmd.Env = append(cmd.Env, extraEnv...)
	if namespace != "" {
		cmd.Env = append(cmd.Env, "GIT_NAMESPACE="+namespace)
	}
	cmd.Stdin = stdin
	cmd.Stdout = w
	var stderr strings.Builder
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// Headers are already sent mid-stream; all we can do is log. The git
		// client sees the truncated pkt stream and reports the failure.
		slog.Error("git exec failed", "service", service, "repo", repo, "err", err, "stderr", stderr.String())
	}
}

// readUpTo reads at most max+1 bytes: full=true means the whole body fit
// in max (EOF reached), so the fast path can consider it; full=false means
// the body is larger and the returned prefix must be replayed to git.
func readUpTo(r io.Reader, max int) (buf []byte, full bool, err error) {
	buf, err = io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, false, err
	}
	if len(buf) > max {
		return buf, false, nil
	}
	return buf, true, nil
}

// anonSubject buckets anonymous traffic per client IP for rate limiting.
func anonSubject(r *http.Request) string {
	ip := r.Header.Get("Fly-Client-IP")
	if ip == "" {
		ip = r.RemoteAddr
	}
	return "anon:" + ip
}
