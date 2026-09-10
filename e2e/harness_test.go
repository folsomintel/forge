// Package e2e boots the full server (real TCP, real git clients, real hook
// subprocesses) against temp storage. Every test gets an isolated instance.
package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/server"
)

var forgedBin string

func TestMain(m *testing.M) {
	// The pre-receive hook re-invokes the forged binary; build it once.
	dir, err := os.MkdirTemp("", "forge-e2e-bin-*")
	if err != nil {
		panic(err)
	}
	forgedBin = filepath.Join(dir, "forged")
	out, err := exec.Command("go", "build", "-o", forgedBin, "github.com/folsomintel/forge/cmd/forged").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("build forged: %v\n%s", err, out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type env struct {
	t     *testing.T
	base  string // http://127.0.0.1:port
	token string
	priv  *ecdsa.PrivateKey
	srv   *server.Server
	dir   string
}

func startServer(t *testing.T) *env {
	return startServerWith(t, nil)
}

func startServerWith(t *testing.T, mutate func(*config.Config)) *env {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Config{
		DataDir:   filepath.Join(dir, "server"),
		DBPath:    filepath.Join(dir, "server", "forge.db"),
		StoreKind: "local",
		StorePath: filepath.Join(dir, "server", "packs"),
		SelfPath:  forgedBin,
		// Test webhook receivers live on loopback.
		WebhookAllowPrivate: true,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := server.Build(cfg)
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	t.Cleanup(func() { srv.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv.Start(ctx)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpSrv := &http.Server{Handler: srv.Mux}
	go httpSrv.Serve(ln)
	t.Cleanup(func() { httpSrv.Close() })

	e := &env{t: t, base: "http://" + ln.Addr().String(), srv: srv, dir: dir}
	e.priv, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pubDER, _ := x509.MarshalPKIXPublicKey(&e.priv.PublicKey)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	if _, err := srv.DB.AddKey(context.Background(), "e2e", string(pubPEM), nil); err != nil {
		t.Fatalf("add key: %v", err)
	}
	e.token = e.mintToken("git:read git:write repo:write org:read", "")
	return e
}

func (e *env) mintToken(scopes, repo string) string {
	claims := jwt.MapClaims{
		"sub": "e2e", "scopes": scopes,
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
	}
	if repo != "" {
		claims["repo"] = repo
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodES256, claims).SignedString(e.priv)
	if err != nil {
		e.t.Fatalf("mint token: %v", err)
	}
	return tok
}

// api performs a JSON request; body may be a map/struct or nil.
func (e *env) api(method, path string, body any) (int, map[string]any) {
	return e.apiToken(e.token, method, path, body)
}

func (e *env) apiToken(token, method, path string, body any) (int, map[string]any) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.base+path, rd)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	if out == nil {
		out = map[string]any{"_raw": string(raw)}
	}
	return resp.StatusCode, out
}

// apiList is api() for endpoints returning JSON arrays.
func (e *env) apiList(method, path string) (int, []map[string]any) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.base+path, nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out []map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) mustAPI(method, path string, body any, wantStatus int) map[string]any {
	e.t.Helper()
	status, out := e.api(method, path, body)
	if status != wantStatus {
		e.t.Fatalf("%s %s: status %d (want %d): %v", method, path, status, wantStatus, out)
	}
	return out
}

func (e *env) createRepo(id string) {
	e.t.Helper()
	e.mustAPI("POST", "/api/repos", map[string]string{"id": id}, http.StatusCreated)
}

// remote returns an authenticated git remote URL; suffix "+ephemeral" gives
// the ephemeral view.
func (e *env) remote(repo, suffix string) string {
	host := strings.TrimPrefix(e.base, "http://")
	return fmt.Sprintf("http://t:%s@%s/%s%s.git", e.token, host, repo, suffix)
}

// git runs git in dir with test-safe identity config.
func (e *env) git(dir string, args ...string) string {
	e.t.Helper()
	out, err := e.gitErr(dir, args...)
	if err != nil {
		e.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return out
}

func (e *env) gitErr(dir string, args ...string) (string, error) {
	base := []string{
		// credential.helper= (empty) clears the helper list at every config
		// scope - including Apple git's baked-in CLT config, which
		// GIT_CONFIG_SYSTEM=/dev/null does NOT suppress. Without this,
		// osxkeychain auto-stores a seed push's token-embedded URL and then
		// silently authenticates the "anonymous" private-repo clone.
		"-c", "credential.helper=",
		"-c", "user.email=e2e@test", "-c", "user.name=e2e",
		"-c", "init.defaultBranch=main", "-c", "protocol.version=2",
	}
	cmd := exec.Command("git", append(base, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// seedRepo creates repo, pushes one commit ("hello\n" in README.md) on main,
// and returns the local clone path.
func (e *env) seedRepo(repo string) string {
	e.t.Helper()
	e.createRepo(repo)
	work := filepath.Join(e.dir, "seed-"+repo)
	e.git(e.dir, "clone", e.remote(repo, ""), work)
	writeFile(e.t, work, "README.md", "hello\n")
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "initial")
	e.git(work, "push", "-q", "origin", "HEAD:refs/heads/main")
	return work
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// assertRepoIntegrity checks the storage-layer invariants for a repo:
// every pack blob in the store passes `git verify-pack`, and the cache
// repo materialized from store+DB passes `git fsck --strict`.
func (e *env) assertRepoIntegrity(repo string) {
	e.t.Helper()
	packDir := filepath.Join(e.dir, "server", "packs", repo)
	entries, err := os.ReadDir(packDir)
	if err != nil {
		e.t.Fatalf("read store packs: %v", err)
	}
	packs := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".idx") {
			// Async maintenance deletes superseded pack blobs .pack-first;
			// an idx observed inside that window is not corruption.
			if _, err := os.Stat(filepath.Join(packDir, strings.TrimSuffix(entry.Name(), ".idx")+".pack")); err != nil {
				continue
			}
			packs++
			e.git(packDir, "verify-pack", "--", filepath.Join(packDir, entry.Name()))
		}
	}
	if packs == 0 {
		e.t.Fatal("no packs in store - nothing was persisted")
	}
	cache := filepath.Join(e.dir, "server", "cache", repo+".git")
	if _, err := os.Stat(cache); err == nil {
		e.git(cache, "fsck", "--strict", "--no-dangling")
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
