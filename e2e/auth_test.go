package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestAuthScopeEnforcement(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.seedRepo("demo")

	// No token → 401 everywhere.
	if status, _ := e.apiToken("", "GET", "/api/repos", nil); status != http.StatusUnauthorized {
		t.Fatalf("no-token list: %d", status)
	}

	// git:read alone cannot create repos or write contents.
	readOnly := e.mintToken("git:read", "")
	if status, _ := e.apiToken(readOnly, "POST", "/api/repos", map[string]string{"id": "nope"}); status != http.StatusUnauthorized {
		t.Fatalf("read-only created repo: %d", status)
	}
	if status, _ := e.apiToken(readOnly, "PUT", "/api/repos/demo/contents/x.txt", map[string]any{
		"message": "x", "content": b64("x"),
	}); status != http.StatusUnauthorized {
		t.Fatalf("read-only wrote contents: %d", status)
	}
	// But it can read contents.
	if status, _ := e.apiToken(readOnly, "GET", "/api/repos/demo/contents/README.md", nil); status != http.StatusOK {
		t.Fatalf("read-only blocked from reading: %d", status)
	}

	// git:write implies git:read: a push negotiates against the remote's
	// refs, so a writer can always read.
	writeOnly := e.mintToken("git:write", "")
	if status, _ := e.apiToken(writeOnly, "GET", "/api/repos/demo/contents/README.md", nil); status != http.StatusOK {
		t.Fatalf("write token blocked from reading: %d", status)
	}

	// Expired tokens rejected.
	expired := func() string {
		claims := jwt.MapClaims{
			"sub": "e2e", "scopes": "git:read git:write repo:write org:read",
			"iat": time.Now().Add(-2 * time.Hour).Unix(), "exp": time.Now().Add(-time.Hour).Unix(),
		}
		tok, _ := jwt.NewWithClaims(jwt.SigningMethodES256, claims).SignedString(e.priv)
		return tok
	}()
	if status, _ := e.apiToken(expired, "GET", "/api/repos", nil); status != http.StatusUnauthorized {
		t.Fatalf("expired token accepted: %d", status)
	}

	// Tokens signed by an unregistered key rejected.
	rogue := startServer(t) // different server, different registered key
	stranger := rogue.mintToken("org:read", "")
	if status, _ := e.apiToken(stranger, "GET", "/api/repos", nil); status != http.StatusUnauthorized {
		t.Fatalf("foreign-key token accepted: %d", status)
	}

	// Repo-scoped token can't touch other repos via API either.
	scoped := e.mintToken("git:read git:write", "demo")
	if status, _ := e.apiToken(scoped, "GET", "/api/repos/demo/contents/README.md", nil); status != http.StatusOK {
		t.Fatalf("scoped token blocked from its repo: %d", status)
	}
	e.createRepo("other")
	if status, _ := e.apiToken(scoped, "GET", "/api/repos/other/contents/README.md", nil); status != http.StatusUnauthorized {
		t.Fatalf("scoped token crossed repos: %d", status)
	}
}
