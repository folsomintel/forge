package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/ssh"

	"github.com/folsomintel/forge/internal/repodb"
)

// keyStore is a minimal DB stub exposing one registered public key.
type keyStore struct {
	repodb.DB
	pem string
}

func (k keyStore) ListKeys(context.Context) ([]repodb.Key, error) {
	return []repodb.Key{{ID: 1, Name: "ed25519", PublicKeyPEM: k.pem}}, nil
}

func TestEd25519PKIXKeySignsAndVerifies(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	pemStr := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": "laptop", "scopes": "git:read git:write",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}

	v := &Verifier{DB: keyStore{pem: pemStr}}
	claims, err := v.Verify(context.Background(), signed)
	if err != nil {
		t.Fatalf("verify ed25519 JWT: %v", err)
	}
	if !claims.Allow(ScopeGitWrite, "any") {
		t.Fatalf("expected git:write, got %v", claims.Scopes)
	}
}

func TestOpenSSHEd25519KeyParses(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	authLine := string(ssh.MarshalAuthorizedKey(sshPub)) // "ssh-ed25519 AAAA...\n"

	parsed, err := ParsePublicKeyPEM(authLine)
	if err != nil {
		t.Fatalf("parse ssh-ed25519 line: %v", err)
	}
	if _, ok := parsed.(ed25519.PublicKey); !ok {
		t.Fatalf("expected ed25519.PublicKey, got %T", parsed)
	}
}

func TestRepoRestrictedTokenDeniesIdlessOps(t *testing.T) {
	c := &Claims{Scopes: []string{ScopeOrgRead, ScopeGitRead}, Repo: "myrepo"}
	// Allowed only on its own repo.
	if !c.Allow(ScopeGitRead, "myrepo") {
		t.Fatal("should allow its own repo")
	}
	if c.Allow(ScopeGitRead, "other") {
		t.Fatal("must deny a different repo")
	}
	// The bug: id-less/org-wide ops (repoID=="") must be denied for a
	// repo-restricted token (listRepos/listKeys/listAudit).
	if c.Allow(ScopeOrgRead, "") {
		t.Fatal("repo-restricted token must be denied on id-less ops")
	}
	// An unrestricted token still works on id-less ops.
	u := &Claims{Scopes: []string{ScopeOrgRead}}
	if !u.Allow(ScopeOrgRead, "") {
		t.Fatal("unrestricted token should allow id-less ops")
	}
}
