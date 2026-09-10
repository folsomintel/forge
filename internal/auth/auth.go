// Package auth verifies self-signed JWTs (the Pierre model): the operator
// registers client public keys; clients mint their own ES256/RS256 tokens
// with scope claims. No token-issuance round trip, and tokens embed in git
// remote URLs as the Basic-auth password.
package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/ssh"

	"github.com/folsomintel/forge/internal/repodb"
)

// Scopes. git:write implies git:read: a git push negotiates against the
// remote's refs, so write access is meaningless without read, and every
// real client that can push can already fetch. Making it implicit removes a
// class of "pushes work, clones 403" tokens.
const (
	ScopeGitRead   = "git:read"
	ScopeGitWrite  = "git:write"
	ScopeRepoWrite = "repo:write"
	ScopeOrgRead   = "org:read"
)

var ErrUnauthorized = errors.New("unauthorized")

type Claims struct {
	Subject string
	Scopes  []string
	Repo    string // non-empty = token restricted to this repo
}

func (c *Claims) Allow(scope, repoID string) bool {
	// A repo-restricted token is confined to exactly its repo - including a
	// hard deny on id-less/org-wide operations (listRepos, listKeys,
	// listAudit pass repoID=""), which would otherwise leak beyond the repo.
	if c.Repo != "" && c.Repo != repoID {
		return false
	}
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
		// git:write implies git:read (see the scope constants).
		if scope == ScopeGitRead && s == ScopeGitWrite {
			return true
		}
	}
	return false
}

type Verifier struct {
	DB repodb.DB

	mu     sync.Mutex
	keys   []any // parsed public keys
	loaded time.Time
}

// Invalidate drops the cached key set so the next verification re-reads from
// the DB. Call it right after a key is revoked in THIS process so the
// revocation takes effect immediately instead of lingering for the cache TTL.
// (A revoke from another process - the forged CLI - is still bounded by the
// 30s TTL, since it can't reach this cache.)
func (v *Verifier) Invalidate() {
	v.mu.Lock()
	v.keys, v.loaded = nil, time.Time{}
	v.mu.Unlock()
}

// publicKeys caches registered keys briefly; key registration is rare.
func (v *Verifier) publicKeys(ctx context.Context) ([]any, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if time.Since(v.loaded) < 30*time.Second && v.keys != nil {
		return v.keys, nil
	}
	rows, err := v.DB.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	var keys []any
	for _, k := range rows {
		pub, err := ParsePublicKeyPEM(k.PublicKeyPEM)
		if err != nil {
			continue // skip malformed rows rather than locking everyone out
		}
		keys = append(keys, pub)
	}
	v.keys, v.loaded = keys, time.Now()
	return keys, nil
}

// ParsePublicKeyPEM accepts a registered public key in either format a
// user is likely to have on hand: a PKIX PEM ("-----BEGIN PUBLIC KEY-----",
// covering RSA / ECDSA P-256 / Ed25519), or an OpenSSH authorized_keys line
// ("ssh-ed25519 AAAA...", "ssh-rsa ...", "ecdsa-sha2-..."). Both resolve to
// a crypto public key the JWT verifier can use. Ed25519 is first-class so
// modern `ssh-keygen -t ed25519` keys work.
func ParsePublicKeyPEM(keyStr string) (any, error) {
	s := strings.TrimSpace(keyStr)
	if strings.HasPrefix(s, "ssh-") || strings.HasPrefix(s, "ecdsa-") || strings.HasPrefix(s, "sk-") {
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s))
		if err != nil {
			return nil, fmt.Errorf("parse ssh key: %w", err)
		}
		ck, ok := pub.(ssh.CryptoPublicKey)
		if !ok {
			return nil, errors.New("unsupported ssh key type")
		}
		return ck.CryptoPublicKey(), nil
	}
	block, _ := pem.Decode([]byte(s))
	if block == nil {
		return nil, errors.New("not a PEM public key or OpenSSH key line")
	}
	return x509.ParsePKIXPublicKey(block.Bytes)
}

// FromRequest authenticates via Bearer token or Basic auth (token as the
// password, any username - the git-over-HTTPS convention).
func (v *Verifier) FromRequest(r *http.Request) (*Claims, error) {
	return v.VerifyAuthorization(r.Context(), r.Header.Get("Authorization"))
}

// VerifyAuthorization authenticates a raw Authorization header value.
func (v *Verifier) VerifyAuthorization(ctx context.Context, header string) (*Claims, error) {
	raw := ""
	if strings.HasPrefix(header, "Bearer ") {
		raw = strings.TrimPrefix(header, "Bearer ")
	} else if strings.HasPrefix(header, "Basic ") {
		if decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic ")); err == nil {
			if _, pass, ok := strings.Cut(string(decoded), ":"); ok {
				raw = pass
			}
		}
	}
	if raw == "" {
		return nil, ErrUnauthorized
	}
	return v.Verify(ctx, raw)
}

func (v *Verifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	keys, err := v.publicKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		tok, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
			switch key.(type) {
			case *ecdsa.PublicKey:
				if _, ok := t.Method.(*jwt.SigningMethodECDSA); !ok {
					return nil, fmt.Errorf("unexpected alg %s", t.Method.Alg())
				}
			case *rsa.PublicKey:
				if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
					return nil, fmt.Errorf("unexpected alg %s", t.Method.Alg())
				}
			case ed25519.PublicKey:
				if _, ok := t.Method.(*jwt.SigningMethodEd25519); !ok {
					return nil, fmt.Errorf("unexpected alg %s", t.Method.Alg())
				}
			}
			return key, nil
		}, jwt.WithValidMethods([]string{"ES256", "RS256", "EdDSA"}), jwt.WithExpirationRequired())
		if err != nil || !tok.Valid {
			continue
		}
		mc, ok := tok.Claims.(jwt.MapClaims)
		if !ok {
			continue
		}
		return claimsFromMap(mc), nil
	}
	return nil, ErrUnauthorized
}

// fullSSHGrant is the historical all-access scope set an SSH key gets when
// it carries no per-key scopes of its own (the GitHub model: your key is
// your credential). Keys registered with explicit scopes are held to them.
var fullSSHGrant = []string{ScopeGitRead, ScopeGitWrite, ScopeRepoWrite, ScopeOrgRead}

// AuthorizeSSHKey matches a public key presented over SSH against the
// registered keys and returns that key's granted scopes (its own, or the
// full grant when it has none). The subject is the key's name. Comparison
// is on the SSH wire encoding, so it works whether the key was registered
// as PKIX PEM or an OpenSSH line.
func (v *Verifier) AuthorizeSSHKey(ctx context.Context, presented ssh.PublicKey) (*Claims, bool) {
	rows, err := v.DB.ListKeys(ctx)
	if err != nil {
		return nil, false
	}
	want := presented.Marshal()
	for _, k := range rows {
		pub, err := ParsePublicKeyPEM(k.PublicKeyPEM)
		if err != nil {
			continue
		}
		sk, err := ssh.NewPublicKey(pub)
		if err != nil {
			continue
		}
		if subtleEqual(sk.Marshal(), want) {
			scopes := k.Scopes
			if len(scopes) == 0 {
				scopes = fullSSHGrant
			}
			return &Claims{Subject: "ssh:" + k.Name, Scopes: scopes}, true
		}
	}
	return nil, false
}

func subtleEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}

func claimsFromMap(mc jwt.MapClaims) *Claims {
	c := &Claims{}
	if sub, _ := mc.GetSubject(); sub != "" {
		c.Subject = sub
	}
	switch v := mc["scopes"].(type) {
	case string:
		c.Scopes = strings.Fields(v)
	case []any:
		for _, s := range v {
			if str, ok := s.(string); ok {
				c.Scopes = append(c.Scopes, str)
			}
		}
	}
	if repo, ok := mc["repo"].(string); ok {
		c.Repo = repo
	}
	return c
}
