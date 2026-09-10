package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/ssh"
)

// credential is a git credential helper: instead of embedding a long-lived
// token in the remote URL (which lands in .git/config, shell history, and
// proxy logs), git calls this on demand and it mints a fresh short-lived
// JWT from the private key. Configure per host:
//
//	git config credential."https://git.example.com".helper \
//	  "!forged credential --key ~/.forge/id.pem"
//
// git invokes `<helper> get` with a key=value request on stdin; we reply
// with username + a freshly-signed password. store/erase are no-ops (there
// is nothing to persist - every token is minted fresh).
func credential(args []string) error {
	fs := flag.NewFlagSet("credential", flag.ExitOnError)
	keyPath := fs.String("key", "", "path to the private key PEM (EC P-256, RSA, or Ed25519)")
	scopes := fs.String("scopes", "git:read git:write repo:write org:read", "space-separated scopes")
	ttl := fs.Duration("ttl", 5*time.Minute, "token lifetime (kept short since it is minted per operation)")
	sub := fs.String("sub", "git", "subject claim")
	fs.Parse(args)

	op := "get"
	if fs.NArg() > 0 {
		op = fs.Arg(0)
	}
	if op != "get" {
		return nil // store/erase: nothing to do
	}
	if *keyPath == "" {
		return fmt.Errorf("--key is required")
	}

	// Read git's request (key=value lines, blank-terminated). We restrict the
	// token to the requested repo path when git supplies one, so a helper
	// scoped to a host still yields least-privilege per-repo tokens.
	req := map[string]string{}
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			break
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			req[k] = v
		}
	}
	repo := strings.TrimSuffix(strings.TrimPrefix(req["path"], "/"), ".git")

	token, err := signJWT(*keyPath, *scopes, repo, *sub, *ttl)
	if err != nil {
		return err
	}
	fmt.Printf("username=%s\npassword=%s\n", *sub, token)
	return nil
}

// signJWT mints a scoped, short-lived JWT signed by a private key of any
// type the verifier accepts (ES256 / RS256 / EdDSA), in any common PEM
// encoding (OpenSSH, PKCS8, SEC1, PKCS1).
func signJWT(keyPath, scopes, repo, sub string, ttl time.Duration) (string, error) {
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}
	raw, err := ssh.ParseRawPrivateKey(pemBytes)
	if err != nil {
		return "", fmt.Errorf("parse private key: %w", err)
	}
	key, method, err := jwtSigner(raw)
	if err != nil {
		return "", err
	}
	claims := jwt.MapClaims{
		"sub": sub, "scopes": scopes,
		"iat": time.Now().Unix(), "exp": time.Now().Add(ttl).Unix(),
	}
	if repo != "" {
		claims["repo"] = repo
	}
	return jwt.NewWithClaims(method, claims).SignedString(key)
}

// jwtSigner maps a parsed private key to its jwt.Key + alg. ParseRawPrivateKey
// returns pointers for EC/RSA and a pointer for Ed25519; the jwt lib wants
// an ed25519.PrivateKey value, so deref that one.
func jwtSigner(raw any) (any, jwt.SigningMethod, error) {
	switch k := raw.(type) {
	case *ecdsa.PrivateKey:
		return k, jwt.SigningMethodES256, nil
	case *rsa.PrivateKey:
		return k, jwt.SigningMethodRS256, nil
	case *ed25519.PrivateKey:
		return *k, jwt.SigningMethodEdDSA, nil
	case ed25519.PrivateKey:
		return k, jwt.SigningMethodEdDSA, nil
	}
	return nil, nil, fmt.Errorf("unsupported private key type %T", raw)
}
