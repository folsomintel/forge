package main

import (
	"flag"
	"fmt"
	"time"
)

// token mints a JWT for local testing. In production, customers sign their
// own with the private key from keygen (or use `forged credential` as a git
// helper so a fresh token is minted per operation).
func token(args []string) error {
	fs := flag.NewFlagSet("token", flag.ExitOnError)
	keyPath := fs.String("key", "", "path to private key PEM (EC P-256, RSA, or Ed25519)")
	scopes := fs.String("scopes", "git:read git:write repo:write org:read", "space-separated scopes")
	repo := fs.String("repo", "", "restrict token to one repo id")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	sub := fs.String("sub", "cli", "subject claim")
	fs.Parse(args)

	if *keyPath == "" {
		return fmt.Errorf("--key is required")
	}
	signed, err := signJWT(*keyPath, *scopes, *repo, *sub, *ttl)
	if err != nil {
		return err
	}
	fmt.Println(signed)
	return nil
}
