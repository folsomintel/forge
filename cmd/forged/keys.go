package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/server"
)

// keygen creates an ECDSA P-256 keypair, registers the public key in the
// metadata DB, and prints the private key PEM to stdout (shown once, never
// stored - the customer keeps it and self-signs tokens).
func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	name := fs.String("name", "default", "key name")
	_ = fs.Parse(args)

	cfg := config.FromEnv()
	db, _, err := server.OpenStores(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return err
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}

	id, err := db.AddKey(context.Background(), *name, string(pubPEM), nil)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "registered public key %d (%s); private key below - save it, it is not stored\n", id, *name)
	return pem.Encode(os.Stdout, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
}

// addkey registers an existing client public key (PEM, base64-encoded so it
// survives argv). Used by the control plane to seed a fresh tenant machine.
func addkey(args []string) error {
	fs := flag.NewFlagSet("addkey", flag.ExitOnError)
	name := fs.String("name", "default", "key name")
	pemB64 := fs.String("pem-b64", "", "base64-encoded public key PEM")
	scopeStr := fs.String("scopes", "", "space-separated SSH scopes (default: full access)")
	_ = fs.Parse(args)
	if *pemB64 == "" {
		return fmt.Errorf("--pem-b64 is required")
	}
	pemBytes, err := base64.StdEncoding.DecodeString(*pemB64)
	if err != nil {
		return fmt.Errorf("decode --pem-b64: %w", err)
	}
	if _, err := auth.ParsePublicKeyPEM(string(pemBytes)); err != nil {
		return fmt.Errorf("invalid public key: %w", err)
	}
	cfg := config.FromEnv()
	db, _, err := server.OpenStores(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	id, err := db.AddKey(context.Background(), *name, string(pemBytes), strings.Fields(*scopeStr))
	if err != nil {
		return err
	}
	if err := db.AddAudit(context.Background(), repodb.AuditEntry{
		Actor: "operator", Action: "key.add", Target: *name,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "warning: audit log failed:", err)
	}
	fmt.Printf("registered key %d (%s)\n", id, *name)
	return nil
}

// delkey revokes a registered key by id. Used by the control plane to
// retire a compromised or stale credential from a tenant machine.
func delkey(args []string) error {
	fs := flag.NewFlagSet("delkey", flag.ExitOnError)
	id := fs.Int64("id", 0, "key id to revoke")
	_ = fs.Parse(args)
	if *id == 0 {
		return fmt.Errorf("--id is required")
	}
	cfg := config.FromEnv()
	db, _, err := server.OpenStores(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	existed, err := db.DeleteKey(context.Background(), *id)
	if err != nil {
		return err
	}
	if !existed {
		return fmt.Errorf("no key with id %d", *id)
	}
	if err := db.AddAudit(context.Background(), repodb.AuditEntry{
		Actor: "operator", Action: "key.delete", Target: fmt.Sprintf("%d", *id),
	}); err != nil {
		fmt.Fprintln(os.Stderr, "warning: audit log failed:", err)
	}
	fmt.Printf("revoked key %d\n", *id)
	return nil
}
