package sshd

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"

	"github.com/folsomintel/forge/internal/blobstore"
)

// hostKeyBlob is the instance-global (not per-repo) SSH host key. Storing
// it in the bucket keeps it stable across machine recreates, so clients
// never see a "host key changed" warning after a heal/migrate. The
// private key never leaves the operator's own storage.
const hostKeyInstance = "_forge"
const hostKeyBlob = "ssh_host_key"

// LoadOrCreateHostKey returns a stable SSH host key: local file first (fast
// path), then the bucket, generating and persisting a fresh ed25519 key if
// neither exists.
func LoadOrCreateHostKey(ctx context.Context, blobs blobstore.Store, dataDir string) (ssh.Signer, error) {
	localPath := filepath.Join(dataDir, "ssh_host_key.pem")

	if pemBytes, err := os.ReadFile(localPath); err == nil {
		if signer, err := ssh.ParsePrivateKey(pemBytes); err == nil {
			return signer, nil
		}
	}

	// Bucket copy (survives recreate). Cache it locally on the way through.
	if rc, err := blobs.Get(ctx, hostKeyInstance, hostKeyBlob); err == nil {
		pemBytes, _ := io.ReadAll(rc)
		rc.Close()
		if signer, err := ssh.ParsePrivateKey(pemBytes); err == nil {
			os.WriteFile(localPath, pemBytes, 0o600)
			return signer, nil
		}
	}

	// First boot for this instance: generate, persist to the bucket, cache.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := blobs.Put(ctx, hostKeyInstance, hostKeyBlob, bytes.NewReader(pemBytes)); err != nil {
		return nil, err
	}
	os.WriteFile(localPath, pemBytes, 0o600)
	return ssh.ParsePrivateKey(pemBytes)
}
