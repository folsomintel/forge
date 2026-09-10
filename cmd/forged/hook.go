package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/server"
)

// hook dispatches git server-side hooks. Runs as a child of receive-pack.
// Fast path: POST the parsed updates to the serving process over its unix
// socket (warm DB pool + S3 client). Fallback: do the work directly.
func hook(args []string) error {
	if len(args) < 1 || args[0] != "pre-receive" {
		return fmt.Errorf("unknown hook %v", args)
	}
	repoID := os.Getenv("FORGE_REPO_ID")
	if repoID == "" {
		return fmt.Errorf("FORGE_REPO_ID not set")
	}
	cfg := config.FromEnv()

	updates, err := ingest.ParseRefUpdates(os.Stdin)
	if err != nil {
		return err
	}
	if ns := os.Getenv("GIT_NAMESPACE"); ns != "" {
		for i := range updates {
			updates[i].Name = "refs/namespaces/" + ns + "/" + updates[i].Name
		}
	}

	sock := os.Getenv("FORGE_HOOK_SOCKET")
	if sock == "" {
		sock = filepath.Join(cfg.DataDir, server.HookSocketName)
	}
	if _, err := os.Stat(sock); err == nil {
		body, _ := json.Marshal(map[string]any{
			"repo": repoID, "pusher": os.Getenv("FORGE_PUSHER"),
			"updates": updates, "quarantine": os.Getenv("GIT_QUARANTINE_PATH"),
			"staged_id": os.Getenv("FORGE_STAGED_ID"),
		})
		client := &http.Client{
			Timeout: 10 * time.Minute,
			Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sock)
			}},
		}
		hreq, _ := http.NewRequest("POST", "http://forge/pre-receive", bytes.NewReader(body))
		hreq.Header.Set("Content-Type", "application/json")
		hreq.Header.Set("X-Forge-Hook-Token", os.Getenv("FORGE_HOOK_TOKEN"))
		resp, err := client.Do(hreq)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			msg, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusConflict {
				fmt.Fprintf(os.Stderr, "rejected: %s (another push updated this ref first - fetch and retry)\n", strings.TrimSpace(string(msg)))
			}
			return fmt.Errorf("push rejected: %s", strings.TrimSpace(string(msg)))
		}
		// Socket present but unreachable: fall through to the direct path.
	}

	db, blobs, err := server.OpenStores(cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	err = ingest.Apply(ctx, db, blobs, repoID, os.Getenv("FORGE_PUSHER"), updates, os.Getenv("GIT_QUARANTINE_PATH"), nil)
	if err != nil && errors.Is(err, repodb.ErrCASFailed) {
		fmt.Fprintf(os.Stderr, "rejected: %v (another push updated this ref first - fetch and retry)\n", err)
	}
	return err
}
