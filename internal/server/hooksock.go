package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
)

// The pre-receive hook calls back into the running server over a unix
// socket instead of reopening SQLite and cold-starting a TLS session to
// the blob store on every push. Same-user socket permissions are the auth
// boundary; the standalone hook path remains as a fallback.

const HookSocketName = "hook.sock"

type hookRequest struct {
	Repo       string             `json:"repo"`
	Pusher     string             `json:"pusher"`
	Updates    []repodb.RefUpdate `json:"updates"`
	Quarantine string             `json:"quarantine"`
	StagedID   string             `json:"staged_id,omitempty"`
}

func (s *Server) startHookSocket() error {
	sock := filepath.Join(s.Cfg.DataDir, HookSocketName)
	os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		// Unix socket paths cap at ~104 bytes (macOS test tmpdirs exceed
		// it); fall back to a short path and tell hooks via env.
		short, terr := os.MkdirTemp("", "forge-*")
		if terr != nil {
			return err
		}
		sock = filepath.Join(short, HookSocketName)
		if ln, err = net.Listen("unix", sock); err != nil {
			return err
		}
	}
	s.HookSocket = sock
	os.Chmod(sock, 0o600)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pre-receive", func(w http.ResponseWriter, r *http.Request) {
		// The 0o600 socket already restricts callers to this UID; the token is
		// defense in depth so an unrelated same-UID process can't inject a
		// push. Constant-time compare; the hook carries it via FORGE_HOOK_TOKEN
		// (a per-boot secret git passes to the hook, never on disk).
		if s.HookToken != "" &&
			subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Forge-Hook-Token")), []byte(s.HookToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req hookRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		staged := s.Stager.Claim(req.StagedID)
		err := ingest.Apply(r.Context(), s.DB, s.Blobs, req.Repo, req.Pusher, req.Updates, req.Quarantine, staged)
		s.Stager.Discard(staged) // no-op when Apply consumed it
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, repodb.ErrCASFailed) {
				status = http.StatusConflict
			}
			http.Error(w, err.Error(), status)
			return
		}
		// Push landed: check whether this repo crossed the maintenance
		// threshold (async, single-flight, cheap when it hasn't).
		s.Maintain.NudgeIfNeeded(r.Context(), req.Repo)
		w.WriteHeader(http.StatusOK)
	})
	go func() {
		if err := (&http.Server{Handler: mux}).Serve(ln); err != nil {
			slog.Error("hook socket server exited", "err", err)
		}
	}()
	return nil
}
