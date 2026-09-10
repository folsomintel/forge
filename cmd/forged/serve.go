package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/server"
	"github.com/folsomintel/forge/internal/sshd"
)

// readyDrainGrace is how long we advertise /readyz unready before we stop
// accepting connections, so the edge health-checker can observe the flip and
// route new traffic elsewhere first. Comfortably above a typical 2-3s probe
// interval; a second Ctrl-C bypasses it.
const readyDrainGrace = 6 * time.Second

func serve() error {
	cfg := config.FromEnv()
	srv, err := server.Build(cfg)
	if err != nil {
		return err
	}
	defer srv.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	srv.Start(ctx)

	// git over SSH (git@host:repo.git), if enabled. Same auth keys, same
	// materialize + hook → WAL path as HTTP; just a different transport.
	if cfg.SSHAddr != "" {
		hostKey, err := sshd.LoadOrCreateHostKey(ctx, srv.Blobs, cfg.DataDir)
		if err != nil {
			return err
		}
		ssrv := &sshd.Server{Addr: cfg.SSHAddr, HostKey: hostKey, Auth: srv.Auth, Git: srv.GitHTTP}
		go func() {
			if err := ssrv.ListenAndServe(ctx); err != nil {
				slog.Error("ssh server exited", "err", err)
			}
		}()
	}

	hsrv := &http.Server{
		Addr:    cfg.Addr,
		Handler: srv.Mux,
		// No global write timeout: clones/pushes of large repos are long-lived
		// and legitimately slow. Bound only the header read (slowloris guard).
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	slog.Info("forged listening", "addr", cfg.Addr, "store", cfg.StoreKind, "db", cfg.DBPath, "ssh", cfg.SSHAddr)

	errCh := make(chan error, 1)
	go func() {
		if err := hsrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		// Graceful shutdown: stop advertising readiness so the edge drains new
		// traffic, then let in-flight requests (pushes are durable only once
		// the hook acks) finish before closing. Fly's default kill grace is
		// generous; bound our wait so we never hang forever.
		stop() // restore default signal behavior for a second Ctrl-C
		slog.Info("shutting down: draining in-flight transfers")
		srv.SetDraining(true)
		// Give the edge health-checker time to observe /readyz flip to
		// unready and stop routing BEFORE we stop accepting: otherwise new
		// connections it hasn't drained yet race in and get refused instead
		// of landing on a healthy instance. A second Ctrl-C (default signal
		// behavior restored above) still hard-kills immediately.
		time.Sleep(readyDrainGrace)
		shutCtx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		if err := hsrv.Shutdown(shutCtx); err != nil {
			slog.Warn("graceful shutdown timed out; forcing close", "err", err)
			hsrv.Close()
		}
		slog.Info("drained; exiting", "active_transfers_remaining", srv.ActiveTransfers())
		return nil
	}
}
