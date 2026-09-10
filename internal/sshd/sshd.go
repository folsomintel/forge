// Package sshd serves git over SSH: the familiar `git clone
// git@host:repo.git` transport, authenticated by the same public keys the
// instance already accepts (ed25519/rsa/ecdsa). It is a thin transport in
// front of the shared git-service path - key auth, then exec
// upload-pack/receive-pack against the cache repo, streaming over the SSH
// channel. All durability still flows through the pre-receive hook → WAL.
package sshd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/githttp"
)

type Server struct {
	Addr    string           // listen address, e.g. "0.0.0.0:2222"
	HostKey ssh.Signer       // persistent host key (stable across recreates)
	Auth    *auth.Verifier   // matches presented keys to registered ones
	Git     *githttp.Handler // shared materialize + git-service + hook/WAL path
}

// ListenAndServe runs until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			claims, ok := s.Auth.AuthorizeSSHKey(ctx, key)
			if !ok {
				return nil, fmt.Errorf("unknown key")
			}
			// Carry the authenticated subject to the session via permissions.
			return &ssh.Permissions{Extensions: map[string]string{"forge-subject": claims.Subject}}, nil
		},
		MaxAuthTries: 6,
	}
	cfg.AddHostKey(s.HostKey)

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); ln.Close() }()
	slog.Info("ssh git listening", "addr", s.Addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				slog.Warn("ssh accept", "err", err)
				continue
			}
		}
		go s.handleConn(ctx, conn, cfg)
	}
}

func (s *Server) handleConn(ctx context.Context, nConn net.Conn, cfg *ssh.ServerConfig) {
	defer nConn.Close()
	// Bound the handshake so a stalled client can't tie up a connection
	// (slowloris). Cleared once the SSH connection is established.
	nConn.SetDeadline(time.Now().Add(30 * time.Second))
	sconn, chans, reqs, err := ssh.NewServerConn(nConn, cfg)
	if err != nil {
		return // failed handshake / bad key; nothing to do
	}
	nConn.SetDeadline(time.Time{}) // transfers are legitimately long
	defer sconn.Close()
	subject := sconn.Permissions.Extensions["forge-subject"]
	go ssh.DiscardRequests(reqs)

	for newChan := range chans {
		if newChan.ChannelType() != "session" {
			newChan.Reject(ssh.UnknownChannelType, "only session channels")
			continue
		}
		ch, chReqs, err := newChan.Accept()
		if err != nil {
			continue
		}
		go s.handleSession(ctx, ch, chReqs, subject)
	}
}

// handleSession waits for the client's "exec" request (git runs a single
// command per connection), runs it, and reports the exit status.
func (s *Server) handleSession(ctx context.Context, ch ssh.Channel, reqs <-chan *ssh.Request, subject string) {
	defer ch.Close()
	for req := range reqs {
		switch req.Type {
		case "exec":
			cmdLine := parseExecPayload(req.Payload)
			if req.WantReply {
				req.Reply(true, nil)
			}
			status := s.runExec(ctx, ch, cmdLine, subject)
			sendExitStatus(ch, status)
			return
		case "shell":
			// Interactive shells are not supported; git only uses exec.
			if req.WantReply {
				req.Reply(false, nil)
			}
			io.WriteString(ch.Stderr(), "forge: interactive shell not supported; use git over SSH\n")
			sendExitStatus(ch, 1)
			return
		default:
			if req.WantReply {
				req.Reply(false, nil)
			}
		}
	}
}

func (s *Server) runExec(ctx context.Context, ch ssh.Channel, cmdLine, subject string) uint32 {
	service, repoArg, ok := githttp.SSHServiceScope(cmdLine)
	if !ok {
		io.WriteString(ch.Stderr(), "forge: only git-upload-pack and git-receive-pack are permitted\n")
		return 1
	}
	// The key already authenticated with full git scope; reconstruct claims.
	claims := &auth.Claims{
		Subject: subject,
		Scopes:  []string{auth.ScopeGitRead, auth.ScopeGitWrite, auth.ScopeRepoWrite, auth.ScopeOrgRead},
	}
	err := s.Git.RunGitSSH(ctx, service, repoArg, subject, claims, ch, ch, ch.Stderr())
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			io.WriteString(ch.Stderr(), "forge: "+err.Error()+"\n")
		}
		return 1
	}
	return 0
}

// parseExecPayload extracts the command string from an SSH "exec" request
// payload (a single length-prefixed string).
func parseExecPayload(payload []byte) string {
	if len(payload) < 4 {
		return ""
	}
	n := int(payload[0])<<24 | int(payload[1])<<16 | int(payload[2])<<8 | int(payload[3])
	if n < 0 || 4+n > len(payload) {
		return ""
	}
	return string(payload[4 : 4+n])
}

// sendExitStatus sends the SSH "exit-status" so the git client sees the
// right exit code instead of a generic transport error.
func sendExitStatus(ch ssh.Channel, code uint32) {
	ch.SendRequest("exit-status", false, []byte{
		byte(code >> 24), byte(code >> 16), byte(code >> 8), byte(code),
	})
}
