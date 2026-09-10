package githttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

// Git-over-SSH runs the classic (non-stateless-rpc) pack protocol straight
// over an SSH channel, reusing the same cache materialization and the same
// pre-receive hook → WAL durability path as smart HTTP. The transport
// differs; the truth pipeline is identical. (SSH pushes go through git
// receive-pack + the hook, not the HTTP-only fast path or staged tee -
// correct, just not the fast path.)

var errSSHUnauthorized = errors.New("not authorized for this repository")

// RunGitSSH serves one git operation for an authenticated SSH session.
// service is "git-upload-pack" | "git-receive-pack"; repoArg is the raw
// path from the SSH command line (e.g. "'/my-repo.git'"). claims come from
// the key that authenticated the connection.
func (h *Handler) RunGitSSH(ctx context.Context, service, repoArg, pusher string, claims *auth.Claims, stdin io.Reader, stdout, stderr io.Writer) error {
	repo, ns := parseSSHTarget(repoArg)
	if repo == "" {
		return errors.New("no repository specified")
	}
	// Defense in depth: reject a malformed repo id at the wire ingress,
	// mirroring the HTTP path, before it reaches materialize/the store.
	if !repodb.ValidRepoID(repo) {
		return fmt.Errorf("repository not found")
	}
	if claims == nil || !claims.Allow(scopeFor(service), repo) {
		return errSSHUnauthorized
	}

	h.active.Add(1)
	defer h.active.Add(-1)

	// Same fork gate as smart HTTP: an SSH push/fetch forks git + the hook,
	// so a stampede of SSH sessions must queue behind the same bound or it
	// can OOM a small box independently of the HTTP path.
	release, ok := h.acquireFork(ctx)
	if !ok {
		return errors.New("server busy - retry")
	}
	defer release()

	lock := h.Cache.Lock(repo)
	dir, err := h.Cache.MaterializeServe(ctx, repo)
	if err != nil {
		return fmt.Errorf("repository not found")
	}
	lock.RLock()
	defer lock.RUnlock()

	bin := strings.TrimPrefix(service, "git-") // upload-pack | receive-pack
	args := []string{}
	if ns == "" {
		args = append(args, "-c", "transfer.hideRefs=refs/namespaces")
	}
	if service == "git-receive-pack" {
		args = append(args, "-c", "core.hooksPath="+h.HooksDir)
		if h.MaxPushBytes > 0 {
			args = append(args, "-c", fmt.Sprintf("receive.maxInputSize=%d", h.MaxPushBytes))
		}
	}
	args = append(args, bin, dir) // classic protocol: no --stateless-rpc

	cmd := exec.CommandContext(ctx, "git", args...)
	// BaseEnv strips credential-bearing vars; HookEnv (re-adds S3 creds the
	// hook fallback needs) is appended only for receive-pack, mirroring HTTP.
	cmd.Env = append(gitcmd.BaseEnv(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"FORGE_REPO_ID="+repo,
		"FORGE_SELF="+h.SelfPath,
		"FORGE_PUSHER="+pusher,
	)
	if service == "git-receive-pack" {
		cmd.Env = append(cmd.Env, h.HookEnv...)
	}
	if ns != "" {
		cmd.Env = append(cmd.Env, "GIT_NAMESPACE="+ns)
	}
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

// SSHServiceScope reports whether an SSH exec command names a git service
// this server handles, returning the canonical service name and repo arg.
func SSHServiceScope(cmdLine string) (service, repoArg string, ok bool) {
	cmdLine = strings.TrimSpace(cmdLine)
	for _, svc := range []string{"git-upload-pack", "git-receive-pack"} {
		if rest, found := strings.CutPrefix(cmdLine, svc); found {
			return svc, strings.TrimSpace(rest), true
		}
		// git clients may also send "git upload-pack ...".
		spaced := "git " + strings.TrimPrefix(svc, "git-")
		if rest, found := strings.CutPrefix(cmdLine, spaced); found {
			return svc, strings.TrimSpace(rest), true
		}
	}
	return "", "", false
}

// parseSSHTarget turns a raw SSH repo argument ("'/my-repo.git'",
// "my-repo+ephemeral.git") into a repo id and namespace, mirroring the
// HTTP parseTarget rules.
func parseSSHTarget(arg string) (repo, namespace string) {
	arg = strings.TrimSpace(arg)
	arg = strings.Trim(arg, "'\"")
	arg = strings.TrimPrefix(arg, "/")
	arg = strings.TrimSuffix(arg, ".git")
	if base, ok := strings.CutSuffix(arg, "+ephemeral"); ok {
		return base, repodb.Namespace
	}
	return arg, ""
}
