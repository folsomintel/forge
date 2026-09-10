// Package gitcmd is the git subprocess layer: one-shot plumbing execs with
// quarantine env support, output parsers, and pooled cat-file daemons.
package gitcmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Exec runs git plumbing against a cache repo, optionally with a quarantine
// object dir layered in front (for write pipelines).
type Exec struct {
	Ctx context.Context
	Dir string
	Env []string // extra env (quarantine overrides etc.)
}

// forkGate bounds concurrent git subprocesses PROCESS-WIDE, the same
// protection the wire paths' runGit gate provides: a burst of forked
// plumbing (API fallbacks, maintenance, merges) queues briefly and then
// errors loudly instead of fork-bombing a small machine. Nil (default,
// e.g. hook subprocesses) = ungated. atomic.Pointer because parallel
// test servers install gates concurrently; acquire captures the channel
// VALUE so a later install can never strand a release (a closure over
// the mutable global once deadlocked the whole e2e suite).
var forkGate atomic.Pointer[chan struct{}]

// SetForkGate installs the process-wide fork bound. First install wins;
// production calls it once at boot.
func SetForkGate(n int) {
	if n <= 0 {
		return
	}
	ch := make(chan struct{}, n)
	forkGate.CompareAndSwap(nil, &ch)
}

var errForkGate = errors.New("git subprocess gate timeout: server busy")

func acquireFork(ctx context.Context) (func(), error) {
	gp := forkGate.Load()
	if gp == nil {
		return func() {}, nil
	}
	g := *gp
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case g <- struct{}{}:
		return func() { <-g }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errForkGate
	}
}

func (x *Exec) command(stdin io.Reader, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(x.Ctx, "git", append([]string{"-C", x.Dir}, args...)...)
	cmd.Env = append(BaseEnv(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
	)
	cmd.Env = append(cmd.Env, x.Env...)
	cmd.Stdin = stdin
	return cmd
}

// secretEnvSubstrings marks env vars that carry credentials git children have
// no business inheriting: S3/AWS keys, the control-plane admin token, the
// seal key, Stripe/WorkOS secrets. Plumbing (ls-tree/log/cat-file) and
// upload-pack need none of these; the receive-pack hook, which DOES need S3
// creds for its direct-apply fallback, gets them re-added explicitly via
// HookEnv (see githttp.Handler). Matched case-insensitively as substrings so
// a new FORGE_*_SECRET / *_TOKEN / *_KEY var is caught without a code change.
var secretEnvSubstrings = []string{
	"SECRET", "TOKEN", "PASSWORD", "PRIVATE", "ACCESS_KEY", "SEAL", "STRIPE", "WORKOS", "X402",
}

// BaseEnv is os.Environ() with credential-bearing vars stripped, the base for
// every git subprocess. Never leak S3 keys or the admin token into a forked
// git or its descendants.
func BaseEnv() []string {
	src := os.Environ()
	out := make([]string, 0, len(src))
	for _, kv := range src {
		name := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			name = kv[:i]
		}
		up := strings.ToUpper(name)
		secret := false
		for _, s := range secretEnvSubstrings {
			if strings.Contains(up, s) {
				secret = true
				break
			}
		}
		if !secret {
			out = append(out, kv)
		}
	}
	return out
}

// Run executes git and returns stdout; stderr is folded into the error.
func (x *Exec) Run(args ...string) (string, error) {
	return x.RunIn(nil, args...)
}

func (x *Exec) RunIn(stdin io.Reader, args ...string) (string, error) {
	release, err := acquireFork(x.Ctx)
	if err != nil {
		return "", err
	}
	defer release()
	cmd := x.command(stdin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// RunCode is Run but exit codes are data, not errors (merge-tree reports
// conflicts via exit 1). err is non-nil only for non-exit failures.
func (x *Exec) RunCode(stdin io.Reader, args ...string) (string, int, error) {
	release, gerr := acquireFork(x.Ctx)
	if gerr != nil {
		return "", -1, gerr
	}
	defer release()
	cmd := x.command(stdin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err == nil {
		return out.String(), 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return out.String(), exitErr.ExitCode(), nil
	}
	return out.String(), -1, fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
}

// RunStream executes git and streams stdout to w (blob/archive serving).
func (x *Exec) RunStream(w io.Writer, args ...string) error {
	release, err := acquireFork(x.Ctx)
	if err != nil {
		return err
	}
	defer release()
	cmd := x.command(nil, args...)
	var errb bytes.Buffer
	cmd.Stdout = w
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(errb.String()))
	}
	return nil
}

// --- read helpers ---

type TreeEntry struct {
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size int64  `json:"size"`
	Path string `json:"path"`
}

// LsTree lists a tree; path may be "" for the root. recursive lists all.
func (x *Exec) LsTree(treeish, path string, recursive bool) ([]TreeEntry, error) {
	args := []string{"ls-tree", "-l", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	// --end-of-options: treeish is caller-supplied ({sha} path param); without
	// this a value like "--output=x" would be parsed as a git option (arg
	// injection), not a revision. Marks everything after as operands.
	args = append(args, "--end-of-options", treeish)
	if path != "" {
		args = append(args, "--", path)
	}
	out, err := x.Run(args...)
	if err != nil {
		return nil, err
	}
	var entries []TreeEntry
	for _, line := range strings.Split(out, "\x00") {
		if line == "" {
			continue
		}
		// <mode> SP <type> SP <sha> SP+ <size> TAB <path>
		info, name, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		fields := strings.Fields(info)
		if len(fields) != 4 {
			continue
		}
		size := int64(0)
		if fields[3] != "-" {
			size, _ = strconv.ParseInt(fields[3], 10, 64)
		}
		entries = append(entries, TreeEntry{Mode: fields[0], Type: fields[1], SHA: fields[2], Size: size, Path: name})
	}
	return entries, nil
}

type Commit struct {
	SHA       string   `json:"sha"`
	Tree      string   `json:"tree"`
	Parents   []string `json:"parents"`
	Author    string   `json:"author"`
	Email     string   `json:"email"`
	Timestamp int64    `json:"timestamp"`
	Message   string   `json:"message"`
}

const logFormat = "%H%x00%T%x00%P%x00%an%x00%ae%x00%at%x00%B%x1e"

func parseCommits(out string) []Commit {
	var commits []Commit
	for _, rec := range strings.Split(out, "\x1e") {
		rec = strings.TrimLeft(rec, "\n")
		if rec == "" {
			continue
		}
		f := strings.Split(rec, "\x00")
		if len(f) != 7 {
			continue
		}
		ts, _ := strconv.ParseInt(f[5], 10, 64)
		commits = append(commits, Commit{
			SHA: f[0], Tree: f[1], Parents: strings.Fields(f[2]),
			Author: f[3], Email: f[4], Timestamp: ts,
			Message: strings.TrimSpace(f[6]),
		})
	}
	return commits
}

// Log returns commits reachable from rev, newest first.
func (x *Exec) Log(rev, path string, limit int) ([]Commit, error) {
	// --end-of-options: rev is caller-supplied; keep an "--output=x"-style
	// value from being parsed as a git option instead of a revision.
	args := []string{"log", "--format=" + logFormat, "-n", strconv.Itoa(limit), "--end-of-options", rev}
	if path != "" {
		args = append(args, "--", path)
	}
	out, err := x.Run(args...)
	if err != nil {
		return nil, err
	}
	return parseCommits(out), nil
}

func (x *Exec) GetCommit(rev string) (*Commit, error) {
	commits, err := x.Log(rev, "", 1)
	if err != nil {
		return nil, err
	}
	if len(commits) == 0 {
		return nil, fmt.Errorf("commit not found: %s", rev)
	}
	return &commits[0], nil
}

// ObjectType returns blob/tree/commit/tag, or error if missing.
func (x *Exec) ObjectType(sha string) (string, error) {
	// --end-of-options: sha is caller-supplied; prevent option injection.
	out, err := x.Run("cat-file", "-t", "--end-of-options", sha)
	return strings.TrimSpace(out), err
}
