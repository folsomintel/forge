package gitcmd

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Pool keeps long-lived `git cat-file --batch-command` processes, one per
// hot repo (the Gitaly lesson): object reads skip fork+exec+pack-index-open
// on every call, which is most of the latency of small API reads on busy
// single-CPU machines.

const maxCatProcs = 16

// catOpTimeout bounds one cat-file request. A wedged process would
// otherwise hold the per-proc mutex forever and convoy every reader of that
// repo; on timeout we kill it (which unblocks the read) and the caller
// falls back to forking git.
const catOpTimeout = 15 * time.Second

type catProc struct {
	mu       sync.Mutex
	cmd      *exec.Cmd
	in       io.WriteCloser
	out      *bufio.Reader
	dead     atomic.Bool
	killOnce sync.Once
}

type Pool struct {
	mu    sync.Mutex
	procs map[string]*catProc // repoID -> proc
}

func (pl *Pool) get(repoID, dir string) (*catProc, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.procs == nil {
		pl.procs = map[string]*catProc{}
	}
	if p, ok := pl.procs[repoID]; ok {
		if !p.dead.Load() {
			return p, nil
		}
		delete(pl.procs, repoID) // reaped by whoever marked it dead; replace it
	}
	// Cap the fleet; evict an arbitrary victim (cheap, self-healing).
	if len(pl.procs) >= maxCatProcs {
		for id, victim := range pl.procs {
			victim.kill()
			delete(pl.procs, id)
			break
		}
	}
	cmd := exec.Command("git", "-C", dir, "cat-file", "--batch-command")
	cmd.Env = append(BaseEnv(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &catProc{cmd: cmd, in: stdin, out: bufio.NewReaderSize(stdout, 64<<10)}
	pl.procs[repoID] = p
	return p, nil
}

// Kill terminates the pooled process for a repo (cache drop/evict).
func (pl *Pool) Kill(repoID string) {
	pl.mu.Lock()
	if p, ok := pl.procs[repoID]; ok {
		p.kill()
		delete(pl.procs, repoID)
	}
	pl.mu.Unlock()
}

// kill reaps the process exactly once (idempotent): a protocol error, a
// timeout, and an explicit evict can all race to kill the same proc.
func (p *catProc) kill() {
	p.killOnce.Do(func() {
		p.dead.Store(true)
		p.in.Close()
		p.cmd.Process.Kill()
		p.cmd.Wait() // reap; without this every protocol error leaks a zombie
	})
}

// do runs one cat-file request under the proc mutex with a watchdog: a
// stuck read is killed (unblocking it) rather than convoying every reader.
// Any error kills+reaps the proc so get() replaces it. fn must not retain
// the reader past return.
func (p *catProc) do(fn func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	timer := time.AfterFunc(catOpTimeout, p.kill)
	defer timer.Stop()
	err := fn()
	if err != nil {
		p.kill()
	}
	return err
}

// ObjectInfo returns type and size without contents ("info" command).
func (pl *Pool) ObjectInfo(repoID, dir, oid string) (typ string, size int64, err error) {
	p, err := pl.get(repoID, dir)
	if err != nil {
		return "", 0, err
	}
	var missing bool
	derr := p.do(func() error {
		if _, err := fmt.Fprintf(p.in, "info %s\n", oid); err != nil {
			return err
		}
		line, err := p.out.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == "missing" {
			missing = true
			return nil
		}
		if len(fields) != 3 {
			return fmt.Errorf("unexpected cat-file reply: %q", line)
		}
		typ = fields[1]
		size, _ = strconv.ParseInt(fields[2], 10, 64)
		return nil
	})
	if derr != nil {
		return "", 0, derr
	}
	if missing {
		return "", 0, fmt.Errorf("object %s missing", oid)
	}
	return typ, size, nil
}

// BlobContents reads a blob through the pooled process.
func (pl *Pool) BlobContents(repoID, dir, oid string) ([]byte, error) {
	p, err := pl.get(repoID, dir)
	if err != nil {
		return nil, err
	}
	var out []byte
	var missing bool
	derr := p.do(func() error {
		if _, err := fmt.Fprintf(p.in, "contents %s\n", oid); err != nil {
			return err
		}
		line, err := p.out.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == "missing" {
			missing = true
			return nil
		}
		if len(fields) != 3 {
			return fmt.Errorf("unexpected cat-file reply: %q", line)
		}
		size, _ := strconv.ParseInt(fields[2], 10, 64)
		buf := make([]byte, size+1) // trailing LF
		if _, err := io.ReadFull(p.out, buf); err != nil {
			return err
		}
		out = buf[:size]
		return nil
	})
	if derr != nil {
		return nil, derr
	}
	if missing {
		return nil, fmt.Errorf("object %s missing", oid)
	}
	return out, nil
}
