package ingest

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/repodb"
)

// Offline import: multi-GB migrations never run inside a push request.
// The client POSTs a bundle URL and polls status; the server downloads,
// indexes out-of-band (retry-safe, no proxy timeouts, no hostage client),
// and lands refs with the same quarantine -> store -> CAS pipeline as
// every other write. Because we also WRITE v2 bundles, we parse them
// natively: header (refs) + pack section fed straight to index-pack.

const importMaxBytes = 32 << 30 // safety cap

// StartImport downloads and applies a bundle; call in a goroutine.
func (s *Service) StartImport(repoID, url string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	// Guarantee a terminal state even on panic. A row stuck at "running"
	// blocks every future import of this repo with no recovery path, so a
	// panicking runImport must not leave it there. Uses a fresh context: the
	// 2h one may already be cancelled by the time we recover.
	settled := false
	defer func() {
		if r := recover(); r != nil {
			slog.Error("import panicked", "repo", repoID, "panic", r)
			s.DB.SetImport(context.Background(), repoID, "error", fmt.Sprintf("panic: %v", r), 0)
		} else if !settled {
			s.DB.SetImport(context.Background(), repoID, "error", "import aborted", 0)
		}
	}()
	s.DB.SetImport(ctx, repoID, "running", "", 0)
	refs, err := s.runImport(ctx, repoID, url)
	if err != nil {
		slog.Error("import failed", "repo", repoID, "err", err)
		s.DB.SetImport(ctx, repoID, "error", err.Error(), 0)
		settled = true
		return
	}
	s.DB.SetImport(ctx, repoID, "done", "", refs)
	settled = true
	slog.Info("import complete", "repo", repoID, "refs", refs)
	// A freshly imported monster should be immediately cheap to clone:
	// force one maintenance run (bitmap, commit-graph, clone bundle).
	if s.Maintain != nil {
		s.Maintain.Nudge(repoID)
	}
}

func (s *Service) runImport(ctx context.Context, repoID, url string) (int, error) {
	// Download to disk first: index-pack wants a seek-free stream, and a
	// flaky origin shouldn't hold a repo lock.
	tmp, err := os.CreateTemp(s.Cache.Dir, "import-*.bundle")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmp.Name())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.ImportClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("download bundle: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("download bundle: %s", resp.Status)
	}
	if _, err := io.Copy(tmp, io.LimitReader(resp.Body, importMaxBytes)); err != nil {
		tmp.Close()
		return 0, err
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		tmp.Close()
		return 0, err
	}
	defer tmp.Close()

	refs, packReader, err := readBundle(tmp)
	if err != nil {
		return 0, err
	}
	if len(refs) == 0 {
		return 0, fmt.Errorf("bundle contains no refs")
	}

	lock := s.Cache.Lock(repoID)
	lock.Lock()
	defer lock.Unlock()
	dir, err := s.Cache.Materialize(ctx, repoID)
	if err != nil {
		return 0, err
	}
	qdir, err := os.MkdirTemp(dir, "quarantine-import-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(qdir)
	if err := os.Mkdir(filepath.Join(qdir, "pack"), 0o755); err != nil {
		return 0, err
	}

	// Index the bundle's pack section into the quarantine (this is the CPU
	// grind, now safely out-of-band). With GIT_OBJECT_DIRECTORY pointed at
	// the quarantine, index-pack writes the canonically-named pack there
	// itself; alternates give --fix-thin its bases.
	x := &gitcmd.Exec{Ctx: ctx, Dir: dir, Env: []string{
		"GIT_OBJECT_DIRECTORY=" + qdir,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES=" + filepath.Join(dir, "objects"),
	}}
	if _, err := x.RunIn(packReader, "index-pack", "--stdin", "--fix-thin"); err != nil {
		return 0, fmt.Errorf("index bundle pack: %w", err)
	}

	// Same durability pipeline as a push: blobs first, then one atomic
	// all-or-nothing ref CAS (fails if any target ref already exists).
	updates := make([]repodb.RefUpdate, 0, len(refs))
	for _, r := range refs {
		updates = append(updates, repodb.RefUpdate{Name: r.Name, Old: repodb.ZeroOID, New: r.Target})
	}
	if err := Apply(ctx, s.DB, s.Blobs, repoID, "import", updates, qdir, nil); err != nil {
		if errors.Is(err, repodb.ErrCASFailed) {
			return 0, fmt.Errorf("import conflicts with existing refs (import requires them absent): %w", err)
		}
		return 0, err
	}
	s.Cache.Invalidate(repoID)
	return len(updates), nil
}

// readBundle parses a v2 bundle: header lines (refs; '-'-prefixed
// prerequisites are rejected - we only accept self-contained bundles),
// blank line, then the packfile.
func readBundle(r io.Reader) ([]repodb.Ref, io.Reader, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	first, err := br.ReadString('\n')
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(first) != "# v2 git bundle" {
		return nil, nil, fmt.Errorf("not a v2 git bundle (got %q)", strings.TrimSpace(first))
	}
	var refs []repodb.Ref
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, nil, fmt.Errorf("truncated bundle header: %w", err)
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			break // header/pack separator
		}
		if strings.HasPrefix(line, "-") {
			return nil, nil, fmt.Errorf("bundle has prerequisites; only self-contained bundles can be imported")
		}
		oid, name, ok := strings.Cut(line, " ")
		if !ok || len(oid) < 40 {
			return nil, nil, fmt.Errorf("malformed bundle ref line %q", line)
		}
		refs = append(refs, repodb.Ref{Name: name, Target: oid})
	}
	return refs, br, nil
}
