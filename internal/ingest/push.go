// Package ingest is everything that creates truth: git pushes (via the
// pre-receive hook), API-driven commits, and offline bundle imports. All
// three share one durability pipeline: quarantined objects are uploaded to
// the blob store and recorded, then every ref moves through one atomic CAS
// batch - only after that does anything get acknowledged.
package ingest

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/maintain"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

// Service wires the ingest paths that need a materialized cache repo
// (commits, imports) or post-write maintenance nudges.
type Service struct {
	Cache *repocache.Cache
	DB    repodb.DB
	Blobs blobstore.Store
	// ImportClient downloads customer-supplied bundle URLs; must be
	// SSRF-guarded (netguard).
	ImportClient *http.Client
	// Maintain, when set, receives post-write nudges (e.g. a completed
	// import immediately gets its bitmap, commit-graph and clone bundle).
	Maintain *maintain.Pipeline
}

// PreReceive is the durability point of a push. git receive-pack invokes us
// while the incoming objects are still quarantined; we (1) upload the
// quarantine pack(s) to the blob store, (2) record them in the pack list,
// (3) CAS every ref update in one metadata transaction. Only if all of that
// succeeds do we exit 0 and let git ack the push - so by the time the client
// sees "ok", the push is durable in the store + DB, not just on this node.
//
// A failed CAS (concurrent writer won) rejects the whole push atomically;
// the uploaded pack becomes unreferenced garbage for maintenance to sweep.
func PreReceive(ctx context.Context, db repodb.DB, blobs blobstore.Store, repoID string, stdin io.Reader, stderr io.Writer) error {
	updates, err := ParseRefUpdates(stdin)
	if err != nil {
		return err
	}
	// Namespaced pushes arrive with stripped names; store them under the
	// namespace they were pushed to.
	if ns := os.Getenv("GIT_NAMESPACE"); ns != "" {
		for i := range updates {
			updates[i].Name = "refs/namespaces/" + ns + "/" + updates[i].Name
		}
	}
	err = Apply(ctx, db, blobs, repoID, os.Getenv("FORGE_PUSHER"), updates, os.Getenv("GIT_QUARANTINE_PATH"), nil)
	if err != nil && errors.Is(err, repodb.ErrCASFailed) {
		fmt.Fprintf(stderr, "rejected: %v (another push updated this ref first - fetch and retry)\n", err)
	}
	return err
}

// Apply is the durability core shared by the in-server hook endpoint (fast
// path: warm S3 client + DB pool), the standalone hook fallback, and
// imports. staged, when non-nil, is the already-uploaded wire pack; a
// trailer match turns the pack upload into a server-side copy.
func Apply(ctx context.Context, db repodb.DB, blobs blobstore.Store, repoID, pusher string, updates []repodb.RefUpdate, quarantine string, staged *StagedPack) error {
	if len(updates) == 0 {
		return nil
	}
	if quarantine != "" {
		uploaded, err := uploadQuarantinePacks(ctx, blobs, repoID, quarantine, staged)
		if err != nil {
			return fmt.Errorf("store packs: %w", err)
		}
		if len(uploaded) > 0 {
			if err := db.AddPacks(ctx, repoID, uploaded); err != nil {
				return fmt.Errorf("record packs: %w", err)
			}
		}
	}
	// Transactional outbox: events commit with the ref CAS or not at all.
	events := webhook.PushEventsFor(repoID, updates, pusher)
	return db.UpdateRefs(ctx, repoID, updates, events)
}

func ParseRefUpdates(r io.Reader) ([]repodb.RefUpdate, error) {
	var updates []repodb.RefUpdate
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			return nil, fmt.Errorf("malformed pre-receive line: %q", sc.Text())
		}
		updates = append(updates, repodb.RefUpdate{Old: fields[0], New: fields[1], Name: fields[2]})
	}
	return updates, sc.Err()
}

// uploadQuarantinePacks ships every pack+idx pair from the quarantine object
// dir to the blob store. .pack goes first so a pack is never discoverable
// via .idx before its data exists (same ordering rule as materialization,
// inverted).
func uploadQuarantinePacks(ctx context.Context, blobs blobstore.Store, repoID, quarantine string, staged *StagedPack) ([]repodb.Pack, error) {
	packDir := filepath.Join(quarantine, "pack")
	entries, err := os.ReadDir(packDir)
	if os.IsNotExist(err) {
		return nil, nil // ref-only push (e.g. deletes)
	}
	if err != nil {
		return nil, err
	}
	var out []repodb.Pack
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".pack")
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		// Staged fast path: the wire pack already sits in the store. A
		// trailer match proves the quarantine pack is byte-identical (git
		// kept the stream; --fix-thin would have recomputed the trailer),
		// so the upload collapses to a server-side copy + the small idx.
		exts := []string{".pack", ".idx"}
		if staged != nil && staged.Trailer != "" &&
			staged.Trailer == packTrailerHex(filepath.Join(packDir, name+".pack")) {
			if err := blobs.Copy(ctx, repoID, staged.Key, name+".pack"); err == nil {
				slog.Info("staged push: server-side copy", "repo", repoID, "pack", name)
				exts = []string{".idx"}
				blobs.Delete(ctx, repoID, staged.Key) // claimed; best-effort cleanup
				staged = nil
			} else {
				slog.Warn("staged push: copy failed, uploading", "repo", repoID, "err", err)
			}
		}
		// Pack and idx ship concurrently; both must land before the CAS.
		errs := make(chan error, len(exts))
		for _, ext := range exts {
			go func(ext string) {
				f, err := os.Open(filepath.Join(packDir, name+ext))
				if err != nil {
					errs <- err
					return
				}
				defer f.Close()
				errs <- blobs.Put(ctx, repoID, name+ext, f)
			}(ext)
		}
		for range exts {
			if err := <-errs; err != nil {
				return nil, err
			}
		}
		out = append(out, repodb.Pack{Name: name, SizeBytes: info.Size(), Source: "receive"})
	}
	return out, nil
}

// packTrailerHex reads a pack file's trailing checksum (last 20 bytes).
func packTrailerHex(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() < 20 {
		return ""
	}
	buf := make([]byte, 20)
	if _, err := f.ReadAt(buf, info.Size()-20); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}
