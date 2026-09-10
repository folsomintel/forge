package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
)

// Staged tee push: while git receive-pack is still reading a push, githttp
// tees the raw wire pack bytes into staged/<id>.pack in the blob store -
// the S3 upload overlaps the client transfer instead of following it. When
// the pre-receive hook fires, the quarantine pack's trailer checksum is
// compared to the wire pack's: a match (non-thin push, the common case for
// big first pushes) means the staged blob already IS the pack, so the
// upload in the ack path collapses to a server-side copy plus a small idx
// upload. Thin packs (--fix-thin appended bases, trailer differs) fall back
// to the normal upload. Unclaimed staged blobs are deleted after a timeout
// here and by the maintenance sweep as a backstop.

const (
	stagedPrefix  = "staged/"
	stagedTTL     = 15 * time.Minute
	stagedWaitMax = 2 * time.Minute
)

// StagedPack describes one teed wire pack, ready once done is closed.
type StagedPack struct {
	RepoID  string
	Key     string // blob key under the repo prefix ("staged/<id>.pack")
	Trailer string // hex of the pack's trailing checksum (last 20 bytes)
	Err     error  // upload failure -> callers fall back to normal upload
	done    chan struct{}
}

// Stager tracks in-flight staged uploads between githttp and the hook path.
type Stager struct {
	Blobs blobstore.Store

	mu sync.Mutex
	m  map[string]*StagedPack
}

// Begin starts an upload for one wire pack; write the pack bytes to the
// returned writer and Close it at EOF (Abort on a broken stream). The id
// travels to the hook via env.
func (st *Stager) Begin(repoID string) (string, *StageWriter) {
	b := make([]byte, 8)
	rand.Read(b)
	id := hex.EncodeToString(b)
	sp := &StagedPack{RepoID: repoID, Key: stagedPrefix + id + ".pack", done: make(chan struct{})}

	st.mu.Lock()
	if st.m == nil {
		st.m = map[string]*StagedPack{}
	}
	st.m[id] = sp
	st.mu.Unlock()

	pr, pw := io.Pipe()
	tw := &trailerWriter{w: pw}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), stagedTTL)
		defer cancel()
		err := st.Blobs.Put(ctx, repoID, sp.Key, pr)
		pr.CloseWithError(err)
		sp.Err = err
		sp.Trailer = tw.trailerHex()
		close(sp.done)
		// Unclaimed (client abort, standalone hook, thin fallback that never
		// waited): remove the entry and blob after the TTL; the maintenance
		// sweep is the backstop.
		time.AfterFunc(stagedTTL, func() { st.expire(id) })
	}()
	return id, &StageWriter{tw: tw, pw: pw}
}

// Claim waits for the staged upload to finish and hands it to the caller
// (removing it from the registry). Returns nil on unknown id, upload error,
// or timeout - all of which mean "use the normal upload path".
func (st *Stager) Claim(id string) *StagedPack {
	if id == "" || st == nil {
		return nil
	}
	st.mu.Lock()
	sp := st.m[id]
	st.mu.Unlock()
	if sp == nil {
		return nil
	}
	select {
	case <-sp.done:
	case <-time.After(stagedWaitMax):
		return nil
	}
	st.mu.Lock()
	delete(st.m, id)
	st.mu.Unlock()
	if sp.Err != nil {
		return nil
	}
	return sp
}

// Discard deletes a staged blob that ended up unused (thin push fallback).
func (st *Stager) Discard(sp *StagedPack) {
	if st == nil || sp == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.Blobs.Delete(ctx, sp.RepoID, sp.Key); err != nil {
		slog.Warn("discard staged pack", "repo", sp.RepoID, "key", sp.Key, "err", err)
	}
}

func (st *Stager) expire(id string) {
	st.mu.Lock()
	sp := st.m[id]
	delete(st.m, id)
	st.mu.Unlock()
	if sp != nil {
		st.Discard(sp)
	}
}

// trailerWriter tracks the last 20 bytes written (the pack checksum).
type trailerWriter struct {
	w    io.Writer
	tail [20]byte
	n    int64
}

func (t *trailerWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		b := p[:n]
		if len(b) >= 20 {
			copy(t.tail[:], b[len(b)-20:])
		} else {
			copy(t.tail[:], append(append([]byte{}, t.tail[len(b):]...), b...))
		}
		t.n += int64(n)
	}
	return n, err
}

func (t *trailerWriter) trailerHex() string {
	if t.n < 20 {
		return ""
	}
	return hex.EncodeToString(t.tail[:])
}

// StageWriter receives the teed pack bytes for one staged upload.
type StageWriter struct {
	tw *trailerWriter
	pw *io.PipeWriter
}

func (s *StageWriter) Write(p []byte) (int, error) { return s.tw.Write(p) }
func (s *StageWriter) Close() error                { return s.pw.Close() }

// Abort poisons the upload so the staged pack is never trailer-matched.
func (s *StageWriter) Abort(err error) { s.pw.CloseWithError(err) }
