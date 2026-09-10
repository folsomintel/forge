package repodb

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/folsomintel/forge/internal/blobstore"
)

// WAL makes the bucket the truth for refs. Every ref transaction first
// lands as refs/wal/<seq>.json via conditional PUT (If-None-Match: * - the
// PUT either creates sequence N or fails because another writer owns it:
// object-store CAS), then applies to SQLite, which is demoted to a local
// read index. Snapshots every snapEvery entries bound replay; boot or a
// lost race re-syncs the index from the bucket. With packs already
// immutable bucket blobs, a tenant's bucket alone can now resurrect the
// whole data plane (RecoverRepos).
//
// Concurrency model: one instance owns a tenant's refs in normal operation
// (per-repo local mutex serializes writers in-process); the conditional PUT
// exists to make split-brain - two machines during a botched migration, a
// standalone hook racing the server - safe rather than silently corrupting.
type WAL struct {
	*SQLite
	Blobs blobstore.Store

	// RegenIdx, when set, rebuilds a pack's idx from its bytes (wired to
	// the ingest parser at boot). Lets inline entries omit the idx.
	RegenIdx func(pack []byte) ([]byte, error)

	// OnCommit, when set, is invoked after every DURABLE ref transaction
	// (the conditional PUT succeeded) with the entry's seq and updates -
	// the feed for the SSE ref-event bus. Called from the group-commit
	// leader; implementations must not block (the hub is non-blocking).
	OnCommit func(repoID string, seq int64, ts int64, updates []RefUpdate)

	mu    sync.Mutex
	repos map[string]*walRepo

	// hookMu/keyMu serialize (index write + bucket mirror) so a stale
	// read-modify-write can never clobber a newer mirror.
	hookMu sync.Mutex
	keyMu  sync.Mutex

	// Group-commit telemetry (cumulative since boot): how fat batches get
	// and what the conditional PUT costs - the two numbers that decide
	// push throughput (txs/s = batch-size / PUT-RTT).
	statPuts       atomic.Int64
	statTxs        atomic.Int64
	statMaxBatch   atomic.Int64
	statPutMSTotal atomic.Int64
	statPutMSMax   atomic.Int64
	statCASRetries atomic.Int64
}

// WALStats is the cumulative group-commit picture since process start.
type WALStats struct {
	Puts       int64 `json:"puts"`
	Txs        int64 `json:"txs"`
	MaxBatch   int64 `json:"max_batch"`
	PutMSTotal int64 `json:"put_ms_total"`
	PutMSMax   int64 `json:"put_ms_max"`
	CASRetries int64 `json:"cas_retries"`
}

func (w *WAL) Stats() WALStats {
	return WALStats{
		Puts: w.statPuts.Load(), Txs: w.statTxs.Load(),
		MaxBatch: w.statMaxBatch.Load(), PutMSTotal: w.statPutMSTotal.Load(),
		PutMSMax: w.statPutMSMax.Load(), CASRetries: w.statCASRetries.Load(),
	}
}

func atomicMax(a *atomic.Int64, v int64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

type walRepo struct {
	mu     sync.Mutex // guards queue + leadership handoff
	queue  []*walTx
	lead   bool
	snapMu sync.Mutex // one snapshot writer at a time

	// seq and synced are owned by the current leader (single leader per
	// repo per process; cross-process races arbitrate via conditional PUT).
	seq    int64
	synced bool

	// flushedMem is this process's best-effort record of which WAL entries
	// have had their inline packs written as standalone blobs. It is an
	// optimization ONLY: pruning correctness comes from re-flushing (and
	// thus confirming) any entry not known-flushed here before deleting it,
	// so a missing/stale entry costs a redundant Put, never data loss.
	// Purely in-memory (no persisted watermark to lose, revert to an
	// ambiguous sentinel, or go stale across processes). Keyed by seq;
	// entries are dropped as they are pruned, so it stays bounded to the
	// unpruned tail.
	flushMu    sync.Mutex
	flushedMem map[int64]bool
}

// walTx is one caller's ref transaction awaiting (group) commit.
type walTx struct {
	updates []RefUpdate
	events  []Event
	pack    *InlinePack // optional: small pack riding inside the WAL entry
	err     error
	done    chan struct{}
}

// InlinePack is a small pack embedded in a WAL entry, so a push's refs AND
// data become durable in ONE conditional PUT (the group commit then
// amortizes that PUT across concurrent pushes). The standalone pack/idx
// blobs are written asynchronously afterwards; the entry itself is the
// durable copy until the flush watermark passes it.
type InlinePack struct {
	Name string `json:"name"`
	Data []byte `json:"data"` // pack bytes (json base64)
	// Idx is omitted from entries when RegenIdx is wired: it is derivable
	// from the pack, and every byte matters - Tigris commits sub-1KB
	// objects ~50ms faster than larger ones (measured), so a lean entry
	// is a faster push ack.
	Idx []byte `json:"idx,omitempty"`
}

const (
	// InlinePackMax caps one pack's inline size; larger packs take the
	// classic put-blobs-then-CAS path.
	InlinePackMax = 256 << 10
	// entryInlineMax caps the total inline payload of one entry; a batch
	// that would exceed it splits across entries.
	entryInlineMax = 2 << 20
)

const (
	walPrefix    = "refs/wal/"
	snapshotBlob = "refs/snapshot.json"
	webhooksBlob = "config/webhooks.json"
	snapEvery    = 64

	// Instance-scoped truth lives under _forge/ - not a valid repo id, so
	// it can never collide with a repo prefix.
	instancePrefix = "_forge"
	keysBlob       = "keys.json"
)

type walEntry struct {
	Seq     int64        `json:"seq"`
	Updates []RefUpdate  `json:"updates"`
	Events  []Event      `json:"events,omitempty"`
	Packs   []InlinePack `json:"packs,omitempty"`
	TS      int64        `json:"ts"`
}

type walSnapshot struct {
	Seq           int64  `json:"seq"`
	DefaultBranch string `json:"default_branch"`
	Public        bool   `json:"public,omitempty"`
	Refs          []Ref  `json:"refs"`
	TS            int64  `json:"ts"`
}

func NewWAL(index *SQLite, blobs blobstore.Store) *WAL {
	return &WAL{SQLite: index, Blobs: blobs, repos: map[string]*walRepo{}}
}

func (w *WAL) repo(repoID string) *walRepo {
	w.mu.Lock()
	defer w.mu.Unlock()
	r, ok := w.repos[repoID]
	if !ok {
		r = &walRepo{}
		w.repos[repoID] = r
	}
	return r
}

func walKey(seq int64) string { return fmt.Sprintf("%s%016d.json", walPrefix, seq) }

// UpdateRefs CAS against the index, conditional-PUT the WAL entry (the
// real arbiter), apply to the index. Group commit: concurrent callers on
// one repo queue behind a leader that lands the whole batch as ONE
// conditional PUT - the entry format is unchanged (just more updates per
// entry), so replay and recovery are untouched. Same-repo ref-transaction
// throughput becomes batch-size/PUT-RTT instead of 1/PUT-RTT.
func (w *WAL) UpdateRefs(ctx context.Context, repoID string, updates []RefUpdate, events []Event) error {
	return w.updateRefs(ctx, repoID, updates, events, nil)
}

// UpdateRefsWithPack is UpdateRefs with a small pack riding INSIDE the WAL
// entry: the push's refs and data become durable in one conditional PUT,
// and the group commit amortizes that PUT across concurrent pushes. The
// standalone pack/idx blobs are flushed asynchronously; until the flush
// watermark passes the entry, the entry itself is the durable copy (and
// prune respects that). Pack must be <= inlinePackMax.
func (w *WAL) UpdateRefsWithPack(ctx context.Context, repoID string, updates []RefUpdate, events []Event, pack *InlinePack) error {
	if pack != nil && len(pack.Data) > InlinePackMax {
		return fmt.Errorf("inline pack %d bytes exceeds cap %d", len(pack.Data), InlinePackMax)
	}
	return w.updateRefs(ctx, repoID, updates, events, pack)
}

func (w *WAL) updateRefs(ctx context.Context, repoID string, updates []RefUpdate, events []Event, pack *InlinePack) error {
	if events != nil && len(events) != len(updates) {
		return fmt.Errorf("events must be nil or match updates (%d vs %d)", len(events), len(updates))
	}
	if _, err := w.SQLite.GetRepo(ctx, repoID); err != nil {
		return err
	}
	r := w.repo(repoID)
	tx := &walTx{updates: updates, events: events, pack: pack, done: make(chan struct{})}

	r.mu.Lock()
	r.queue = append(r.queue, tx)
	if r.lead {
		r.mu.Unlock()
		<-tx.done // a leader is already draining; it will take ours
		return tx.err
	}
	r.lead = true
	r.mu.Unlock()

	// If a batch panics, clear leadership and fail anything still queued -
	// otherwise r.lead stays true and every future push on this repo parks
	// forever. Re-panic to keep the failure loud (net/http recovers per
	// request); the repo is no longer wedged.
	defer func() {
		if p := recover(); p != nil {
			r.mu.Lock()
			r.lead = false
			orphaned := r.queue
			r.queue = nil
			r.mu.Unlock()
			failTxs(orphaned, fmt.Errorf("wal leader aborted: %v", p))
			panic(p)
		}
	}()

	for {
		r.mu.Lock()
		batch := r.queue
		r.queue = nil
		if len(batch) == 0 {
			r.lead = false
			r.mu.Unlock()
			break
		}
		r.mu.Unlock()
		w.commitBatch(repoID, r, batch)
	}
	<-tx.done // committed in one of the batches we led
	return tx.err
}

// commitBatch validates each transaction against the evolving state (in
// arrival order - later transactions see earlier ones' effects), lands the
// accepted set as one WAL entry, and applies it to the index. Individual
// CAS losers fail individually; they never poison the batch. Uses a
// detached context: one impatient caller must not abort a batch that
// carries other callers' durability.
func (w *WAL) commitBatch(repoID string, r *walRepo, batch []*walTx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	candidates := batch
	defer func() {
		if p := recover(); p != nil {
			failTxs(candidates, fmt.Errorf("wal commit panic: %v", p))
			panic(p)
		}
	}()

	if !r.synced {
		if err := w.syncLocked(ctx, repoID, r, false); err != nil {
			failTxs(candidates, fmt.Errorf("wal sync: %w", err))
			return
		}
	}

	for attempt := 0; ; attempt++ {
		refs, err := w.SQLite.ListRefs(ctx, repoID)
		if err != nil {
			failTxs(candidates, err)
			return
		}
		cur := make(map[string]string, len(refs))
		for _, ref := range refs {
			cur[ref.Name] = ref.Target
		}
		var accepted, deferred []*walTx
		var updates []RefUpdate
		var events []Event
		var packs []InlinePack
		inlineBytes := 0
		for i, tx := range candidates {
			// Entry size cap: once this entry is full of inline packs, the
			// rest of the batch waits for the next entry (still this leader).
			if tx.pack != nil && inlineBytes > 0 && inlineBytes+len(tx.pack.Data) > entryInlineMax {
				deferred = candidates[i:]
				break
			}
			if err := casCheckMap(cur, tx.updates); err != nil {
				tx.err = err
				close(tx.done)
				continue
			}
			for _, u := range tx.updates { // later txs in the batch see these
				if u.New == ZeroOID {
					delete(cur, u.Name)
				} else {
					cur[u.Name] = u.New
				}
			}
			accepted = append(accepted, tx)
			updates = append(updates, tx.updates...)
			events = append(events, tx.events...)
			if tx.pack != nil {
				packs = append(packs, *tx.pack)
				inlineBytes += len(tx.pack.Data)
			}
		}
		if len(deferred) > 0 {
			// Back on the queue, in order, ahead of new arrivals.
			r.mu.Lock()
			r.queue = append(append([]*walTx{}, deferred...), r.queue...)
			r.mu.Unlock()
		}
		candidates = accepted
		if len(accepted) == 0 {
			return // every tx CAS-failed and was signaled
		}

		entry := walEntry{Seq: r.seq + 1, Updates: updates, Events: events, Packs: packs, TS: time.Now().Unix()}
		data, err := json.Marshal(entry)
		if err != nil {
			failTxs(accepted, err)
			return
		}
		data = encodeEntry(data)
		putStart := time.Now()
		err = w.Blobs.PutIfAbsent(ctx, repoID, walKey(entry.Seq), data)
		putMS := time.Since(putStart).Milliseconds()
		if errors.Is(err, blobstore.ErrExists) {
			w.statCASRetries.Add(1)
			if attempt >= 3 {
				failTxs(accepted, fmt.Errorf("%w: wal sequence contention", ErrCASFailed))
				return
			}
			if err := w.syncLocked(ctx, repoID, r, false); err != nil {
				failTxs(accepted, fmt.Errorf("wal re-sync: %w", err))
				return
			}
			continue // state moved under us; re-validate the survivors
		}
		if err != nil {
			failTxs(accepted, fmt.Errorf("wal append: %w", err))
			return
		}
		w.statPuts.Add(1)
		w.statTxs.Add(int64(len(accepted)))
		w.statPutMSTotal.Add(putMS)
		atomicMax(&w.statPutMSMax, putMS)
		atomicMax(&w.statMaxBatch, int64(len(accepted)))
		// The conditional PUT is the source of truth and it succeeded, so the
		// push IS durable - ACK it. If the local index apply fails, do NOT
		// tell the client "rejected" (a phantom rejection: the ref is live in
		// the bucket). Ack, advance seq, and mark the index unsynced so it
		// replays this entry from the bucket on the next access.
		if err := w.SQLite.ApplyWAL(ctx, repoID, updates, events, packRows(packs), entry.Seq); err != nil {
			slog.Error("wal apply after durable PUT; index will re-sync",
				"repo", repoID, "seq", entry.Seq, "err", err)
			r.seq = entry.Seq
			r.synced = false
			for _, tx := range accepted {
				close(tx.done) // durable = success
			}
			go w.flushEntry(repoID, r, entry)
			if w.OnCommit != nil {
				w.OnCommit(repoID, entry.Seq, entry.TS, updates)
			}
			return
		}
		r.seq = entry.Seq
		if entry.Seq%snapEvery == 0 {
			go w.snapshot(repoID, r)
		}
		for _, tx := range accepted {
			close(tx.done)
		}
		go w.flushEntry(repoID, r, entry)
		if w.OnCommit != nil {
			w.OnCommit(repoID, entry.Seq, entry.TS, updates)
		}
		return
	}
}

// ReplayRefEvents reads the WAL tail above afterSeq and returns its ref
// updates in commit order - the SSE reconnect/replay path. complete=false
// means the tail below the current state was already pruned (snapshot),
// so the subscriber must reset (re-list refs) instead of trusting a gap.
func (w *WAL) ReplayRefEvents(ctx context.Context, repoID string, afterSeq int64) (updates []RefUpdate, seqs []int64, ts []int64, complete bool, err error) {
	snap, entrySeqs, err := w.scan(ctx, repoID)
	if err != nil {
		return nil, nil, nil, false, err
	}
	// Anchor on the BUCKET's max seq, not the local index: an entry can be
	// durable in the bucket while SQLite still lags (ApplyWAL failed after a
	// durable PUT; a replica behind the primary). Anchoring on WALSeq would
	// tell a subscriber "nothing missed" for an entry it in fact missed.
	cur := int64(0)
	if snap != nil {
		cur = snap.Seq
	}
	for _, s := range entrySeqs {
		if s > cur {
			cur = s
		}
	}
	if afterSeq >= cur {
		return nil, nil, nil, true, nil // nothing missed
	}
	// Every seq in (afterSeq, cur] must still exist as an entry, or the
	// history the caller missed is gone.
	have := map[int64]bool{}
	for _, s := range entrySeqs {
		have[s] = true
	}
	for s := afterSeq + 1; s <= cur; s++ {
		if !have[s] {
			return nil, nil, nil, false, nil
		}
	}
	for s := afterSeq + 1; s <= cur; s++ {
		entry, err := w.readEntry(ctx, repoID, s)
		if err != nil {
			return nil, nil, nil, false, err
		}
		for _, u := range entry.Updates {
			updates = append(updates, u)
			seqs = append(seqs, s)
			ts = append(ts, entry.TS)
		}
	}
	return updates, seqs, ts, true, nil
}

// packRows converts inline packs to index rows.
func packRows(packs []InlinePack) []Pack {
	if len(packs) == 0 {
		return nil
	}
	rows := make([]Pack, len(packs))
	for i, p := range packs {
		rows[i] = Pack{Name: p.Name, SizeBytes: int64(len(p.Data)), Source: "receive"}
	}
	return rows
}

// markFlushed records (best-effort) that entry seq's inline packs are
// durable as standalone blobs, so prune can delete it without re-flushing.
func (r *walRepo) markFlushed(seq int64) {
	r.flushMu.Lock()
	if r.flushedMem == nil {
		r.flushedMem = map[int64]bool{}
	}
	r.flushedMem[seq] = true
	r.flushMu.Unlock()
}

func (r *walRepo) isFlushed(seq int64) bool {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	return r.flushedMem[seq]
}

func (r *walRepo) forgetFlushed(seq int64) {
	r.flushMu.Lock()
	delete(r.flushedMem, seq)
	r.flushMu.Unlock()
}

// flushEntry writes an entry's inline packs as standalone blobs after
// commit - a best-effort warm so reads don't fall through to
// RecoverInlinePack. A pack-free entry is trivially flushed. Marking the
// seq lets prune skip re-flushing it; on failure the seq stays unmarked and
// prune re-flushes (and thus confirms) it before deleting.
func (w *WAL) flushEntry(repoID string, r *walRepo, entry walEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, p := range entry.Packs {
		if err := w.flushPack(ctx, repoID, p); err != nil {
			slog.Warn("inline pack flush; prune will retry", "repo", repoID, "pack", p.Name, "err", err)
			return
		}
	}
	r.markFlushed(entry.Seq)
}

func (w *WAL) flushPack(ctx context.Context, repoID string, p InlinePack) error {
	idx := p.Idx
	if len(idx) == 0 {
		if w.RegenIdx == nil {
			return fmt.Errorf("inline pack %s has no idx and no RegenIdx", p.Name)
		}
		var err error
		if idx, err = w.RegenIdx(p.Data); err != nil {
			return fmt.Errorf("regen idx for %s: %w", p.Name, err)
		}
	}
	if err := w.Blobs.Put(ctx, repoID, p.Name+".pack", bytes.NewReader(p.Data)); err != nil {
		return err
	}
	return w.Blobs.Put(ctx, repoID, p.Name+".idx", bytes.NewReader(idx))
}

// RecoverInlinePack closes the ack-to-flush race: a reader that wants a
// pack whose entry is durable but whose standalone blob is still pending
// (or whose flusher died) finds it in the WAL tail and flushes it now.
// Returns true if the pack was found and its blobs written.
func (w *WAL) RecoverInlinePack(ctx context.Context, repoID, packName string) bool {
	_, seqs, err := w.scan(ctx, repoID)
	if err != nil {
		return false
	}
	// Newest first: the raced pack is almost always the most recent entry.
	for i := len(seqs) - 1; i >= 0; i-- {
		entry, err := w.readEntry(ctx, repoID, seqs[i])
		if err != nil {
			continue
		}
		for _, p := range entry.Packs {
			if p.Name == packName {
				return w.flushPack(ctx, repoID, p) == nil
			}
		}
	}
	return false
}

func failTxs(txs []*walTx, err error) {
	for _, tx := range txs {
		if tx.err == nil {
			select {
			case <-tx.done: // already signaled
			default:
				tx.err = err
				close(tx.done)
			}
		}
	}
}

func casCheckMap(cur map[string]string, updates []RefUpdate) error {
	for _, u := range updates {
		got, exists := cur[u.Name]
		if u.Old == ZeroOID {
			if exists {
				return fmt.Errorf("%w: %s", ErrCASFailed, u.Name)
			}
		} else if !exists || got != u.Old {
			return fmt.Errorf("%w: %s", ErrCASFailed, u.Name)
		}
	}
	return nil
}

// RefreshIndex brings a follower replica's local index up to the bucket
// WAL before it serves a ref-dependent read - the freshness-on-read gate
// that makes multi-replica reads correct (a push committed on another
// machine is durable in the WAL but not yet in this machine's index).
//
// Cheap in steady state: it only pays for a full sync when the next WAL
// sequence actually exists (a writer elsewhere advanced the log). ErrNotFound
// on that one small object means we are current - the common case, one
// bucket round trip, no index work. It is only wired in on replicas; the
// primary/single-machine writer is current by construction and never calls
// it. A follower reads on every request, so it can't silently fall a whole
// snapshot window behind - a cold/far-behind replica is rebuilt by a full
// materialize instead.
func (w *WAL) RefreshIndex(ctx context.Context, repoID string) (bool, error) {
	r := w.repo(repoID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.synced {
		if err := w.syncLocked(ctx, repoID, r, false); err != nil {
			return false, err
		}
		return true, nil
	}
	rc, err := w.Blobs.Get(ctx, repoID, walKey(r.seq+1))
	if errors.Is(err, blobstore.ErrNotFound) {
		return false, nil // current
	}
	if err != nil {
		return false, err
	}
	rc.Close()
	if err := w.syncLocked(ctx, repoID, r, false); err != nil {
		return false, err
	}
	return true, nil
}

// syncLocked reconciles the index with the bucket: reads snapshot + WAL
// tail, applies anything the index has not seen. force is the disaster
// path (fresh index): the snapshot applies even at equal seq (a repo whose
// whole life is its seq-0 snapshot - forks, backfills - has nothing to
// replay). Caller holds r.mu.
func (w *WAL) syncLocked(ctx context.Context, repoID string, r *walRepo, force bool) error {
	localSeq, err := w.SQLite.WALSeq(ctx, repoID)
	if err != nil {
		return err
	}
	snap, entrySeqs, err := w.scan(ctx, repoID)
	if err != nil {
		return err
	}
	if snap != nil && (snap.Seq > localSeq || (force && snap.Seq >= localSeq)) {
		// The tail below the snapshot may be pruned: adopt the snapshot
		// wholesale, then replay entries above it.
		if err := w.SQLite.ApplySnapshot(ctx, repoID, snap.Refs, snap.Seq); err != nil {
			return err
		}
		localSeq = snap.Seq
	}
	maxSeq := localSeq
	for _, seq := range entrySeqs {
		if seq <= localSeq {
			continue
		}
		entry, err := w.readEntry(ctx, repoID, seq)
		if err != nil {
			return err
		}
		if err := w.SQLite.ApplyWAL(ctx, repoID, entry.Updates, entry.Events, packRows(entry.Packs), seq); err != nil {
			return err
		}
		// Replayed inline packs: make sure the standalone blobs exist so
		// this index's pack rows are readable (recovery on a fresh volume,
		// or a replica ahead of the primary's async flush). Idempotent.
		for _, p := range entry.Packs {
			if err := w.flushPack(ctx, repoID, p); err != nil {
				slog.Warn("replay inline pack flush", "repo", repoID, "pack", p.Name, "err", err)
			}
		}
		maxSeq = seq
	}
	r.seq = maxSeq
	r.synced = true
	return nil
}

// scan lists the repo's WAL blobs: snapshot (nil if none) + sorted entry
// seqs. Both live under "refs/", so we scan just that subtree - no need to
// enumerate the repo's packs on every sync.
func (w *WAL) scan(ctx context.Context, repoID string) (*walSnapshot, []int64, error) {
	blobs, err := w.Blobs.List(ctx, repoID, "refs/")
	if err != nil {
		return nil, nil, err
	}
	var seqs []int64
	haveSnap := false
	for _, b := range blobs {
		if b.Name == snapshotBlob {
			haveSnap = true
			continue
		}
		if rest, ok := strings.CutPrefix(b.Name, walPrefix); ok {
			if n, err := strconv.ParseInt(strings.TrimSuffix(rest, ".json"), 10, 64); err == nil {
				seqs = append(seqs, n)
			}
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	var snap *walSnapshot
	if haveSnap {
		if snap, err = w.readSnapshot(ctx, repoID); err != nil {
			return nil, nil, err
		}
	}
	return snap, seqs, nil
}

func (w *WAL) readEntry(ctx context.Context, repoID string, seq int64) (*walEntry, error) {
	rc, err := w.Blobs.Get(ctx, repoID, walKey(seq))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	data, err = decodeEntry(data)
	if err != nil {
		return nil, fmt.Errorf("corrupt wal entry %d: %w", seq, err)
	}
	var e walEntry
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("corrupt wal entry %d: %w", seq, err)
	}
	return &e, nil
}

// encodeEntry gzips entry JSON when that wins. Tigris commits sub-1KB
// objects ~50ms faster than larger ones (measured on the tenant boxes),
// so shaving an entry under that knee is a directly faster push ack -
// and bigger batched entries still save bandwidth. Plain JSON stays
// valid on the read side (old entries, foreign writers).
func encodeEntry(data []byte) []byte {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	zw.Write(data)
	zw.Close()
	if buf.Len() >= len(data) {
		return data
	}
	return buf.Bytes()
}

// decodeEntry inverts encodeEntry (gzip magic sniff; plain JSON passes
// through untouched).
func decodeEntry(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func (w *WAL) readSnapshot(ctx context.Context, repoID string) (*walSnapshot, error) {
	rc, err := w.Blobs.Get(ctx, repoID, snapshotBlob)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var s walSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("corrupt wal snapshot: %w", err)
	}
	return &s, nil
}

// snapshot publishes the current full ref state and prunes entries at or
// below it (bounding replay). Serialized per repo, and the claimed Seq is
// read from wal_state BEFORE the refs: the content is then guaranteed at
// least as new as the claim, and pruning stays <= the claim - two racing
// snapshots can therefore never prune entries a surviving older claim
// still needs. (Replaying entries above the claim onto newer-than-claimed
// content converges: updates are absolute, applied in order to the tail.)
func (w *WAL) snapshot(repoID string, r *walRepo) {
	if !r.snapMu.TryLock() {
		return // a snapshot is already in flight; the next threshold re-fires
	}
	defer r.snapMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	seq, err := w.SQLite.WALSeq(ctx, repoID)
	if err != nil || seq == 0 {
		return
	}
	refs, err := w.SQLite.ListRefs(ctx, repoID)
	if err != nil {
		slog.Warn("wal snapshot: list refs", "repo", repoID, "err", err)
		return
	}
	branch, public := "main", false
	if repo, err := w.SQLite.GetRepo(ctx, repoID); err == nil {
		branch, public = repo.DefaultBranch, repo.Public
	}
	data, _ := json.Marshal(walSnapshot{Seq: seq, DefaultBranch: branch, Public: public, Refs: refs, TS: time.Now().Unix()})
	if err := w.writeSnapshotBlob(ctx, repoID, data); err != nil {
		slog.Warn("wal snapshot: write", "repo", repoID, "err", err)
		return
	}
	// Prune replayed history - but NEVER an entry whose inline packs are not
	// yet standalone blobs: until then the entry IS the only durable copy of
	// that data. Correctness comes from actually confirming each entry
	// (re-flushing when this process didn't record it as flushed), not from a
	// persisted watermark - so a lost/ambiguous watermark, a crash, or a
	// foreign writer can never authorize pruning an unflushed entry.
	blobs, err := w.Blobs.List(ctx, repoID, walPrefix)
	if err != nil {
		return
	}
	for _, b := range blobs {
		rest, ok := strings.CutPrefix(b.Name, walPrefix)
		if !ok {
			continue
		}
		n, perr := strconv.ParseInt(strings.TrimSuffix(rest, ".json"), 10, 64)
		if perr != nil || n > seq {
			continue
		}
		if !w.confirmFlushed(ctx, repoID, r, n) {
			continue // couldn't confirm its packs are durable: keep the entry
		}
		w.Blobs.Delete(ctx, repoID, b.Name)
		r.forgetFlushed(n)
	}
}

// confirmFlushed guarantees entry n's inline packs (if any) are durable as
// standalone blobs before it may be pruned. Fast path: this process already
// flushed it (flushedMem). Otherwise read the entry and re-flush its packs
// (idempotent Put); a pack-free entry needs nothing. Re-Put beats an
// existence probe: a missing-object GET on Tigris costs ~200ms, an
// idempotent small Put ~30ms. Returns false (keep the entry, retry next
// snapshot) only when a required flush fails.
func (w *WAL) confirmFlushed(ctx context.Context, repoID string, r *walRepo, n int64) bool {
	if r.isFlushed(n) {
		return true
	}
	entry, err := w.readEntry(ctx, repoID, n)
	if err != nil {
		// The entry blob is already gone (a prior prune / foreign writer):
		// nothing of it remains to protect, so it is safe to "prune" (no-op).
		return errors.Is(err, blobstore.ErrNotFound)
	}
	for _, p := range entry.Packs {
		if err := w.flushPack(ctx, repoID, p); err != nil {
			slog.Warn("prune: confirm flush", "repo", repoID, "seq", n, "pack", p.Name, "err", err)
			return false
		}
	}
	r.markFlushed(n)
	return true
}

func (w *WAL) writeSnapshotBlob(ctx context.Context, repoID string, data []byte) error {
	return w.Blobs.Put(ctx, repoID, snapshotBlob, strings.NewReader(string(data)))
}

// CreateRepo seeds the bucket-side snapshot so recovery can rediscover the
// repo from the bucket alone. It claims the bucket FIRST with a conditional
// PUT: if a snapshot already exists (a real repo whose index rows are just
// missing because recovery failed), the create is refused instead of
// overwriting truth with an empty seq-0 snapshot.
func (w *WAL) CreateRepo(ctx context.Context, id, defaultBranch string) error {
	data, _ := json.Marshal(walSnapshot{Seq: 0, DefaultBranch: defaultBranch, TS: time.Now().Unix()})
	err := w.Blobs.PutIfAbsent(ctx, id, snapshotBlob, data)
	if errors.Is(err, blobstore.ErrExists) {
		return ErrExists // repo already exists in the bucket - never clobber it
	}
	if err != nil {
		return fmt.Errorf("seed snapshot: %w", err)
	}
	if err := w.SQLite.CreateRepo(ctx, id, defaultBranch); err != nil {
		return err // bucket snapshot stays; recovery will reconcile the index
	}
	return nil
}

// ForkRepo copies refs in the index, then mirrors them to the fork's
// bucket prefix as its seq-0 snapshot.
func (w *WAL) ForkRepo(ctx context.Context, srcID, dstID string) error {
	if err := w.SQLite.ForkRepo(ctx, srcID, dstID); err != nil {
		return err
	}
	refs, err := w.SQLite.ListRefs(ctx, dstID)
	if err != nil {
		return err
	}
	branch := "main"
	if repo, err := w.SQLite.GetRepo(ctx, dstID); err == nil {
		branch = repo.DefaultBranch
	}
	data, _ := json.Marshal(walSnapshot{Seq: 0, DefaultBranch: branch, Refs: refs, TS: time.Now().Unix()})
	if err := w.writeSnapshotBlob(ctx, dstID, data); err != nil {
		slog.Warn("wal: fork snapshot", "repo", dstID, "err", err)
	}
	return nil
}

// SetRepoPublic flips visibility and refreshes the bucket snapshot so the
// flag survives resurrection.
func (w *WAL) SetRepoPublic(ctx context.Context, id string, public bool) error {
	if err := w.SQLite.SetRepoPublic(ctx, id, public); err != nil {
		return err
	}
	seq, _ := w.SQLite.WALSeq(ctx, id)
	refs, err := w.SQLite.ListRefs(ctx, id)
	if err != nil {
		return nil
	}
	branch := "main"
	if repo, err := w.SQLite.GetRepo(ctx, id); err == nil {
		branch = repo.DefaultBranch
	}
	data, _ := json.Marshal(walSnapshot{Seq: seq, DefaultBranch: branch, Public: public, Refs: refs, TS: time.Now().Unix()})
	if err := w.writeSnapshotBlob(ctx, id, data); err != nil {
		slog.Warn("wal: visibility snapshot", "repo", id, "err", err)
	}
	return nil
}

// DeleteRepo purges the whole bucket prefix (packs, idx, LFS, snapshot,
// WAL) so recovery cannot resurrect a deleted repo and its storage stops
// being billed. The SQLite delete runs first, dropping this repo's own pack
// rows, so any pack blob still referenced afterwards belongs to a live fork
// (zero-copy fork: blob_repo points here) and is kept.
func (w *WAL) DeleteRepo(ctx context.Context, id string) error {
	if err := w.SQLite.DeleteRepo(ctx, id); err != nil {
		return err
	}
	w.mu.Lock()
	delete(w.repos, id)
	w.mu.Unlock()
	// A List error must surface: a silent nil here is how a deleted repo's
	// packs get left paying storage forever (the broken-tenant saga in miniature).
	blobs, err := w.Blobs.List(ctx, id, "")
	if err != nil {
		return fmt.Errorf("delete repo %s: list blobs: %w", id, err)
	}
	var kept, failed int
	for _, b := range blobs {
		if pack, ok := packBlobName(b.Name); ok {
			ref, err := w.SQLite.BlobReferenced(ctx, id, pack)
			if err != nil {
				return fmt.Errorf("delete repo %s: fork check %s: %w", id, b.Name, err)
			}
			if ref {
				kept++
				continue // a fork still reads this pack from our prefix
			}
		}
		if err := w.Blobs.Delete(ctx, id, b.Name); err != nil {
			slog.Warn("delete repo: blob", "repo", id, "blob", b.Name, "err", err)
			failed++
		}
	}
	if kept > 0 {
		slog.Info("delete repo: kept fork-referenced packs", "repo", id, "kept", kept)
	}
	if failed > 0 {
		return fmt.Errorf("delete repo %s: %d blob(s) failed to delete", id, failed)
	}
	return nil
}

// packBlobName maps a pack blob filename (pack-<hash>.pack / .idx) to the
// pack name recorded in the packs table (no extension); ok is false for any
// non-pack blob (snapshot, WAL entries, LFS, webhooks config).
func packBlobName(blob string) (string, bool) {
	if n, ok := strings.CutSuffix(blob, ".pack"); ok {
		return n, true
	}
	if n, ok := strings.CutSuffix(blob, ".idx"); ok {
		return n, true
	}
	return "", false
}

// RecoverRepos rebuilds the index from the bucket alone - the disaster
// path (volume gone, fresh SQLite). Only safe against an EMPTY index; the
// caller checks. Refs replay from snapshot+WAL; pack rows are index state
// too, so recovery re-records every pack blob found under the prefix
// (source "recover") - anything unreachable gets swept by maintenance.
func (w *WAL) RecoverRepos(ctx context.Context) (int, error) {
	if err := w.restoreKeys(ctx); err != nil {
		return 0, err
	}
	prefixes, err := w.Blobs.Prefixes(ctx)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, repoID := range prefixes {
		if repoID == instancePrefix {
			continue
		}
		snap, entrySeqs, err := w.scan(ctx, repoID)
		if err != nil {
			return recovered, fmt.Errorf("scan %s: %w", repoID, err)
		}
		if snap == nil && len(entrySeqs) == 0 {
			continue // prefix has no ref WAL - not a repo (or pre-WAL data)
		}
		branch := "main"
		if snap != nil && snap.DefaultBranch != "" {
			branch = snap.DefaultBranch
		}
		if err := w.SQLite.CreateRepo(ctx, repoID, branch); err != nil && !errors.Is(err, ErrExists) {
			return recovered, err
		}
		if snap != nil && snap.Public {
			if err := w.SQLite.SetRepoPublic(ctx, repoID, true); err != nil {
				return recovered, err
			}
		}
		r := w.repo(repoID)
		r.mu.Lock()
		err = w.syncLocked(ctx, repoID, r, true)
		r.mu.Unlock()
		if err != nil {
			return recovered, fmt.Errorf("replay %s: %w", repoID, err)
		}
		// Pack list: every pack blob in the prefix is a live pack candidate
		// (orphans get swept by maintenance later). LFS index rows rebuild
		// from the lfs/ blobs; webhook configs from their bucket mirror.
		blobs, err := w.Blobs.List(ctx, repoID, "")
		if err != nil {
			return recovered, err
		}
		var packs []Pack
		for _, b := range blobs {
			switch {
			case strings.HasPrefix(b.Name, "pack-") && strings.HasSuffix(b.Name, ".pack"):
				packs = append(packs, Pack{
					Name: strings.TrimSuffix(b.Name, ".pack"), SizeBytes: b.Size, Source: "recover",
				})
			case strings.HasPrefix(b.Name, "lfs/"):
				if err := w.SQLite.AddLFSObject(ctx, repoID, LFSObject{
					OID: strings.TrimPrefix(b.Name, "lfs/"), Size: b.Size,
				}); err != nil {
					return recovered, err
				}
			}
		}
		if len(packs) > 0 {
			if err := w.SQLite.AddPacks(ctx, repoID, packs); err != nil {
				return recovered, err
			}
		}
		if err := w.restoreWebhooks(ctx, repoID); err != nil {
			return recovered, err
		}
		recovered++
		slog.Info("recovered repo from bucket", "repo", repoID, "packs", len(packs))
	}
	return recovered, nil
}

// --- webhook configs: mirrored to the bucket (they are truth, not cache) ---

// walWebhook is the bucket-side webhook shape; unlike the API type it
// carries the secret (the tenant bucket is the same trust domain as the
// repo data it signs for).
type walWebhook struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Secret    string   `json:"secret"`
	Events    []string `json:"events"`
	Active    bool     `json:"active"`
	CreatedAt int64    `json:"created_at"`
}

func (w *WAL) persistWebhooks(ctx context.Context, repoID string) error {
	hooks, err := w.SQLite.ListWebhooks(ctx, repoID)
	if err != nil {
		return err
	}
	out := make([]walWebhook, 0, len(hooks))
	for _, h := range hooks {
		out = append(out, walWebhook{ID: h.ID, URL: h.URL, Secret: h.Secret,
			Events: h.Events, Active: h.Active, CreatedAt: h.CreatedAt.Unix()})
	}
	data, _ := json.Marshal(out)
	return w.Blobs.Put(ctx, repoID, webhooksBlob, strings.NewReader(string(data)))
}

func (w *WAL) restoreWebhooks(ctx context.Context, repoID string) error {
	rc, err := w.Blobs.Get(ctx, repoID, webhooksBlob)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var hooks []walWebhook
	if err := json.Unmarshal(data, &hooks); err != nil {
		return fmt.Errorf("corrupt webhooks mirror: %w", err)
	}
	for _, h := range hooks {
		hook := &Webhook{ID: h.ID, RepoID: repoID, URL: h.URL, Secret: h.Secret,
			Events: h.Events, Active: h.Active, CreatedAt: time.Unix(h.CreatedAt, 0).UTC()}
		if err := w.SQLite.RestoreWebhook(ctx, hook); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) CreateWebhook(ctx context.Context, hook *Webhook) error {
	w.hookMu.Lock()
	defer w.hookMu.Unlock()
	if err := w.SQLite.CreateWebhook(ctx, hook); err != nil {
		return err
	}
	return w.persistWebhooks(ctx, hook.RepoID)
}

func (w *WAL) UpdateWebhook(ctx context.Context, hook *Webhook) error {
	w.hookMu.Lock()
	defer w.hookMu.Unlock()
	if err := w.SQLite.UpdateWebhook(ctx, hook); err != nil {
		return err
	}
	return w.persistWebhooks(ctx, hook.RepoID)
}

func (w *WAL) DeleteWebhook(ctx context.Context, repoID, id string) error {
	w.hookMu.Lock()
	defer w.hookMu.Unlock()
	if err := w.SQLite.DeleteWebhook(ctx, repoID, id); err != nil {
		return err
	}
	return w.persistWebhooks(ctx, repoID)
}

// --- client keys: mirrored to the bucket (public keys only) ---

type walKeyRec struct {
	ID     int64    `json:"id"`
	Name   string   `json:"name"`
	PEM    string   `json:"pem"`
	Scopes []string `json:"scopes,omitempty"`
}

func (w *WAL) persistKeys(ctx context.Context) error {
	keys, err := w.SQLite.ListKeys(ctx)
	if err != nil {
		return err
	}
	out := make([]walKeyRec, 0, len(keys))
	for _, k := range keys {
		out = append(out, walKeyRec{ID: k.ID, Name: k.Name, PEM: k.PublicKeyPEM, Scopes: k.Scopes})
	}
	data, _ := json.Marshal(out)
	return w.Blobs.Put(ctx, instancePrefix, keysBlob, strings.NewReader(string(data)))
}

func (w *WAL) restoreKeys(ctx context.Context) error {
	rc, err := w.Blobs.Get(ctx, instancePrefix, keysBlob)
	if errors.Is(err, blobstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	var keys []walKeyRec
	if err := json.Unmarshal(data, &keys); err != nil {
		return fmt.Errorf("corrupt keys mirror: %w", err)
	}
	for _, k := range keys {
		if err := w.SQLite.RestoreKey(ctx, Key{ID: k.ID, Name: k.Name, PublicKeyPEM: k.PEM, Scopes: k.Scopes}); err != nil {
			return err
		}
	}
	if len(keys) > 0 {
		slog.Info("recovered client keys from bucket", "count", len(keys))
	}
	return nil
}

// AddKey mirrors the key set to the bucket so auth survives volume loss
// without control-plane intervention.
func (w *WAL) AddKey(ctx context.Context, name, publicKeyPEM string, scopes []string) (int64, error) {
	w.keyMu.Lock()
	defer w.keyMu.Unlock()
	id, err := w.SQLite.AddKey(ctx, name, publicKeyPEM, scopes)
	if err != nil {
		return 0, err
	}
	if err := w.persistKeys(ctx); err != nil {
		slog.Warn("wal: persist keys", "err", err)
	}
	return id, nil
}

// DeleteKey revokes a key and re-mirrors the set so recovery cannot
// resurrect the revoked key.
func (w *WAL) DeleteKey(ctx context.Context, id int64) (bool, error) {
	w.keyMu.Lock()
	defer w.keyMu.Unlock()
	existed, err := w.SQLite.DeleteKey(ctx, id)
	if err != nil {
		return false, err
	}
	if existed {
		if err := w.persistKeys(ctx); err != nil {
			return true, fmt.Errorf("re-mirror keys after delete: %w", err)
		}
	}
	return existed, nil
}

// Backfill seeds bucket-side truth for repos that predate the WAL (refs
// exist in the index but the bucket has no snapshot or entries) - the
// migration step that lets Litestream retire. Run once at boot; idempotent
// and cheap for already-covered repos (one List each).
func (w *WAL) Backfill(ctx context.Context) error {
	repos, err := w.SQLite.ListRepos(ctx)
	if err != nil {
		return err
	}
	if _, err := w.Blobs.Get(ctx, instancePrefix, keysBlob); errors.Is(err, blobstore.ErrNotFound) {
		if keys, err := w.SQLite.ListKeys(ctx); err == nil && len(keys) > 0 {
			if err := w.persistKeys(ctx); err != nil {
				return fmt.Errorf("backfill keys: %w", err)
			}
			slog.Info("wal backfill: seeded bucket key mirror", "keys", len(keys))
		}
	}
	for _, repo := range repos {
		snap, entries, err := w.scan(ctx, repo.ID)
		if err != nil {
			return fmt.Errorf("backfill scan %s: %w", repo.ID, err)
		}
		if snap != nil || len(entries) > 0 {
			continue // already bucket-covered
		}
		refs, err := w.SQLite.ListRefs(ctx, repo.ID)
		if err != nil {
			return err
		}
		seq, err := w.SQLite.WALSeq(ctx, repo.ID)
		if err != nil {
			return err
		}
		data, _ := json.Marshal(walSnapshot{Seq: seq, DefaultBranch: repo.DefaultBranch,
			Public: repo.Public, Refs: refs, TS: time.Now().Unix()})
		if err := w.writeSnapshotBlob(ctx, repo.ID, data); err != nil {
			return fmt.Errorf("backfill snapshot %s: %w", repo.ID, err)
		}
		if hooks, err := w.SQLite.ListWebhooks(ctx, repo.ID); err == nil && len(hooks) > 0 {
			if err := w.persistWebhooks(ctx, repo.ID); err != nil {
				return fmt.Errorf("backfill webhooks %s: %w", repo.ID, err)
			}
		}
		slog.Info("wal backfill: seeded bucket snapshot", "repo", repo.ID, "refs", len(refs))
	}
	return nil
}
