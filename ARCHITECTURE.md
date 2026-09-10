# Architecture

Forge serves the real git wire protocol, but its source of truth is an object
store, not a disk. Every push becomes an immutable pack in the store plus a
compare-and-swap entry in a ref write-ahead log; the local SQLite index and the
on-disk repo are disposable caches, rebuilt from the bucket. A serving node
holds no authoritative state — delete its cache and it recovers from the store.

This is the JGit-DFS storage shape (proven by Google and AWS CodeCommit), and
the same "object store is the truth" bet Cursor described in *Git at any scale*.

## Components (one binary, `forged`)

```
git client  ── smart HTTP / SSH ──▶ githttp / sshd ──▶ git or Go-native serving
REST client ── JSON ──────────────▶ api
                                      │
                              ingest (pushes, API commits, imports)
                                      │
                              repodb (SQLite): repos, refs (CAS), pack list,
                                      webhooks, LFS, keys — disposable index
                                      │
                              blobstore (local | S3): immutable pack + idx blobs
                                      │
                              repocache: materialized, disposable cache repos
```

- **repodb** — the metadata index. Refs are compare-and-swap rows; a push's ref
  updates commit atomically or not at all. Disposable: an empty node rebuilds it
  from the bucket WAL.
- **blobstore** — immutable pack+idx blobs. Written once, deleted only by
  maintenance after the pack list has moved on.
- **repocache** — materializes cache repos from store+DB, LRU-evicts them under
  disk pressure, and pools `cat-file` daemons for object reads.
- **ingest** — everything that creates truth: pushes (pre-receive), API commits,
  bundle imports. One quarantine → blobs → CAS pipeline; loose objects never
  exist outside a quarantine.
- **githttp / sshd** — the git wire protocol over HTTP and SSH.
- **maintain** — background pipeline: consolidate → derive → advertise → sweep.
- **api** — REST surface and a Server-Sent-Events stream of ref updates.

## Durability: the ref WAL

Each ref transaction is a conditional PUT (`If-None-Match: *` on a
monotonically-keyed `refs/wal/<seq>`) — object-store compare-and-swap, native on
S3 and Tigris. That PUT is the commit point: a push is acknowledged only once
the bucket has it (RPO = 0). Concurrent ref transactions on a repo batch behind
a leader into a single PUT (group commit). A snapshot every N entries bounds
replay, and a node booting with an empty index recovers repos, refs, and the
pack list from the bucket alone. SQLite runs `synchronous=NORMAL` because it's a
disposable read index — the WAL is the truth.

## Write path (push)

1. `git receive-pack --stateless-rpc` runs against the cache repo with the
   incoming objects in a quarantine directory (stock git behavior).
2. The pre-receive hook (the same binary, re-invoked) uploads the pack+idx to
   the store, inserts the pack rows, CASes all ref updates in one transaction,
   and enqueues webhook deliveries. For a small push the pack rides *inside* the
   WAL entry, so refs and data are durable in one conditional PUT.
3. The hook exits 0 → git migrates the pack into the cache and acks. By then the
   push is already durable off-node. A CAS failure rejects the whole push
   atomically; the uploaded pack becomes garbage for compaction.

API-driven writes (contents PUT, commit-from-diff, merge) follow the same
pipeline with git plumbing standing in for receive-pack.

## Read path

Reads run against a materialized cache repo — idempotent and cheap when warm; a
per-repo change token skips the sync entirely when nothing moved. The hot paths
— receive, fetch, ref advertisement, and REST object reads — are served
natively in Go (native delta and thin-pack resolution, a pooled `cat-file`), so
they don't fork git; git is used for repacking and as a correctness fallback. A
cold repo is just DB rows plus S3 blobs, and hydration is a cache fill.

## Concurrency

A per-repo `RWMutex` guards cache-file races only; the DB/WAL CAS is the real
arbiter, so correctness never depends on in-process locks. Pushes lock the ref
transaction, never the transfer: receive-packs run concurrently per repo
(isolated quarantines, CAS arbitrates), and the serve path never convoys behind
an in-flight transfer — a stale advertisement is harmless because CAS decides.

## Derived data

The bucket holds truth (packs + refs); everything else is regenerable and
produced only by background workers. Reads use the freshest derived form present
and fall back a level when it's missing — nothing ever waits for derivation.
Artifacts (`internal/maintain/artifacts.go` defines the `Artifact` interface and
registry):

- **gc pack** — a re-deltified closure of every ref, for clone/fetch efficiency.
- **reachability bitmap** and **commit-graph** — O(1) clone counting; log,
  merge-base, and ancestry at scale.
- **bundle chains** — a full clone bundle plus incremental catch-up bundles,
  published as static bucket objects and advertised via `bundle-uri`, so clients
  bootstrap clones and repeat fetches from the store. Bundles are synthesized
  (v2 header + refs + the gc pack's bytes) rather than `git bundle create` — an
  `io.Copy`, not a whole-repo recompression.
- **history pack** — a blobless (commits + trees) pack a read replica keeps
  local, so refs/log/tree reads stay local while blob bytes stream from the
  bucket in blocks (the "remote reader"), letting a replica serve a repo larger
  than its disk.

## Ephemeral branches

Git ref namespaces (`gitnamespaces(7)`): `repo+ephemeral.git` serves with
`GIT_NAMESPACE=eph`, so ephemeral refs live at `refs/namespaces/eph/…` and are
invisible to normal clones. They share the same object store — promotion is a
ref CAS with zero object copying.

## Design choices

- **Real git for the wire protocol.** Negotiation and pack generation are a
  fidelity treadmill; forge stands on git's rather than reimplementing them. The
  Go-native paths are optimizations layered on top, each with a git fallback.
- **Standard git formats in the store** (pack, idx) — debuggable with
  `git verify-pack`, no migration lock-in.
- **stdlib `net/http`, no ORM.** The hottest query is a CAS whose correctness is
  `RowsAffected == 1` inside a transaction; hand-written SQL behind an interface.
- **No replication.** Durability is the object store's job and nodes are
  evictable caches, so scaling never migrates data. Multi-node correctness comes
  from object-store CAS, not consensus — no Raft, no Redis, no shared mutable
  state.
