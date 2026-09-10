# Architecture

Headless git hosting where **object storage + a transactional metadata DB
are the only sources of truth** and local disk is a disposable cache. This
is the JGit-DFS storage shape (Google/CodeCommit-proven) served by real git.

**We are not Spokes.** Pierre/GitHub replicate hot repos across 3 local
disks with quorum writes. We don't replicate at all: durability comes from
the pack store (S3) and the DB. A serving node holds zero authoritative
state — `rm -rf` its cache and it rebuilds. That is the core bet: cheaper,
simpler ops, stateless scale-out; the cost is S3 latency on cold reads,
absorbed by the cache tier.

## Components (one binary, `forged`)

```
git client ──smart HTTP──▶ githttp ──exec──▶ git upload-pack/receive-pack
REST client ──JSON──────▶ api     ──────────▶ gitcmd plumbing (read ops)
                              │                    │
                     ingest (pushes, commits, imports)
                              │
                              ▼
                        repodb.DB (SQLite): repos, refs (CAS), pack list,
                        webhooks, deliveries, LFS, keys — role interfaces
                        (RefStore, PackIndex, ...) composed into DB
                              │
                        blobstore.Store (local | S3): immutable pack+idx blobs
                              │
                        repocache (materialized, disposable cache repos)
```

Packages are named for their role in the doctrine (truth / ingest /
artifacts / serving):

- **`repodb`** — the only component with transactions. Refs are rows updated
  by compare-and-swap; a push's ref updates commit atomically or not at all.
- **`blobstore`** — immutable blobs. Written once (one pack per push/commit
  op), deleted only by maintenance after the pack list has moved on.
- **`repocache`** — materializes cache repos (sync packs from store, write
  loose refs from DB, atomic per-file), LRU-evicts them under disk pressure,
  and pools cat-file daemons for API reads.
- **`gitcmd`** — the git subprocess layer: plumbing execs and parsers.
- **`ingest`** — everything that creates truth: pushes (pre-receive), API
  commits, bundle imports. One shared quarantine → blobs → CAS pipeline;
  loose objects never exist outside a quarantine.
- **`maintain`** — the background pipeline (consolidate → derive → advertise
  → sweep). Internal lifecycle only: a periodic worker plus post-write
  nudges (import completion, pushes crossing the pack threshold). The ops
  endpoint POST /api/repos/{id}/maintenance exists but is hidden from the
  spec and SDK — customers never think about packs.
- **`githttp`** — smart HTTP by exec'ing real git. The wire protocol,
  negotiation, and pack generation are never reimplemented.
- **`api`** — REST surface (contents, git-data, branches, commits, merge,
  webhooks, imports, usage).

## Write path (push)

1. `git receive-pack --stateless-rpc` runs against the cache repo with the
   incoming objects in a **quarantine** dir (stock git behavior).
2. Our `pre-receive` hook (the same binary, re-invoked): upload quarantine
   pack+idx to the store → insert pack rows → **CAS all ref updates in one
   DB transaction** → enqueue webhook deliveries.
3. Hook exits 0 → git migrates the pack into the cache and acks. By then
   the push is already durable off-node. CAS failure rejects the whole push
   atomically; the uploaded pack becomes garbage for compaction.

API-driven writes (contents PUT, commit-from-diff, merge) follow the same
pipeline with git plumbing standing in for receive-pack: build objects in a
quarantine (`GIT_OBJECT_DIRECTORY`), `pack-objects` the new closure, upload,
CAS, install pack into the cache.

## Read path

Materialize (idempotent, cheap when warm) → exec `git upload-pack` or
plumbing. Ref reads need only the DB; pack bytes hydrate from the store on
miss. Cold repo = DB rows + S3 blobs; "hydration" is just a cache fill.

## Locking (single node)

Per-repo `RWMutex`: pushes/commit-ops hold it exclusively for their whole
git exec; fetches/reads hold it shared after a brief exclusive materialize.
Cross-node correctness never depends on these locks — the DB CAS is the
real arbiter; node locks only prevent cache-file races.

## Ephemeral branches

Git ref namespaces (`gitnamespaces(7)`). `repo+ephemeral.git` serves with
`GIT_NAMESPACE=eph`, so ephemeral refs live at
`refs/namespaces/eph/refs/heads/*` in the DB and are invisible to normal
clones (plus `transfer.hideRefs=refs/namespaces` on non-namespaced serves).
Same object store — promotion is a ref CAS, zero object copying.

## Decisions log

- **Real git, not a reimplementation** — protocol fidelity is a treadmill;
  we stand on git's (see research: gitoxide has no server side, CodeCommit
  paid a decade of feature lag).
- **stdlib `net/http`, no framework** — Go 1.22+ ServeMux covers the whole
  route surface; zero deps is a feature for auditable infrastructure. If it
  ever hurts, `chi` is the only acceptable swap (stdlib-compatible).
- **No ORM** — the hottest query is a CAS whose correctness is
  `RowsAffected == 1` inside a transaction; hand SQL behind `meta.Store`.
- **Metadata topology (decided 2026-08): SQLite+Litestream IS the data
  plane; Postgres is control-plane only.** No Postgres port of the
  refs/pack store. Each serving node's SQLite is a disposable read index:
  refs are synchronously durable in the bucket via the conditional-PUT
  ref WAL (RPO=0 - the ack implies the bucket has it), webhook configs
  mirror to the bucket, and a fresh volume self-recovers on boot.
  Postgres arrives only with the hosted control plane (tenants, keys,
  billing). History: v1 used Litestream (async, sub-second replication
  window); retired 2026-08-18 when the ref WAL shipped - one less
  sidecar, no restore path, no async window.
- **Background jobs: table-driven worker, no queue library.** The
  deliveries table is a transactional outbox drained by an in-process
  worker (bounded-concurrency deliveries — no head-of-line blocking).
  River (Postgres-native job queue) is the upgrade path once the control
  plane lands and job types multiply; not before.
- **Webhook egress: SSRF-guarded by default.** Deliveries dial through a
  guard that rejects loopback/private/link-local/reserved targets at
  Dialer.Control time (post-DNS, re-checked per redirect hop).
  `FORGE_WEBHOOK_ALLOW_PRIVATE=true` opts out for dev.
  `FORGE_EGRESS_PROXY` layers a CONNECT proxy (Stripe Smokescreen) on top
  for hosted deployments — belt and suspenders, per Stripe's own webhook
  posture.
- **No internal RPC yet** — API and git serving co-locate because the REST
  data plane needs the materialized repos. Multi-node = homogeneous nodes
  behind rendezvous-hash routing (plain HTTP); nodes coordinate through DB
  CAS, not with each other. If a control-plane/serving split ever happens,
  use Connect RPC (curl-able, protobuf-schema'd, no grpc-go weight).
- **Standard git formats in the store** (packs, idx) — debuggability
  (`git verify-pack`), zero-migration escape hatches, future Rust/JGit
  interop.
- **No storage engine (SlateDB/RocksDB/Iceberg/ZeroFS) under the store** —
  evaluated 2026-08. Our S3 usage is Put/Get/Delete of immutable blobs; the
  problems LSM engines solve (mutable KV on immutable storage, SST
  compaction) aren't ours, and *git* compaction (re-deltifying packs) can
  only be done by git — a storage engine merging our blobs would not
  re-delta anything. ZeroFS/JuiceFS are the POSIX-over-S3 trap above.
  Iceberg/DuckLake are analytics table formats, wrong domain. The
  "pure S3" idea shipped as the native ref WAL (conditional-write CAS);
  SlateDB stays rejected (cgo bindings). Litestream served as v1's
  durability layer and was retired when the WAL landed.

## The derived-data flywheel

Cursor's Continuity post (2026) independently converged on this design and
validated it at 120-300 pushes/s; their one addition - the ref WAL living
in S3 itself - is our named next milestone (below).

Doctrine: **the bucket holds truth (packs + refs); everything else is
derived, redundant, regenerable, and produced only by background
workers.** Reads use the freshest derived form present and fall back one
level when it is missing; nothing ever waits for derivation. We happily
spend bucket storage (bought at cost) to buy latency (what we sell).

Vocabulary is now code: `internal/maintain/artifacts.go` defines the
`Artifact` interface (Kind / Derive / Hydrate) and the registry; the
pipeline in `internal/maintain/pipeline.go` runs consolidate -> derive
(concurrent) -> advertise -> sweep. Adding an artifact = one type + one registry entry;
hydration is best-effort by contract and can never fail a
materialization. Bundle derivation is synthesized (v2 header + refs +
the gc pack's bytes verbatim - `git bundle verify`-clean) instead of
`git bundle create`, turning a whole-repo recompression into an io.Copy.

Current artifacts, all emitted by the maintenance pipeline:
- **gc pack** (re-deltified closure) - clone/fetch efficiency
- **reachability bitmap** - object counting for clones: walk -> O(1)
- **commit-graph** - log/merge-base/ancestry at scale
- **clone bundle + signed capability URL** (bundle-uri) - opted-in clients
  bootstrap clones from the bucket path, origin only serves the delta
- **NVMe materialization** itself - disposable cache with a change-token
  fast path (SQLite data_version) so unchanged repos cost zero sync
- **cat-file --batch-command pool** - per-hot-repo daemons replace
  fork+exec for object reads (killed on eviction; LRU-capped)

Push path concurrency (the Cursor lesson): lock the ref transaction,
never the transfer. Receive-packs run concurrently per repo (isolated
quarantines; DB CAS arbitrates; git takes its own per-ref locks); the
serve path never convoys behind in-flight transfers - a stale
advertisement is harmless because CAS decides. Hooks call into the
running server over a unix socket (warm S3 client + DB pool).

**SHIPPED (2026-08-18) - refs WAL on S3 (repodb/wal.go); now the ONLY
write path (flag deleted same day after a Tigris-verified soak), with
group commit: concurrent ref transactions on a repo batch behind a
leader into one conditional PUT (measured 15.9x same-repo ref-tx
throughput at 10ms PUT latency; entry format unchanged so replay and
recovery are untouched). Safety review hardened: snapshots serialized
per repo with the claimed seq read before the refs (kills a
snapshot/prune race), webhook/key mirrors serialized, eviction has a
minimum-idle guard, and SQLite dropped to synchronous(NORMAL) - the
index is disposable and WAL entries carry events, so replay restores
anything a crash window loses. Original design:**
each ref transaction is a conditional PUT (If-None-Match:* on a
monotonically keyed refs/wal/<seq>.json = object-store CAS; minio-go
SetMatchETagExcept, native on S3 and Tigris); snapshot every 64 entries
bounds replay and prunes the tail; SQLite is demoted to a disposable
local read index; server boot with an empty index auto-recovers repos,
refs, and the pack list from the bucket alone. Library check re-run at
build time: no Go-native WAL-on-object-storage lib exists (Chroma's
wal3 is Rust; raft-wal/fgrosse-wal are local-disk; slatedb-go is now
FFI bindings over the Rust engine - cgo, fights the static musl build).
Native implementation is ~350 lines with zero new deps. Flag-gated off
while it soaks; the whole e2e suite runs in both modes.

**SHIPPED (2026-08-18) - staged tee push (ingest/stage.go, githttp/tee.go):**
the receive-pack body's pack section tees into a staged blob upload
DURING the client transfer; the hook trailer-matches the quarantine pack
and collapses the ack-path upload into a server-side copy + idx upload
(thin packs fall back). FORGE_STAGED_PUSH=false is the kill switch.

**Also queued:** fleet rollout should drain active transfers before
restarting a machine (a mid-push rollout killed a 21-minute llvm push;
health-gating alone does not protect long transfers).

## Deployment

Scaling never replicates data — durability is the bucket's job; nodes are
evictable caches. That single fact is the whole ops story.

**Platform (decided 2026-08): Fly.io.** Control plane on Fly (Fly Managed
Postgres, stateless API); one Fly Machine + NVMe volume per tenant for the
data plane, provisioned via the Machines API. Customer buckets live
wherever the customer wants — FORGE_S3_* already speaks any S3 endpoint.

**Customer bucket credentials (decided 2026-08, revised: all-Fly, no
AWS/KMS).** Auth needs no secret storage at all (customers self-sign
JWTs; we hold only public keys). Bucket creds: (1) primary home is a
**Fly secret on the tenant's own machine** — the control plane writes
FORGE_S3_* via the Fly secrets API at provision time; our Postgres holds
no bucket secrets and a control-plane DB dump contains nothing to steal;
(2) for machine re-provisioning without customer action, an AES-256-GCM
encrypted copy in Postgres under a master key that is itself a Fly
secret on the control plane (key id stored beside ciphertext; rotation =
re-encrypt). Known trade vs a KMS: control-plane RCE yields both halves
and decrypts aren't externally audited — acceptable now; wrapping the
same data keys with a KMS later needs no schema change. Rejected:
encrypting with a customer-held key (the data plane needs bucket access
for maintenance/WAL/retries with no customer request in flight)
and AWS KMS/AssumeRole (no AWS infrastructure in a Fly stack; AWS-bucket
customers supply scoped keys like every other provider). Per-tenant
machines contain the blast radius either way.

**Phase 1 — one node per tenant (now).** A tenant is one VM + one bucket:
`forged` (+ optional Smokescreen) via docker-compose; S3 is the
customer's bucket (BYO) or ours. Fly Machines is the preferred home
(API-driven machine+NVMe provisioning → tenant creation is an API call;
the Pierre-style "automated single-tenant" story); Hetzner for cost;
Railway acceptable for pilots. Failover = boot a fresh machine on the
same bucket; the ref WAL self-recovers the index. No coordination
software exists in this phase.

**Phase 2 — cells, not consensus (hosted fleet).** Stateless router +
control plane (Postgres, tiny API) + N interchangeable NVMe cache nodes.
Repo→node by rendezvous hashing over the node registry, single writer per
repo enforced by routing + a control-plane ownership lease. Refinement
that makes rebalancing free: the ref WAL already lives in each repo's
own S3 prefix beside its packs — a repo is a self-contained prefix, so
adding/removing nodes just shifts hash ownership and new owners replay
a few-KB WAL on first touch. Nothing migrates. Scale-down is safe
because nodes are caches.

**Explicitly rejected:** Raft (consensus is outsourced to S3 + CAS; Raft
is for logs that live on nodes — the GitLab/Spokes path we designed
away), Redis (no shared mutable state), Kafka (the outbox table is the
queue; customer event streams are an integration, not infra), Flink (no
stream processing exists), Cloud Run for serving nodes (memory-backed
ephemeral disk, CPU throttling off-request, no instance routing — fine
for the future control plane only), Kubernetes for our own fleet (Helm
chart only if an enterprise BYO customer demands it).

## Known gaps (tracked for Day 3+)

- Pushes serialized per repo per node (correctness first; relax later).
- Orphan packs from rejected pushes await compaction.
- No pack consolidation yet → per-push packs accumulate until the
  compaction service lands.
- Webhook delivery worker is single-process polling; multi-node claims via
  per-node outbox (each node delivers what its data plane enqueued).
