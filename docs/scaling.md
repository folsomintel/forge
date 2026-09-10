# Auto-scaling runners: vertical, horizontal, and replicas

Investigation, 2026-08-20. How Forge can auto-scale a tenant's compute —
up/down (vertical) and out (horizontal replicas) — given the current
one-machine-per-tenant model and the bucket-as-truth architecture.

## What exists today

- **Tenant = one Fly machine + one volume** (`Tenant.MachineID/VolumeID`),
  one instance per subscription (enforced in `orgCreateTenant`).
- **`RunMonitor`** probes each tenant's public URL and heals: restart →,
  if that fails, recreate-from-bucket. This is the HA primitive and the
  proof that a tenant's machine is disposable.
- **`resize`** changes CPU/RAM/volume (admin only, now 1–8 CPU / ≤16GB),
  by recreating/updating the machine. Vertical scaling exists but is
  manual.
- **`drainTransfers`** waits for in-flight git transfers before a
  restart — the graceful-swap primitive both autoscale paths need.
- **`/api/usage`** already exposes the load signals: `active_transfers`,
  and now WAL group-commit + go-receive telemetry. Disk headroom too.

The architecture is already shaped for scaling: **bucket-as-truth means a
machine holds no unique state**, so we can add, remove, resize, or move
machines freely — exactly the property Cursor leans on.

## Vertical autoscaling (fits the current model, low risk)

A policy loop in the monitor samples each running tenant and resizes it
between plan-bounded tiers.

- **Scale-up signals** (sustained, over a window): CPU near cap (Fly
  machine metrics), `active_transfers` persistently above a threshold,
  memory pressure / OOM-restart events (we saw the 256MB tier OOM under
  16 concurrent pushes — that's an auto-bump trigger), disk headroom low
  → grow the volume.
- **Scale-down signals**: idle (no transfers, low CPU) for a long window
  → step down to save cost.
- **Mechanics**: reuse `resize` + `drainTransfers` (drain, update machine
  guest, restart — the machine self-recovers from the bucket). Add
  **hysteresis** (separate up/down thresholds), a **cooldown** (reuse the
  monitor's 10-min pattern), and a **per-plan tier ceiling**.
- **Risk**: low. It is the resize we already have, driven by signals we
  already emit. The only user-visible effect is a brief restart, which
  drain makes graceful. This is the recommended first build.

Sketch: extend `monitorTick` with a `scalePolicy(tenant, usage)` that
returns a target shape; if it differs from current and cooldown allows,
call `resize`. Ship it **off by default per tenant** (an `autoscale`
column / plan flag) so it is opt-in until proven.

## Horizontal autoscaling — auto-replicas (the bigger prize)

Multiple machines per tenant, all materializing the same bucket. This is
Cursor's model ("one replica for tiny repos, hundreds for a monorepo,
GC'd when idle") and bucket-as-truth makes it reachable — but three
pieces are missing.

**What's free on Fly:** a Fly *app* can hold many machines and its proxy
load-balances across them. So "add a read replica" is literally "create
another machine in `node-<x>` pointing at the same bucket," and the
anycast IP fans traffic out. Materialize-on-boot already exists. Removing
a replica is just destroying a machine — no data lives only there.

**The three missing pieces (all have a clear path):**

1. **Read freshness across replicas.** Each machine has its own SQLite
   index; a push committed on machine A (durable in the WAL) isn't in
   machine B's index until B re-syncs. Cursor solves this with a
   conditional GET on the WAL-index ETag before serving (304 = current,
   ~10ms). We already have `syncLocked` (snapshot + WAL replay) — the
   work is to **gate a read on a cheap staleness check**: HEAD/ETag the
   WAL-index blob, and if it moved, replay the tail before serving. That
   turns our index into Cursor's "warm cache, verified against S3 on
   read." Without it, a replica can serve a stale ref right after a push
   on another replica — the exact eventual-consistency failure the
   article warns breaks git clients and CI.

2. **Single-writer / primary-only compaction.** Today each machine runs
   its own maintenance. With replicas, two machines repacking the same
   repo concurrently is the "compaction coordination failure" Cursor
   calls out. Fix: **elect a primary by rendezvous hashing** over the
   healthy machine set (stateless, the same technique they use) and let
   only the primary run `consolidate`/derive. Writes themselves are
   already safe on any machine — the WAL conditional-PUT CAS arbitrates
   split-brain (see `wal.go`), so this is only about not duplicating
   compaction work, not correctness.

3. **Replica lifecycle / routing policy.** A controller that sizes the
   replica set to read load: clone/fetch rate and CPU high → add a
   machine; idle → destroy (down to one, or zero with
   materialize-on-demand). This is the monitor loop plus the Fly Machines
   API (create/destroy), keyed off `active_transfers` and CPU. Optional
   refinement: a read/write split so heavy CI clone traffic hits replicas
   while pushes prefer the primary (Fly handles basic LB; a smarter
   router is a later optimization).

**Effort:** freshness-on-read is the load-bearing ~week (it is the
correctness gate); primary-only compaction is a few days (rendezvous hash
+ a guard on the maintenance trigger); the replica controller is a few
days on top of the existing monitor. Reads scale linearly once
freshness-on-read lands — matching their "linear to 100 replicas."

**Product framing:** vertical autoscale fits the current
one-instance-per-sub plan transparently. Horizontal replicas are an
enterprise/scale tier ("your monorepo, fanned across N replicas for CI")
and also the instant-failover HA story (a machine death is transparent
instead of a recreate-from-bucket wait).

## Recommended order

1. **Vertical autoscale** (opt-in, monitor + resize + hysteresis) — low
   risk, immediate value, no new architecture.
2. **Freshness-on-read** (WAL-index ETag check before serving) — unlocks
   correct multi-replica reads and is good hygiene regardless.
3. **Primary-only compaction** (rendezvous-hash leader) — prerequisite
   for safe replicas.
4. **Replica controller** (autoscale machine count by read load) — the
   full horizontal story and instant-failover HA.

## Horizontal scaling: concrete plan (2026-08-20, post-reconciler)

The reconciler (`reconcile.go`) is now the control loop, which gives the
replica story a home: a replica set is just desired state the reconciler
drives, exactly like machine size or image today.

**What's already true and reusable:**
- Bucket-as-truth: any machine reconstructs a tenant from the bucket
  (`syncLocked` = snapshot + WAL replay; `MaterializeServe` on the read
  path). Machines hold no unique state.
- A Fly *app* load-balances across its machines for free, so N machines in
  `node-<x>` behind the anycast IP already fan reads out.
- The WAL conditional-PUT CAS already makes concurrent writers from
  *different machines* safe (split-brain-proof) — the hard correctness
  bit is done.

**The three additions, now concretely specified:**

1. **Freshness-on-read (the correctness gate).** A replica's SQLite index
   lags a push committed on another machine until it re-syncs. Cheap fix
   using what exists: before serving a ref-dependent read, check whether
   `refs/wal/<localSeq+1>.json` exists in the bucket (one conditional GET,
   ~10ms). 404 = current, serve immediately; 200 = the WAL advanced, run
   `syncLocked` to replay the tail, then serve. This is Cursor's
   ETag-conditional-GET in our idiom, and it reuses `WAL.scan` /
   `syncLocked` verbatim. Without it, a replica can serve a stale ref
   right after a push landed elsewhere.

2. **Single-writer/primary-only compaction.** Two machines repacking one
   repo concurrently is wasteful (and the "compaction coordination
   failure" Cursor warns of). Elect a primary per repo by rendezvous hash
   over the healthy machine set (stateless) and gate `maintain.Pipeline`
   on "am I this repo's primary?". Writes stay safe on any machine (CAS
   arbitrates); this is purely to not duplicate maintenance work.

3. **Replica controller (a reconcile step).** Desired replica count per
   tenant = f(read load): clone/fetch rate + CPU high → add a machine
   (materialize-on-boot already works); idle → remove (down to 1, or 0
   with materialize-on-demand). Implemented as a `reconcileReplicas` step
   using the Fly Machines API (create/destroy), keyed off the
   `active_transfers` and usage signals the reconciler already reads. A
   tenant row gains `desired_replicas` (or an autoscale policy); the loop
   converges actual→desired, same as everything else it does.

**Effort:** freshness-on-read ~1 week (the load-bearing correctness work +
tests); primary-only compaction ~2–3 days (rendezvous hash + a guard);
replica controller ~2–3 days on the reconciler. Reads then scale linearly
with replica count (Cursor's "linear to 100 replicas"), and instant
failover falls out (a dead replica is just one of N, not a
recreate-from-bucket wait).

**Sequencing:** freshness-on-read first (also useful for the ETag read
API), then primary-only compaction, then the controller. All three land
as reconcile steps, so the operational surface (admin panel events, backoff,
parking) is inherited for free.
