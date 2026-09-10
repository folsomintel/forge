# Use cases to optimize: git protocol + REST API

Review, 2026-08-20. What Forge is actually used for, split by the two
surfaces, with what's optimized and where the gaps are. Two audiences:
**agents** (machine-to-machine, latency-sensitive, often no local git) and
**humans/CI** (clone/fetch/push over standard git).

## Git protocol surface (`/{repo}/...`)

Present: smart-HTTP upload-pack / receive-pack / info/refs, LFS batch,
bundle-uri (`/bundles/{repo}`), ephemeral namespace view (`repo+ephemeral`).

| Use case | State | Note |
|---|---|---|
| Agent commit storm (small pushes) | ✅ optimized | Go fast path (no fork), group commit; ~2×/core |
| Clone / fetch (CI, human) | ✅ | bundle-uri cold-clone offload, bitmap, commit-graph; **midx now** for multi-pack reads |
| Shallow / partial clone | ✅ | stock git (`--depth`, `--filter`) via the on-disk repo |
| Large-file push | ✅ | LFS + staged tee (overlap upload with transfer) + memory-tier push caps |
| Ephemeral branches (agent scratch) | ✅ | namespaced, invisible to normal view |
| Clone from any machine (auth) | ✅ **now** | access tokens as git password; ed25519/OpenSSH keys accepted |
| **SSH transport (`git@host`)** | ❌ gap | people expect `git clone git@…` with their ssh key; today it's HTTPS+token only. Biggest missing familiarity item |
| Protocol v2 | ✅ passthrough | `GIT_PROTOCOL` forwarded to git |

**Top git-protocol gap: SSH transport.** The ed25519 work makes keys
*registerable*, but a human with an ssh key still can't `git clone git@…`.
An SSH front door (authenticate by registered ssh key → exec
upload-pack/receive-pack against the cache repo) is the highest-value
familiarity feature and reuses everything below the transport. Scoped as
a follow-up; it's a real server (sshd-style), not a small patch.

## REST API surface (`/api/...`)

Present and genuinely broad: repos, branches, refs, commits, contents,
trees, blobs, diff, merge, webhooks, import, fork, raw files, usage,
audit, keys. OpenAPI spec generated (huma) → agents can codegen clients.

| Use case | State | Note |
|---|---|---|
| Read file / tree / commit without cloning | ✅ | `getContents`/`getTree`/`getBlob`/`getRawFile`, cat-file pool amortizes |
| **Write code without git** (agents) | ✅ strong | `putContents`, `deleteContents`, `commitFromDiff`, `merge` — no local git needed. A real differentiator |
| Create repo → first push | ✅ **now** | empty-repo state with push commands instead of 404 |
| CI triggers | ✅ | webhooks with deliveries log |
| Migration in | ✅ | `startImport` (offline bundle/pack ingest) |
| **Conditional reads (ETag / 304)** | ❌ gap | agents polling contents/commits re-transfer unchanged bodies; ETag+`If-None-Match` would make polling ~free (and mirrors Cursor's read model) |
| **Batch file fetch** | ❌ gap | an agent reading N files makes N round trips; a `trees/{sha}?blobs=path,path` or multi-blob batch cuts agent latency materially |
| Pagination at scale | ⚠️ verify | `listCommits`/`listRepos` need cursor pagination before a big repo/tenant; confirm limits + `next` cursors exist |
| Rate-limit backpressure | ✅ | per-key bucket, `Retry-After` set; now tier-scaled |

**Top REST gaps, in agent-value order:**

1. **Conditional reads (ETag/304)** on contents/commits/trees. Agents
   poll; today every poll re-transfers. An ETag (the ref SHA or blob OID
   is a natural validator) turns a poll into a ~10ms 304. Cheap, high
   impact, and philosophically the same move Cursor made for replica
   reads.
2. **Batch blob/tree fetch** — kill the N+1 round trips when an agent
   reads a working set of files. One request, many blobs.
3. **Pagination audit** — make sure every list endpoint has a bounded
   default + cursor before someone hits a 100k-commit repo.

## Cross-cutting

- **Auth clarity** (this pass): access token = simple git password;
  signing keys (incl. ed25519/ssh) = advanced/agent self-signing. The two
  paths are now distinct in the panel copy. SSH transport would add a
  third, most-familiar path.
- **Consistency**: single machine per tenant = reads are always current
  (no replica staleness). If/when replicas land (see `scaling.md`),
  freshness-on-read becomes mandatory — and the ETag work above is the
  same mechanism, so doing #1 now pays double.

## Recommended order

1. Conditional reads (ETag/304) — small, high agent value, reusable for
   replicas.
2. Batch blob/tree fetch — removes agent N+1 latency.
3. Pagination audit on list endpoints.
4. SSH transport — the big familiarity feature; separate project.

## SSH transport — shipped, with an infra opt-in (2026-08-20)

`git clone git@host:repo.git` is implemented end-to-end (`internal/sshd`):
public-key auth against the same registered keys (ed25519/rsa/ecdsa),
sessions run upload-pack/receive-pack over the shared materialize +
hook→WAL path, host key persisted in the bucket. Verified locally
(push/clone/unknown-key-reject) and on real infra (the Go sshd responds
and enforces publickey).

**One infra requirement to enable it per tenant:** raw-TCP SSH (port 22)
needs a **dedicated IPv4** — Fly's shared v4 only routes 80/443 through
the HTTP proxy (confirmed: over shared v4 the handshake never reaches the
listener; over a dedicated v4 it does). So enabling SSH for a tenant
means: `fly ips allocate-v4` (~$2/mo) + point the custom-domain A record
at it. This is a natural **opt-in feature flag** ("Enable SSH" allocates
the v4 + repoints DNS), so only tenants that want SSH pay for it. The
code is done; wiring the flag to IP-allocation + CF DNS is the remaining
step. (Dedicated IPv6 is already allocated per tenant and works today for
v6-capable clients.)
