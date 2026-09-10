# Eliminating the fork: an in-process Go receive path

Investigation → implementation, 2026-08-19. Follows the storm benchmark
(see `benchmarks.md`), which located the push-throughput ceiling at ~60/s
on performance-2x and proved it is **not** the WAL — it is git's
fork-per-push saturating CPU.

## Result (measured)

Shipped a gated Go fast path and re-ran the same storm on the same
performance-2x instance. Every push in the agent-commit shape was
eligible (4160 eligible, 0 fell back):

| scenario | fork path | Go fast path |
|---|---|---|
| 64 writers, 8 repos | 35.8 /s | **100.1 /s** |
| 64 writers, 32 repos | 59.6 /s | **126.7 /s** |
| 64 writers, 1 repo | 8.5 /s | 9.7 /s |

~2× the throughput per core, p50 latency down from 732ms to 415ms. The
single-repo case barely moves because it serializes at the per-repo WAL
leader (that is group commit, not the fork) — but agent fleets push to
many repos, which is exactly where the win lands. Projecting the per-core
rate, 300/s is now a ~4-core instance instead of the ~8-16 the fork path
implied. The remaining ceiling is Tigris PUT latency (avg 22-54ms, with
occasional tail stalls), not CPU.

## Why this is worth doing (measured)

Every push forks three processes: `git receive-pack`, the `index-pack`
it spawns to verify the incoming pack, and the pre-receive hook. On this
dev machine:

- bare `git` process startup: **9.1 ms**
- `index-pack` on a tiny pack: **24.7 ms** (incl. its own spawn)

A fresh agent commit is ~3 objects totalling a few hundred bytes. The
actual work on those bytes — zlib inflate + SHA-1 — is **sub-millisecond**.
So for the push shape we care about, **>95% of per-push CPU is process
spawning, not git doing anything useful.** The storm's 60/s on 2 cores
(~33 ms CPU/push) is almost entirely fork/exec/startup overhead.

That is the case for a Go path: it is not "reimplement git faster," it is
"stop paying 45 ms of process startup to do 1 ms of work." Expected
payoff is large precisely because the cost being removed is fixed
overhead, independent of pack contents.

If the fast path turns 45 ms of overhead into ~2-5 ms in-process, a
2-core instance plausibly moves from ~60/s to ~200-400/s — putting
Cursor's 300/s within reach of a **modest** instance instead of a
16-core one.

## The fork inventory

Only the push path is in scope. Reads (upload-pack), API commits
(commit-tree/pack-objects, low volume), imports (index-pack, rare), and
maintenance (pack-objects with bitmaps, background) keep forking git —
none was a storm bottleneck, and the pooled `cat-file` daemons are
already long-lived.

Push path today (`internal/githttp/smart.go` → git → hook):

1. `git receive-pack --stateless-rpc` — reads the POST body
2. → `index-pack` — verifies the pack into the quarantine
3. → pre-receive hook (`forged hook`) — POSTs updates to the warm
   in-process socket, which runs the WAL CAS (`ingest.Apply`)

Step 3's *work* is already in-process (the socket fast path); the fork is
just git's extension mechanism. Steps 1-2 are the expensive forks.

## Recommended design: Go fast path + git fallback

Do **not** reimplement all of receive-pack. Add a Go handler for the
common agent push and fall back to forking git for everything else — the
same fast-path-with-correct-fallback shape the staged-tee already uses.
git stays the source of truth for correctness; we only skip it when we
are certain we can do the exact same thing.

The Go `git-receive-pack` handler must:

1. **Parse pkt-line commands** from the POST body: `<old> <new> <ref>`
   lines + capabilities, flush-pkt, then the packfile. (~30 lines; we
   already emit pkt-lines in `storm.go`.)
2. **Parse + verify the packfile.** ✅ *Spiked and tested* —
   `internal/ingest/packread.go` parses a v2 pack, inflates each object,
   computes OIDs **bit-identical to git** (test cross-checks git's
   canonical hash for `hello\n`), and verifies the trailing SHA-1. On any
   delta object it returns `ErrNeedsGit`.
3. **Connectivity check** — the security-critical step. For every new ref
   target, walk its object closure (commit→tree→parents→blobs) and prove
   each referenced OID is either in the pack or already in the store.
   Without this a client could point a ref at objects that don't exist
   and corrupt the repo for everyone. Needs commit/tree parsing (small)
   and an object-existence lookup (the `cat-file` pool or pack/idx reads).
4. **Generate the `.idx`** (v2: fanout + sorted OID table + offsets + CRC
   + trailer) so `upload-pack` can serve what we stored. Mechanical.
5. **Persist via the existing pipeline.** The staged tee *already*
   uploaded the raw wire pack; a non-thin verified pack IS the pack to
   keep. Store it + the generated idx, then call `ingest.Apply` (the same
   WAL CAS the hook uses today) — no new durability code.
6. **Write report-status** pkt-lines: `unpack ok`, then `ok <ref>` /
   `ng <ref> <reason>`. Trivial.

When to fall back to git (return the body to a forked `receive-pack`):
delta/thin packs (until delta resolution lands), packs over a size
threshold, unfamiliar capabilities, or any parse anomaly. Falling back is
always safe; the only cost is the fork we were trying to avoid.

## The hard parts, honestly

- **Delta resolution.** ofs-delta (base by offset, in-pack) and ref-delta
  (base by OID, possibly *outside* the pack = thin) require applying
  copy/insert instructions against a reconstructed base. This is the
  genuinely fiddly ~1 week. The spike punts on it (`ErrNeedsGit`), and
  the common fresh-commit push is undeltified — but real clients *do*
  send deltas once a push touches many or large objects, so the fast
  path's hit rate without delta support needs measuring before we lean on
  it.
- **Thin packs.** ref-deltas against objects the client assumes we have.
  git runs `index-pack --fix-thin` to complete them; a Go path must
  resolve against existing store objects and re-index. Ships with delta
  support.
- **Connectivity + security.** This code parses untrusted input from the
  network. It must be fuzzed (`go-fuzz` on the pack parser and the delta
  applier) and must never over-allocate on an attacker-controlled length
  (the spike already bounds objects by what actually inflates). A missed
  connectivity check is a repo-corruption bug, not a perf bug.
- **`upload-pack` compatibility.** Objects we write must be servable by
  the git we keep on the read path — satisfied by storing standard
  packfiles + valid v2 idx, which the spike's output format targets.

## Scope + effort

| Piece | State |
|---|---|
| pkt-line command parse + report-status (side-band aware) | ✅ shipped |
| pack parse + OID verify + trailer | ✅ shipped & tested |
| v2 .idx generation | ✅ shipped, validated by `git verify-pack` |
| connectivity check (commit/tree/tag walk over pack+store) | ✅ shipped |
| fast-path handler + git fallback wiring | ✅ shipped (gated) |
| delta + thin-pack resolution | not started (~1 week) |
| fuzzing + hardening | not started (ongoing) |

The **non-delta fast path** is done and measured (2× per core above).
Delta/thin resolution is the remaining work; until then those pushes
fall back to git, correctly. Whether delta support is worth building
depends on how many *real* agent pushes are delta-free — which the
shipped `fell_back` counter now measures in production.

## Next steps

The non-delta fast path is shipped, gated (`FORGE_GORECEIVE`), and proven
2× on the storm. To turn it on for real tenants:

1. **Measure real eligibility.** Enable it on a dogfood tenant and watch
   `/api/usage` → `go_receive` (eligible vs fell_back) under real client
   pushes, not just the synthetic storm (which is 100% eligible by
   construction). Real `git push` from a working clone often sends
   delta-compressed packs — the `fell_back` rate tells us whether delta
   resolution is worth the ~week.
2. **Fuzz before default-on.** `go test -fuzz` the pack parser and the
   tree/commit walkers on malformed input — this is the untrusted-input
   surface, and a bad connectivity check corrupts a repo.
3. **Then flip the default** (or scale the CP template to enable it per
   tier), and revisit delta/thin resolution if the fell_back rate is high.
