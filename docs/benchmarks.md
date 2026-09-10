# Git performance benchmarks

`scripts/bench-git.sh` runs the same workload against any provider —
creates `bench-*` repos, exercises every major git use case, prints a
table, and deletes what it made:

```sh
PROVIDER=forge   FORGE_URL=https://host FORGE_TOKEN=jwt scripts/bench-git.sh
PROVIDER=github  scripts/bench-git.sh          # gh auth; add delete_repo scope for cleanup
PROVIDER=gitlab  GITLAB_TOKEN=glpat-… scripts/bench-git.sh
PROVIDER=generic BENCH_URL_SMALL=… BENCH_URL_MED=… BENCH_URL_BIG=… scripts/bench-git.sh
```

`generic` takes three pre-created empty repos as authenticated remote
URLs and runs the git-only subset (no management-API cases) — that is
how you point it at Cursor origin or any other git-over-HTTPS host:
create three repos in the provider's UI/CLI, grab the remote URL with an
embedded token for each, and export them as the three variables.

## 2026-08-19 — Forge (entry tier) vs GitHub, same client, same script

Forge = loadtest instance, shared-1x / 256MB / sjc, managed Tigris
bucket — the $29 tier as sold. GitHub = github.com free tier. Client on
residential fiber; medians shown, 95MB blob. Network floors differ
(93ms to the Fly sjc edge vs 308ms to api.github.com), so locality is
part of Forge's edge on the latency rows — but every gap below is
bigger than the 215ms floor difference, and the clone/bandwidth gaps
dwarf it.

| Case | Forge | GitHub |
|---|---|---|
| ping (network floor) | 93 ms | 308 ms |
| ls-remote | 233 ms | 761 ms |
| no-op fetch | 242 ms | 715 ms |
| API: list repos | 76 ms | 542 ms |
| API: tree listing | 97 ms | 470 ms |
| small push (sequential) | 658 ms | 1445 ms |
| concurrent pushes (8×3) | 6.8 /s | 4.3 /s |
| tag push | 567 ms | 1341 ms |
| clone small (100 commits) | 256 ms | 1026 ms |
| clone medium (1000 commits, ~18MB) | 987 ms | 6008 ms |
| clone medium, shallow | 878 ms | 5194 ms (p95 25s!) |
| clone medium, partial | 1220 ms | 7848 ms |
| fork (API) | 314 ms | n/a (can't fork own repo) |
| ephemeral branch push | 785 ms | n/a |
| anonymous ls-remote | 133 ms | 460 ms |
| anonymous clone (small) | 190 ms | 838 ms |
| push 95MB blob | 7.5 MB/s | 0.7 MB/s |
| clone 95MB blob | 15.9 MB/s | 3.1 MB/s |

Notes on the GitHub column: large non-LFS pushes crawl (0.7 MB/s — they
want you on LFS, and warn above 50MB); medium-repo clones pay a
pack-on-the-fly cost Forge's bucket offload doesn't; shallow clone p95
hit 25s on one run. GitLab column pending a `GITLAB_TOKEN`; Cursor
origin pending three pre-created repos via the generic provider.

## 2026-08-19 — push storm: how far to Cursor's 300/s?

`forged storm` drives N concurrent writers over the receive-pack wire
protocol built in pure Go (no client git forks), run from a co-located
Fly machine in sjc (~1ms RTT) so the numbers are the *server* ceiling,
not client RTT. It samples the new WAL group-commit telemetry
(`/api/usage`) before/after so each row shows batch fatness and
conditional-PUT cost. Target: loadtest at performance-2x / 4GB.

| writers | repos | throughput | p50 lat | WAL: batch / PUT |
|---|---|---|---|---|
| 8 | 1 | 5.3 /s | 646 ms | 1.3 avg (max 46) / 24 ms |
| 32 | 1 | 6.9 /s | 4.7 s | 4.1 / 40 ms |
| 64 | 1 | 8.5 /s | 7.4 s | 8.6 / 55 ms |
| 32 | 8 | 26.3 /s | 493 ms | 1.0 / 22 ms |
| 64 | 8 | 35.8 /s | 1.3 s | 1.4 / 31 ms |
| 64 | 32 | **59.6 /s** | 732 ms | 1.0 / 36 ms |
| 128 | 64 | 45.0 /s | 2.4 s | 1.0 / 141 ms |

**The durability layer is not the bottleneck.** This is the headline.
Under single-repo contention group commit does exactly what it was built
to do — batches fatten from 1.3 to 8.6 as writers pile up (max batch
seen: 46), and the conditional PUT stays cheap (24–55 ms). At 64 writers
on one repo the WAL was doing ~155 tx/s worth of committing while the
system delivered 8.5 pushes/s. The WAL has ~20x headroom over what
reaches it.

**The wall is git's fork-per-push.** Every push spawns receive-pack +
index-pack + the pre-receive hook — ~3 forks and real object work per
push. That saturates the 2 performance CPUs at ~60 pushes/s regardless
of how the writers are arranged. Confirmed three ways: single-repo
throughput is nearly flat (5→8/s) while latency explodes (queueing
behind CPU, not the WAL); spreading across repos lifts it to ~60/s (more
parallelism to feed the same CPUs); and `--direct` (skipping the
info/refs round trip) changes nothing, so it isn't the extra HTTP call.
Past ~60/s Tigris PUT latency starts rising (36→141 ms) as unbatched
per-repo PUTs flood it — a secondary limit, not the first one.

**The entry tier (256MB) can't do concurrency at all** — 16 simultaneous
pushes OOM-restarted the machine. That tier is for one developer, not a
fleet; concurrency needs a performance tier.

### Fork elimination: measured (2026-08-19, same perf-2x)

With the gated Go receive fast path on (`FORGE_GORECEIVE=true`), the same
storm, 100% eligible (4160 eligible / 0 fell back):

| scenario | fork path | Go fast path |
|---|---|---|
| 64 writers, 8 repos | 35.8 /s | 100.1 /s |
| 64 writers, 32 repos | 59.6 /s | 126.7 /s |

~2× per core, and it removes the fork the storm identified as the wall.
See `fork-elimination.md`. Default off until real-world eligibility is
confirmed; falls back to git on anything it cannot prove.

### The path to 300/s

1. **Scale by CPU, today, no code change.** ~60/s on 2 performance CPUs
   is roughly linear in cores (the limit is fork throughput): ~240/s on
   8, ~480/s on 16. 300/s is an 8–16 core instance. This is why the
   resize guardrail was lifted from 4 to 8 CPUs / 16GB in this pass —
   the panel already offered those sizes; the API now accepts them.
2. **Eliminate the fork (the real fix).** An in-process Go receive path
   for small packs — the agent-commit case — removes the receive-pack +
   index-pack + hook forks entirely, turning a push into an HTTP handler
   that appends to the WAL. That gets 300/s on far fewer cores.
   `internal/ingest` is the seed. This is the roadmap item.
3. **Per-instance framing beats fleet framing.** Cursor's 300/s is a
   multi-tenant fleet number. Ours is per dedicated instance — "your
   instance sustains 300 concurrent pushes/s" is the stronger claim once
   (1) or (2) lands.

Also fixed this pass: `FORGE_RATE_GIT` defaulted to 10 rps/key (shared-
infra abuse sizing, wrong for a dedicated instance whose owner holds the
key) — now scales with the tier (200 × CPUs). At 10 rps no fleet test
was even possible; it was blocker zero.

## 2026-08-19 — does hardware matter? entry tier vs performance-2x

Same loadtest instance resized to performance-2x / 4GB ($62/mo Fly
list), same client, same script, then resized back down.

| Case | shared-1x / 256MB | performance-2x / 4GB |
|---|---|---|
| ls-remote | 233 ms | 208 ms |
| no-op fetch | 242 ms | 224 ms |
| small push | 658 ms | 594 ms |
| concurrent pushes (8×3) | 6.8 /s | 4.5 /s * |
| clone medium (full) | 987 ms | 857 ms |
| clone medium (shallow) | 878 ms | 765 ms |
| clone medium (partial) | 1220 ms | 1021 ms |
| fork (API) | 314 ms | 295 ms |
| push 95MB blob | 7.5 MB/s | 8.4 MB/s |
| clone 95MB blob | 15.9 MB/s | **30.9 MB/s** |

\* single burst measurement; the 3–5s window is dominated by Tigris PUT
tail latency, and this row has swung 4.5–8.3/s across runs on both
shapes — read it as noise, not regression.

Conclusion: the workload is bucket-I/O-bound, not CPU-bound. 16x the
compute price buys ~10% on latency rows and nothing on write
throughput; the one real win is large-transfer download (2x — pack
streaming is CPU-hungry). The entry tier is the right default; upsize
for high-bandwidth clone traffic, not for push latency.

## 2026-08-19 — loadtest (shared-1x / 256MB / sjc, managed Tigris bucket)

Client: macOS over residential fiber to sjc; network floor (healthz
round trip) was **93ms**, so subtract ~90–100ms from every number to get
server-side cost.

| Case | Median | Notes |
|---|---|---|
| healthz (network floor) | 93 ms | p95 99 ms |
| ls-remote (ref advertisement) | 219 ms | p95 649 ms |
| no-op fetch (up to date) | 232 ms | p95 252 ms |
| API: list repos | 89 ms | p95 95 ms |
| API: tree listing | 103 ms | p95 253 ms |
| small push (1 commit, sequential) | 647 ms | p95 1268 ms |
| concurrent pushes (8 writers × 3) | 2903 ms total | 8.3 pushes/s |
| ephemeral branch push | 657 ms | p95 2197 ms |
| tag push | 585 ms | p95 637 ms |
| clone small (100 commits) | 244 ms | |
| clone medium (1000 commits, ~19MB) | 578 ms | full history |
| clone medium, shallow depth=1 | 498 ms | |
| clone medium, partial blob:none | 748 ms | |
| fork medium repo (API) | 339 ms | zero-copy |
| anonymous ls-remote (public) | 137 ms | |
| anonymous clone (public, small) | 195 ms | |
| push medium repo (initial, 19MB) | 5394 ms | ~3.7 MB/s effective |
| push 100MB blob | 15334 ms | 6.5 MB/s up (client uplink bound) |
| clone 100MB blob | 5523 ms | 18.1 MB/s down |

Reading of the numbers:

- **Reads are cheap.** API reads run at the network floor; clones add
  one or two round trips plus transfer. Partial clone costs slightly
  more than full on this repo size (extra negotiation round trip
  dominates when the repo is small) — it pays off on repos where blobs
  dominate history.
- **Writes carry the durability tax.** ~550ms of a small push is the
  bucket round trips: staged pack tee + conditional ref-WAL PUT on
  Tigris. That's the price of bucket-as-truth on the entry tier.
- **Group commit works.** 24 pushes from 8 concurrent writers landed in
  2.9s — 8.3 pushes/s against ~1.5/s sequential, because concurrent
  ref-WAL appends batch into one conditional PUT.
- **Fork is O(1).** ~240ms server-side for a repo of any size; refs are
  copied and immutable packs shared.
- Bandwidth numbers are bounded by the client uplink (6.5 MB/s up) and
  the shared-1x CPU on the download path; rerun from a cloud box for
  server ceilings.

One harness lesson baked into the script: generation-speed commit loops
must run with `gc.auto=0` — a detached auto-gc mid-run repacks under
`pack-objects` and kills the push with `Could not read <sha>`.
