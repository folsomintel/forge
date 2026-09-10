#!/usr/bin/env bash
# Git performance benchmark - same workload against any provider:
#
#   PROVIDER=forge   FORGE_URL=https://host FORGE_TOKEN=jwt scripts/bench-git.sh
#   PROVIDER=github  scripts/bench-git.sh            # uses gh auth; BENCH_OWNER overrides
#   PROVIDER=gitlab  GITLAB_TOKEN=glpat-… scripts/bench-git.sh   # GITLAB_HOST default gitlab.com
#   PROVIDER=generic BENCH_URL_SMALL=… BENCH_URL_MED=… BENCH_URL_BIG=… scripts/bench-git.sh
#                    # pre-created empty repos, token embedded in the URLs
#                    # (this is how you point it at Cursor origin or anything else)
#
# Workload: ref advertisement, no-op fetch, API reads, sequential and
# concurrent small pushes, tag pushes, full/shallow/partial clones,
# anonymous public reads, and a large-blob bandwidth pass. Provider
# extras (Forge: zero-copy fork, ephemeral namespace) run when supported
# and print n/a elsewhere. BENCH_BLOB_MB sizes the bandwidth blob
# (default 95 - GitHub hard-blocks files over 100MiB).
set -euo pipefail

PROVIDER="${PROVIDER:-forge}"
BLOB_MB="${BENCH_BLOB_MB:-95}"
WORK="$(mktemp -d)"
# gc.auto=0: rapid generation commits otherwise spawn a detached auto-gc
# that repacks under pack-objects mid-push ("Could not read <sha>").
GIT=(git -c user.email=bench@forge -c user.name=bench -c init.defaultBranch=main -c advice.detachedHead=false -c gc.auto=0 -c gc.autodetach=false)
RESULTS="$WORK/results.tsv"
trap 'rm -rf "$WORK"' EXIT

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
note() { echo "-- $*" >&2; }
na() { echo -e "$1\tn/a\t$2\t" >> "$RESULTS"; }

# time_runs LABEL N CMD... -> median/p95 over N runs (ms)
time_runs() {
  local label="$1" n="$2"; shift 2
  local times=() t0 t1
  for _ in $(seq 1 "$n"); do
    t0=$(now_ms)
    if ! "$@" >"$WORK/last.out" 2>&1; then
      echo "FAILED during '$label':" >&2
      tail -20 "$WORK/last.out" >&2
      exit 1
    fi
    t1=$(now_ms)
    times+=($((t1 - t0)))
  done
  python3 - "$label" "$n" "${times[@]}" <<'PY' >> "$RESULTS"
import sys
label, n, ts = sys.argv[1], sys.argv[2], sorted(map(int, sys.argv[3:]))
med = ts[len(ts)//2]; p95 = ts[min(len(ts)-1, int(len(ts)*0.95))]
print(f"{label}\t{med} ms\tp95 {p95} ms\tn={n}")
PY
}

# ---- provider adapters -----------------------------------------------------
# Each provider defines: p_create R, p_delete R, p_url R (authed remote),
# p_ping (network-floor probe), p_cleanup_list, and optionally
# p_make_public R + p_anon_url R, p_tree R + p_list_api,
# p_fork SRC DST (CAP_FORK=1), CAP_EPH=1 (Forge ephemeral view).

HAS_API=1
HAS_PUBLIC=1
case "$PROVIDER" in
forge)
  : "${FORGE_URL:?set FORGE_URL}"; : "${FORGE_TOKEN:?set FORGE_TOKEN}"
  HOST="${FORGE_URL#https://}"
  CAP_FORK=1 CAP_EPH=1
  api() { curl -fsS -H "Authorization: Bearer $FORGE_TOKEN" -H "Content-Type: application/json" "$@"; }
  p_ping() { curl -fsS "$FORGE_URL/healthz"; }
  p_create() { api -X POST -d "{\"id\":\"$1\"}" "$FORGE_URL/api/repos"; }
  p_delete() { api -X DELETE "$FORGE_URL/api/repos/$1"; }
  p_url() { echo "https://t:${FORGE_TOKEN}@${HOST}/$1.git"; }
  p_anon_url() { echo "$FORGE_URL/$1.git"; }
  p_make_public() { api -X PATCH -d '{"public":true}' "$FORGE_URL/api/repos/$1"; }
  p_tree() { api "$FORGE_URL/api/repos/$1/contents"; }
  p_list_api() { api "$FORGE_URL/api/repos"; }
  p_fork() { api -X POST -d "{\"id\":\"$2\"}" "$FORGE_URL/api/repos/$1/fork"; }
  p_cleanup_list() { p_list_api | python3 -c 'import json,sys
d = json.load(sys.stdin); rs = d["repos"] if isinstance(d, dict) else d
for x in rs:
    rid = x["id"] if isinstance(x, dict) else x
    if rid.startswith("bench-"): print(rid)'; }
  ;;
github)
  OWNER="${BENCH_OWNER:-$(gh api user -q .login)}"
  GHTOKEN="$(gh auth token)"
  CAP_FORK=0 CAP_EPH=0   # can't fork your own repo into the same account
  p_ping() { curl -fsS -H "Authorization: Bearer $GHTOKEN" https://api.github.com/rate_limit; }
  p_create() { gh repo create "$OWNER/$1" --private >/dev/null 2>&1 || true; }  # tolerate leftovers
  p_delete() { gh repo delete "$OWNER/$1" --yes 2>/dev/null || note "could not delete $1 (token may lack delete_repo scope) - clean up manually"; }
  p_url() { echo "https://x-access-token:${GHTOKEN}@github.com/$OWNER/$1.git"; }
  p_anon_url() { echo "https://github.com/$OWNER/$1.git"; }
  p_make_public() { gh repo edit "$OWNER/$1" --visibility public --accept-visibility-change-consequences 2>/dev/null || gh repo edit "$OWNER/$1" --visibility public; }
  p_tree() { gh api "repos/$OWNER/$1/contents"; }
  p_list_api() { gh api "user/repos?per_page=10"; }
  p_cleanup_list() { gh repo list "$OWNER" --json name -q '.[].name' | grep '^bench-' || true; }
  ;;
gitlab)
  : "${GITLAB_TOKEN:?set GITLAB_TOKEN (api scope)}"
  GL_HOST="${GITLAB_HOST:-gitlab.com}"
  GL_API="https://$GL_HOST/api/v4"
  glapi() { curl -fsS -H "PRIVATE-TOKEN: $GITLAB_TOKEN" "$@"; }
  GL_NS="$(glapi "$GL_API/user" | python3 -c 'import json,sys; print(json.load(sys.stdin)["username"])')"
  CAP_FORK=0 CAP_EPH=0   # same-namespace fork isn't allowed
  enc() { python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$GL_NS/$1"; }
  p_ping() { glapi "$GL_API/version"; }
  p_create() { glapi -X POST --data-urlencode "name=$1" --data-urlencode "visibility=private" "$GL_API/projects" >/dev/null 2>&1 || true; }
  p_delete() { glapi -X DELETE "$GL_API/projects/$(enc "$1")"; }
  p_url() { echo "https://oauth2:${GITLAB_TOKEN}@$GL_HOST/$GL_NS/$1.git"; }
  p_anon_url() { echo "https://$GL_HOST/$GL_NS/$1.git"; }
  p_make_public() { glapi -X PUT --data-urlencode "visibility=public" "$GL_API/projects/$(enc "$1")" >/dev/null; }
  p_tree() { glapi "$GL_API/projects/$(enc "$1")/repository/tree"; }
  p_list_api() { glapi "$GL_API/projects?membership=true&per_page=10"; }
  p_cleanup_list() { glapi "$GL_API/projects?membership=true&search=bench-&per_page=50" | python3 -c 'import json,sys
for p in json.load(sys.stdin):
    if p["path"].startswith("bench-"): print(p["path"])'; }
  ;;
generic)
  # Pre-created empty repos; authed remote URLs supplied by you. This is
  # how you point the harness at Cursor origin or any git-over-HTTPS host
  # without knowing its management API.
  : "${BENCH_URL_SMALL:?set BENCH_URL_SMALL}"; : "${BENCH_URL_MED:?set BENCH_URL_MED}"; : "${BENCH_URL_BIG:?set BENCH_URL_BIG}"
  CAP_FORK=0 CAP_EPH=0 HAS_API=0 HAS_PUBLIC=0
  p_ping() { git ls-remote "$BENCH_URL_SMALL" HEAD >/dev/null; }
  p_create() { :; }
  p_delete() { :; }
  p_url() { case "$1" in bench-small) echo "$BENCH_URL_SMALL";; bench-med) echo "$BENCH_URL_MED";; bench-big) echo "$BENCH_URL_BIG";; esac; }
  p_cleanup_list() { :; }
  ;;
*) echo "unknown PROVIDER=$PROVIDER" >&2; exit 1 ;;
esac

note "provider=$PROVIDER blob=${BLOB_MB}MB"

# ---- setup: repos (idempotent - clear leftovers from aborted runs) --------
for r in $(p_cleanup_list); do p_delete "$r" >/dev/null 2>&1 || true; done
for r in bench-small bench-med bench-big; do p_create "$r" >/dev/null; done

note "generating small repo (100 commits, 20 files)"
mkdir -p "$WORK/small" && cd "$WORK/small" && "${GIT[@]}" init -q
for i in $(seq 1 20); do echo "seed $i" > "f$i.txt"; done
"${GIT[@]}" add -A && "${GIT[@]}" commit -qm seed
for i in $(seq 1 99); do
  echo "$i" >> "f$((i % 20 + 1)).txt"
  "${GIT[@]}" commit -qam "change $i"
done
"${GIT[@]}" push -qf "$(p_url bench-small)" HEAD:refs/heads/main

note "generating medium repo (1000 commits, 200 files, mixed sizes)"
mkdir -p "$WORK/med" && cd "$WORK/med" && "${GIT[@]}" init -q
for i in $(seq 1 200); do head -c 2048 /dev/urandom | base64 > "src$i.txt"; done
head -c 3145728 /dev/urandom > big1.bin
head -c 3145728 /dev/urandom > big2.bin
"${GIT[@]}" add -A && "${GIT[@]}" commit -qm seed
for i in $(seq 1 999); do
  echo "$i $RANDOM" >> "src$((i % 200 + 1)).txt"
  if [ $((i % 100)) = 0 ]; then head -c 1048576 /dev/urandom > "blob$i.bin"; "${GIT[@]}" add -A; fi
  "${GIT[@]}" commit -qam "change $i"
done
"${GIT[@]}" tag v1.0 && "${GIT[@]}" tag v1.1
MED_SIZE=$(du -sk .git | cut -f1)
note "medium repo .git = ${MED_SIZE}KB"
t0=$(now_ms)
"${GIT[@]}" push -qf "$(p_url bench-med)" HEAD:refs/heads/main --tags
t1=$(now_ms)
echo -e "push medium repo (initial, $((MED_SIZE/1024))MB)\t$((t1 - t0)) ms\t\tn=1" >> "$RESULTS"

# ---- latency -------------------------------------------------------------
cd "$WORK"
time_runs "ping (network floor)" 10 p_ping
time_runs "ls-remote (ref advertisement)" 10 "${GIT[@]}" ls-remote "$(p_url bench-med)"
"${GIT[@]}" clone -q "$(p_url bench-small)" uptodate
time_runs "no-op fetch (up to date)" 10 git -C uptodate fetch
if [ "$HAS_API" = 1 ]; then
  time_runs "API: list repos" 10 p_list_api
  time_runs "API: tree listing (med root)" 10 p_tree bench-med
else
  na "API: list repos" "no management API"
  na "API: tree listing (med root)" "no management API"
fi

# ---- writes --------------------------------------------------------------
note "sequential small pushes"
cd "$WORK/small"
push_one() {
  echo "x$RANDOM" >> f1.txt
  "${GIT[@]}" commit -qam bump
  "${GIT[@]}" push -q "$(p_url bench-small)" HEAD:refs/heads/main
}
time_runs "small push (1 commit, sequential)" 10 push_one

note "concurrent pushes: 8 writers x 3 pushes, distinct branches"
cd "$WORK"
for w in $(seq 1 8); do
  "${GIT[@]}" clone -q "$(p_url bench-small)" "w$w" 2>/dev/null
done
t0=$(now_ms)
pids=()
for w in $(seq 1 8); do
  (
    cd "w$w"
    for c in 1 2 3; do
      echo "$w-$c" >> f2.txt
      "${GIT[@]}" commit -qam "w$w c$c"
      "${GIT[@]}" push -q origin "HEAD:refs/heads/writer-$w" 2>/dev/null
    done
  ) & pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done
t1=$(now_ms)
python3 -c "d=$((t1-t0)); print(f'concurrent pushes (8 writers x 3)\t{d} ms\t{24000/d:.1f} pushes/s\tn=24')" >> "$RESULTS"

cd "$WORK/small"
if [ "${CAP_EPH:-0}" = 1 ]; then
  note "ephemeral-namespace push"
  eph_push() {
    echo "e$RANDOM" >> f3.txt
    "${GIT[@]}" commit -qam eph
    "${GIT[@]}" push -q "$(p_url bench-small+ephemeral)" "HEAD:refs/heads/scratch-$RANDOM$RANDOM"
  }
  time_runs "ephemeral branch push" 5 eph_push
else
  na "ephemeral branch push" "forge-only"
fi
tag_push() {
  "${GIT[@]}" tag "bench-$RANDOM-$RANDOM"
  "${GIT[@]}" push -q "$(p_url bench-small)" --tags
}
time_runs "tag push" 5 tag_push

# ---- clones --------------------------------------------------------------
cd "$WORK"
clone_med() { rm -rf cm; "${GIT[@]}" clone -q "$(p_url bench-med)" cm; }
clone_med_shallow() { rm -rf cs; "${GIT[@]}" clone -q --depth 1 "$(p_url bench-med)" cs; }
clone_med_partial() { rm -rf cp1; "${GIT[@]}" clone -q --filter=blob:none "$(p_url bench-med)" cp1; }
clone_small() { rm -rf csm; "${GIT[@]}" clone -q "$(p_url bench-small)" csm; }
time_runs "clone small (100 commits)" 3 clone_small
time_runs "clone medium (1000 commits, full)" 3 clone_med
time_runs "clone medium (shallow, depth=1)" 3 clone_med_shallow
time_runs "clone medium (partial, blob:none)" 3 clone_med_partial

# ---- fork ----------------------------------------------------------------
if [ "${CAP_FORK:-0}" = 1 ]; then
  fork_one() { p_fork bench-med "bench-fork-$RANDOM$RANDOM"; }
  time_runs "fork medium repo (API)" 5 fork_one
else
  na "fork medium repo (API)" "same-account fork unsupported"
fi

# ---- anonymous public read ----------------------------------------------
if [ "$HAS_PUBLIC" = 1 ]; then
  p_make_public bench-small >/dev/null
  anon_clone() { rm -rf anon; "${GIT[@]}" clone -q "$(p_anon_url bench-small)" anon; }
  time_runs "anonymous ls-remote (public)" 5 "${GIT[@]}" ls-remote "$(p_anon_url bench-small)"
  time_runs "anonymous clone (public, small)" 3 anon_clone
else
  na "anonymous ls-remote (public)" "no visibility API"
  na "anonymous clone (public, small)" "no visibility API"
fi

# ---- bandwidth: large blob -------------------------------------------------
note "bandwidth: ${BLOB_MB}MB blob"
mkdir -p "$WORK/bigrepo" && cd "$WORK/bigrepo" && "${GIT[@]}" init -q
head -c $((BLOB_MB * 1048576)) /dev/urandom > blob.bin
"${GIT[@]}" add -A && "${GIT[@]}" commit -qm blob
t0=$(now_ms); "${GIT[@]}" push -qf "$(p_url bench-big)" HEAD:refs/heads/main; t1=$(now_ms)
python3 -c "d=$((t1-t0)); print(f'push ${BLOB_MB}MB blob\t{d} ms\t{$BLOB_MB*1000/d:.1f} MB/s up\tn=1')" >> "$RESULTS"
cd "$WORK"
t0=$(now_ms); "${GIT[@]}" clone -q "$(p_url bench-big)" bigclone; t1=$(now_ms)
python3 -c "d=$((t1-t0)); print(f'clone ${BLOB_MB}MB blob\t{d} ms\t{$BLOB_MB*1000/d:.1f} MB/s down\tn=1')" >> "$RESULTS"

# ---- cleanup ---------------------------------------------------------------
note "cleanup"
for r in $(p_cleanup_list); do p_delete "$r" >/dev/null 2>&1 || true; done
for r in bench-small bench-med bench-big; do p_delete "$r" >/dev/null 2>&1 || true; done

echo
echo "== RESULTS (provider=$PROVIDER) =="
column -t -s $'\t' "$RESULTS"
