#!/usr/bin/env bash
# End-to-end: keygen -> token -> create repo -> clone -> push -> re-clone ->
# CAS rejection -> cache-wipe rebuild (store+DB are the only source of truth).
set -euo pipefail

BIN="$(cd "$(dirname "$0")/.." && pwd)/bin/forged"
WORK="$(mktemp -d)"
export FORGE_DATA_DIR="$WORK/server"
export FORGE_ADDR="127.0.0.1:${FORGE_PORT:-8347}"
BASE="http://$FORGE_ADDR"

cleanup() { kill "$SRV" 2>/dev/null || true; rm -rf "$WORK"; }
trap cleanup EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

say "keygen + serve"
"$BIN" keygen --name e2e > "$WORK/key.pem" 2>/dev/null
"$BIN" serve &>"$WORK/server.log" & SRV=$!
for i in $(seq 1 50); do curl -fsS "$BASE/healthz" &>/dev/null && break; sleep 0.1; done
TOKEN="$("$BIN" token --key "$WORK/key.pem")"

say "create repo"
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -d '{"id":"demo"}' "$BASE/api/repos" | grep -q '"demo"'

say "unauthenticated is rejected"
if curl -fsS "$BASE/api/repos" &>/dev/null; then echo "FAIL: no-auth list succeeded"; exit 1; fi

say "clone empty repo, commit, push"
GIT=(git -c user.email=e2e@test -c user.name=e2e -c init.defaultBranch=main -c advice.detachedHead=false)
cd "$WORK"
"${GIT[@]}" clone -q "http://t:$TOKEN@$FORGE_ADDR/demo.git" c1 2>/dev/null
cd c1
echo "hello forge" > README.md
mkdir -p src && printf 'package main\n' > src/main.go
"${GIT[@]}" add -A && "${GIT[@]}" commit -qm "initial commit"
"${GIT[@]}" push -q origin HEAD:refs/heads/main

say "fresh clone sees the push"
cd "$WORK"
"${GIT[@]}" clone -q "http://t:$TOKEN@$FORGE_ADDR/demo.git" c2
cmp c1/README.md c2/README.md
[ "$(cd c2 && git rev-parse HEAD)" = "$(cd c1 && git rev-parse HEAD)" ]

say "second push + pull"
cd "$WORK/c1"
echo "v2" >> README.md && "${GIT[@]}" commit -qam "second"
"${GIT[@]}" push -q origin main
cd "$WORK/c2" && "${GIT[@]}" pull -q && grep -q v2 README.md

say "non-fast-forward push is rejected by ref CAS"
cd "$WORK/c2"
"${GIT[@]}" reset -q --hard HEAD~1
echo "diverged" >> README.md && "${GIT[@]}" commit -qam "diverge"
if "${GIT[@]}" push -q origin main 2>/dev/null; then echo "FAIL: non-ff push succeeded"; exit 1; fi
"${GIT[@]}" push -q --force origin main   # force push updates DB target, so CAS accepts

say "tag push"
cd "$WORK/c1" && "${GIT[@]}" fetch -q origin && "${GIT[@]}" reset -q --hard origin/main
"${GIT[@]}" tag -a v0.1 -m "release" && "${GIT[@]}" push -q origin v0.1

say "branch delete"
"${GIT[@]}" push -q origin HEAD:refs/heads/scratch
"${GIT[@]}" push -q origin :refs/heads/scratch

say "cache wipe: rebuild purely from store + DB"
rm -rf "$FORGE_DATA_DIR/cache"
cd "$WORK"
"${GIT[@]}" clone -q "http://t:$TOKEN@$FORGE_ADDR/demo.git" c3
grep -q diverged c3/README.md
(cd c3 && git tag -l | grep -q v0.1)

say "repo-scoped token cannot touch other repos"
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -d '{"id":"other"}' "$BASE/api/repos" > /dev/null
SCOPED="$("$BIN" token --key "$WORK/key.pem" --repo demo --scopes 'git:read git:write')"
"${GIT[@]}" clone -q "http://t:$SCOPED@$FORGE_ADDR/demo.git" c4
if "${GIT[@]}" clone -q "http://t:$SCOPED@$FORGE_ADDR/other.git" c5 2>/dev/null; then
  echo "FAIL: repo-scoped token cloned another repo"; exit 1
fi

say "delete repo"
curl -fsS -X DELETE -H "Authorization: Bearer $TOKEN" "$BASE/api/repos/other" -o /dev/null -w '%{http_code}\n' | grep -q 204

echo
echo "ALL E2E TESTS PASSED"
