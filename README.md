# forge

A fast, headless Git server backed by S3.

Host repositories in your own bucket. Push and clone with standard Git clients,
or work with files, commits, and branches through the API. Runs as a single Go
binary.

## Run it

```
docker run -p 8347:8347 -v forge-data:/data ghcr.io/folsomintel/forge
```

or build it:

```
go build ./cmd/forged && ./forged serve
```

Local disk is the default store. To use S3 / R2 / Tigris / MinIO:

```
FORGE_STORE=s3 FORGE_S3_ENDPOINT=… FORGE_S3_BUCKET=… \
FORGE_S3_ACCESS_KEY=… FORGE_S3_SECRET_KEY=… forged serve
```

Register a key, mint a token, clone:

```
forged keygen --name laptop
TOKEN=$(forged token --key laptop --scopes 'git:read git:write repo:write')
git clone http://t:$TOKEN@localhost:8347/hello.git
```

## Protocol

Smart HTTP (v0 and v2), SSH, LFS, partial clone (`--filter`), and bundle-uri
including incremental bundle chains. SHA-1 repositories only — no SHA-256 yet.

There's also a REST API (contents, trees, commits, diffs, branches, tags,
webhooks) and a Server-Sent Events stream of ref updates.

## How it works

- Every push becomes one immutable `pack`+`idx` in the store. No loose objects.
- Refs live in a write-ahead log as compare-and-swap entries; a push's ref
  updates commit atomically. The pre-receive hook makes the pack and the log
  entry durable in the bucket before the push is acknowledged.
- Reads run against a cache repo materialized from the store on demand.
  Receive, fetch, ref advertisement, and the REST read paths are written in Go
  with native delta and thin-pack resolution, so they don't fork git; git is
  used for repacking and as a fallback.
- Maintenance publishes clone and catch-up bundles as static objects. A read
  replica can serve a repo larger than its disk: it keeps pack indexes local
  and reads pack data from the bucket in blocks.

See `ARCHITECTURE.md` for the full design.

## Git on object storage

Forge implements common Git operations in Go to avoid starting a Git process
for each request and to read packs directly from the bucket.

- Ref advertisements (`info/refs`) read from the metadata index without loading
  the repository into the local cache.
- Pushes use a Go packfile parser that resolves deltas and thin packs.
- Fetches assemble incremental packs from previous pushes. Full clones can
  stream packs directly from the bucket.
- Object reads use Go tree and commit parsers with a pool of `git cat-file`
  processes. On replicas, a pack reader can fetch blocks from the bucket and
  resolve objects, including deltas, without downloading entire packs to disk.

Git still handles pack generation and repacking. Requests the Go paths cannot
handle fall back to Git.

## Configuration

Everything is set through `FORGE_*` environment variables. The common ones:

| Variable | Default | Meaning |
|---|---|---|
| `FORGE_ADDR` | `127.0.0.1:8347` | HTTP listen address |
| `FORGE_SSH_ADDR` | off | git-over-SSH listen address |
| `FORGE_STORE` | `local` | `local` or `s3` |
| `FORGE_STORE_PATH`, `FORGE_S3_*` | | store location and credentials |
| `FORGE_DATA_DIR` | | cache and SQLite index (disposable) |
| `FORGE_PUBLIC_URL` | | enables bundle-uri clone offload |
| `FORGE_RATE_API`, `FORGE_RATE_GIT` | | per-subject rate limits |
| `FORGE_REPLICA` | `false` | read-only follower |
| `FORGE_REMOTE_PLACEMENT_BYTES` | `0` | on a replica, serve repos over N bytes from the bucket |

## Status

Pre-1.0. The design and hot paths are covered by an e2e suite that runs real
git and checks forge's read output byte-for-byte against it, but forge has not
been run in production or benchmarked on a large monorepo.

## License

AGPL-3.0 (see `LICENSE`). If you run a modified forge as a network service, you
must offer its users the modified source.
