# Contributing

Thanks for your interest in forge.

## Development

```sh
go build ./...          # compile
go vet ./...            # static checks
go test ./...           # unit + e2e (the e2e suite runs real git and diffs
                        # forge's output against it — you need git installed)
gofmt -w .              # format before committing
```

The e2e suite in `e2e/` is the contract: it exercises push/fetch/clone/LFS/SSH
against a live server and, for the read paths, asserts **byte-for-byte parity
with stock git**. New protocol or storage behavior should come with an e2e
test; new pack-format code should come with a test that diffs against
`git cat-file`.

## Architecture

Read `ARCHITECTURE.md` first — the whole design turns on one idea: the object
store is the source of truth (a conditional-PUT ref WAL + immutable packs),
and local disk is a disposable cache. `docs/` has the deeper rationale.

## Pull requests

- One logical change per PR; keep the diff focused.
- Match the surrounding code's style and comment density.
- CI must be green (build, vet, gofmt, lint, tests, govulncheck).
- By contributing you agree your work is licensed under AGPL-3.0.

## Reporting bugs / security

Functional bugs: open an issue. Security issues: **do not** open a public
issue — see `SECURITY.md`.
