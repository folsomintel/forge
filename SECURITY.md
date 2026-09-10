# Security Policy

## Reporting a vulnerability

**Do not open a public issue for security problems.**

Report privately via GitHub Security Advisories: on the repository's
**Security → Advisories → Report a vulnerability** page. We aim to acknowledge
within 72 hours and to ship a fix or mitigation before any public disclosure.

Because forge is a git *server* that fronts an object store, we're especially
interested in:

- authentication / authorization bypass (JWT scopes, per-key SSH scopes)
- path traversal or cross-repo access in the pack/ref/blob paths
- SSRF via webhook delivery or repository import
- denial of service (unbounded memory/CPU on a push, fetch, or advertisement)
- durability bugs that could lose an acknowledged push

## Supported versions

Until a 1.0 release, only the latest tagged release receives security fixes.
