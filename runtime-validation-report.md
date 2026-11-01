# Runtime Validation Report

**Generated**: 2026-10-31T03:18:00+05:30  
**Target**: `C:\Users\Asus\Downloads\smoke`

## Summary

| Step | Status | Exit Code | Details |
|---|---:|---:|---|
| Startup | PASS | 0 | Smoke `/healthz` returned 200 on port 10001 |
| Integration tests | PASS | 0 | 3 test files, 3 tests passed |
| NODES validation | PASS | 0 | Typecheck, backend tests, and production build passed |
| Dependency audit | PASS | 0 | `npm audit --omit=dev`: 0 vulnerabilities |

## Test evidence

- `test/rooms.test.mjs`: two authenticated members, isolated rooms, persisted
  message history, and WebSocket message broadcast.
- `test/auth.test.mjs`: missing and expired Ed25519 JWT rejection.
- `test/admin.test.mjs`: authenticated revocation, access loss, and idempotent
  internal event handling.

## Environment

- Docker: not required; SQLite is embedded locally.
- Node.js: available (`v24.14.1`).
- Browser E2E: not run; the acceptance tests exercise the backend and WebSocket
  boundary directly. The existing Three.js client remains unchanged except for
  room metadata and message routing.

## Remaining verification

The final manual journey still requires two browser clients to establish a
fresh NODES mesh, obtain NODES-issued tokens, open `http://localhost:10001`,
and exchange messages. The local service and contract tests are passing, but
this manual multi-browser journey is not represented as a completed claim here.
