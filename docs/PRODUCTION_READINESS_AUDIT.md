# Smoke Room production-readiness audit

## Scope

This audit covers the current local Smoke Room implementation, its NODES
integration, Supabase adapter, mobile layout, and available validation tools.
It does not claim that a 1,000-user load test has been completed.

## Current architecture

- Static Three.js client in `index.html`.
- Node.js HTTP server in `server.mjs`.
- Local SQLite persistence for rooms, membership, messages, revocations, and
  idempotent internal events.
- WebSocket room fanout with one connection per authenticated browser.
- NODES-issued Ed25519 JWTs for room authorization.
- Optional Supabase service-key adapter for persistent shared records.
- NODES remains authoritative for membership, leadership, revocation, and team
  identity.

## Findings

### Implemented

- JWT issuer, audience, expiry, subject, room, membership, and revocation
  validation.
- Room-isolated message history and WebSocket broadcasts.
- Member join/leave notifications.
- Internal revocation, role, and team-name synchronization.
- SQLite persistence and restart-safe room history.
- Shared Supabase schema, unique normalized username/team indexes, and RLS
  migration files.
- Backend-only Supabase secret loading.
- Mobile selection carousel and bottom-sheet controls.
- Mobile viewport-safe spacing and reduced pixel-ratio rendering.
- Three.js rendering pauses while the document is hidden.

### Not yet verified

- Supabase migration has not been applied to the remote project.
- No remote `player_profiles` or room tables were present during the last safe
  REST check.
- `SUPABASE_SECRET_KEY` is not available in the normal test environment.
- Supabase Realtime quotas and private-channel authorization have not been
  measured.
- No 100-, 500-, or 1,000-authenticated-user test has been run.
- No one-hour stability test has been run.
- No physical iPhone or Android device verification has been run.
- Playwright, k6, and distributed load-generation tooling are not installed.
- Docker daemon is unavailable on the current Windows environment.

## Capacity risks

The current WebSocket server stores connection sets in one Node process. It is
appropriate for local development and small deployments, but it is not evidence
of 1,000-user production capacity. A production 1,000-user test requires a
managed realtime service or a horizontally scalable WebSocket layer, connection
limits, backpressure, metrics, and an isolated staging project.

## Validation baseline

Smoke:

```text
3 tests passed
```

NODES:

```text
41 focused tests passed
TypeScript check passed
```

The existing tests validate behavior and security boundaries, not high-scale
capacity or physical-device performance.

## Recommended next gates

1. Apply and review the duplicate-data SQL report in the Supabase SQL Editor.
2. Apply the shared migration and rerun REST health/table checks.
3. Add Playwright mobile tests at 320, 390, 768, and 1440 widths.
4. Add k6 WebSocket scenarios for staged 10/100/500/1,000-user tests.
5. Measure Supabase Realtime and database quotas before any 1,000-user run.
6. Run browser and device tests, then record actual Core Web Vitals.
