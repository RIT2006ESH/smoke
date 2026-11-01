# Smoke Room

Smoke Room preserves the original Three.js smoking experience while adding
private, authenticated, room-specific communication for NODES teams.

## Features

- Existing 3D environment, cigarette selection, smoke effects, animation, and
  responsive controls.
- Private rooms keyed by the stable NODES `roomId`.
- Ed25519 JWT verification using the NODES public key.
- Room membership and role checks on every request and WebSocket connection.
- SQLite persistence for local development.
- Optional Supabase persistence shared with NODES.
- Message history, timestamps, online presence, join/leave events, revocation,
  leader changes, and team-name synchronization.
- Room isolation: messages never cross room boundaries.

## Requirements

- Node.js 24 or newer.
- npm.
- The NODES backend and its public verification key for authenticated rooms.
- Optional Supabase project credentials.

## Install and configure

```powershell
cd C:\Users\Asus\Downloads\smoke
npm install
```

Copy the settings from [.env.example](./.env.example) into `.env`:

```text
SMOKE_PORT=10001
DATABASE_PATH=.\data\smoke.db
NODES_PUBLIC_KEY_FILE=C:\Users\Asus\Downloads\NODES-main\data\smoke-nodes-public.pem
NODES_JWT_ISSUER=nodes
NODES_JWT_AUDIENCE=smoke-room
SMOKE_INTERNAL_API_SECRET=local-only-change-me
SUPABASE_URL=https://xusbdawjmbrfztqjfmwh.supabase.co
SUPABASE_PUBLISHABLE_KEY=<publishable-key>
SUPABASE_SECRET_KEY=<backend-only-secret-key>
```

`.env`, SQLite databases, and keys are ignored by Git. Never place
`SUPABASE_SECRET_KEY`, the NODES private key, or the internal secret in
frontend JavaScript.

## Start locally

```powershell
cd C:\Users\Asus\Downloads\smoke
npm start
```

The service runs at <http://localhost:10001> and serves both the static
Three.js client and the local API. Start NODES separately with
`SMOKE_BASE_URL=http://localhost:10001` and
`VITE_SMOKE_BASE_URL=http://localhost:10001`.

Open Smoke through NODES using **OPEN SMOKE ROOM**. A direct visit to
`http://localhost:10001` is the anonymous lounge and does not grant private
room access.

## API

Authenticated:

- `POST /api/v1/rooms/:roomId/authorize`
- `POST /api/v1/realtime/ticket`
- `GET /api/v1/rooms/:roomId/members`
- `GET /api/v1/rooms/:roomId/messages`
- `POST /api/v1/rooms/:roomId/messages`

NODES internal synchronization:

- `POST /api/v1/internal/rooms/:roomId/revoke`
- `POST /api/v1/internal/rooms/:roomId/role`
- `POST /api/v1/internal/rooms/:roomId/team`

Internal routes require `x-smoke-internal-secret` and are not public APIs.
Every authenticated request validates the NODES JWT, including issuer,
audience, expiry, subject, node ID, room ID, role, membership, and revocation.

## Database and Supabase

Local SQLite is the default persistence store at `data/smoke.db`.

The shared Supabase schema is at
[supabase/migrations/20261031000000_shared_nodes_smoke.sql](./supabase/migrations/20261031000000_shared_nodes_smoke.sql).
Apply the matching NODES migration after reviewing
[the duplicate report](../NODES-main/supabase/preflight_duplicate_report.sql).

When `SUPABASE_URL` and `SUPABASE_SECRET_KEY` are configured, Smoke mirrors
rooms, members, roles, team names, revocations, and messages to Supabase.
Without them, local SQLite and WebSockets remain active.

## Testing

```powershell
cd C:\Users\Asus\Downloads\smoke
node --check server.mjs
node --check supabase.mjs
npm test
npm audit --omit=dev
```

The tests cover:

- Authenticated room authorization.
- Invalid and expired JWT rejection.
- Room isolation.
- Persistent history.
- WebSocket message delivery.
- Revocation and idempotent internal events.

The current scalability and mobile audit is documented in
[docs/PRODUCTION_READINESS_AUDIT.md](./docs/PRODUCTION_READINESS_AUDIT.md).
It clearly separates implemented changes from unverified 1,000-user, Supabase
quota, and physical-device requirements.

Manual integration test:

1. Start Smoke and NODES.
2. Join NODES in two browser windows.
3. Connect the two players and create a team.
4. Open Smoke Room from the NODES dashboard in both windows.
5. Send messages and verify both clients receive them.
6. Create a second team and verify it cannot see the first room’s messages.
7. Remove a member and verify their token and WebSocket session are rejected.

## Security

- Smoke receives only the NODES public key.
- NODES private signing keys remain in the NODES backend.
- Supabase secret keys remain backend-only.
- Room access is never granted from URL parameters alone.
- Membership and leadership remain authoritative in NODES.
- Supabase RLS protects exposed tables for publishable-key access.
