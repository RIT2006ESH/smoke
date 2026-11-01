import http from "node:http";
import { readFile } from "node:fs/promises";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { extname, join, normalize, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID, timingSafeEqual } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { jwtVerify, importSPKI } from "jose";
import { WebSocketServer } from "ws";

try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}

const { supabase } = await import("./supabase.mjs");
const { initOutbox } = await import("./outbox.mjs");
const { createSyncWorker } = await import("./sync-worker.mjs");
const root = fileURLToPath(new URL(".", import.meta.url));
const port = Number(process.env.SMOKE_PORT || process.env.PORT || 10001);
const issuer = process.env.NODES_JWT_ISSUER || "nodes";
const audience = process.env.NODES_JWT_AUDIENCE || "smoke-room";
const publicKeyCandidates = [
  process.env.NODES_PUBLIC_KEY_FILE,
  resolve(root, "../NODES-main/data/smoke-nodes-public.pem"),
  resolve(root, "data/smoke-nodes-public.pem"),
  resolve(process.cwd(), "data/smoke-nodes-public.pem"),
  resolve(process.cwd(), "../NODES-main/data/smoke-nodes-public.pem"),
].filter(Boolean);
const resolvedPublicKeyFile = publicKeyCandidates.find((f) => existsSync(f));
const publicKeyText = process.env.NODES_PUBLIC_KEY ||
  (resolvedPublicKeyFile ? readFileSync(resolvedPublicKeyFile, "utf8") : "");
const internalSecret = process.env.SMOKE_INTERNAL_API_SECRET || "";
const databasePath = resolve(process.env.DATABASE_PATH || join(root, "data", "smoke.db"));
mkdirSync(dirname(databasePath), { recursive: true });

const db = new DatabaseSync(databasePath);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS rooms (
    room_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    team_name TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS room_members (
    room_id TEXT NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('leader', 'member')),
    joined_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (room_id, node_id)
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    room_id TEXT NOT NULL REFERENCES rooms(room_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    text TEXT NOT NULL CHECK (length(text) BETWEEN 1 AND 140),
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS revoked_tokens (
    jti TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    revoked_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS messages_room_created ON messages(room_id, created_at);
`);
initOutbox(db);

let syncWorker = null;
if (supabase) {
  supabase.setDatabase(db);
  void supabase.verify().catch((error) => {
    console.error("Supabase connection verification failed:", error.message);
  });
  const remoteClient = supabase.getClient ? supabase.getClient() : null;
  if (remoteClient) {
    syncWorker = createSyncWorker({ db, client: remoteClient });
    syncWorker.start();
  }
}

const tickets = new Map();
const sockets = new Map();

function json(res, status, value) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(value));
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  } catch {
    throw new Error("Invalid JSON");
  }
}

function bearer(req) {
  const value = req.headers.authorization || "";
  return value.startsWith("Bearer ") ? value.slice(7) : null;
}

async function verifyNodeToken(token) {
  if (!token || !publicKeyText) throw new Error("Smoke public key is not configured");
  const key = await importSPKI(publicKeyText.replace(/\\n/g, "\n"), "Ed25519");
  const { payload } = await jwtVerify(token, key, { issuer, audience });
  if (typeof payload.nodeId !== "string" || payload.sub !== payload.nodeId ||
      typeof payload.roomId !== "string" || typeof payload.jti !== "string") {
    throw new Error("Invalid NODES token claims");
  }
  const revoked = db.prepare("SELECT 1 FROM revoked_tokens WHERE jti = ?").get(payload.jti);
  if (revoked) throw new Error("Token is revoked");
  return payload;
}

function member(roomId, nodeId) {
  return db.prepare(`
    SELECT room_id, node_id, role, revoked_at
    FROM room_members WHERE room_id = ? AND node_id = ?
  `).get(roomId, nodeId);
}

async function requireMember(req, roomId) {
  const claims = await verifyNodeToken(bearer(req));
  if (claims.roomId !== roomId) throw new Error("Token room mismatch");
  const record = member(roomId, claims.nodeId);
  if (!record || record.revoked_at) throw new Error("Room membership is revoked");
  return { claims, member: record };
}

function authorizeInternal(req) {
  const value = req.headers["x-smoke-internal-secret"];
  if (!internalSecret || !value) return false;
  const provided = Buffer.from(String(value));
  const expected = Buffer.from(internalSecret);
  return provided.length === expected.length && timingSafeEqual(provided, expected);
}

function broadcast(roomId, event) {
  for (const ws of sockets.get(roomId) || []) {
    if (ws.readyState === 1) ws.send(JSON.stringify(event));
  }
}

function online(roomId, nodeId) {
  return [...sockets.get(roomId) || []].some((ws) => ws.nodeId === nodeId);
}

async function api(req, res, pathname, url) {
  if (pathname === "/api/v1/realtime/ticket" && req.method === "POST") {
    try {
      const input = await readJson(req);
      const { claims } = await requireMember(req, String(input.roomId || ""));
      const ticket = randomUUID();
      tickets.set(ticket, { roomId: claims.roomId, nodeId: claims.nodeId, expiresAt: Date.now() + 30_000 });
      return json(res, 201, { ticket, expiresIn: 30 });
    } catch (error) {
      return json(res, 403, { error: error.message });
    }
  }

  const roomMatch = pathname.match(/^\/api\/v1\/rooms\/([^/]+)(?:\/(authorize|members|messages))?$/);
  if (roomMatch) {
    const roomId = decodeURIComponent(roomMatch[1]);
    const action = roomMatch[2];
    try {
      if (action === "authorize" && req.method === "POST") {
        const claims = await verifyNodeToken(bearer(req));
        if (claims.roomId !== roomId) throw new Error("Token room mismatch");
        const existing = member(roomId, claims.nodeId);
        if (existing?.revoked_at) {
          const revokedTime = new Date(existing.revoked_at).getTime();
          const tokenIssuedAt = (Number(claims.iat) || 0) * 1000;
          if (tokenIssuedAt <= revokedTime) throw new Error("Room membership is revoked");
        }
        const now = new Date().toISOString();
        db.prepare(`
          INSERT INTO rooms(room_id, team_id, team_name, created_at) VALUES (?, ?, ?, ?)
          ON CONFLICT(room_id) DO UPDATE SET team_name = excluded.team_name
        `).run(roomId, String(claims.teamId || roomId), String(claims.teamName || roomId), now);
        if (supabase) {
          await supabase.upsertRoom(
            roomId,
            String(claims.teamId || roomId),
            String(claims.teamName || roomId),
            claims.nodeId,
            claims.role === "leader" ? "leader" : "member",
          );
          await supabase.upsertMember(roomId, String(claims.teamId || roomId), claims.nodeId, claims.role === "leader" ? "leader" : "member");
        }
        db.prepare(`
          INSERT INTO room_members(room_id, node_id, role, joined_at, revoked_at)
          VALUES (?, ?, ?, ?, NULL)
          ON CONFLICT(room_id, node_id) DO UPDATE SET role = excluded.role, revoked_at = NULL
        `).run(roomId, claims.nodeId, claims.role === "leader" ? "leader" : "member", now);
        return json(res, 200, {
          authorized: true, roomId, teamName: claims.teamName, nodeId: claims.nodeId,
          role: claims.role === "leader" ? "leader" : "member",
          members: db.prepare("SELECT node_id, role FROM room_members WHERE room_id = ? AND revoked_at IS NULL").all(roomId)
            .map((m) => ({ nodeId: m.node_id, role: m.role, online: online(roomId, m.node_id) })),
          realtime: { transport: "websocket", url: "/ws" },
        });
      }

      const { claims } = await requireMember(req, roomId);
      if (action === "members" && req.method === "GET") {
        const rows = db.prepare("SELECT node_id, role FROM room_members WHERE room_id = ? AND revoked_at IS NULL").all(roomId);
        return json(res, 200, { roomId, members: rows.map((m) => ({
          nodeId: m.node_id, role: m.role, online: online(roomId, m.node_id),
        })) });
      }
      if (action === "messages" && req.method === "GET") {
        const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 50)));
        const rows = db.prepare(`
          SELECT id, node_id, text, created_at FROM messages
          WHERE room_id = ? ORDER BY created_at DESC LIMIT ?
        `).all(roomId, limit).reverse();
        return json(res, 200, { roomId, messages: rows.map((m) => ({
          id: m.id, nodeId: m.node_id, text: m.text, createdAt: m.created_at,
        })), nextCursor: null });
      }
      if (action === "messages" && req.method === "POST") {
        const input = await readJson(req);
        const text = typeof input.text === "string" ? input.text.trim() : "";
        if (!text || text.length > 140) return json(res, 400, { error: "Message must be 1-140 characters" });
        const message = { id: randomUUID(), roomId, nodeId: claims.nodeId, text, createdAt: new Date().toISOString() };
        db.prepare("INSERT INTO messages(id, room_id, node_id, text, created_at) VALUES (?, ?, ?, ?, ?)")
          .run(message.id, roomId, message.nodeId, message.text, message.createdAt);
        if (supabase) await supabase.insertMessage(message);
        broadcast(roomId, { t: "room:message", roomId, message });
        return json(res, 201, { success: true, message });
      }
    } catch (error) {
      const status = action === "authorize" || /Unauthorized|revoked|mismatch|Token|membership|configured|signature|expired/.test(error.message) ? 403 : 500;
      return json(res, status, { error: error.message });
    }
  }

  const internal = pathname.match(/^\/api\/v1\/internal\/rooms\/([^/]+)\/(revoke|role|team)$/);
  if (internal && req.method === "POST") {
    if (!authorizeInternal(req)) return json(res, 401, { error: "Unauthorized internal request" });
    const roomId = decodeURIComponent(internal[1]);
    const input = await readJson(req);
    const eventId = String(input.eventId || "");
    if (!eventId) return json(res, 400, { error: "eventId is required" });
    if (db.prepare("SELECT 1 FROM processed_events WHERE event_id = ?").get(eventId)) return json(res, 200, { success: true, duplicate: true });
    const now = new Date().toISOString();
    try {
      if (internal[2] === "revoke") {
        db.prepare("UPDATE room_members SET revoked_at = ? WHERE room_id = ? AND node_id = ?").run(now, roomId, input.nodeId);
        if (input.jti) db.prepare("INSERT OR IGNORE INTO revoked_tokens(jti, node_id, room_id, revoked_at) VALUES (?, ?, ?, ?)").run(input.jti, input.nodeId, roomId, now);
        broadcast(roomId, { t: "room:member_removed", roomId, nodeId: input.nodeId, reason: input.reason });
        for (const ws of sockets.get(roomId) || []) if (ws.nodeId === input.nodeId) ws.close(4003, "membership revoked");
        if (supabase) await supabase.revokeMember(roomId, String(input.nodeId));
      } else if (internal[2] === "role") {
        db.prepare("UPDATE room_members SET role = ? WHERE room_id = ? AND node_id = ?").run(input.role === "leader" ? "leader" : "member", roomId, input.nodeId);
        broadcast(roomId, { t: "room:role_updated", roomId, nodeId: input.nodeId, role: input.role });
        if (supabase) await supabase.updateRole(roomId, String(input.nodeId), input.role === "leader" ? "leader" : "member");
      } else {
        db.prepare("UPDATE rooms SET team_name = ? WHERE room_id = ?").run(String(input.teamName || ""), roomId);
        broadcast(roomId, { t: "room:team_updated", roomId, teamName: input.teamName });
        if (supabase) await supabase.updateTeam(roomId, String(input.teamName || ""));
      }
      db.prepare("INSERT INTO processed_events(event_id, processed_at) VALUES (?, ?)").run(eventId, now);
      return json(res, 200, { success: true });
    } catch (error) {
      return json(res, 500, { error: error.message });
    }
  }
  return false;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (url.pathname.startsWith("/api/")) {
    const handled = await api(req, res, url.pathname, url);
    if (handled !== false) return;
  }
  if (url.pathname === "/healthz") return json(res, 200, { ok: true, database: databasePath });
  const requested = normalize(url.pathname === "/" ? "/index.html" : url.pathname);
  const file = join(root, requested);
  if (!file.startsWith(root)) return res.writeHead(404).end();
  try {
    const content = await readFile(file);
    const type = {
      ".html": "text/html; charset=utf-8",
      ".js": "text/javascript; charset=utf-8",
      ".css": "text/css; charset=utf-8",
      ".json": "application/json; charset=utf-8",
      ".svg": "image/svg+xml",
      ".glb": "model/gltf-binary",
      ".png": "image/png",
    }[extname(file).toLowerCase()] || "application/octet-stream";
    res.writeHead(200, { "content-type": type });
    res.end(content);
  } catch { res.writeHead(404).end("Not found"); }
});

const wss = new WebSocketServer({ noServer: true });
server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url || "/", "http://localhost");
  if (url.pathname !== "/ws") return socket.destroy();
  const session = tickets.get(url.searchParams.get("ticket"));
  if (!session || session.expiresAt < Date.now()) return socket.destroy();
  tickets.delete(url.searchParams.get("ticket"));
  const active = member(session.roomId, session.nodeId);
  if (!active || active.revoked_at) return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.roomId = session.roomId;
    ws.nodeId = session.nodeId;
    if (!sockets.has(ws.roomId)) sockets.set(ws.roomId, new Set());
    sockets.get(ws.roomId).add(ws);
    ws.send(JSON.stringify({ t: "room:joined", roomId: ws.roomId, nodeId: ws.nodeId }));
    broadcast(ws.roomId, { t: "room:member_joined", roomId: ws.roomId, nodeId: ws.nodeId });
    ws.on("message", (raw) => {
      let data;
      try { data = JSON.parse(String(raw)); } catch { return; }
      if (data.t === "msg" && typeof data.text === "string") {
        const text = data.text.trim();
        if (!text || text.length > 140) return;
        const message = { id: randomUUID(), roomId: ws.roomId, nodeId: ws.nodeId, text, createdAt: new Date().toISOString() };
        db.prepare("INSERT INTO messages(id, room_id, node_id, text, created_at) VALUES (?, ?, ?, ?, ?)").run(message.id, ws.roomId, message.nodeId, message.text, message.createdAt);
        if (supabase) void supabase.insertMessage(message).catch(() => {});
        broadcast(ws.roomId, { t: "room:message", roomId: ws.roomId, message });
      }
    });
    ws.on("close", () => {
      sockets.get(ws.roomId)?.delete(ws);
      broadcast(ws.roomId, { t: "room:member_left", roomId: ws.roomId, nodeId: ws.nodeId });
    });
  });
});

server.listen(port, () => console.log(`Smoke Room local backend listening on http://localhost:${port}`));
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    if (syncWorker) syncWorker.stop();
    for (const room of sockets.values()) for (const ws of room) ws.close();
    db.close();
    server.close(() => process.exit(0));
  });
}
