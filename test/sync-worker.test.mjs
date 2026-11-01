import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { initOutbox, enqueueOutbox } from "../outbox.mjs";
import { createSyncWorker, calculateBackoff } from "../sync-worker.mjs";

test("calculateBackoff returns exponential values within bounds", () => {
  const d0 = calculateBackoff(0, 1000, 10000);
  assert.ok(d0 >= 1000 && d0 <= 1300);

  const d1 = calculateBackoff(1, 1000, 10000);
  assert.ok(d1 >= 2000 && d1 <= 2500);

  const d10 = calculateBackoff(10, 1000, 5000);
  assert.ok(d10 <= 6000);
});

test("createSyncWorker dispatches outbox operations to client", async () => {
  const db = new DatabaseSync(":memory:");
  initOutbox(db);
  db.exec(`
    CREATE TABLE room_members (
      room_id TEXT NOT NULL,
      node_id TEXT NOT NULL,
      role TEXT NOT NULL,
      joined_at TEXT NOT NULL,
      revoked_at TEXT,
      PRIMARY KEY (room_id, node_id)
    );
  `);

  const mockCalls = [];
  const mockClient = {
    from(table) {
      return {
        upsert: async (data) => {
          mockCalls.push({ type: "from.upsert", table, data });
          return { error: null };
        },
        insert: async (data) => {
          mockCalls.push({ type: "from.insert", table, data });
          return { error: null };
        },
        select: (cols) => ({
          eq: (field, val) => ({
            single: async () => {
              mockCalls.push({ type: "from.select", table, field, val });
              return { data: { team_id: "team-1" }, error: null };
            },
          }),
        }),
      };
    },
    rpc: async (fn, args) => {
      mockCalls.push({ type: "rpc", fn, args });
      return { data: null, error: null };
    },
  };

  // 1. Enqueue room upsert
  enqueueOutbox(db, {
    id: "op-room-1",
    entity: "room",
    action: "upsert",
    payload: {
      roomId: "room-1",
      teamId: "team-1",
      teamName: "Team One",
      leaderNodeId: "node-leader",
      role: "leader",
    },
  });

  // 2. Enqueue member upsert
  enqueueOutbox(db, {
    id: "op-member-1",
    entity: "member",
    action: "upsert",
    payload: {
      roomId: "room-1",
      teamId: "team-1",
      nodeId: "node-member",
      role: "member",
    },
  });

  // 3. Enqueue message
  enqueueOutbox(db, {
    id: "op-msg-1",
    entity: "message",
    action: "insert",
    payload: {
      id: "msg-1",
      roomId: "room-1",
      nodeId: "node-member",
      text: "hello world",
      createdAt: new Date().toISOString(),
    },
  });

  // 4. Enqueue revoke
  enqueueOutbox(db, {
    id: "op-revoke-1",
    entity: "member",
    action: "revoke",
    payload: {
      roomId: "room-1",
      nodeId: "node-member",
    },
  });

  // 5. Enqueue updateRole
  enqueueOutbox(db, {
    id: "op-role-1",
    entity: "member",
    action: "updateRole",
    payload: {
      roomId: "room-1",
      nodeId: "node-member",
      role: "leader",
    },
  });

  // 6. Enqueue updateTeam
  enqueueOutbox(db, {
    id: "op-team-1",
    entity: "team",
    action: "update",
    payload: {
      roomId: "room-1",
      teamName: "Updated Team One",
    },
  });

  const worker = createSyncWorker({ db, client: mockClient, batchSize: 10 });
  const processed = await worker.processBatch();

  assert.equal(processed, 6);
  const remaining = db.prepare("SELECT count(*) as cnt FROM supabase_outbox").get();
  assert.equal(remaining.cnt, 0);

  // Check RPC calls were invoked
  assert.ok(mockCalls.some((c) => c.fn === "create_team_with_room"));
  assert.ok(mockCalls.some((c) => c.fn === "add_team_member"));
  assert.ok(mockCalls.some((c) => c.fn === "remove_team_member"));
  assert.ok(mockCalls.some((c) => c.fn === "transfer_team_leadership"));
  assert.ok(mockCalls.some((c) => c.fn === "rename_team"));
});

test("stale member upsert is cancelled if member was revoked locally", async () => {
  const db = new DatabaseSync(":memory:");
  initOutbox(db);
  db.exec(`
    CREATE TABLE room_members (
      room_id TEXT NOT NULL,
      node_id TEXT NOT NULL,
      role TEXT NOT NULL,
      joined_at TEXT NOT NULL,
      revoked_at TEXT,
      PRIMARY KEY (room_id, node_id)
    );
  `);

  // Insert locally revoked member
  db.prepare(`
    INSERT INTO room_members (room_id, node_id, role, joined_at, revoked_at)
    VALUES ('room-1', 'node-revoked', 'member', datetime('now'), datetime('now'))
  `).run();

  // Enqueue member upsert for the revoked user
  enqueueOutbox(db, {
    id: "op-stale-join",
    entity: "member",
    action: "upsert",
    payload: {
      roomId: "room-1",
      teamId: "team-1",
      nodeId: "node-revoked",
      role: "member",
    },
  });

  const mockCalls = [];
  const mockClient = {
    rpc: async (fn, args) => {
      mockCalls.push({ fn, args });
      return { data: null, error: null };
    },
  };

  const worker = createSyncWorker({ db, client: mockClient });
  await worker.processBatch();

  // Member upsert RPC should NOT have been called
  assert.equal(mockCalls.filter((c) => c.fn === "add_team_member").length, 0);

  // Outbox row was cleaned up (cancelled)
  const remaining = db.prepare("SELECT count(*) as cnt FROM supabase_outbox WHERE id = 'op-stale-join'").get();
  assert.equal(remaining.cnt, 0);
});

test("failing sync retries with exponential backoff and eventually routes to dead-letter", async () => {
  const db = new DatabaseSync(":memory:");
  initOutbox(db);

  enqueueOutbox(db, {
    id: "op-fail-1",
    entity: "room",
    action: "upsert",
    payload: {
      roomId: "room-fail",
      teamId: "team-fail",
      teamName: "Team Fail",
    },
  });

  const failingClient = {
    from() {
      return {
        upsert: async () => ({ error: null }),
      };
    },
    rpc: async () => {
      throw new Error("Network connection failed 500");
    },
  };

  const worker = createSyncWorker({
    db,
    client: failingClient,
    maxRetries: 3,
  });

  // Attempt 1: fail and set status to failed with backoff
  await worker.processBatch();
  let row = db.prepare("SELECT * FROM supabase_outbox WHERE id = 'op-fail-1'").get();
  assert.equal(row.status, "failed");
  assert.equal(row.attempt_count, 1);
  assert.ok(row.last_error.includes("Network connection failed"));

  // Reset next_attempt_at to now for attempt 2
  db.prepare("UPDATE supabase_outbox SET next_attempt_at = datetime('now') WHERE id = 'op-fail-1'").run();
  await worker.processBatch();
  row = db.prepare("SELECT * FROM supabase_outbox WHERE id = 'op-fail-1'").get();
  assert.equal(row.attempt_count, 2);

  // Reset next_attempt_at to now for attempt 3 (reaches maxRetries=3)
  db.prepare("UPDATE supabase_outbox SET next_attempt_at = datetime('now') WHERE id = 'op-fail-1'").run();
  await worker.processBatch();

  // Outbox should now be cleared
  const outboxCount = db.prepare("SELECT count(*) as cnt FROM supabase_outbox WHERE id = 'op-fail-1'").get();
  assert.equal(outboxCount.cnt, 0);

  // Dead letter table should contain the record
  const deadRow = db.prepare("SELECT * FROM supabase_dead_letter WHERE id = 'op-fail-1'").get();
  assert.ok(deadRow);
  assert.equal(deadRow.id, "op-fail-1");
  assert.ok(deadRow.error.includes("Max retries (3) exceeded"));
});

