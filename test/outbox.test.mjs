import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initOutbox, enqueueOutbox, getOutboxEntry, validatePayload, OUTBOX_STATUS } from "../outbox.mjs";
import { createSupabaseAdapter } from "../supabase.mjs";

test("1. Successful outbox insertion", async () => {
  const dir = await mkdtemp(join(tmpdir(), "smoke-outbox-test-"));
  const dbPath = join(dir, "test.db");
  const db = new DatabaseSync(dbPath);
  try {
    initOutbox(db);
    const result = enqueueOutbox(db, {
      id: "op-test-1",
      entity: "room",
      action: "upsert",
      payload: { roomId: "mesh-alpha", teamId: "team-alpha", teamName: "Team Alpha" },
    });

    assert.equal(result.queued, true);
    assert.equal(result.synchronized, false);
    assert.equal(result.operationId, "op-test-1");
    assert.equal(result.status, OUTBOX_STATUS.PENDING);
    assert.ok(result.createdAt);

    const stored = getOutboxEntry(db, "op-test-1");
    assert.ok(stored);
    assert.equal(stored.id, "op-test-1");
    assert.equal(stored.entity, "room");
    assert.equal(stored.action, "upsert");
    assert.equal(stored.status, "pending");
    assert.equal(stored.attempt_count, 0);
    assert.equal(stored.last_error, null);
    assert.deepEqual(stored.payload, { roomId: "mesh-alpha", teamId: "team-alpha", teamName: "Team Alpha" });
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("2. Duplicate operation ID rejection", async () => {
  const dir = await mkdtemp(join(tmpdir(), "smoke-outbox-test-"));
  const db = new DatabaseSync(join(dir, "test.db"));
  try {
    initOutbox(db);
    enqueueOutbox(db, {
      id: "op-dup-1",
      entity: "room",
      action: "upsert",
      payload: { roomId: "room-dup", teamId: "team-dup", teamName: "Team Dup" },
    });

    assert.throws(
      () => {
        enqueueOutbox(db, {
          id: "op-dup-1",
          entity: "room",
          action: "upsert",
          payload: { roomId: "room-dup-different", teamId: "team-dup", teamName: "Team Dup New" },
        });
      },
      (err) => err instanceof Error && err.message.includes("Duplicate operation ID: op-dup-1")
    );
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("3. Persistence after SQLite database close and reopen", async () => {
  const dir = await mkdtemp(join(tmpdir(), "smoke-outbox-test-"));
  const dbPath = join(dir, "persist.db");
  let db = new DatabaseSync(dbPath);
  try {
    initOutbox(db);
    enqueueOutbox(db, {
      id: "op-persist-1",
      entity: "room",
      action: "upsert",
      payload: { roomId: "mesh-durable", teamId: "team-durable", teamName: "Team Durable" },
    });
    db.close();

    // Reopen database from disk
    db = new DatabaseSync(dbPath);
    const entry = getOutboxEntry(db, "op-persist-1");
    assert.ok(entry);
    assert.equal(entry.id, "op-persist-1");
    assert.equal(entry.entity, "room");
    assert.equal(entry.status, "pending");
    assert.equal(entry.payload.roomId, "mesh-durable");
    assert.equal(entry.payload.teamName, "Team Durable");
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("4. Invalid payload rejection", async () => {
  const dir = await mkdtemp(join(tmpdir(), "smoke-outbox-test-"));
  const db = new DatabaseSync(join(dir, "test.db"));
  try {
    initOutbox(db);

    // Null or undefined
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: null }), /Invalid payload/);
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: undefined }), /Invalid payload/);

    // Non-object
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: "hello" }), /Invalid payload/);

    // Missing required fields for room upsert
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: { teamId: "t1", teamName: "T1" } }), /roomId is required/);
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: { roomId: "r1", teamName: "T1" } }), /teamId is required/);
    assert.throws(() => enqueueOutbox(db, { entity: "room", action: "upsert", payload: { roomId: "r1", teamId: "t1" } }), /teamName is required/);
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("5. Missing database initialization", async () => {
  assert.throws(
    () => enqueueOutbox(null, { entity: "room", action: "upsert", payload: { roomId: "r1", teamId: "t1", teamName: "T1" } }),
    /Outbox database is not initialized/
  );

  const mockClient = { rpc: async () => assert.fail("remote rpc must not be called") };
  const adapterWithoutDb = createSupabaseAdapter(mockClient);
  await assert.rejects(
    async () => adapterWithoutDb.upsertRoom("r1", "t1", "T1", "leader-1", "leader"),
    /Outbox database is not initialized/
  );
});

test("6. upsertRoom() queues rather than making a remote call", async () => {
  const dir = await mkdtemp(join(tmpdir(), "smoke-outbox-test-"));
  const db = new DatabaseSync(join(dir, "test.db"));
  try {
    initOutbox(db);

    let rpcCallCount = 0;
    const mockClient = {
      async rpc() {
        rpcCallCount++;
        throw new Error("Remote RPC should not be called by upsertRoom");
      },
      from() {
        assert.fail("from() should not be called by upsertRoom");
      },
    };

    const adapter = createSupabaseAdapter(mockClient, db);

    // Caller with explicit operation ID
    const res1 = await adapter.upsertRoom("room-xyz", "team-xyz", "Team XYZ", "leader-xyz", "leader", "op-explicit-1");
    assert.equal(rpcCallCount, 0, "No remote RPC should be dispatched");
    assert.equal(res1.queued, true);
    assert.equal(res1.synchronized, false);
    assert.equal(res1.operationId, "op-explicit-1");

    // Caller without explicit operation ID
    const res2 = await adapter.upsertRoom("room-abc", "team-abc", "Team ABC", "member-abc", "member");
    assert.equal(rpcCallCount, 0, "No remote RPC should be dispatched");
    assert.equal(res2.queued, true);
    assert.equal(res2.synchronized, false);
    assert.notEqual(res2.operationId, "op-explicit-1");
    assert.ok(res2.operationId.length > 10);

    // Verify both are distinct entries in SQLite
    const entry1 = getOutboxEntry(db, "op-explicit-1");
    const entry2 = getOutboxEntry(db, res2.operationId);
    assert.ok(entry1);
    assert.ok(entry2);
    assert.equal(entry1.payload.roomId, "room-xyz");
    assert.equal(entry2.payload.roomId, "room-abc");
  } finally {
    db.close();
    await rm(dir, { recursive: true, force: true });
  }
});

