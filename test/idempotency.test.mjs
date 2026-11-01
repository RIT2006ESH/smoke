import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

// Simulation harness for PostgreSQL operation ledger & concurrency mechanics
class RemoteIdempotencyEngine {
  constructor() {
    this.ledger = new Map(); // opId -> { opType, hash, status, response, error }
    this.inFlightLocks = new Map(); // opId -> Promise
    this.teams = new Map(); // teamId -> { teamId, name, leaderNodeId, version, status }
    this.members = new Map(); // `${teamId}:${nodeId}` -> { teamId, nodeId, role, status }
    this.messages = new Map(); // messageId -> { id, roomId, nodeId, text, createdAt }
    this.mutationCounters = {
      createTeam: 0,
      addMember: 0,
      transferLeadership: 0,
      renameTeam: 0,
      insertMessage: 0,
    };
  }

  computeHash(payload) {
    return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
  }

  // Atomically executes an operation with operation-ledger idempotency
  async executeIdempotent(opId, opType, payload, mutationFn) {
    if (!opId) {
      return mutationFn();
    }

    const hash = this.computeHash(payload);

    // Concurrency sequencing: if another request is currently in-flight for this opId, wait for it
    while (this.inFlightLocks.has(opId)) {
      await this.inFlightLocks.get(opId);
    }

    const existing = this.ledger.get(opId);
    if (existing) {
      if (existing.hash !== hash) {
        throw new Error(`operation ID ${opId} already used with different payload`);
      }
      if (existing.status === "completed") {
        return existing.response;
      }
    }

    // Register lock and in_progress status
    let releaseLock;
    const lockPromise = new Promise((resolve) => { releaseLock = resolve; });
    this.inFlightLocks.set(opId, lockPromise);
    this.ledger.set(opId, { opType, hash, status: "in_progress", response: null });

    try {
      // Execute the domain mutation within atomic context
      const result = await mutationFn();

      // Complete ledger entry atomically on success
      this.ledger.set(opId, {
        opType,
        hash,
        status: "completed",
        response: result,
        completedAt: new Date().toISOString(),
      });
      return result;
    } catch (err) {
      // Failed transaction: rollback ledger entry
      this.ledger.delete(opId);
      throw err;
    } finally {
      this.inFlightLocks.delete(opId);
      releaseLock();
    }
  }

  async createTeamWithRoom({ teamId, meshId, teamName, roomId, leaderNodeId, opId }) {
    return this.executeIdempotent(opId, "create_team_with_room", { teamId, meshId, teamName, roomId, leaderNodeId }, async () => {
      this.mutationCounters.createTeam++;
      if (this.teams.has(teamId)) {
        const existing = this.teams.get(teamId);
        if (existing.meshId !== meshId || existing.roomId !== roomId) {
          throw new Error("stable team, mesh, or room identity mismatch");
        }
        return existing;
      }
      const team = { teamId, meshId, teamName, roomId, leaderNodeId, status: "active", version: 1 };
      this.teams.set(teamId, team);
      this.members.set(`${teamId}:${leaderNodeId}`, { teamId, nodeId: leaderNodeId, role: "leader", status: "active" });
      return team;
    });
  }

  async addTeamMember({ teamId, nodeId, opId }) {
    return this.executeIdempotent(opId, "add_team_member", { teamId, nodeId }, async () => {
      this.mutationCounters.addMember++;
      const key = `${teamId}:${nodeId}`;
      const existing = this.members.get(key);
      if (existing && existing.status === "revoked") {
        throw new Error(`cannot add member ${nodeId} to team ${teamId}: membership is revoked`);
      }
      const member = { teamId, nodeId, role: existing?.role || "member", status: "active" };
      this.members.set(key, member);
      return member;
    });
  }

  async revokeTeamMember({ teamId, nodeId }) {
    const key = `${teamId}:${nodeId}`;
    const member = this.members.get(key) || { teamId, nodeId, role: "member" };
    member.status = "revoked";
    this.members.set(key, member);
    return member;
  }

  async renameTeam({ teamId, teamName, expectedVersion, opId }) {
    return this.executeIdempotent(opId, "rename_team", { teamId, teamName, expectedVersion }, async () => {
      this.mutationCounters.renameTeam++;
      const team = this.teams.get(teamId);
      if (!team) throw new Error("team does not exist");
      if (expectedVersion !== undefined && team.version !== expectedVersion) {
        throw new Error(`team rename conflict: current version is ${team.version} but expected ${expectedVersion}`);
      }
      team.teamName = teamName;
      team.version++;
      return { ...team };
    });
  }

  async transferLeadership({ teamId, newLeaderNodeId, expectedLeaderNodeId, opId }) {
    return this.executeIdempotent(opId, "transfer_team_leadership", { teamId, newLeaderNodeId, expectedLeaderNodeId }, async () => {
      this.mutationCounters.transferLeadership++;
      const team = this.teams.get(teamId);
      if (!team) throw new Error("team does not exist");
      if (expectedLeaderNodeId && team.leaderNodeId !== expectedLeaderNodeId) {
        throw new Error(`leadership transfer conflict: current leader is ${team.leaderNodeId} but expected ${expectedLeaderNodeId}`);
      }
      team.leaderNodeId = newLeaderNodeId;
      team.version++;
      return { ...team };
    });
  }

  async insertRoomMessage({ messageId, roomId, nodeId, text, opId }) {
    return this.executeIdempotent(opId, "insert_room_message", { messageId, roomId, nodeId, text }, async () => {
      this.mutationCounters.insertMessage++;
      if (this.messages.has(messageId)) {
        const existing = this.messages.get(messageId);
        if (existing.roomId !== roomId || existing.nodeId !== nodeId) {
          throw new Error(`message ID ${messageId} already exists with different room or author`);
        }
        return existing;
      }
      const msg = { id: messageId, roomId, nodeId, text, createdAt: new Date().toISOString() };
      this.messages.set(messageId, msg);
      return msg;
    });
  }
}

test("1. First operation succeeds and records in ledger", async () => {
  const engine = new RemoteIdempotencyEngine();
  const res = await engine.createTeamWithRoom({
    teamId: "team-1", meshId: "mesh-1", teamName: "First Team", roomId: "room-1", leaderNodeId: "node-1", opId: "op-create-1"
  });

  assert.equal(res.teamName, "First Team");
  assert.equal(engine.mutationCounters.createTeam, 1);
  const ledgerEntry = engine.ledger.get("op-create-1");
  assert.ok(ledgerEntry);
  assert.equal(ledgerEntry.status, "completed");
  assert.equal(ledgerEntry.response.teamId, "team-1");
});

test("2. Identical retry returns original result without repeating mutation", async () => {
  const engine = new RemoteIdempotencyEngine();
  const res1 = await engine.createTeamWithRoom({
    teamId: "team-1", meshId: "mesh-1", teamName: "First Team", roomId: "room-1", leaderNodeId: "node-1", opId: "op-create-2"
  });

  // Retry with same opId and identical payload
  const res2 = await engine.createTeamWithRoom({
    teamId: "team-1", meshId: "mesh-1", teamName: "First Team", roomId: "room-1", leaderNodeId: "node-1", opId: "op-create-2"
  });

  assert.deepEqual(res1, res2);
  assert.equal(engine.mutationCounters.createTeam, 1, "Mutation counter must NOT increment on retry");
});

test("3. Same ID with different payload is rejected", async () => {
  const engine = new RemoteIdempotencyEngine();
  await engine.createTeamWithRoom({
    teamId: "team-1", meshId: "mesh-1", teamName: "Original Name", roomId: "room-1", leaderNodeId: "node-1", opId: "op-create-3"
  });

  await assert.rejects(
    async () => {
      await engine.createTeamWithRoom({
        teamId: "team-1", meshId: "mesh-1", teamName: "Tampered Name", roomId: "room-1", leaderNodeId: "node-1", opId: "op-create-3"
      });
    },
    /already used with different payload/
  );
  assert.equal(engine.mutationCounters.createTeam, 1);
});

test("4. Concurrent duplicate requests execute mutation only once", async () => {
  const engine = new RemoteIdempotencyEngine();
  const [res1, res2] = await Promise.all([
    engine.createTeamWithRoom({
      teamId: "team-concurrent", meshId: "mesh-c", teamName: "Concurrent", roomId: "room-c", leaderNodeId: "node-c", opId: "op-concurrent-1"
    }),
    engine.createTeamWithRoom({
      teamId: "team-concurrent", meshId: "mesh-c", teamName: "Concurrent", roomId: "room-c", leaderNodeId: "node-c", opId: "op-concurrent-1"
    }),
  ]);

  assert.deepEqual(res1, res2);
  assert.equal(engine.mutationCounters.createTeam, 1, "Concurrent duplicate requests must execute mutation only once");
});

test("5. Failed transaction does not leave a completed operation record", async () => {
  const engine = new RemoteIdempotencyEngine();

  // Try to rename non-existent team
  await assert.rejects(
    async () => {
      await engine.renameTeam({ teamId: "non-existent", teamName: "New Name", opId: "op-fail-1" });
    },
    /team does not exist/
  );

  assert.equal(engine.ledger.has("op-fail-1"), false, "Failed transaction must roll back ledger entry");
});

test("6. Timeout after remote success followed by retry", async () => {
  const engine = new RemoteIdempotencyEngine();

  // Remote operation succeeds
  const serverResult = await engine.createTeamWithRoom({
    teamId: "team-timeout", meshId: "mesh-t", teamName: "Timeout Team", roomId: "room-t", leaderNodeId: "node-t", opId: "op-timeout-1"
  });

  // Client network timed out before receiving response, so client retries
  const retryResult = await engine.createTeamWithRoom({
    teamId: "team-timeout", meshId: "mesh-t", teamName: "Timeout Team", roomId: "room-t", leaderNodeId: "node-t", opId: "op-timeout-1"
  });

  assert.deepEqual(retryResult, serverResult);
  assert.equal(engine.mutationCounters.createTeam, 1);
});

test("7. Out-of-order team updates rejected via version check", async () => {
  const engine = new RemoteIdempotencyEngine();
  const team = await engine.createTeamWithRoom({
    teamId: "team-version", meshId: "mesh-v", teamName: "Version 1", roomId: "room-v", leaderNodeId: "node-v", opId: "op-v-init"
  });
  assert.equal(team.version, 1);

  // Newer update arrives first: rename to V2 (expectedVersion: 1)
  const v2 = await engine.renameTeam({
    teamId: "team-version", teamName: "Version 2 (Newer)", expectedVersion: 1, opId: "op-rename-v2"
  });
  assert.equal(v2.version, 2);
  assert.equal(v2.teamName, "Version 2 (Newer)");

  // Stale update (expectedVersion: 1) arrives late
  await assert.rejects(
    async () => {
      await engine.renameTeam({
        teamId: "team-version", teamName: "Version 1.5 (Stale)", expectedVersion: 1, opId: "op-rename-stale"
      });
    },
    /team rename conflict: current version is 2 but expected 1/
  );

  // Final team name is intact
  assert.equal(engine.teams.get("team-version").teamName, "Version 2 (Newer)");
});

test("8. Stale membership addition after revocation is rejected", async () => {
  const engine = new RemoteIdempotencyEngine();
  await engine.createTeamWithRoom({
    teamId: "team-revo", meshId: "mesh-r", teamName: "Revocation Team", roomId: "room-r", leaderNodeId: "leader-r", opId: "op-revo-init"
  });

  // Member joins
  await engine.addTeamMember({ teamId: "team-revo", nodeId: "member-bad", opId: "op-join" });
  assert.equal(engine.members.get("team-revo:member-bad").status, "active");

  // Admin revokes member
  await engine.revokeTeamMember({ teamId: "team-revo", nodeId: "member-bad" });
  assert.equal(engine.members.get("team-revo:member-bad").status, "revoked");

  // Stale add_team_member arrives
  await assert.rejects(
    async () => {
      await engine.addTeamMember({ teamId: "team-revo", nodeId: "member-bad", opId: "op-stale-join" });
    },
    /cannot add member member-bad to team team-revo: membership is revoked/
  );

  // Status remains revoked
  assert.equal(engine.members.get("team-revo:member-bad").status, "revoked");
});

test("9. Duplicate message operations prevented", async () => {
  const engine = new RemoteIdempotencyEngine();

  // Send message
  const msg1 = await engine.insertRoomMessage({
    messageId: "msg-uuid-1", roomId: "room-1", nodeId: "node-1", text: "Hello smoke", opId: "op-msg-1"
  });
  assert.equal(engine.mutationCounters.insertMessage, 1);

  // Retry same message
  const msgRetry = await engine.insertRoomMessage({
    messageId: "msg-uuid-1", roomId: "room-1", nodeId: "node-1", text: "Hello smoke", opId: "op-msg-1"
  });
  assert.deepEqual(msgRetry, msg1);
  assert.equal(engine.mutationCounters.insertMessage, 1, "Duplicate message retry must not increment count");
  assert.equal(engine.messages.size, 1);

  // Distinct message with same messageId but different content rejected
  await assert.rejects(
    async () => {
      await engine.insertRoomMessage({
        messageId: "msg-uuid-1", roomId: "room-different", nodeId: "node-1", text: "Different", opId: "op-msg-2"
      });
    },
    /already exists with different room or author/
  );
});

