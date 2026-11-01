import { OUTBOX_STATUS } from "./outbox.mjs";

function fallbackUsername(nodeId) {
  const value = String(nodeId).replace(/[^A-Za-z0-9_]/g, "_").slice(0, 20);
  return value.length >= 3 ? value : `${value}___`.slice(0, 3);
}

/**
 * Calculates exponential backoff delay with jitter.
 * @param {number} attempt
 * @param {number} baseDelayMs
 * @param {number} maxDelayMs
 * @returns {number}
 */
export function calculateBackoff(attempt, baseDelayMs = 1000, maxDelayMs = 300000) {
  const exponential = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
  const jitter = Math.random() * (exponential * 0.2); // 20% jitter
  return Math.round(exponential + jitter);
}

/**
 * Creates and manages the background sync worker.
 * @param {{
 *   db: import("node:sqlite").DatabaseSync,
 *   client: any,
 *   pollIntervalMs?: number,
 *   maxRetries?: number,
 *   batchSize?: number,
 *   requestTimeoutMs?: number,
 * }} options
 */
export function createSyncWorker({
  db,
  client,
  pollIntervalMs = 2000,
  maxRetries = 10,
  batchSize = 10,
  requestTimeoutMs = 5000,
}) {
  let timer = null;
  let running = false;
  let processing = false;

  /**
   * Helper to execute remote Supabase calls with timeout.
   */
  async function withTimeout(promise, timeoutMs) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error(`Sync timeout after ${timeoutMs}ms`)), timeoutMs);
    });
    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Dispatches a single outbox record to Supabase.
   */
  async function dispatchRecord(record) {
    const { id, entity, action, payload } = record;

    if (entity === "room" && action === "upsert") {
      const { roomId, teamId, teamName, leaderNodeId, role } = payload;
      await withTimeout(
        (async () => {
          if (leaderNodeId) {
            const { error: profileError } = await client.from("player_profiles").upsert(
              { node_id: leaderNodeId, username: fallbackUsername(leaderNodeId) },
              { onConflict: "node_id", ignoreDuplicates: true },
            );
            if (profileError) throw profileError;
          }
          const { error } = await client.rpc("create_team_with_room", {
            p_team_id: teamId,
            p_mesh_id: teamId,
            p_team_name: teamName,
            p_room_id: roomId,
            p_leader_node_id: leaderNodeId || null,
          });
          if (error) throw error;
        })(),
        requestTimeoutMs,
      );
    } else if (entity === "member" && action === "upsert") {
      const { roomId, teamId, nodeId, role } = payload;

      // Stale revocation guard: check if user was revoked in local SQLite
      const current = db.prepare("SELECT revoked_at FROM room_members WHERE room_id = ? AND node_id = ?").get(roomId, nodeId);
      if (current && current.revoked_at) {
        // User is revoked locally; cancel this upsert to prevent resurrecting revoked access
        return { cancelled: true, reason: "member is revoked locally" };
      }

      await withTimeout(
        (async () => {
          const { error: profileError } = await client.from("player_profiles").upsert(
            { node_id: nodeId, username: fallbackUsername(nodeId) },
            { onConflict: "node_id", ignoreDuplicates: true },
          );
          if (profileError) throw profileError;
          const { error } = await client.rpc("add_team_member", {
            p_team_id: teamId,
            p_node_id: nodeId,
          });
          if (error) throw error;
        })(),
        requestTimeoutMs,
      );
    } else if (entity === "message" && action === "insert") {
      const { id: msgId, roomId, nodeId, text, createdAt } = payload;
      await withTimeout(
        (async () => {
          const { error } = await client.from("messages").insert({
            id: msgId,
            room_id: roomId,
            node_id: nodeId,
            message_text: text,
            created_at: createdAt,
          });
          if (error) {
            // If primary key collision on messages table, treat as already processed
            if (error.code === "23505") return;
            throw error;
          }
        })(),
        requestTimeoutMs,
      );
    } else if (entity === "member" && action === "revoke") {
      const { roomId, nodeId } = payload;
      await withTimeout(
        (async () => {
          const { data: room, error: roomError } = await client
            .from("smoke_rooms")
            .select("team_id")
            .eq("room_id", roomId)
            .single();
          if (roomError) throw roomError;
          const { error } = await client.rpc("remove_team_member", {
            p_team_id: room.team_id,
            p_node_id: nodeId,
            p_status: "revoked",
          });
          if (error) throw error;
        })(),
        requestTimeoutMs,
      );
    } else if (entity === "member" && action === "updateRole") {
      const { roomId, nodeId, role } = payload;
      await withTimeout(
        (async () => {
          const { data: room, error: roomError } = await client
            .from("smoke_rooms")
            .select("team_id")
            .eq("room_id", roomId)
            .single();
          if (roomError) throw roomError;
          const { error } = role === "leader"
            ? await client.rpc("transfer_team_leadership", {
                p_team_id: room.team_id,
                p_new_leader_node_id: nodeId,
              })
            : await client.rpc("set_team_member_member", {
                p_team_id: room.team_id,
                p_node_id: nodeId,
              });
          if (error) throw error;
        })(),
        requestTimeoutMs,
      );
    } else if (entity === "team" && action === "update") {
      const { roomId, teamName } = payload;
      await withTimeout(
        (async () => {
          const { data: room, error: roomError } = await client
            .from("smoke_rooms")
            .select("team_id")
            .eq("room_id", roomId)
            .single();
          if (roomError) throw roomError;
          const { error } = await client.rpc("rename_team", {
            p_team_id: room.team_id,
            p_team_name: teamName,
          });
          if (error) throw error;
        })(),
        requestTimeoutMs,
      );
    } else {
      throw new Error(`Unknown outbox operation: ${entity}:${action}`);
    }

    return { success: true };
  }

  /**
   * Processes a single batch of pending/failed outbox tasks.
   */
  async function processBatch() {
    if (!client || !db || processing) return 0;
    processing = true;

    try {
      const now = new Date().toISOString();

      // Reset stale processing locks (e.g. from crash or unclean exit > 60s ago)
      const staleTime = new Date(Date.now() - 60000).toISOString();
      db.prepare(`
        UPDATE supabase_outbox
        SET status = 'pending'
        WHERE status = 'processing' AND next_attempt_at < ?
      `).run(staleTime);

      // Select batch of ready records
      const candidates = db.prepare(`
        SELECT id, entity, action, payload, attempt_count
        FROM supabase_outbox
        WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?
        ORDER BY created_at ASC
        LIMIT ?
      `).all(now, batchSize);

      let processedCount = 0;

      for (const row of candidates) {
        // Optimistic concurrency lock: mark record as processing
        const updated = db.prepare(`
          UPDATE supabase_outbox
          SET status = 'processing', next_attempt_at = ?
          WHERE id = ? AND status IN ('pending', 'failed')
        `).run(now, row.id);

        if (updated.changes === 0) {
          // Another worker claimed this row
          continue;
        }

        let parsedPayload;
        try {
          parsedPayload = JSON.parse(row.payload);
        } catch (parseErr) {
          // Unparseable payload -> immediately dead-letter
          db.prepare(`
            INSERT INTO supabase_dead_letter (id, entity, action, payload, created_at, failed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
          `).run(row.id, row.entity, row.action, row.payload, now, now, `Unparseable payload: ${parseErr.message}`);
          db.prepare("DELETE FROM supabase_outbox WHERE id = ?").run(row.id);
          continue;
        }

        try {
          await dispatchRecord({
            id: row.id,
            entity: row.entity,
            action: row.action,
            payload: parsedPayload,
          });

          // Successfully synchronized -> remove from outbox
          db.prepare("DELETE FROM supabase_outbox WHERE id = ?").run(row.id);
          processedCount++;
        } catch (err) {
          const attempt = (row.attempt_count || 0) + 1;
          const errorMsg = err?.message || String(err);

          if (attempt >= maxRetries) {
            // Reached max retries -> route to dead letter table
            db.prepare(`
              INSERT INTO supabase_dead_letter (id, entity, action, payload, created_at, failed_at, error)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            `).run(row.id, row.entity, row.action, row.payload, now, now, `Max retries (${maxRetries}) exceeded: ${errorMsg}`);
            db.prepare("DELETE FROM supabase_outbox WHERE id = ?").run(row.id);
          } else {
            // Exponential backoff with jitter
            const delay = calculateBackoff(attempt);
            const nextAttempt = new Date(Date.now() + delay).toISOString();

            db.prepare(`
              UPDATE supabase_outbox
              SET status = 'failed',
                  attempt_count = ?,
                  last_error = ?,
                  next_attempt_at = ?
              WHERE id = ?
            `).run(attempt, errorMsg, nextAttempt, row.id);
          }
        }
      }

      return processedCount;
    } finally {
      processing = false;
    }
  }

  function start() {
    if (running) return;
    running = true;
    timer = setInterval(async () => {
      try {
        await processBatch();
      } catch (err) {
        console.error("[SyncWorker] Error in sync loop:", err);
      }
    }, pollIntervalMs);
    // Unref timer so it doesn't block process exit if used as daemon
    if (timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }

  function stop() {
    running = false;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  }

  return {
    start,
    stop,
    processBatch,
    isRunning: () => running,
  };
}

