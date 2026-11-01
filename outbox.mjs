import { randomUUID } from "node:crypto";

export const OUTBOX_STATUS = {
  PENDING: "pending",
  PROCESSING: "processing",
  COMPLETED: "completed",
  FAILED: "failed",
};

/**
 * Initializes the SQLite outbox and dead-letter tables.
 * @param {import("node:sqlite").DatabaseSync} db
 */
export function initOutbox(db) {
  if (!db || typeof db.exec !== "function") {
    throw new Error("Outbox database is not initialized");
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS supabase_outbox (
      id TEXT PRIMARY KEY,
      entity TEXT NOT NULL,
      action TEXT NOT NULL,
      payload TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
      attempt_count INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL,
      next_attempt_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS supabase_dead_letter (
      id TEXT PRIMARY KEY,
      entity TEXT NOT NULL,
      action TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at TEXT NOT NULL,
      failed_at TEXT NOT NULL,
      error TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_supabase_outbox_dispatch
      ON supabase_outbox(status, next_attempt_at, created_at);
  `);
}

/**
 * Validates payload structure and returns JSON string.
 */
export function validatePayload(entity, action, payload) {
  if (payload === null || payload === undefined) {
    throw new Error("Invalid payload: payload cannot be null or undefined");
  }
  if (typeof payload !== "object") {
    throw new Error("Invalid payload: payload must be an object");
  }

  if (entity === "room" && action === "upsert") {
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (!payload.teamId || typeof payload.teamId !== "string") {
      throw new Error("Invalid payload: teamId is required and must be a string");
    }
    if (!payload.teamName || typeof payload.teamName !== "string") {
      throw new Error("Invalid payload: teamName is required and must be a string");
    }
  } else if (entity === "member" && action === "upsert") {
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (!payload.teamId || typeof payload.teamId !== "string") {
      throw new Error("Invalid payload: teamId is required and must be a string");
    }
    if (!payload.nodeId || typeof payload.nodeId !== "string") {
      throw new Error("Invalid payload: nodeId is required and must be a string");
    }
    if (payload.role && !["leader", "member"].includes(payload.role)) {
      throw new Error("Invalid payload: role must be leader or member");
    }
  } else if (entity === "message" && action === "insert") {
    if (!payload.id || typeof payload.id !== "string") {
      throw new Error("Invalid payload: id is required and must be a string");
    }
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (!payload.nodeId || typeof payload.nodeId !== "string") {
      throw new Error("Invalid payload: nodeId is required and must be a string");
    }
    if (typeof payload.text !== "string" || !payload.text.trim()) {
      throw new Error("Invalid payload: text is required and cannot be empty");
    }
    if (!payload.createdAt || typeof payload.createdAt !== "string") {
      throw new Error("Invalid payload: createdAt is required and must be a string");
    }
  } else if (entity === "member" && action === "revoke") {
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (!payload.nodeId || typeof payload.nodeId !== "string") {
      throw new Error("Invalid payload: nodeId is required and must be a string");
    }
  } else if (entity === "member" && action === "updateRole") {
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (!payload.nodeId || typeof payload.nodeId !== "string") {
      throw new Error("Invalid payload: nodeId is required and must be a string");
    }
    if (!payload.role || !["leader", "member"].includes(payload.role)) {
      throw new Error("Invalid payload: role must be leader or member");
    }
  } else if (entity === "team" && action === "update") {
    if (!payload.roomId || typeof payload.roomId !== "string") {
      throw new Error("Invalid payload: roomId is required and must be a string");
    }
    if (typeof payload.teamName !== "string") {
      throw new Error("Invalid payload: teamName is required and must be a string");
    }
  }

  try {
    return JSON.stringify(payload);
  } catch (err) {
    throw new Error(`Invalid payload: not JSON serializable (${err.message})`);
  }
}

/**
 * Persists an operation to the SQLite outbox queue.
 * @param {import("node:sqlite").DatabaseSync} db
 * @param {{ id?: string, entity: string, action: string, payload: any, status?: string }} params
 */
export function enqueueOutbox(db, { id, entity, action, payload, status = OUTBOX_STATUS.PENDING }) {
  if (!db || typeof db.prepare !== "function") {
    throw new Error("Outbox database is not initialized");
  }
  if (!entity || typeof entity !== "string") {
    throw new Error("Invalid entity: entity is required");
  }
  if (!action || typeof action !== "string") {
    throw new Error("Invalid action: action is required");
  }

  const serializedPayload = validatePayload(entity, action, payload);
  const opId = id ? String(id) : randomUUID();
  const now = new Date().toISOString();

  const existing = db.prepare("SELECT id FROM supabase_outbox WHERE id = ?").get(opId);
  if (existing) {
    throw new Error(`Duplicate operation ID: ${opId}`);
  }

  db.prepare(`
    INSERT INTO supabase_outbox (id, entity, action, payload, status, attempt_count, last_error, created_at, next_attempt_at)
    VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?)
  `).run(opId, entity, action, serializedPayload, status, now, now);

  return {
    queued: true,
    synchronized: false,
    operationId: opId,
    status,
    createdAt: now,
  };
}

/**
 * Retrieves a single outbox entry by operation ID.
 * @param {import("node:sqlite").DatabaseSync} db
 * @param {string} id
 */
export function getOutboxEntry(db, id) {
  if (!db || typeof db.prepare !== "function") {
    throw new Error("Outbox database is not initialized");
  }
  const row = db.prepare("SELECT * FROM supabase_outbox WHERE id = ?").get(id);
  if (!row) return null;
  return {
    ...row,
    payload: JSON.parse(row.payload),
  };
}

