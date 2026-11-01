import { createClient } from "@supabase/supabase-js";
import { enqueueOutbox, initOutbox } from "./outbox.mjs";

const url = process.env.SUPABASE_URL;
const secretKey = process.env.SUPABASE_SECRET_KEY;
const defaultClient = url && secretKey
  ? createClient(url, secretKey, { auth: { autoRefreshToken: false, persistSession: false } })
  : null;

function fallbackUsername(nodeId) {
  const value = String(nodeId).replace(/[^A-Za-z0-9_]/g, "_").slice(0, 20);
  return value.length >= 3 ? value : `${value}___`.slice(0, 3);
}

export function createSupabaseAdapter(client, db = null) {
  let database = db;
  if (database) {
    initOutbox(database);
  }

  return {
    setDatabase(newDb) {
      database = newDb;
      if (database) {
        initOutbox(database);
      }
    },
    getDatabase() {
      return database;
    },
    getClient() {
      return client;
    },
    async verify() {
      if (!client) throw new Error("Supabase client is not configured");
      const { error } = await client.from("smoke_rooms").select("room_id", { count: "exact", head: true });
      if (error) throw error;
    },
    async upsertRoom(roomId, teamId, teamName, leaderNodeId, role, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId,
        entity: "room",
        action: "upsert",
        payload: {
          roomId,
          teamId,
          teamName,
          leaderNodeId,
          role,
        },
      });
    },
    async upsertMember(roomId, teamId, nodeId, role, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId,
        entity: "member",
        action: "upsert",
        payload: {
          roomId,
          teamId,
          nodeId,
          role,
        },
      });
    },
    async insertMessage(message, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId || message.id,
        entity: "message",
        action: "insert",
        payload: {
          id: message.id,
          roomId: message.roomId,
          nodeId: message.nodeId,
          text: message.text,
          createdAt: message.createdAt,
        },
      });
    },
    async revokeMember(roomId, nodeId, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId,
        entity: "member",
        action: "revoke",
        payload: {
          roomId,
          nodeId,
        },
      });
    },
    async updateRole(roomId, nodeId, role, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId,
        entity: "member",
        action: "updateRole",
        payload: {
          roomId,
          nodeId,
          role,
        },
      });
    },
    async updateTeam(roomId, teamName, operationId) {
      if (!database) {
        throw new Error("Outbox database is not initialized");
      }
      return enqueueOutbox(database, {
        id: operationId,
        entity: "team",
        action: "update",
        payload: {
          roomId,
          teamName,
        },
      });
    },
  };
}

export const supabase = defaultClient ? createSupabaseAdapter(defaultClient) : null;
