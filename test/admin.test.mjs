import test from "node:test";
import assert from "node:assert/strict";
import { startSmoke, authorize } from "./test-helpers.mjs";

test("internal revocation is idempotent and removes access", async () => {
  const app = await startSmoke();
  try {
    const jwt = await app.token("node-a", "mesh-a");
    await authorize(app.base, "mesh-a", jwt);
    const revoke = () => fetch(`${app.base}/api/v1/internal/rooms/mesh-a/revoke`, {
      method: "POST", headers: { ...app.internal, "content-type": "application/json" },
      body: JSON.stringify({ nodeId: "node-a", eventId: "offline-1", reason: "offline_timeout" }),
    });
    assert.equal((await revoke()).status, 200);
    assert.equal((await revoke()).status, 200);
    assert.equal((await fetch(`${app.base}/api/v1/rooms/mesh-a/members`, { headers: { authorization: `Bearer ${jwt}` } })).status, 403);
  } finally { await app.close(); }
});
