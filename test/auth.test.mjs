import test from "node:test";
import assert from "node:assert/strict";
import { startSmoke, authorize } from "./test-helpers.mjs";

test("invalid and expired NODES tokens are rejected", async () => {
  const app = await startSmoke();
  try {
    const missing = await fetch(`${app.base}/api/v1/rooms/mesh-a/authorize`, { method: "POST" });
    assert.equal(missing.status, 403);
    const expired = await authorize(app.base, "mesh-a", await app.token("node-a", "mesh-a", "member", Math.floor(Date.now() / 1000) - 1));
    assert.equal(expired.response.status, 403);
  } finally { await app.close(); }
});
