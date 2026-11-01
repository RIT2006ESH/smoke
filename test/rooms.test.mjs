import test from "node:test";
import assert from "node:assert/strict";
import WebSocket from "ws";
import { startSmoke, authorize } from "./test-helpers.mjs";

test("authorized members share a persistent isolated room", async () => {
  const app = await startSmoke();
  try {
    const a = await authorize(app.base, "mesh-a", await app.token("node-a", "mesh-a", "leader"));
    const b = await authorize(app.base, "mesh-a", await app.token("node-b", "mesh-a"));
    assert.equal(a.response.status, 200);
    assert.equal(b.body.members.length, 2);
    const sockets = [];
    for (const nodeId of ["node-a", "node-b"]) {
      const jwt = await app.token(nodeId, "mesh-a");
      const ticket = await fetch(`${app.base}/api/v1/realtime/ticket`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${jwt}` },
        body: JSON.stringify({ roomId: "mesh-a" }),
      });
      const ticketBody = await ticket.json();
      const ws = new WebSocket(`${app.base.replace("http", "ws")}/ws?ticket=${ticketBody.ticket}`);
      await new Promise((resolve, reject) => {
        ws.once("open", resolve);
        ws.once("error", reject);
      });
      sockets.push(ws);
    }
    const senderJwt = await app.token("node-a", "mesh-a");
    const received = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("message was not broadcast")), 1000);
      sockets[1].on("message", (data) => {
        const event = JSON.parse(String(data));
        if (event.t === "room:message") {
          clearTimeout(timer);
          resolve(event.message.text);
        }
      });
      fetch(`${app.base}/api/v1/rooms/mesh-a/messages`, {
        method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${senderJwt}` },
        body: JSON.stringify({ text: "hello" }),
      }).then((response) => {
        if (response.status !== 201) reject(new Error(`message send failed: ${response.status}`));
      }).catch(reject);
    });
    assert.equal(received, "hello");
    const history = await fetch(`${app.base}/api/v1/rooms/mesh-a/messages`, { headers: { authorization: `Bearer ${await app.token("node-b", "mesh-a")}` } });
    assert.equal((await history.json()).messages[0].text, "hello");
    const other = await fetch(`${app.base}/api/v1/rooms/mesh-b/members`, { headers: { authorization: `Bearer ${await app.token("node-a", "mesh-b")}` } });
    assert.equal(other.status, 403);
  } finally { await app.close(); }
});
