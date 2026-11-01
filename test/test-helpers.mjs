import { generateKeyPairSync } from "node:crypto";
import { writeFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { SignJWT } from "jose";

export async function startSmoke() {
  const dir = await mkdtemp(join(tmpdir(), "smoke-test-"));
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicFile = join(dir, "public.pem");
  writeFileSync(publicFile, publicKey.export({ type: "spki", format: "pem" }));
  const port = 12000 + Math.floor(Math.random() * 500);
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: new URL("..", import.meta.url),
    env: { ...process.env, SUPABASE_URL: "", SUPABASE_SECRET_KEY: "", SMOKE_PORT: String(port), NODES_PUBLIC_KEY_FILE: publicFile, DATABASE_PATH: join(dir, "smoke.db"), SMOKE_INTERNAL_API_SECRET: "test-secret" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  for (let i = 0; i < 50; i++) {
    try { if ((await fetch(`http://localhost:${port}/healthz`)).ok) break; } catch {}
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  const token = async (nodeId, roomId, role = "member", exp = Math.floor(Date.now() / 1000) + 300) =>
    new SignJWT({ nodeId, roomId, teamId: `team-${roomId}`, teamName: `Team ${roomId}`, role })
      .setProtectedHeader({ alg: "EdDSA", typ: "JWT" }).setIssuer("nodes").setAudience("smoke-room")
      .setSubject(nodeId).setIssuedAt().setExpirationTime(exp).setJti(`${nodeId}-${roomId}-${Date.now()}`)
      .sign(privateKey);
  return {
    base: `http://localhost:${port}`,
    token,
    internal: { "x-smoke-internal-secret": "test-secret" },
    async close() {
      child.kill("SIGTERM");
      await new Promise((resolve) => child.once("exit", resolve));
      await rm(dir, { recursive: true, force: true });
    },
  };
}

export async function authorize(base, roomId, jwt) {
  const response = await fetch(`${base}/api/v1/rooms/${roomId}/authorize`, { method: "POST", headers: { authorization: `Bearer ${jwt}` } });
  return { response, body: await response.json() };
}
