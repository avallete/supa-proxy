/**
 * Shared infrastructure helpers for pg-gateway E2E tests.
 *
 * Each test file calls setupInfrastructure() in beforeAll and
 * teardownInfrastructure() in afterAll, giving it an isolated
 * Docker PG container + proxy process on dynamic ports.
 */

import { createHmac } from "node:crypto";
import * as net from "node:net";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { Pool, neonConfig } from "@neondatabase/serverless";
import WS from "ws";

// ── Types ─────────────────────────────────────────────────────────────────────

export type NdjsonLine = Record<string, unknown>;

export interface ColumnInfo {
  name: string;
  type_oid: number;
}

export interface CompleteInfo {
  command: string;
  row_count: number;
  duration_ms: number;
}

export interface ErrorInfo {
  message: string;
  code?: string;
  detail?: string;
  hint?: string;
}

export interface ProxyProcess {
  proc: ReturnType<typeof Bun.spawn>;
  baseUrl: string;
  httpPort: number;
}

export interface InfraHandle {
  proxy: ProxyProcess;
  pgContainer: StartedTestContainer;
  pgPort: number;
  token: string;
  baseUrl: string;
}

// ── PostgresContainer ─────────────────────────────────────────────────────────

export class PostgresContainer extends GenericContainer {
  constructor() {
    super("postgres:16-alpine");
    this.withExposedPorts(5432);
    this.withEnvironment({
      POSTGRES_USER: "testuser",
      POSTGRES_PASSWORD: "testpass",
      POSTGRES_DB: "testdb",
    });
    this.withHealthCheck({
      test: ["CMD-SHELL", "pg_isready -U testuser -d testdb"],
      interval: 1_000,
      timeout: 5_000,
      retries: 10,
    });
    this.withWaitStrategy(Wait.forHealthCheck());
    this.withStartupTimeout(60_000);
    this.withTmpFs({
      "/var/lib/postgresql/data": "rw,noexec,nosuid,size=512m",
    });
    this.withCommand(["postgres", "-c", "log_min_messages=FATAL"]);
  }
}

// ── Port helpers ──────────────────────────────────────────────────────────────

export function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address() as net.AddressInfo;
      srv.close((err) => {
        if (err) reject(err);
        else resolve(addr.port);
      });
    });
    srv.on("error", reject);
  });
}

// ── Binary build ──────────────────────────────────────────────────────────────

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const GATEWAY_DIR = path.resolve(__dirname, "..");
const BINARY_PATH = "/tmp/pg-gateway-e2e";

export async function buildBinary(): Promise<string> {
  const proc = Bun.spawn(["go", "build", "-o", BINARY_PATH, "."], {
    cwd: GATEWAY_DIR,
    stdout: "pipe",
    stderr: "pipe",
  });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    const stderr = await new Response(proc.stderr).text();
    throw new Error(`go build failed (exit ${exitCode}):\n${stderr}`);
  }
  return BINARY_PATH;
}

// ── Proxy spawn + ready ───────────────────────────────────────────────────────

export interface ProxyConfig {
  binaryPath: string;
  httpPort: number;
  metricsPort: number;
  pgPort: number;
  jwtSecret: string;
  envOverrides?: Record<string, string>;
}

export function startProxy(cfg: ProxyConfig): ProxyProcess {
  const env: Record<string, string> = {
    ...(process.env as Record<string, string>),
    LISTEN_PORT: `:${cfg.httpPort}`,
    METRICS_PORT: `:${cfg.metricsPort}`,
    JWT_SECRET: cfg.jwtSecret,
    PG_HOST: "127.0.0.1",
    PG_PORT: String(cfg.pgPort),
    PG_USER: "testuser",
    PG_PASSWORD: "testpass",
    PG_DATABASE: "testdb",
    APPEND_PORT: `127.0.0.1:${cfg.pgPort}`,
    STATEMENT_TIMEOUT: "3s",
    MAX_FIELD_BYTES: "1048576",
    MAX_ROWS: "0",
    CURSOR_BATCH_SIZE: "100",
    LOG_QUERIES: "false",
    ...cfg.envOverrides,
  };

  const proc = Bun.spawn([cfg.binaryPath], {
    env,
    stdout: "pipe",
    stderr: "pipe",
  });

  return {
    proc,
    httpPort: cfg.httpPort,
    baseUrl: `http://127.0.0.1:${cfg.httpPort}`,
  };
}

export async function waitForReady(
  baseUrl: string,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${baseUrl}/ready`, {
        signal: AbortSignal.timeout(2_000),
      });
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    await Bun.sleep(200);
  }
  throw new Error(
    `Proxy at ${baseUrl} did not become ready within ${timeoutMs}ms`,
  );
}

// ── Setup / Teardown ──────────────────────────────────────────────────────────

const JWT_SECRET = "pg-gateway-e2e-secret";

export async function setupInfrastructure(
  _prefix: string,
  envOverrides?: Record<string, string>,
): Promise<InfraHandle> {
  // Start PG container + allocate ports + build binary in parallel
  const [pgContainer, httpPort, metricsPort, binaryPath] = await Promise.all([
    new PostgresContainer().start(),
    getFreePort(),
    getFreePort(),
    buildBinary(),
  ]);

  const pgPort = pgContainer.getMappedPort(5432);

  const proxy = startProxy({
    binaryPath,
    httpPort,
    metricsPort,
    pgPort,
    jwtSecret: JWT_SECRET,
    envOverrides,
  });

  await waitForReady(proxy.baseUrl);

  const token = mintJWT(JWT_SECRET, "e2e-test");

  return {
    proxy,
    pgContainer,
    pgPort,
    token,
    baseUrl: proxy.baseUrl,
  };
}

export async function teardownInfrastructure(
  handle: InfraHandle,
): Promise<void> {
  handle.proxy.proc.kill("SIGTERM");
  await handle.proxy.proc.exited;
  await handle.pgContainer.stop();
}

// ── JWT helpers ───────────────────────────────────────────────────────────────

export function mintJWT(
  secret: string,
  sub: string,
  overrides?: { exp?: number; iat?: number },
): string {
  const enc = (s: string) => Buffer.from(s).toString("base64url");
  const now = Math.floor(Date.now() / 1000);
  const hdr = enc(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const pay = enc(
    JSON.stringify({
      sub,
      iat: overrides?.iat ?? now,
      exp: overrides?.exp ?? now + 3600,
    }),
  );
  const sig = createHmac("sha256", secret)
    .update(`${hdr}.${pay}`)
    .digest("base64url");
  return `${hdr}.${pay}.${sig}`;
}

export function mintExpiredJWT(secret: string, sub: string): string {
  const now = Math.floor(Date.now() / 1000);
  return mintJWT(secret, sub, { iat: now - 7200, exp: now - 3600 });
}

export function mintWrongAlgoJWT(secret: string, sub: string): string {
  // Advertises RS256 in header — server should reject because it only accepts HS256
  const enc = (s: string) => Buffer.from(s).toString("base64url");
  const now = Math.floor(Date.now() / 1000);
  const hdr = enc(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const pay = enc(JSON.stringify({ sub, iat: now, exp: now + 3600 }));
  // Sign with HMAC-SHA256 anyway — the header mismatch causes rejection
  const sig = createHmac("sha256", secret)
    .update(`${hdr}.${pay}`)
    .digest("base64url");
  return `${hdr}.${pay}.${sig}`;
}

// ── NDJSON helpers ────────────────────────────────────────────────────────────

export async function sql(
  baseUrl: string,
  query: string,
  token: string,
  extra?: Record<string, unknown>,
): Promise<NdjsonLine[]> {
  const res = await fetch(`${baseUrl}/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, ...extra }),
  });

  const lines: NdjsonLine[] = [];
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const parts = buf.split("\n");
    buf = parts.pop()!;
    for (const p of parts) if (p.trim()) lines.push(JSON.parse(p));
  }
  if (buf.trim()) lines.push(JSON.parse(buf));
  return lines;
}

export async function sqlRaw(
  baseUrl: string,
  query: string,
  token: string | null,
  extra?: Record<string, unknown>,
): Promise<{ status: number; lines: NdjsonLine[] }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${baseUrl}/sql`, {
    method: "POST",
    headers,
    body: JSON.stringify({ query, ...extra }),
  });

  const lines: NdjsonLine[] = [];
  if (res.body) {
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      const parts = buf.split("\n");
      buf = parts.pop()!;
      for (const p of parts) {
        try {
          if (p.trim()) lines.push(JSON.parse(p));
        } catch {
          // ignore malformed lines on error responses
        }
      }
    }
    if (buf.trim()) {
      try {
        lines.push(JSON.parse(buf));
      } catch {
        // ignore
      }
    }
  }

  return { status: res.status, lines };
}

export async function* streamSQL(
  baseUrl: string,
  query: string,
  token: string,
  extra?: Record<string, unknown>,
): AsyncGenerator<NdjsonLine> {
  const res = await fetch(`${baseUrl}/sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query, ...extra }),
  });
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const parts = buf.split("\n");
    buf = parts.pop()!;
    for (const p of parts) if (p.trim()) yield JSON.parse(p);
  }
  if (buf.trim()) yield JSON.parse(buf);
}

export function getColumns(lines: NdjsonLine[]): ColumnInfo[] {
  const line = lines.find((l) => "columns" in l);
  return (line?.["columns"] as ColumnInfo[]) ?? [];
}

export function getRows(lines: NdjsonLine[]): Record<string, unknown>[] {
  return lines
    .filter((l) => "row" in l)
    .map((l) => l["row"] as Record<string, unknown>);
}

export function getComplete(lines: NdjsonLine[]): CompleteInfo | undefined {
  const line = lines.find((l) => "complete" in l);
  return line?.["complete"] as CompleteInfo | undefined;
}

export function getError(lines: NdjsonLine[]): ErrorInfo | undefined {
  const line = lines.find((l) => "error" in l);
  return line?.["error"] as ErrorInfo | undefined;
}

export function getTruncatedFields(rowLine: NdjsonLine): string[] {
  return (rowLine["truncated_fields"] as string[]) ?? [];
}

// ── Connection leak detection ─────────────────────────────────────────────────

export async function getActiveConnections(
  baseUrl: string,
  token: string,
): Promise<number> {
  const lines = await sql(
    baseUrl,
    "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()",
    token,
  );
  const rows = getRows(lines);
  return (rows[0]?.["n"] as number) ?? 0;
}

export async function waitForZeroConnections(
  baseUrl: string,
  token: string,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const n = await getActiveConnections(baseUrl, token);
    if (n === 0) return;
    await Bun.sleep(100);
  }
  const n = await getActiveConnections(baseUrl, token);
  if (n !== 0) {
    throw new Error(`Expected 0 connections, got ${n} after ${timeoutMs}ms`);
  }
}

// ── WebSocket Pool factory ────────────────────────────────────────────────────

export function makeNeonPool(
  httpPort: number,
  token: string,
  opts?: Record<string, unknown>,
): Pool {
  // AuthWS injects the Bearer token into the HTTP Upgrade request headers.
  // Pass options as 2nd arg (not protocols string) so ws identifies it as ClientOptions.
  class AuthWS extends WS {
    constructor(url: string | URL, _protocols?: string | string[]) {
      super(url as string, { headers: { Authorization: `Bearer ${token}` } });
    }
  }

  neonConfig.wsProxy = () => `127.0.0.1:${httpPort}/v1`;
  neonConfig.useSecureWebSocket = false;
  neonConfig.pipelineConnect = false;
  neonConfig.webSocketConstructor = AuthWS as unknown as typeof WebSocket;

  return new Pool({
    user: "testuser",
    password: "testpass",
    database: "testdb",
    host: "localhost",
    port: 5432,
    ssl: false,
    max: 1,
    ...opts,
  });
}
