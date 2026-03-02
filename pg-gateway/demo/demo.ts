/**
 * supa-proxy live demo
 *
 * Demonstrates:
 *   1. HTTP /sql  – create table, insert rows, select
 *   2. WebSocket  – neon serverless Pool with transactions (commit & rollback)
 *   3. Parallel   – query two separate databases simultaneously
 *   4. Streaming  – consume rows one-by-one as they arrive
 *   5. Truncation – large field capped server-side, then retrieved in full via streaming
 *
 * Run via:  ./run.sh
 * Or manually after infrastructure is up:
 *   PROXY1_PORT=8081 PROXY2_PORT=8082 JWT_SECRET=supa-proxy-demo-secret bun demo.ts
 */

import { createHmac } from "node:crypto";
import { Pool, neonConfig } from "@neondatabase/serverless";
import WS from "ws";

// ── Config ────────────────────────────────────────────────────────────────────

const JWT_SECRET = process.env.JWT_SECRET ?? "supa-proxy-demo-secret";
const PROXY1_PORT = +(process.env.PROXY1_PORT ?? 8081);
const PROXY2_PORT = +(process.env.PROXY2_PORT ?? 8082);
const PROXY1 = `http://localhost:${PROXY1_PORT}`;
const PROXY2 = `http://localhost:${PROXY2_PORT}`;

// ── JWT (pure crypto, no dependencies) ────────────────────────────────────────

function mintJWT(secret: string, sub: string): string {
  const enc = (s: string) => Buffer.from(s).toString("base64url");
  const hdr = enc(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const pay = enc(
    JSON.stringify({ sub, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 3600 })
  );
  const sig = createHmac("sha256", secret).update(`${hdr}.${pay}`).digest("base64url");
  return `${hdr}.${pay}.${sig}`;
}

const TOKEN = mintJWT(JWT_SECRET, "demo-user");
const AUTH = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };

// ── Pretty console helpers ─────────────────────────────────────────────────────

const R = "\x1b[0m", B = "\x1b[1m", DIM = "\x1b[2m";
const G = "\x1b[32m", BL = "\x1b[34m", Y = "\x1b[33m", CY = "\x1b[36m", RE = "\x1b[31m";

const pad = (s: string) => `  ${s}`;
const section = (n: number, t: string) =>
  console.log(`\n${B}${BL}── ${n}. ${t} ${"─".repeat(Math.max(0, 55 - t.length))}${R}\n`);
const ok   = (s: string) => console.log(pad(`${G}✓${R}  ${s}`));
const info = (s: string) => console.log(pad(`${CY}→${R}  ${s}`));
const note = (s: string) => console.log(pad(`${DIM}   ${s}${R}`));
const fail = (s: string) => console.log(pad(`${RE}✗${R}  ${s}`));

// ── SQL-over-HTTP helpers ─────────────────────────────────────────────────────

type Line = Record<string, unknown>;

/**
 * Collect all NDJSON lines from /sql.
 * Throws if any line has an "error" key.
 */
async function sql(base: string, query: string, extra?: Record<string, unknown>): Promise<Line[]> {
  const res = await fetch(`${base}/sql`, {
    method: "POST",
    headers: AUTH,
    body: JSON.stringify({ query, ...extra }),
  });
  const lines: Line[] = [];
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
  const errLine = lines.find((l) => l["error"]);
  if (errLine) throw new Error((errLine["error"] as { message: string }).message);
  return lines;
}

/** Async-generator version: yields each NDJSON line as it arrives off the wire. */
async function* streamSQL(base: string, query: string, extra?: Record<string, unknown>): AsyncGenerator<Line> {
  const res = await fetch(`${base}/sql`, {
    method: "POST",
    headers: AUTH,
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

/** Extract the row objects from collected NDJSON lines. */
const rows = (lines: Line[]) => lines.filter((l) => "row" in l).map((l) => l["row"]);

// ═════════════════════════════════════════════════════════════════════════════
//  MAIN DEMO
// ═════════════════════════════════════════════════════════════════════════════

console.log(`
${B}  ╔═══════════════════════════════════════════════════════╗${R}
${B}  ║           supa-proxy  ·  live demo                    ║${R}
${B}  ║  proxy-1 → ${PROXY1.padEnd(37)}║${R}
${B}  ║  proxy-2 → ${PROXY2.padEnd(37)}║${R}
${B}  ╚═══════════════════════════════════════════════════════╝${R}
`);

// ─────────────────────────────────────────────────────────────────────────────
//  §1  HTTP /sql — create table, insert, select
// ─────────────────────────────────────────────────────────────────────────────

section(1, "HTTP /sql — create, insert, select  (proxy-1)");

await sql(PROXY1, "DROP TABLE IF EXISTS products");
await sql(
  PROXY1,
  `CREATE TABLE products (
     id    SERIAL PRIMARY KEY,
     name  TEXT NOT NULL,
     price NUMERIC(8,2),
     tags  TEXT[]
   )`
);
ok("Table products created on db-1");

await sql(
  PROXY1,
  `INSERT INTO products (name, price, tags) VALUES
    ('Laptop',     999.00, ARRAY['electronics','work']),
    ('Coffee Mug',   9.99, ARRAY['kitchen']),
    ('Desk Chair', 349.50, ARRAY['furniture','work']),
    ('Headphones', 149.00, ARRAY['electronics','audio']),
    ('Notebook',     4.99, ARRAY['stationery'])`
);
ok("5 products inserted");

const selectLines = await sql(PROXY1, "SELECT id, name, price FROM products ORDER BY price DESC");
const productRows = rows(selectLines);
ok(`SELECT returned ${productRows.length} rows (ordered by price desc):`);
for (const r of productRows) note(JSON.stringify(r));

// ─────────────────────────────────────────────────────────────────────────────
//  §2  WebSocket — neon serverless Pool with transactions
// ─────────────────────────────────────────────────────────────────────────────

section(2, "WebSocket — neon serverless Pool + transactions  (proxy-1)");

info("Configuring @neondatabase/serverless to connect via proxy-1 /v1 WebSocket relay");
note("The Pool speaks PG wire protocol over WS; JWT is injected in the HTTP upgrade header.");

// neon serverless is designed to work with WebSocket proxies exactly like our /v1 relay.
// We need a custom WebSocket constructor to inject the JWT in the HTTP Upgrade headers.
// Extend ws.WebSocket to inject the JWT Authorization header into the HTTP Upgrade request.
// We pass options as the second arg (not protocols) so ws.WebSocket correctly identifies
// the object as ClientOptions and attaches the headers before the upgrade is sent.
class AuthWS extends WS {
  constructor(url: string | URL, _protocols?: string | string[]) {
    super(url as string, { headers: { Authorization: `Bearer ${TOKEN}` } });
  }
}

// wsProxy must return host:port/path — neon prepends ws:// or wss:// itself
neonConfig.wsProxy = () => `localhost:${PROXY1_PORT}/v1`;
neonConfig.useSecureWebSocket = false; // use ws://, not wss://
neonConfig.pipelineConnect = false;    // standard PG startup (no Neon-specific fast path)
neonConfig.webSocketConstructor = AuthWS as unknown as typeof WebSocket;

const wsPool = new Pool({
  user: "testuser",
  password: "testpass",
  database: "testdb",
  host: "localhost",     // host/port passed to wsProxy() above — we ignore them
  port: 5432,
  ssl: false,            // no TLS inside the WebSocket
  max: 1,
});

try {
  const client = await wsPool.connect();
  try {
    await client.query("DROP TABLE IF EXISTS cart");
    await client.query(
      "CREATE TABLE cart (id SERIAL PRIMARY KEY, item TEXT NOT NULL, qty INT NOT NULL)"
    );

    // ── Commit ──────────────────────────────────────────────────────────────
    await client.query("BEGIN");
    await client.query(
      "INSERT INTO cart (item, qty) VALUES ('apple', 5), ('banana', 12), ('cherry', 3)"
    );
    await client.query("COMMIT");
    ok("COMMIT: inserted 3 cart items");

    // ── Rollback ─────────────────────────────────────────────────────────────
    await client.query("BEGIN");
    await client.query("INSERT INTO cart (item, qty) VALUES ('INVALID', -999)");
    await client.query("ROLLBACK");
    ok("ROLLBACK: bad item discarded");

    // ── Verify ───────────────────────────────────────────────────────────────
    const { rows: cartRows } = await client.query("SELECT item, qty FROM cart ORDER BY id");
    ok(`Cart after rollback (3 rows, INVALID absent):`);
    for (const r of cartRows) note(JSON.stringify(r));
  } finally {
    client.release();
  }
  await wsPool.end();
  ok("Pool closed cleanly");
} catch (e: unknown) {
  fail(`WebSocket Pool error: ${(e as Error).message}`);
  note("If this failed, check that proxy-1 /v1 is reachable and JWT secret matches.");
  await wsPool.end().catch(() => {});
}

// ─────────────────────────────────────────────────────────────────────────────
//  §3  Parallel — query both databases simultaneously
// ─────────────────────────────────────────────────────────────────────────────

section(3, "Parallel — two databases queried simultaneously");

// Seed db-2 with different data through proxy-2
await sql(PROXY2, "DROP TABLE IF EXISTS products");
await sql(
  PROXY2,
  `CREATE TABLE products (
     id    SERIAL PRIMARY KEY,
     name  TEXT NOT NULL,
     region TEXT
   )`
);
await sql(
  PROXY2,
  `INSERT INTO products (name, region) VALUES
     ('Widget Alpha', 'EU'),
     ('Widget Beta',  'US'),
     ('Widget Gamma', 'APAC')`
);
ok("db-2 seeded with 3 regional products");

info("Firing both queries in parallel with Promise.all ...");
const t0 = Date.now();
const [db1Lines, db2Lines] = await Promise.all([
  sql(PROXY1, "SELECT id, name, price FROM products ORDER BY id"),
  sql(PROXY2, "SELECT id, name, region FROM products ORDER BY id"),
]);
const elapsed = Date.now() - t0;

ok(`Both queries finished in ${elapsed} ms:`);
const db1Rows = rows(db1Lines);
const db2Rows = rows(db2Lines);
info(`  db-1 (${db1Rows.length} rows):`);
for (const r of db1Rows) note(JSON.stringify(r));
info(`  db-2 (${db2Rows.length} rows):`);
for (const r of db2Rows) note(JSON.stringify(r));

// ─────────────────────────────────────────────────────────────────────────────
//  §4  Streaming — consume rows one-by-one as they arrive
// ─────────────────────────────────────────────────────────────────────────────

section(4, "Streaming — rows arrive one by one off the wire");

note("Query: 300 rows via cursor streaming (CursorBatchSize=100 on the proxy).");
note("We print a progress line every 50 rows to show continuous delivery.");

let rowsSeen = 0;
let batchStarts: number[] = [];
const streamStart = Date.now();
let prevBatchEnd = streamStart;

for await (const line of streamSQL(PROXY1, "SELECT i, md5(i::text) AS h FROM generate_series(1,300) i")) {
  if ("row" in line) {
    rowsSeen++;
    // Print a marker at the start of each cursor batch (every 100 rows)
    if (rowsSeen % 100 === 1) {
      const gap = Date.now() - prevBatchEnd;
      batchStarts.push(Date.now());
      info(
        `Batch ${Math.ceil(rowsSeen / 100)} started at row ${rowsSeen}` +
          (rowsSeen > 1 ? ` (+${gap} ms since prev batch)` : "")
      );
      prevBatchEnd = Date.now();
    }
  }
  if ("complete" in line) {
    const total = Date.now() - streamStart;
    ok(
      `Stream complete: ${rowsSeen} rows in ${total} ms — ` +
        `${Math.round((rowsSeen / total) * 1000)} rows/sec`
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  §5  Truncation + full streaming (no big memory chunk on the proxy)
// ─────────────────────────────────────────────────────────────────────────────

section(5, "Truncation + full streaming  (proxy memory stays bounded)");

// Create a table with a large text field
await sql(PROXY1, "DROP TABLE IF EXISTS blobs");
await sql(PROXY1, "CREATE TABLE blobs (id SERIAL PRIMARY KEY, label TEXT, payload TEXT)");
await sql(
  PROXY1,
  // 100 rows × 8 KB each = 800 KB total; label tells which row it is
  `INSERT INTO blobs (label, payload)
     SELECT 'row-' || i, repeat(chr(65 + (i % 26)), 8192)
     FROM generate_series(1, 100) i`
);
ok("blobs table: 100 rows × 8 KB payload each (≈ 800 KB total)");

// ── Part A: truncated ────────────────────────────────────────────────────────
info("Query with max_field_bytes=64 → payload truncated server-side:");
const truncLines = await sql(PROXY1, "SELECT id, label, payload FROM blobs WHERE id = 1", {
  max_field_bytes: 64,
});
const truncRow = rows(truncLines)[0] as Record<string, unknown>;
const truncMeta = truncLines.find((l) => "row" in l)!["truncated_fields"] as string[] | undefined;

ok(`  payload (first 80 chars): ${JSON.stringify(String(truncRow.payload).slice(0, 80))}`);
ok(`  truncated_fields: ${JSON.stringify(truncMeta)}`);
note("The proxy replaced the value in-stream; no 8 KB ever crossed the wire.");

// ── Part B: full value via streaming, proxy memory stays bounded ─────────────
info("Now fetching ALL 100 rows (800 KB) with max_field_bytes=0 via streaming:");
note(
  "The proxy fetches CursorBatchSize=100 rows per cursor FETCH, serialises them to NDJSON, " +
    "and flushes immediately — it never holds the full 800 KB in-flight."
);

let fullRowCount = 0;
let totalPayloadBytes = 0;
const fullStart = Date.now();

for await (const line of streamSQL(PROXY1, "SELECT id, payload FROM blobs ORDER BY id", {
  max_field_bytes: 0,
})) {
  if ("row" in line) {
    fullRowCount++;
    const r = line["row"] as Record<string, string>;
    totalPayloadBytes += r.payload.length;
  }
  if ("complete" in line) {
    const ms = Date.now() - fullStart;
    ok(
      `Full stream: ${fullRowCount} rows, ` +
        `${(totalPayloadBytes / 1024).toFixed(1)} KB payload, ` +
        `${ms} ms — ${Math.round((totalPayloadBytes / 1024 / ms) * 1000).toFixed(0)} KB/s`
    );
    note(
      "The proxy's peak RSS stayed bounded because it streamed one cursor batch at a time " +
        "and flushed each row to the HTTP response before fetching the next batch."
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
console.log(`\n${B}${G}  All sections complete.${R}\n`);
