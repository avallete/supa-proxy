/**
 * HTTP /sql endpoint E2E tests — 22 tests
 *
 * Covers: auth, DDL, DML, SELECT, transactions, truncation, limits.
 * Each test file manages its own isolated Docker PG container + proxy process.
 */

import { describe, beforeAll, afterAll, it, expect } from "bun:test";
import {
  setupInfrastructure,
  teardownInfrastructure,
  mintExpiredJWT,
  mintWrongAlgoJWT,
  sql,
  sqlRaw,
  getColumns,
  getRows,
  getComplete,
  getError,
  getTruncatedFields,
  getFreePort,
  startProxy,
  waitForReady,
  buildBinary,
} from "./helpers.ts";
import type { InfraHandle } from "./helpers.ts";

const JWT_SECRET = "pg-gateway-e2e-secret";

let infra: InfraHandle;

beforeAll(async () => {
  infra = await setupInfrastructure("http");
});

afterAll(async () => {
  await teardownInfrastructure(infra);
});

// ── Auth (5) ──────────────────────────────────────────────────────────────────

describe("Auth", () => {
  it("GET /health → 200 + {status:ok,version}", async () => {
    const res = await fetch(`${infra.baseUrl}/health`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, string>;
    expect(body).toMatchInlineSnapshot(`
      {
        "status": "ok",
        "version": "0.1.0",
      }
    `);
  });

  it("GET /ready → 200", async () => {
    const res = await fetch(`${infra.baseUrl}/ready`);
    expect(res.status).toBe(200);
  });

  it("/sql without Authorization header → 401", async () => {
    const { status } = await sqlRaw(infra.baseUrl, "SELECT 1", null);
    expect(status).toBe(401);
  });

  it("/sql with expired JWT → 401", async () => {
    const expired = mintExpiredJWT(JWT_SECRET, "test");
    const { status } = await sqlRaw(infra.baseUrl, "SELECT 1", expired);
    expect(status).toBe(401);
  });

  it("/sql with wrong-algorithm JWT → 401", async () => {
    const badAlgo = mintWrongAlgoJWT(JWT_SECRET, "test");
    const { status } = await sqlRaw(infra.baseUrl, "SELECT 1", badAlgo);
    expect(status).toBe(401);
  });
});

// ── DDL (3) ───────────────────────────────────────────────────────────────────

describe("DDL", () => {
  it("CREATE TABLE → no error, complete.command starts with CREATE", async () => {
    const lines = await sql(
      infra.baseUrl,
      "CREATE TABLE http_test_ddl (id SERIAL PRIMARY KEY, val TEXT)",
      infra.token,
    );
    const { duration_ms: _, ...complete } = getComplete(lines)!;
    expect(complete).toMatchInlineSnapshot(`
      {
        "command": "CREATE TABLE",
        "row_count": 0,
      }
    `);
  });

  it("ALTER TABLE ADD COLUMN → follow-up SELECT includes new column", async () => {
    await sql(
      infra.baseUrl,
      "ALTER TABLE http_test_ddl ADD COLUMN extra INT",
      infra.token,
    );
    const lines = await sql(
      infra.baseUrl,
      "SELECT * FROM http_test_ddl LIMIT 0",
      infra.token,
    );
    expect(getColumns(lines)).toMatchInlineSnapshot(`
      [
        {
          "name": "id",
          "type_oid": 23,
        },
        {
          "name": "val",
          "type_oid": 25,
        },
        {
          "name": "extra",
          "type_oid": 23,
        },
      ]
    `);
  });

  it("DROP TABLE → follow-up SELECT returns error (table not found)", async () => {
    await sql(infra.baseUrl, "DROP TABLE http_test_ddl", infra.token);
    const lines = await sql(
      infra.baseUrl,
      "SELECT * FROM http_test_ddl",
      infra.token,
    );
    const err = getError(lines);
    expect(err?.message).toMatchInlineSnapshot(
      `"ERROR: relation "http_test_ddl" does not exist (SQLSTATE 42P01)"`,
    );
  });
});

// ── DML (4) ───────────────────────────────────────────────────────────────────

describe("DML", () => {
  beforeAll(async () => {
    await sql(
      infra.baseUrl,
      "CREATE TABLE IF NOT EXISTS http_test_dml (id SERIAL PRIMARY KEY, name TEXT, score INT)",
      infra.token,
    );
  });

  afterAll(async () => {
    await sql(infra.baseUrl, "DROP TABLE IF EXISTS http_test_dml", infra.token);
  });

  it("INSERT (no RETURNING) → columns:[] + complete.row_count ≥ 1", async () => {
    const lines = await sql(
      infra.baseUrl,
      "INSERT INTO http_test_dml (name, score) VALUES ('alice', 10)",
      infra.token,
    );
    const { duration_ms: _, ...complete } = getComplete(lines)!;
    expect({ columns: getColumns(lines), complete }).toMatchInlineSnapshot(`
      {
        "columns": [],
        "complete": {
          "command": "INSERT 0 1",
          "row_count": 1,
        },
      }
    `);
  });

  it("INSERT … RETURNING → columns present, row lines present", async () => {
    const lines = await sql(
      infra.baseUrl,
      "INSERT INTO http_test_dml (name, score) VALUES ('bob', 20) RETURNING id, name",
      infra.token,
    );
    const { duration_ms: _, ...complete } = getComplete(lines)!;
    expect({ columns: getColumns(lines), rows: getRows(lines), complete })
      .toMatchInlineSnapshot(`
      {
        "columns": [
          {
            "name": "id",
            "type_oid": 23,
          },
          {
            "name": "name",
            "type_oid": 25,
          },
        ],
        "complete": {
          "command": "INSERT 0 1",
          "row_count": 1,
        },
        "rows": [
          {
            "id": 2,
            "name": "bob",
          },
        ],
      }
    `);
  });

  it("UPDATE … RETURNING → updated value visible in rows", async () => {
    const lines = await sql(
      infra.baseUrl,
      "UPDATE http_test_dml SET score = 99 WHERE name = 'alice' RETURNING name, score",
      infra.token,
    );
    expect(getRows(lines)).toMatchInlineSnapshot(`
      [
        {
          "name": "alice",
          "score": 99,
        },
      ]
    `);
  });

  it("DELETE → complete.row_count === N", async () => {
    // Insert 3 rows to delete
    await sql(
      infra.baseUrl,
      "INSERT INTO http_test_dml (name, score) VALUES ('x1', 1), ('x2', 2), ('x3', 3)",
      infra.token,
    );
    const lines = await sql(
      infra.baseUrl,
      "DELETE FROM http_test_dml WHERE name LIKE 'x%'",
      infra.token,
    );
    const { duration_ms: _, ...complete } = getComplete(lines)!;
    expect(complete).toMatchInlineSnapshot(`
      {
        "command": "DELETE 3",
        "row_count": 3,
      }
    `);
  });
});

// ── SELECT (4) ────────────────────────────────────────────────────────────────

describe("SELECT", () => {
  beforeAll(async () => {
    await sql(
      infra.baseUrl,
      "CREATE TABLE IF NOT EXISTS http_test_select (id SERIAL PRIMARY KEY, val INT, tag TEXT)",
      infra.token,
    );
    await sql(
      infra.baseUrl,
      "INSERT INTO http_test_select (val, tag) VALUES (3,'c'), (1,'a'), (2,'b')",
      infra.token,
    );
  });

  afterAll(async () => {
    await sql(
      infra.baseUrl,
      "DROP TABLE IF EXISTS http_test_select",
      infra.token,
    );
  });

  it("ORDER BY → rows in sorted order", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT val FROM http_test_select ORDER BY val DESC",
      infra.token,
    );
    expect(getRows(lines)).toMatchInlineSnapshot(`
      [
        {
          "val": 3,
        },
        {
          "val": 2,
        },
        {
          "val": 1,
        },
      ]
    `);
  });

  it("WHERE → filtered row count", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT val FROM http_test_select WHERE val < 3",
      infra.token,
    );
    expect(getRows(lines)).toMatchInlineSnapshot(`
      [
        {
          "val": 1,
        },
        {
          "val": 2,
        },
      ]
    `);
  });

  it("NULL → row field is null", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT NULL::text AS v",
      infra.token,
    );
    expect(getRows(lines)[0]).toMatchInlineSnapshot(`
      {
        "v": null,
      }
    `);
  });

  it("Text array → row field is a JS array", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT ARRAY['a','b','c']::text[] AS arr",
      infra.token,
    );
    expect(getRows(lines)[0]).toMatchInlineSnapshot(`
      {
        "arr": [
          "a",
          "b",
          "c",
        ],
      }
    `);
  });
});

// ── Transactions (2) ──────────────────────────────────────────────────────────

describe("Transactions", () => {
  beforeAll(async () => {
    await sql(
      infra.baseUrl,
      "CREATE TABLE IF NOT EXISTS http_test_tx (id SERIAL PRIMARY KEY, val INT)",
      infra.token,
    );
  });

  afterAll(async () => {
    await sql(infra.baseUrl, "DROP TABLE IF EXISTS http_test_tx", infra.token);
  });

  it("BEGIN; INSERT; COMMIT → row visible after commit", async () => {
    const txLines = await sql(
      infra.baseUrl,
      "BEGIN; INSERT INTO http_test_tx (val) VALUES (42); COMMIT",
      infra.token,
    );
    expect(getError(txLines)).toBeUndefined();

    const selLines = await sql(
      infra.baseUrl,
      "SELECT val FROM http_test_tx WHERE val = 42",
      infra.token,
    );
    expect(getRows(selLines).length).toBe(1);
  });

  it("BEGIN; INSERT; ROLLBACK → row not visible after rollback", async () => {
    const txLines = await sql(
      infra.baseUrl,
      "BEGIN; INSERT INTO http_test_tx (val) VALUES (999); ROLLBACK",
      infra.token,
    );
    expect(getError(txLines)).toBeUndefined();

    const selLines = await sql(
      infra.baseUrl,
      "SELECT val FROM http_test_tx WHERE val = 999",
      infra.token,
    );
    expect(getRows(selLines).length).toBe(0);
  });
});

// ── Truncation + per-query override (2) ───────────────────────────────────────

describe("Truncation", () => {
  beforeAll(async () => {
    await sql(
      infra.baseUrl,
      "CREATE TABLE IF NOT EXISTS http_test_trunc (id SERIAL PRIMARY KEY, big TEXT)",
      infra.token,
    );
    await sql(
      infra.baseUrl,
      "INSERT INTO http_test_trunc (big) VALUES (repeat('x', 1024))",
      infra.token,
    );
  });

  afterAll(async () => {
    await sql(
      infra.baseUrl,
      "DROP TABLE IF EXISTS http_test_trunc",
      infra.token,
    );
  });

  it("max_field_bytes=64 → truncated_fields present, value starts with [truncated:", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT big FROM http_test_trunc LIMIT 1",
      infra.token,
      { max_field_bytes: 64 },
    );
    const rowLine = lines.filter((l) => "row" in l)[0]!;
    expect(rowLine).toMatchInlineSnapshot(`
      {
        "row": {
          "big": "[truncated: 1024 bytes, showing first 64]xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        },
        "truncated_fields": [
          "big",
        ],
      }
    `);
  });

  it("max_field_bytes=0 → full value returned, no truncated_fields", async () => {
    const lines = await sql(
      infra.baseUrl,
      "SELECT big FROM http_test_trunc LIMIT 1",
      infra.token,
      { max_field_bytes: 0 },
    );
    const rowLine = lines.filter((l) => "row" in l)[0]!;
    const val = (rowLine["row"] as Record<string, unknown>)["big"] as string;
    expect({
      truncated_fields: getTruncatedFields(rowLine),
      val_length: val.length,
    }).toMatchInlineSnapshot(`
      {
        "truncated_fields": [],
        "val_length": 1024,
      }
    `);
  });
});

// ── Limits (2) ────────────────────────────────────────────────────────────────

describe("Limits", () => {
  it("MAX_ROWS=5 → exactly 5 row lines for generate_series(1,20)", async () => {
    // Spawn a secondary proxy with MAX_ROWS=5
    const binaryPath = await buildBinary();
    const [httpPort, metricsPort] = await Promise.all([
      getFreePort(),
      getFreePort(),
    ]);
    const proxy = startProxy({
      binaryPath,
      httpPort,
      metricsPort,
      pgPort: infra.pgPort,
      jwtSecret: JWT_SECRET,
      envOverrides: { MAX_ROWS: "5" },
    });

    try {
      await waitForReady(proxy.baseUrl);
      const lines = await sql(
        proxy.baseUrl,
        "SELECT i FROM generate_series(1,20) i",
        infra.token,
      );
      expect(getError(lines)).toBeUndefined();
      expect(getRows(lines).length).toBe(5);
    } finally {
      proxy.proc.kill("SIGTERM");
      await proxy.proc.exited;
    }
  });

  it("Statement timeout: pg_sleep(10) → error line, no complete line", async () => {
    // Default proxy has STATEMENT_TIMEOUT=3s
    const lines = await sql(
      infra.baseUrl,
      "SELECT pg_sleep(10)",
      infra.token,
      {},
    );
    expect(getError(lines)).toMatchInlineSnapshot(`
      {
        "message": "ERROR: canceling statement due to statement timeout (SQLSTATE 57014)",
      }
    `);
    expect(getComplete(lines)).toBeUndefined();
  }, 15_000);
});
