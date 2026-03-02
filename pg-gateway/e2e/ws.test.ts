/**
 * WebSocket /v1 relay E2E tests — 9 tests
 *
 * Uses @neondatabase/serverless Pool with a custom AuthWS constructor
 * to inject the Bearer token into the HTTP Upgrade request.
 */

import { describe, beforeAll, afterAll, it, expect } from "bun:test";
import {
  setupInfrastructure,
  teardownInfrastructure,
  makeNeonPool,
  sql,
  getRows,
  waitForZeroConnections,
} from "./helpers.ts";
import type { InfraHandle } from "./helpers.ts";
import type { Pool } from "@neondatabase/serverless";

let infra: InfraHandle;
let pool: Pool;

beforeAll(async () => {
  infra = await setupInfrastructure("ws");
  pool = makeNeonPool(infra.proxy.httpPort, infra.token);
});

afterAll(async () => {
  await pool.end().catch(() => {}); // may already be ended by the last test
  await teardownInfrastructure(infra);
});

describe("WebSocket /v1", () => {
  it("no Authorization header → GET /v1 returns 401", async () => {
    // Auth check happens before WS upgrade, so a plain GET also returns 401
    const res = await fetch(`${infra.baseUrl}/v1`);
    expect(res.status).toBe(401);
  });

  it("SELECT 1 via Pool → rows[0].n === 1", async () => {
    const client = await pool.connect();
    try {
      const { rows } = await client.query("SELECT 1 AS n");
      expect(rows[0]).toMatchInlineSnapshot(`
        {
          "n": 1,
        }
      `);
    } finally {
      client.release();
    }
  });

  it("DDL: CREATE TABLE + DROP TABLE via Pool → no throw", async () => {
    const client = await pool.connect();
    try {
      await client.query("CREATE TABLE ws_test_ddl (id SERIAL PRIMARY KEY, val TEXT)");
      await client.query("DROP TABLE ws_test_ddl");
    } finally {
      client.release();
    }
  });

  it("Transaction commit: BEGIN → INSERT → COMMIT → count = 1", async () => {
    const client = await pool.connect();
    try {
      await client.query("CREATE TABLE IF NOT EXISTS ws_test_tx (id SERIAL PRIMARY KEY, val INT)");
      await client.query("BEGIN");
      await client.query("INSERT INTO ws_test_tx (val) VALUES (42)");
      await client.query("COMMIT");
      const { rows } = await client.query("SELECT count(*)::int AS n FROM ws_test_tx WHERE val = 42");
      expect(rows[0]).toMatchInlineSnapshot(`
        {
          "n": 1,
        }
      `);
    } finally {
      await client.query("DROP TABLE IF EXISTS ws_test_tx").catch(() => {});
      client.release();
    }
  });

  it("Transaction rollback: BEGIN → INSERT → ROLLBACK → count = 0", async () => {
    const client = await pool.connect();
    try {
      await client.query("CREATE TABLE IF NOT EXISTS ws_test_rollback (id SERIAL PRIMARY KEY, val INT)");
      await client.query("BEGIN");
      await client.query("INSERT INTO ws_test_rollback (val) VALUES (999)");
      await client.query("ROLLBACK");
      const { rows } = await client.query(
        "SELECT count(*)::int AS n FROM ws_test_rollback WHERE val = 999",
      );
      expect(rows[0]).toMatchInlineSnapshot(`
        {
          "n": 0,
        }
      `);
    } finally {
      await client.query("DROP TABLE IF EXISTS ws_test_rollback").catch(() => {});
      client.release();
    }
  });

  it("Parameterized query: SELECT $1::int + $2::int → correct sum", async () => {
    const client = await pool.connect();
    try {
      const { rows } = await client.query("SELECT $1::int + $2::int AS result", [7, 13]);
      expect(rows[0]).toMatchInlineSnapshot(`
        {
          "result": 20,
        }
      `);
    } finally {
      client.release();
    }
  });

  it("Error recovery: query nonexistent table → catch + release; new client; SELECT 1 succeeds", async () => {
    const client = await pool.connect();
    try {
      await client.query("SELECT * FROM ws_nonexistent_table_xyz");
      client.release();
      throw new Error("Expected error but query succeeded");
    } catch (_err) {
      // pgx error — release the errored client
      client.release(true); // destroy the connection
    }

    // A new client from the same pool should work fine
    const client2 = await pool.connect();
    try {
      const { rows } = await client2.query("SELECT 1 AS n");
      expect(rows[0]).toMatchInlineSnapshot(`
        {
          "n": 1,
        }
      `);
    } finally {
      client2.release();
    }
  });

  it("100 sequential queries on one client", async () => {
    const client = await pool.connect();
    try {
      for (let i = 0; i < 100; i++) {
        const { rows } = await client.query("SELECT $1::int AS n", [i]);
        expect(rows[0]?.n).toBe(i);
      }
    } finally {
      client.release();
    }
  }, 60_000);

  it("After pool.end() → zero connections via HTTP /sql", async () => {
    // End the outer pool first (this is the last test, so it won't affect others)
    await pool.end().catch(() => {});

    // Now open + close a fresh pool
    const freshPool = makeNeonPool(infra.proxy.httpPort, infra.token);
    const client = await freshPool.connect();
    await client.query("SELECT 1");
    client.release();
    await freshPool.end();

    // All WS pools are closed — wait up to 10s for PG backends to be released
    await waitForZeroConnections(infra.baseUrl, infra.token, 10_000);
  }, 20_000);
});
