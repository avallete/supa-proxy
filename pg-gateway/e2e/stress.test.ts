/**
 * Stress + connection-leak tests — 8 tests
 *
 * Uses a proxy with STATEMENT_TIMEOUT=120s and MAX_FIELD_BYTES=0 (no truncation).
 * Creates a 'big' table seeded once in beforeAll, then runs heavy queries.
 */

import { describe, beforeAll, afterAll, it, expect } from "bun:test";
import {
  setupInfrastructure,
  teardownInfrastructure,
  sql,
  streamSQL,
  getRows,
  getComplete,
  getError,
  waitForZeroConnections,
} from "./helpers.ts";
import type { InfraHandle } from "./helpers.ts";

let infra: InfraHandle;

beforeAll(async () => {
  infra = await setupInfrastructure("stress", {
    STATEMENT_TIMEOUT: "120s",
    MAX_FIELD_BYTES: "0",
  });

  // Create and seed the base table
  await sql(
    infra.baseUrl,
    "CREATE TABLE big (id INT PRIMARY KEY, payload TEXT)",
    infra.token,
  );
}, 120_000);

afterAll(async () => {
  await teardownInfrastructure(infra);
});

describe("Stress", () => {
  it("Insert 100K rows", async () => {
    const lines = await sql(
      infra.baseUrl,
      "INSERT INTO big SELECT i, repeat('x', 1024) FROM generate_series(1, 100000) i",
      infra.token,
    );
    expect(getError(lines)).toBeUndefined();
    const complete = getComplete(lines);
    expect(complete?.row_count).toBe(100000);
  }, 120_000);

  it("Stream 100K rows", async () => {
    let rowCount = 0;
    for await (const line of streamSQL(
      infra.baseUrl,
      "SELECT id, payload FROM big",
      infra.token,
    )) {
      if ("row" in line) rowCount++;
    }
    expect(rowCount).toBe(100000);
  }, 180_000);

  it("~1 GB stream: insert 100K more with 5 KB payload, stream all 200K", async () => {
    // Insert second batch with 5 KB payload
    await sql(
      infra.baseUrl,
      "INSERT INTO big SELECT i, repeat('y', 5120) FROM generate_series(100001, 200000) i",
      infra.token,
      {},
    );

    let rowCount = 0;
    for await (const line of streamSQL(
      infra.baseUrl,
      "SELECT id, payload FROM big ORDER BY id",
      infra.token,
      { max_field_bytes: 0 },
    )) {
      if ("row" in line) rowCount++;
    }
    expect(rowCount).toBe(200000);
  }, 600_000);

  it("Large single column: 10 rows × 10MB each, no OOM", async () => {
    const bytesPerRow = 10 * 1024 * 1024; // 10MB
    let rowCount = 0;
    for await (const line of streamSQL(
      infra.baseUrl,
      `SELECT repeat('x', ${bytesPerRow}) AS huge FROM generate_series(1, 10) i`,
      infra.token,
      { max_field_bytes: 0 },
    )) {
      if ("row" in line) {
        rowCount++;
        const val = (line["row"] as Record<string, unknown>)["huge"] as string;
        expect(val.length).toBe(bytesPerRow);
      }
    }
    expect(rowCount).toBe(10);
  }, 120_000);

  it("20 concurrent requests → all have 100 rows, no errors", async () => {
    const results = await Promise.all(
      Array.from({ length: 20 }, () =>
        sql(
          infra.baseUrl,
          "SELECT i FROM generate_series(1, 100) i",
          infra.token,
        ),
      ),
    );
    for (const lines of results) {
      expect(getError(lines)).toBeUndefined();
      expect(getRows(lines).length).toBe(100);
    }
  }, 60_000);

  it("50 concurrent requests → all succeed", async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, () =>
        sql(
          infra.baseUrl,
          "SELECT i FROM generate_series(1, 100) i",
          infra.token,
        ),
      ),
    );
    for (const lines of results) {
      expect(getError(lines)).toBeUndefined();
      expect(getRows(lines).length).toBe(100);
    }
  }, 60_000);

  it("Connection leak: 10 sequential calls → zero connections after", async () => {
    for (let i = 0; i < 10; i++) {
      await sql(infra.baseUrl, "SELECT 1", infra.token);
    }
    await waitForZeroConnections(infra.baseUrl, infra.token);
  }, 30_000);

  it("Connection leak: 20 concurrent calls → zero connections after", async () => {
    await Promise.all(
      Array.from({ length: 20 }, () =>
        sql(
          infra.baseUrl,
          "SELECT i FROM generate_series(1, 10) i",
          infra.token,
        ),
      ),
    );
    await waitForZeroConnections(infra.baseUrl, infra.token);
  }, 30_000);
});
