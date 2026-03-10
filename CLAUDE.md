# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**pg-gateway** — A Go HTTP/WebSocket proxy that sits between browser clients and PostgreSQL. It exposes two main endpoints: a streaming NDJSON endpoint (`POST /sql`) and a WebSocket relay (`/v1`) compatible with the Neon serverless driver. All requests are authenticated via HS256 JWT tokens.

## Build & Test Commands

All commands run from `pg-gateway/`:

```bash
# Build
make build                # CGO_ENABLED=0 stripped binary

# Lint & format
make lint                 # golangci-lint run
make fmt                  # go fmt ./...
make vet                  # go vet ./...

# Unit tests (no Docker)
make test                 # go test -v -race ./...

# Integration tests (requires Docker)
make test-integration     # go test -tags integration -v -race -timeout 5m ./...

# Bun E2E tests (requires Docker)
make test-e2e             # cd e2e && bun install && bun test --timeout 120000
make test-stress          # cd e2e && bun test stress.test.ts --timeout 600000

# Benchmarks (requires Docker)
make bench                # go test -tags integration -bench=. -benchmem

# Docker
make docker-up            # docker compose up --build -d
make docker-down          # docker compose down -v
```

Run a single Go test: `cd pg-gateway && go test -run TestName -v ./...`
Run a single integration test: `cd pg-gateway && go test -tags integration -run TestName -v -timeout 5m ./...`
Run a single Bun E2E test: `cd pg-gateway/e2e && bun test -t "test name" --timeout 120000`

## Architecture

```
Client
├─ POST /sql              (streaming NDJSON, wire-level DataRow parsing)
│   ├─ validateJWT() → JWTClaims (role, db, host, port, readonly)
│   ├─ classifyQuery() via multigres parser → SELECT | DML+RETURNING | DML
│   ├─ resolveOptions(cfg, req) → queryOptions (per-query overrides merged with server defaults)
│   ├─ acquireOwnedConn() → pgx conn (hijacked from pool or direct)
│   ├─ applySession() → SET ROLE, SET statement_timeout, GUCs, read_only
│   ├─ pgconn.Hijack() → raw net.Conn for wire-level parsing
│   └─ streamSelectWire() | streamReturningWire() | execDML()
│
└─ WS /v1?address=host:port  (zero-copy TCP relay, full PG wire protocol)
    ├─ validateJWT()
    ├─ resolveTarget() → host:port
    └─ bidirectional io.Copy (WebSocket ↔ TCP)
```

**Key files:**
- `main.go` — HTTP mux, server startup, config loading
- `sql_handler.go` — POST /sql: query classification, per-query options, wire-level streaming, NDJSON output
- `wire_reader.go` — PG wire protocol reader: DataRow column-by-column with truncation, RowDescription/CommandComplete/ErrorResponse parsers, batch sizing
- `pg_to_json.go` — Type-aware conversion from raw PG text-format bytes to JSON; direct streaming JSON writer
- `ws_relay.go` — WS /v1: WebSocket-to-TCP relay
- `pool.go` — Per-target pgxpool.Pool manager with lazy creation and idle eviction
- `auth.go` — JWT validation (HS256), JWTClaims struct with multi-tenant fields
- `config.go` — Config struct, env var parsing
- `metrics.go` — Prometheus metrics + `statusResponseWriter` (must implement `http.Hijacker` + `http.Flusher`)

**NDJSON output format (`/sql`):**
```
{"columns":[{"name":"id","type_oid":23},...]}
{"row":{"id":1,...},"truncated_fields":["big_col"]}
{"complete":{"command":"SELECT","row_count":1,"duration_ms":4.2}}
```

## Connection Pooling

Disabled by default (`POOL_ENABLED=false`). When enabled:
- Per-target pools (keyed by `host:port/db`), lazy-created
- `DISCARD ALL` on every release (resets role, GUCs, temp tables)
- Idle pools evicted after 10 minutes
- Only used by `/sql`; `/v1` always opens raw TCP

## Test Layers

1. **Unit tests** (`*_test.go` without build tag) — httptest-based, no Docker
2. **Integration tests** (`//go:build integration`) — testcontainers-go spins up PG 16
3. **Bun E2E tests** (`e2e/`) — full Docker stack, tests HTTP and WS endpoints from TypeScript

## Memory Model & Per-Query Options

**Wire-level DataRow parsing** (SELECT + DML RETURNING) ensures a hard per-query memory ceiling:

1. pgx handles connection setup, auth, `BEGIN`, `DECLARE CURSOR`, and session GUCs
2. `pgconn.Hijack()` takes the raw `net.Conn`
3. DataRow messages are parsed column-by-column: each column's 4-byte length prefix is read, then `min(col_length, max_field_bytes)` bytes are kept; the rest is discarded via `io.Discard`
4. JSON is written directly from raw PG text bytes — no `json.Marshal`, no intermediate Go types
5. Dynamic batch sizing: `batch_size = max_memory / (num_cols × max_field_bytes)`, minimum 1

**Per-query options** — all safety limits are overridable per request via JSON body:
- `max_field_bytes` (int) — truncate columns exceeding this; 0 = unlimited
- `max_rows` (int) — stop after N rows; 0 = unlimited
- `statement_timeout` (string, e.g. "5s") — overrides server default
- `cursor_batch_size` (int) — rows per FETCH
- `max_memory` (int, bytes) — per-query memory budget for dynamic batch sizing

When pooling is enabled, SELECT and DML+RETURNING queries take the connection out of the pool (via `poolConn.Hijack()`) since wire-level parsing requires ownership. Plain DML keeps the pooled connection.

## Critical Implementation Notes

- `statusResponseWriter` in `metrics.go` wraps all responses; it **must** forward `Hijack()` (for WS upgrades) and `Flush()` (for chunked streaming) or those features break silently
- JWT claims carry `Host`, `Port`, `DB`, `Role`, `ReadOnly` for multi-tenant routing
- Query classification falls back to regex string matching if multigres parser fails
- Cursor-based SELECTs use `DECLARE CURSOR` + `FETCH N` for bounded memory; batch size is dynamically adjusted from `max_memory` budget
- Wire-level truncation replaces the old `truncateValue()` — truncation now happens at the TCP read level, before data enters a Go buffer
- For truly unlimited streaming of large fields (100MB+) without truncation, use the WS relay (`/v1`)
- Dev JWT tokens: `scripts/mint-dev-token.sh [sub] [ttl]`

## Environment Variables

Key configuration (see `config.go` for full list):

| Variable | Default | Description |
|----------|---------|-------------|
| `JWT_SECRET` | (required) | HS256 signing key |
| `PG_HOST` / `PG_PORT` / `PG_USER` / `PG_PASSWORD` / `PG_DATABASE` | localhost defaults | Target PostgreSQL |
| `LISTEN_PORT` | `:15432` | Proxy listen address |
| `METRICS_PORT` | `:2112` | Prometheus metrics |
| `STATEMENT_TIMEOUT` | `30s` | Per-query timeout (overridable per request) |
| `MAX_FIELD_BYTES` | `1048576` | Truncate fields over 1MB (0 = unlimited) |
| `MAX_ROWS` | `0` | Max rows per query (0 = unlimited) |
| `CURSOR_BATCH_SIZE` | `100` | Rows per FETCH (auto-adjusted by MAX_MEMORY) |
| `MAX_MEMORY` | `10485760` | Per-query memory budget (10MB); 0 = no budget |
| `POOL_ENABLED` | `false` | Enable connection pooling |
| `REGION` | (empty) | Region tag for routing/logging |
