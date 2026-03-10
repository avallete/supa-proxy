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
├─ POST /sql              (streaming NDJSON, cursor-based SELECTs)
│   ├─ validateJWT() → JWTClaims (role, db, host, port, readonly)
│   ├─ classifyQuery() via multigres parser → SELECT | DML+RETURNING | DML
│   ├─ acquireConn() → pooled (if POOL_ENABLED) or direct pgx conn
│   ├─ applySession() → SET ROLE, GUCs, read_only
│   └─ streamSelect() | streamReturning() | execDML()
│
└─ WS /v1?address=host:port  (zero-copy TCP relay, full PG wire protocol)
    ├─ validateJWT()
    ├─ resolveTarget() → host:port
    └─ bidirectional io.Copy (WebSocket ↔ TCP)
```

**Key files:**
- `main.go` — HTTP mux, server startup, config loading
- `sql_handler.go` — POST /sql: query classification, cursor streaming, truncation, NDJSON output
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

## Critical Implementation Notes

- `statusResponseWriter` in `metrics.go` wraps all responses; it **must** forward `Hijack()` (for WS upgrades) and `Flush()` (for chunked streaming) or those features break silently
- JWT claims carry `Host`, `Port`, `DB`, `Role`, `ReadOnly` for multi-tenant routing
- Query classification falls back to regex string matching if multigres parser fails
- Cursor-based SELECTs use `DECLARE CURSOR` + `FETCH N` for bounded memory
- `truncateValue()` caps field size at `MAX_FIELD_BYTES` and reports truncated column names per row
- Dev JWT tokens: `scripts/mint-dev-token.sh [sub] [ttl]`

## Environment Variables

Key configuration (see `config.go` for full list):

| Variable | Default | Description |
|----------|---------|-------------|
| `JWT_SECRET` | (required) | HS256 signing key |
| `PG_HOST` / `PG_PORT` / `PG_USER` / `PG_PASSWORD` / `PG_DATABASE` | localhost defaults | Target PostgreSQL |
| `LISTEN_PORT` | `:15432` | Proxy listen address |
| `METRICS_PORT` | `:2112` | Prometheus metrics |
| `STATEMENT_TIMEOUT` | `30s` | Per-query timeout |
| `MAX_FIELD_BYTES` | `1048576` | Truncate fields over 1MB |
| `CURSOR_BATCH_SIZE` | `100` | Rows per FETCH |
| `POOL_ENABLED` | `false` | Enable connection pooling |
| `REGION` | (empty) | Region tag for routing/logging |
