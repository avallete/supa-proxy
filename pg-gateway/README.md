# pg-gateway

**A fork of [Neon wsproxy](https://github.com/neondatabase/wsproxy) with JWT auth,
credential injection, and a streaming JSON SQL endpoint.**

Single static Go binary. Zero dependencies. Replaces `postgres-meta /query`.

## What it does

```
┌─────────────────────────────────────────────────────────────────────┐
│  pg-gateway (single binary, one process)                            │
│                                                                     │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐ │
│  │  WS /v1  (wsproxy-compatible)│  │  POST /sql  (JSON endpoint) │ │
│  │                              │  │                              │ │
│  │  Browser opens WebSocket     │  │  Browser does fetch()        │ │
│  │  → JWT validated             │  │  → JWT validated             │ │
│  │  → TCP conn to PG opened    │  │  → PG conn opened with       │ │
│  │    with SERVER-SIDE creds    │  │    server-side creds         │ │
│  │  → raw PG wire protocol     │  │  → cursor-based streaming    │ │
│  │    bytes forwarded 1:1       │  │  → NDJSON rows flushed       │ │
│  │  → zero-copy, zero-buffer   │  │    one at a time             │ │
│  │                              │  │  → large values truncated    │ │
│  │  Client: @neondatabase/      │  │    at configurable limit     │ │
│  │          serverless driver   │  │                              │ │
│  │  Use: SQL editor, full PG    │  │  Client: any fetch() caller  │ │
│  │       protocol fidelity      │  │  Use: AI builders, simple    │ │
│  │                              │  │       integrations, scripts  │ │
│  └──────────────────────────────┘  └──────────────────────────────┘ │
│                                                                     │
│  GET /health                                                        │
│  GET /metrics  (prometheus, from original wsproxy)                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Deployment modes

| Mode | How pg-gateway runs | Dashboard talks to |
|------|--------------------|--------------------|
| **Local dev** | In PG Docker container (entrypoint) or compose sidecar | `localhost:15432` directly |
| **Self-hosted** | systemd unit alongside PG on the same machine | `<host>:15432` directly |
| **Hosted platform** | On each PG instance (AMI), always-whitelisted | mgmt-api relays, or direct if no IP restrictions |

## Endpoints

### `WS /v1?address=host:port` — PG wire protocol relay (wsproxy-compatible)

Identical to wsproxy behavior, plus:
- **JWT auth**: `Authorization: Bearer <JWT>` header on the WS upgrade request
- **Credential injection**: the proxy rewrites the PG StartupMessage, replacing
  the username/password with server-side credentials from config
- **Dynamic routing**: `?address=` param chooses the backend. Validated against
  `ALLOW_ADDR_REGEX`. For single-DB deployments, use `APPEND_PORT` to hardcode
  the target and skip the param.

Client code (browser):
```js
import { Pool, neonConfig } from '@neondatabase/serverless';

neonConfig.wsProxy = (host, port) => `pg-gateway.example.com/v1`;
neonConfig.useSecureWebSocket = true;  // wss:// in prod
neonConfig.pipelineConnect = false;

const pool = new Pool({
  connectionString: 'postgres://user:pass@dbhost:5432/mydb',
  // In hosted mode, these creds are injected server-side;
  // the values here are ignored and can be dummies
});

const { rows } = await pool.query('SELECT * FROM users LIMIT 10');
```

### `POST /sql` — Streaming JSON SQL endpoint

For simple integrations where you don't want a PG driver in the browser.

Request:
```json
POST /sql
Authorization: Bearer <JWT>
Content-Type: application/json

{"query": "SELECT * FROM large_table", "db": "mydb"}
```

Response (NDJSON, `Transfer-Encoding: chunked`):
```
{"columns":[{"name":"id","type_oid":23},{"name":"data","type_oid":25}]}
{"row":{"id":1,"data":"small value"}}
{"row":{"id":2,"data":"[truncated: 2147483648 bytes, showing first 65536]Ymlu..."}}
{"row":{"id":3,"data":"another value"}}
{"complete":{"command":"SELECT","row_count":3,"duration_ms":42.1}}
```

Key behaviors:
- **Streaming**: each row flushed immediately, never buffered in full
- **Cursor-based**: `DECLARE CURSOR` + `FETCH N` so PG doesn't materialize all rows
- **Large value truncation**: fields > `MAX_FIELD_BYTES` (default 1MB) are truncated
  with a marker. The full raw bytes are never read into memory — we `io.CopyN` up to
  the limit from the PG connection, then discard the rest.
- **Per-field streaming**: each column value is read and written independently. A row
  with a 2GB bytea and a 10-byte text costs ~1MB peak memory (the truncation buffer),
  not 2GB.

### `GET /health`

```json
{"status":"ok","version":"0.1.0"}
```

When `REGION` is set:
```json
{"status":"ok","version":"0.1.0","region":"ap-southeast-1"}
```

## Configuration (env vars)

| Var | Default | Description |
|-----|---------|-------------|
| `LISTEN_PORT` | `:15432` | Listen address |
| `JWT_SECRET` | (required) | HS256 shared secret |
| `PG_HOST` | `127.0.0.1` | Default PG host |
| `PG_PORT` | `5432` | Default PG port |
| `PG_USER` | `postgres` | PG user (server-side, never exposed) |
| `PG_PASSWORD` | (required) | PG password (server-side) |
| `PG_DATABASE` | `postgres` | Default database |
| `ALLOW_ADDR_REGEX` | `.*` | Regex for allowed `?address=` targets |
| `APPEND_PORT` | | Fixed target (e.g. `127.0.0.1:5432`), ignores `?address=` |
| `STATEMENT_TIMEOUT` | `30s` | Per-query timeout |
| `MAX_FIELD_BYTES` | `1048576` | Truncation limit per field (1MB) |
| `MAX_ROWS` | `0` | Row limit (0 = unlimited, streaming) |
| `CURSOR_BATCH_SIZE` | `100` | Rows per FETCH batch |
| `ALLOWED_ORIGINS` | `*` | CORS origins |
| `LOG_QUERIES` | `false` | Log SQL to stdout |
| `LOG_TRAFFIC` | `false` | Log raw wire protocol traffic |
| `METRICS_PORT` | `:2112` | Prometheus metrics |
| `REGION` | | Region identifier (e.g. `ap-southeast-1`), included in `/health` |
| `POOL_ENABLED` | `false` | Enable connection pooling for `/sql` |
| `POOL_MAX_CONNS` | `20` | Max connections per target database |
| `POOL_MIN_CONNS` | `2` | Min idle connections per target database |
| `POOL_MAX_CONN_LIFETIME` | `30m` | Max lifetime of a pooled connection |
| `POOL_MAX_CONN_IDLE_TIME` | `5m` | Max idle time before a connection is closed |

## Connection Pooling

Enable with `POOL_ENABLED=true`. Only the `/sql` endpoint uses pooled connections;
`/v1` (WebSocket relay) always opens a raw TCP passthrough.

- **Per-target pools**: each unique `host:port/db` gets its own `pgxpool.Pool`
- **Session reset**: `DISCARD ALL` runs on every connection release (resets ROLE, GUCs, temp tables, prepared statements, LISTEN channels)
- **Idle eviction**: pools unused for 10 minutes are automatically closed (checked every 1 minute)
- **Lazy creation**: pools are created on first request, with double-checked locking

## Build

```bash
# Direct
go build -o pg-gateway .

# Via Makefile (stripped binary)
make build
```

## Testing

```bash
# Unit tests (no Docker required)
make test

# Integration tests (requires Docker — uses testcontainers)
make test-integration

# Bun E2E suite (requires Docker + Bun)
make test-e2e

# Stress tests (~5-10 min, manual only)
make test-stress

# Benchmarks
make bench
```

## Prometheus Metrics

All metrics are exposed on `METRICS_PORT` (default `:2112`) at `GET /metrics`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pgw_http_request_duration_seconds` | Histogram | `path`, `status` | HTTP request duration |
| `pgw_http_requests_total` | Counter | `path`, `status` | Total HTTP requests |
| `pgw_active_connections` | Gauge | `type` | Currently active connections |
| `pgw_query_duration_seconds` | Histogram | `command`, `status` | Query execution duration |
| `pgw_rows_processed_total` | Counter | `command` | Total rows processed |
| `pgw_truncated_fields_total` | Counter | — | Fields truncated by `MAX_FIELD_BYTES` |
| `pgw_relay_bytes_total` | Counter | `direction` | Bytes relayed (WS) |
| `pgw_relay_duration_seconds` | Histogram | — | WebSocket relay session duration |
| `pgw_auth_failures_total` | Counter | `reason` | Authentication failures |
| `pgw_db_errors_total` | Counter | `error_type` | Database errors |
| `pgw_pool_acquired_conns` | Gauge | `target` | Acquired connections from pool |
| `pgw_pool_idle_conns` | Gauge | `target` | Idle connections in pool |
| `pgw_pool_total_conns` | Gauge | `target` | Total connections in pool |

## Multi-tenant scaling

One pg-gateway instance can serve multiple databases:
- Each JWT carries a `db` claim (database name) and optionally `host`/`port` claims
- The `/sql` endpoint uses these to connect to the right PG instance
- The `/v1` WS endpoint uses `?address=host:port` (validated against `ALLOW_ADDR_REGEX`)
- Go's goroutine-per-connection model handles thousands of concurrent connections
- Add more instances behind a load balancer for horizontal scaling
