#!/bin/bash
# Combined entrypoint: starts Postgres then pg-gateway.
# Drop-in replacement for the official postgres docker-entrypoint.sh,
# which handles first-run initialization automatically.
set -e

# Forward pg env vars into pg-gateway config so they stay in sync.
export PG_HOST="${PG_HOST:-127.0.0.1}"
export PG_PORT="${PG_PORT:-5432}"
export PG_USER="${PG_USER:-${POSTGRES_USER:-postgres}}"
export PG_PASSWORD="${PG_PASSWORD:-${POSTGRES_PASSWORD:-}}"
export PG_DATABASE="${PG_DATABASE:-${POSTGRES_DB:-postgres}}"
export LISTEN_PORT="${LISTEN_PORT:-:15432}"
export METRICS_PORT="${METRICS_PORT:-:2112}"

# Start Postgres using the official entrypoint (handles initdb on first run).
docker-entrypoint.sh postgres &
PG_PID=$!

# Wait for Postgres to accept connections.
echo "[entrypoint] waiting for postgres..."
until pg_isready -h 127.0.0.1 -p "${PG_PORT:-5432}" -U "${PG_USER}" -q 2>/dev/null; do
  sleep 0.5
done
echo "[entrypoint] postgres is ready"

# Start pg-gateway.
/usr/local/bin/pg-gateway &
GW_PID=$!
echo "[entrypoint] pg-gateway listening on ${LISTEN_PORT}"

# Exit when either process dies.
wait -n $PG_PID $GW_PID
EXIT_CODE=$?
echo "[entrypoint] a process exited (code=$EXIT_CODE), shutting down"
kill $PG_PID $GW_PID 2>/dev/null || true
exit $EXIT_CODE
