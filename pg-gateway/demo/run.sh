#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  supa-proxy demo runner
#
#  What it does:
#    1. Compiles the pg-gateway binary
#    2. Starts two isolated PostgreSQL 16 containers (db-1 and db-2)
#    3. Starts two pg-gateway proxy instances pointing at each container
#    4. Installs Bun dependencies and runs demo.ts
#    5. Cleans up everything on exit
#
#  Prerequisites: docker, go, bun
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATEWAY_DIR="$SCRIPT_DIR/.."

# ── Tunables ──────────────────────────────────────────────────────────────────
JWT_SECRET="supa-proxy-demo-secret"
PG1_PORT=5441;   PG2_PORT=5442
PROXY1_PORT=8081; PROXY2_PORT=8082
PG_USER=testuser; PG_PASS=testpass; PG_DB=testdb
CONTAINER1=supa-proxy-demo-pg1; CONTAINER2=supa-proxy-demo-pg2
BINARY="/tmp/pg-gateway-demo-$$"
PROXY1_PID=""; PROXY2_PID=""

# ── Colors ────────────────────────────────────────────────────────────────────
R='\033[0m'; B='\033[1m'; G='\033[32m'; Y='\033[33m'; RE='\033[31m'; DIM='\033[2m'
step()  { echo -e "\n${B}${G}▶ $*${R}"; }
info()  { echo -e "  ${DIM}→ $*${R}"; }
fail()  { echo -e "${RE}✗ $*${R}" >&2; exit 1; }

# ── Prerequisites ─────────────────────────────────────────────────────────────
step "Checking prerequisites"
for cmd in docker go bun; do
  command -v "$cmd" &>/dev/null || fail "Required command not found: $cmd"
  info "$cmd $(${cmd} version 2>&1 | head -1)"
done

# ── Cleanup on exit ───────────────────────────────────────────────────────────
cleanup() {
  echo -e "\n${B}${Y}▶ Cleaning up…${R}"
  [[ -n "$PROXY1_PID" ]] && kill "$PROXY1_PID" 2>/dev/null && info "proxy-1 stopped"
  [[ -n "$PROXY2_PID" ]] && kill "$PROXY2_PID" 2>/dev/null && info "proxy-2 stopped"
  docker rm -f "$CONTAINER1" "$CONTAINER2" 2>/dev/null && info "containers removed"
  [[ -n "$BINARY" && -f "$BINARY" ]] && rm -f "$BINARY" && info "binary removed"
  rm -f "$LOG1" "$LOG2" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ── Build pg-gateway binary ───────────────────────────────────────────────────
step "Building pg-gateway"
(cd "$GATEWAY_DIR" && go build -o "$BINARY" .) \
  || fail "go build failed — check pg-gateway source"
info "Binary: $BINARY"

# ── Start PostgreSQL containers ───────────────────────────────────────────────
step "Starting PostgreSQL containers"

# Remove stale containers from a previous interrupted run
docker rm -f "$CONTAINER1" "$CONTAINER2" 2>/dev/null || true

docker run --rm -d --name "$CONTAINER1" \
  -e POSTGRES_USER="$PG_USER" -e POSTGRES_PASSWORD="$PG_PASS" -e POSTGRES_DB="$PG_DB" \
  -p "${PG1_PORT}:5432" \
  postgres:16-alpine -c "log_min_messages=FATAL" \
  > /dev/null
info "db-1 container started (port $PG1_PORT)"

docker run --rm -d --name "$CONTAINER2" \
  -e POSTGRES_USER="$PG_USER" -e POSTGRES_PASSWORD="$PG_PASS" -e POSTGRES_DB="$PG_DB" \
  -p "${PG2_PORT}:5432" \
  postgres:16-alpine -c "log_min_messages=FATAL" \
  > /dev/null
info "db-2 container started (port $PG2_PORT)"

# ── Helper: start one proxy instance ─────────────────────────────────────────
start_proxy() {
  local num="$1" http_port="$2" pg_port="$3" metrics_port="$4" log="$5"
  LISTEN_PORT=":${http_port}" \
  METRICS_PORT=":${metrics_port}" \
  JWT_SECRET="$JWT_SECRET" \
  PG_HOST=127.0.0.1 \
  PG_PORT="$pg_port" \
  PG_USER="$PG_USER" \
  PG_PASSWORD="$PG_PASS" \
  PG_DATABASE="$PG_DB" \
  APPEND_PORT="127.0.0.1:${pg_port}" \
  MAX_FIELD_BYTES=1048576 \
  CURSOR_BATCH_SIZE=100 \
  STATEMENT_TIMEOUT=30s \
  LOG_QUERIES=false \
    "$BINARY" > "$log" 2>&1 &
  echo $!
}

# ── Helper: wait for an HTTP endpoint to respond ──────────────────────────────
wait_for() {
  local url="$1" label="$2" deadline=$(( $(date +%s) + 60 ))
  info "Waiting for $label ($url) …"
  while ! curl -sf "$url" > /dev/null 2>&1; do
    [[ $(date +%s) -ge $deadline ]] && fail "Timeout waiting for $label"
    sleep 1
  done
  info "$label is ready"
}

# ── Start proxy instances ─────────────────────────────────────────────────────
step "Starting proxy instances"
LOG1="/tmp/supa-proxy1-$$.log"
LOG2="/tmp/supa-proxy2-$$.log"
PROXY1_PID="$(start_proxy 1 $PROXY1_PORT $PG1_PORT $((PROXY1_PORT+100)) "$LOG1")"
PROXY2_PID="$(start_proxy 2 $PROXY2_PORT $PG2_PORT $((PROXY2_PORT+100)) "$LOG2")"
info "proxy-1 pid=$PROXY1_PID  log=$LOG1"
info "proxy-2 pid=$PROXY2_PID  log=$LOG2"

# ── Wait for proxies to be ready (they in turn check PG connectivity) ─────────
step "Waiting for proxies to be ready"
# Proxies hit /ready which checks PG — so we wait for PG implicitly
wait_for "http://localhost:${PROXY1_PORT}/ready" "proxy-1"
wait_for "http://localhost:${PROXY2_PORT}/ready" "proxy-2"
echo -e "  ${G}✓${R}  Both proxies are healthy and connected to their databases"

# ── Quick smoke-test ──────────────────────────────────────────────────────────
step "Smoke test (/sql on both proxies)"
for port in $PROXY1_PORT $PROXY2_PORT; do
  token=$(JWT_SECRET="$JWT_SECRET" python3 - <<'EOF'
import base64, hashlib, hmac, json, time, os
secret = os.environ["JWT_SECRET"].encode()
now = int(time.time())
hdr = base64.urlsafe_b64encode(json.dumps({"alg":"HS256","typ":"JWT"}).encode()).rstrip(b"=")
pay = base64.urlsafe_b64encode(json.dumps({"sub":"run.sh","exp":now+3600}).encode()).rstrip(b"=")
msg = hdr + b"." + pay
sig = base64.urlsafe_b64encode(hmac.new(secret, msg, hashlib.sha256).digest()).rstrip(b"=")
print((msg + b"." + sig).decode())
EOF
)
  result=$(curl -sf -X POST "http://localhost:${port}/sql" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -d '{"query":"SELECT 1+1 AS n"}' | tail -2 | head -1)
  info "proxy on :${port}: $result"
done

# ── Install Bun deps and run the demo ─────────────────────────────────────────
step "Installing Bun dependencies"
cd "$SCRIPT_DIR"
bun install --frozen-lockfile --silent 2>/dev/null || bun install --silent
info "Dependencies ready"

step "Running demo.ts"
echo
export JWT_SECRET PROXY1_PORT PROXY2_PORT
bun run demo.ts

echo -e "\n${B}${G}Demo finished.${R}  Proxy logs:\n  proxy-1: $LOG1\n  proxy-2: $LOG2\n"
