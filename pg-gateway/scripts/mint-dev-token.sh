#!/usr/bin/env bash
# Mint a dev JWT signed with the same secret as the dev docker-compose.
# Requires: python3 (stdlib only) OR node
# Usage: ./scripts/mint-dev-token.sh [sub] [ttl_seconds]
#
# Examples:
#   curl -H "Authorization: Bearer $(./scripts/mint-dev-token.sh)" ...
#   ./scripts/mint-dev-token.sh alice 3600

SECRET="${JWT_SECRET:-dev-secret-change-me}"
SUB="${1:-dev-user}"
TTL="${2:-3600}"

python3 - <<EOF
import base64, hashlib, hmac, json, time, sys

secret = "$SECRET".encode()
sub = "$SUB"
ttl = int("$TTL")

now = int(time.time())
header = base64.urlsafe_b64encode(json.dumps({"alg":"HS256","typ":"JWT"}).encode()).rstrip(b"=")
payload = base64.urlsafe_b64encode(json.dumps({"sub":sub,"iat":now,"exp":now+ttl}).encode()).rstrip(b"=")
msg = header + b"." + payload
sig = base64.urlsafe_b64encode(hmac.new(secret, msg, hashlib.sha256).digest()).rstrip(b"=")
print((msg + b"." + sig).decode())
EOF
