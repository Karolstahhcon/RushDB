#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-./target/release/rushdb}"
OUT="${2:-dist/capabilities.json}"
PORT="${RUSHDB_CAPABILITIES_PORT:-39117}"
DATA_DIR="$(mktemp -d)"
TOKEN="capability-snapshot-token"
PID=""

cleanup() {
  if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$(dirname "$OUT")"
"$BIN" serve --port "$PORT" --pgwire-port 0 --data-dir "$DATA_DIR" --token "$TOKEN" >"$DATA_DIR/node.log" 2>&1 &
PID="$!"
for _ in $(seq 1 100); do
  if curl --fail --silent "http://127.0.0.1:$PORT/ready" >/dev/null; then break; fi
  sleep 0.05
done
curl --fail --silent "http://127.0.0.1:$PORT/ready" >/dev/null

