#!/usr/bin/env bash
set -euo pipefail

# Validate a release archive as a consumer would: checksum, package contents,
# machine-readable capability evidence, and the documented one-command startup
# path. This deliberately uses only the archived binaries, never `target/`.

ARCHIVE="${1:?usage: verify-release-artifact.sh ARCHIVE CHECKSUM CAPABILITIES_JSON}"
CHECKSUM="${2:?usage: verify-release-artifact.sh ARCHIVE CHECKSUM CAPABILITIES_JSON}"
CAPABILITIES="${3:?usage: verify-release-artifact.sh ARCHIVE CHECKSUM CAPABILITIES_JSON}"
TMP_DIR="$(mktemp -d)"
PID=""
PORT="${RUSHDB_VERIFY_PORT:-$((30000 + RANDOM % 20000))}"
BASE_URL="http://127.0.0.1:${PORT}"

cleanup() {
  if [ -n "$PID" ]; then kill "$PID" 2>/dev/null || true; fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

[ -f "$ARCHIVE" ]
[ -f "$CHECKSUM" ]
[ -f "$CAPABILITIES" ]

if command -v shasum >/dev/null 2>&1; then
  (cd "$(dirname "$CHECKSUM")" && shasum -a 256 -c "$(basename "$CHECKSUM")")
else
  (cd "$(dirname "$CHECKSUM")" && sha256sum -c "$(basename "$CHECKSUM")")
fi

tar -xzf "$ARCHIVE" -C "$TMP_DIR"
for file in rushdb README.md LICENSE; do
  [ -e "$TMP_DIR/$file" ] || { echo "release archive is missing $file" >&2; exit 1; }
done
[ -x "$TMP_DIR/rushdb" ]
"$TMP_DIR/rushdb" --help >/dev/null
"$TMP_DIR/rushdb" serve --help >/dev/null

python3 - "$CAPABILITIES" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1]))
for name in ('build', 'capabilities', 'queryLanguages'):
    assert payload[name]['protocolVersion'] == 1, name
build = payload['build']['response']['build']
assert build['engine'] == 'rushdb-lmpg'
assert build['protocolVersion'] == 1
assert isinstance(payload['capabilities']['response']['capabilities'], list)
assert isinstance(payload['queryLanguages']['response']['languages'], list)
PY

"$TMP_DIR/rushdb" serve --port "$PORT" --pgwire-port 0 --data-dir "$TMP_DIR/data" >"$TMP_DIR/rushdb.log" 2>&1 &
PID="$!"
