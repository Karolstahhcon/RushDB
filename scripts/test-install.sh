#!/usr/bin/env bash
# Run install.sh the way users do (`curl ... | sh`) inside clean Linux
# containers against a locally served release directory, then start the
# installed database and write a record through it.
#
#   scripts/test-install.sh RELEASE_DIR [IMAGE...]
#
# RELEASE_DIR holds rushdb-<target>.tar.gz and its .sha256 for this machine's
# architecture. Images default to supported glibc distributions plus two that
# must be refused with a clear message (musl, and a glibc older than 2.28).
set -euo pipefail

RELEASE_DIR="$(cd "${1:?usage: test-install.sh RELEASE_DIR [IMAGE...]}" && pwd)"
shift
IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
  IMAGES=(debian:12 ubuntu:20.04 rockylinux:8 amazonlinux:2023 alpine:3 centos:7)
fi
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVE="$(mktemp -d)"
PORT="$((40000 + RANDOM % 10000))"
SERVER=""
# shellcheck disable=SC2329 # called by the EXIT trap
cleanup() {
  if [ -n "$SERVER" ]; then kill "$SERVER" 2>/dev/null || true; fi
  rm -rf "$SERVE"
}
trap cleanup EXIT

cp "$ROOT/install.sh" "$RELEASE_DIR"/rushdb-*.tar.gz "$RELEASE_DIR"/rushdb-*.tar.gz.sha256 "$SERVE/"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$SERVE" >/dev/null 2>&1 &
SERVER="$!"
for _ in $(seq 1 50); do
  curl -fs "http://127.0.0.1:$PORT/install.sh" >/dev/null && break
  sleep 0.1
done

# The script each container runs. $1 = served base URL, $2 = expected outcome.
