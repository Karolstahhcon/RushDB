#!/usr/bin/env bash
# Smoke-test a RushDB container image: it turns healthy, accepts a durable
# write that survives a restart, and refuses to start without a token.
#
#   scripts/smoke-image.sh IMAGE [PLATFORM]
set -euo pipefail

IMAGE="${1:?usage: smoke-image.sh IMAGE [PLATFORM]}"
PLATFORM="${2:-}"
NAME="rushdb-smoke-$$"
PORT="$((40000 + RANDOM % 10000))"
PLATFORM_ARGS=()
[ -n "$PLATFORM" ] && PLATFORM_ARGS=(--platform "$PLATFORM")
# shellcheck disable=SC2329 # called by the EXIT trap
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_healthy() {
  for _ in $(seq 1 90); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$NAME")" = healthy ] && return 0
    sleep 1
  done
  docker logs "$NAME" >&2
  echo "the container never became healthy" >&2
