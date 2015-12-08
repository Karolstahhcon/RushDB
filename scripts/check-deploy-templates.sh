#!/usr/bin/env bash
# Static checks for deploy/ and charts/: every template parses, the Compose
# files and the Helm chart render, and the systemd unit matches the manual.
# Needs docker (compose) and helm; CI runs it in the deploy-templates job.
set -euo pipefail
cd "$(dirname "$0")/.."

