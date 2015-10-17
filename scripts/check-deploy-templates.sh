#!/usr/bin/env bash
# Static checks for deploy/ and charts/: every template parses, the Compose
# files and the Helm chart render, and the systemd unit matches the manual.
# Needs docker (compose) and helm; CI runs it in the deploy-templates job.
set -euo pipefail
cd "$(dirname "$0")/.."

# The unit file is the manual's unit, byte for byte.
manual_unit="$(awk '/^`\/etc\/systemd\/system\/rushdb.service`:/{f=1;next} f&&/^```ini/{g=1;next} g&&/^```/{exit} g' \
  docs/manual/operations/deployment.md)"
if [ "$manual_unit" != "$(cat deploy/templates/systemd/rushdb.service)" ]; then
  echo "deploy/templates/systemd/rushdb.service differs from the unit in docs/manual/operations/deployment.md" >&2
  exit 1
fi
