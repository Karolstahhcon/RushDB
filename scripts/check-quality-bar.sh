#!/usr/bin/env bash
set -euo pipefail

# This is a ratchet, not a claim that the current files are ideally sized.
# Refactors may lower a ceiling; a new growth in a known hotspot must be
# intentional and reviewed instead of silently increasing architectural debt.
declare -A LIMITS=(
  [crates/rushdb-server/src/node.rs]=1900
  # 3.0 baseline: the Catalog impl is cohesive but oversized; extraction is
  # tracked separately, so ratchet it here instead of letting it grow.
  [crates/rushdb-server/src/node/catalog.rs]=1700
  [crates/rushdb-server/src/protocol.rs]=1300
  [crates/rushdb-query/src/lib.rs]=2250
  [crates/rushdb-storage/src/lib.rs]=2100
)

for file in "${!LIMITS[@]}"; do
  lines="$(wc -l < "$file")"
  if (( lines > LIMITS[$file] )); then
    printf 'quality bar: %s grew to %s lines (ceiling %s)\n' \
      "$file" "$lines" "${LIMITS[$file]}" >&2
    exit 1
  fi
done

# Extracted production modules must remain focused. Test fixtures have their
# own files and do not consume a production module's size allowance.
for file in crates/rushdb-server/src/node/*.rs crates/rushdb-server/src/protocol/*.rs \
  crates/rushdb-query/src/{planner,aggregation,property_predicate,equality,cycle}.rs \
  crates/rushdb-storage/src/{commit,maintenance,transaction_view,health,publish,conflict_window,fair_gate,persist,derive,tenant_dir}.rs; do
  case "$file" in
    */tests.rs|*_tests.rs) continue ;;
    # Ratcheted in LIMITS above; exempt from the 1300 extracted-module limit.
