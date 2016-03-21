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

