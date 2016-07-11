#!/usr/bin/env bash
# Fetch and convert the frozen vector-search calibration fixtures
# (docs/disk-backed-storage/11-filter-aware-ann.md §5.2 and
# 03-operations-migration-validation.md §6.8):
#
#   1. DBPedia-100K (Qdrant / OpenAI text-embedding-3-large, 1536-dim)
#      -> data/dbpedia/vectors.fvecs + data/dbpedia/payload.csv
#   2. SIFT1M (ann-benchmarks sift-128-euclidean, 128-dim)
#      -> data/sift1m/base.fvecs + data/sift1m/query.fvecs + data/sift1m/neighbors.ivecs
#
# Each fixture is written as `.fvecs` (uint32 dim + float32 rows) / `.ivecs`
# (uint32 k + uint32 ids) and a payload CSV for filter predicates, so the Rust
# benchmark loads them with no heavy deps. SHA-256 pins are recorded after the
# run in data/FIXTURES.sha256 (provenance for the frozen gate).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
DATA_DIR="$ROOT/data/vector-fixtures"
mkdir -p "$DATA_DIR/dbpedia" "$DATA_DIR/sift1m"

