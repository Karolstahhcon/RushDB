#!/usr/bin/env bash
# Check every traversal of the Gremlin profile
# (crates/rushdb-language/conformance/gremlin-profile-v1.json) against the
# official TinkerPop ANTLR grammar it pins. Needs a JDK and network access.
#
# usage: scripts/validate-gremlin-grammar.sh <work-dir>
set -euo pipefail

work="${1:?usage: validate-gremlin-grammar.sh <work-dir>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
profile="$root/crates/rushdb-language/conformance/gremlin-profile-v1.json"
antlr_version=4.13.2
