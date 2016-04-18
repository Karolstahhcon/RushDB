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
antlr_sha256=eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76

mkdir -p "$work/classes"
read -r tag path grammar_sha256 < <(python3 -I -c '
import json, sys
g = json.load(open(sys.argv[1]))["grammar"]
print(g["tag"], g["path"], g["sha256"])' "$profile")

curl -sSfL -o "$work/Gremlin.g4" \
  "https://raw.githubusercontent.com/apache/tinkerpop/$tag/$path"
curl -sSfL -o "$work/antlr.jar" \
  "https://repo1.maven.org/maven2/org/antlr/antlr4/$antlr_version/antlr4-$antlr_version-complete.jar"
echo "$grammar_sha256  $work/Gremlin.g4" | sha256sum -c --quiet
