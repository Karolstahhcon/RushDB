#!/usr/bin/env python3
"""Create a complete mapping ledger for pinned LDBC SNB Cypher query inputs.

LDBC SNB is a public workload input for RushDB's GQL profile, not an audited
benchmark. The report accounts for every reference Cypher query in the pinned
implementation repository. A query is accepted only when an executable RushDB
mapping names that exact source file and checksum; all other queries remain
explicit mapping gaps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path


UPSTREAM_COMMIT = "11db98cc2ba14c33492f6c0c34e68c8be7e22e5f"
UPSTREAM_REPOSITORY = "https://github.com/ldbc/ldbc_snb_interactive_v1_impls"
DEFAULT_MAPPING = (
    Path(__file__).resolve().parent.parent
    / "crates/rushdb-language/conformance/gql-ldbc-snb-v1-mapping.json"
)


def git_head(source: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="pinned LDBC checkout")
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING, help="RushDB mapping manifest")
    parser.add_argument("--output", type=Path, required=True, help="ledger JSON path")
    args = parser.parse_args()

    source = args.source.resolve()
    query_root = source / "cypher" / "queries"
    if not query_root.is_dir():
        raise SystemExit(f"not an LDBC SNB Cypher implementation checkout: {source}")
    commit = git_head(source)
    if commit != UPSTREAM_COMMIT:
        raise SystemExit(f"expected LDBC source commit {UPSTREAM_COMMIT}, found {commit}")

    mapping = json.loads(args.mapping.read_text(encoding="utf-8"))
    source_mapping = mapping.get("source", {})
    if source_mapping.get("repository") != UPSTREAM_REPOSITORY or source_mapping.get("commit") != commit:
        raise SystemExit("mapping manifest does not match the pinned LDBC source")
    classified: dict[str, dict] = {}
    for case in mapping.get("cases", []):
        if not isinstance(case, dict) or case.get("status") not in {"accepted", "typedUnsupported"}:
            continue
        path = case.get("sourcePath")
        sha = case.get("sourceSha256")
        if not isinstance(path, str) or not isinstance(sha, str):
            raise SystemExit(f"classified mapping {case.get('id')!r} lacks an exact source path and checksum")
        if path in classified:
            raise SystemExit(f"duplicate classified source mapping: {path}")
        if case["status"] == "typedUnsupported" and not isinstance(case.get("reason"), str):
            raise SystemExit(f"typed-unsupported mapping {case.get('id')!r} lacks a reason")
        classified[path] = {
            "id": case.get("id"),
            "sha256": sha,
            "status": case["status"],
            "reason": case.get("reason"),
        }

    ledger = []
    digest = hashlib.sha256()
    for query in sorted(query_root.glob("*.cypher")):
        relative = query.relative_to(source).as_posix()
        sha = hashlib.sha256(query.read_bytes()).hexdigest()
        mapped = classified.get(relative)
        if mapped is not None:
