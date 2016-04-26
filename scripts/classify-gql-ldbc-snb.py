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
