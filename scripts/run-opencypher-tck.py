#!/usr/bin/env python3
"""Run the pinned openCypher TCK against a Protocol v1 server.

Every scenario of every feature (Scenario Outlines expanded per Examples row)
runs on an isolated graph: the tenant is purged, the scenario's graph and
setup statements are executed, its query runs, and its result table, error
expectation and side effects are compared with the upstream expectations.

Each scenario ends `pass` or `fail` with a category (see `categorize`). The
outcome of every scenario is compared with the committed ledger
(`--ledger`): any difference fails the run, so a regression cannot land and
an improvement must update the ledger (`--write-ledger`). The profile file
explains every failure category, as an `exclusion` (a feature outside the
supported Cypher profile, advertised as a capability limitation) or a
`defect` (a wrong result of the embedded Grafeo engine); a failure whose
category it does not explain fails the run.

With `--reference` (CI passes it), every defect failure is rerun on plain
Grafeo (`crates/rushdb-language/examples/grafeo_reference.rs`: Grafeo's own
store, no LMPG, no RushDB adapter). A scenario that passes there fails
because of RushDB: it becomes `rushdb-adapter`, which the profile never
explains, unless its rule is `confirmed` upstream with a reason.

Comparison rules (documented in docs/QUERY_LANGUAGE_CONFORMANCE.md):
- an expected error passes when the statement is refused with any error
  (the TCK's error type is not compared);
- values compare by TCK semantics: integers and floats are distinct, nodes
  compare by labels and properties, relationships by type and properties,
  lists in order unless the scenario ignores list element order.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

UPSTREAM_COMMIT = "59edf2e1c17b845bf97c334ed06b2eb780950c13"
CONFORMANCE = Path(__file__).resolve().parent.parent / "crates/rushdb-server/conformance"
DEFAULT_LEDGER = CONFORMANCE / "opencypher-tck-v2024.2-ledger.json"
DEFAULT_PROFILE = CONFORMANCE / "opencypher-tck-v2024.2-profile.json"


# ---------------------------------------------------------------- Gherkin


@dataclass
class Step:
    keyword: str
    text: str
    doc: str | None = None
    table: list[list[str]] | None = None


@dataclass
class Scenario:
    feature: str
    name: str
    steps: list[Step] = field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.feature}::{self.name}"


def table_row(line: str) -> list[str]:
    cells, current, escaped = [], "", False
    body = line.strip()[1:]
    for char in body:
        if escaped:
            # Gherkin cell escapes: \| \\ \n; any other pair stays as is.
            current += {"|": "|", "\\": "\\", "n": "\n"}.get(char, "\\" + char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            cells.append(current.strip())
            current = ""
        else:
            current += char
    return cells


def parse_feature(path: Path, relative: str) -> list[Scenario]:
    lines = path.read_text(encoding="utf-8").splitlines()
    scenarios: list[Scenario] = []
    i = 0
    current: Scenario | None = None
    outline: tuple[Scenario, list[list[str]]] | None = None
    in_examples = False

    def flush_outline() -> None:
        nonlocal outline
        if outline is None:
            return
        template, rows = outline
        if rows:
            header, *values = rows
            for index, row in enumerate(values, start=1):
                binding = dict(zip(header, row))

                def bind(text: str | None) -> str | None:
                    if text is None:
                        return None
                    return re.sub(r"<([^<>]+)>", lambda m: binding.get(m.group(1), m.group(0)), text)

                steps = [
                    Step(
                        s.keyword,
                        bind(s.text) or "",
                        bind(s.doc),
                        [[bind(c) or "" for c in r] for r in s.table] if s.table else None,
                    )
                    for s in template.steps
                ]
                scenarios.append(Scenario(template.feature, f"{template.name} #{index}", steps))
        outline = None

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("Scenario Outline:") or stripped.startswith("Scenario:"):
            flush_outline()
            if current is not None:
                scenarios.append(current)
                current = None
            name = stripped.split(":", 1)[1].strip()
            scenario = Scenario(relative, name)
            if stripped.startswith("Scenario Outline:"):
                outline = (scenario, [])
            else:
                current = scenario
            in_examples = False
        elif stripped.startswith("Examples:"):
            in_examples = True
        elif in_examples and stripped.startswith("|") and outline is not None:
            outline[1].append(table_row(stripped))
        elif re.match(r"^(Given|When|Then|And|But)\b", stripped):
            target = current if current is not None else (outline[0] if outline else None)
            keyword, _, text = stripped.partition(" ")
            step = Step(keyword, text.rstrip(":").strip())
            # Doc string or table follows.
            j = i + 1
            if j < len(lines) and lines[j].strip().startswith('"""'):
                indent = len(lines[j]) - len(lines[j].lstrip())
                body = []
                j += 1
                while not lines[j].strip().startswith('"""'):
                    body.append(lines[j][indent:] if len(lines[j]) >= indent else lines[j].strip())
                    j += 1
                step.doc = "\n".join(body).strip()
                i = j
            elif j < len(lines) and lines[j].strip().startswith("|"):
                rows = []
                while j < len(lines) and lines[j].strip().startswith("|"):
                    rows.append(table_row(lines[j]))
                    j += 1
                step.table = rows
                i = j - 1
            if target is not None:
                target.steps.append(step)
        i += 1
    flush_outline()
    if current is not None:
        scenarios.append(current)
    return scenarios


# ------------------------------------------------------------ TCK values


class ValueParser:
    """Recursive-descent parser for TCK result cells and parameter values."""

    def __init__(self, text: str):
        self.text = text
        self.pos = 0

    def parse(self):
        value = self.value()
        self.ws()
        if self.pos != len(self.text):
            raise ValueError(f"trailing input in TCK value {self.text!r}")
        return value

    def ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def peek(self) -> str:
        self.ws()
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def expect(self, char: str) -> None:
        self.ws()
        if not self.text.startswith(char, self.pos):
            raise ValueError(f"expected {char!r} at {self.pos} in {self.text!r}")
        self.pos += len(char)

    def value(self):
        char = self.peek()
        if char == "'":
            return self.string()
        if char == "[":
            return self.list_or_relationship()
        if char == "{":
            return ("map", self.map_body())
        if char == "(":
            return self.node()
        if char == "<":
            return self.path()
        return self.scalar()

    def string(self) -> str:
        self.expect("'")
        out = []
        while self.text[self.pos] != "'":
            if self.text[self.pos] == "\\":
                self.pos += 1
                out.append({"n": "\n", "t": "\t"}.get(self.text[self.pos], self.text[self.pos]))
            else:
                out.append(self.text[self.pos])
            self.pos += 1
        self.pos += 1
        return out_str("".join(out))

    def scalar(self):
        self.ws()
        match = re.compile(r"-?(?:\d+\.\d*(?:[eE][-+]?\d+)?|\.\d+(?:[eE][-+]?\d+)?|\d+[eE][-+]?\d+|0x[0-9a-fA-F]+|0o[0-7]+|\d+|Infinity|NaN)|-Infinity|true|false|null").match(self.text, self.pos)
        if not match:
            raise ValueError(f"cannot parse TCK value at {self.pos} in {self.text!r}")
        self.pos = match.end()
        token = match.group(0)
        if token == "null":
            return None
        if token in {"true", "false"}:
            return token == "true"
        if token in {"Infinity", "-Infinity", "NaN"}:
            return ("float", float(token.replace("Infinity", "inf")))
        if token.lstrip("-").startswith("0x"):
            return int(token, 16)
        if token.lstrip("-").startswith("0o"):
            return int(token.replace("0o", ""), 8)
        if re.fullmatch(r"-?\d+", token):
            return int(token)
        return ("float", float(token))

    def map_body(self) -> tuple:
        self.expect("{")
        entries = []
        if self.peek() != "}":
            while True:
                self.ws()
                key = re.compile(r"`[^`]*`|[A-Za-z_][A-Za-z0-9_]*").match(self.text, self.pos)
                if not key:
                    raise ValueError(f"bad map key in {self.text!r}")
                self.pos = key.end()
                self.expect(":")
                entries.append((key.group(0).strip("`"), self.value()))
                if self.peek() == ",":
                    self.pos += 1
                    continue
                break
        self.expect("}")
        return tuple(sorted(entries, key=lambda e: e[0]))

    def labels(self) -> tuple:
        labels = []
        while self.peek() == ":":
            self.pos += 1
            self.ws()
            match = re.compile(r"`[^`]*`|[A-Za-z_][A-Za-z0-9_]*").match(self.text, self.pos)
            self.pos = match.end()
            labels.append(match.group(0).strip("`"))
        return tuple(sorted(labels))

    def node(self):
        self.expect("(")
        labels = self.labels()
        props = self.map_body() if self.peek() == "{" else ()
        self.expect(")")
        return ("node", labels, props)

    def list_or_relationship(self):
        self.expect("[")
        if self.peek() == ":":
            labels = self.labels()
            props = self.map_body() if self.peek() == "{" else ()
            self.expect("]")
            return ("rel", labels[0] if labels else "", props)
        items = []
        if self.peek() != "]":
            while True:
                items.append(self.value())
                if self.peek() == ",":
                    self.pos += 1
                    continue
                break
        self.expect("]")
        return ("list", tuple(items))

    def path(self):
        self.expect("<")
        parts = [self.node()]
        while self.peek() in {"-", "<"}:
            backwards = False
            if self.text.startswith("<-", self.pos):
                backwards = True
                self.pos += 2
            else:
                self.expect("-")
            rel = self.list_or_relationship()
            if self.text.startswith("->", self.pos):
                self.pos += 2
            else:
                self.expect("-")
            parts.append((rel, backwards))
            parts.append(self.node())
        self.expect(">")
        return ("path", tuple(parts))


class out_str(str):
    """A parsed TCK string (distinct from map keys and identifiers)."""


def tck_value(text: str):
    return ValueParser(text).parse()


# The engine emits protocol record shapes (`__id`, `__label`, `__type`,
# `__source_id`, `__target_id`); the Grafeo reference emits the harness shapes
# (`_id`, `__labels`, `type`, `source`, `target`). Normalise both to the TCK
# value model, where entity ids are not part of a result.
NODE_ID_KEYS = {"_id", "__id"}
NODE_LABEL_KEYS = {"__label", "__labels"}
REL_ENTITY_KEYS = {
    "_id",
    "__id",
    "type",
    "__type",
    "source",
    "__source_id",
    "target",
    "__target_id",
}


def entity_id(entity: dict):
    return entity.get("__id", entity.get("_id"))


def entity_labels(entity: dict) -> list:
    if "__labels" in entity:
        return list(entity["__labels"])
    label = entity.get("__label")
    return [label] if label else []


def is_node(entity: dict) -> bool:
    return entity_id(entity) is not None and any(key in entity for key in NODE_LABEL_KEYS)


def is_relationship(entity: dict) -> bool:
    return (
        entity_id(entity) is not None
        and ("type" in entity or "__type" in entity)
        and ("source" in entity or "__source_id" in entity)
        and ("target" in entity or "__target_id" in entity)
    )


def from_json(value):
    """A Protocol v1 JSON value in the TCK value model."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return ("float", value)
    if isinstance(value, str):
        return out_str(value)
    if isinstance(value, list):
        return ("list", tuple(from_json(v) for v in value))
    if isinstance(value, dict):
        if is_node(value):
            props = tuple(
                sorted(
                    (k, from_json(v))
                    for k, v in value.items()
                    if k not in NODE_ID_KEYS | NODE_LABEL_KEYS
                )
            )
            labels = tuple(sorted(label for label in entity_labels(value) if label))
            return ("node", labels, props)
        if is_relationship(value):
            props = tuple(
                sorted((k, from_json(v)) for k, v in value.items() if k not in REL_ENTITY_KEYS)
            )
            return ("rel", value.get("type", value.get("__type")), props)
        if set(value.keys()) == {"nodes", "edges"} and isinstance(value["nodes"], list):
            nodes = [from_json(n) for n in value["nodes"]]
            edges = [from_json(e) for e in value["edges"]]
            parts = [nodes[0]] if nodes else []
            for index, edge in enumerate(edges):
                backwards = False
                raw = value["edges"][index]
                raw_nodes = value["nodes"]
                if isinstance(raw, dict) and isinstance(raw_nodes[index], dict):
                    source = raw.get("source", raw.get("__source_id"))
                    backwards = source != entity_id(raw_nodes[index])
                parts.append((edge, backwards))
                parts.append(nodes[index + 1] if index + 1 < len(nodes) else None)
            return ("path", tuple(parts))
        return ("map", tuple(sorted((k, from_json(v)) for k, v in value.items())))
    raise ValueError(f"unsupported JSON value {value!r}")


def unordered_lists(value):
    if isinstance(value, tuple) and value and value[0] == "list":
        return ("list", tuple(sorted((unordered_lists(v) for v in value[1]), key=repr)))
    if isinstance(value, tuple) and value and value[0] == "map":
        return ("map", tuple((k, unordered_lists(v)) for k, v in value[1]))
    return value


def same(expected, actual) -> bool:
    if isinstance(expected, tuple) and isinstance(actual, tuple):
        if expected[:1] == ("float",) and actual[:1] == ("float",):
            a, b = expected[1], actual[1]
            if math.isnan(a) and math.isnan(b):
