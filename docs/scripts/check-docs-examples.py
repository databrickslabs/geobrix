#!/usr/bin/env python3
"""Structural guards for the RasterX documentation example tabs & output tables.

Two checks, both pure file parsing (host-only, no Docker, no Spark):

  1. OUTPUT TABLES — every ``*_example_output`` string constant in the doc-example
     modules renders width- and tick-consistent ASCII result tables. Catches orphan
     separator rows and mismatched column widths (the ``rst_bng_tessellate`` /
     ``rst_quadbin_tessellate`` output-table bug class).

  2. TAB COMPLETENESS — every ``<FunctionExamples name=X ...>`` on the raster-functions
     page renders one tab per tier listed in ``function-info.json`` ``functions[gbx_X].
     bindings``; for each declared tier the referenced source module must define BOTH the
     example symbol (``X<suffix>``) and its output constant (``X<suffix>_output``). Also
     flags single-tab ``<CodeFromTest>`` used on a function whose bindings declare more
     than one tier. Catches missing ``_output`` constants (``rst_rastertoworldcoordx``)
     and SQL-only-tab regressions (``gbx_h3_cell_bbox``).

Exit code: 0 when clean, 1 on any gap. Run via ``gbx:test:docs-examples`` (or directly).
Pure stdlib; runs on the host, no Docker.
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MDX = REPO_ROOT / "docs" / "docs" / "api" / "raster-functions.mdx"
FUNCTION_INFO = (
    REPO_ROOT / "src/main/resources/com/databricks/labs/gbx/function-info.json"
)
API_DIR = REPO_ROOT / "docs" / "tests" / "python" / "api"
SCALA_EXAMPLES = (
    REPO_ROOT / "docs" / "tests" / "scala" / "api" / "ScalaApiExamples.scala"
)

_BORDER = re.compile(r"^\+[-+]*\+$")  # +----+----+
_ROW = re.compile(r"^\|.*\|$")  # |..|..|

_SUFFIX = {
    "sql": "_sql_example",
    "python-light": "_python_light_example",
    "python-heavy": "_python_heavy_example",
    "scala": "_scala_example",
}
_PROP_TO_TIER = {
    "sqlSource": "sql",
    "pythonLightSource": "python-light",
    "pythonHeavySource": "python-heavy",
    "scalaSource": "scala",
}


# --------------------------------------------------------------------------- #
# Check 1: output-table well-formedness
# --------------------------------------------------------------------------- #
def _example_modules() -> list[Path]:
    files: set[Path] = set()
    files.update(API_DIR.glob("*_sql.py"))
    files.update(API_DIR.glob("rasterx_*python_light.py"))
    files.add(API_DIR / "rasterx_functions.py")
    return sorted(f for f in files if f.exists() and not f.name.startswith("test_"))


def _output_constants(path: Path):
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.endswith("_output"):
                        yield target.id, node.value.value


def _table_blocks(text: str):
    cur: list[str] = []
    for line in text.splitlines():
        s = line.rstrip()
        if _BORDER.match(s) or _ROW.match(s):
            cur.append(s)
        elif cur:
            yield cur
            cur = []
    if cur:
        yield cur


def _table_problems(block: list[str]) -> list[str]:
    problems: list[str] = []
    widths = {len(l) for l in block}
    if len(widths) > 1:
        problems.append(f"inconsistent line widths {sorted(widths)}")
    borders = [l for l in block if _BORDER.match(l)]
    rows = [l for l in block if _ROW.match(l) and not _BORDER.match(l)]
    if borders and rows:
        ticks = {i for i, c in enumerate(borders[0]) if c == "+"}
        for r in rows:
            pipes = {i for i, c in enumerate(r) if c == "|"}
            if pipes != ticks:
                problems.append(
                    f"column ticks misaligned: border {sorted(ticks)} != row {sorted(pipes)}"
                )
                break
    return problems


def check_output_tables() -> list[str]:
    failures: list[str] = []
    scanned = 0
    for path in _example_modules():
        for name, text in _output_constants(path):
            scanned += 1
            probs = []
            for block in _table_blocks(text):
                probs.extend(_table_problems(block))
            if probs:
                rel = path.relative_to(REPO_ROOT)
                failures.append(f"{rel}::{name}: " + "; ".join(probs))
    print(f"  scanned {scanned} *_example_output constants")
    return failures


# --------------------------------------------------------------------------- #
# Check 2: tab completeness
# --------------------------------------------------------------------------- #
_name_cache: dict[Path, set[str]] = {}


def _module_names(rel_path: str) -> set[str]:
    path = REPO_ROOT / rel_path
    if path in _name_cache:
        return _name_cache[path]
    names: set[str] = set()
    if path.exists():
        if path.suffix == ".py":
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.FunctionDef):
                    names.add(node.name)
                elif isinstance(node, ast.Assign):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            names.add(t.id)
        else:
            names.update(re.findall(r"\bval\s+([A-Za-z0-9_]+)\s*:", path.read_text()))
    _name_cache[path] = names
    return names


def _bindings_for(fns: dict, name: str) -> set[str]:
    entry = fns.get(name) or fns.get("gbx_" + name) or {}
    return set(entry.get("bindings", []))


def check_tab_completeness() -> list[str]:
    failures: list[str] = []
    if not FUNCTION_INFO.exists():
        print("  function-info.json absent — skipping tab-completeness check")
        return failures
    fns = json.loads(FUNCTION_INFO.read_text())["functions"]
    mdx = MDX.read_text()

    fe_checked = 0
    for block in re.findall(r"<FunctionExamples\b(.*?)/>", mdx, re.S):
        nm = re.search(r'name="([^"]+)"', block)
        if not nm:
            continue
        name = nm.group(1)
        tiers = _bindings_for(fns, name)
        if not tiers:
            failures.append(
                f"{name}: no bindings in function-info.json (no tabs render)"
            )
            continue
        sources = dict(re.findall(r'(\w+Source)="([^"]+)"', block))
        for prop, tier in _PROP_TO_TIER.items():
            if tier not in tiers:
                continue
            src = sources.get(prop)
            if not src:
                failures.append(f"{name}: binding '{tier}' present but no {prop} attr")
                continue
            example = name + _SUFFIX[tier]
            output = example + "_output"
            defined = _module_names(src)
            if example not in defined:
                failures.append(
                    f"{name}: tab '{tier}' missing example `{example}` in {src}"
                )
            if output not in defined:
                failures.append(
                    f"{name}: tab '{tier}' missing output `{output}` in {src}"
                )
            fe_checked += 1

    # single-tab CodeFromTest on a multi-tier function
    for block in re.findall(r"<CodeFromTest\b(.*?)/>", mdx, re.S):
        fn = re.search(r'functionName="([^"]+)"', block)
        if not fn:
            continue
        base = re.sub(
            r"_(sql|python_light|python_heavy|scala)_example$", "", fn.group(1)
        )
        tiers = _bindings_for(fns, base)
        if len(tiers) > 1:
            failures.append(
                f"{base}: single-tab <CodeFromTest> but bindings={sorted(tiers)} "
                f"(should be 4-tab <FunctionExamples>)"
            )
    print(f"  checked {fe_checked} declared FunctionExamples tabs")
    return failures


# --------------------------------------------------------------------------- #
# Check 3: trailing-annotation consistency across identical-output tabs
# --------------------------------------------------------------------------- #
# A doc-example output constant is an ASCII table optionally followed by ONE
# trailing prose annotation describing the output, e.g.
# "(aspect in compass degrees: 0=N, 90=E, ...)". Tabs of the SAME function whose
# output TABLE is byte-identical describe the same result, so they must carry the
# same annotation. Tabs whose tables legitimately differ (e.g. the rst_*_rastertogrid*
# family: flat rows vs ARRAY<ARRAY<struct>> vs Seq[Seq[Row]], or the SQL dual
# heavy/BINARY block) are never compared here — so this only flags true drift.
#
# A trailing "; light tier returns ..." (or heavyweight) clause is tier-specific and
# allowed to differ; it is stripped before comparison.

_TIER_CLAUSE = re.compile(r"^(the )?(light|heavy)(weight)?( tier)?\b", re.I)


def _split_output(text: str):
    lines = [ln.rstrip() for ln in text.strip().splitlines() if ln.strip()]
    table = "\n".join(ln for ln in lines if ln[:1] in ("+", "|"))
    ann = lines[-1] if lines and lines[-1][:1] not in ("+", "|", "#") else None
    return table, ann


def _normalize_annotation(ann):
    """Canonical form for comparison: drop surrounding parens, tier-specific
    clauses, case, whitespace, trailing period."""
    if ann is None:
        return None
    s = ann.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    parts = [p.strip() for p in s.split(";")]
    parts = [p for p in parts if p and not _TIER_CLAUSE.match(p)]
    s = "; ".join(parts)
    s = re.sub(r"\s+", " ", s).lower().rstrip(".")
    return s or None


def check_annotation_consistency() -> list[str]:
    from collections import defaultdict

    constants: dict[str, str] = {}
    for path in _example_modules():
        for name, text in _output_constants(path):
            constants[name] = text
    if SCALA_EXAMPLES.exists():
        stext = SCALA_EXAMPLES.read_text()
        for m in re.finditer(
            r'val\s+(\w+_output)\s*:\s*String\s*=\s*\n?\s*"""(.*?)"""', stext, re.S
        ):
            constants[m.group(1)] = m.group(2)

    groups: dict[str, dict] = defaultdict(dict)
    for name, text in constants.items():
        mo = re.search(r"_(sql|python_light|python_heavy|scala)_example_output$", name)
        if mo:
            groups[name[: mo.start()]][mo.group(1)] = _split_output(text)

    failures: list[str] = []
    compared = 0
    for base, tiers in sorted(groups.items()):
        by_table: dict[str, list] = defaultdict(list)
        for tier, (table, ann) in tiers.items():
            if table.strip():
                by_table[table].append((tier, ann))
        for table, members in by_table.items():
            if len(members) < 2:
                continue
            compared += 1
            cores = {_normalize_annotation(ann) for _, ann in members}
            if len(cores) > 1:
                detail = ", ".join(f"{t}={ann!r}" for t, ann in members)
                failures.append(
                    f"{base}: tabs with identical output table disagree on the trailing "
                    f"annotation — {detail}"
                )
    print(f"  compared {compared} identical-output tab groups")
    return failures


def main() -> int:
    print("Docs example guards (host-only):")
    print("• output tables")
    table_failures = check_output_tables()
    print("• tab completeness")
    tab_failures = check_tab_completeness()
    print("• annotation consistency")
    annotation_failures = check_annotation_consistency()

    all_failures = table_failures + tab_failures + annotation_failures
    print()
    if all_failures:
        print(f"❌ docs example guards FAILED — {len(all_failures)} issue(s):")
        for f in all_failures:
            print(f"   - {f}")
        return 1
    print(
        "✅ docs example guards OK — output tables well-formed, all declared tabs "
        "complete, annotations consistent."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
