"""Structural guard for the hand-written ASCII result tables in the doc-example
modules (the ``*_sql_example_output`` / ``*_python_*_example_output`` constants).

These constants are prose (rendered in the docs "output" panels), not executed, so
nothing else validates them — malformed tables (orphan separator rows, mismatched
column widths, misaligned ticks) slip through silently, as happened with
rst_bng_tessellate / rst_quadbin_tessellate. This test parses every example module,
extracts every ``*_output`` string constant, and asserts each contiguous ASCII table
block is well-formed. Pure-Python, no Spark — runs on host and in Docker.

If you intentionally have a non-table code fence or free-form text in an output
constant, that's fine: only lines that look like table borders (``+--+--+``) or
rows (``|..|..|``) are checked, and only when they form a contiguous block.
"""

import ast
import glob
import os
import re

import pytest

_HERE = os.path.dirname(__file__)


def _example_modules():
    pats = (
        os.path.join(_HERE, "*_sql.py"),
        os.path.join(_HERE, "rasterx_*python_light.py"),
        os.path.join(_HERE, "rasterx_functions.py"),
        os.path.join(_HERE, "vectorx_*python_light.py"),
        os.path.join(_HERE, "vectorx_functions.py"),
    )
    files = set()
    for p in pats:
        files.update(glob.glob(p))
    return sorted(f for f in files if "/test_" not in f.replace(os.sep, "/"))


_BORDER = re.compile(r"^\+[-+]*\+$")  # +----+----+
_ROW = re.compile(r"^\|.*\|$")  # |..|..|


def _output_constants(path):
    """Yield (constant_name, value) for every top-level ``*_output`` str constant."""
    with open(path) as fh:
        tree = ast.parse(fh.read())
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.endswith("_output"):
                        yield target.id, node.value.value


def _table_blocks(text):
    """Yield contiguous runs of border/row lines (one ASCII table each)."""
    cur = []
    for line in text.splitlines():
        s = line.rstrip()
        if _BORDER.match(s) or _ROW.match(s):
            cur.append(s)
        elif cur:
            yield cur
            cur = []
    if cur:
        yield cur


def _table_problems(block):
    problems = []
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
                    f"column ticks misaligned: border ticks {sorted(ticks)} "
                    f"!= row pipes {sorted(pipes)} in {r!r}"
                )
                break
    return problems


_CASES = [
    (os.path.basename(path), name, value)
    for path in _example_modules()
    for name, value in _output_constants(path)
]


@pytest.mark.parametrize(
    "module,const_name,text",
    _CASES,
    ids=[f"{m}::{n}" for m, n, _ in _CASES],
)
def test_example_output_table_is_well_formed(module, const_name, text):
    """Every ASCII table in a doc-example output constant is width/tick consistent."""
    all_problems = []
    for block in _table_blocks(text):
        all_problems.extend(_table_problems(block))
    assert (
        not all_problems
    ), f"{module}::{const_name} has a malformed ASCII output table:\n  " + "\n  ".join(
        all_problems
    )


def test_found_output_constants():
    """Sanity: the discovery globbed a meaningful number of output constants."""
    assert len(_CASES) > 100, f"expected 100+ output constants, found {len(_CASES)}"
