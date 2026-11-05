"""Example notebooks and Lakeflow files never hand-build a tile struct with missing TILE fields.

A tile built with ``named_struct('cellid', ..., 'raster', ..., 'metadata', ...)`` has only some of
the TILE fields, and a function that expects the full tile refuses it (DATATYPE_MISMATCH). The
fix is to name every field of the current TILE schema, in order.
"""

import json
import pathlib
import re

import pytest

from databricks.labs.gbx.manifest import TILE_SQL

EXAMPLES = pathlib.Path(__file__).resolve().parents[4] / "examples"

pytestmark = pytest.mark.skipif(
    not EXAMPLES.is_dir(), reason="the examples are not in this checkout"
)

_START = re.compile(r"named_struct\(\s*['\"]cellid['\"]", re.IGNORECASE)


def _tile_fields():
    """The top-level field names of the TILE schema, in order."""
    body = TILE_SQL[len("struct<") : -1]
    names, depth, token = [], 0, ""
    for ch in body:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if depth == 0 and ch == ",":
            names.append(token.split(":")[0])
            token = ""
        else:
            token += ch
    names.append(token.split(":")[0])
    return names


def _hand_built_tiles(text):
    """The argument text of every ``named_struct('cellid', ...)`` call in ``text``."""
    out = []
    for m in _START.finditer(text):
        i = text.index("(", m.start())
        depth, j = 0, i
        while j < len(text):
            depth += text[j] == "("
            depth -= text[j] == ")"
            if depth == 0:
                break
            j += 1
        out.append(text[i + 1 : j])
    return out


def _missing(args):
    """The TILE fields a hand-built struct does not name."""
    named = set(re.findall(r"['\"](\w+)['\"]\s*,", args))
    return [f for f in _tile_fields() if f not in named]


def _sources():
    for p in sorted(EXAMPLES.rglob("*")):
        if p.suffix == ".ipynb":
            cells = json.loads(p.read_text(encoding="utf-8"))["cells"]
            text = "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")
        elif p.suffix == ".py":
            text = p.read_text(encoding="utf-8")
        else:
            continue
        if "named_struct" in text:
            yield p, text


def test_tile_fields_include_path_mode():
    assert "path_mode" in _tile_fields()


def test_checker_flags_the_old_three_field_tile():
    old = "named_struct('cellid', t.cellid, 'raster', t.raster, 'metadata', t.metadata)"
    (args,) = _hand_built_tiles(old)
    assert "path_mode" in _missing(args)


def test_examples_build_full_tiles():
    bad = []
    for path, text in _sources():
        for args in _hand_built_tiles(text):
            missing = _missing(args)
            if missing:
                bad.append(f"{path.relative_to(EXAMPLES)}: missing {missing}")
    assert not bad, "hand-built tile structs missing TILE fields:\n" + "\n".join(bad)
