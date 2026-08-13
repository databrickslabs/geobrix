"""G1 — standing invariant: every registered light-tier function that RETURNS a
raster tile emits the 8-field ``V2_TILE_SCHEMA``.

Two guards, both data-driven so they cannot go stale:

1. ``test_registered_tile_op_emits_v2_schema`` — enumerates the LIVE registration
   map (``functions._sql_tile_ops``) and asserts each tile-returning member's UDF
   ``returnType`` IS ``V2_TILE_SCHEMA``. New tile functions are covered
   automatically; a regression to a legacy/other schema fails here. A small,
   name-keyed exclusion set covers the registered members that legitimately return
   something OTHER than a tile struct (boolean, array, contour struct, encoded
   image bytes).

2. ``test_no_legacy_tile_schema_as_function_output`` — a static guard that greps
   the pyrx source for the legacy ``TILE_SCHEMA`` constant used as a function
   OUTPUT, in its three declaration forms (``@f.udf(_serde.TILE_SCHEMA)`` decorator,
   ``f.udf(_serde.TILE_SCHEMA)(fn)`` functional, ``@udtf(returnType=_serde.TILE_SCHEMA)``).
   GeoBrix reads/loads v1 OR v2 tiles but OUTPUTS only v2, so the constant is
   retained as an INPUT/reader schema (v1 read/load path) — a bare reference is
   fine; only OUTPUT declarations are forbidden. The registry-only guard (#1)
   cannot see the INTERNAL functional-form UDF (``_as_tile_cellid_envelope_udf``)
   because it is not registered — this static guard catches a regression there too.

Replaces per-function schema assertions (folded in — see Task 6 consolidation).
"""

import re
from pathlib import Path

import pytest

from databricks.labs.gbx.pyrx import functions as fns
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

# Registered light-tier functions in _sql_tile_ops that DO NOT return a tile
# struct — excluded by name, each with the type it actually returns. Everything
# else in _sql_tile_ops must emit V2_TILE_SCHEMA.
_NON_TILE_REGISTERED = {
    "gbx_rst_tryopen": "BooleanType (open-success flag)",
    "gbx_rst_sample": "ArrayType(DoubleType) (per-band pixel values)",
    "gbx_rst_contour": "contour result struct (vector output)",
    "gbx_rst_tilexyz": "BinaryType (encoded PNG/JPEG/WEBP image tile)",
}


def _tile_returning_registered_udfs():
    return [
        (name, udf)
        for name, udf in fns._sql_tile_ops.items()
        if name not in _NON_TILE_REGISTERED
    ]


@pytest.mark.parametrize(
    "name,udf",
    _tile_returning_registered_udfs(),
    ids=[n for n, _ in _tile_returning_registered_udfs()],
)
def test_registered_tile_op_emits_v2_schema(name, udf):
    """Each registered tile-returning UDF declares the 8-field v2 return type."""
    rt = getattr(udf, "returnType", None)
    assert rt is not None, f"{name}: UDF has no returnType (not an @f.udf?)"
    assert rt == V2_TILE_SCHEMA, (
        f"{name}: returnType is not V2_TILE_SCHEMA — got "
        f"{[fld.name for fld in rt.fields] if hasattr(rt, 'fields') else rt}"
    )


def test_v2_schema_field_contract():
    """Lock the exact v2 field names + order (guards an accidental schema edit)."""
    assert [fld.name for fld in V2_TILE_SCHEMA.fields] == [
        "cellid",
        "raster",
        "path",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
    ]


def test_no_legacy_tile_schema_as_function_output():
    """No pyrx source declares the legacy 3-field TILE_SCHEMA as a function OUTPUT.

    GeoBrix reads/loads v1 OR v2 tiles but OUTPUTS only v2. The legacy
    ``_serde.TILE_SCHEMA`` constant is retained as an INPUT/reader schema (v1
    read/load path), so a bare reference is legitimate — but it must NEVER be a
    tile-returning UDF/UDTF's declared return type. This guard matches only the
    three OUTPUT-declaration forms and catches the internal (unregistered)
    functional-form UDF that the registry guard above cannot see:
      - ``@f.udf(_serde.TILE_SCHEMA)`` decorator
      - ``f.udf(_serde.TILE_SCHEMA)(...)`` functional
      - ``@udtf(returnType=_serde.TILE_SCHEMA)``
    Input-schema uses (``StructField("tile", _serde.TILE_SCHEMA, ...)``) and the
    constant's own definition are allowed.
    """
    src_dir = Path(fns.__file__).parent
    output_decl = re.compile(
        r"f\.udf\(\s*_serde\.TILE_SCHEMA\s*\)"  # @f.udf(..) or f.udf(..)(fn)
        r"|udtf\(\s*returnType\s*=\s*_serde\.TILE_SCHEMA"  # @udtf(returnType=..)
    )
    offenders = []
    for py in src_dir.rglob("*.py"):
        for i, line in enumerate(py.read_text().splitlines(), 1):
            if output_decl.search(line):
                offenders.append(f"{py.relative_to(src_dir)}:{i}: {line.strip()}")
    assert not offenders, (
        "Legacy TILE_SCHEMA used as a function OUTPUT (must be V2_TILE_SCHEMA); "
        "TILE_SCHEMA is input-only:\n" + "\n".join(offenders)
    )
