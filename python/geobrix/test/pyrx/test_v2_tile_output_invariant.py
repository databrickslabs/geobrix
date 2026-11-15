"""Standing invariant: every registered light-tier function that RETURNS a
raster tile emits the 9-field ``V2_TILE_SCHEMA``.

Two guards, both data-driven so they cannot go stale:

1. ``test_registered_tile_op_emits_v2_schema`` — enumerates the manifest's light (L3)
   UDF refs of every function that returns a TILE and asserts each UDF's
   ``returnType`` IS ``V2_TILE_SCHEMA``. New tile functions are covered
   automatically; a regression to a legacy/other schema fails here.

2. ``test_no_legacy_tile_schema_as_function_output`` — a static guard that greps
   the pyrx source for the legacy ``TILE_SCHEMA`` constant used as a function
   OUTPUT, in its three declaration forms (``@f.udf(_serde.TILE_SCHEMA)`` decorator,
   ``f.udf(_serde.TILE_SCHEMA)(fn)`` functional, ``@udtf(returnType=_serde.TILE_SCHEMA)``).
   GeoBrix reads/loads v1 OR v2 tiles but OUTPUTS only v2, so the constant is
   retained as an INPUT/reader schema (v1 read/load path) — a bare reference is
   fine; only OUTPUT declarations are forbidden. The registry-only guard (#1)
   cannot see the INTERNAL functional-form UDF (``_as_tile_cellid_envelope_udf``)
   because it is not registered — this static guard catches a regression there too.

Replaces per-function schema assertions (folded in).
"""

import re
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.manager.l3 import resolve_ref
from databricks.labs.gbx.pyrx import functions as fns
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA


def _tile_returning_registered_udfs():
    out = []
    for name, fn in manifest.load().functions.items():
        spec = fn.levels.get("L3", {})
        if fn.returns_session != "TILE" or "variants" not in spec:
            continue
        for variant, ref in spec["variants"].items():
            out.append((f"{name}[{variant}]", resolve_ref(ref)))
    return out


@pytest.mark.parametrize(
    "name,udf",
    _tile_returning_registered_udfs(),
    ids=[n for n, _ in _tile_returning_registered_udfs()],
)
def test_registered_tile_op_emits_v2_schema(name, udf):
    """Each registered tile-returning UDF declares the 9-field v2 return type."""
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
        "path_mode",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
    ]


def test_v2_field_order_invariant():
    """Positive invariant: metadata is the LAST field AND path_mode immediately follows path."""
    names = [fld.name for fld in V2_TILE_SCHEMA.fields]
    assert names[-1] == "metadata", f"metadata must be the last field; got {names[-1]}"
    assert (
        names[names.index("path") + 1] == "path_mode"
    ), f"path_mode must immediately follow path; got {names[names.index('path') + 1]}"


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
