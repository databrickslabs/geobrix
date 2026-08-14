"""Guard test: rst_* functions must route tile opens through the ``_open`` /
``open_header`` front-door, not a raw v1 bytes-only path.

Increment 4 (Phase B) swept every ``rst_*`` function so it consumes a tile via
the shared virtual-aware chokepoint (``open_tile._open`` / ``open_tile.open_header``).
This test greps the *source* of the modules a registered function can reach and
asserts no v1 bytes-only tile open survives outside an explicit allowlist — it
fails loudly if a future edit reintroduces the bytes-only pattern (which would
silently return NULL / nothing on a virtual-tile input).

It grep BOTH ``functions.py`` (the UDF bodies) AND ``_udf.py`` (the shared UDF
builders). The original guard grepped only functions.py, which is exactly why
the ``_udf.py`` v1 builders — used by the 4 coord x/y accessors — were invisible
until a whole-branch review. The widened guard would have caught that miss.
"""

import re
from pathlib import Path

import databricks.labs.gbx.pyrx._udf as udf_module
import databricks.labs.gbx.pyrx.functions as functions_module

# Each allowed call site is keyed by the exact call form and a one-line
# justification. These open ALREADY-MATERIALIZED bytes that the code obtained via
# the front-door itself (or a byte-length contract), so they are not v1 leaks.
_ALLOWLIST = {
    # rst_maketiles: the power-of-4 split count keys on the encoded GTiff byte
    # length (heavy BalancedSubdivision parity), so the UDTF materialises the
    # tile through the front-door FIRST (ot._to_virtual_tile + materialize_to_bytes
    # for a virtual tile; verbatim bytes otherwise) and opens those bytes with
    # a length it can measure. The open is on front-door-obtained bytes, not on
    # tile["raster"].
    "_serde.open_tile(raster)": 1,
    # _merge_bytes / _combineavg_bytes / _frombands_bytes (Task 5 corrupt-skip):
    # each helper validates that a member's bytes are openable before adding them
    # to the collective list that agg_core opens all-at-once. The variable
    # `candidate` holds front-door-obtained bytes (ot.materialize_to_bytes(vt).raster
    # for a virtual tile; bytes(vt.raster) otherwise) — NOT tile["raster"] directly
    # — so this does not reintroduce the virtual-tile-NULL bug the guard prevents.
    # There are 3 such validation opens in the scalar *_bytes helpers + 5 in the
    # grouped-agg pandas_udfs (_merge_agg_udf, _combineavg_agg_udf,
    # _combineavg_agg_sql_udf, _frombands_agg_udf, _derivedband_agg_udf) = 8 total.
    # Group 3 FILE-aware variants follow the identical pattern: 3 scalar FILE UDFs
    # (_uf_merge, _uf_combineavg, _uf_frombands) + 3 pandas_udf FILE variants
    # (_merge_agg_file_udf, _combineavg_agg_file_udf, _frombands_agg_file_udf)
    # add 6 more pre-validation opens on front-door-obtained bytes = 14 total.
    "_serde.open_tile(candidate)": 14,
}

# The v1 shared UDF builders in _udf.py. Every registered rst_* function was
# swept off these onto the virtual-aware front-door; functions.py must not
# reference any of them (a reference means some function silently NULLs on a
# virtual tile, as the coord x/y accessors did before the coord-fix wave).
_V1_BUILDER_NAMES = (
    "tile_scalar_udf",
    "tile_scalar_udf2",
    "sql_scalar_udf",
    "sql_scalar_udf2",
)


def _functions_source() -> str:
    path = Path(functions_module.__file__)
    return path.read_text()


def _udf_source() -> str:
    path = Path(udf_module.__file__)
    return path.read_text()


def test_no_v1_open_tile_bytes_pattern():
    """No ``_serde.open_tile(bytes(tile[...]))`` (the v1 raster-only) call remains."""
    src = _functions_source()
    # The exact v1 anti-pattern the sweep eliminated.
    v1_hits = re.findall(r"_serde\.open_tile\(bytes\(tile\[", src)
    assert not v1_hits, (
        f"Found {len(v1_hits)} v1 `_serde.open_tile(bytes(tile[...]))` call(s) in "
        "functions.py. Route tile opens through open_tile._open (pixels) or "
        "open_tile.open_header (header-only) instead."
    )


def test_only_allowlisted_open_tile_calls_remain():
    """Every surviving ``_serde.open_tile(`` call is on the explicit allowlist."""
    src = _functions_source()
    # Count each distinct open_tile(...) call form.
    calls = re.findall(r"_serde\.open_tile\([^)]*\)", src)
    unexpected = []
    counts = {}
    for c in calls:
        counts[c] = counts.get(c, 0) + 1
    for call_form, n in counts.items():
        allowed = _ALLOWLIST.get(call_form)
        if allowed is None or n > allowed:
            unexpected.append((call_form, n, allowed))
    assert not unexpected, (
        "Unexpected `_serde.open_tile(` call form(s) in functions.py "
        f"(form, found, allowed): {unexpected}. Add a justified allowlist entry "
        "only for a byte-first constructor that opens front-door-obtained bytes."
    )


def test_front_door_helpers_are_used():
    """Sanity: the sweep actually wired the front-door helpers into functions.py."""
    src = _functions_source()
    assert "ot._open(" in src, "expected ot._open(...) usage in functions.py"
    assert (
        "ot.open_header(" in src
    ), "expected ot.open_header(...) usage in functions.py"


def test_functions_do_not_use_v1_udf_builders():
    """functions.py must not reference the v1 bytes-only UDF builders from _udf.py.

    ``tile_scalar_udf`` / ``sql_scalar_udf`` (and the /2 arg forms) take only the
    raster subfield, so they return NULL on a virtual tile. Every registered
    rst_* function was swept off them onto the virtual-aware front-door. A
    reference here = a function silently NULLing on virtual input (the exact bug
    the coord x/y accessors had before the coord-fix wave). Comments are stripped
    so an explanatory mention in prose doesn't trip the guard.
    """
    src = _functions_source()
    code = "\n".join(
        line.split("#", 1)[0] for line in src.splitlines()
    )  # drop comments
    offenders = [name for name in _V1_BUILDER_NAMES if re.search(rf"\b{name}\b", code)]
    assert not offenders, (
        f"functions.py references v1 UDF builder(s) {offenders} from _udf.py. "
        "Route the function through a struct-accepting @f.udf that opens via "
        "ot._open (pixel) or ot.open_header (header-only) instead."
    )


def test_udf_module_v1_opens_are_only_in_the_builders():
    """Every ``_serde.open_tile(bytes(`` in _udf.py lives inside a v1 builder def.

    The v1 builders remain defined in _udf.py (they are the documented shape the
    sweep migrated OFF of) but must not be reachable by any registered function —
    ``test_functions_do_not_use_v1_udf_builders`` enforces that. This test just
    pins that the only v1 bytes-opens in _udf.py are the four builder bodies, so a
    NEW v1 open added elsewhere in _udf.py is caught.
    """
    src = _udf_source()
    v1_opens = re.findall(r"_serde\.open_tile\(bytes\(", src)
    # Exactly the four builder bodies (tile_scalar_udf, tile_scalar_udf2,
    # sql_scalar_udf, sql_scalar_udf2) each open bytes once.
    assert len(v1_opens) == 4, (
        f"Expected exactly 4 v1 `_serde.open_tile(bytes(` opens in _udf.py "
        f"(the four legacy builders), found {len(v1_opens)}. A new one was added "
        "outside the known builders — route it through the front-door instead."
    )


# escape.py intentionally keeps v1 opens: tile_to_numpy / rst_apply are
# documented Python-only escape hatches (NOT registered rst_* functions), so a
# user passing raw bytes is expected. They are deliberately NOT covered by this
# guard — only registered-function-reachable paths (functions.py + _udf.py) are.
