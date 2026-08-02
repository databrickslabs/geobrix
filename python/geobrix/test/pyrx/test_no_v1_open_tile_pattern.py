"""Guard test: functions.py must route tile opens through the ``_open`` /
``open_header`` front-door, not the raw v1 ``_serde.open_tile(bytes(tile[...]))``
pattern.

Increment 4 (Phase B) swept every ``rst_*`` function so it consumes a tile via
the shared virtual-aware chokepoint (``open_tile._open`` / ``open_tile.open_header``).
This test greps the functions.py source and asserts no ``_serde.open_tile(`` call
survives outside a small allowlist of legitimate byte-first constructors — it
fails loudly if a future edit reintroduces the bytes-only pattern (which would
silently break virtual-tile consumption for that function).
"""

import re
from pathlib import Path

import databricks.labs.gbx.pyrx.functions as functions_module

# Each allowed call site is keyed by the enclosing function/class and a one-line
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
}


def _functions_source() -> str:
    path = Path(functions_module.__file__)
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
