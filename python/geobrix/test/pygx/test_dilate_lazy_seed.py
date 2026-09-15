"""Unit tests for the lazy boundary-seed infrastructure in _dilate.py.

Task 1 of the boundary-as-line efficiency refactor:
  - _classify_cell: extracted per-cell loop body (pure refactor; behavior-identical
    to the original classify() loop)
  - _boundary_cells: O(perimeter) boundary cell set via ring-sampling
  - _local_perimeter: covering-set perimeter computed from a band neighbourhood

RED before implementation (all three symbols raise ImportError), GREEN after.
"""

from __future__ import annotations

import pytest
from shapely.geometry import box

from databricks.labs.gbx.pygx import _dilate as D

# These three imports are the RED/GREEN trigger — ImportError until Task 1 is done.
from databricks.labs.gbx.pygx._dilate import (
    _boundary_cells,
    _classify_cell,
    _local_perimeter,
)


# ===========================================================================
# Synthetic grid helpers  (pattern from test_dilate_engine.py)
# ===========================================================================

#: Cell edge length for the unit grid (1×1 cells).
CELL_STEP: float = 1.0


def _cid(x: int, y: int) -> int:
    """Cell id encoding: (x<<20)|y for 0 <= x, y < 2^20."""
    return (x << 20) | y


def _xy(c: int) -> tuple[int, int]:
    return (c >> 20, c & 0xFFFFF)


def _neighbors(c: int) -> list[int]:
    """8-connected neighbours with non-negative coordinates."""
    x, y = _xy(c)
    return [
        _cid(x + dx, y + dy)
        for dx in (-1, 0, 1)
        for dy in (-1, 0, 1)
        if not (dx == 0 and dy == 0) and x + dx >= 0 and y + dy >= 0
    ]


def _cell_geom(c: int):
    """Map cell id to its 1×1 bounding box."""
    x, y = _xy(c)
    return box(x, y, x + 1, y + 1)


def _point_to_cell(px: float, py: float) -> int:
    """Floor map: (x, y) -> the containing unit cell."""
    return _cid(int(px), int(py))


def _polyfill(geom, res) -> list[int]:
    """Return all unit cells whose bounding box overlaps geom's bbox."""
    minx, miny, maxx, maxy = geom.bounds
    return [
        _cid(x, y)
        for x in range(int(minx) - 1, int(maxx) + 2)
        for y in range(int(miny) - 1, int(maxy) + 2)
    ]


def _in_region(cover_set: set):
    """Return a closure `in_region(c) -> bool` over the given cover set."""
    return lambda c: c in cover_set


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def simple_poly():
    """5×5 box with half-integer corners (non-grid-aligned)."""
    return box(0.5, 0.5, 5.5, 5.5)


@pytest.fixture
def aligned_poly():
    """4×4 box with exact-integer corners (grid-aligned: s_border will be empty)."""
    return box(2.0, 2.0, 6.0, 6.0)


@pytest.fixture
def simple_cls(simple_poly):
    return D.classify(simple_poly, 1, _polyfill, _cell_geom, _point_to_cell)


@pytest.fixture
def aligned_cls(aligned_poly):
    return D.classify(aligned_poly, 1, _polyfill, _cell_geom, _point_to_cell)


# ===========================================================================
# (c) _classify_cell: pure-refactor contract
# ===========================================================================


def test_classify_cell_returns_nine_keys(simple_poly, simple_cls):
    """_classify_cell returns a dict with exactly the 9 membership keys."""
    S, H = D._solid_and_holes(simple_poly)
    dim = D._geom_dimension(simple_poly)
    # Any cell from the covering set is a valid probe.
    c = next(iter(simple_cls.s_cover))
    m = _classify_cell(c, _cell_geom, simple_poly, S, H, dim)
    expected_keys = {
        "p_cover", "p_centroid", "p_core",
        "s_cover", "s_centroid", "s_core",
        "h_cover", "h_centroid", "h_core",
    }
    assert set(m.keys()) == expected_keys, f"unexpected keys: {set(m.keys())}"


def test_classify_cell_all_values_are_bool(simple_poly, simple_cls):
    """Every value in the returned dict is a bool (not a truthiness proxy)."""
    S, H = D._solid_and_holes(simple_poly)
    dim = D._geom_dimension(simple_poly)
    c = next(iter(simple_cls.s_cover))
    m = _classify_cell(c, _cell_geom, simple_poly, S, H, dim)
    for k, v in m.items():
        assert isinstance(v, bool), f"{k!r} is {type(v).__name__}, expected bool"


def test_classify_cell_refactor_matches_classify_output(simple_poly):
    """Rebuilding the 9 sets from _classify_cell calls produces the same Classification.

    This is the key regression guard for the pure-refactor step: if _classify_cell
    disagrees with classify's loop, these sets will differ.
    """
    cls = D.classify(simple_poly, 1, _polyfill, _cell_geom, _point_to_cell)
    S, H = D._solid_and_holes(simple_poly)
    dim = D._geom_dimension(simple_poly)
    # Use the SAME candidate set that classify would use (polyfill over S, no fallback
    # needed for a non-degenerate polygon).
    cands = set(_polyfill(S, 1))

    rebuilt = {k: set() for k in (
        "p_cover", "p_centroid", "p_core",
        "s_cover", "s_centroid", "s_core",
        "h_cover", "h_centroid", "h_core",
    )}
    for c in cands:
        for k, v in _classify_cell(c, _cell_geom, simple_poly, S, H, dim).items():
            if v:
                rebuilt[k].add(c)

    assert rebuilt["p_cover"] == cls.p_cover, "p_cover mismatch"
    assert rebuilt["p_centroid"] == cls.p_centroid, "p_centroid mismatch"
    assert rebuilt["p_core"] == cls.p_core, "p_core mismatch"
    assert rebuilt["s_cover"] == cls.s_cover, "s_cover mismatch"
    assert rebuilt["s_centroid"] == cls.s_centroid, "s_centroid mismatch"
    assert rebuilt["s_core"] == cls.s_core, "s_core mismatch"
    assert rebuilt["h_cover"] == cls.h_cover, "h_cover mismatch (should both be empty)"
    assert rebuilt["h_centroid"] == cls.h_centroid, "h_centroid mismatch"
    assert rebuilt["h_core"] == cls.h_core, "h_core mismatch"


# ===========================================================================
# (c) _boundary_cells
# ===========================================================================


def test_boundary_cells_aligned_box_nonempty(aligned_poly):
    """_boundary_cells must be non-empty for a grid-aligned box.

    The alignment guard: when no cell straddles the boundary (s_border empty),
    the old s_border seed was empty; _boundary_cells must still find boundary cells.
    """
    rings = [aligned_poly.exterior]
    cells = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    assert cells, "_boundary_cells must be non-empty for a grid-aligned box"


def test_boundary_cells_returns_set(simple_poly):
    """_boundary_cells returns a plain set."""
    rings = [simple_poly.exterior]
    result = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    assert isinstance(result, set), f"expected set, got {type(result).__name__}"


def test_boundary_cells_simple_poly_subset_of_cover(simple_poly, simple_cls):
    """For a non-aligned polygon, boundary cells are a subset of the covering set."""
    rings = [simple_poly.exterior]
    band = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    assert band, "_boundary_cells must be non-empty for the simple polygon"
    assert band <= simple_cls.s_cover, (
        "boundary cells must be a subset of s_cover; "
        f"extra cells outside s_cover: {band - simple_cls.s_cover}"
    )


# ===========================================================================
# (b) alignment invariant: _local_perimeter == outer_perimeter
#
# This is the central correctness claim of the lazy-seed design: for BOTH a
# non-aligned polygon and a grid-aligned polygon (where s_border is empty!),
# _local_perimeter(_boundary_cells(exterior), neighbors, in_cover)
# must equal outer_perimeter(s_cover, neighbors).
# ===========================================================================


def test_local_perimeter_equals_outer_perimeter_simple(simple_poly, simple_cls):
    """_local_perimeter == outer_perimeter for a non-aligned (simple) polygon."""
    rings = [simple_poly.exterior]
    band = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    lp = _local_perimeter(band, _neighbors, _in_region(simple_cls.s_cover))
    op = D.outer_perimeter(simple_cls.s_cover, _neighbors)

    assert lp, "_local_perimeter must be non-empty for the simple polygon"
    assert op, "outer_perimeter must be non-empty for the simple polygon"
    assert lp == op, (
        "_local_perimeter != outer_perimeter for the simple polygon:\n"
        f"  local_perimeter  ({len(lp)} cells): {sorted(lp)[:6]}…\n"
        f"  outer_perimeter  ({len(op)} cells): {sorted(op)[:6]}…\n"
        f"  in lp but not op: {sorted(lp - op)[:4]}\n"
        f"  in op but not lp: {sorted(op - lp)[:4]}"
    )


def test_local_perimeter_equals_outer_perimeter_aligned(aligned_poly, aligned_cls):
    """_local_perimeter == outer_perimeter for a GRID-ALIGNED polygon.

    This is the key alignment guard: for an aligned polygon s_border is empty (no
    straddling cells exist), so the old s_border seed was empty.  Both
    outer_perimeter and _local_perimeter must be non-empty AND equal.
    """
    # Fixture invariant: confirm alignment (s_border empty = no straddling cells).
    assert aligned_cls.s_border == set(), (
        "aligned_poly fixture broken: s_border must be empty for a grid-aligned polygon"
    )

    rings = [aligned_poly.exterior]
    band = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    lp = _local_perimeter(band, _neighbors, _in_region(aligned_cls.s_cover))
    op = D.outer_perimeter(aligned_cls.s_cover, _neighbors)

    assert lp, "_local_perimeter must be non-empty even when s_border is empty"
    assert op, "outer_perimeter must be non-empty even when s_border is empty"
    assert lp == op, (
        "_local_perimeter != outer_perimeter for the grid-aligned polygon "
        "(the alignment guard FAILED):\n"
        f"  local_perimeter  ({len(lp)} cells): {sorted(lp)[:6]}…\n"
        f"  outer_perimeter  ({len(op)} cells): {sorted(op)[:6]}…\n"
        f"  in lp but not op: {sorted(lp - op)[:4]}\n"
        f"  in op but not lp: {sorted(op - lp)[:4]}"
    )


def test_local_perimeter_returns_frozenset(simple_poly, simple_cls):
    """_local_perimeter returns a frozenset."""
    rings = [simple_poly.exterior]
    band = _boundary_cells(rings, _point_to_cell, CELL_STEP)
    result = _local_perimeter(band, _neighbors, _in_region(simple_cls.s_cover))
    assert isinstance(result, frozenset), (
        f"expected frozenset, got {type(result).__name__}"
    )
