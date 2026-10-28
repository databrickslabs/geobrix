"""Spark-free unit tests for the pygx BNG polyfill (centroid BFS flood-fill).

Faithful port of ``BNG.polyfill`` (gridx/grid/BNG.scala line 207): seed the queue
with the cell ids of every geometry coordinate plus the centroid, BFS-expand via
``kLoop(.,1)``, and include a cell iff its CENTROID is ``contains``-ed by the
geometry. Exact cell-set parity vs heavy is the bar; these
tests pin the membership semantics that parity depends on.
"""

import pytest

shapely = pytest.importorskip("shapely")
from shapely import to_wkb as _towkb  # noqa: E402
from shapely.geometry import box as _box  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402


def test_polyfill_small_box_1km():
    # A 3km x 3km box aligned to the 1km grid around TQ3080.
    geom = _towkb(_box(530000.0, 180000.0, 533000.0, 183000.0))
    cells = _bng.polyfill_str(geom, _bng.get_resolution("1km"))
    assert len(cells) > 0
    assert all(isinstance(c, str) for c in cells)
    # Cells are 1km (6-char TQ#### form).
    assert all(c.startswith("TQ") for c in cells)


def test_polyfill_empty_geom_is_empty():
    assert _bng.polyfill_str(None, 3) == []


def test_polyfill_grid_aligned_box_exact_cellset():
    # A 3km x 3km grid-aligned box has exactly nine 1km cells whose CENTROIDS
    # ((530500,180500) .. (532500,182500)) fall strictly inside the box.
    # Centroid-membership (contains, strict interior) admits all nine here
    # because each cell centre sits 500 m inside the box, never on its boundary.
    geom = _towkb(_box(530000.0, 180000.0, 533000.0, 183000.0))
    cells = set(_bng.polyfill_str(geom, _bng.get_resolution("1km")))
    expected = {
        _bng.east_north_as_bng(e + 500.0, n + 500.0, "1km")
        for e in (530000.0, 531000.0, 532000.0)
        for n in (180000.0, 181000.0, 182000.0)
    }
    assert cells == expected
    assert len(cells) == 9


def test_polyfill_membership_is_centroid_not_intersects():
    # A box covering exactly one full cell plus a thin (200 m) sliver into the
    # neighbouring cell. The neighbour's CENTROID is NOT contained (the box only
    # reaches 200 m into it, centre is at 500 m), so centroid-membership returns
    # only the single fully-covered cell -- proving membership is centroid-in,
    # not intersects.
    geom = _towkb(_box(530000.0, 180000.0, 531200.0, 181000.0))
    cells = set(_bng.polyfill_str(geom, _bng.get_resolution("1km")))
    assert cells == {_bng.east_north_as_bng(530500.0, 180500.0, "1km")}


def test_polyfill_accepts_wkt_input():
    # parse_geom path: WKT in, same cell set as the WKB form.
    box = _box(530000.0, 180000.0, 533000.0, 183000.0)
    from_wkb = set(_bng.polyfill_str(_towkb(box), 3))
    from_wkt = set(_bng.polyfill_str(box.wkt, 3))
    assert from_wkt == from_wkb
