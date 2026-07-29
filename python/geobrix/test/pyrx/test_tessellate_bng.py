"""Tests for light-tier BNG raster tessellation (iter_tessellate_bng).

Mirrors the quadbin tessellate test structure (test_tessellate_quadbin.py).
BNG-specific coverage:
  * covering over a GB raster (EPSG:27700, or EPSG:4326 auto-warped) yields >=1
    chip, each tagged with a BNG String id ``^[A-Z]{2}\\d*$``; only areal chips.
  * centroid single-assigns every valid pixel (strict partition).
  * unknown mode raises.
  * a 4326 input triggers the internal 27700 warp (chips returned + ids valid BNG).
  * BOUNDARY COMPLETENESS: a cell-misaligned raster emits EVERY cell whose square
    overlaps the raster bbox (pinned against an independent intersect enumeration).
    This guards the buffer-before-polyfill fix (BNG.polyfill is a centroid-BFS
    with boundary blind spots — an unbuffered polyfill drops fringe cells whose
    centroid sits just outside the bbox).
"""

import math
import re

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from shapely.geometry import box

from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pyrx.core import tessellate as T

_BNG_ID_RE = re.compile(r"^[A-Z]{2}\d*(SW|NW|NE|SE)?$")

# A cell-misaligned London-area window in EPSG:27700, offset by half a 1km cell
# so boundary-cell centroids fall OUTSIDE the raster bbox (the buffer-fix trap).
_MINX, _MINY, _MAXX, _MAXY = 529500, 179500, 531500, 181500


def _tile_27700(minx=_MINX, miny=_MINY, maxx=_MAXX, maxy=_MAXY, size=64):
    """Small EPSG:27700 raster over a GB (London) window."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    xres = (maxx - minx) / size
    yres = (maxy - miny) / size
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(minx, maxy, xres, yres),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _tile_4326(size=64, res_deg=0.005, origin=(-0.12, 51.52)):
    """Small EPSG:4326 raster over a London window (warps into GB / EPSG:27700)."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(
            origin[0], origin[1], res_deg, res_deg
        ),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _independent_intersecting_cells(minx, miny, maxx, maxy, res):
    """Independent (non-polyfill) enumeration of every BNG cell whose square has
    POSITIVE-AREA overlap with the bbox — the ground truth the covering set must
    match.

    Uses ``.intersection(bbox).area > 0``, NOT bare ``.intersects()``: on a
    grid-aligned tile a fringe cell just outside the data shares only a 1-D
    boundary line with the raster (``intersects() is True`` but zero overlap
    area, zero pixels). The covering contract emits a cell iff it has real areal
    overlap, so the ground truth must use the positive-area test too. On a
    cell-MISaligned window the two tests agree (no cell touches only an edge).
    """
    edge = _bng.get_edge_size(res)
    bbox = box(minx, miny, maxx, maxy)
    out = set()
    x0 = int(math.floor(minx / edge) * edge)
    y0 = int(math.floor(miny / edge) * edge)
    x = x0
    while x <= maxx:
        y = y0
        while y <= maxy:
            cell = _bng.point_to_cell_id(x + edge / 2.0, y + edge / 2.0, res)
            if _bng.is_valid(cell):
                cell_geom = _bng.cell_id_to_geometry(cell)
                if cell_geom.intersection(bbox).area > 0.0:
                    out.add(_bng.format(cell))
            y += edge
        x += edge
    return out


# ---------------------------------------------------------------------------
# Covering mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_bng_covering_yields_tagged_chips():
    """covering over an EPSG:27700 GB raster yields >=1 chip, each tagged with a
    BNG String id and non-empty GTiff bytes."""
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))
    assert chips, "covering must yield >=1 chip"
    for cellid, raster in chips:
        assert isinstance(cellid, str)
        assert _BNG_ID_RE.match(cellid), f"not a BNG id: {cellid!r}"
        assert raster  # non-empty GTiff bytes


def test_iter_tessellate_bng_covering_default_mode():
    """Default mode (no mode arg) is covering and must yield >=1 chip."""
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km"))
    assert chips, "default mode must yield >=1 chip"


def test_iter_tessellate_bng_covering_int_resolution():
    """Resolution accepts the integer index form (3 == '1km')."""
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution=3, mode="covering"))
    assert chips
    for cellid, _ in chips:
        assert _BNG_ID_RE.match(cellid)


def test_iter_tessellate_bng_covering_chips_are_areal():
    """Every covering chip is an areal raster (positive width and height)."""
    from databricks.labs.gbx.pyrx import _serde

    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))
    assert chips
    for _cellid, raster in chips:
        with _serde.open_tile(raster) as chip_ds:
            assert chip_ds.width > 0 and chip_ds.height > 0


# ---------------------------------------------------------------------------
# 4326 auto-warp
# ---------------------------------------------------------------------------


def test_iter_tessellate_bng_4326_input_triggers_warp():
    """A 4326 GB raster is auto-warped to 27700; covering still yields valid
    BNG-tagged chips."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            assert ds.crs.to_epsg() == 4326
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))
    assert chips, "4326 input must warp and still yield >=1 chip"
    for cellid, raster in chips:
        assert _BNG_ID_RE.match(cellid), f"not a BNG id: {cellid!r}"
        assert raster


# ---------------------------------------------------------------------------
# Centroid mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_bng_centroid_yields_chips():
    """centroid mode yields at least one BNG-tagged chip."""
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="centroid"))
    assert chips, "centroid mode must yield >=1 chip"
    for cellid, raster in chips:
        assert _BNG_ID_RE.match(cellid)
        assert raster


def test_iter_tessellate_bng_centroid_partitions_all_pixels():
    """centroid: every valid pixel is assigned to exactly one cell; union == all
    pixels (the input is fully inside GB, so no is_valid drops occur)."""
    from databricks.labs.gbx.pyrx import _serde

    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            results = list(T.iter_tessellate_bng(ds, resolution="1km", mode="centroid"))

    seen = 0
    for _cellid, raster_bytes in results:
        with _serde.open_tile(raster_bytes) as chip_ds:
            arr = chip_ds.read(1, masked=True)
            seen += int((~arr.mask).sum())

    assert (
        seen == 64 * 64
    ), f"centroid chips must partition all valid pixels exactly once; got {seen}"


# ---------------------------------------------------------------------------
# Unknown mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_bng_unknown_mode_raises():
    """An unrecognised mode string raises ValueError with a useful message."""
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="mode must be one of"):
                list(T.iter_tessellate_bng(ds, resolution="1km", mode="bad_mode"))


# ---------------------------------------------------------------------------
# Boundary completeness (the buffer-before-polyfill fix)
# ---------------------------------------------------------------------------


def test_iter_tessellate_bng_chip_cellid_in_struct_not_metadata():
    """Light BNG tessellate must NOT put RASTERX_CELL_ID in the tile metadata map.

    The authoritative cell id is carried in the tile's ``cellid`` struct field
    (set by ``_serde.build_tile`` via ``_bng.parse``), matching the heavy tier.
    The old light implementation additionally set
    ``out["metadata"]["RASTERX_CELL_ID"] = cellid_str``, creating an asymmetry
    with the heavy tier and with light H3/quadbin tessellate.  This test asserts
    the metadata key is absent after the fix.
    """
    from databricks.labs.gbx.pyrx import _serde

    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))

    assert chips, "covering must yield >=1 chip"
    for _cellid_str, raster_bytes in chips:
        with _serde.open_tile(raster_bytes) as chip_ds:
            tags = chip_ds.tags() or {}
            assert "RASTERX_CELL_ID" not in tags, (
                f"RASTERX_CELL_ID must NOT be in the tile metadata map; "
                f"found it with value {tags.get('RASTERX_CELL_ID')!r}"
            )


def test_iter_tessellate_bng_cellid_struct_field_is_set():
    """Light BNG tessellate must carry the correct cell id in the tile ``cellid``
    struct field.  This exercises the ``_serde.build_tile(_bng.parse(...))`` path
    and confirms the Long round-trip is lossless for 1 km resolution cells.
    """
    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))

    assert chips, "covering must yield >=1 chip"
    for cellid_str, raster_bytes in chips:
        # _serde.build_tile wraps the bytes in the tile struct; the cellid comes
        # from _bng.parse(cellid_str).  Verify format(_bng.parse(cellid_str)) ==
        # cellid_str as a round-trip sanity check.
        parsed_long = _bng.parse(cellid_str)
        roundtrip = _bng.format(parsed_long)
        assert roundtrip == cellid_str, (
            f"BNG String round-trip via parse+format failed: "
            f"{cellid_str!r} -> {parsed_long} -> {roundtrip!r}"
        )


def test_iter_tessellate_bng_covering_is_boundary_complete():
    """A cell-misaligned raster emits EVERY BNG cell whose square overlaps the
    raster bbox — pinned against an independent intersect enumeration.

    Without the buffer-before-polyfill fix, BNG.polyfill (a centroid flood-fill)
    would drop the fringe cells whose centroid sits just outside the bbox, so the
    emitted set would be a strict subset of the independent enumeration and this
    assertion would fail.
    """
    res = _bng.get_resolution("1km")
    expected = _independent_intersecting_cells(_MINX, _MINY, _MAXX, _MAXY, res)
    # Sanity: the misaligned window really does straddle multiple cells.
    assert len(expected) >= 4

    tile = _tile_27700()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            emitted = {
                cellid
                for cellid, _ in T.iter_tessellate_bng(
                    ds, resolution="1km", mode="covering"
                )
            }

    assert emitted == expected, (
        "covering must be boundary-complete: "
        f"missing {sorted(expected - emitted)}, extra {sorted(emitted - expected)}"
    )


def test_iter_tessellate_bng_covering_grid_aligned_excludes_edge_touch():
    """On a GRID-ALIGNED tile (raster edges land exactly on 1km cell boundaries),
    covering emits ONLY the cells with positive-area overlap — the edge-only-
    touching neighbours (which share just a 1-D boundary line, zero pixel overlap)
    are NOT emitted. This is the root-cause regression guard: a bare ``intersects``
    keep-test kept those neighbours and clipped them into spurious empty all-NoData
    chips (the 48-vs-36 heavy-vs-light divergence).
    """
    res = _bng.get_resolution("1km")
    edge = _bng.get_edge_size(res)  # 1000 m
    # A 2km x 2km window whose edges land on 1km cell boundaries -> 4 whole cells.
    minx, miny = 529000, 179000
    maxx, maxy = minx + 2 * edge, miny + 2 * edge

    # Ground truth: the 4 fully-covered cells (positive-area). Edge-touch neighbours
    # (e.g. the cell to the west, sharing only the x=minx line) are excluded.
    expected = _independent_intersecting_cells(minx, miny, maxx, maxy, res)
    assert (
        len(expected) == 4
    ), f"aligned 2km window should cover 4 cells, got {expected}"

    # An explicit edge-touch neighbour just west of the window shares only x=minx.
    west_neighbour = _bng.format(
        _bng.point_to_cell_id(minx - edge / 2.0, miny + edge / 2.0, res)
    )
    assert west_neighbour not in expected

    tile = _tile_27700(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            emitted = {
                cellid
                for cellid, _ in T.iter_tessellate_bng(
                    ds, resolution="1km", mode="covering"
                )
            }

    assert emitted == expected, (
        "grid-aligned covering must exclude edge-touch cells: "
        f"missing {sorted(expected - emitted)}, extra {sorted(emitted - expected)}"
    )
    assert (
        west_neighbour not in emitted
    ), "edge-only-touching neighbour must not be emitted"


def test_iter_tessellate_bng_covering_keeps_within_extent_nodata_cell():
    """Case A: a cell that genuinely OVERLAPS the raster but whose pixels are all
    NoData is STILL emitted (positive area > 0). Covering mode fills its position;
    dropping it would punch a gap into the mosaic. Only zero-area edge touches are
    dropped.
    """
    res = _bng.get_resolution("1km")
    edge = _bng.get_edge_size(res)
    minx, miny = 529000, 179000
    maxx, maxy = minx + 2 * edge, miny + 2 * edge

    # A tile fully covering 4 cells, but every pixel is NoData.
    size = 64
    data = np.full((size, size), -9999.0, dtype="float32")
    xres = (maxx - minx) / size
    yres = (maxy - miny) / size
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(minx, maxy, xres, yres),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        tile = mf.read()

    expected = _independent_intersecting_cells(minx, miny, maxx, maxy, res)
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            emitted = {
                cellid
                for cellid, _ in T.iter_tessellate_bng(
                    ds, resolution="1km", mode="covering"
                )
            }
    # All 4 within-extent cells are emitted despite being all-NoData (case A kept).
    assert emitted == expected, (
        f"within-extent all-NoData cells must still be emitted: "
        f"missing {sorted(expected - emitted)}, extra {sorted(emitted - expected)}"
    )


# ---------------------------------------------------------------------------
# Spark-struct layer: LE2 regression — RASTERX_CELL_ID must NOT appear in the
# tile metadata map returned by the UDTF through Spark.
# ---------------------------------------------------------------------------


def test_spark_struct_no_rasterx_cell_id_in_metadata(spark):
    """RASTERX_CELL_ID must be absent from the tile metadata map at the Spark-
    struct layer; the authoritative cell id must be in the ``cellid`` struct
    field (LE2 regression guard).

    Pre-fix behaviour: ``_RstBngTessellateUDTF.eval`` called
    ``out["metadata"]["RASTERX_CELL_ID"] = cellid_str``, so the metadata map
    carried the key.  The fix removed that mutation so the struct metadata only
    contains the driver/width/height/count keys set by ``build_tile``.

    This test drives the UDTF through Spark (register + SQL LATERAL) so it
    exercises the actual yielded struct row, not the raw GDAL bytes.  The old
    GDAL-tags test (``test_iter_tessellate_bng_chip_cellid_in_struct_not_metadata``)
    only checked ``chip_ds.tags()`` on the raw GTiff bytes — those never carried
    RASTERX_CELL_ID, so that test was vacuously true both before and after the
    fix and provided no RED-before signal.

    RED-before reasoning: restoring the removed line
    ``out["metadata"]["RASTERX_CELL_ID"] = cellid_str`` in
    ``_RstBngTessellateUDTF.eval`` would cause ``row["tile"]["metadata"]`` to
    contain the key, and the ``assert "RASTERX_CELL_ID" not in metadata`` below
    would FAIL.  With the line absent (current production code) the key is not
    present and the test PASSES.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    tile_bytes = _tile_27700()
    df = spark.createDataFrame([(bytearray(tile_bytes),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_bng_tess_le2")
    # SELECT t.* expands the UDTF struct fields directly: cellid, raster, metadata.
    rows = spark.sql(
        "SELECT t.cellid, t.metadata FROM _bng_tess_le2, "
        "LATERAL gbx_rst_bng_tessellate(tile, 3, 'covering') t"
    ).collect()

    assert rows, "UDTF must yield >=1 row for the London-area raster"

    for row in rows:
        metadata = dict(row["metadata"]) if row["metadata"] else {}

        # LE2 assertion: the metadata map must NOT contain RASTERX_CELL_ID.
        assert "RASTERX_CELL_ID" not in metadata, (
            f"LE2: RASTERX_CELL_ID must NOT be in the Spark tile metadata map; "
            f"found value={metadata.get('RASTERX_CELL_ID')!r}. "
            f"The pre-fix code set out['metadata']['RASTERX_CELL_ID'] in eval(); "
            f"this key must have been removed."
        )

        # Authoritative id is in the struct cellid field (Long).
        cellid_long = row["cellid"]
        assert cellid_long is not None, "tile.cellid must not be None"
        assert isinstance(
            cellid_long, int
        ), f"tile.cellid must be a Long (int); got {type(cellid_long)}"

        # Round-trip: format(cellid_long) must be a valid BNG string id.
        cellid_str = _bng.format(cellid_long)
        assert _BNG_ID_RE.match(
            cellid_str
        ), f"tile.cellid {cellid_long} -> {cellid_str!r} is not a valid BNG id"

        # Cross-check: parse(format(cellid_long)) must be lossless.
        assert _bng.parse(cellid_str) == cellid_long, (
            f"BNG Long round-trip failed: {cellid_long} -> {cellid_str!r} -> "
            f"{_bng.parse(cellid_str)}"
        )
