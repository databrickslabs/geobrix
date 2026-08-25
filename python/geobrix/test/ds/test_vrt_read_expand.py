"""Task 5 (SDD — Phase A/B native mini-COG mosaic): VRT expansion in the reader.

Tests that raster_gbx (and cog_gbx) detect a .vrt path at load time, parse the
VRT XML to enumerate member mini-COG paths, and emit one whole-file virtual tile
row per member — so all downstream rst_* ops work per-tile UNCHANGED.

Pure Python (no JAR, no osgeo).

Coverage:
  1. raster_gbx reads a persisted mosaic.vrt → row count == member count.
  2. Each row is a whole-file virtual tile: path→member, window→None.
  3. rst_avg(col("tile")) returns non-null values for each row (rst_* unchanged proof).
  4. A minted VRT (absolute SourceFilename paths, from mint_vrt) also expands correctly.
  5. cog_gbx reads the same .vrt → same row count (write→read round-trip).
  6. No osgeo import in raster.py (light-tier compliance).
  7. Loading the mosaic DIRECTORY must not double-count the VRT + tiles.
  8. _parse_vrt_members uses the hardened defusedxml parser.
  9. Quadbin mosaic.vrt expansion surfaces cellid + gridSystem in tile.metadata.
  10. Native (gridSystem="none") mosaic rows have NO cellid/gridSystem in metadata.
  11. cog_gbx load of a quadbin mosaic.vrt also surfaces cellid in tile.metadata.

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/ds/test_vrt_read_expand.py \\
        --log vrt-read-expand.log
"""

from __future__ import annotations

import importlib.util
import os
import pathlib

import numpy as np
import rasterio
from pyspark.sql.functions import col
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, parse_mosaic_options
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.functions import rst_avg

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TILE_W = 100
_TILE_H = 80
_PIXEL_SIZE = 10.0
_ORIGIN_X = 400000.0
_ORIGIN_Y = 5000000.0


def _write_src(
    path: str,
    w: int,
    h: int,
    dtype: str = "uint16",
) -> str:
    """Write a single GTiff source with deterministic nonzero pixel values."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(_ORIGIN_X, _ORIGIN_Y, _PIXEL_SIZE, _PIXEL_SIZE),
    )
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = (np.arange(w * h, dtype=np.uint32).reshape(1, h, w) + 1).astype(dtype)
    data = data % max(np.iinfo(dtype).max, 1)
    data[data == 0] = 1  # no all-zero tiles (avoid NoData pruning side-effects)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


def _write_tile(
    path: str,
    col_offset: int = 0,
    row_offset: int = 0,
    w: int = _TILE_W,
    h: int = _TILE_H,
    dtype: str = "uint16",
) -> str:
    """Write one GTiff tile spatially positioned in the grid (for mint_vrt tests)."""
    origin_x = _ORIGIN_X + col_offset * w * _PIXEL_SIZE
    origin_y = _ORIGIN_Y - row_offset * h * _PIXEL_SIZE
    seed = col_offset * 10000 + row_offset * 1000 + 1  # ensure nonzero pixels
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(origin_x, origin_y, _PIXEL_SIZE, _PIXEL_SIZE),
    )
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = (np.arange(w * h, dtype=np.uint32).reshape(1, h, w) + seed).astype(
        dtype
    ) % np.iinfo(dtype).max
    # Guarantee no all-zero rows (NoData pruning should not silence any tile)
    data[data == 0] = 1
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


def _build_mosaic(base_dir: pathlib.Path, n_cols: int = 2, n_rows: int = 1):
    """Write ONE large source raster and mosaic-tile it into mini-COGs + VRT.

    The CogGbxWriter receives a SINGLE input source (the large raster) and
    the tileSize option drives the grid split.  n_cols * n_rows mini-COGs are
    produced, referencing from mosaic.vrt.

    Returns (vrt_path, member_paths) where member_paths are the written mini-COGs.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    # One large source: n_cols * _TILE_W × n_rows * _TILE_H pixels
    src_w = n_cols * _TILE_W
    src_h = n_rows * _TILE_H
    src = str(base_dir / "source.tif")
    _write_src(src, w=src_w, h=src_h)

    out_dir = str(base_dir.parent / "mosaic_out")
    os.makedirs(out_dir, exist_ok=True)

    schema = StructType([StructField("path", StringType(), False)])
    opts = parse_mosaic_options(
        {
            "vrtMosaic": "true",
            "gridSystem": "none",
            "tileSize": str(
                _TILE_W
            ),  # mosaic writer tileSize is a single int (square tiles)
        }
    )
    writer = CogGbxWriter(
        out_dir,
        schema,
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=opts,
    )
    # Single source row — writer tiles it into n_cols*n_rows mini-COGs
    msg = writer.write(iter([{"path": src}]))
    writer.commit([msg])

    vrt_path = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt_path), f"mosaic.vrt not created at {vrt_path}"
    return vrt_path, msg.paths


# ---------------------------------------------------------------------------
# 1. raster_gbx reads a persisted mosaic.vrt → row count == member count
# ---------------------------------------------------------------------------


def test_vrt_raster_gbx_count(spark, tmp_path):
    """spark.read.format('raster_gbx').load(mosaic.vrt) returns one row per member."""
    base_dir = tmp_path / "src1"
    base_dir.mkdir()
    # 3-column × 1-row mosaic → 3 mini-COG members
    vrt_path, members = _build_mosaic(base_dir, n_cols=3, n_rows=1)

    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(vrt_path)
    count = df.count()
    expected = len(members)
    assert (
        count == expected
    ), f"Expected {expected} rows (one per member mini-COG), got {count}"


# ---------------------------------------------------------------------------
# 2. Each row is a whole-file virtual tile: path→member, window→None
# ---------------------------------------------------------------------------


def test_vrt_rows_are_whole_file_virtual(spark, tmp_path):
    """Each reader row has tile.path pointing to a member and tile.window == None."""
    base_dir = tmp_path / "src2"
    base_dir.mkdir()
    vrt_path, members = _build_mosaic(base_dir, n_cols=2, n_rows=1)

    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(vrt_path).collect()

    member_set = set(members)
    for row in rows:
        tile = row["tile"]
        assert (
            tile["path"] in member_set
        ), f"tile.path {tile['path']!r} not in expected members"
        assert tile["raster"] is None, "whole-file virtual tile must have raster=None"
        assert tile["window"] is None, "whole-file virtual tile must have window=None"


# ---------------------------------------------------------------------------
# 3. rst_avg(col("tile")) returns non-null values per row (rst_* unchanged proof)
# ---------------------------------------------------------------------------


def test_vrt_rst_avg_runs_unchanged(spark, tmp_path):
    """rst_avg applied to VRT-expanded tiles returns non-null per-member averages.

    This proves that downstream rst_* pixel ops work on VRT-expanded tiles without
    any modification — the payoff of VRT expansion into per-member tile rows.
    """
    base_dir = tmp_path / "src3"
    base_dir.mkdir()
    vrt_path, members = _build_mosaic(base_dir, n_cols=2, n_rows=1)

    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(vrt_path)
    result = df.select(rst_avg(col("tile")).alias("avg")).collect()

    assert len(result) == len(
        members
    ), f"Expected {len(members)} avg results, got {len(result)}"
    for i, row in enumerate(result):
        assert (
            row["avg"] is not None
        ), f"row {i}: rst_avg returned None (tile not readable)"
        assert len(row["avg"]) >= 1, f"row {i}: avg list is empty"
        assert not any(
            v is None for v in row["avg"]
        ), f"row {i}: avg contains None band values: {row['avg']}"


# ---------------------------------------------------------------------------
# 4. Minted VRT (absolute SourceFilename paths) also expands correctly
# ---------------------------------------------------------------------------


def test_vrt_minted_absolute_paths(spark, tmp_path):
    """mint_vrt produces a transient VRT with absolute paths; reader expands it correctly."""
    from databricks.labs.gbx.ds._mosaic import mint_vrt

    tile_dir = tmp_path / "mint_tiles"
    tile_dir.mkdir()
    tile_paths = []
    for c in range(3):
        p = str(tile_dir / f"tile_0_{c}.tif")
        _write_tile(p, col_offset=c, row_offset=0)
        tile_paths.append(p)

    vrt_path = mint_vrt(tile_paths)

    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(vrt_path)
    count = df.count()
    assert count == len(
        tile_paths
    ), f"mint_vrt expansion: expected {len(tile_paths)} rows, got {count}"

    rows = df.collect()
    abs_paths = {os.path.abspath(p) for p in tile_paths}
    for row in rows:
        tile = row["tile"]
        tile_abs = os.path.abspath(tile["path"])
        assert (
            tile_abs in abs_paths
        ), f"tile.path {tile['path']!r} not in minted tile set"
        assert (
            tile["window"] is None
        ), "minted-VRT tile must be whole-file (window=None)"


# ---------------------------------------------------------------------------
# 5. cog_gbx reads the same .vrt → same row count (round-trip)
# ---------------------------------------------------------------------------


def test_vrt_cog_gbx_reads_vrt(spark, tmp_path):
    """cog_gbx.load(mosaic.vrt) returns the same count as raster_gbx (round-trip)."""
    base_dir = tmp_path / "src5"
    base_dir.mkdir()
    # 2x2 mosaic → 4 mini-COG members
    vrt_path, members = _build_mosaic(base_dir, n_cols=2, n_rows=2)

    spark.dataSource.register(CogGbxDataSource)
    df = spark.read.format("cog_gbx").load(vrt_path)
    count = df.count()
    assert count == len(
        members
    ), f"cog_gbx round-trip: expected {len(members)} rows, got {count}"


# ---------------------------------------------------------------------------
# 6. No osgeo import in raster.py (light-tier compliance)
# ---------------------------------------------------------------------------


def test_vrt_no_osgeo_in_raster_py():
    """raster.py must not import osgeo — light-tier / Serverless compliance."""
    import re

    spec = importlib.util.find_spec("databricks.labs.gbx.ds.raster")
    assert spec is not None, "ds.raster module not found"
    src_text = pathlib.Path(spec.origin).read_text(encoding="utf-8")
    osgeo_imports = re.findall(r"^(?:import|from)\s+osgeo\b.*$", src_text, re.MULTILINE)
    assert not osgeo_imports, f"raster.py must not import osgeo: {osgeo_imports}"


# ---------------------------------------------------------------------------
# 7. FIX 5 — loading the mosaic DIRECTORY must not double-count the VRT + tiles
# ---------------------------------------------------------------------------


def test_dir_load_excludes_vrt(spark, tmp_path):
    """Loading the mosaic DIRECTORY yields one row per tile — not tiles + the VRT.

    A directory walk that treats mosaic.vrt as a walkable raster reads it as ONE
    extra whole-mosaic source on top of every tile_*.tif it indexes → double
    coverage.  The .vrt is an index (honored only when the load path points
    directly at it), so the directory walk must exclude it.
    """
    base_dir = tmp_path / "srcdir"
    base_dir.mkdir()
    vrt_path, members = _build_mosaic(base_dir, n_cols=3, n_rows=1)
    out_dir = os.path.dirname(vrt_path)
    assert os.path.exists(os.path.join(out_dir, "mosaic.vrt"))

    spark.dataSource.register(RasterGbxDataSource)
    count = spark.read.format("raster_gbx").load(out_dir).count()
    assert count == len(members), (
        f"directory load must yield {len(members)} tile rows (VRT excluded), "
        f"got {count} — the VRT was double-counted as a walkable member"
    )


# ---------------------------------------------------------------------------
# 8. FIX 2 — _parse_vrt_members uses the hardened defusedxml parser
# ---------------------------------------------------------------------------


def test_parse_vrt_members_uses_defusedxml(tmp_path):
    """External VRT parsing must be entity-hardened (defusedxml), not raw stdlib.

    defusedxml is declared in the [light-base] extra so production installs get
    it; the fallback to stdlib xml.etree exists only for resilience.  A VRT that
    declares an XML entity must be REJECTED (defusedxml raises) rather than
    expanded — proof the hardened parser is active.
    """
    import defusedxml  # noqa: F401  (must be importable in the light tier)
    from defusedxml.common import EntitiesForbidden

    from databricks.labs.gbx.ds.raster import _parse_vrt_members

    # Small, bounded entity declaration (not an actual bomb) — enough to trip
    # defusedxml's entity guard while never risking runaway expansion.
    bomb = (
        '<?xml version="1.0"?>\n'
        "<!DOCTYPE VRTDataset [\n"
        '  <!ENTITY a "aaaaaaaaaa">\n'
        "]>\n"
        '<VRTDataset rasterXSize="1" rasterYSize="1">\n'
        '  <VRTRasterBand dataType="Byte" band="1">\n'
        "    <SimpleSource>\n"
        '      <SourceFilename relativeToVRT="1">&a;.tif</SourceFilename>\n'
        "    </SimpleSource>\n"
        "  </VRTRasterBand>\n"
        "</VRTDataset>\n"
    )
    vrt = tmp_path / "bomb.vrt"
    vrt.write_text(bomb, encoding="utf-8")

    try:
        _parse_vrt_members(str(vrt))
    except EntitiesForbidden:
        return  # hardened parser rejected the entity — expected
    raise AssertionError(
        "_parse_vrt_members did not reject an entity-bearing VRT; the vulnerable "
        "stdlib xml.etree parser is active instead of defusedxml"
    )


# ---------------------------------------------------------------------------
# 9-11. Quadbin VRT expansion surfaces cellid + gridSystem in tile.metadata
#       (Task 5 — Phase B quadbin mini-COG mosaic reader)
# ---------------------------------------------------------------------------


def _write_qb_src(path: str) -> None:
    """200×200, 100 m pixels, EPSG:32630 (London area UTM 30N).

    Spans ≥ 2 quadbin resolution-12 cells (cells are ~6–10 km wide here).
    """
    w, h = 200, 200
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint16",
        crs="EPSG:32630",
        transform=from_origin(500000.0, 5700000.0, 100.0, 100.0),
    )
    data = np.arange(w * h, dtype="uint16").reshape(1, h, w) % np.iinfo("uint16").max
    data[data == 0] = 1
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _build_quadbin_mosaic_for_t5(base_dir: pathlib.Path):
    """Build a quadbin mini-COG mosaic; return (vrt_path, member_paths).

    Uses gridSystem='quadbin', gridResolution=12 so ≥2 cells are produced and
    each written mini-COG carries GBX_CELLID + GBX_GRIDSYSTEM tags.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    src = str(base_dir / "src.tif")
    _write_qb_src(src)

    out_dir = str(base_dir.parent / (base_dir.name + "_out"))
    os.makedirs(out_dir, exist_ok=True)

    schema = StructType([StructField("path", StringType(), False)])
    opts = parse_mosaic_options({"gridSystem": "quadbin", "gridResolution": "12"})
    writer = CogGbxWriter(
        out_dir, schema, overwrite=True, cog_blocksize=256, mosaic_opts=opts
    )
    msg = writer.write(iter([{"path": src}]))
    writer.commit([msg])

    vrt_path = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt_path), f"mosaic.vrt not created at {vrt_path}"
    assert len(msg.paths) >= 2, f"expected ≥2 quadbin mini-COGs, got {len(msg.paths)}"
    return vrt_path, msg.paths


def test_vrt_quadbin_cellid_in_metadata(spark, tmp_path):
    """Expanding a quadbin mosaic.vrt surfaces cellid and gridSystem in tile.metadata.

    Each member was written with GBX_CELLID and GBX_GRIDSYSTEM='quadbin' COG tags
    (Task 3).  The VRT expansion must read those tags and propagate them into the
    virtual tile row's metadata dict so downstream ops can dispatch by cell.
    """
    base_dir = tmp_path / "t9_qb"
    base_dir.mkdir()
    vrt_path, members = _build_quadbin_mosaic_for_t5(base_dir)

    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(vrt_path).collect()

    assert len(rows) == len(
        members
    ), f"Expected {len(members)} rows (one per member), got {len(rows)}"
    for row in rows:
        meta = row["tile"]["metadata"]
        assert (
            "cellid" in meta
        ), f"tile.metadata missing 'cellid'; got keys: {list(meta.keys())}"
        assert (
            "gridSystem" in meta
        ), f"tile.metadata missing 'gridSystem'; got keys: {list(meta.keys())}"
        assert (
            meta["gridSystem"] == "quadbin"
        ), f"expected gridSystem='quadbin', got {meta['gridSystem']!r}"
        cellid_val = int(meta["cellid"])
        assert cellid_val > 0, f"cellid must be a positive int; got {meta['cellid']!r}"


def test_vrt_native_no_cellid(spark, tmp_path):
    """Native (gridSystem='none') mosaic.vrt rows have no cellid/gridSystem in metadata.

    Native mini-COG members carry no GBX_CELLID tag, so the reader must emit
    tile rows without those keys rather than propagating absent/null values.
    """
    base_dir = tmp_path / "t10_native"
    base_dir.mkdir()
    # _build_mosaic uses gridSystem="none" → no GBX_CELLID tag on members
    vrt_path, _members = _build_mosaic(base_dir, n_cols=2, n_rows=1)

    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(vrt_path).collect()

    for row in rows:
        meta = row["tile"]["metadata"]
        assert (
            "cellid" not in meta
        ), f"native tile must not have 'cellid' in metadata; got {meta}"
        assert (
            "gridSystem" not in meta
        ), f"native tile must not have 'gridSystem' in metadata; got {meta}"


def test_vrt_cog_gbx_quadbin_cellid(spark, tmp_path):
    """cog_gbx.load(quadbin mosaic.vrt) inherits VRT-member cellid in tile.metadata.

    cog_gbx extends RasterGbxReader, so the same VRT-expansion path runs and
    the same GBX tag propagation applies.
    """
    base_dir = tmp_path / "t11_cog"
    base_dir.mkdir()
    vrt_path, members = _build_quadbin_mosaic_for_t5(base_dir)

    spark.dataSource.register(CogGbxDataSource)
    rows = spark.read.format("cog_gbx").load(vrt_path).collect()

    assert len(rows) == len(
        members
    ), f"cog_gbx: expected {len(members)} rows, got {len(rows)}"
    for row in rows:
        meta = row["tile"]["metadata"]
        assert (
            "cellid" in meta
        ), f"cog_gbx tile.metadata missing 'cellid'; got keys: {list(meta.keys())}"
        assert (
            "gridSystem" in meta
        ), f"cog_gbx tile.metadata missing 'gridSystem'; got keys: {list(meta.keys())}"
        assert (
            meta["gridSystem"] == "quadbin"
        ), f"cog_gbx expected gridSystem='quadbin', got {meta['gridSystem']!r}"
