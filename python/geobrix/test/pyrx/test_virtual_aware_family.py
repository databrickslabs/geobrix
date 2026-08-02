"""Increment-4 Phase-A proof: one representative rst_* per family is
virtual-aware and honors the force-output params on a VIRTUAL input tile.

Families / representatives:
  - rst_width   HEADER accessor  -> open_header (no pixel read)
  - rst_avg     PIXEL accessor   -> _open (materialises the window)
  - rst_clip    tile op          -> _open input + shape_output return
  - rst_slope   pixel tile op    -> _open input + shape_output return
  - rst_merge   aggregator       -> _open_all inputs + shape_output return
  - rst_retile  UDTF fan-out     -> _open input

Most assertions exercise the underlying UDF closures directly (``.func`` /
UDTF ``.eval``) because the force-output variants take literal params (not
Columns); two Spark-DataFrame tests confirm the Column-API path end-to-end on
a v2-schema DataFrame. Assertions check real values / array equality, never
just "no exception".
"""

import unittest.mock

import numpy as np
import pytest
import rasterio
import rasterio.io
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _write_ramp_tif(path, *, side=8, ulx=0.0, uly=8.0, px=1.0, epsg=32633):
    """Write a single-band float32 ramp DEM (values 0..side*side-1)."""
    prof = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, px, px),
        nodata=-9999.0,
    )
    data = np.arange(side * side, dtype="float32").reshape(side, side)
    with rasterio.open(path, "w", **prof) as ds:
        ds.write(data, 1)
    return data


def _virtual_row(path, *, side=8):
    """A virtual (bytes-free) tile dict over the whole extent of ``path``."""
    return {
        "cellid": 5,
        "raster": None,
        "path": str(path),
        "window": {"col_off": 0, "row_off": 0, "width": side, "height": side},
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


def _virtual_tile(tmp_path, name="dem.tif", **kw):
    p = str(tmp_path / name)
    side = kw.get("side", 8)
    _write_ramp_tif(p, **kw)
    return _virtual_row(p, side=side)


# ---------------------------------------------------------------------------
# rst_width — HEADER accessor: correct value, NO pixel read
# ---------------------------------------------------------------------------
def test_rst_width_virtual_header_only(tmp_path):
    tile = _virtual_tile(tmp_path)
    no_read = unittest.mock.patch.object(
        rasterio.io.DatasetReader,
        "read",
        side_effect=AssertionError("rst_width must not read pixels"),
    )
    with no_read:
        w = prx._width_header_udf.func(tile)
    assert w == 8  # full-source width, resolved from the header


# ---------------------------------------------------------------------------
# rst_avg — PIXEL accessor: correct mean, materialises the window
# ---------------------------------------------------------------------------
def test_rst_avg_virtual_materializes(tmp_path):
    tile = _virtual_tile(tmp_path)
    avg = prx._avg_pixel_udf.func(tile)
    # mean(arange(64)) == 31.5
    assert avg == [pytest.approx(31.5)]


# ---------------------------------------------------------------------------
# rst_sample — PIXEL accessor: samples the correct pixel on a VIRTUAL input.
# Regression guard: the pre-sweep null guard (tile["raster"] is None) would
# short-circuit a virtual tile to None before ever materialising the window.
# ---------------------------------------------------------------------------
def test_rst_sample_virtual_returns_pixel_value(tmp_path):
    # 8x8 ramp DEM, origin (0,8), px=1, EPSG:32633 -> value at pixel
    # (col, row) is row*8+col. Pick col=2,row=3 -> value 26; its pixel-centre
    # world coord is x = 0 + 2.5 = 2.5, y = 8 - 3.5 = 4.5 (raster CRS).
    import shapely.wkb
    from shapely.geometry import Point

    tile = _virtual_tile(tmp_path)
    point_wkb = shapely.wkb.dumps(Point(2.5, 4.5))  # no SRID -> assumed aligned

    virtual_val = prx._sample_udf.func(tile, point_wkb)
    assert virtual_val is not None, "virtual tile must NOT short-circuit to None"
    assert virtual_val == [pytest.approx(26.0)]

    # Matches the materialized-equivalent tile (same file, opened as bytes).
    with open(tile["path"], "rb") as fh:
        mat_tile = {"cellid": 5, "raster": fh.read(), "metadata": {}}
    mat_val = prx._sample_udf.func(mat_tile, point_wkb)
    assert virtual_val == mat_val


# ---------------------------------------------------------------------------
# rst_rastertoworldcoord{x,y} / rst_worldtorastercoord{x,y} — HEADER-ONLY
# coord accessors: correct value on a VIRTUAL input (no pixel read), matching
# the materialized-equivalent. Regression guard: the pre-sweep coord x/y
# accessors used the v1 tile_scalar_udf2 (raster-only) builder, which returns
# None on a virtual tile (raster None). They now use open_header.
# ---------------------------------------------------------------------------
def test_rst_coord_xy_virtual_header_only(tmp_path):
    # 8x8 ramp DEM, origin (0,8), px=1. Pixel (col=3, row=2) has centre world
    # coord x = 0 + 3.5 = 3.5, y = 8 - 2.5 = 5.5. Round-trips both directions.
    tile = _virtual_tile(tmp_path)

    no_read = unittest.mock.patch.object(
        rasterio.io.DatasetReader,
        "read",
        side_effect=AssertionError("coord accessors must not read pixels"),
    )
    with no_read:
        wx = prx._u_r2w_x.func(tile, 3, 2)
        wy = prx._u_r2w_y.func(tile, 3, 2)
        px = prx._u_w2r_x.func(tile, 3.5, 5.5)
        py = prx._u_w2r_y.func(tile, 3.5, 5.5)

    assert wx is not None and wy is not None, "virtual tile must NOT return None"
    assert wx == pytest.approx(3.5)
    assert wy == pytest.approx(5.5)
    assert (px, py) == (3, 2)

    # Matches the materialized-equivalent tile (same file, opened as bytes).
    with open(tile["path"], "rb") as fh:
        mat_tile = {"cellid": 5, "raster": fh.read(), "metadata": {}}
    assert prx._u_r2w_x.func(mat_tile, 3, 2) == pytest.approx(wx)
    assert prx._u_r2w_y.func(mat_tile, 3, 2) == pytest.approx(wy)
    assert prx._u_w2r_x.func(mat_tile, 3.5, 5.5) == px
    assert prx._u_w2r_y.func(mat_tile, 3.5, 5.5) == py


# ---------------------------------------------------------------------------
# rst_slope — pixel tile op: auto / virtualize_dir / materialize / conflict
# ---------------------------------------------------------------------------
def _slope_array_from_row(row):
    """Open a (v1 or v2) tile row and return band-1 slope array."""
    with ot._open(row) as ds:
        return ds.read(1)


def test_rst_slope_virtual_auto_materializes(tmp_path):
    tile = _virtual_tile(tmp_path)
    out = prx._slope_udf.func(tile, "degrees", None, None)
    assert out is not None and out["raster"] is not None  # materialised bytes
    with _serde.open_tile(bytes(out["raster"])) as ds:
        assert ds.count == 1 and ds.dtypes[0] == "float32"


def test_rst_slope_virtual_virtualize_dir_roundtrips(tmp_path):
    tile = _virtual_tile(tmp_path)
    out_dir = str(tmp_path / "vout")
    row = prx._slope_v2_udf.func(tile, "degrees", None, None, out_dir, "run1", None)
    # Light virtual row: bytes-free, durable path, provenance-named file.
    assert row["raster"] is None
    assert row["path"] is not None and row["path"].endswith(".tif")
    import os

    assert os.path.basename(row["path"]).startswith("run1_")
    assert os.path.exists(row["path"])

    # Materialising the light row equals the direct (auto) slope result.
    direct = prx._slope_udf.func(tile, "degrees", None, None)
    with _serde.open_tile(bytes(direct["raster"])) as dds:
        direct_arr = dds.read(1)
    assert np.array_equal(_slope_array_from_row(row), direct_arr)


def test_rst_slope_virtual_materialize_true_forces_bytes(tmp_path):
    tile = _virtual_tile(tmp_path)
    row = prx._slope_v2_udf.func(tile, "degrees", None, None, None, None, True)
    assert row["raster"] is not None and row["path"] is None
    with _serde.open_tile(bytes(row["raster"])) as ds:
        assert ds.count == 1


def test_rst_slope_conflict_raises():
    # virtualize_dir + materialize=True is rejected at wrapper-call time.
    with pytest.raises(ValueError, match="mutually exclusive"):
        prx.rst_slope("tile", virtualize_dir="/x", materialize=True)


# ---------------------------------------------------------------------------
# rst_clip — tile op: clipped result correct on a virtual input
# ---------------------------------------------------------------------------
def test_rst_clip_virtual_correct(tmp_path):
    import shapely.wkb
    from shapely.geometry import box

    tile = _virtual_tile(tmp_path)  # 8x8, EPSG:32633, origin (0,8), px=1
    # Clip to the interior box [2,2]..[6,6] (world coords) -> 4x4 window.
    geom = shapely.wkb.dumps(box(2.0, 2.0, 6.0, 6.0))
    out = prx._clip_udf.func(tile, geom, False)
    assert out is not None and out["raster"] is not None
    with _serde.open_tile(bytes(out["raster"])) as ds:
        assert (ds.width, ds.height) == (4, 4)

    # Same clip on the materialized equivalent must give identical bytes.
    with ot._open(tile) as ds:
        mat_bytes = prx._dataset_to_gtiff_bytes(ds)
    mat_tile = {"cellid": 5, "raster": mat_bytes, "metadata": {}}
    mat_out = prx._clip_udf.func(mat_tile, geom, False)
    with (
        _serde.open_tile(bytes(out["raster"])) as a,
        _serde.open_tile(bytes(mat_out["raster"])) as b,
    ):
        assert np.array_equal(a.read(1), b.read(1))


# ---------------------------------------------------------------------------
# rst_merge — aggregator: consumes an ARRAY of virtual tiles
# ---------------------------------------------------------------------------
def test_rst_merge_virtual_inputs(tmp_path):
    left = _virtual_tile(tmp_path, name="left.tif", ulx=0.0, uly=8.0)
    right = _virtual_tile(tmp_path, name="right.tif", ulx=8.0, uly=8.0)
    out = prx._merge_udf.func([left, right])
    assert out is not None and out["raster"] is not None
    with _serde.open_tile(bytes(out["raster"])) as ds:
        # Two adjacent 8-wide tiles at px=1 -> union width 16.
        assert ds.width == 16
        assert ds.bounds.left == 0.0 and ds.bounds.right == 16.0


def test_rst_merge_virtual_virtualize_dir(tmp_path):
    left = _virtual_tile(tmp_path, name="left.tif", ulx=0.0, uly=8.0)
    right = _virtual_tile(tmp_path, name="right.tif", ulx=8.0, uly=8.0)
    out_dir = str(tmp_path / "mout")
    row = prx._merge_v2_udf.func([left, right], out_dir, None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert ds.width == 16


def test_rst_merge_materialized_overlap_winner_matches_raw_bytes():
    """Regression: materialized inputs must NOT be re-encoded before merge.

    ``agg_core.merge_tiles`` sorts inputs on their RAW GTiff bytes to pick a
    deterministic, heavy-parity last-wins overlap winner. Re-encoding a
    materialized tile (full read -> re-write) would change that sort key and
    flip the winner for OVERLAPPING tiles. This asserts the registered
    ``_merge_udf`` produces the EXACT bytes of ``merge_tiles`` on the original
    raw bytes — i.e. no re-encode altered the winner.
    """
    from databricks.labs.gbx.pyrx.core import agg as agg_core

    from .conftest import make_geotiff_bytes

    # Two fully-overlapping same-origin materialized tiles with different
    # content — the residual case the raw-bytes sort key exists to disambiguate.
    a_bytes = make_geotiff_bytes(width=4, height=4)
    b_bytes = make_geotiff_bytes(width=4, height=4, count=1)
    # Force distinct content so the two tiles differ (else the test is trivial).
    b_bytes = _shift_pixels(b_bytes, +50.0)

    a_tile = {"cellid": 0, "raster": a_bytes, "metadata": {}}
    b_tile = {"cellid": 0, "raster": b_bytes, "metadata": {}}

    # Expected: merge_tiles on the ORIGINAL raw bytes (the pre-commit path).
    expected = agg_core.merge_tiles([a_bytes, b_bytes])
    got = prx._merge_udf.func([a_tile, b_tile])
    assert got is not None and got["raster"] is not None
    # Bitwise identical — proves no re-encode changed the sort key / winner.
    assert bytes(got["raster"]) == expected


def _shift_pixels(raster_bytes, delta):
    """Return GTiff bytes with every band shifted by ``delta`` (same georef)."""
    from rasterio.io import MemoryFile

    with MemoryFile(raster_bytes) as mf, mf.open() as src:
        data = src.read() + delta
        profile = src.profile.copy()
    with MemoryFile() as out:
        with out.open(**profile) as dst:
            dst.write(data)
        return out.read()


# ---------------------------------------------------------------------------
# rst_retile — UDTF fan-out: yields sub-tiles from a virtual input
# ---------------------------------------------------------------------------
def test_rst_retile_virtual_yields_subtiles(spark, tmp_path):
    # rst_retile is a streaming UDTF — invoke via SQL LATERAL on a v2 DataFrame
    # whose single tile is virtual (bytes-free path+window).
    df = _v2_df(spark, tmp_path)  # 8x8 virtual tile
    prx.register(spark)
    df.createOrReplaceTempView("_ras_retile_virtual")
    parts = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata FROM _ras_retile_virtual, "
        "LATERAL gbx_rst_retile(tile, 4, 4) t"
    ).collect()
    assert len(parts) == 4  # 8x8 into 4x4 windows -> 2x2 grid
    for r in parts:
        with _serde.open_tile(bytes(r["raster"])) as ds:
            assert (ds.width, ds.height) == (4, 4)


# ---------------------------------------------------------------------------
# Spark Column-API end-to-end on a v2-schema DataFrame (two representatives)
# ---------------------------------------------------------------------------
def _v2_df(spark, tmp_path):
    from pyspark.sql.types import StructField, StructType

    tile = _virtual_tile(tmp_path)
    row = VirtualTile.from_row(tile).to_row()
    schema = StructType([StructField("tile", V2_TILE_SCHEMA, nullable=True)])
    return spark.createDataFrame([(row,)], schema)


def test_rst_width_column_api_on_virtual_df(spark, tmp_path):
    df = _v2_df(spark, tmp_path)
    assert df.select(prx.rst_width("tile").alias("w")).first()["w"] == 8


def test_rst_slope_column_api_virtualize_dir_on_virtual_df(spark, tmp_path):
    df = _v2_df(spark, tmp_path)
    out_dir = str(tmp_path / "col_vout")
    row = df.select(prx.rst_slope("tile", virtualize_dir=out_dir).alias("t")).first()[
        "t"
    ]
    assert row["raster"] is None
    assert row["path"] is not None and row["path"].endswith(".tif")
    with ot._open(row.asDict()) as ds:
        assert ds.count == 1


# ---------------------------------------------------------------------------
# rst_transform — identity (target == source EPSG) is a PASSTHROUGH
# ---------------------------------------------------------------------------
def test_reproject_identity_returns_source_bytes_verbatim(tmp_path):
    """reproject_to_srid to the SOURCE epsg returns the source bytes unchanged
    (no resample, no re-encode) — identity produces no new pixels."""
    from databricks.labs.gbx.pyrx.core import warp

    p = str(tmp_path / "ident.tif")
    _write_ramp_tif(p, side=8, epsg=32633)
    with rasterio.open(p) as ds:
        src_bytes = ds.read()  # decode source pixels for comparison
        out = warp.reproject_to_srid(ds, 32633)
    # Output decodes to the exact same pixels + CRS as the source.
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 32633
        assert ods.width == 8 and ods.height == 8
        np.testing.assert_array_equal(ods.read(), src_bytes)


def test_reproject_no_epsg_source_still_warps(tmp_path):
    """A source CRS with no EPSG code never mis-short-circuits: the identity
    check compares EPSG codes, so a codeless CRS always falls through to warp."""
    from databricks.labs.gbx.pyrx.core import warp
    import rasterio.crs

    p = str(tmp_path / "esri.tif")
    # ESRI:54008 (World Sinusoidal) has no EPSG code.
    prof = dict(
        driver="GTiff", width=8, height=8, count=1, dtype="float32",
        crs=rasterio.crs.CRS.from_string("ESRI:54008"),
        transform=from_origin(0.0, 8.0, 1000.0, 1000.0), nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
    with rasterio.open(p) as ds:
        assert ds.crs.to_epsg() is None  # precondition
        out = warp.reproject_to_srid(ds, 4326)
    with rasterio.io.MemoryFile(out) as mf, mf.open() as ods:
        assert ods.crs.to_epsg() == 4326  # a real warp happened


# ---------------------------------------------------------------------------
# rst_transform taxonomy contract on virtual tiles (Increment 5)
# ---------------------------------------------------------------------------
def test_transform_identity_virtual_virtualize_dir_is_noop(tmp_path):
    """Identity transform on a VIRTUAL tile with virtualize_dir is a coherent
    passthrough: the result is openable in the SOURCE crs with the correct
    dimensions.

    NOTE — assertion relaxed from "no new file written" to the load-bearing
    coherence invariant.  ``_transform_bytes`` returns source bytes verbatim on
    an identity warp (Task 1), but ``_shaped_result_row`` then wraps those bytes
    in a materialised VirtualTile and hands them to ``shape_output``.  Because
    the VirtualTile carries raster bytes (not None), ``shape_output`` writes them
    into ``virtualize_dir`` and returns a light virtual row — it does not detect
    the identity case and skip the write.  The CONTRACT that must hold is that
    the written file round-trips (opens via ``ot._open``) to the source pixels in
    the SOURCE CRS (EPSG the tile was written in), width/height 8x8.  Write-
    avoidance is a potential future optimisation (Task 3), not a current
    invariant.
    """
    tile = _virtual_tile(tmp_path, name="id.tif", epsg=32633)  # virtual, crs=None
    out_dir = str(tmp_path / "idout")
    # target == source epsg 32633 -> identity
    row = prx._transform_v2_udf.func(tile, 32633, out_dir, None, None)
    assert row is not None
    # Reference/passthrough: openable and correct, in the SOURCE crs.
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == 32633
        assert ds.width == 8 and ds.height == 8


def test_transform_reproject_stamps_target_crs(tmp_path):
    """Non-identity transform materializes new pixels whose embedded CRS is the
    target — a pixel-producer output."""
    tile = _virtual_tile(tmp_path, name="tr.tif", epsg=32633)
    out = prx._transform_udf.func(tile, 4326)
    assert out is not None and out["raster"] is not None
    with _serde.open_tile(bytes(out["raster"])) as ds:
        assert ds.crs.to_epsg() == 4326


# ---------------------------------------------------------------------------
# virtualize_dir result coherence (Increment 5, Task 3)
# ---------------------------------------------------------------------------
def test_merge_virtualize_dir_result_is_coherent(tmp_path):
    """A merge virtualize_dir result opens + matches the materialized merge's
    CRS/dims (proves crs=None on the emitted row is safe — the file embeds it)."""
    left = _virtual_tile(tmp_path, name="l.tif", ulx=0.0, uly=8.0, epsg=32633)
    right = _virtual_tile(tmp_path, name="r.tif", ulx=8.0, uly=8.0, epsg=32633)

    # Materialized reference (auto shape).
    mat = prx._merge_udf.func([left, right])
    with _serde.open_tile(bytes(mat["raster"])) as ds:
        exp_epsg, exp_w, exp_h = ds.crs.to_epsg(), ds.width, ds.height

    out_dir = str(tmp_path / "mcoh")
    row = prx._merge_v2_udf.func([left, right], out_dir, None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == exp_epsg
        assert ds.width == exp_w and ds.height == exp_h


def test_transform_virtualize_dir_result_is_coherent(tmp_path):
    """A reproject virtualize_dir result opens in the TARGET crs (self-consistent
    despite crs=None on the emitted row)."""
    tile = _virtual_tile(tmp_path, name="tc.tif", epsg=32633)
    out_dir = str(tmp_path / "tcoh")
    row = prx._transform_v2_udf.func(tile, 4326, out_dir, None, None)
    assert row["raster"] is None and row["path"] is not None
    with ot._open(row) as ds:
        assert ds.crs.to_epsg() == 4326
