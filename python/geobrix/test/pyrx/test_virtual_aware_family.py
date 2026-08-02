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
