"""Tests for rst_clip_grouped — first pixel op via the grouped executor.

Exercises the materialized-tile fallback path (local[2], no FILE type needed).
FILE fast-path is validated on-cluster in Task 9.

Key proof: rst_clip_grouped pixel-equals the scalar rst_clip on the same
materialized tile (same clip geometry, same all_touched flag).
"""

import numpy as np
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructField as SF
from pyspark.sql.types import StructType
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

# Raster parameters:
# origin (10.0, 50.0), pixel 0.5x0.5, 4 wide × 3 tall
# → extent west=10.0 east=12.0 north=50.0 south=48.5
_RASTER_W = 4
_RASTER_H = 3


def _make_bytes(width=_RASTER_W, height=_RASTER_H, count=1, epsg=4326):
    """In-memory single-band GTiff bytes (float32) with a known georeference."""
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


@pytest.fixture(scope="module")
def raster_bytes():
    """Single-band 4×3 GTiff with known pixel values in raster CRS EPSG:4326."""
    return _make_bytes()


@pytest.fixture(scope="module")
def materialized_tile_df(spark, raster_bytes):
    """DataFrame with two materialized tiles (cellid 1 and 2) for the same raster."""
    rows = [
        (VirtualTile.from_v1(cellid=1, raster=raster_bytes).to_row(),),
        (VirtualTile.from_v1(cellid=2, raster=raster_bytes).to_row(),),
    ]
    return spark.createDataFrame(rows, StructType([SF("tile", V2_TILE_SCHEMA)]))


@pytest.fixture(scope="module")
def clip_geom_wkb():
    """WKB for a box covering the left half of the raster (x: 10→11, y: 48.5→50)."""
    return box(10.0, 48.5, 11.0, 50.0).wkb


# ---------------------------------------------------------------------------
# Step 1 / Step 2: basic import + output test (drives the TDD RED phase)
# ---------------------------------------------------------------------------


def test_rst_clip_grouped_produces_clipped_tile(materialized_tile_df, clip_geom_wkb):
    """rst_clip_grouped returns a non-null clipped tile for a materialized tile."""
    from databricks.labs.gbx.pyrx.functions import rst_clip_grouped

    out = rst_clip_grouped(
        materialized_tile_df, clip_geom_wkb, all_touched=False, out_col="clipped"
    )
    rows = out.select("clipped").collect()
    assert len(rows) == 2
    for r in rows:
        tile = r["clipped"]
        assert tile is not None, "Expected a clipped tile, got None"
        assert tile["raster"] is not None, "Clipped tile raster bytes must not be None"


def test_rst_clip_grouped_cellids_preserved(materialized_tile_df, clip_geom_wkb):
    """Output tiles carry the same cellid as the corresponding input tiles (R1)."""
    from databricks.labs.gbx.pyrx.functions import rst_clip_grouped

    out = rst_clip_grouped(
        materialized_tile_df, clip_geom_wkb, all_touched=False, out_col="clipped"
    )
    input_ids = {r["tile"]["cellid"] for r in materialized_tile_df.collect()}
    output_ids = {r["clipped"]["cellid"] for r in out.select("clipped").collect()}
    assert (
        output_ids == input_ids
    ), f"Cellid mismatch: input={input_ids} output={output_ids}"


# ---------------------------------------------------------------------------
# Step 5 / Step 6: fallback-parity test
# ---------------------------------------------------------------------------


def test_rst_clip_grouped_parity_with_scalar(spark, raster_bytes, clip_geom_wkb):
    """rst_clip_grouped pixel-equals the scalar clip UDF on the same materialized tile.

    Uses _uf_clip with file_ref=None to bypass try_to_file (not available in local
    mode) while exercising the same clip_to_geom math.  This is the intended
    fallback-path comparison: grouped executor (view='pixels') vs per-row UDF.
    """
    from databricks.labs.gbx.pyrx.functions import _uf_clip, rst_clip_grouped

    # Single tile so comparison is 1-to-1.
    df = spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=7, raster=raster_bytes).to_row(),)],
        StructType([SF("tile", V2_TILE_SCHEMA)]),
    )

    # Grouped variant (materialized fallback, view="pixels")
    grouped_row = (
        rst_clip_grouped(df, clip_geom_wkb, all_touched=False, out_col="clipped")
        .select("clipped")
        .first()
    )
    assert grouped_row is not None
    grouped_tile = grouped_row["clipped"]
    assert grouped_tile is not None and grouped_tile["raster"] is not None

    # Scalar variant: _uf_clip with file_ref=None so open_tile uses the
    # materialized raster bytes (same path as grouped executor fallback).
    scalar_row = (
        df.withColumn(
            "clipped",
            _uf_clip(
                F.col("tile"),
                F.lit(None),  # file_ref=None → materialized path
                F.lit(clip_geom_wkb),
                F.lit(False),
                F.lit(None),  # clip_crs=None
            ),
        )
        .select("clipped")
        .first()
    )
    assert scalar_row is not None
    scalar_tile = scalar_row["clipped"]
    assert scalar_tile is not None and scalar_tile["raster"] is not None

    # Pixel equality; PySpark returns BinaryType as bytearray, cast to bytes.
    def _read_pixels(raw) -> np.ndarray:
        with MemoryFile(bytes(raw)) as mf:
            with mf.open() as ds:
                return ds.read()

    grouped_arr = _read_pixels(grouped_tile["raster"])
    scalar_arr = _read_pixels(scalar_tile["raster"])
    np.testing.assert_array_equal(
        grouped_arr,
        scalar_arr,
        err_msg="rst_clip_grouped pixel output differs from scalar _uf_clip",
    )


# ---------------------------------------------------------------------------
# Task 6: parity tests for the remaining FILE-aware pixel-op _grouped variants
# ---------------------------------------------------------------------------
# Shared helpers


def _make_dem_bytes(width=8, height=8, epsg=32632):
    """Single-band float32 DEM-like GTiff in a projected CRS (EPSG:32632)."""
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    transform = from_bounds(0, 0, 800, 800, width, height)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=rasterio.CRS.from_epsg(epsg),
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width) * 10.0
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def _parity_df(spark, raster_bytes):
    """Single materialized tile for parity comparisons."""
    return spark.createDataFrame(
        [(VirtualTile.from_v1(cellid=42, raster=raster_bytes).to_row(),)],
        StructType([SF("tile", V2_TILE_SCHEMA)]),
    )


def _read_grouped(grouped_out, col="out"):
    """Read the first tile's raster bytes from the grouped output."""
    row = grouped_out.select(col).first()
    assert row is not None
    tile = row[col]
    assert (
        tile is not None and tile["raster"] is not None
    ), f"{col} tile or raster is None"
    return bytes(tile["raster"])


def _read_scalar(df, scalar_col_expr, col="out"):
    """Read the first tile's raster bytes from the scalar UDF output."""
    row = df.withColumn(col, scalar_col_expr).select(col).first()
    assert row is not None
    tile = row[col]
    assert (
        tile is not None and tile["raster"] is not None
    ), f"{col} tile or raster is None"
    return bytes(tile["raster"])


def _assert_pixel_equal(grouped_bytes, scalar_bytes, name):
    """Assert pixel-level equality between two GTiff byte buffers."""
    from rasterio.io import MemoryFile

    with MemoryFile(grouped_bytes) as mf:
        with mf.open() as ds:
            g_arr = ds.read().astype(float)
    with MemoryFile(scalar_bytes) as mf:
        with mf.open() as ds:
            s_arr = ds.read().astype(float)
    np.testing.assert_array_equal(
        g_arr, s_arr, err_msg=f"{name} grouped output differs from scalar"
    )


# --- rst_transform_grouped ---------------------------------------------------


def test_rst_transform_grouped_parity(spark, raster_bytes):
    """rst_transform_grouped pixel-equals _uf_transform on the same materialized tile."""
    from databricks.labs.gbx.pyrx.functions import _uf_transform, rst_transform_grouped

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_transform_grouped(df, 3857, out_col="out"))
    s_bytes = _read_scalar(df, _uf_transform(F.col("tile"), F.lit(None), F.lit(3857)))
    _assert_pixel_equal(g_bytes, s_bytes, "rst_transform_grouped")


# --- rst_to_webmercator_grouped -----------------------------------------------


def test_rst_to_webmercator_grouped_parity(spark, raster_bytes):
    """rst_to_webmercator_grouped pixel-equals _uf_to_webmercator (bilinear)."""
    from databricks.labs.gbx.pyrx.functions import (
        _uf_to_webmercator,
        rst_to_webmercator_grouped,
    )

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_to_webmercator_grouped(df, "bilinear", out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_to_webmercator(F.col("tile"), F.lit(None), F.lit("bilinear")),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_to_webmercator_grouped")


# --- rst_transformcrs_grouped ------------------------------------------------


def test_rst_transformcrs_grouped_parity(spark, raster_bytes):
    """rst_transformcrs_grouped pixel-equals _uf_transformcrs for EPSG:3857."""
    from databricks.labs.gbx.pyrx.functions import (
        _uf_transformcrs,
        rst_transformcrs_grouped,
    )

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_transformcrs_grouped(df, "EPSG:3857", out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_transformcrs(F.col("tile"), F.lit(None), F.lit("EPSG:3857")),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_transformcrs_grouped")


# --- rst_resample_grouped ---------------------------------------------------


def test_rst_resample_grouped_parity(spark, raster_bytes):
    """rst_resample_grouped pixel-equals _uf_resample (nearest, factor=2.0)."""
    from databricks.labs.gbx.pyrx.functions import _uf_resample, rst_resample_grouped

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_resample_grouped(df, 2.0, "nearest", out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_resample(F.col("tile"), F.lit(None), F.lit(2.0), F.lit("nearest")),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_resample_grouped")


# --- rst_resample_to_size_grouped -------------------------------------------


def test_rst_resample_to_size_grouped_parity(spark, raster_bytes):
    """rst_resample_to_size_grouped pixel-equals _uf_resample_to_size (2x2, nearest)."""
    from databricks.labs.gbx.pyrx.functions import (
        _uf_resample_to_size,
        rst_resample_to_size_grouped,
    )

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(
        rst_resample_to_size_grouped(df, 2, 2, "nearest", out_col="out")
    )
    s_bytes = _read_scalar(
        df,
        _uf_resample_to_size(
            F.col("tile"), F.lit(None), F.lit(2), F.lit(2), F.lit("nearest")
        ),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_resample_to_size_grouped")


# --- rst_resample_to_res_grouped --------------------------------------------


def test_rst_resample_to_res_grouped_parity(spark, raster_bytes):
    """rst_resample_to_res_grouped pixel-equals _uf_resample_to_res (1.0, nearest)."""
    from databricks.labs.gbx.pyrx.functions import (
        _uf_resample_to_res,
        rst_resample_to_res_grouped,
    )

    df = _parity_df(spark, raster_bytes)
    # Original pixel res = 0.5 degrees; resample to 1.0 degrees (halve pixel count)
    g_bytes = _read_grouped(
        rst_resample_to_res_grouped(df, 1.0, 1.0, "nearest", out_col="out")
    )
    s_bytes = _read_scalar(
        df,
        _uf_resample_to_res(
            F.col("tile"), F.lit(None), F.lit(1.0), F.lit(1.0), F.lit("nearest")
        ),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_resample_to_res_grouped")


# --- rst_threshold_grouped --------------------------------------------------


def test_rst_threshold_grouped_parity(spark, raster_bytes):
    """rst_threshold_grouped pixel-equals _uf_threshold (> 5.0)."""
    from databricks.labs.gbx.pyrx.functions import _uf_threshold, rst_threshold_grouped

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_threshold_grouped(df, ">", 5.0, out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_threshold(F.col("tile"), F.lit(None), F.lit(">"), F.lit(5.0)),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_threshold_grouped")


# --- rst_updatetype_grouped -------------------------------------------------


def test_rst_updatetype_grouped_parity(spark, raster_bytes):
    """rst_updatetype_grouped pixel-equals _uf_update_type (Float64)."""
    from databricks.labs.gbx.pyrx.functions import (
        _uf_update_type,
        rst_updatetype_grouped,
    )

    df = _parity_df(spark, raster_bytes)
    g_bytes = _read_grouped(rst_updatetype_grouped(df, "Float64", out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_update_type(F.col("tile"), F.lit(None), F.lit("Float64")),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_updatetype_grouped")


# --- rst_slope_grouped -------------------------------------------------------


def test_rst_slope_grouped_parity(spark):
    """rst_slope_grouped pixel-equals _uf_slope on a projected DEM tile."""
    from databricks.labs.gbx.pyrx.functions import _uf_slope, rst_slope_grouped

    dem_bytes = _make_dem_bytes()
    df = _parity_df(spark, dem_bytes)
    g_bytes = _read_grouped(rst_slope_grouped(df, "degrees", out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_slope(
            F.col("tile"), F.lit(None), F.lit("degrees"), F.lit(None), F.lit(None)
        ),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_slope_grouped")


# --- rst_aspect_grouped -------------------------------------------------------


def test_rst_aspect_grouped_parity(spark):
    """rst_aspect_grouped pixel-equals _uf_aspect on a projected DEM tile."""
    from databricks.labs.gbx.pyrx.functions import _uf_aspect, rst_aspect_grouped

    dem_bytes = _make_dem_bytes()
    df = _parity_df(spark, dem_bytes)
    g_bytes = _read_grouped(
        rst_aspect_grouped(df, trigonometric=False, zero_for_flat=False, out_col="out")
    )
    s_bytes = _read_scalar(
        df,
        _uf_aspect(F.col("tile"), F.lit(None), F.lit(False), F.lit(False)),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_aspect_grouped")


# --- rst_hillshade_grouped ---------------------------------------------------


def test_rst_hillshade_grouped_parity(spark):
    """rst_hillshade_grouped pixel-equals _uf_hillshade on a projected DEM tile."""
    from databricks.labs.gbx.pyrx.functions import _uf_hillshade, rst_hillshade_grouped

    dem_bytes = _make_dem_bytes()
    df = _parity_df(spark, dem_bytes)
    g_bytes = _read_grouped(rst_hillshade_grouped(df, 315.0, 45.0, 1.0, out_col="out"))
    s_bytes = _read_scalar(
        df,
        _uf_hillshade(
            F.col("tile"),
            F.lit(None),
            F.lit(315.0),
            F.lit(45.0),
            F.lit(1.0),
            F.lit(None),
            F.lit(None),
        ),
    )
    _assert_pixel_equal(g_bytes, s_bytes, "rst_hillshade_grouped")
