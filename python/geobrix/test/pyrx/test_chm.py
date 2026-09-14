"""Tests for pyrx gbx_rst_chm (Canopy Height Model).

CHM = clamp(align(DSM->DEM) - DEM, min=0).
NoData in either input propagates to the output.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin


# ---------------------------------------------------------------------------
# Local helper: build GTiff bytes from a custom numpy array
# ---------------------------------------------------------------------------


def _make_geotiff_bytes(
    data,
    nodata=-9999.0,
    epsg=4326,
    ulx=10.0,
    uly=50.0,
    pixel_size=0.5,
):
    """Return single-band Float32 GTiff bytes from *data* (2-D array)."""
    arr = np.asarray(data, dtype="float32")
    h, w = arr.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, pixel_size, pixel_size),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# Step 1 / Step 4 test (RED -> GREEN after implementation)
# ---------------------------------------------------------------------------


def test_chm_subtract_and_clamp(spark):
    """CHM = max(DSM-DEM, 0) on aligned same-grid inputs."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark, only=["gbx_rst_chm", "gbx_rst_fromcontent"])

    # DSM [[10,20],[5,8]] - DEM [[10,15],[7,8]] = [[0,5],[-2,0]] -> clamp [[0,5],[0,0]]
    dsm = _make_geotiff_bytes(np.array([[10.0, 20.0], [5.0, 8.0]], dtype="float32"))
    dem = _make_geotiff_bytes(np.array([[10.0, 15.0], [7.0, 8.0]], dtype="float32"))

    df = spark.createDataFrame([(dsm, dem)], "dsm binary, dem binary")
    out = df.selectExpr(
        "gbx_rst_chm(gbx_rst_fromcontent(dsm, 'GTiff'), gbx_rst_fromcontent(dem, 'GTiff')) AS r"
    ).collect()[0]["r"]

    with MemoryFile(bytes(out["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)

    assert arr[0, 0] == pytest.approx(0.0)
    assert arr[0, 1] == pytest.approx(5.0)
    assert arr[1, 0] == pytest.approx(0.0)
    assert arr[1, 1] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Step 5 tests
# ---------------------------------------------------------------------------


def test_chm_negative_surface_all_zeros(spark):
    """When DSM < DEM everywhere, CHM must be all zeros (no negatives)."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark, only=["gbx_rst_chm", "gbx_rst_fromcontent"])

    # DSM is uniformly lower than DEM -> all differences negative -> clamp to 0
    dsm = _make_geotiff_bytes(np.full((3, 3), 5.0, dtype="float32"))
    dem = _make_geotiff_bytes(np.full((3, 3), 10.0, dtype="float32"))

    df = spark.createDataFrame([(dsm, dem)], "dsm binary, dem binary")
    out = df.selectExpr(
        "gbx_rst_chm(gbx_rst_fromcontent(dsm, 'GTiff'), gbx_rst_fromcontent(dem, 'GTiff')) AS r"
    ).collect()[0]["r"]

    with MemoryFile(bytes(out["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)

    assert np.all(arr == pytest.approx(0.0)), f"Expected all zeros, got: {arr}"


def test_chm_misaligned_grid_output_shape_equals_dem(spark):
    """CHM aligns DSM to DEM grid; output shape must match the DEM."""
    from databricks.labs.gbx.pyrx import functions as fns

    fns.register(spark, only=["gbx_rst_chm", "gbx_rst_fromcontent"])

    # DEM: 2x2 at 1.0-degree pixels
    dem = _make_geotiff_bytes(
        np.array([[10.0, 12.0], [8.0, 9.0]], dtype="float32"),
        pixel_size=1.0,
    )
    # DSM: 4x4 at 0.5-degree pixels (same general extent, different grid)
    dsm = _make_geotiff_bytes(
        np.full((4, 4), 15.0, dtype="float32"),
        pixel_size=0.5,
    )

    df = spark.createDataFrame([(dsm, dem)], "dsm binary, dem binary")
    out = df.selectExpr(
        "gbx_rst_chm(gbx_rst_fromcontent(dsm, 'GTiff'), gbx_rst_fromcontent(dem, 'GTiff')) AS r"
    ).collect()[0]["r"]

    with MemoryFile(bytes(out["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)
        out_h, out_w = arr.shape

    # Output must be on the DEM's 2x2 grid
    assert out_h == 2, f"Expected height 2 (DEM grid), got {out_h}"
    assert out_w == 2, f"Expected width 2 (DEM grid), got {out_w}"
    # DSM=15 > DEM everywhere -> CHM should be positive (no clamping here)
    assert np.all(arr >= 0.0), "CHM must be non-negative after clamping"
