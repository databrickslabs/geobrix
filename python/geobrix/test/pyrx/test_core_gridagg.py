"""Pure-function tests for pyrx raster->grid aggregation (h3 + quadbin).

Spark-free: open the GTiff bytes with rasterio and call gridagg.raster_to_grid
directly, mirroring the heavyweight RST_{H3,Quadbin}_RasterToGrid semantics.
"""

import h3
import numpy as np
import pytest
import quadbin
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import gridagg

from .conftest import make_geotiff_bytes


def _open(raster_bytes):
    return MemoryFile(raster_bytes).open()


def _custom_raster(data, nodata=-9999.0, epsg=4326, origin=(10.0, 50.0), px=0.5):
    """Single-band GTiff from a 2-D numpy array with a WGS84 georeference."""
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- count: total counts == number of valid pixels, values are ints ---------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_count_total_equals_valid_pixels(grid):
    raster = make_geotiff_bytes(width=4, height=3, count=1)  # all 12 valid
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 6, grid, "count")
    assert len(result) == 1
    band = result[0]
    total = sum(c["measure"] for c in band)
    assert total == 12
    assert all(isinstance(c["measure"], int) for c in band)
    assert all(isinstance(c["cellID"], int) for c in band)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_count_excludes_nodata(grid):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    data[0, 0] = -9999.0  # one invalid pixel
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 7, grid, "count")
    total = sum(c["measure"] for c in result[0])
    assert total == 11


# --- avg / min / max: cell measures within [band min, band max] -------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["avg", "min", "max"])
def test_agg_within_band_range(grid, agg):
    data = np.arange(12, dtype="float32").reshape(3, 4)
    raster = _custom_raster(data)
    lo, hi = float(data.min()), float(data.max())
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 5, grid, agg)
    for c in result[0]:
        assert lo <= c["measure"] <= hi
        assert isinstance(c["measure"], float)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_avg_is_mean_of_cell_pixels(grid):
    # Coarse resolution so all pixels land in a single cell; avg == grand mean.
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "avg")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(5.0)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_sum_is_total_of_cell_pixels(grid):
    # Coarse resolution so all pixels land in a single cell; sum == grand total.
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "sum")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(20.0)
    assert isinstance(band[0]["measure"], float)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_sum_excludes_nodata(grid):
    # One nodata pixel is skipped; sum == 2+6+8 == 16 (single coarse cell).
    data = np.array([[2.0, -9999.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "sum")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(16.0)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_variance_is_population_variance_of_cell_pixels(grid):
    # Coarse resolution so [2,4,6,8] land in one cell. Population (ddof=0):
    # mean=5, sq-devs=[9,1,1,9] sum 20, variance = 20/4 = 5.0 (NOT 20/3).
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "variance")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(5.0)
    assert isinstance(band[0]["measure"], float)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_stddev_is_sqrt_of_population_variance(grid):
    # stddev == sqrt(population variance); sqrt(5.0) == 2.2360679...
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "stddev")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(np.sqrt(5.0))
    assert isinstance(band[0]["measure"], float)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
@pytest.mark.parametrize("agg", ["variance", "stddev"])
def test_single_pixel_cell_variance_stddev_zero(grid, agg):
    # A cell covering exactly one valid pixel has zero spread -> variance 0,
    # stddev 0 (clean under population semantics; no divide-by-(n-1)).
    # Fine resolution so each pixel occupies its own cell.
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _custom_raster(data)
    res = 12 if grid == "quadbin" else 9
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, res, grid, agg)
    band = result[0]
    assert len(band) == 4  # one cell per pixel
    for cell in band:
        assert cell["measure"] == pytest.approx(0.0)


# --- median: even/odd correctness on a hand-built single cell ---------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_median_even_count(grid):
    # 4 pixels -> single coarse cell; median of [1,2,3,4] = 2.5 (numpy.median).
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "median")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(2.5)


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_median_odd_count(grid):
    # 3 valid pixels (one nodata) -> single coarse cell; median of [1,3,5] = 3.
    data = np.array([[1.0, 3.0], [5.0, -9999.0]], dtype="float32")
    raster = _custom_raster(data)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 0, grid, "median")
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(3.0)


# --- multi-band -> outer array length == band count -------------------------
@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_multiband_outer_length(grid):
    raster = make_geotiff_bytes(width=4, height=3, count=2)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 6, grid, "count")
    assert len(result) == 2
    for band in result:
        assert sum(c["measure"] for c in band) == 12


# --- cell ids are real, positive int64 cells --------------------------------
def test_h3_cell_ids_are_valid():
    raster = make_geotiff_bytes(width=4, height=3, count=1)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 7, "h3", "count")
    for c in result[0]:
        cid = c["cellID"]
        assert 0 < cid < 2**63
        assert h3.is_valid_cell(h3.int_to_str(cid))


def test_quadbin_cell_ids_are_valid():
    raster = make_geotiff_bytes(width=4, height=3, count=1)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 10, "quadbin", "count")
    for c in result[0]:
        cid = c["cellID"]
        assert 0 < cid < 2**63
        tile = quadbin.cell_to_tile(cid)  # raises if not a real cell
        assert tile is not None


# --- resolution validation --------------------------------------------------
def test_h3_resolution_too_high_raises():
    raster = make_geotiff_bytes(width=2, height=2)
    with _open(raster) as ds:
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, 16, "h3", "count")


def test_quadbin_resolution_too_high_raises():
    raster = make_geotiff_bytes(width=2, height=2)
    with _open(raster) as ds:
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, 21, "quadbin", "count")


@pytest.mark.parametrize("grid", ["h3", "quadbin"])
def test_negative_resolution_raises(grid):
    raster = make_geotiff_bytes(width=2, height=2)
    with _open(raster) as ds:
        with pytest.raises(ValueError):
            gridagg.raster_to_grid(ds, -1, grid, "count")
