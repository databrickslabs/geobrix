"""netcdf_gbx raster mode: the tile is the V2 tile the gbx_rst_* functions take, and bbox cuts the grid on read."""

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource, NetcdfRasterReader
from databricks.labs.gbx.ds.raster import _FilePartition, reader_schema_v2

# A 6 x 8 grid at 0.1 degree: lon -94.0 .. -93.3, lat 42.5 .. 42.0 (pixel edges -94.05 .. -93.25 and 42.55 .. 41.95).
# Pixel value = day * 100 + north-up row * 10 + col, so the pixels of any window are known exactly.
BBOX = "-93.88,42.02,-93.52,42.38"  # inside pixels, away from their edges: cols 1..5, rows 1..5
NORTH_UP = np.arange(6)[:, None] * 10 + np.arange(8)[None, :]


def _write_cube(path, ndays=4, ascending_lat=False):
    with Dataset(path, "w") as ds:
        ds.createDimension("day", ndays)
        ds.createDimension("lat", 6)
        ds.createDimension("lon", 8)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        north = 42.5 - 0.1 * np.arange(6)
        lat[:] = north[::-1] if ascending_lat else north
        lon[:] = -94.0 + 0.1 * np.arange(8)
        ds.createVariable("day", "f8", ("day",))[:] = 44925.0 + np.arange(ndays)
        v = ds.createVariable(
            "wind_speed", "f4", ("day", "lat", "lon"), fill_value=-9999.0
        )
        for d in range(ndays):
            grid = (d * 100 + NORTH_UP).astype("f4")
            v[d] = grid[::-1] if ascending_lat else grid
    return path


def _rows(path, **options):
    reader = NetcdfRasterReader({"path": path, "variable": "wind_speed", **options})
    return list(reader.read(_FilePartition(path, -1)))


def _pixels(tile):
    with MemoryFile(bytes(tile[1])) as mf, mf.open() as rds:
        return rds.read(), rds.transform, rds.count


def test_raster_schema_is_the_v2_tile():
    ds = NetcdfGbxDataSource(options={"path": "/tmp/none", "variable": "wind_speed"})
    assert ds.schema() == reader_schema_v2()


@pytest.mark.parametrize("ascending_lat", [False, True])
def test_bbox_reads_only_the_window(tmp_path, ascending_lat):
    p = _write_cube(str(tmp_path / "c.nc"), ndays=1, ascending_lat=ascending_lat)
    ((_, tile),) = _rows(p, bbox=BBOX)
    arr, transform, count = _pixels(tile)
    assert count == 1 and arr.shape == (1, 5, 5)
    np.testing.assert_array_equal(arr[0], NORTH_UP[1:6, 1:6])
    assert transform.c == pytest.approx(-93.95) and transform.f == pytest.approx(42.45)
    assert tile[8]["sourceWindow"] == "1,1,5,5"
    assert tile[2] is None and tile[4] is None  # materialized: no path, no window
    assert tile[7] == "EPSG:4326"


def test_banddim_and_bbox_give_a_small_stacked_tile(tmp_path):
    p = _write_cube(str(tmp_path / "c.nc"), ndays=4)
    ((_, tile),) = _rows(p, bbox=BBOX, bandDim="day")
    arr, _, count = _pixels(tile)
    assert count == 4 and arr.shape == (4, 5, 5)
    for d in range(4):
        np.testing.assert_array_equal(arr[d], d * 100 + NORTH_UP[1:6, 1:6])
    assert tile[8]["bandDim"] == "day"


def test_bbox_in_another_crs_gives_the_same_window(tmp_path):
    from pyproj import Transformer

    p = _write_cube(str(tmp_path / "c.nc"), ndays=1)
    w, s, e, n = (float(v) for v in BBOX.split(","))
    (x0, x1), (y0, y1) = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True
    ).transform([w, e], [s, n])
    ((_, tile),) = _rows(p, bbox=f"{x0},{y0},{x1},{y1}", bboxCrs="EPSG:3857")
    assert tile[8]["sourceWindow"] == "1,1,5,5"


def test_a_bbox_that_misses_the_grid_gives_no_row(tmp_path):
    p = _write_cube(str(tmp_path / "c.nc"), ndays=1)
    assert _rows(p, bbox="10,40,11,41") == []


@pytest.mark.parametrize("raw", ["1,2,3", "a,b,c,d", "3,0,1,1"])
def test_a_malformed_bbox_is_refused(raw):
    with pytest.raises(ValueError, match="minx,miny,maxx,maxy"):
        NetcdfRasterReader({"path": "/tmp/none", "bbox": raw})


def test_vector_mode_refuses_bbox():
    ds = NetcdfGbxDataSource(
        options={"path": "/tmp/none", "mode": "vector", "bbox": "0,0,1,1"}
    )
    with pytest.raises(ValueError, match="raster mode only"):
        ds.schema()


def test_gbx_rst_functions_take_the_tile(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    p = _write_cube(str(tmp_path / "c.nc"), ndays=4)
    gbx_manager.register_sources(spark, only=["netcdf"])
    gbx_manager.register_family(
        spark,
        ("gbx_rst_", "gbx_pmtiles_"),
        only=["gbx_rst_numbands", "gbx_rst_width", "gbx_rst_height"],
        level="L3",
    )
    shape = [
        "gbx_rst_numbands(tile) AS n",
        "gbx_rst_width(tile) AS w",
        "gbx_rst_height(tile) AS h",
    ]
    whole = (
        spark.read.format("netcdf_gbx")
        .option("variable", "wind_speed")
        .load(p)
        .selectExpr(*shape)
        .first()
    )
    assert (whole["n"], whole["w"], whole["h"]) == (1, 8, 6)
    cut = (
        spark.read.format("netcdf_gbx")
        .option("variable", "wind_speed")
        .option("bbox", BBOX)
        .option("bandDim", "day")
        .load(p)
        .selectExpr(*shape)
        .first()
    )
    assert (cut["n"], cut["w"], cut["h"]) == (4, 5, 5)
