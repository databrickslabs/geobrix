"""Executes the netcdf_gbx reader doc examples against synthesized NetCDF (Docker)."""

import sys
from pathlib import Path

import numpy as np
from netCDF4 import Dataset

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import netcdf_gbx_read_examples as ex  # noqa: E402


def _write_grid(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        ds.createVariable("t2m", "f4", ("lat", "lon"))[:] = np.arange(
            12, dtype="float32"
        ).reshape(3, 4)


def _write_points(path):
    with Dataset(path, "w") as ds:
        ds.createDimension("obs", 4)
        lat = ds.createVariable("latitude", "f8", ("obs",))
        lon = ds.createVariable("longitude", "f8", ("obs",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 50.1, 50.2, 50.3]
        lon[:] = [10.0, 10.1, 10.2, 10.3]
        ds.createVariable("methane_mixing_ratio_bias_corrected", "f4", ("obs",))[:] = (
            np.arange(4, dtype="float32")
        )
        ds.createVariable("qa_value", "i4", ("obs",))[:] = np.array(
            [1, 1, 0, 1], dtype="int32"
        )


def test_read_raster(spark, tmp_path):
    p = str(tmp_path / "era5.nc")
    _write_grid(p)
    assert ex.read_raster(spark, p).count() == 1


def test_read_vector(spark, tmp_path):
    p = str(tmp_path / "s5p.nc")
    _write_points(p)
    df = ex.read_vector(spark, p, "methane_mixing_ratio_bias_corrected,qa_value")
    assert df.count() == 4
