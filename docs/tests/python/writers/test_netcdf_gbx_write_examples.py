"""Executes the netcdf_gbx writer doc examples against synthesized NetCDF (Docker).

Synthesizes tiny CF grid / CF-DSG point .nc inputs with netCDF4.Dataset in a tmp
dir (no external sample data), imports the examples module, and runs each verifier
against the real lightweight writer. Run via gbx:test:python-docs.
"""

import sys
from pathlib import Path

import numpy as np
from netCDF4 import Dataset

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import netcdf_gbx_write_examples as ex  # noqa: E402

_VARS = "methane_mixing_ratio_bias_corrected,qa_value"


def _write_grid(path):
    """A small regular CF lat/lon grid with one f4 data variable."""
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
    """A small CF Discrete Sampling Geometry point layer (obs dimension)."""
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


def test_write_raster(spark, tmp_path):
    in_nc = str(tmp_path / "era5.nc")
    _write_grid(in_nc)
    out_dir = str(tmp_path / "out-raster")
    ex.write_raster(spark, in_nc, out_dir, "t2m")


def test_write_vector(spark, tmp_path):
    in_nc = str(tmp_path / "s5p.nc")
    _write_points(in_nc)
    out_dir = str(tmp_path / "out-vector")
    ex.write_vector(spark, in_nc, out_dir, _VARS)


def test_write_singlefile(spark, tmp_path):
    in_nc = str(tmp_path / "s5p.nc")
    _write_points(in_nc)
    out_dir = str(tmp_path / "out-single")
    ex.write_singlefile(spark, in_nc, out_dir, _VARS)


def test_write_merge(spark, tmp_path):
    in_nc = str(tmp_path / "s5p.nc")
    _write_points(in_nc)
    out_dir = str(tmp_path / "out-merge")
    ex.write_merge(spark, in_nc, out_dir, _VARS)
