# scripts/testdata/make_netcdf_dsg.py
#
# Generates a tiny CF Discrete Sampling Geometry (DSG) "point" netCDF fixture that
# the OGR netCDF driver can surface as native point features. Run once and commit
# the resulting .nc — it is not required at test time, only the .nc is.
#
#   python scripts/testdata/make_netcdf_dsg.py
#
# The existing netcdf-coral/CMIP5/ECMWF fixtures are grids; the OGR netCDF driver
# reads DSG features, not grids, so a grid .nc yields zero features. This produces a
# CF-DSG point file the driver keys on via the featureType="point" global attribute.
import os

import numpy as np
from netCDF4 import Dataset

out_path = "src/test/resources/binary/netcdf-dsg/points.nc"
os.makedirs(os.path.dirname(out_path), exist_ok=True)

with Dataset(out_path, "w") as ds:
    ds.featureType = "point"          # CF-DSG marker the OGR driver keys on
    ds.createDimension("obs", 5)
    lat = ds.createVariable("latitude", "f8", ("obs",)); lat.standard_name = "latitude"; lat.units = "degrees_north"
    lon = ds.createVariable("longitude", "f8", ("obs",)); lon.standard_name = "longitude"; lon.units = "degrees_east"
    val = ds.createVariable("ch4", "f4", ("obs",)); val.coordinates = "latitude longitude"
    lat[:] = [50.0, 50.1, 50.2, 50.3, 50.4]
    lon[:] = [10.0, 10.1, 10.2, 10.3, 10.4]
    val[:] = np.arange(5, dtype="float32")

print(f"wrote {out_path}")
