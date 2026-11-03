# scripts/testdata/make_netcdf_dsg_types.py
#
# Generates tiny CF-1.8 Discrete Sampling Geometry (DSG) netCDF fixtures, one per feature type the vector readers
# surface, beside the "point" fixture (netcdf-dsg/points.nc) made by make_netcdf_dsg.py. Run once and commit the .nc
# files: only the files are needed at test time. No data is downloaded.
#
#   python scripts/testdata/make_netcdf_dsg_types.py
#
# The layouts are the ones the GDAL netCDF driver (netcdf_ogr) reads, found by trying them: it gives one point feature
# per observation. It does not read a contiguous ragged array (a `row_size` variable with `sample_dimension`; it refuses
# the file: "not all share the same primary dimension"), nor an indexed trajectory; netcdf_gbx reads the first, so
# profiles_contiguous.nc holds the profiles.nc data in that form (no oracle: only netcdf_gbx reads it).
#
#   profiles.nc      featureType="profile": 3 profiles (lat/lon/time on the profile dimension), 9 observations (3 per
#                    profile), the indexed ragged array representation: `profile_index` on the observation dimension
#                    (instance_dimension="profile") says which profile an observation belongs to; z and temp are on the
#                    observation dimension. netcdf_ogr gives 9 3D points (z is the point's Z), fields profile, temp, time.
#   trajectories.nc  featureType="trajectory": 2 trajectories, 6 observations (4 + 2); every variable is on the
#                    observation dimension (the trajectory id too). netcdf_ogr gives 6 2D points, fields trajectory,
#                    temp, time.
import os

import numpy as np
from netCDF4 import Dataset

ROOT = "src/test/resources/binary"
PROFILES = f"{ROOT}/netcdf-dsg-profile/profiles.nc"
TRAJECTORIES = f"{ROOT}/netcdf-dsg-trajectory/trajectories.nc"
CONTIGUOUS = f"{ROOT}/netcdf-dsg-ragged/profiles_contiguous.nc"
for p in (PROFILES, TRAJECTORIES, CONTIGUOUS):
    os.makedirs(os.path.dirname(p), exist_ok=True)


def coord(ds, name, dims, units, standard_name, values, axis=None):
    v = ds.createVariable(name, "f8", dims)
    v.standard_name = standard_name
    v.units = units
    if axis:
        v.axis = axis
    v[:] = values
    return v


with Dataset(PROFILES, "w") as ds:
    ds.Conventions = "CF-1.8"
    ds.featureType = "profile"
    ds.createDimension("profile", 3)
    ds.createDimension("obs", 9)
    pid = ds.createVariable("profile", "i4", ("profile",))
    pid.cf_role = "profile_id"
    pid[:] = [101, 102, 103]
    coord(ds, "latitude", ("profile",), "degrees_north", "latitude", [50.0, 51.0, 52.0])
    coord(ds, "longitude", ("profile",), "degrees_east", "longitude", [10.0, 11.0, 12.0])
    coord(ds, "time", ("profile",), "days since 2020-01-01", "time", [0.0, 1.0, 2.0])
    ix = ds.createVariable("profile_index", "i4", ("obs",))
    ix.long_name = "which profile this observation belongs to"
    ix.instance_dimension = "profile"
    ix[:] = [0, 0, 0, 1, 1, 1, 2, 2, 2]
    z = coord(ds, "z", ("obs",), "m", "altitude", [0.0, 10.0, 20.0] * 3)
    z.positive = "up"
    temp = ds.createVariable("temp", "f4", ("obs",))
    temp.standard_name = "sea_water_temperature"
    temp.units = "degree_C"
    temp.coordinates = "time latitude longitude z"
    temp[:] = np.arange(9, dtype="float32") + 0.5

with Dataset(TRAJECTORIES, "w") as ds:
    ds.Conventions = "CF-1.8"
    ds.featureType = "trajectory"
    ds.createDimension("obs", 6)
    tid = ds.createVariable("trajectory", "i4", ("obs",))
    tid.cf_role = "trajectory_id"
    tid[:] = [7, 7, 7, 7, 8, 8]
    coord(ds, "latitude", ("obs",), "degrees_north", "latitude", [50.0, 50.1, 50.2, 50.3, 40.0, 40.1])
    coord(ds, "longitude", ("obs",), "degrees_east", "longitude", [10.0, 10.1, 10.2, 10.3, 20.0, 20.1])
    coord(ds, "time", ("obs",), "days since 2020-01-01", "time", [0.0, 0.1, 0.2, 0.3, 1.0, 1.1])
    temp = ds.createVariable("temp", "f4", ("obs",))
    temp.standard_name = "air_temperature"
    temp.units = "degree_C"
    temp.coordinates = "time latitude longitude"
    temp[:] = np.arange(6, dtype="float32") + 0.25

with Dataset(CONTIGUOUS, "w") as ds:
    ds.Conventions = "CF-1.8"
    ds.featureType = "profile"
    ds.createDimension("profile", 3)
    ds.createDimension("obs", 9)
    pid = ds.createVariable("profile", "i4", ("profile",))
    pid.cf_role = "profile_id"
    pid[:] = [101, 102, 103]
    coord(ds, "latitude", ("profile",), "degrees_north", "latitude", [50.0, 51.0, 52.0])
    coord(ds, "longitude", ("profile",), "degrees_east", "longitude", [10.0, 11.0, 12.0])
    coord(ds, "time", ("profile",), "days since 2020-01-01", "time", [0.0, 1.0, 2.0])
    rs = ds.createVariable("row_size", "i4", ("profile",))
    rs.long_name = "number of observations for this profile"
    rs.sample_dimension = "obs"
    rs[:] = [3, 3, 3]
    z = coord(ds, "z", ("obs",), "m", "altitude", [0.0, 10.0, 20.0] * 3)
    z.positive = "up"
    temp = ds.createVariable("temp", "f4", ("obs",))
    temp.standard_name = "sea_water_temperature"
    temp.units = "degree_C"
    temp.coordinates = "time latitude longitude z"
    temp[:] = np.arange(9, dtype="float32") + 0.5

print(f"wrote {PROFILES}, {TRAJECTORIES} and {CONTIGUOUS}")
