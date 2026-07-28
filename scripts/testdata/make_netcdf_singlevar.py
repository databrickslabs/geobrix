# scripts/testdata/make_netcdf_singlevar.py
#
# Generates a tiny single-variable CF lat/lon grid netCDF fixture. GDAL's netCDF
# driver only exposes a SUBDATASETS metadata domain when a file has >1 data
# variable; a single-variable file opens as a DIRECT raster with NO subdatasets.
# NetCDF_Batch used to only enumerate subdatasets, so such a file yielded zero
# partitions (the bug this fixture guards against). Run once and commit the .nc —
# it is only needed at test time as the .nc, not the generator.
#
#   python scripts/testdata/make_netcdf_singlevar.py
#
# Confirm it has NO subdatasets:
#   gdalinfo src/test/resources/binary/netcdf-singlevar/tas_singlevar.nc | grep -i subdataset
# (should print nothing)
import os

import numpy as np
from netCDF4 import Dataset

out_path = "src/test/resources/binary/netcdf-singlevar/tas_singlevar.nc"
os.makedirs(os.path.dirname(out_path), exist_ok=True)

ny, nx = 6, 8
lats = np.linspace(-2.5, 2.5, ny)   # regular 1-degree-ish grid centered on equator
lons = np.linspace(10.0, 17.0, nx)

with Dataset(out_path, "w") as ds:
    ds.Conventions = "CF-1.7"
    ds.createDimension("lat", ny)
    ds.createDimension("lon", nx)

    lat = ds.createVariable("lat", "f8", ("lat",))
    lat.standard_name = "latitude"
    lat.units = "degrees_north"
    lat[:] = lats

    lon = ds.createVariable("lon", "f8", ("lon",))
    lon.standard_name = "longitude"
    lon.units = "degrees_east"
    lon[:] = lons

    # A SINGLE data variable => GDAL opens the file as a direct raster with no
    # SUBDATASETS domain. This is the whole point of the fixture.
    tas = ds.createVariable("tas", "f4", ("lat", "lon"))
    tas.standard_name = "air_temperature"
    tas.units = "K"
    tas.grid_mapping = "crs"
    yy, xx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    tas[:] = (280.0 + yy + xx).astype("float32")

    # WGS84 CRS so GetProjectionRef is non-empty (georeferenced grid test passes).
    crs = ds.createVariable("crs", "i4")
    crs.grid_mapping_name = "latitude_longitude"
    crs.longitude_of_prime_meridian = 0.0
    crs.semi_major_axis = 6378137.0
    crs.inverse_flattening = 298.257223563
    crs.spatial_ref = (
        'GEOGCS["WGS 84",DATUM["WGS_1984",'
        'SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
    )

print(f"wrote {out_path}")
