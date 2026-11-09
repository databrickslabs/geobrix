"""The formats GeoBrix's own readers and writers handle without a GDAL driver, one entry per format.

Pure data (no GDAL, no PySpark), read by the ``built_in`` column of ``gbx_drivers`` and by the docs generator for
``docs/docs/gdal-drivers.mdx``. Each entry is held to the source: the Zarr package and its engines (zarr/_engine.py),
xarray with netCDF4 (ds/_netcdf.py, ds/_write_netcdf.py), the pmtiles package (ds/_pmtiles_read.py,
pmtiles/_agg_light.py), mapbox-vector-tile (pyvx/_mvt.py), Pillow with exifread (ds/exif.py) and laspy (ds/lidar.py).
A reader that goes through rasterio or pyogrio uses its GDAL driver and has no entry. ``test_drivers.py`` holds the
driver names to the build and every reader or function name to the manifest.

Keys: ``anchor`` (the row's anchor on the docs page), ``format``, ``driver`` (the GDAL driver of the same format, or
None), ``note`` (the short text of the ``built_in`` column; only where there is a driver), ``uses`` and ``where``.
"""

BUILT_IN_FORMATS = (
    {
        "anchor": "builtin-zarr",
        "format": "Zarr",
        "driver": "Zarr",
        "note": "The Zarr readers and writers use the zarr package, not this driver.",
        "uses": "The zarr package, with tensorstore or zarrs as optional engines.",
        "where": "`zarr_gbx` (reader and writer), `gbx_zarr_read`, `gbx_zarr_write`",
    },
    {
        "anchor": "builtin-netcdf",
        "format": "netCDF",
        "driver": "netCDF",
        "note": "`netcdf_gbx` reads with xarray and writes with netCDF4. The COG writer and raster functions that open a netCDF file use this driver.",
        "uses": "xarray with the netCDF4 engine to read, and netCDF4 to write. The COG writer and raster functions that open a netCDF file still use the GDAL driver.",
        "where": "`netcdf_gbx` (reader and writer)",
    },
    {
        "anchor": "builtin-pmtiles",
        "format": "PMTiles",
        "driver": "PMTiles",
        "note": "`pmtiles_gbx` and `gbx_pmtiles_agg` use the pmtiles package, not this driver.",
        "uses": "The pmtiles package.",
        "where": "`pmtiles_gbx` (reader and writer), `gbx_pmtiles_agg`, `pmtiles_info`",
    },
    {
        "anchor": "builtin-mvt",
        "format": "Mapbox Vector Tiles",
        "driver": "MVT",
        "note": "`gbx_st_asmvt` and `gbx_st_asmvt_pyramid` encode with mapbox-vector-tile, not this driver.",
        "uses": "The mapbox-vector-tile package.",
        "where": "`gbx_st_asmvt`, `gbx_st_asmvt_pyramid`. The PMTiles writer stores the encoded tiles as they are.",
    },
    {
        "anchor": "builtin-exif",
        "format": "EXIF and GPS image metadata",
        "driver": "JPEG",
        "note": "`exif_gbx` reads image metadata with Pillow and exifread, not this driver.",
        "uses": "Pillow and exifread. The JPEG driver reads pixels only.",
        "where": "`exif_gbx` (reader)",
    },
    {
        "anchor": "builtin-lidar",
        "format": "LiDAR point clouds (LAS and LAZ)",
        "driver": None,
        "note": None,
        "uses": "The laspy package.",
        "where": "`lidar_gbx` (reader and writer)",
    },
)

# driver name -> the note of the `built_in` column
BUILT_IN = {f["driver"]: f["note"] for f in BUILT_IN_FORMATS if f["driver"]}
