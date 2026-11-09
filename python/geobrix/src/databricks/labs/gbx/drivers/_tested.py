"""The GDAL drivers that GeoBrix's extended driver tests cover, with the GeoBrix reader or writer each is tested through.

Pure data (no GDAL, no PySpark), read by the ``tested`` and ``tested_with`` columns of ``gbx_drivers`` and by the docs
generator for ``docs/docs/gdal-drivers.mdx``. It describes the release, not the environment a call runs in.

A driver is listed when the suite in ``test/drivers_extended`` has a passing case for it on every GeoBrix path it
claims: it writes a sample with GDAL, reads it through the GeoBrix entry named here, and holds the result to what plain
GDAL reads from the same file (pixels, NoData, CRS and band count for a raster; geometry, attributes and CRS for a
vector). ``test_registry.py`` there derives this table from the cases and fails when the two differ, so a driver cannot
be listed without a test, and a case cannot pass without a listing.

Keys: ``read`` and ``write`` (the GeoBrix readers, writers and functions it was tested through, comma separated; write
is None when no GeoBrix writer was tested for the driver) and ``kinds`` (the tables of the docs page it is tested in:
a driver of both kinds can be tested as one only).
"""

from typing import Dict, Optional

TESTED_DRIVERS: Dict[str, Dict[str, object]] = {
    "AAIGrid": {
        "read": "raster_gbx",
        "write": "gbx_rst_asformat",
        "kinds": ("raster",),
    },
    "COG": {
        "read": "cog_gbx, raster_gbx",
        "write": "gbx_rst_asformat",
        "kinds": ("raster",),
    },
    "DXF": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "EHdr": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "ENVI": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "ESRI Shapefile": {
        "read": "shapefile_gbx, vector_gbx",
        "write": "shapefile_gbx",
        "kinds": ("vector",),
    },
    "FlatGeobuf": {"read": "vector_gbx", "write": "vector_gbx", "kinds": ("vector",)},
    "GeoJSON": {
        "read": "geojson_gbx, vector_gbx",
        "write": "geojson_gbx",
        "kinds": ("vector",),
    },
    "GeoJSONSeq": {
        "read": "geojsonl_gbx, vector_gbx",
        "write": "geojsonl_gbx",
        "kinds": ("vector",),
    },
    "GML": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "GPKG": {
        "read": "raster_gbx, gpkg_gbx, vector_gbx",
        "write": "gpkg_gbx",
        "kinds": ("raster", "vector"),
    },
    "GPX": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "GRIB": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "GSAG": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "GTiff": {
        "read": "gtiff_gbx, raster_gbx",
        "write": "gbx_rst_asformat",
        "kinds": ("raster",),
    },
    "HFA": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "JP2OpenJPEG": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "JPEG": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "JPEGXL": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "KML": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "MapInfo File": {"read": "vector_gbx", "write": "vector_gbx", "kinds": ("vector",)},
    "MBTiles": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "MEM": {"read": "gbx_rst_contour", "write": None, "kinds": ("vector",)},
    "netCDF": {"read": "netcdf_gbx, raster_gbx", "write": None, "kinds": ("raster",)},
    "NITF": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "OGR_VRT": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "OpenFileGDB": {
        "read": "file_gdb_gbx, vector_gbx",
        "write": "file_gdb_gbx",
        "kinds": ("vector",),
    },
    "OSM": {"read": "vector_gbx", "write": None, "kinds": ("vector",)},
    "PNG": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "RMF": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "SAGA": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "SQLite": {"read": "vector_gbx", "write": "vector_gbx", "kinds": ("vector",)},
    "VRT": {"read": "raster_gbx", "write": None, "kinds": ("raster",)},
    "WEBP": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
    "XYZ": {"read": "raster_gbx", "write": "gbx_rst_asformat", "kinds": ("raster",)},
}


def is_tested(driver: str) -> bool:
    """Whether the driver is covered by the extended driver tests of this release."""
    return driver in TESTED_DRIVERS


def tested_in(driver: str, kind: str) -> bool:
    """Whether the driver is tested as a ``kind`` ('raster' or 'vector') driver."""
    e = TESTED_DRIVERS.get(driver)
    return bool(e) and kind in e["kinds"]


def tested_with(driver: str) -> Optional[str]:
    """The GeoBrix entries the driver was tested through, such as ``raster_gbx (read), gbx_rst_asformat (write)``;
    None for a driver that is not tested."""
    e = TESTED_DRIVERS.get(driver)
    if not e:
        return None
    parts = [f"{e['read']} (read)"]
    if e["write"]:
        parts.append(f"{e['write']} (write)")
    return ", ".join(parts)
