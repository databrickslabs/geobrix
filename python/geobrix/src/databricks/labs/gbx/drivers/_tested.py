"""The GDAL drivers that GeoBrix's extended driver tests cover, with the GeoBrix reader or writer each is tested through.

Pure data (no GDAL, no PySpark), read by the ``tested`` and ``tested_with`` columns of ``gbx_drivers`` and by the docs
generator for ``docs/docs/gdal-drivers.mdx``. It describes the release, not the environment a call runs in.

A driver is listed for a kind ('raster' or 'vector') when the suite in ``test/drivers_extended`` has a passing case for
it on every GeoBrix path it claims: it writes a sample with GDAL, reads it through the GeoBrix entry named here, and
holds the result to what plain GDAL reads from the same file (pixels, NoData, CRS and band count for a raster; geometry,
attributes and CRS for a vector). ``test_registry.py`` there derives this table from the cases and fails when the two
differ, and ``test_zz_ran.py`` fails when a listed case was skipped or expected to fail, so a driver cannot be listed
without a passing test, and a case cannot pass without a listing. A driver that GeoBrix has its own reader and writer
for (netCDF, Zarr, PMTiles, MVT) is not listed: its tests run, but GeoBrix does not standardise the GDAL driver.

Each driver maps its tested kinds to ``read`` (the GeoBrix readers and functions it was tested through, comma
separated) and ``write`` (the writers and functions; None when no GeoBrix writer was tested for the driver).
"""

from typing import Dict, Optional

TESTED_DRIVERS: Dict[str, Dict[str, Dict[str, Optional[str]]]] = {
    "AAIGrid": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "BAG": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "CAD": {
        "vector": {"read": "vector_gbx", "write": None},
    },
    "COG": {
        "raster": {"read": "cog_gbx, raster_gbx", "write": "gbx_rst_asformat"},
    },
    "DXF": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "EHdr": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "ENVI": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "ESRI Shapefile": {
        "vector": {"read": "shapefile_gbx, vector_gbx", "write": "shapefile_gbx"},
    },
    "FlatGeobuf": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "GML": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "GPKG": {
        "raster": {"read": "raster_gbx", "write": None},
        "vector": {"read": "gpkg_gbx, vector_gbx", "write": "gpkg_gbx"},
    },
    "GPX": {
        "vector": {"read": "vector_gbx", "write": None},
    },
    "GRIB": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "GSAG": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "GTX": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "GTiff": {
        "raster": {"read": "gtiff_gbx, raster_gbx", "write": "gbx_rst_asformat"},
    },
    "GeoJSON": {
        "vector": {"read": "geojson_gbx, vector_gbx", "write": "geojson_gbx"},
    },
    "GeoJSONSeq": {
        "vector": {"read": "geojsonl_gbx, vector_gbx", "write": "geojsonl_gbx"},
    },
    "HFA": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "JP2OpenJPEG": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "JPEG": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "JPEGXL": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "KML": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "MBTiles": {
        "raster": {"read": "raster_gbx", "write": None},
        "vector": {"read": "vector_gbx", "write": None},
    },
    "MapInfo File": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "NITF": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "OGR_VRT": {
        "vector": {"read": "vector_gbx", "write": None},
    },
    "OSM": {
        "vector": {"read": "vector_gbx", "write": None},
    },
    "OpenFileGDB": {
        "vector": {"read": "file_gdb_gbx, vector_gbx", "write": "file_gdb_gbx"},
    },
    "PNG": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "RMF": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "SAGA": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "SQLite": {
        "vector": {"read": "vector_gbx", "write": "vector_gbx"},
    },
    "SRTMHGT": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "VRT": {
        "raster": {"read": "raster_gbx", "write": None},
    },
    "WEBP": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
    "XYZ": {
        "raster": {"read": "raster_gbx", "write": "gbx_rst_asformat"},
    },
}


def is_tested(driver: str, kind: Optional[str] = None) -> bool:
    """Whether the driver is covered by the extended driver tests of this release, as a ``kind`` driver or (kind None)
    as any kind."""
    e = TESTED_DRIVERS.get(driver)
    if not e:
        return False
    return kind is None or kind in e


def tested_in(driver: str, kind: str) -> bool:
    """Whether the driver is tested as a ``kind`` ('raster' or 'vector') driver."""
    return is_tested(driver, kind)


def tested_kinds(driver: str) -> tuple:
    """The kinds the driver is tested as, raster first."""
    e = TESTED_DRIVERS.get(driver) or {}
    return tuple(k for k in ("raster", "vector") if k in e)


def _how(entry: Dict[str, Optional[str]]) -> str:
    parts = [f"{entry['read']} (read)"]
    if entry["write"]:
        parts.append(f"{entry['write']} (write)")
    return ", ".join(parts)


def tested_with(driver: str, kind: Optional[str] = None) -> Optional[str]:
    """The GeoBrix entries the driver was tested through, such as ``raster_gbx (read), gbx_rst_asformat (write)``;
    None for a driver (or a kind) that is not tested. With no ``kind``, a driver tested as both kinds names each:
    ``raster: ...; vector: ...``."""
    e = TESTED_DRIVERS.get(driver) or {}
    if kind is not None:
        return _how(e[kind]) if kind in e else None
    kinds = tested_kinds(driver)
    if not kinds:
        return None
    if len(kinds) == 1:
        return _how(e[kinds[0]])
    return "; ".join(f"{k}: {_how(e[k])}" for k in kinds)
