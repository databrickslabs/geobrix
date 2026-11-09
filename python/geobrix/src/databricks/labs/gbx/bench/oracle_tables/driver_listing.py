"""Oracle table for ``gbx_drivers``: the GDAL drivers of the runtime, with GeoBrix's policy beside each.

Expected values are the facts of GDAL 3.13.3's driver metadata and the manifest's driver classification, written out
by hand: AAIGrid and XYZ are raster drivers that read and create-copy and that GeoBrix allows; the refused drivers are
not registered; a shapefile is a vector driver whose sidecar files are named in `requires`. The raster names are also
held to rasterio's own registry (see `oracle_checks.check_drivers`).
"""

from databricks.labs.gbx.bench.oracles import Case

D = "gbx_drivers"

_RASTER = {
    "AAIGrid": {
        "raster": True,
        "vector": False,
        "read": True,
        "write": True,
        "extensions": ["asc"],
        "allowed": True,
        "tile_bytes": True,
        "available": True,
        "requires": None,
        "help_url": "https://gdal.org/drivers/raster/aaigrid.html",
    },
    "XYZ": {
        "raster": True,
        "vector": False,
        "read": True,
        "write": True,
        "extensions": ["xyz"],
        "allowed": True,
        "tile_bytes": True,
        "available": True,
    },
    "GTiff": {
        "raster": True,
        "read": True,
        "write": True,
        "tile_bytes": True,
        "shipped": True,
    },
    # a driver that reads a header file beside the data: allowed, but not a tile's bytes
    "ENVI": {"allowed": True, "tile_bytes": False, "available": True},
    # write only
    "COG": {"read": False, "write": True},
}
_VECTOR = {
    "ESRI Shapefile": {
        "raster": False,
        "vector": True,
        "read": True,
        "write": True,
        "allowed": True,
        "tile_bytes": False,
        "available": True,
        "requires": "the .shx and .dbf files beside the .shp file, and the .prj file for the CRS",
    },
    "GeoJSON": {
        "vector": True,
        "extensions": ["json", "geojson"],
        "tile_bytes": False,
        "requires": None,
    },
    # opened by a connection string to a service the runtime does not provide
    "Elasticsearch": {
        "vector": True,
        "allowed": True,
        "available": False,
        "requires": "a service or program this runtime does not provide (it opens a connection string that starts with 'ES:')",
    },
}
# the drivers the policy refuses are deregistered, so they are not listed
_REFUSED = ["WMS", "GDALG", "HTTP", "WFS", "MRF"]

DRIVERS = (
    Case(
        "drivers_all",
        D,
        (None,),
        "drivers",
        {
            "kind": None,
            "present": {**_RASTER, **_VECTOR},
            "absent": _REFUSED,
        },
        ("session",),
    ),
    Case(
        "drivers_raster",
        D,
        ("raster",),
        "drivers",
        {"kind": "raster", "present": _RASTER, "absent": ["ESRI Shapefile", *_REFUSED]},
        ("session",),
    ),
    Case(
        "drivers_vector",
        D,
        ("vector",),
        "drivers",
        {"kind": "vector", "present": _VECTOR, "absent": ["AAIGrid", *_REFUSED]},
        ("session",),
    ),
    Case(
        "drivers_kind_any_case",
        D,
        ("Vector",),
        "drivers",
        {"kind": "vector", "present": _VECTOR},
        ("session",),
    ),
    Case("drivers_bad_kind", D, ("table",), "raises", "table", ("session",)),
)
