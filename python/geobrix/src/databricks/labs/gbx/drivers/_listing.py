"""The GDAL drivers this runtime can run: the rows of ``gbx_drivers``.

The rows come from GDAL's own registry in this process (the env wheel's ``osgeo`` on rasterio's libgdal), so they
change when a package is added or removed. Every policy column is read from ``_gdal_policy`` (the manifest's driver
classification), never from a list kept here. The ``shipped`` column compares a driver with the committed snapshot of
the env wheel's own build (``manifest/shipped_drivers.json``).
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import functools
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql.functions import udtf

from databricks.labs.gbx import _gdal_policy, _osgeo

KINDS = ("raster", "vector")
GDAL_DOCS = "https://gdal.org/"
SNAPSHOT = Path(__file__).resolve().parent.parent / "manifest" / "shipped_drivers.json"

# (name, SQL type) of each column, in order: the manifest row's `columns` repeat this.
COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("driver", "STRING"),
    ("description", "STRING"),
    ("raster", "BOOLEAN"),
    ("vector", "BOOLEAN"),
    ("read", "BOOLEAN"),
    ("write", "BOOLEAN"),
    ("extensions", "ARRAY<STRING>"),
    ("allowed", "BOOLEAN"),
    ("tile_bytes", "BOOLEAN"),
    ("available", "BOOLEAN"),
    ("requires", "STRING"),
    ("shipped", "BOOLEAN"),
    ("built_in", "STRING"),
    ("help_url", "STRING"),
)
# The columns the snapshot holds: every one but `shipped` (which is read from it).
SNAPSHOT_COLUMNS = tuple(n for n, _ in COLUMNS if n != "shipped")
_DDL = ", ".join(f"{n}: {t.lower()}" for n, t in COLUMNS)


# Formats GeoBrix's own readers and writers handle without the GDAL driver of the same name. One short note per driver,
# each held to the source: the Zarr package and its engines (zarr/_engine.py), xarray with netCDF4 (ds/_netcdf.py,
# ds/_write_netcdf.py), the pmtiles package (ds/_pmtiles_read.py, pmtiles/_agg_light.py), mapbox-vector-tile
# (pyvx/_mvt.py) and Pillow with exifread (ds/exif.py). A driver reached through rasterio or pyogrio is not listed.
# GDAL has no LAS driver, so the LAS and LAZ reader (laspy) has no row. `test_drivers.py` holds the names and the
# readers the notes mention to the build and the manifest.
BUILT_IN = {
    "Zarr": "The Zarr readers and writers use the zarr package, not this driver.",
    "netCDF": "`netcdf_gbx` reads with xarray and writes with netCDF4. The COG writer and raster functions that open a netCDF file use this driver.",
    "PMTiles": "`pmtiles_gbx` and `gbx_pmtiles_agg` use the pmtiles package, not this driver.",
    "MVT": "`gbx_st_asmvt` and `gbx_st_asmvt_pyramid` encode with mapbox-vector-tile, not this driver.",
    "JPEG": "`exif_gbx` reads image metadata with Pillow and exifread, not this driver.",
}


def check_kind(kind: Optional[str]) -> Optional[str]:
    """``kind`` as the filter it names ('raster' or 'vector', in any case), None for all; ValueError otherwise."""
    if kind is None:
        return None
    k = str(kind).strip().lower()
    if k not in KINDS:
        raise ValueError(
            f"gbx_drivers: kind {kind!r} is not 'raster' or 'vector'. "
            "Fix: pass 'raster', 'vector' or NULL for every driver."
        )
    return k


@functools.lru_cache(maxsize=1)
def shipped_names() -> frozenset:
    """The drivers of the env wheel's own build: the snapshot's names."""
    doc = json.loads(SNAPSHOT.read_text())
    return frozenset(r["driver"] for r in doc["drivers"])


def help_url(topic: Optional[str]) -> Optional[str]:
    """GDAL's ``DMD_HELPTOPIC`` as an absolute URL, None when the driver has none."""
    if not topic:
        return None
    return topic if topic.startswith("http") else GDAL_DOCS + topic.lstrip("/")


def _yes(md, key: str) -> bool:
    return str(md.get(key, "")).upper() == "YES"


def driver_row(drv, shipped: Optional[frozenset] = None) -> Dict[str, Any]:
    """One driver of GDAL's registry as the row of ``gbx_drivers`` (a dict by column)."""
    md = drv.GetMetadata() or {}
    name = drv.ShortName
    raster, vector = _yes(md, "DCAP_RASTER"), _yes(md, "DCAP_VECTOR")
    allowed, tile_bytes = _gdal_policy.driver_verdict(name, raster)
    requires, usable = _gdal_policy.driver_requires(name, md)
    return {
        "driver": name,
        "description": md.get("DMD_LONGNAME") or name,
        "raster": raster,
        "vector": vector,
        "read": _yes(md, "DCAP_OPEN"),
        "write": _yes(md, "DCAP_CREATE") or _yes(md, "DCAP_CREATECOPY"),
        "extensions": (md.get("DMD_EXTENSIONS") or "").split(),
        "allowed": allowed,
        "tile_bytes": tile_bytes,
        "available": allowed and usable,
        "requires": requires,
        "shipped": None if shipped is None else name in shipped,
        "built_in": BUILT_IN.get(name),
        "help_url": help_url(md.get("DMD_HELPTOPIC")),
    }


def driver_rows(
    gdal, kind: Optional[str] = None, shipped: Optional[frozenset] = None
) -> List[Dict[str, Any]]:
    """Every driver registered in ``gdal`` (the ``osgeo.gdal`` module), once, sorted by name; ``kind`` keeps the
    raster or the vector ones. A GDAL registers a driver once, but the rows are de-duplicated by name regardless.
    """
    kind = check_kind(kind)
    rows: Dict[str, Dict[str, Any]] = {}
    for i in range(gdal.GetDriverCount()):
        row = driver_row(gdal.GetDriver(i), shipped)
        if kind is None or row[kind]:
            rows.setdefault(row["driver"], row)
    return sorted(rows.values(), key=lambda r: (r["driver"].lower(), r["driver"]))


def list_drivers(kind: Optional[str] = None) -> List[Dict[str, Any]]:
    """``gbx_drivers(kind)`` on this process's GDAL: raises ``OsgeoUnavailable`` (with its Fix) where the env
    wheel's ``osgeo`` is not rasterio's GDAL."""
    check_kind(kind)
    gdal = _osgeo.bindings("gbx_drivers").gdal
    _gdal_policy.activate()  # the refused drivers are deregistered before the registry is read
    return driver_rows(gdal, kind, shipped_names())


def snapshot_document(gdal_version: str, rows: List[Dict[str, Any]]) -> str:
    """The text of ``manifest/shipped_drivers.json``: the GDAL release and every driver of ``rows`` by its snapshot
    columns, one driver per line. The test that refreshes the file (``test_drivers.py``) is the only writer.
    """
    lines = [
        "    " + json.dumps({c: r[c] for c in SNAPSHOT_COLUMNS}, ensure_ascii=False)
        for r in rows
    ]
    return (
        '{\n  "gdal": '
        + json.dumps(gdal_version)
        + ',\n  "drivers": [\n'
        + ",\n".join(lines)
        + "\n  ]\n}\n"
    )


@udtf(returnType=_DDL)
class _DriversUDTF:
    """Table function: one row per GDAL driver registered in the worker's process."""

    def eval(self, kind=None):
        for row in list_drivers(kind):
            yield tuple(row[n] for n, _ in COLUMNS)
