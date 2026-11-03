"""The light DataSources, and the one function that puts them into a session.

The manager registers them (``databricks.labs.gbx.register(spark)``, or ``manager.register_sources``); nothing here
registers on import. The format strings raster_gbx / gtiff_gbx / pmtiles_gbx / lidar_gbx / zarr_gbx do not collide
with the Scala-registered gdal / gtiff_gdal, so both tiers coexist.
"""

from __future__ import annotations

import difflib
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.exif import ExifGbxDataSource
from databricks.labs.gbx.ds.file import FileGbxDataSource
from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.lidar import LidarGbxDataSource
from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
from databricks.labs.gbx.ds.pmtiles import PMTilesGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.ds.vector import (
    FileGdbGbxDataSource,
    GeoJSONGbxDataSource,
    GeoJSONLGbxDataSource,
    GpkgGbxDataSource,
    ShapefileGbxDataSource,
    VectorGbxDataSource,
)
from databricks.labs.gbx.zarr.datasource import ZarrGbxDataSource

_SOURCES = (
    RasterGbxDataSource,
    GTiffGbxDataSource,
    NetcdfGbxDataSource,
    PMTilesGbxDataSource,
    FileGbxDataSource,
    CogGbxDataSource,
    VectorGbxDataSource,
    ShapefileGbxDataSource,
    GeoJSONGbxDataSource,
    GeoJSONLGbxDataSource,
    GpkgGbxDataSource,
    FileGdbGbxDataSource,
    LidarGbxDataSource,
    ExifGbxDataSource,
    ZarrGbxDataSource,
)


def _format_name(name: str) -> str:
    """``raster``, ``RASTER`` and ``raster_gbx`` all name ``raster_gbx``."""
    n = name.strip().lower()
    return n if n.endswith("_gbx") else f"{n}_gbx"


def _install(spark: SparkSession, only: Optional[Iterable[str]] = None) -> List[str]:
    """Register the light DataSources on ``spark``: every one, or the formats in ``only`` (the format name with or
    without ``_gbx``, case-insensitively; ``[]`` registers nothing). An unrecognized format raises ``ValueError``
    that lists the close matches. Returns the registered format names."""
    by_name = {src.name(): src for src in _SOURCES}
    if only is None:
        selected = list(_SOURCES)
    else:
        requested = [(orig, _format_name(orig)) for orig in only]
        unknown = [(orig, norm) for orig, norm in requested if norm not in by_name]
        if unknown:
            lines = []
            for orig, norm in unknown:
                near = difflib.get_close_matches(norm, by_name, n=3)
                lines.append(
                    f"  {orig!r}"
                    + (f" -> did you mean: {', '.join(near)}?" if near else "")
                )
            raise ValueError(
                "register(only=...) got unrecognized data source name(s):\n"
                + "\n".join(lines)
                + "\nPass a registerable format name (or its short form)."
            )
        selected = [by_name[n] for n in dict.fromkeys(n for _o, n in requested)]
    from databricks.labs.gbx import _gdal_policy

    # each reader carries the driver's remote-path switch into its planning (the workers never read it)
    allowed = _gdal_policy.driver_switch()
    for source in selected:
        spark.dataSource.register(_gdal_policy.carrying_source(source, allowed))
    return [s.name() for s in selected]
