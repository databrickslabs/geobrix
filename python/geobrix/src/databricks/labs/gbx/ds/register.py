"""Register the light DataSources with a Spark session.

Mirrors pyrx.functions.register: call once, consciously. The format strings
raster_gbx / gtiff_gbx / pmtiles_gbx do not collide with the Scala-registered
gdal / gtiff_gdal, so both tiers coexist.
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import SparkSession

from databricks.labs.gbx import _register
from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.file import FileGbxDataSource
from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
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
)


def register(
    spark: Optional[SparkSession] = None, only: Optional[List[str]] = None
) -> None:
    """Register the light DataSources (raster_gbx, gtiff_gbx, pmtiles_gbx, and the
    vector readers/writers). Uses the active session if not given.

    Args:
        spark: Spark session (active session if not provided).
        only: Optional list of format names to register (instead of all 9).
            Accepts the format name with or without the ``_gbx`` suffix
            (``raster`` or ``raster_gbx``), case-insensitively. ``None`` registers
            everything; ``[]`` registers nothing. An unrecognized format raises
            ``ValueError``.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    by_name = {src.name(): src for src in _SOURCES}
    if only is None:
        selected = list(_SOURCES)
    else:
        wanted = _register.resolve_only(
            only, by_name.keys(), normalizer=_register.normalize_datasource_name
        )
        selected = [by_name[n] for n in wanted]
    for source in selected:
        spark.dataSource.register(source)


def _try_register_on_import() -> None:
    """Best-effort register if a session is already live (no-op otherwise)."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            register(spark)
    except Exception:
        pass
