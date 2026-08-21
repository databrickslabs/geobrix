"""Function-layer FILE read for vector sources (light tier).

Parity with raster's grouped_tile_map: a driver, session-ful entry that
enumerates members via the session-free file_gbx core, injects a _file_ref
column on the driver when FILE is available, and reads each member with pyogrio
inside mapInPandas — resolving the source to a local path via the FileRef's
as_local_file() (FILE) or to_local_path() (FUSE). This is the FILE-tier vector
read the session-less DataSource reader cannot do.

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import logging
from typing import Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from databricks.labs.gbx.ds.file_gbx import (
    file_access_tier,
    file_ref_arg,
    file_supported,
    list_local_files,
    resolve_access,
    to_local_path,
)

_LOG = logging.getLogger(__name__)

_EXT_FOR_DRIVER = {
    "GeoJSON": (".geojson", ".json"),
    "GeoJSONSeq": (".geojsonl", ".geojsons"),
    "ESRI Shapefile": (".shp", ".shz", ".zip"),
    "GPKG": (".gpkg",),
    "OpenFileGDB": (".gdb", ".gdb.zip", ".zip"),
}


def _inject_file_ref(df: DataFrame, col) -> DataFrame:
    """Add the ``_file_ref`` column to *df*; extracted for testability."""
    return df.withColumn("_file_ref", col)


def vector_file_read(
    spark: SparkSession,
    path: str,
    *,
    driver: str,
    access: str = "auto",
    as_wkb: bool = True,
    layer: Union[int, str] = 0,
) -> DataFrame:
    """Read a directory of vector files at the function layer with FILE leverage.

    ``access`` gating happens HERE (session present): explicit "managed"/"external"
    on a FUSE-only runtime raises the actionable error via ``resolve_access``.
    """
    if access == "managed":
        # Aligned with gbx_file_read: MANAGED is valid ONLY for a MANAGED
        # FILE-column TABLE source. vector_file_read reads a Volume path/directory
        # of vector files (a location source), which can never yield a MANAGED
        # reference — that is minted on WRITE via vector_file_write. Raise the
        # same-shape error and point at the table-capable read-backs.
        raise ValueError(
            "access='managed' is only valid for a MANAGED FILE-column table source. "
            "vector_file_read reads a Volume path/directory of vector files (a "
            "location source), which cannot yield a MANAGED FILE reference — that is "
            "minted on write via vector_file_write. "
            "To read back a MANAGED vector FILE table, use read_file_table (or "
            "gbx_file_read(access='managed')) on that table. Use access='external' "
            "for FILE EXTERNAL references to existing Volume files, or access='auto' "
            "for graceful FILE/FUSE fallback."
        )
    resolve_access(access, tier=file_access_tier(spark), spark=spark)

    exts = _EXT_FOR_DRIVER.get(driver) or None
    members = list_local_files(path, recursive=True, extensions=exts)
    members_df = spark.createDataFrame(
        [(m,) for m in members], StructType([StructField("source", StringType())])
    )

    # Driver-side FILE-ref injection (parity with grouped_tile_map): file_ref_arg
    # expects a column with a ["path"] subscript, so wrap the source string in a
    # 1-field struct named "path".
    if file_supported(spark):
        members_df = _inject_file_ref(
            members_df,
            file_ref_arg(F.struct(F.col("source").alias("path")), spark=spark),
        )

    geom_type = BinaryType() if as_wkb else StringType()
    out_schema = StructType(
        [StructField("source", StringType()), StructField("geometry", geom_type)]
    )
    _layer = layer
    _as_wkb = as_wkb

    def _map(pdf_iter):
        import pandas as pd
        import pyogrio

        for pdf in pdf_iter:
            has_fr = "_file_ref" in pdf.columns
            srcs: list = []
            geoms: list = []
            for _, row in pdf.iterrows():
                src = row["source"]
                local = None
                if has_fr and row["_file_ref"] is not None:
                    try:
                        local = row["_file_ref"].as_local_file()
                    except Exception:
                        _LOG.debug(
                            "FILE fast-path unavailable for %s; falling back to FUSE",
                            src,
                            exc_info=True,
                        )
                        local = None
                if local is None:
                    local = to_local_path(src)
                # Use read_arrow (pyogrio, no external geometry framework needed). The geometry column is
                # returned as raw WKB bytes; meta["geometry_name"] names it.
                # GeoJSON drivers return geometry_name="" and use "wkb_geometry".
                meta, arrow_table = pyogrio.read_arrow(local, layer=_layer)
                geom_col = meta.get("geometry_name") or "wkb_geometry"
                geom_array = arrow_table.column(geom_col)
                for wkb_scalar in geom_array:
                    wkb_bytes = wkb_scalar.as_py()
                    srcs.append(src)
                    if wkb_bytes is None:
                        geoms.append(None)
                    elif _as_wkb:
                        geoms.append(bytearray(wkb_bytes))
                    else:
                        import shapely as _shapely

                        geoms.append(_shapely.from_wkb(bytes(wkb_bytes)).wkt)
            if srcs:
                yield pd.DataFrame({"source": srcs, "geometry": geoms})
            else:
                yield pd.DataFrame(
                    {
                        "source": pd.Series([], dtype="object"),
                        "geometry": pd.Series([], dtype="object"),
                    }
                )

    return members_df.mapInPandas(_map, schema=out_schema)
