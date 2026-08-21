"""Function-layer FILE read for vector sources (light tier).

Parity with raster's grouped_tile_map: a driver, session-ful entry that
enumerates members via the session-free file_gbx core, injects a _file_ref
column on the driver when FILE is available, and reads each member with pyogrio
inside mapInPandas — resolving the source to a local path via the FileRef's
as_local_file() (FILE) or to_local_path() (FUSE). This is the FILE-tier vector
read the session-less DataSource reader cannot do.

Two source kinds (resolved by ``source_type``):

- **path**: enumerate files under a Volume path/directory, optionally inject FILE
  refs, decode each via pyogrio inside ``mapInPandas``.
- **table**: resolve a FILE-column Delta table via :func:`resolve_file_table`
  (shared core from ``ds/file_gbx.py``) → ordered source paths → decode each
  via the same pyogrio ``mapInPandas`` path.  No decode duplication (DRY).

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import logging
import os as _os
import re as _re
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
    resolve_file_table,
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


def _classify_vector_source(source: str, source_type: str) -> str:
    """Classify *source* as ``'table'`` or ``'path'``.

    ``source_type='auto'`` heuristic:

    - Starts with ``/`` or matches a URI scheme → **path**.
    - Ends with a known vector file extension (``_EXT_FOR_DRIVER`` values) → **path**.
    - Exists on the local filesystem → **path**.
    - Otherwise (dotted table-name components, no known extension, non-existent) → **table**.

    Explicit ``source_type='table'`` or ``'path'`` bypasses the heuristic.
    Mirrors the ``source_type`` kwarg of :func:`gbx_file_read`.

    Note: only *known* vector extensions trigger the extension check because
    ``os.path.splitext("schema.table")`` returns ``(".table",)`` — the table-name
    dot separator looks like an extension to the stdlib, so a generic extension
    check would misclassify valid table names like ``schema.roads``.
    """
    if source_type == "table":
        return "table"
    if source_type == "path":
        return "path"
    # auto: classify by heuristic
    if source.startswith("/") or _re.match(r"^[A-Za-z][A-Za-z0-9+.\-]*://?", source):
        return "path"
    # Only treat known vector file extensions as path indicators.
    _, ext = _os.path.splitext(source)
    _known_exts = {e for exts in _EXT_FOR_DRIVER.values() for e in exts}
    if ext.lower() in _known_exts:
        return "path"
    if _os.path.exists(source):
        return "path"
    # Dotted table-name components, no known extension, non-existent → table
    return "table"


def _make_vector_decode_fn(as_wkb: bool, layer):
    """Return a ``mapInPandas`` decode function for pyogrio vector files.

    The returned generator opens each source path via ``pyogrio.read_arrow``
    and yields a pandas DataFrame with ``{source, geometry}`` rows.

    Works for both the **path mode** (may include a ``_file_ref`` column for
    FILE fast-path) and the **table mode** (plain ``source`` column only —
    falls back to FUSE via ``to_local_path``).  Extracting the decode here
    keeps both modes DRY: neither duplicates the ``read_arrow`` / WKB / WKT
    logic.
    """
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

    return _map


def vector_file_read(
    spark: SparkSession,
    path: str,
    *,
    driver: str = "",
    access: str = "auto",
    as_wkb: bool = True,
    layer: Union[int, str] = 0,
    source_type: str = "auto",
    skip_ordering: bool = False,
) -> DataFrame:
    """Read a directory of vector files OR a FILE-column table at the function layer.

    Two source kinds, selected by ``source_type``:

    - **path** (``source_type='path'`` or ``source_type='auto'`` + path string):
      enumerate files under the Volume path/directory, optionally inject FILE
      refs, decode each via ``pyogrio.read_arrow`` inside ``mapInPandas``.
      ``driver`` is used to filter listed files by extension; ``access`` gates
      FILE vs FUSE.

    - **table** (``source_type='table'`` or ``source_type='auto'`` + dotted/
      extension-less/non-existent string): resolve the FILE-column Delta table
      via :func:`resolve_file_table` (shared core), then decode each resolved
      source path via the same ``mapInPandas`` pyogrio decode.  ``driver`` and
      ``access`` are ignored in this mode.  One ``.gpkg`` = one row = one FILE
      ref → decoding a path yields all its features.

    ``source_type`` auto-detection:

    - Starts with ``/`` or has a URI scheme → **path**.
    - Has a file extension → **path**.
    - Exists as a filesystem path → **path**.
    - Otherwise (dotted, extension-less, non-existent) → **table**.

    ``access`` gating (path mode only):

    - ``"auto"`` (default): FILE when available, FUSE fallback.
    - ``"external"``: requires a FILE-capable runtime.
    - ``"managed"``: raises for path mode (only valid for a MANAGED FILE-column
      table source; use the table mode or :func:`read_file_table`).

    ``skip_ordering`` (table mode): passed to :func:`resolve_file_table` to
    suppress the auto-sort by source path.  Ignored in path mode.

    Connect-safe: no ``sparkContext``/``_sc``/``_jvm``/``_jsc``/``df.rdd``/
    ``spark.conf.set``.
    """
    effective_st = _classify_vector_source(path, source_type)

    geom_type = BinaryType() if as_wkb else StringType()
    out_schema = StructType(
        [StructField("source", StringType()), StructField("geometry", geom_type)]
    )
    decode_fn = _make_vector_decode_fn(as_wkb, layer)

    if effective_st == "table":
        # Table mode: resolve FILE-column table → plain source paths → decode.
        # decode_fn is the same closure used by path mode — no decode duplication.
        resolved = resolve_file_table(spark, path, skip_ordering=skip_ordering)
        members_df = resolved.select(F.col("source"))
        return members_df.mapInPandas(decode_fn, schema=out_schema)

    # Path mode (original behavior).
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

    return members_df.mapInPandas(decode_fn, schema=out_schema)
