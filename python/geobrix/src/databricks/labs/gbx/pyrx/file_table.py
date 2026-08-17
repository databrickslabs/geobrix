"""Read/write GeoBrix FILE-column Delta tables (light tier).

Serverless-GC-safe by construction: the reader projects only plain columns
(path/window/crs/...) and never touches the FILE column, because Spark Connect
cannot fetch a FILE-containing schema or collect a FILE value. The FILE ref is
reconstructed lazily at compute time via file_ref_arg(tile["path"]).
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType

from . import file_props

# plain (non-FILE) columns the reader is willing to project into a tile
_TILE_PLAIN_COLS = (
    "cellid",
    "path",
    "window",
    "clip_polygon",
    "clip_crs",
    "crs",
    "metadata",
)


def _absent_fields():
    """Return typed null Column expressions for each absent tile struct field.

    Built lazily (inside a function) because F.lit / F.expr require an active
    SparkContext and cannot be evaluated at module import time.

    Spark 4 refuses to cast VOID→BINARY/MAP/STRUCT at the struct level
    (DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION), so each null carries the
    correct DataType before the struct is assembled — no struct-level cast needed.
    """
    return {
        "raster": F.lit(None).cast(BinaryType()),
        "window": F.expr(
            "CAST(NULL AS STRUCT<col_off:INT,row_off:INT,width:INT,height:INT>)"
        ),
        "clip_polygon": F.lit(None).cast(BinaryType()),
        "clip_crs": F.lit(None).cast(StringType()),
        "metadata": F.expr("CAST(NULL AS MAP<STRING,STRING>)"),
    }


def _table_props(spark: SparkSession, table: str) -> dict:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _project_sql(table: str, present: list) -> str:
    cols = ", ".join(present)
    return f"SELECT {cols} FROM {table}"


def read_file_table(
    spark: SparkSession,
    table: str,
    *,
    tile_cols: Optional[dict] = None,
) -> DataFrame:
    """Read a GeoBrix FILE-column table, returning a DataFrame with a ``tile`` column.

    The ``tile`` column is ``V2_TILE_SCHEMA``-shaped, built only from plain columns
    (path/window/crs/...).  The FILE column is never selected (Serverless-GC-safe).
    ``path_mode`` on each tile is taken from the table's ``file_mode`` property, or
    ``"external"`` when the table is not GeoBrix-stamped.  Non-tile columns pass through.
    """
    parsed = file_props.parse_props(_table_props(spark, table))
    path_mode = parsed["file_mode"] or "external"

    # Enumerate columns via DESCRIBE TABLE — NOT spark.table(...).schema, which is
    # unsafe on Serverless GC when a FILE column is present.
    desc = spark.sql(f"DESCRIBE TABLE {table}").collect()
    plain = {
        r["col_name"]
        for r in desc
        if r["col_name"]
        and not r["col_name"].startswith("#")
        and (r["data_type"] or "").lower() != "file"
    }
    present = [c for c in _TILE_PLAIN_COLS if c in plain]
    passthrough = [c for c in plain if c not in _TILE_PLAIN_COLS]

    base = spark.sql(_project_sql(table, present + passthrough))

    # Build typed null expressions lazily (requires active SparkContext).
    absent = _absent_fields()
    null_string = F.lit(None).cast(StringType())

    # Build tile struct with pre-typed null expressions for absent fields so that
    # no struct-level cast is required (Spark 4 rejects VOID→complex type casts).
    tile_struct = F.struct(
        *[
            F.col(c).alias(c) if c in present else absent.get(c, null_string).alias(c)
            for c in (
                "cellid",
                "raster",
                "path",
                "window",
                "clip_polygon",
                "clip_crs",
                "crs",
                "metadata",
            )
        ],
        F.lit(path_mode).alias("path_mode"),
    )
    # "raster" is intentionally absent from a FILE table (null BINARY).
    out = base.withColumn("tile", tile_struct)
    return out.select("tile", *passthrough)
