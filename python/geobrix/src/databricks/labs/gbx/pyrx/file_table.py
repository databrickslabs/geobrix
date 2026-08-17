"""Read/write GeoBrix FILE-column Delta tables (light tier).

Serverless-GC-safe by construction: the reader projects only plain columns
(path/window/crs/...) and never touches the FILE column, because Spark Connect
cannot fetch a FILE-containing schema or collect a FILE value. The FILE ref is
reconstructed lazily at compute time via file_ref_arg(tile["path"]).
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, LongType, StringType

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
        # Every field that could be absent from the source table gets an explicit
        # typed null matching its position in V2_TILE_SCHEMA.  cellid uses LongType
        # (not StringType) — an absent cellid must produce BIGINT null, not STRING null.
        "cellid": F.lit(None).cast(LongType()),
        "raster": F.lit(None).cast(BinaryType()),
        "path": F.lit(None).cast(StringType()),
        "window": F.expr(
            "CAST(NULL AS STRUCT<col_off:INT,row_off:INT,width:INT,height:INT>)"
        ),
        "clip_polygon": F.lit(None).cast(BinaryType()),
        "clip_crs": F.lit(None).cast(StringType()),
        "crs": F.lit(None).cast(StringType()),
        "metadata": F.expr("CAST(NULL AS MAP<STRING,STRING>)"),
    }


def _table_props(spark: SparkSession, table: str) -> dict:
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _project_sql(table: str, present: list) -> str:
    if not present:
        raise ValueError(f"no plain columns to project from {table!r}")
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
    absent = (
        _absent_fields()
    )  # every struct field has an explicit V2_TILE_SCHEMA-typed entry

    # field order/types match V2_TILE_SCHEMA
    # (cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata, path_mode)
    tile_struct = F.struct(
        *[
            F.col(c).alias(c) if c in present else absent[c].alias(c)
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


# ---------------------------------------------------------------------------
# Writer: SQL builders + orchestration
# ---------------------------------------------------------------------------


def _library_version() -> str:
    try:
        from databricks.labs.gbx import __version__

        return str(__version__)
    except (ImportError, AttributeError):
        return "0.0.0"


def build_create_sql(
    table: str,
    *,
    plain_cols: list,
    file_col: str,
    file_mode: str,
    filespace: Optional[str],
    cluster: bool,
) -> str:
    """Return a CREATE TABLE … USING DELTA DDL with a typed FILE column.

    Raises ValueError for an invalid ``file_mode`` or a managed table without
    a ``filespace``.  Never emits PARTITIONED BY or ZORDER.
    """
    if file_mode not in ("external", "managed"):
        raise ValueError(f"file_mode must be external|managed, got {file_mode!r}")
    if file_mode == "managed" and not filespace:
        raise ValueError("managed file_mode requires a filespace (/Volumes/...)")

    col_defs = ", ".join(f"{name} {dtype}" for name, dtype in plain_cols)
    file_kw = "MANAGED" if file_mode == "managed" else "EXTERNAL"
    layout = "cluster" if cluster else "order"

    props = file_props.build_props(
        file_mode=file_mode,
        layout=layout,
        filespace=filespace,
        library_version=_library_version(),
    )
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())

    ddl = (
        f"CREATE TABLE {table} ({col_defs}, {file_col} FILE {file_kw}) "
        f"USING DELTA TBLPROPERTIES ({props_sql})"
    )
    if cluster:
        ddl += " CLUSTER BY (path)"
    return ddl


def build_insert_sql(
    table: str,
    src_view: str,
    *,
    select_exprs: list,
    order_by_path: bool,
) -> str:
    """Return an INSERT INTO … SELECT statement.

    Pass ``create_file(content => raster) AS <file_col>`` or
    ``try_to_file(path) AS <file_col>`` as the last element of ``select_exprs``.
    ``order_by_path=True`` appends ``ORDER BY path`` for locality-aware writes.
    """
    exprs = ", ".join(select_exprs)
    sql = f"INSERT INTO {table} SELECT {exprs} FROM {src_view}"
    if order_by_path:
        sql += " ORDER BY path"
    return sql


def write_file_table(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    *,
    file_mode: str = "external",
    filespace: Optional[str] = None,
    layout: str = "order",
    overwrite: bool = False,
    file_col: str = "tile_file",
) -> None:
    """Create a typed FILE-column Delta table and INSERT df FILE-aligned.

    The DataFrame must have a ``tile`` column whose struct contains at least
    ``path`` (external mode) or ``raster`` (managed mode), plus any extra
    columns to pass through as plain table columns.

    The FILE column is never written via saveAsTable / CTAS — it is materialised
    through ``create_file`` (managed) or referenced via ``try_to_file`` (external)
    in the INSERT statement so Databricks can stamp the correct FILE metadata.

    ``file_mode`` defaults to ``"external"`` (portable, no filespace needed).
    ``layout`` controls clustering: ``"cluster"`` → CLUSTER BY (path);
    ``"order"`` or ``"plain"`` → no clustering (write still honours ORDER BY path).
    """
    view = "_gbx_file_src"
    df.createOrReplaceTempView(view)

    if overwrite:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

    # Plain columns: all top-level df fields except the tile struct itself.
    # Callers typically have a flat df (tile fields already at top level) but
    # we handle the struct-wrapper case too: project tile.* fields except raster.
    tile_fields = (
        {f.name for f in df.schema["tile"].dataType.fields}
        if "tile" in [f.name for f in df.schema.fields]
        else set()
    )

    if tile_fields:
        # df has a `tile` struct — flatten to top-level for the table schema
        plain_cols = [
            (fname, df.schema["tile"].dataType[fname].simpleString())
            for fname in tile_fields
            if fname not in ("raster", "path_mode")
        ]
        # also include any non-tile top-level columns
        plain_cols += [
            (f.name, f.dataType.simpleString())
            for f in df.schema.fields
            if f.name != "tile"
        ]
    else:
        # df is already flat
        plain_cols = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]

    spark.sql(
        build_create_sql(
            table,
            plain_cols=plain_cols,
            file_col=file_col,
            file_mode=file_mode,
            filespace=filespace,
            cluster=(layout == "cluster"),
        )
    )

    # Use qualified struct references so the INSERT SELECT is unambiguous regardless
    # of whether the view has bare top-level columns.  SELECT-level aliases (like the
    # `tile.path AS path` in flat_exprs) are NOT visible to sibling expressions in the
    # same SELECT list — only ORDER BY/HAVING/outer queries see them.
    if tile_fields:
        file_expr = (
            f"create_file(content => tile.raster) AS {file_col}"
            if file_mode == "managed"
            else f"try_to_file(tile.path) AS {file_col}"
        )
    else:
        file_expr = (
            f"create_file(content => raster) AS {file_col}"
            if file_mode == "managed"
            else f"try_to_file(path) AS {file_col}"
        )

    if tile_fields:
        flat_exprs = [
            f"tile.{fname} AS {fname}"
            for fname in tile_fields
            if fname not in ("raster", "path_mode")
        ]
        passthrough_exprs = [f.name for f in df.schema.fields if f.name != "tile"]
        select_exprs = flat_exprs + passthrough_exprs + [file_expr]
    else:
        select_exprs = [f.name for f in df.schema.fields] + [file_expr]

    spark.sql(
        build_insert_sql(
            table,
            view,
            select_exprs=select_exprs,
            order_by_path=(layout in ("order", "cluster")),
        )
    )
