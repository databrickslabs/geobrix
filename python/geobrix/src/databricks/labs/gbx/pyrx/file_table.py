"""Read/write GeoBrix FILE-column Delta tables (light tier).

Serverless-GC-safe by construction: the reader projects only plain columns
(path/window/crs/...) and never touches the FILE column, because Spark Connect
cannot fetch a FILE-containing schema or collect a FILE value. The FILE ref is
reconstructed lazily at compute time via file_ref_arg(tile["path"]).
"""

import uuid
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, LongType, StringType

# _table_props and _describe_cols are now canonical in ds/file_gbx.py.
# Import them here so existing code that references them via
# ``from databricks.labs.gbx.pyrx.file_table import _table_props`` keeps working.
from databricks.labs.gbx.ds.file_gbx import (  # noqa: F401  re-exported for backward compatibility
    _describe_cols,
    _table_props,
    resolve_file_table,
)

from . import file_props
from .core.virtual_tile import V2_TILE_SCHEMA


def _strip_dbfs_scheme(uri):
    """Return a FUSE-openable /Volumes path from a FileRef .uri (dbfs:/Volumes/...)."""
    if uri is None:
        return None
    return uri[len("dbfs:") :] if uri.startswith("dbfs:") else uri


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


def _project_sql(table: str, present: list) -> str:
    if not present:
        raise ValueError(f"no plain columns to project from {table!r}")
    cols = ", ".join(present)
    return f"SELECT {cols} FROM {table}"


def read_file_table(
    spark: SparkSession,
    table: str,
    *,
    skip_ordering: bool = False,
) -> DataFrame:
    """Read a GeoBrix FILE-column table, returning a DataFrame with a ``tile`` column.

    The ``tile`` column is ``V2_TILE_SCHEMA``-shaped.

    Delegates FILE-column detection, path resolution (MANAGED uri-stripping /
    EXTERNAL / plain), and source-path ordering to
    :func:`~databricks.labs.gbx.ds.file_gbx.resolve_file_table`.  The raster
    tile-struct wrapping (``V2_TILE_SCHEMA`` field assembly) is performed here.

    **Managed + FILE-capable runtime**: ``tile.path`` is set to the FILE column's
    ``.uri`` subfield with the ``dbfs:`` scheme stripped to a FUSE-openable
    ``/Volumes/...`` path; ``path_mode`` is ``"managed"``.

    **All other cases** (external table, or ``file_supported`` returns False):
    only plain columns are projected; the FILE column is never referenced
    (Serverless-GC-safe).  ``path_mode`` is taken from the table's ``file_mode``
    property, or ``"external"`` when the table is not GeoBrix-stamped.

    Non-tile columns pass through unchanged.

    Args:
        spark: Active SparkSession.
        table: Fully-qualified or unqualified Delta table name.
        skip_ordering: Passed through to :func:`resolve_file_table`.  Default
            ``False`` preserves (and centralises) the T8 sort-by-source
            convention for table reads.
    """
    # Delegate FILE-column detection + path resolution + ordering to the
    # shared core in ds/file_gbx.py.  The resolved DF has columns:
    #   source (STRING), size (BIGINT|null), path_mode (STRING), <passthrough>
    resolved = resolve_file_table(spark, table, skip_ordering=skip_ordering)

    # Identify tile fields and non-tile passthrough columns from the resolved schema.
    # 'source', 'size', 'path_mode' are consumed here; everything else is passthrough.
    resolved_col_set = set(resolved.schema.fieldNames()) - {
        "source",
        "size",
        "path_mode",
    }
    # Recognized tile fields (V2_TILE_SCHEMA minus 'path', which comes from 'source')
    tile_fields_present = {
        c for c in _TILE_PLAIN_COLS if c != "path" and c in resolved_col_set
    }
    # Non-tile extra columns — sit alongside 'tile' in the final output
    non_tile_passthrough = [c for c in resolved_col_set if c not in _TILE_PLAIN_COLS]

    # Build typed null expressions lazily (requires active SparkContext).
    absent = _absent_fields()

    # Build the tile struct by iterating V2_TILE_SCHEMA.fieldNames() so the
    # emitted field order exactly matches the schema regardless of future reorders.
    # Three computed columns get special treatment:
    #   path      — taken from the 'source' column produced by resolve_file_table
    #   path_mode — taken from the 'path_mode' column (resolved file mode)
    #   raster    — always absent (null BINARY); FILE tables carry no BINARY raster
    tile_struct = F.struct(
        *[
            (
                F.col("source").alias("path")
                if c == "path"
                else (
                    F.col("path_mode").alias("path_mode")
                    if c == "path_mode"
                    else (
                        F.col(c).alias(c)
                        if c in tile_fields_present
                        else absent[c].alias(c)
                    )
                )
            )
            for c in V2_TILE_SCHEMA.fieldNames()
        ]
    )
    # "raster" is intentionally absent from a FILE table (null BINARY).
    out = resolved.withColumn("tile", tile_struct)
    return out.select("tile", *non_tile_passthrough)


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
    layout: str,
) -> str:
    """Return a CREATE TABLE … USING DELTA DDL with a typed FILE column.

    ``layout`` must be ``"plain"``, ``"order"``, or ``"cluster"``.
    ``"cluster"`` adds ``CLUSTER BY (path)``; the others do not.
    The value is stamped verbatim in ``write_strategy`` so the reader
    knows exactly how the table was written.

    Raises ValueError for an invalid ``file_mode`` or a managed table without
    a ``filespace``.  Never emits PARTITIONED BY or ZORDER.
    """
    if file_mode not in ("external", "managed"):
        raise ValueError(f"file_mode must be external|managed, got {file_mode!r}")
    if file_mode == "managed" and not filespace:
        raise ValueError("managed file_mode requires a filespace (/Volumes/...)")

    from databricks.labs.gbx.ds.file_gbx import _validate_layout

    _validate_layout(layout)

    col_defs = ", ".join(f"{name} {dtype}" for name, dtype in plain_cols)
    file_kw = "MANAGED" if file_mode == "managed" else "EXTERNAL"
    cluster = layout == "cluster"

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
    ``layout`` controls write strategy: ``"cluster"`` → CLUSTER BY (path) with
    ORDER BY path; ``"order"`` → ORDER BY path, no clustering; ``"plain"`` → no
    clustering, no ORDER BY.  The value is stamped verbatim in ``write_strategy``
    so the reader knows exactly how the table was written.
    """
    # I1: validate before any side effect (DROP, view creation).
    # build_create_sql validates too, but that runs AFTER the DROP — validating
    # here prevents data loss when overwrite=True and the arguments are invalid.
    if file_mode not in ("external", "managed"):
        raise ValueError(f"file_mode must be external|managed, got {file_mode!r}")
    if file_mode == "managed" and not filespace:
        raise ValueError("managed file_mode requires a filespace (/Volumes/...)")

    from databricks.labs.gbx.ds.file_gbx import _validate_layout

    _validate_layout(layout)

    # M4: unique view name to avoid cross-request clobber.
    view = f"_gbx_file_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
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
                (fname, df.schema["tile"].dataType[fname].dataType.simpleString())
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
                layout=layout,
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
    finally:
        try:
            spark.sql(f"DROP VIEW IF EXISTS {view}")
        except Exception:
            pass
