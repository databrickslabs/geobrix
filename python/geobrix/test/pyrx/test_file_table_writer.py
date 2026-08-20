"""Unit tests for the FILE-table writer SQL builders (Task 4).

These are pure string assertions — no Spark session required.
Execution against a real FILE table is verified in Task 9 (dogfood DBR-19).
"""

from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx.pyrx import file_table as ft


def test_create_sql_external_no_partition_no_zorder():
    sql = ft.build_create_sql(
        "cat.sch.t",
        plain_cols=[("cellid", "bigint"), ("path", "string")],
        file_col="tile_file",
        file_mode="external",
        filespace=None,
        layout="cluster",
    )
    assert "tile_file FILE EXTERNAL" in sql
    assert "USING DELTA" in sql
    assert "CLUSTER BY (path)" in sql
    assert "PARTITIONED BY" not in sql
    assert "ZORDER" not in sql.upper()
    assert "geobrix_writer_version" in sql


def test_create_sql_managed_requires_filespace():
    with pytest.raises(ValueError):
        ft.build_create_sql(
            "t",
            plain_cols=[("path", "string")],
            file_col="f",
            file_mode="managed",
            filespace=None,
            layout="order",
        )
    sql = ft.build_create_sql(
        "t",
        plain_cols=[("path", "string")],
        file_col="f",
        file_mode="managed",
        filespace="/Volumes/c/s/v",
        layout="order",
    )
    assert "f FILE MANAGED" in sql
    assert "databricks.filespace-preview" in sql


def test_insert_sql_orders_by_path_and_materializes_managed():
    sql_m = ft.build_insert_sql(
        "t",
        "src",
        select_exprs=["cellid", "path", "create_file(content => raster) AS tile_file"],
        order_by_path=True,
    )
    assert "ORDER BY path" in sql_m
    assert "create_file(content => raster)" in sql_m

    sql_e = ft.build_insert_sql(
        "t",
        "src",
        select_exprs=["cellid", "path", "try_to_file(path) AS tile_file"],
        order_by_path=True,
    )
    assert "try_to_file(path)" in sql_e


def test_create_sql_invalid_file_mode():
    with pytest.raises(ValueError, match="file_mode must be"):
        ft.build_create_sql(
            "t",
            plain_cols=[("path", "string")],
            file_col="f",
            file_mode="bogus",
            filespace=None,
            layout="order",
        )


def test_insert_sql_no_order_by():
    sql = ft.build_insert_sql(
        "t",
        "src",
        select_exprs=["path", "try_to_file(path) AS f"],
        order_by_path=False,
    )
    assert "ORDER BY" not in sql
    assert "INSERT INTO t SELECT path, try_to_file(path) AS f FROM src" == sql


def test_create_sql_no_cluster():
    sql = ft.build_create_sql(
        "sch.tbl",
        plain_cols=[("cellid", "bigint"), ("path", "string")],
        file_col="tile_file",
        file_mode="external",
        filespace=None,
        layout="order",
    )
    assert "CLUSTER BY" not in sql
    assert "tile_file FILE EXTERNAL" in sql
    assert "USING DELTA" in sql


# ---------------------------------------------------------------------------
# Capture tests: write_file_table with tile-struct df — qualify refs without
# executing FILE DDL (which requires DBR-19).  spark.sql is monkeypatched to
# collect SQL strings; createOrReplaceTempView is a no-op.
# ---------------------------------------------------------------------------

_WINDOW_STRUCT = StructType(
    [
        StructField("col_off", IntegerType()),
        StructField("row_off", IntegerType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType()),
    ]
)

_TILE_STRUCT = StructType(
    [
        StructField("cellid", LongType()),
        StructField("raster", BinaryType()),
        StructField("path", StringType()),
        StructField("window", _WINDOW_STRUCT),
        StructField("crs", StringType()),
        StructField("path_mode", StringType()),
    ]
)

_DF_SCHEMA = StructType([StructField("tile", _TILE_STRUCT)])


def _make_mock_df():
    df = MagicMock()
    df.schema = _DF_SCHEMA
    df.createOrReplaceTempView = MagicMock()
    return df


def test_write_file_table_external_uses_qualified_path():
    """tile-struct df + external: INSERT uses tile.path; CREATE DDL has well-formed col defs."""
    captured = []
    spark = MagicMock()
    spark.sql.side_effect = lambda sql: captured.append(sql)

    ft.write_file_table(
        spark,
        _make_mock_df(),
        "cat.sch.t",
        file_mode="external",
        layout="order",
    )

    insert_sqls = [s for s in captured if s.startswith("INSERT")]
    assert insert_sqls, "no INSERT captured"
    insert = insert_sqls[0]
    assert (
        "try_to_file(tile.path)" in insert
    ), f"expected 'try_to_file(tile.path)' in INSERT; got:\n{insert}"
    assert "try_to_file(path)" not in insert.replace(
        "try_to_file(tile.path)", ""
    ), "bare try_to_file(path) found — still using unqualified reference"

    # CREATE DDL must emit well-formed column defs: "<name> <type>", not "<name> <name>:<type>".
    # StructField.simpleString() returns "name:type"; .dataType.simpleString() returns just "type".
    # Using the wrong one yields "window window:struct<...>" → Spark PARSE_SYNTAX_ERROR.
    create_sqls = [s for s in captured if s.startswith("CREATE")]
    assert create_sqls, "no CREATE captured"
    create = create_sqls[0]
    assert (
        "window struct<" in create
    ), f"expected 'window struct<' (well-formed col def) in CREATE DDL; got:\n{create}"
    assert "window:struct" not in create, (
        "stray 'window:struct' in CREATE DDL — "
        "StructField.simpleString() used instead of .dataType.simpleString()"
    )


def test_write_file_table_managed_uses_qualified_raster():
    """tile-struct df + managed: INSERT must use tile.raster, not bare raster."""
    captured = []
    spark = MagicMock()
    spark.sql.side_effect = lambda sql: captured.append(sql)

    ft.write_file_table(
        spark,
        _make_mock_df(),
        "cat.sch.t",
        file_mode="managed",
        filespace="/Volumes/c/s/v",
        layout="order",
    )

    insert_sqls = [s for s in captured if s.startswith("INSERT")]
    assert insert_sqls, "no INSERT captured"
    insert = insert_sqls[0]
    assert (
        "create_file(content => tile.raster)" in insert
    ), f"expected 'create_file(content => tile.raster)' in INSERT; got:\n{insert}"
    assert "create_file(content => raster)" not in insert.replace(
        "create_file(content => tile.raster)", ""
    ), "bare create_file(content => raster) found — still using unqualified reference"


# ---------------------------------------------------------------------------
# I1: validate before DROP
# ---------------------------------------------------------------------------


def test_write_file_table_validates_before_drop():
    """write_file_table raises ValueError BEFORE issuing any DROP when args are invalid.

    Previously build_create_sql (which validates) ran AFTER DROP TABLE, so an
    invalid file_mode with overwrite=True would destroy the existing table, then
    raise.  The fix hoists the same checks to the top of write_file_table.
    """
    dropped = []
    spark = MagicMock()

    def capture_sql(sql):
        if "DROP" in sql.upper():
            dropped.append(sql)

    spark.sql.side_effect = capture_sql

    with pytest.raises(ValueError, match="managed file_mode requires a filespace"):
        ft.write_file_table(
            spark,
            _make_mock_df(),
            "cat.sch.t",
            file_mode="managed",
            filespace=None,
            overwrite=True,
        )
    assert not dropped, f"DROP was issued before validation: {dropped}"


def test_write_file_table_invalid_mode_raises_before_drop():
    """Bogus file_mode also raises before any DROP."""
    dropped = []
    spark = MagicMock()

    def capture_sql(sql):
        if "DROP" in sql.upper():
            dropped.append(sql)

    spark.sql.side_effect = capture_sql

    with pytest.raises(ValueError, match="file_mode must be"):
        ft.write_file_table(
            spark,
            _make_mock_df(),
            "cat.sch.t",
            file_mode="bogus",
            overwrite=True,
        )
    assert not dropped, f"DROP was issued before validation: {dropped}"


# ---------------------------------------------------------------------------
# M3: plain layout → honest write_strategy, no ORDER BY
# ---------------------------------------------------------------------------


def test_write_file_table_plain_layout_no_order_by():
    """layout='plain' → write_strategy is external:plain and INSERT has no ORDER BY."""
    captured = []
    spark = MagicMock()
    spark.sql.side_effect = lambda sql: captured.append(sql)

    ft.write_file_table(
        spark,
        _make_mock_df(),
        "cat.sch.t",
        file_mode="external",
        layout="plain",
    )

    insert_sqls = [s for s in captured if s.startswith("INSERT")]
    assert insert_sqls, "no INSERT captured"
    assert (
        "ORDER BY" not in insert_sqls[0]
    ), f"unexpected ORDER BY in plain layout INSERT:\n{insert_sqls[0]}"

    create_sqls = [s for s in captured if s.startswith("CREATE")]
    assert create_sqls, "no CREATE captured"
    assert (
        "external:plain" in create_sqls[0]
    ), f"write_strategy should be external:plain; got:\n{create_sqls[0]}"
    assert "CLUSTER BY" not in create_sqls[0], "unexpected CLUSTER BY in plain layout"


# ---------------------------------------------------------------------------
# Task 5: build_create_sql/write_file_table compose file_gbx._validate_layout
# ---------------------------------------------------------------------------


def test_build_create_sql_rejects_zorder_layout_via_shared_validator():
    from databricks.labs.gbx.pyrx import file_table

    with pytest.raises(ValueError, match="layout must be one of"):
        file_table.build_create_sql(
            "t",
            plain_cols=[("path", "string")],
            file_col="tile_file",
            file_mode="external",
            filespace=None,
            layout="zorder",
        )
