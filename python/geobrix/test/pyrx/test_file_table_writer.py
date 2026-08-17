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
        cluster=True,
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
            cluster=False,
        )
    sql = ft.build_create_sql(
        "t",
        plain_cols=[("path", "string")],
        file_col="f",
        file_mode="managed",
        filespace="/Volumes/c/s/v",
        cluster=False,
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
            cluster=False,
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
        cluster=False,
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
