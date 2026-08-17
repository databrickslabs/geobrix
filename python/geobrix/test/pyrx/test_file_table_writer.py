"""Unit tests for the FILE-table writer SQL builders (Task 4).

These are pure string assertions — no Spark session required.
Execution against a real FILE table is verified in Task 9 (dogfood DBR-19).
"""

import pytest

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
