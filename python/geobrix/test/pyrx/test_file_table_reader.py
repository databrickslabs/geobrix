import shutil
from pathlib import Path

from databricks.labs.gbx.pyrx.file_table import read_file_table


def _make_plain_table(spark, name):
    # Spark 4.0 refuses saveAsTable when the physical warehouse location exists
    # without a matching catalog entry (LOCATION_ALREADY_EXISTS).  Drop the catalog
    # entry first, then remove any stale physical directory so the write is clean.
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [
            (1, "/Volumes/main/s/v/a.tif", "EPSG:4326"),
            (2, "/Volumes/main/s/v/b.tif", "EPSG:4326"),
        ],
        "cellid bigint, path string, crs string",
    )
    df.write.saveAsTable(name)


def test_read_projects_plain_columns_into_v2_tile(spark):
    _make_plain_table(spark, "file_tbl_r1")
    out = read_file_table(spark, "file_tbl_r1")
    assert "tile" in out.columns
    tfields = [f.name for f in out.schema["tile"].dataType.fields]
    assert "path" in tfields and "path_mode" in tfields
    rows = {r["tile"]["path"]: r["tile"]["path_mode"] for r in out.collect()}
    # not geobrix-stamped -> path_mode inferred external
    assert rows["/Volumes/main/s/v/a.tif"] == "external"


def _make_no_cellid_table(spark, name):
    """Table without a cellid column — tests absent-field type safety."""
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame(
        [("/Volumes/main/s/v/a.tif", "EPSG:4326")],
        "path string, crs string",
    )
    df.write.saveAsTable(name)


def test_cellid_absent_field_is_bigint(spark):
    # When the table has no cellid column the absent cellid must produce a BIGINT
    # null (LongType), NOT a STRING null.  A generic null_string fallback would
    # silently contaminate the tile schema vs V2_TILE_SCHEMA.
    _make_no_cellid_table(spark, "file_tbl_r3")
    out = read_file_table(spark, "file_tbl_r3")
    cellid_field = [
        f for f in out.schema["tile"].dataType.fields if f.name == "cellid"
    ][0]
    assert cellid_field.dataType.simpleString() == "bigint"


def test_read_never_selects_star_or_file_column(spark, monkeypatch):
    # guard: the SQL the reader issues must project named plain columns, not *
    import databricks.labs.gbx.pyrx.file_table as ft

    seen = {}
    real = ft._project_sql
    monkeypatch.setattr(
        ft, "_project_sql", lambda *a, **k: seen.setdefault("sql", real(*a, **k))
    )
    _make_plain_table(spark, "file_tbl_r2")
    read_file_table(spark, "file_tbl_r2")
    assert "*" not in seen["sql"] and "SELECT" in seen["sql"].upper()
