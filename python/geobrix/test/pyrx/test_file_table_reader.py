import shutil
from pathlib import Path

from databricks.labs.gbx.pyrx.file_table import read_file_table


def test_strip_dbfs_scheme():
    from databricks.labs.gbx.pyrx.file_table import _strip_dbfs_scheme

    assert _strip_dbfs_scheme("dbfs:/Volumes/c/s/v/f.tif") == "/Volumes/c/s/v/f.tif"
    assert _strip_dbfs_scheme("/Volumes/c/s/v/f.tif") == "/Volumes/c/s/v/f.tif"
    assert _strip_dbfs_scheme(None) is None


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


def _make_managed_stub_table(spark, name):
    """Table with a tile_file STRUCT<uri STRING> column standing in for a FILE column.

    In local[2] Spark the FILE type is unavailable, so we use a plain struct whose
    .uri subfield is accessible at plan-analysis time.  Tests monkeypatch
    _describe_cols to report tile_file as type ``file`` so the reader branches
    into the managed+capable path.
    """
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    from pyspark.sql import Row

    rows = [
        Row(
            cellid=1,
            path="/Volumes/main/s/v/a.tif",
            crs="EPSG:4326",
            tile_file=Row(uri="dbfs:/Volumes/main/s/v/a.tif"),
        ),
        Row(
            cellid=2,
            path="/Volumes/main/s/v/b.tif",
            crs="EPSG:4326",
            tile_file=Row(uri="dbfs:/Volumes/main/s/v/b.tif"),
        ),
    ]
    df = spark.createDataFrame(
        rows,
        "cellid bigint, path string, crs string, tile_file struct<uri:string>",
    )
    df.write.saveAsTable(name)


def test_managed_capable_branch_uses_file_uri(spark, monkeypatch):
    """Managed+capable: tile.path comes from FILE .uri (dbfs: stripped); path_mode=managed."""
    _make_managed_stub_table(spark, "file_tbl_managed_cap")

    import databricks.labs.gbx.pyrx._file_ref as fr
    import databricks.labs.gbx.pyrx.file_table as ft
    from databricks.labs.gbx.pyrx import file_props

    # Stamp as managed table
    monkeypatch.setattr(
        ft,
        "_table_props",
        lambda *a, **k: {
            file_props.WRITE_STRATEGY_KEY: "managed:plain",
            file_props.WRITER_VERSION_KEY: "1",
        },
    )
    # Report tile_file as the FILE-typed column (plain set excludes it)
    monkeypatch.setattr(
        ft,
        "_describe_cols",
        lambda s, t: ({"cellid", "path", "crs"}, "tile_file"),
    )
    # FILE is supported
    monkeypatch.setattr(fr, "_FILE_SUPPORT_CACHE", {id(spark): True})

    out = read_file_table(spark, "file_tbl_managed_cap")

    # Verify schema has path_mode field
    tile_fields = {f.name for f in out.schema["tile"].dataType.fields}
    assert "path" in tile_fields and "path_mode" in tile_fields

    rows = {r["tile"]["cellid"]: r["tile"] for r in out.collect()}
    # path_mode must be "managed"
    assert rows[1]["path_mode"] == "managed"
    assert rows[2]["path_mode"] == "managed"
    # path must be the stripped uri (no dbfs: prefix)
    assert rows[1]["path"] == "/Volumes/main/s/v/a.tif"
    assert rows[2]["path"] == "/Volumes/main/s/v/b.tif"


def test_not_capable_branch_no_file_col_reference(spark, monkeypatch):
    """Not-capable: FILE column is never referenced; path comes from plain column."""
    _make_managed_stub_table(spark, "file_tbl_managed_notcap")

    import databricks.labs.gbx.pyrx._file_ref as fr
    import databricks.labs.gbx.pyrx.file_table as ft
    from databricks.labs.gbx.pyrx import file_props

    # Stamp as managed table
    monkeypatch.setattr(
        ft,
        "_table_props",
        lambda *a, **k: {
            file_props.WRITE_STRATEGY_KEY: "managed:plain",
            file_props.WRITER_VERSION_KEY: "1",
        },
    )
    # Report tile_file as the FILE-typed column — but FILE is NOT supported
    monkeypatch.setattr(
        ft,
        "_describe_cols",
        lambda s, t: ({"cellid", "path", "crs"}, "tile_file"),
    )
    # FILE is NOT supported
    monkeypatch.setattr(fr, "_FILE_SUPPORT_CACHE", {id(spark): False})

    # Capture the SQL issued to verify file col is never referenced
    issued_sqls = []
    real_spark_sql = spark.sql
    monkeypatch.setattr(
        spark, "sql", lambda s, **k: issued_sqls.append(s) or real_spark_sql(s, **k)
    )

    out = read_file_table(spark, "file_tbl_managed_notcap")
    rows = {r["tile"]["cellid"]: r["tile"] for r in out.collect()}

    # path_mode is "managed" (from property) but path comes from plain column
    assert rows[1]["path_mode"] == "managed"
    assert rows[1]["path"] == "/Volumes/main/s/v/a.tif"

    # FILE column name must not appear in any SQL the reader issued
    file_col_refs = [s for s in issued_sqls if "tile_file" in s]
    assert (
        not file_col_refs
    ), f"FILE column referenced when not capable: {file_col_refs}"


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
