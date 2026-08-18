import shutil
from pathlib import Path

import pytest

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


# ---------------------------------------------------------------------------
# T9c: V1 seam tests — file_supported session contract + _describe_cols
# ---------------------------------------------------------------------------


def test_read_file_table_passes_spark_to_file_supported(spark, monkeypatch):
    """read_file_table must call file_supported WITH the spark session, not None.

    V1 root-cause investigation: the leading hypothesis was that spark wasn't
    passed, letting getActiveSession()=None on Spark Connect (DBR 14+) defeat
    the FILE capability gate.  Confirmed: the existing code already passes
    spark correctly.  This test locks in the contract as a regression guard.

    We set up a managed table context (file_mode='managed', file_col present)
    so the code reaches the file_supported gate rather than short-circuiting.
    """
    import databricks.labs.gbx.pyrx._file_ref as fr
    import databricks.labs.gbx.pyrx.file_table as ft
    from databricks.labs.gbx.pyrx import file_props

    _make_managed_stub_table(spark, "file_tbl_session_spy")

    received_sessions = []
    real_file_supported = fr.file_supported

    def spy_file_supported(sess=None):
        received_sessions.append(sess)
        return real_file_supported(sess)

    monkeypatch.setattr(ft, "_table_props", lambda *a, **k: {
        file_props.WRITE_STRATEGY_KEY: "managed:plain",
        file_props.WRITER_VERSION_KEY: "1",
    })
    monkeypatch.setattr(ft, "_describe_cols", lambda s, t: ({"cellid", "path", "crs"}, "tile_file"))
    monkeypatch.setattr(fr, "file_supported", spy_file_supported)

    read_file_table(spark, "file_tbl_session_spy")

    assert received_sessions, "file_supported was never called"
    assert any(s is not None for s in received_sessions), (
        "file_supported was called without a spark session — Spark-Connect "
        "getActiveSession() pitfall; pass spark explicitly (T9c regression)"
    )
    # At least one call must have received the exact session we passed in.
    assert any(s is spark for s in received_sessions), (
        f"file_supported was not called with the caller's spark session; "
        f"got sessions: {[type(s).__name__ for s in received_sessions]}"
    )


@pytest.mark.parametrize(
    "data_type,expected_is_file",
    [
        ("file", True),
        ("FILE", True),  # case-insensitive
        ("file managed", True),  # DBR-19 qualifier variant (V1 root cause)
        ("file external", True),
        ("file (managed)", True),
        ("managed file", True),
        ("external file", True),
        ("string", False),
        ("bigint", False),
        ("binary", False),
        ("struct<uri:string>", False),
    ],
)
def test_describe_cols_file_type_detection(data_type, expected_is_file):
    """_describe_cols must detect FILE columns even when DBR returns a qualified type.

    V1 root cause: on DBR-19, DESCRIBE TABLE may return 'file managed' or
    'managed file' rather than the bare 'file' token.  The original exact check
    (data_type == 'file') would miss these, leaving file_col_name=None and
    use_managed_uri=False even when file_supported(spark)=True.
    """
    import databricks.labs.gbx.pyrx.file_table as ft

    # Simulate DESCRIBE TABLE rows: 'path' STRING and 'tile_file' <data_type>.
    # _describe_cols uses r["col_name"] subscript access, so fake rows must
    # support __getitem__.  Use a simple dict-backed class.
    class _Row(dict):
        pass

    fake_rows = [
        _Row(col_name="path", data_type="string"),
        _Row(col_name="tile_file", data_type=data_type),
    ]

    class _FakeSpark:
        def sql(self, _q):
            class _FakeDF:
                def collect(self_):
                    return fake_rows
            return _FakeDF()

    plain, file_col = ft._describe_cols(_FakeSpark(), "dummy_table")
    if expected_is_file:
        assert file_col == "tile_file", (
            f"data_type={data_type!r}: expected FILE detection, got file_col={file_col!r}. "
            f"V1 root cause: _describe_cols must handle qualified FILE type tokens."
        )
        assert "tile_file" not in plain
    else:
        assert file_col is None, (
            f"data_type={data_type!r}: falsely detected as FILE; file_col={file_col!r}"
        )
        assert "tile_file" in plain
