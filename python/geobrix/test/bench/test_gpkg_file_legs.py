"""Tests for GeoPackage FILE read+write bench legs.

FUSE legs (fuse tier, local[2]):
  - run_gpkg_file_read with file_mode="fuse" → status="ok", chunk_size recorded
  - run_gpkg_file_write with file_mode="fuse" → status="ok" (or "na_by_design"
    if the gpkg_gbx writer is unavailable in the local env)

External leg on FUSE tier:
  - run_gpkg_file_read with file_mode="external" → status="na_by_design"
    (vector_file_read raises ValueError when FILE is unavailable)

Fixtures use pyogrio.write_arrow (no geopandas required).
"""


def _write_gpkg(path, rows=5, offset_y=0.0):
    """Write a .gpkg with `rows` Point features via pyogrio.write_arrow (no geopandas)."""
    import pyarrow as pa
    import pyogrio
    import shapely

    wkbs = [
        bytes(shapely.to_wkb(shapely.Point(float(j), offset_y))) for j in range(rows)
    ]
    tbl = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
    pyogrio.write_arrow(
        tbl,
        path,
        driver="GPKG",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )


def _write_n_gpkg(tmp_path, n, rows=5):
    """Write n .gpkg files under tmp_path, each with `rows` point features."""
    for i in range(n):
        _write_gpkg(str(tmp_path / f"t{i}.gpkg"), rows=rows, offset_y=float(i))


def _write_one_gpkg(tmp_path, rows=5):
    """Write a single .gpkg file under tmp_path and return its path."""
    p = str(tmp_path / "out.gpkg")
    _write_gpkg(p, rows=rows)
    return p


def test_gpkg_file_read_external_skips_on_fuse_tier(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 2, rows=5)
    r = run_gpkg_file_read(
        spark, str(tmp_path), "t", 0, 1, file_mode="external", where="venv"
    )
    assert r.status == "na_by_design"
    assert r.file_mode == "external"


def test_gpkg_file_read_fuse_ok_records_chunksize(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 2, rows=5)
    r = run_gpkg_file_read(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_size=1000,
        where="venv",
    )
    assert r.status == "ok"
    assert r.rows == 10  # 2 files x 5 features
    assert r.chunk_size == 1000


def test_gpkg_file_read_managed_directory_path_is_na_by_design(spark, tmp_path):
    """BUG A: run_gpkg_file_read(file_mode='managed', source=<dir>) must return
    status='na_by_design', NOT crash with a Spark parse error.
    The managed path requires a FILE-column table name, not a Volume directory.
    This test FAILS (error/exception) until the BUG-A fix is applied.
    """
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 1, rows=3)
    r = run_gpkg_file_read(
        spark, str(tmp_path), "t", 0, 1, file_mode="managed", where="venv"
    )
    assert r.status == "na_by_design", (
        f"Expected na_by_design for managed read over a directory path, got: "
        f"status={r.status!r} note={r.note!r}"
    )
    assert r.file_mode == "managed"


def test_gpkg_file_write_fuse_ok(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_write

    out = _write_one_gpkg(tmp_path, rows=5)
    r = run_gpkg_file_write(
        spark,
        out,
        str(tmp_path / "wt"),
        "t",
        0,
        1,
        file_mode="fuse",
        where="venv",
    )
    assert r.status in ("ok", "na_by_design")


def test_gpkg_file_write_external_na_by_design_on_fuse(spark, tmp_path):
    """external FILE write on a FUSE-only tier (local[2]) → na_by_design, not a crash.

    vector_file_write copies the assembled .gpkg into the staging filespace, then
    gbx_file_write raises ValueError (FILE write-primitive unavailable) → the probe
    catches it and returns na_by_design.
    """
    from databricks.labs.gbx.bench.readers import run_gpkg_file_write

    out = _write_one_gpkg(tmp_path, rows=5)
    r = run_gpkg_file_write(
        spark,
        out,
        "no_cat.no_sch.bench_gpkg_ext",
        "t",
        0,
        1,
        file_mode="external",
        filespace=str(tmp_path / "fs"),  # vector external needs a staging filespace
        where="venv",
    )
    assert r.status == "na_by_design", f"got status={r.status!r} note={r.note!r}"
    assert r.file_mode == "external"


def test_gpkg_file_write_managed_na_by_design_on_fuse(spark, tmp_path):
    """managed FILE write on a FUSE-only tier (local[2]) → na_by_design, not a crash."""
    from databricks.labs.gbx.bench.readers import run_gpkg_file_write

    out = _write_one_gpkg(tmp_path, rows=5)
    r = run_gpkg_file_write(
        spark,
        out,
        "no_cat.no_sch.bench_gpkg_mgd",
        "t",
        0,
        1,
        file_mode="managed",
        filespace=str(tmp_path / "fs"),
        where="venv",
    )
    assert r.status == "na_by_design", f"got status={r.status!r} note={r.note!r}"
    assert r.file_mode == "managed"


# ---------------------------------------------------------------------------
# FILE-mode readback (_gpkg_file_readback): a vector FILE write stores the whole
# .gpkg as ONE FILE reference in a FILE-column Delta table. Correctness is FEATURE
# parity, resolved via read_file_table -> tile.path -> vector_file_read(PATH).
# These tests stand in for read_file_table (FILE tier absent on local[2]) so the
# round-trip logic itself is exercised on the fuse tier.
# ---------------------------------------------------------------------------


def _tile_path_df(spark, gpkg_path):
    """Mimic read_file_table's output: a DataFrame with a `tile` struct whose
    ``.path`` sub-field is a resolved /Volumes-style path to the written .gpkg."""
    from pyspark.sql import functions as F

    return spark.createDataFrame([(gpkg_path,)], "path string").select(
        F.struct(F.col("path")).alias("tile")
    )


def test_gpkg_file_readback_reads_path_not_table_name(spark, tmp_path, monkeypatch):
    """The FILE-mode readback resolves tile.path via read_file_table and reads THAT
    path with vector_file_read — it must NOT hand the schema.table name to
    vector_file_read (which reads a LOCATION and would raise FileNotFoundError).

    If the readback regressed to passing the table name, the real vector_file_read
    below would fail and status would be 'error', not 'ok'.
    """
    from databricks.labs.gbx.bench import readers as rd

    gpkg = _write_one_gpkg(tmp_path, rows=7)
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda _s, _t: _tile_path_df(_s, gpkg),
    )
    status, note, refs = rd._gpkg_file_readback(
        spark, "cat.sch.bench_layout_gpkg_external_order", 7, "external"
    )
    assert status == "ok", f"expected ok, got status={status!r} note={note!r}"
    assert refs == 1
    assert "7 features" in note


def test_gpkg_file_readback_feature_count_mismatch_is_error(
    spark, tmp_path, monkeypatch
):
    """Round-trip feature count that disagrees with the source → status='error'."""
    from databricks.labs.gbx.bench import readers as rd

    gpkg = _write_one_gpkg(tmp_path, rows=5)
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table",
        lambda _s, _t: _tile_path_df(_s, gpkg),
    )
    status, note, refs = rd._gpkg_file_readback(spark, "cat.sch.tbl", 999, "managed")
    assert status == "error"
    assert "readback 5 != 999" in note


def test_gpkg_file_readback_no_file_refs_is_error(spark, monkeypatch):
    """A table with no non-null tile.path row → status='error' (0 file refs)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import readers as rd

    def _null_tile(_s, _t):
        return _s.createDataFrame([(None,)], "path string").select(
            F.struct(F.col("path")).alias("tile")
        )

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.file_table.read_file_table", _null_tile
    )
    status, note, refs = rd._gpkg_file_readback(spark, "cat.sch.tbl", 5, "external")
    assert status == "error"
    assert refs == 0
    assert "0 file refs" in note
