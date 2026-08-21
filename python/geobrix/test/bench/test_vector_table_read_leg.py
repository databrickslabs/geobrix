"""Tests for run_vector_table_read_sweep (Task 5: vector FILE-column-table read leg).

Covers:
  - fuse mode returns na_by_design without calling vector_file_read
  - managed/external: monkeypatched vector_file_read (table mode) returns n features → "ok"
  - managed: mismatch between decoded count and expected_n → "error"
  - managed: ValueError from vector_file_read → na_by_design
"""


def _make_feature_df(spark, n):
    """Return a 1-column DataFrame with n rows (simulates decoded feature rows)."""
    from pyspark.sql import functions as F

    return spark.range(n).select(
        F.lit("source.gpkg").alias("source"),
        F.lit(b"\x00").alias("geometry"),
    )


def test_vector_table_read_fuse_is_na_by_design(spark):
    """fuse mode returns na_by_design immediately, no vector_file_read call needed."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    r = run_vector_table_read_sweep(
        spark, "", "t", 0, 1, file_mode="fuse", where="venv"
    )
    assert r.status == "na_by_design"
    assert r.file_mode == "fuse"
    assert "na_by_design" not in r.note or "FILE FUSE" in r.note


def test_vector_table_read_external_ok_on_parity(spark, monkeypatch):
    """external mode: vector_file_read returns expected_n features → status='ok'."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    n = 7

    def _mock_vfr(_s, _table, *, source_type="auto", **_kw):
        return _make_feature_df(_s, n)

    monkeypatch.setattr(
        "databricks.labs.gbx.pyvx.file_read.vector_file_read", _mock_vfr
    )

    r = run_vector_table_read_sweep(
        spark,
        "cat.sch.bench_layout_gpkg_external_order",
        "t",
        0,
        1,
        file_mode="external",
        expected_n=n,
        where="venv",
    )
    assert r.status == "ok", f"got status={r.status!r} note={r.note!r}"
    assert r.rows == n
    assert r.file_mode == "external"


def test_vector_table_read_managed_ok_on_parity(spark, monkeypatch):
    """managed mode: vector_file_read returns expected_n features → status='ok'."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    n = 12

    def _mock_vfr(_s, _table, *, source_type="auto", **_kw):
        return _make_feature_df(_s, n)

    monkeypatch.setattr(
        "databricks.labs.gbx.pyvx.file_read.vector_file_read", _mock_vfr
    )

    r = run_vector_table_read_sweep(
        spark,
        "cat.sch.bench_layout_gpkg_managed_order",
        "t",
        0,
        1,
        file_mode="managed",
        expected_n=n,
        where="venv",
    )
    assert r.status == "ok", f"got status={r.status!r} note={r.note!r}"
    assert r.rows == n
    assert r.file_mode == "managed"


def test_vector_table_read_mismatch_is_error(spark, monkeypatch):
    """Decoded feature count != expected_n → status='error' with readback note."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    decoded_n = 5
    expected_n = 99

    def _mock_vfr(_s, _table, *, source_type="auto", **_kw):
        return _make_feature_df(_s, decoded_n)

    monkeypatch.setattr(
        "databricks.labs.gbx.pyvx.file_read.vector_file_read", _mock_vfr
    )

    r = run_vector_table_read_sweep(
        spark,
        "cat.sch.bench_layout_gpkg_external_order",
        "t",
        0,
        1,
        file_mode="external",
        expected_n=expected_n,
        where="venv",
    )
    assert r.status == "error", f"got status={r.status!r} note={r.note!r}"
    assert f"readback {decoded_n} != {expected_n}" in r.note


def test_vector_table_read_value_error_is_na_by_design(spark, monkeypatch):
    """ValueError from vector_file_read (FILE tier unavailable) → na_by_design."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    def _raise_vfr(_s, _table, *, source_type="auto", **_kw):
        raise ValueError("FILE tier unavailable on FUSE-only tier")

    monkeypatch.setattr(
        "databricks.labs.gbx.pyvx.file_read.vector_file_read", _raise_vfr
    )

    r = run_vector_table_read_sweep(
        spark,
        "cat.sch.bench_layout_gpkg_external_order",
        "t",
        0,
        1,
        file_mode="external",
        where="venv",
    )
    assert r.status == "na_by_design", f"got status={r.status!r} note={r.note!r}"
    assert r.file_mode == "external"


def test_vector_table_read_no_expected_n_ok_on_nonzero(spark, monkeypatch):
    """When expected_n=0 (unknown), any non-zero feature count → status='ok'."""
    from databricks.labs.gbx.bench.readers import run_vector_table_read_sweep

    def _mock_vfr(_s, _table, *, source_type="auto", **_kw):
        return _make_feature_df(_s, 3)

    monkeypatch.setattr(
        "databricks.labs.gbx.pyvx.file_read.vector_file_read", _mock_vfr
    )

    r = run_vector_table_read_sweep(
        spark,
        "cat.sch.bench_layout_gpkg_external_order",
        "t",
        0,
        1,
        file_mode="external",
        expected_n=0,  # unknown
        where="venv",
    )
    assert r.status == "ok", f"got status={r.status!r} note={r.note!r}"
    assert r.rows == 3
