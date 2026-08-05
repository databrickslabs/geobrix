"""register(only=[...]) selective registration for the pyvx tier."""

import pytest

from databricks.labs.gbx.pyvx import functions as pvx


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_mvt(spark):
    for n in ("gbx_st_asmvt", "gbx_st_legacyaswkb"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pvx.register(spark, only=["st_asmvt"])
    assert _exists(spark, "gbx_st_asmvt")
    assert not _exists(spark, "gbx_st_legacyaswkb")


def test_only_selects_udtf_and_pmtiles(spark):
    for n in ("gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    pvx.register(spark, only=["gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"])
    assert _exists(spark, "gbx_st_asmvt_pyramid")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_does_not_trip_unselected_guard(spark, monkeypatch):
    from databricks.labs.gbx.pyvx import _env

    def _boom():
        raise RuntimeError("guard should not be called")

    monkeypatch.setattr(_env, "assert_tin_available", _boom)
    monkeypatch.setattr(_env, "assert_legacy_available", _boom)
    pvx.register(spark, only=["gbx_st_asmvt"])  # must not raise
    assert _exists(spark, "gbx_st_asmvt")


def test_only_unknown_raises(spark):
    with pytest.raises(ValueError) as ei:
        pvx.register(spark, only=["st_asmtv"])
    assert "st_asmtv" in str(ei.value)


def test_only_none_registers_all(spark):
    pvx.register(spark)
    for n in (
        "gbx_st_asmvt",
        "gbx_st_legacyaswkb",
        "gbx_st_triangulate",
        "gbx_pmtiles_agg",
        "gbx_st_crs",
        "gbx_st_setcrs",
        "gbx_st_transformcrs",
    ):
        assert _exists(spark, n)
