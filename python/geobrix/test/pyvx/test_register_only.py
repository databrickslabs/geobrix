"""register(only=[...]) selective registration for the pyvx tier."""

import pytest


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_mvt(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_st_asmvt", "gbx_st_legacyaswkb"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(spark, ("gbx_st_",), only=["st_asmvt"], level="L3")
    assert _exists(spark, "gbx_st_asmvt")
    assert not _exists(spark, "gbx_st_legacyaswkb")


def test_only_selects_udtf_and_pmtiles(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(
        spark,
        ("gbx_st_",),
        only=["gbx_st_asmvt_pyramid", "gbx_pmtiles_agg"],
        level="L3",
    )
    assert _exists(spark, "gbx_st_asmvt_pyramid")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_unknown_raises(spark):
    from databricks.labs.gbx import manager as gbx_manager

    with pytest.raises(ValueError) as ei:
        gbx_manager.register_family(spark, ("gbx_st_",), only=["st_asmtv"], level="L3")
    assert "st_asmtv" in str(ei.value)


def test_only_none_registers_all(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_st_",), level="L3")
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
