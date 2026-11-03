"""register(only=[...]) selective registration for the pyrx tier."""

import pytest


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_registers_just_those(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_rst_slope", "gbx_rst_clip"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(
        spark, ("gbx_rst_", "gbx_pmtiles_"), only=["rst_slope"], level="L3"
    )
    assert _exists(spark, "gbx_rst_slope")
    assert not _exists(spark, "gbx_rst_clip")


def test_only_accepts_both_name_forms(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_rst_width", "gbx_rst_height"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(
        spark,
        ("gbx_rst_", "gbx_pmtiles_"),
        only=["gbx_rst_width", "RST_Height"],
        level="L3",
    )
    assert _exists(spark, "gbx_rst_width")
    assert _exists(spark, "gbx_rst_height")


def test_only_selects_udtf_and_pmtiles_agg(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_rst_retile", "gbx_pmtiles_agg"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(
        spark,
        ("gbx_rst_", "gbx_pmtiles_"),
        only=["gbx_rst_retile", "gbx_pmtiles_agg"],
        level="L3",
    )
    assert _exists(spark, "gbx_rst_retile")
    assert _exists(spark, "gbx_pmtiles_agg")


def test_only_unknown_name_raises(spark):
    from databricks.labs.gbx import manager as gbx_manager

    with pytest.raises(ValueError) as ei:
        gbx_manager.register_family(
            spark, ("gbx_rst_", "gbx_pmtiles_"), only=["rst_slpe"], level="L3"
        )
    assert "rst_slpe" in str(ei.value)


def test_only_none_registers_full_set(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(spark, ("gbx_rst_", "gbx_pmtiles_"), level="L3")
    for n in ("gbx_rst_width", "gbx_rst_slope", "gbx_rst_retile", "gbx_pmtiles_agg"):
        assert _exists(spark, n)
