"""register(only=[...]) selective registration for the pygx tier."""

import pytest


def _exists(spark, name):
    return spark.catalog.functionExists(name)


def test_only_subset_quadbin(spark):
    from databricks.labs.gbx import manager as gbx_manager

    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill"):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    gbx_manager.register_family(
        spark,
        ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"),
        only=["quadbin_polyfill"],
        level="L3",
    )
    assert _exists(spark, "gbx_quadbin_polyfill")
    assert not _exists(spark, "gbx_bng_polyfill")


def test_only_accepts_camelcase(spark):
    from databricks.labs.gbx import manager as gbx_manager

    spark.sql("DROP TEMPORARY FUNCTION IF EXISTS gbx_bng_polyfill")
    gbx_manager.register_family(
        spark,
        ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"),
        only=["BNG_Polyfill"],
        level="L3",
    )
    assert _exists(spark, "gbx_bng_polyfill")


def test_only_unknown_raises(spark):
    from databricks.labs.gbx import manager as gbx_manager

    with pytest.raises(ValueError) as ei:
        gbx_manager.register_family(
            spark,
            ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"),
            only=["quadbin_polifyll"],
            level="L3",
        )
    assert "quadbin_polifyll" in str(ei.value)


def test_only_does_not_trip_unselected_subgroup_guard(spark, monkeypatch):
    # Selecting only a quadbin fn must NOT call the bng/custom availability guards.
    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx.pygx import _env

    def _boom():
        raise RuntimeError("guard should not be called")

    monkeypatch.setattr(_env, "assert_bng_available", _boom)
    monkeypatch.setattr(_env, "assert_custom_available", _boom)
    gbx_manager.register_family(
        spark,
        ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"),
        only=["gbx_quadbin_resolution"],
        level="L3",
    )  # must not raise
    assert _exists(spark, "gbx_quadbin_resolution")


def test_only_none_registers_all_subgroups(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"), level="L3"
    )
    for n in ("gbx_quadbin_polyfill", "gbx_bng_polyfill", "gbx_custom_polyfill"):
        assert _exists(spark, n)
