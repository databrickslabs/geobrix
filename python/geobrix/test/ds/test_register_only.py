"""register(only=[...]) selective registration for the light DataSources."""

import pytest


def _format_ok(spark, fmt):
    """A format is registered if .format(fmt) builds a reader without an
    'unsupported data source' error. Loading an empty path raises a DIFFERENT
    (path/IO) error, so we treat only the unsupported-format error as 'absent'."""
    try:
        spark.read.format(fmt).load("/tmp/__nonexistent_gbx_probe__")
        return True
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if "unable to find" in msg or ("data source" in msg and "not" in msg):
            return False
        return True  # some other error => the format WAS resolved


def test_only_subset_registers_just_those(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark, only=["raster_gbx", "gtiff_gbx"])
    assert _format_ok(spark, "raster_gbx")
    assert _format_ok(spark, "gtiff_gbx")


def test_only_accepts_bare_name_without_suffix(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark, only=["raster"])  # -> raster_gbx
    assert _format_ok(spark, "raster_gbx")


def test_only_unknown_format_raises(spark):
    from databricks.labs.gbx import manager as gbx_manager

    with pytest.raises(ValueError) as ei:
        gbx_manager.register_sources(spark, only=["raster_gpx"])
    assert "raster_gpx" in str(ei.value)


def test_only_none_registers_all(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    for fmt in ("raster_gbx", "gtiff_gbx", "shapefile_gbx", "geojson_gbx"):
        assert _format_ok(spark, fmt)


def test_lidar_registered(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark, only=["lidar"])
    assert _format_ok(spark, "lidar_gbx")
