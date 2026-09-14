"""register(only=[...]) selective registration for the light DataSources."""

import pytest

from databricks.labs.gbx.ds import register as ds_register


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
    ds_register.register(spark, only=["raster_gbx", "gtiff_gbx"])
    assert _format_ok(spark, "raster_gbx")
    assert _format_ok(spark, "gtiff_gbx")


def test_only_accepts_bare_name_without_suffix(spark):
    ds_register.register(spark, only=["raster"])  # -> raster_gbx
    assert _format_ok(spark, "raster_gbx")


def test_only_unknown_format_raises(spark):
    with pytest.raises(ValueError) as ei:
        ds_register.register(spark, only=["raster_gpx"])
    assert "raster_gpx" in str(ei.value)


def test_only_none_registers_all(spark):
    ds_register.register(spark)
    for fmt in ("raster_gbx", "gtiff_gbx", "shapefile_gbx", "geojson_gbx"):
        assert _format_ok(spark, fmt)


def test_lidar_registered(spark):
    ds_register.register(spark, only=["lidar"])
    assert _format_ok(spark, "lidar_gbx")
