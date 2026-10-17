"""Executes the gpkg_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import geopackage_gbx_examples as ex  # noqa: E402


def test_read_gpkg_gbx(spark):
    ex.read_gpkg_gbx(spark)


def test_both_tiers_name_the_geometry_column_as_the_layer_does(spark):
    """The page says both tiers produce the same schema: the geometry column keeps the
    layer's name (`geom` in the sample file; `geom_0` only for a layer that names none)."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    light = spark.read.format("gpkg_gbx").load(ex.SAMPLE)
    heavy = spark.read.format("gpkg_ogr").load(ex.SAMPLE)
    assert light.columns == heavy.columns
    assert light.columns[-3:] == ["geom", "geom_srid", "geom_srid_proj"]
    assert "geom_0" not in light.columns
