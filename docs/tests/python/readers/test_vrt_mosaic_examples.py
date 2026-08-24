"""Executes the VRT-mosaic doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import vrt_mosaic_examples as ex  # noqa: E402


def test_vrt_prepare(spark):
    """cog_gbx vrtMosaic mode writes mini-COGs and a mosaic.vrt covering the source."""
    ex.vrt_prepare(spark)


def test_vrt_read_expand(spark):
    """raster_gbx expands mosaic.vrt into one virtual tile row per member."""
    ex.vrt_read_expand(spark)


def test_vrt_quadbin_mosaic(spark):
    """cog_gbx quadbin mode writes cell mini-COGs in EPSG:3857 with cellid metadata."""
    ex.vrt_quadbin_mosaic(spark)


def test_vrt_h3_mosaic(spark):
    """cog_gbx h3 mode writes cell mini-COGs in EPSG:4326 with cellid metadata and equi-join."""
    ex.vrt_h3_mosaic(spark)


def test_vrt_bng_mosaic(spark):
    """cog_gbx bng mode writes cell mini-COGs in EPSG:27700 with cellid metadata and equi-join."""
    ex.vrt_bng_mosaic(spark)


def test_vrt_mint_windowed(spark):
    """mint_vrt builds a transient VRT for a windowed rasterio read across members."""
    ex.vrt_mint_windowed(spark)
