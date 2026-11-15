"""Executes the raster_gbx writer doc examples (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import raster_gbx_write_examples as ex  # noqa: E402


def test_write_raster_gbx(spark):
    ex.write_raster_gbx(spark)


def test_write_gtiff_gbx(spark):
    ex.write_gtiff_gbx(spark)


def test_write_with_namecol(spark):
    ex.write_with_namecol(spark)


def test_write_with_compress(spark):
    ex.write_with_compress(spark)
