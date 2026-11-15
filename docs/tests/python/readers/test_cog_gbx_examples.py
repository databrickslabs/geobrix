"""Executes the file_gbx / cog_gbx round-trip doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import cog_gbx_examples as ex  # noqa: E402


def test_list_files_file_gbx(spark):
    """file_gbx emits path-reference rows with the expected schema."""
    ex.list_files_file_gbx(spark)


def test_list_files_filter_regex(spark):
    """filterRegex option keeps only matching files."""
    ex.list_files_filter_regex(spark)


def test_halo_mode_prepare_cog(spark):
    """file_gbx -> cog_gbx writer produces valid COG files (GDAL's validator: needs osgeo)."""
    from databricks.labs.gbx import _osgeo

    try:
        ex.halo_mode_prepare_cog(spark)
    except _osgeo.OsgeoUnavailable as exc:  # raised before any work, by the validator lookup
        pytest.skip(str(exc))


def test_halo_mode_bbox_read(spark):
    """file_gbx -> cog_gbx writer -> cog_gbx reader whole-file round-trip.

    Confirms the full preparation pipeline: list, write COGs, read back.
    Bbox clipping is unit-tested in test_raster_bbox.py / test_cog_reader.py.
    """
    ex.halo_mode_bbox_read(spark)
