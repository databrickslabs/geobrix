"""Executes the file_gbx / cog_gbx round-trip doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

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
    """file_gbx -> cog_gbx writer produces valid COG files."""
    ex.halo_mode_prepare_cog(spark)


def test_halo_mode_bbox_read(spark):
    """cog_gbx reader bbox clip returns non-empty tiles within the AOI."""
    ex.halo_mode_bbox_read(spark)
