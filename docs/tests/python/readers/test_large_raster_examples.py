"""Executes the large-raster reader doc examples against real sample data (Docker)."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
import large_raster_examples as ex  # noqa: E402


def test_read_large_raster_defaults(spark):
    """Default auto-split: directory load emits at least one tile."""
    ex.read_large_raster_defaults(spark)


def test_read_large_raster_split_none(spark):
    """splitStrategy=none yields exactly one tile per file."""
    ex.read_large_raster_split_none(spark)


def test_read_large_raster_cog_output(spark):
    """tileFormat=cog tiles have driver=COG or GTiff (COG is a GTiff profile)."""
    ex.read_large_raster_cog_output(spark)


def test_cog_writer_round_trip(spark):
    """Write with cog=true option then read back; verifies end-to-end COG round-trip."""
    ex.cog_writer_round_trip(spark)
