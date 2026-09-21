"""Executes the exif_gbx reader doc examples against a synthetic JPEG fixture.

Runs in Docker (gbx:test:python-docs --path docs/tests/python/readers/test_exif_reader_example.py).
Pillow is available in the dev container; the JPEG fixture requires no sample-data mount.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import exif_reader_example as ex  # noqa: E402


@pytest.fixture(scope="module")
def exif_dir(tmp_path_factory):
    """Synthesise a JPEG with known EXIF data into a temp directory."""
    d = tmp_path_factory.mktemp("exif_images")
    ex._make_exif_jpeg(d)
    return str(d)


def test_exif_metadata_mode(spark, exif_dir):
    """metadata mode: one row per file, GPS + camera fields populated, geom_wkb set."""
    ex.demo_exif_metadata_mode(spark, exif_dir)


def test_exif_schema_fields(spark, exif_dir):
    """All EXIF_META_SCHEMA fields present in the collected row."""
    ex.demo_exif_schema_fields(spark, exif_dir)


def test_exif_geom_wkb_point(spark, exif_dir):
    """geom_wkb decodes to a 2D WKB POINT with the correct lon/lat."""
    ex.demo_exif_geom_wkb_point(spark, exif_dir)
