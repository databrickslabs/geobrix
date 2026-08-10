"""
Tests that the committed multiband GeoTIFF fixture is present and valid.

Fixture path: src/test/resources/binary/geotiff-small/rgb_nir_small.tif

These tests exercise rasterio directly (no Spark, no JAR) so they run
quickly in the docs-test Docker container.

See _fixtures.py for the generator that produced the .tif.
"""

from pathlib import Path

import pytest

try:
    from . import _fixtures
except (ImportError, ModuleNotFoundError):
    import _fixtures  # type: ignore[no-redef]

MULTIBAND = _fixtures.MULTIBAND

# Resolve fixture path relative to the repo root
REPO_ROOT = Path(__file__).parents[4]
FIXTURE_PATH = REPO_ROOT / MULTIBAND


# ---------------------------------------------------------------------------
# Existence
# ---------------------------------------------------------------------------


def test_multiband_fixture_exists():
    """The committed .tif must be present on disk."""
    assert FIXTURE_PATH.exists(), (
        f"Multiband fixture not found at {FIXTURE_PATH}. "
        "Run docs/tests/python/api/_fixtures.py to regenerate."
    )


# ---------------------------------------------------------------------------
# Band count
# ---------------------------------------------------------------------------


def test_multiband_fixture_has_3_bands():
    """Fixture must have exactly 3 bands (red, NIR, green)."""
    rasterio = pytest.importorskip("rasterio")
    with rasterio.open(FIXTURE_PATH) as ds:
        assert ds.count == 3, f"Expected 3 bands, got {ds.count}"


# ---------------------------------------------------------------------------
# CRS
# ---------------------------------------------------------------------------


def test_multiband_fixture_has_crs():
    """Fixture must carry a valid CRS (EPSG:4326)."""
    rasterio = pytest.importorskip("rasterio")
    with rasterio.open(FIXTURE_PATH) as ds:
        assert ds.crs is not None, "CRS is None — fixture missing projection"
        assert ds.crs.to_epsg() == 4326, (
            f"Expected EPSG:4326, got {ds.crs.to_epsg()}"
        )


# ---------------------------------------------------------------------------
# Per-band metadata (tags)
# ---------------------------------------------------------------------------


def test_multiband_fixture_band1_tags_nonempty():
    """Band 1 must have at least one metadata tag so rst_bandmetadata is meaningful."""
    rasterio = pytest.importorskip("rasterio")
    with rasterio.open(FIXTURE_PATH) as ds:
        tags = ds.tags(1)
        assert tags, (
            f"Band 1 tags are empty: {tags!r}. "
            "Regenerate fixture with make_multiband_fixture()."
        )
        # Spot-check the name tag written by the generator
        assert "name" in tags, f"Expected 'name' key in band 1 tags; got {tags}"
        assert tags["name"] == "red", f"Expected name=red; got {tags['name']}"


def test_multiband_fixture_all_bands_have_name_tag():
    """All three bands must have a 'name' metadata tag."""
    rasterio = pytest.importorskip("rasterio")
    expected = {1: "red", 2: "nir", 3: "green"}
    with rasterio.open(FIXTURE_PATH) as ds:
        for band_idx, expected_name in expected.items():
            tags = ds.tags(band_idx)
            assert "name" in tags, (
                f"Band {band_idx} missing 'name' tag; got {tags!r}"
            )
            assert tags["name"] == expected_name, (
                f"Band {band_idx}: expected name={expected_name!r}, got {tags['name']!r}"
            )


# ---------------------------------------------------------------------------
# Shape sanity
# ---------------------------------------------------------------------------


def test_multiband_fixture_shape():
    """Fixture must be the expected 8x8 pixel size."""
    rasterio = pytest.importorskip("rasterio")
    with rasterio.open(FIXTURE_PATH) as ds:
        assert ds.width == 8, f"Expected width 8, got {ds.width}"
        assert ds.height == 8, f"Expected height 8, got {ds.height}"
