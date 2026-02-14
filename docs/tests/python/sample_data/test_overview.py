"""
Tests for Overview page code examples (constants = payload only in docs).

Documentation: docs/docs/sample-data/overview.mdx
Run: pytest docs/tests/python/sample_data/test_overview.py -v
"""
import pytest
from . import overview


@pytest.mark.structure
def test_geographic_coherence_nyc_zonal():
    assert isinstance(overview.GEOGRAPHIC_COHERENCE_NYC_ZONAL, str)
    assert "sample_path" in overview.GEOGRAPHIC_COHERENCE_NYC_ZONAL
    assert "geojson_ogr" in overview.GEOGRAPHIC_COHERENCE_NYC_ZONAL
    assert "rst_avg" in overview.GEOGRAPHIC_COHERENCE_NYC_ZONAL


@pytest.mark.structure
def test_geographic_coherence_nyc_clip():
    assert isinstance(overview.GEOGRAPHIC_COHERENCE_NYC_CLIP, str)
    assert "sample_path" in overview.GEOGRAPHIC_COHERENCE_NYC_CLIP
    assert "rst_clip" in overview.GEOGRAPHIC_COHERENCE_NYC_CLIP


@pytest.mark.structure
def test_geographic_coherence_nyc_multiscale():
    assert isinstance(overview.GEOGRAPHIC_COHERENCE_NYC_MULTISCALE, str)
    assert "sample_path" in overview.GEOGRAPHIC_COHERENCE_NYC_MULTISCALE
    assert "spatial_within" in overview.GEOGRAPHIC_COHERENCE_NYC_MULTISCALE


@pytest.mark.structure
def test_geographic_coherence_london_bng():
    assert isinstance(overview.GEOGRAPHIC_COHERENCE_LONDON_BNG, str)
    assert "sample_path" in overview.GEOGRAPHIC_COHERENCE_LONDON_BNG
    assert "bng_polyfill" in overview.GEOGRAPHIC_COHERENCE_LONDON_BNG
    assert "bng_tessellate" in overview.GEOGRAPHIC_COHERENCE_LONDON_BNG
