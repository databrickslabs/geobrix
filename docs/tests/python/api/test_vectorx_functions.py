"""
Tests for VectorX Function Reference Examples

Validates heavy-tier (vectorx) Python examples: st_legacyaswkb and
the CRS family (st_crs, st_setcrs, st_transformcrs).
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import vectorx_functions


@pytest.fixture(scope="module")
def vectorx_heavy_setup(spark):
    """Register VectorX (full) and create setup views for heavy-tier CRS examples.

    The ``vector_geoms`` view (1 row: geom = 'SRID=4326;POINT (13 42)') is used
    by all three CRS heavy Python examples; this fixture creates it once per module.
    """
    from databricks.labs.gbx import manager as gbx_manager
    from ._fixtures import create_setup_views_vectorx_heavy  # noqa: PLC0415

    gbx_manager.register_family(spark, ('gbx_st_',))
    create_setup_views_vectorx_heavy(spark)
    yield spark


def test_vectorx_setup_example():
    """Common setup example exists and has output constant."""
    assert hasattr(vectorx_functions, "vectorx_setup_example")
    assert callable(vectorx_functions.vectorx_setup_example)
    assert hasattr(vectorx_functions, "vectorx_setup_example_output")


def test_st_legacyaswkb_sql_example_constant():
    """SQL example constant exists for docs."""
    assert hasattr(vectorx_functions, "ST_LEGACYASWKB_SQL_EXAMPLE")
    assert "gbx_st_legacyaswkb" in vectorx_functions.ST_LEGACYASWKB_SQL_EXAMPLE
    assert hasattr(vectorx_functions, "ST_LEGACYASWKB_SQL_EXAMPLE_output")


# ---------------------------------------------------------------------------
# T4: TIN family heavy-tier tests
# Fixture: ``tin_survey`` view — 1 row: pts ARRAY<BINARY> (4 POINT Z), bl empty.
# The 4 points form a 10×10 m square → 2 Delaunay triangles.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T3: Vector-tile family heavy-tier tests
# Fixtures: ``mvt_features`` view (for st_asmvt) and inline WGS-84 data
#            (for st_asmvt_pyramid) — both created/available via vectorx_heavy_setup.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T2: CRS family heavy-tier tests
# Fixture: ``vector_geoms`` view via ``vectorx_heavy_setup``.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# T5: Legacy Mosaic conversion heavy-tier tests
# Fixture: ``legacy_geoms`` view — 1 row: geom_legacy STRUCT (POINT(13, 42)).
# The view is created by create_setup_views_vectorx_heavy (via vectorx_heavy_setup).
# The example function registers vectorx.jts.legacy internally (idempotent).
# ---------------------------------------------------------------------------

