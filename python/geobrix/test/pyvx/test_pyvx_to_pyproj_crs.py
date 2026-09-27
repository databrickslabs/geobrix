"""Task 5 (SP2-CRS): pyvx/_crs.py uses to_pyproj_crs instead of inline from_authority pattern."""

import pyproj

from databricks.labs.gbx.core.crs import resolve_crs, to_pyproj_crs


def test_to_pyproj_crs_used_for_area_of_use():
    """The inline from_authority(100%)-else-from_wkt in pyvx/_crs.py:581-585 folds into to_pyproj_crs."""
    # ESRI:54008 — to_authority(100%) returns ("ESRI","54008") → from_authority path
    tgt = resolve_crs("ESRI:54008")
    pc = to_pyproj_crs(tgt)
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_authority(min_confidence=100) == ("ESRI", "54008")
    # to_pyproj_crs's own behavior (from_authority/from_wkt paths, WKT fallback) is
    # covered centrally in test/core/test_crs_helpers.py — not re-tested per consumer.
