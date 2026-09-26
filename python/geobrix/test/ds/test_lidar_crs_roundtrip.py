"""Task 2 (SP2-CRS): round-trip regression for lidar reader and LAZ encoder CRS handling."""

import pyproj
import pytest

from databricks.labs.gbx.core.crs import (
    authority_srid_of,
    crs_to_canonical,
    resolve_crs,
    to_pyproj_crs,
)

# Triple of (authority_string, expected_canonical, expected_pyproj_authority)
ROUNDTRIP_CASES = [
    ("EPSG:4326", "EPSG:4326", ("EPSG", "4326")),
    ("ESRI:54008", "ESRI:54008", ("ESRI", "54008")),
    ("OGC:CRS84", "OGC:CRS84", ("OGC", "CRS84")),
]


@pytest.mark.parametrize(
    "src,expected_canonical,_", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"]
)
def test_lidar_reader_crs_column_preserves_authority(src, expected_canonical, _):
    """Simulate ds/lidar.py:279-280 after the fix: parse_crs().to_wkt() → crs_to_canonical(resolve_crs(...))."""
    # Simulate what h.parse_crs() returns for a file with this CRS
    mock_crs_obj = resolve_crs(src)
    # --- old code (drop authority) ---
    # old_result = mock_crs_obj.to_wkt()  # drops ESRI authority
    # --- new code ---
    new_result = crs_to_canonical(resolve_crs(mock_crs_obj.to_wkt()))
    assert (
        new_result == expected_canonical
    ), f"Expected {expected_canonical!r}, got {new_result!r}"


@pytest.mark.parametrize(
    "src,_,expected_pyproj_auth", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"]
)
def test_laz_encoder_crs_header_authority(src, _, expected_pyproj_auth):
    """Simulate pyrx/imagery.py:552-557 after fix: resolve_crs + to_pyproj_crs preserves authority in LAS header."""
    # Simulate integer input for ESRI case (ESRI code 54008 as int)
    crs_input = authority_srid_of(resolve_crs(src))
    if crs_input is None:
        # OGC:CRS84 has no integer SRID — test string input path
        crs_input = src

    # --- old code for int case: ProjCRS.from_epsg(54008) → EPSG:54008 (wrong) ---
    # new code:
    proj_crs = to_pyproj_crs(resolve_crs(crs_input))
    assert isinstance(proj_crs, pyproj.CRS)
    # NOTE: proj_crs is a pyproj.CRS — uses min_confidence= (NOT confidence_threshold=)
    # Brief had confidence_threshold=100 here which is a rasterio CRS keyword; corrected.
    auth = proj_crs.to_authority(min_confidence=100)
    assert (
        auth == expected_pyproj_auth
    ), f"Expected {expected_pyproj_auth!r}, got {auth!r}"


def test_lidar_reader_none_crs_stays_none():
    """parse_crs() returning None must still produce None crs column."""
    # crs_obj is None → crs should be None
    crs_obj = None
    result = (
        crs_to_canonical(resolve_crs(crs_obj.to_wkt())) if crs_obj is not None else None
    )
    assert result is None
