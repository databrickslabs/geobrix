"""Task 3 (SP2-CRS): round-trip regression for writers: _write_lidar, cog_writer, gridagg, vector, _write_netcdf."""
import pytest
from rasterio.crs import CRS

from databricks.labs.gbx.core.crs import (
    authority_srid_of,
    crs_equal,
    crs_to_canonical,
    crs_to_proj4,
    resolve_crs,
)

ROUNDTRIP_CASES = [
    ("EPSG:4326", "EPSG:4326"),
    ("ESRI:54008", "ESRI:54008"),
    ("OGC:CRS84", "OGC:CRS84"),
]


@pytest.mark.parametrize("src,expected", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"])
def test_write_lidar_inferred_crs_preserves_authority(src, expected):
    """Simulate ds/_write_lidar.py:147-150: laspy parse_crs → crs_to_canonical."""
    mock_parsed = resolve_crs(src)
    # --- old code ---
    # _epsg = mock_parsed.to_epsg()
    # crs = _epsg if _epsg is not None else mock_parsed.to_wkt()  # ESRI → WKT (authority lost)
    # --- new code ---
    crs = crs_to_canonical(resolve_crs(mock_parsed.to_wkt()))
    assert crs == expected, f"Expected {expected!r}, got {crs!r}"


@pytest.mark.parametrize("src,expected", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"])
def test_cog_writer_vrt_srs_preserves_authority(src, expected):
    """Simulate ds/cog_writer.py:320: crs.to_wkt() → crs_to_canonical(crs)."""
    crs_obj = resolve_crs(src)
    # --- old code: crs_obj.to_wkt() drops ESRI authority ---
    # --- new code ---
    srs_text = crs_to_canonical(crs_obj)
    assert srs_text == expected, f"Expected {expected!r}, got {srs_text!r}"


@pytest.mark.parametrize("srid", [3857, 4326, 27700])
def test_cog_writer_tiling_constants_are_behavior_preserving(srid):
    """cog_writer.py:1188,1378,1597: CRS.from_epsg(n) → resolve_crs(n) for known EPSG."""
    from rasterio.crs import CRS

    old_crs = CRS.from_epsg(srid)
    new_crs = resolve_crs(srid)
    assert crs_equal(old_crs, new_crs)


@pytest.mark.parametrize("src,expected", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"])
def test_vector_crs_to_srid_proj_authority_code(src, expected):
    """Simulate ds/vector.py:182-198 (_crs_to_srid_proj): srid string is the authority code, not 0."""
    crs_obj = resolve_crs(src)
    # authority_srid_of returns int for EPSG/ESRI, None for OGC
    srid_int = authority_srid_of(crs_obj)
    proj4_str = crs_to_proj4(crs_obj)
    # EPSG and ESRI must yield a numeric srid; OGC:CRS84 has no integer srid
    if src in ("EPSG:4326", "ESRI:54008"):
        assert srid_int is not None
        assert str(srid_int) != "0"
    else:
        assert srid_int is None  # OGC:CRS84 → no integer SRID
    assert proj4_str is not None
    assert "+proj=" in proj4_str


@pytest.mark.parametrize("src,expected", ROUNDTRIP_CASES, ids=["epsg", "esri", "ogc"])
def test_netcdf_writer_epsg_extraction(src, expected):
    """Simulate ds/_write_netcdf.py:67-71,206,217,617: authority_srid_of instead of to_epsg()."""
    crs_obj = resolve_crs(src)
    epsg = authority_srid_of(crs_obj)
    if src == "EPSG:4326":
        assert epsg == 4326
    elif src == "ESRI:54008":
        assert epsg == 54008   # was None under old to_epsg()
    else:  # OGC:CRS84
        assert epsg is None    # non-integer authority code — correct
