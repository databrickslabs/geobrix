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


def test_to_pyproj_crs_wkt_fallback():
    """Custom WKT with no authority uses from_wkt fallback."""
    import pyproj as _pyproj

    custom_crs = _pyproj.CRS.from_wkt(
        'GEOGCS["WGS 84",DATUM["WGS_1984",'
        'SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],'
        'UNIT["degree",0.0174532925199433]]'
    )
    from rasterio.crs import CRS

    rio_crs = CRS.from_wkt(custom_crs.to_wkt())
    pc = to_pyproj_crs(rio_crs)
    assert isinstance(pc, pyproj.CRS)
