import rasterio
from shapely.geometry import box

from databricks.labs.gbx.core.crs import area_m2


def test_area_m2_geographic_is_metric():
    crs = rasterio.crs.CRS.from_epsg(
        4326
    )  # test-only construction; not production code
    poly = box(-77.968, 43.233, -77.968 + 3e-4, 43.233 + 3e-4)
    assert poly.area < 1e-6  # raw degrees: tiny
    assert 500 < area_m2(poly, crs) < 1500  # metric: hundreds of m² at lat 43


def test_area_m2_projected_is_planar():
    crs = rasterio.crs.CRS.from_epsg(32633)
    assert area_m2(box(0, 0, 10, 10), crs) == 100.0
