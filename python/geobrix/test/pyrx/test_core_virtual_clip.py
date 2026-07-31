"""clip_dataset: clip_crs precedence, per-tile intersection, disjoint->None."""
import numpy as np
import shapely
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip

from .conftest import make_geotiff_bytes


def _open(b):
    return _serde.open_tile(b)


def test_clip_full_cover_returns_all_pixels():
    # 4x3 raster over extent x[10,12] y[48.5,50] (0.5 deg pixels).
    poly = box(10.0, 48.0, 12.0, 50.0)  # fully covers
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs="EPSG:4326")
    assert out is not None
    with _open(out) as ds2:
        assert ds2.width == 4 and ds2.height == 3


def test_clip_partial_returns_slice():
    # Cover only the left half in X.
    poly = box(10.0, 48.0, 11.0, 50.0)
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs=None)  # plain WKB -> assume raster CRS
    assert out is not None
    with _open(out) as ds2:
        assert ds2.width < 4  # clipped narrower than source


def test_clip_disjoint_returns_none():
    poly = box(100.0, 10.0, 101.0, 11.0)  # far away
    wkb = shapely.wkb.dumps(poly)
    with _open(make_geotiff_bytes(width=4, height=3, epsg=4326)) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs=None)
    assert out is None


def test_clip_crs_overrides_and_reprojects():
    # UTM raster; polygon given in lon/lat via explicit clip_crs must reproject
    # and clip successfully (not raise "do not overlap").
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin
    from rasterio.warp import transform_bounds

    tr = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    prof = dict(driver="GTiff", width=8, height=8, count=1, dtype="float32",
                crs="EPSG:32633", transform=tr, nodata=-9999.0)
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.arange(64, dtype="float32").reshape(8, 8), 1)
        utm_bytes = mf.read()

    minx, miny, maxx, maxy = transform_bounds(
        "EPSG:32633", "EPSG:4326", 500000, 4999200, 500800, 5000000
    )
    poly = box(minx, miny, maxx, maxy)
    wkb = shapely.wkb.dumps(poly)  # plain WKB (no SRID)
    with _open(utm_bytes) as ds:
        out = _clip.clip_dataset(ds, wkb, clip_crs="EPSG:4326")
    assert out is not None
    with _open(out) as ds2:
        assert ds2.crs.to_epsg() == 32633  # raster CRS unchanged; polygon moved
