"""Out-of-extent robustness of the viewshed observer and the by-design constructors (light tier).

A viewshed observer far outside the DEM gives an all-0 tile, not a crash; the constructors (rasterize /
gridfrompoints / dtmfromgeoms) take a target SRID and assume the geom is already in it (matching heavy, which does NOT
reproject those), and degrade gracefully (empty / NoData tile) when the geom falls outside the target extent. The
clip and sample cases of this file are rows of the raster oracle tables now.
"""

import numpy as np
import shapely
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import analysis, features
from databricks.labs.gbx.pyrx.core import tin as tin_core


def _utm_geotiff_bytes(width=8, height=8, count=1):
    """In-memory GTiff in EPSG:32633 (UTM 33N). Origin (500000, 5000000), 100m px."""
    transform = from_origin(500000.0, 5000000.0, 100.0, 100.0)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs="EPSG:32633",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(count * width * height, dtype="float32").reshape(
        count, height, width
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


# --- viewshed: reproject + graceful out-of-bounds --------------------------
def test_viewshed_observer_outside_extent_returns_all_invisible():
    """An observer far outside the DEM returns an all-0 tile, not a crash."""
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        out = analysis.viewshed(ds, 0.0, 0.0, 1.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert arr.dtype == np.uint8
        assert int(arr.max()) == 0  # nothing visible from off-raster observer


def test_viewshed_in_bounds_observer_still_produces_visibility():
    """A regression guard: an in-extent observer still yields some visible cells."""
    with _serde.open_tile(_utm_geotiff_bytes()) as ds:
        # observer near the raster center, in UTM CRS
        out = analysis.viewshed(ds, 500400.0, 4999600.0, 5.0, 0.0, None)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert int(arr.max()) == 255  # at least the observer cell is visible


# --- by-design constructors: graceful out-of-extent ------------------------
def test_rasterize_geom_outside_extent_is_all_nodata():
    """rasterize a geom that misses the target extent -> all-NoData tile, no crash."""
    geom_wkb = shapely.wkb.dumps(box(1000.0, 1000.0, 1010.0, 1010.0))
    out = features.rasterize_geom(geom_wkb, 42.0, 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert np.all(arr == -9999.0)  # geom never overlaps -> all NoData


def test_gridfrompoints_points_outside_extent_no_crash():
    """IDW grid still produces a tile when all points are far outside the extent."""
    pts = [(5000.0, 5000.0), (5010.0, 5010.0)]
    out = tin_core.idw_grid(pts, [1.0, 2.0], 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        assert o.width == 8 and o.height == 8  # well-formed tile, no crash


def test_dtmfromgeoms_points_outside_extent_is_all_nodata():
    """DTM cells outside the points' convex hull -> NoData; off-extent -> all NoData."""
    pts_xyz = np.array(
        [[5000.0, 5000.0, 1.0], [5100.0, 5000.0, 2.0], [5050.0, 5100.0, 3.0]]
    )
    out = tin_core.delaunay_dtm(pts_xyz, None, 0.0, 0.0, 100.0, 100.0, 8, 8, 32633)
    with _serde.open_tile(out) as o:
        arr = o.read(1)
        assert np.all(arr == -9999.0)  # whole grid is outside the hull
