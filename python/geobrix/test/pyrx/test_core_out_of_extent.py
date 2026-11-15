"""Out-of-extent robustness of the by-design constructors (light tier).

The constructors (rasterize /
gridfrompoints / dtmfromgeoms) take a target SRID and assume the geom is already in it (matching heavy, which does NOT
reproject those), and degrade gracefully (empty / NoData tile) when the geom falls outside the target extent. The
clip and sample cases of this file are rows of the raster oracle tables now.
"""

import numpy as np
import shapely
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features
from databricks.labs.gbx.pyrx.core import tin as tin_core


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
