"""The viewshed row on the engine: the window a distance limit gives, and the edge of the DEM.

The rows of SPEC_ANALYSIS judge the masks; these two pin what a mask cannot say: where the window lies on the DEM's pixel grid,
and which pixel an observer near the edge stands in. They run where the engine does (the dev container).
"""

import math

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import run

pytestmark = pytest.mark.skipif(
    bool(run.unavailable_reason()), reason=str(run.unavailable_reason())
)
FN = "gbx_rst_viewshed"


def _flat_dem(n: int) -> bytes:
    """An n x n flat DEM, 1 m pixels, origin (0, n)."""
    profile = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=from_origin(0.0, float(n), 1.0, 1.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.zeros((1, n, n), dtype="float32"))
        return mf.read()


def _viewshed(n: int, x: float, y: float, height: float, limit):
    values = {
        "observer_geom": f"POINT({x} {y})",
        "observer_height": height,
        "target_height": 0.0,
        "max_distance": limit,
        "crs": None,
    }
    res = run.run(manifest.load().functions[FN], [_flat_dem(n)], values)
    assert res.error is None, res.error
    return res.raster


@pytest.mark.parametrize("n, r", [(200, 80.0), (1000, 200.0)])
def test_a_distance_limit_gives_the_window_around_the_observer(n, r):
    """The output is the window around the observer (side 2r + 1, its origin r + half a pixel off the observer), on the
    DEM's own pixel grid, holding a disc of pi r^2 visible pixels: not the DEM with the disc stretched across it.
    """
    ox, oy = n // 2 + 0.5, n // 2 - 0.5  # a pixel centre
    with MemoryFile(_viewshed(n, ox, oy, 10.0, r)) as mf, mf.open() as o:
        a = o.read(1)
        side = int(2 * r) + 1
        assert a.shape == (side, side)
        assert (o.transform.c, o.transform.f) == (ox - r - 0.5, oy + r + 0.5)
        assert (o.transform.a, o.transform.e) == (1.0, -1.0)
    disc = math.pi * r * r
    assert abs(int((a == 255).sum()) - disc) <= 0.01 * disc


def test_an_observer_between_the_edge_and_the_first_pixel_centre_stands_in_that_pixel():
    """GDAL takes the pixel the observer stands in, so an observer within half a pixel of the edge gets a real viewshed."""
    with MemoryFile(_viewshed(40, 0.2, 20.0, 5.0, None)) as mf, mf.open() as o:
        assert int((o.read(1) == 255).sum()) > 0
