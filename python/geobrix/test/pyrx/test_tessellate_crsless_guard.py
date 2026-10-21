"""CF3 guard: CRS-less raster must NOT raise in iter_tessellate.

Before the guard, both centroid+complete and covering+complete called
    transform_bounds(ds.crs, _WGS84, *ds.bounds)
with ds.crs=None, which raises. The guard uses ds.bounds directly (grid-native)
when ds.crs is None.

Sites fixed:
  - _centroid_complete  (~L344): centroid+complete path
  - iter_tessellate covering (~L441): covering+complete path
  - _assign_pixels: pixel centres are reprojected (``to_lonlat``) only when the
    raster has a CRS that is not EPSG:4326 (centroid+sparse/complete)
"""

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate


def _make_crsless_tile(lon=-0.10, lat=51.50, size=4, res_deg=0.01):
    """Small raster with NO CRS (crs=None), coords in WGS84-ish range."""
    data = np.ones((size, size), dtype="float32") * 100.0
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs=None,
        transform=rasterio.transform.from_origin(lon, lat, res_deg, res_deg),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


@pytest.fixture
def crsless_ds():
    tile = _make_crsless_tile()
    with MemoryFile(tile) as mf:
        with mf.open() as ds:
            yield ds


def test_centroid_complete_crsless_does_not_raise(tmp_path, crsless_ds):
    """centroid+complete on CRS-less tile must not raise (CF3 guard at L344).

    transform_bounds(None, _WGS84, ...) previously raised here.
    """
    chips = list(
        iter_tessellate(
            crsless_ds,
            resolution=5,
            grid="quadbin",
            mode="centroid",
            coverage="complete",
        )
    )
    assert chips, "expected >= 1 chip from centroid+complete on a CRS-less tile"


def test_covering_complete_crsless_does_not_raise(tmp_path, crsless_ds):
    """covering+complete on CRS-less tile must not raise (CF3 guard at L441).

    transform_bounds(None, _WGS84, ...) previously raised here.
    """
    chips = list(
        iter_tessellate(
            crsless_ds,
            resolution=5,
            grid="quadbin",
            mode="covering",
            coverage="complete",
        )
    )
    assert chips, "expected >= 1 chip from covering+complete on a CRS-less tile"


def test_centroid_sparse_crsless_does_not_raise(crsless_ds):
    """centroid+sparse on CRS-less tile must not raise (_assign_pixels guard)."""
    chips = list(
        iter_tessellate(
            crsless_ds, resolution=5, grid="quadbin", mode="centroid", coverage="sparse"
        )
    )
    assert isinstance(chips, list)


def test_covering_sparse_crsless_does_not_raise(crsless_ds):
    """covering+sparse on CRS-less tile must not raise."""
    chips = list(
        iter_tessellate(
            crsless_ds, resolution=5, grid="quadbin", mode="covering", coverage="sparse"
        )
    )
    assert isinstance(chips, list)
