"""H3 covering at resolution 9 over a 64 x 64 raster: the cells are exactly the h3 overlap set (about 8,600 cells, 25 s).

A perf-set case: the (grid x assignment x coverage) table in test_core_tessellate.py holds the same definition on
smaller rasters.
"""

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate as T

pytestmark = pytest.mark.perf


def _tile_4326(size=64, res_deg=0.01, origin=(-0.1, 51.5)):
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(
            origin[0], origin[1], res_deg, res_deg
        ),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def test_covering_mode_is_overlap_set():
    """covering mode cell ids must exactly match h3.polygon_to_cells_experimental overlap set."""
    import h3
    from shapely.geometry import box

    tile = _tile_4326()
    with _serde.open_tile(tile) as ds:
        cells = {
            cellid
            for cellid, _ in T.iter_tessellate_h3(ds, resolution=9, mode="covering")
        }

    # tile extent: from_origin(-0.1, 51.5, 0.01, 0.01) → west=-0.1, north=51.5, 64px at 0.01deg
    minx, maxx = -0.1, -0.1 + 64 * 0.01
    miny, maxy = 51.5 - 64 * 0.01, 51.5
    shp = h3.geo_to_h3shape(box(minx, miny, maxx, maxy).__geo_interface__)
    oracle = {
        T._h3_str_to_signed_int64(c)
        for c in h3.polygon_to_cells_experimental(shp, 9, contain="overlap")
    }
    assert cells == oracle
