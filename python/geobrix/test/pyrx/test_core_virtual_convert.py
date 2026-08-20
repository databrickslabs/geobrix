"""Tile-shape conversions: v1->v2 widen (lossless) and virtual->materialized
(heavy-useful). The virtual->materialized output must round-trip through the
raster-precedence path identically to reading the source window directly.
"""

import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _layouts
from .conftest import make_geotiff_bytes

WINDOW = (128, 64, 200, 300)


def test_from_v1_widens_losslessly():
    b = make_geotiff_bytes(width=4, height=3)
    t = VirtualTile.from_v1(cellid=7, raster=b, metadata={"driver": "GTiff"})
    assert t.raster == b
    assert t.path is None and t.window is None and t.clip_polygon is None
    assert t.clip_crs is None and t.crs is None
    assert not t.is_virtual()
    assert t.metadata == {"driver": "GTiff"}


def test_materialize_to_bytes_produces_heavy_useful_tile(tmp_path):
    path = _layouts.write_tiled_gtiff(str(tmp_path / "a.tif"), 512, 512, 256)
    virt = VirtualTile(cellid=8, path=path, window=WINDOW, metadata={"k": "v"})
    mat = ot.materialize_to_bytes(virt)
    # materialized: raster set, provenance preserved + tile_byte_size stamped
    assert mat.raster is not None and not mat.is_virtual()
    assert mat.path == path and mat.window == WINDOW
    assert mat.cellid == 8 and mat.metadata == {
        "k": "v",
        "tile_byte_size": str(len(mat.raster)),
    }
    # raster-precedence path yields exactly the window's pixels
    with ot.open_tile(mat) as ds:
        got = ds.read(1)
    with rasterio.open(path) as ds:
        full = ds.read(1)
    c, r, w, h = WINDOW
    assert np.array_equal(got, full[r : r + h, c : c + w])


def test_materialize_to_bytes_none_metadata(tmp_path):
    """materialize_to_bytes must not TypeError when tile.metadata is None."""
    path = _layouts.write_tiled_gtiff(str(tmp_path / "b.tif"), 512, 512, 256)
    virt = VirtualTile(cellid=9, path=path, window=WINDOW, metadata=None)
    mat = ot.materialize_to_bytes(virt)
    assert mat.raster is not None
    assert isinstance(mat.metadata, dict)
