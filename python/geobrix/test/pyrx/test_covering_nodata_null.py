"""Covering over a raster with an interior nodata hole: every all-nodata cell
must reduce to None (not NaN), and cells with data keep their value."""
import math
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate, accessors


def _raster_with_hole(nodata=-9999.0):
    data = np.full((9, 9), 42.0, dtype="float32")
    data[3:6, 3:6] = nodata  # interior hole
    with MemoryFile() as mf:
        with mf.open(driver="GTiff", width=9, height=9, count=1, dtype="float32",
                     crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.0022, 0.0022),
                     nodata=nodata) as dst:
            dst.write(data, 1)
        return mf.read()


def test_covering_empty_cells_reduce_to_none():
    tif = _raster_with_hole()
    empties = 0
    with _serde.open_tile(tif) as ds:
        chips = list(tessellate.iter_tessellate_h3(ds, 10, mode="covering"))
    assert len(chips) > 0
    for _cellid, rb in chips:
        with _serde.open_tile(rb) as cds:
            mx = accessors.maximum(cds)[0]
            pc = accessors.pixelcount(cds)[0]
        if pc == 0:
            empties += 1
            assert mx is None                      # NULL, not NaN
            assert not (isinstance(mx, float) and math.isnan(mx))
        else:
            assert mx is not None and mx == 42.0   # real cells unaffected
    assert empties > 0   # the hole DOES create empty cells (bug would be masked otherwise)
