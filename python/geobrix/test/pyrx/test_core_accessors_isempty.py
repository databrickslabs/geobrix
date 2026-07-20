"""isempty parity: an all-nodata raster is empty (matches heavy RasterAccessors.isEmpty)."""

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors


def _raster(data, nodata=-9999.0):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def test_isempty_all_nodata_is_empty():
    data = np.full((2, 2), -9999.0, dtype="float32")
    with _serde.open_tile(_raster(data)) as ds:
        assert accessors.isempty(ds) is True


def test_isempty_has_valid_pixels_is_not_empty():
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    with _serde.open_tile(_raster(data)) as ds:
        assert accessors.isempty(ds) is False
