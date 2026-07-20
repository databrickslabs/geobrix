"""Issue #59 regression: an all-nodata chip's reducer must not poison MAX() in a
seam-reconciliation GROUP BY. Pure-accessor level (Spark-free) — asserts that the
empty-band reducer yields None and that None is ignored by a max() over a group
where a real value also exists, whereas the old NaN would win.
"""

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


def test_all_nodata_chip_does_not_poison_group_max():
    # One H3 cell reconciled across two chips at a tile seam: a real-data chip
    # (max 42.0) and an all-nodata chip (empty -> None). MAX over the group must
    # be 42.0, not None and not NaN.
    real = _raster(np.array([[42.0, 1.0], [2.0, 3.0]], dtype="float32"))
    empty = _raster(np.full((2, 2), -9999.0, dtype="float32"))

    with _serde.open_tile(real) as r, _serde.open_tile(empty) as e:
        real_max = accessors.maximum(r)[0]
        empty_max = accessors.maximum(e)[0]

    assert empty_max is None
    # SQL MAX() ignores NULL: emulate the reconciliation with a NULL-skipping max.
    group = [v for v in (real_max, empty_max) if v is not None]
    assert max(group) == 42.0
