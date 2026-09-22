"""Tests for the light-tier percentile-stretch core (rst_percentile_stretch).

Per-band percentile contrast stretch to uint8, NoData preserved — the persisted
data-engineering transform behind ``gbx_rst_percentile_stretch`` / the orthomosaic
example's colour correction. Session-free (numpy/rasterio only).
"""

import numpy as np
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import stretch


def _two_band_tif(nodata_zero=True):
    """A 2-band uint16 raster with distinct per-band ranges + one NoData pixel."""
    b1 = np.linspace(100, 200, 100, dtype="uint16").reshape(10, 10)
    b2 = np.linspace(1000, 2000, 100, dtype="uint16").reshape(10, 10)
    if nodata_zero:
        b1[0, 0] = 0
        b2[0, 0] = 0
    mf = MemoryFile()
    with mf.open(driver="GTiff", height=10, width=10, count=2, dtype="uint16", nodata=0) as ds:
        ds.write(b1, 1)
        ds.write(b2, 2)
    return mf


def _read(out_bytes):
    with MemoryFile(out_bytes) as mf, mf.open() as r:
        return r.read(), r.dtypes


def test_percentile_stretch_outputs_uint8_full_range_per_band():
    with _two_band_tif() as mf, mf.open() as ds:
        out = stretch.percentile_stretch(ds, 2.0, 98.0)
    arr, dtypes = _read(out)
    assert all(dt == "uint8" for dt in dtypes)
    assert arr.shape == (2, 10, 10)
    # each band stretched independently toward the full 0..255 range
    assert arr[0].max() >= 200 and arr[1].max() >= 200


def test_percentile_stretch_preserves_nodata():
    with _two_band_tif() as mf, mf.open() as ds:
        out = stretch.percentile_stretch(ds, 2.0, 98.0)
    arr, _ = _read(out)
    # the NoData pixel (0 in the source) stays 0 in every band
    assert arr[0, 0, 0] == 0 and arr[1, 0, 0] == 0
