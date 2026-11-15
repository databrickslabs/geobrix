"""The value a focal filter shows where a window holds nothing valid, when the raster declares no NoData.

An alpha layout declares none: the transparent pixel stays transparent, and its value is the type's own fallback,
which a table row cannot read (it judges valid pixels only).
"""

import numpy as np
import pytest
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import focal

from . import _alpha_layouts as al


def _blank_value(op):
    """A UInt8 gray + alpha raster with one transparent pixel, filtered over a 1 x 1 window: that pixel's value."""
    data = np.full((2, 6, 6), 7, dtype="uint8")
    data[-1] = 255
    data[-1, 2, 3] = 0
    raw = al.layout_bytes("gray_alpha", data=data, transform=from_origin(0, 6, 1, 1))
    with _serde.open_tile(raw) as ds:
        out = focal.filt(ds, 1, op)
    with _serde.open_tile(out) as o:
        band = o.read(1).astype("float64")
    assert (np.delete(band.ravel(), 2 * 6 + 3) == 7).all()
    return band[2, 3]


def test_a_mean_shows_minus_9999_in_an_empty_window():
    # a mean is Float32 whatever the input type, so -9999 fits and a true mean of 0 stays distinct
    data = np.zeros((2, 6, 6), dtype="uint8")
    data[-1] = 255
    data[-1, 2, 3] = 0
    raw = al.layout_bytes("gray_alpha", data=data, transform=from_origin(0, 6, 1, 1))
    with _serde.open_tile(raw) as ds:
        out = focal.filt(ds, 1, "mean")
    with _serde.open_tile(out) as o:
        assert o.read(1)[2, 3] == -9999.0


@pytest.mark.parametrize("op", ["min", "max", "median"])
def test_an_order_statistic_of_a_uint8_raster_shows_the_type_fallback(op):
    # -9999 is not a UInt8 value: the empty window shows the fallback 0, not an error
    assert _blank_value(op) == 0
