"""``rst_asformat`` (``ops.as_format``): a driver that cannot encode a tile through rasterio's buffered writer is a clear
ValueError with a Fix, never the binding's raw SystemError or TypeError; JPEG 2000 keeps the CRS.
"""

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import ops


def _dataset():
    mf = MemoryFile()
    with mf.open(
        driver="GTiff",
        width=8,
        height=8,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=from_origin(10, 50, 0.1, 0.1),
    ) as dst:
        dst.write(np.arange(64, dtype="int16").reshape(1, 8, 8))
    return mf


@pytest.mark.parametrize("error", [SystemError("Unknown GDAL Error"), TypeError("x")])
def test_a_raw_binding_error_becomes_a_value_error_with_a_fix(monkeypatch, error):
    real_open = MemoryFile.open

    def fail(self, **kw):
        if kw.get("driver") == "PNG":
            raise error
        return real_open(self, **kw)

    with _dataset() as mf, mf.open() as ds:
        monkeypatch.setattr(MemoryFile, "open", fail)
        with pytest.raises(ValueError) as raised:
            ops.as_format(ds, "PNG")
    text = str(raised.value)
    assert "PNG" in text and "cannot encode a tile here" in text and "Fix:" in text


def test_jpeg_2000_keeps_the_crs():
    pytest.importorskip("rasterio")
    with _dataset() as mf, mf.open() as ds:
        try:
            out = ops.as_format(ds, "JP2OpenJPEG")
        except ValueError as exc:  # the build has no OpenJPEG
            pytest.skip(str(exc))
    with MemoryFile(out) as mf, rasterio.open(mf.name) as back:
        assert back.driver == "JP2OpenJPEG"
        assert back.crs is not None and back.crs.to_epsg() == 4326
