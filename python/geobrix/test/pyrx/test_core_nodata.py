import math

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _nodata as nd


def _tile(arr, nodata):
    profile = dict(
        driver="GTiff",
        width=arr.shape[1],
        height=arr.shape[0],
        count=1,
        dtype="float32",
        crs=rasterio.crs.CRS.from_epsg(4326),
        transform=rasterio.transform.from_origin(0, 0, 1, 1),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr.astype("float32"), 1)
        return mf.read()


def test_propagate_invalid_border_ring_only_when_all_valid():
    valid = np.ones((7, 7), dtype=bool)
    inv = nd.propagate_invalid(valid)
    assert inv[0, :].all() and inv[-1, :].all() and inv[:, 0].all() and inv[:, -1].all()
    assert not inv[1:-1, 1:-1].any()


def test_propagate_invalid_spreads_one_invalid_through_3x3():
    valid = np.ones((7, 7), dtype=bool)
    valid[3, 3] = False
    inv = nd.propagate_invalid(valid)
    assert inv[2:5, 2:5].all()
    assert not inv[3, 1]


def test_read_masked_respects_declared_nodata():
    arr = np.array([[1.0, 2.0], [-9999.0, 4.0]], dtype="float32")
    with _serde.open_tile(_tile(arr, -9999.0)) as ds:
        data, valid = nd.read_masked(ds, 1)
    assert valid[0, 0] and valid[0, 1] and valid[1, 1]
    assert not valid[1, 0]
    assert data.dtype == np.float64


def test_read_masked_all_valid_when_no_declared_nodata():
    arr = np.array([[1.0, -9999.0], [3.0, 4.0]], dtype="float32")
    with _serde.open_tile(_tile(arr, None)) as ds:
        _, valid = nd.read_masked(ds, 1)
    assert valid.all()


def test_emit_writes_nodata_at_invalid_and_nonfinite():
    arr = np.array([[1.0, 2.0], [3.0, np.inf]], dtype="float64")
    invalid = np.array([[False, True], [False, False]], dtype=bool)
    with _serde.open_tile(_tile(np.zeros((2, 2), "float32"), -9999.0)) as ds:
        out = nd.emit(ds, arr, -9999.0, invalid, "float32")
    with _serde.open_tile(out) as o:
        r = o.read(1)
        assert o.nodata == -9999.0
        assert r[0, 1] == -9999.0
        assert r[1, 1] == -9999.0
        assert r[0, 0] == 1.0


@pytest.mark.parametrize(
    "dtype,expected",
    [
        ("uint8", 0.0),
        ("int8", 0.0),
        ("uint16", 65535.0),
        ("int16", -32768.0),
        ("uint32", 4294967294.0),
        ("int32", -2147483648.0),
        ("float32", float(np.finfo(np.float32).min)),
        ("float64", float(np.finfo(np.float64).min)),
        ("int64", 0.0),
    ],
)
def test_fallback_nodata_matches_heavy_getNoDataConstant(dtype, expected):
    v = nd.fallback_nodata(dtype)
    assert v == expected and not math.isnan(v)
    assert nd.nodata_fits_dtype(v, dtype)


def _mask_kind_tile(kind):
    """4x4 GTiff: ``plain``, ``masked`` (internal per-dataset mask), ``masked-nodata``
    (the mask and NoData 7), or ``rgba`` (an alpha band)."""
    rgba = kind == "rgba"
    profile = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=4 if rgba else 1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(10, 50, 0.5, 0.5),
        nodata=7 if kind == "masked-nodata" else None,
    )
    if rgba:
        profile.update(photometric="RGB", alpha="YES")
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(np.full((profile["count"], 4, 4), 9, dtype="uint8"))
                if kind.startswith("masked"):
                    dst.write_mask(np.eye(4, dtype="uint8") * 255)
            return mf.read()


@pytest.mark.parametrize(
    "kind,alpha,own_mask",
    [
        ("plain", False, False),
        ("masked", False, True),
        ("masked-nodata", False, True),
        ("rgba", True, False),
    ],
)
def test_has_alpha_and_has_own_mask(kind, alpha, own_mask):
    with MemoryFile(_mask_kind_tile(kind)) as mf, mf.open() as ds:
        assert nd.has_alpha(ds) is alpha
        assert nd.has_own_mask(ds) is own_mask
