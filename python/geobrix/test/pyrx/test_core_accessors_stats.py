"""Pure-function tests for pyrx Group-1 accessors & statistics.

Spark-free: open GTiff bytes with rasterio and call accessors.* directly,
mirroring the heavyweight RST_* accessor/statistics expressions. Stats are
computed over VALID pixels only (nodata sentinel -9999 excluded).
"""

import json

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors, coords

from .conftest import make_geotiff_bytes


def _ds(**kw):
    return _serde.open_tile(make_geotiff_bytes(**kw))


def _custom_raster(data, nodata=-9999.0, epsg=4326, origin=(10.0, 50.0), px=0.5):
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- stats correctness (hand-checkable, nodata excluded) --------------------
def test_stats_exclude_nodata():
    # 3x3 with one nodata sentinel; valid values are 0..7 (8 valid pixels).
    data = np.array(
        [[0.0, 1.0, 2.0], [3.0, 4.0, -9999.0], [5.0, 6.0, 7.0]], dtype="float32"
    )
    valid = np.array([0, 1, 2, 3, 4, 5, 6, 7], dtype="float32")
    raster = _custom_raster(data)
    with _serde.open_tile(raster) as ds:
        assert accessors.avg(ds) == [pytest.approx(float(valid.mean()))]
        assert accessors.minimum(ds) == [pytest.approx(0.0)]
        assert accessors.maximum(ds) == [pytest.approx(7.0)]
        assert accessors.median(ds) == [pytest.approx(float(np.median(valid)))]
        assert accessors.pixelcount(ds) == [8]


def test_stats_multiband():
    with _ds(width=4, height=3, count=2) as ds:
        # band data = arange(12); band2 = +100. No pixel equals -9999 -> all valid.
        avg = accessors.avg(ds)
        assert len(avg) == 2
        assert avg[0] == pytest.approx(np.arange(12).mean())
        assert avg[1] == pytest.approx((np.arange(12) + 100).mean())
        assert accessors.pixelcount(ds) == [12, 12]


def test_stats_all_invalid_band_is_null_zero():
    # A band that is entirely NoData has zero valid pixels: reducers must return
    # None (SQL NULL), never NaN or 0.0 (issue #59). pixelcount stays 0.
    data = np.full((2, 2), -9999.0, dtype="float32")
    raster = _custom_raster(data)
    with _serde.open_tile(raster) as ds:
        assert accessors.avg(ds) == [None]
        assert accessors.minimum(ds) == [None]
        assert accessors.maximum(ds) == [None]
        assert accessors.median(ds) == [None]
        assert accessors.pixelcount(ds) == [0]


def test_stats_genuine_zero_is_not_null():
    # A band of genuine 0.0 valid pixels must return 0.0, not None — the
    # zero-not-null trap. nodata is a sentinel that no pixel equals.
    data = np.zeros((2, 2), dtype="float32")
    raster = _custom_raster(data)  # nodata defaults to -9999.0, unmatched
    with _serde.open_tile(raster) as ds:
        assert accessors.avg(ds) == [pytest.approx(0.0)]
        assert accessors.minimum(ds) == [pytest.approx(0.0)]
        assert accessors.maximum(ds) == [pytest.approx(0.0)]
        assert accessors.median(ds) == [pytest.approx(0.0)]
        assert accessors.pixelcount(ds) == [4]


# --- geotransform-derived accessors -----------------------------------------
def test_rotation_skew_georeference():
    with _ds() as ds:
        # north-up: scaleX=0.5, scaleY=-0.5, skews 0 -> rotation 0.
        assert accessors.rotation(ds) == pytest.approx(0.0)
        assert accessors.skewx(ds) == pytest.approx(0.0)
        assert accessors.skewy(ds) == pytest.approx(0.0)
        gr = accessors.georeference(ds)
        assert set(gr.keys()) == {
            "upperLeftX",
            "upperLeftY",
            "scaleX",
            "scaleY",
            "skewX",
            "skewY",
        }
        assert gr["upperLeftX"] == pytest.approx(10.0)
        assert gr["upperLeftY"] == pytest.approx(50.0)
        assert gr["scaleX"] == pytest.approx(0.5)
        assert gr["scaleY"] == pytest.approx(-0.5)
        assert gr["skewX"] == pytest.approx(0.0)
        assert gr["skewY"] == pytest.approx(0.0)


def test_format():
    with _ds() as ds:
        assert accessors.format(ds) == "GTiff"


def test_bandmetadata_returns_dict():
    with _ds() as ds:
        md = accessors.bandmetadata(ds, 1)
        assert isinstance(md, dict)
        assert all(isinstance(k, str) and isinstance(v, str) for k, v in md.items())


def test_subdatasets_empty_for_plain_gtiff():
    with _ds() as ds:
        assert accessors.subdatasets(ds) == {}


def test_getsubdataset_no_match_raises():
    with _ds() as ds:
        with pytest.raises(ValueError):
            accessors.getsubdataset(ds, "doesnotexist")


# --- summary ----------------------------------------------------------------
def test_summary_is_valid_json_with_band_stats():
    with _ds(width=4, height=3, count=2) as ds:
        s = accessors.summary(ds)
        obj = json.loads(s)
        assert obj["driverShortName"] == "GTiff"
        assert obj["size"] == [4, 3]
        assert len(obj["bands"]) == 2
        b0 = obj["bands"][0]
        assert b0["min"] == pytest.approx(0.0)
        assert b0["max"] == pytest.approx(11.0)


# --- histogram --------------------------------------------------------------
def test_histogram_bucket_sum_equals_valid_pixels():
    data = np.array(
        [[0.0, 1.0, 2.0], [3.0, 4.0, -9999.0], [5.0, 6.0, 7.0]], dtype="float32"
    )
    raster = _custom_raster(data)
    with _serde.open_tile(raster) as ds:
        hist = accessors.histogram(ds, 4, 0.0, 7.0, False)
        assert list(hist.keys()) == ["band_1"]
        counts = hist["band_1"]
        assert len(counts) == 4
        assert sum(counts) == 8  # 8 valid pixels, all within [0,7]


def test_histogram_default_range_and_buckets():
    with _ds(width=4, height=3, count=1) as ds:
        hist = accessors.histogram(ds)  # 256 buckets, derived range
        assert list(hist.keys()) == ["band_1"]
        assert len(hist["band_1"]) == 256
        assert sum(hist["band_1"]) == 12


def test_histogram_n_buckets_validation():
    with _ds() as ds:
        with pytest.raises(ValueError):
            accessors.histogram(ds, 0)


# --- struct coords round-trip -----------------------------------------------
def test_coord_structs_roundtrip():
    with _ds() as ds:
        # world coord of pixel (col=2, row=1)
        wc = coords.raster_to_world_coord(ds, 2, 1)
        assert set(wc.keys()) == {"x", "y"}
        rc = coords.world_to_raster_coord(ds, wc["x"], wc["y"])
        assert (rc["x"], rc["y"]) == (2, 1)
