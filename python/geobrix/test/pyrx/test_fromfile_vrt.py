"""rst_fromfile(<a .vrt>, materialize=True): a VRT is rendered into a GTiff tile, or the answer is an error tile
with the reason. It is never a silent NULL (the error-tile contract); the virtual default is unchanged.

The heavy tier's ``rasterx.rst_fromfile`` is the same pyrx function with materialize forced on, so one body serves
both tiers; the Column-API rows below run it through Spark as each tier's surface does.
"""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.rasterx import functions as rx

pytestmark = pytest.mark.filterwarnings("ignore")


def _source_tif(path, w=16, h=12):
    data = (np.arange(w * h, dtype="float32").reshape(1, h, w) % 31).astype("float32")
    with rasterio.open(
        path, "w", driver="GTiff", width=w, height=h, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5),
    ) as dst:  # fmt: skip
        dst.write(data)
    return data


def _vrt_over(tif, vrt):
    # A one-source VRT written as XML from the source's own metadata: no osgeo needed, so the
    # light CI shards (rasterio only) run this file too.
    with rasterio.open(tif) as src:
        w, h, gt = src.width, src.height, src.transform.to_gdal()
        dtype = {"float32": "Float32", "float64": "Float64", "uint8": "Byte", "int16": "Int16",
                 "uint16": "UInt16", "int32": "Int32"}[src.dtypes[0]]  # fmt: skip
        bands = "".join(
            f'<VRTRasterBand dataType="{dtype}" band="{b}"><SimpleSource>'
            f'<SourceFilename relativeToVRT="0">{tif}</SourceFilename><SourceBand>{b}</SourceBand>'
            f'<SrcRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/>'
            f'<DstRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/></SimpleSource></VRTRasterBand>'
            for b in range(1, src.count + 1)
        )
        srs = f"<SRS>{src.crs.to_wkt()}</SRS>" if src.crs else ""
    gt_txt = ", ".join(repr(v) for v in gt)
    with open(vrt, "w") as fh:
        fh.write(
            f'<VRTDataset rasterXSize="{w}" rasterYSize="{h}">{srs}<GeoTransform>{gt_txt}</GeoTransform>{bands}</VRTDataset>'
        )
    return vrt


def _error_of(row):
    assert row is not None, "a refused VRT must be an error tile, never NULL"
    assert row["raster"] is None and row["path"] is None
    return row["metadata"]["last_error"]


def test_a_materialized_vrt_is_a_gtiff_tile_with_the_vrts_pixels(tmp_path):
    tif, vrt = tmp_path / "a.tif", tmp_path / "a.vrt"
    data = _source_tif(str(tif))
    _vrt_over(tif, vrt)
    row = prx._fromfile_impl(str(vrt), "GTiff", True)
    assert row is not None and row["raster"] is not None
    assert row["path"] is None  # self-contained: no path back to the VRT
    with _serde.open_tile(bytes(row["raster"])) as ds:
        assert ds.driver == "GTiff"
        assert (ds.width, ds.height, ds.count) == (16, 12, 1)
        assert ds.crs.to_epsg() == 4326
        assert np.array_equal(ds.read(), data)
    with rasterio.open(vrt) as v:  # the VRT's own pixels
        assert v.driver == "VRT"
        assert np.array_equal(v.read(), data)


def test_a_vrt_over_the_cap_is_an_error_tile_with_the_reason_and_fix(
    tmp_path, monkeypatch
):
    tif, vrt = tmp_path / "b.tif", tmp_path / "b.vrt"
    _source_tif(str(tif), 256, 256)  # 256 KiB decoded
    _vrt_over(tif, vrt)
    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(64 * 1024))
    msg = _error_of(prx._fromfile_impl(str(vrt), "GTiff", True))
    assert "decoded" in msg and "cap" in msg, msg
    assert "Fix:" in msg and "virtual tile" in msg, msg
    # an explicit (driver-captured) cap is used directly, and a large one renders it
    row = prx._fromfile_impl(str(vrt), "GTiff", True, cap_bytes=10 * 1024**3)
    assert row["raster"] is not None


def test_a_vrt_with_a_missing_source_is_an_error_tile_with_the_reason(tmp_path):
    tif, vrt = tmp_path / "c.tif", tmp_path / "c.vrt"
    _source_tif(str(tif))
    _vrt_over(tif, vrt)
    tif.unlink()
    msg = _error_of(prx._fromfile_impl(str(vrt), "GTiff", True))
    assert "c.vrt" in msg or "c.tif" in msg, msg
    assert "Fix:" in msg, msg


def test_a_vrt_naming_a_remote_source_is_an_error_tile_with_the_policy_reason(tmp_path):
    vrt = tmp_path / "r.vrt"
    vrt.write_text(
        '<VRTDataset rasterXSize="4" rasterYSize="4"><VRTRasterBand dataType="Byte" band="1">'
        '<SimpleSource><SourceFilename relativeToVRT="0">/vsicurl/http://127.0.0.1:9/x.tif'
        "</SourceFilename><SourceBand>1</SourceBand></SimpleSource></VRTRasterBand></VRTDataset>"
    )
    msg = _error_of(prx._fromfile_impl(str(vrt), "GTiff", True))
    assert "r.vrt" in msg and "Fix:" in msg, msg
    # a VRT that is itself on a URL cannot be rendered: its reason, not a silent NULL
    url = "s3://bucket/r.vrt"
    msg = _error_of(prx._fromfile_render_vrt(url, url, "GTiff", None))
    assert (
        "local or FUSE-mounted" in msg and "virtual tile" in msg and "Fix:" in msg
    ), msg


def test_a_virtual_vrt_is_unchanged(tmp_path):
    tif, vrt = tmp_path / "d.tif", tmp_path / "d.vrt"
    _source_tif(str(tif))
    _vrt_over(tif, vrt)
    row = prx._fromfile_impl(str(vrt), "GTiff", False)
    assert row["raster"] is None and row["path"] == str(vrt)
    assert (row["window"]["width"], row["window"]["height"]) == (16, 12)


def test_a_missing_path_is_still_null_materialized_or_virtual(tmp_path):
    assert prx._fromfile_impl(str(tmp_path / "nope.tif"), "GTiff", True) is None
    assert prx._fromfile_impl(str(tmp_path / "nope.tif"), "GTiff", False) is None


def test_a_policy_refusal_of_a_non_vrt_file_is_an_error_tile_not_null(tmp_path):
    """Bytes the policy refuses (here an ENVI header-less .hdr-class label file) answer the reason, not NULL."""
    from databricks.labs.gbx import _gdal_policy

    p = tmp_path / "x.tif"
    _source_tif(str(p))
    refusal = _gdal_policy.DriverPolicyError("x is a TEST raster. Fix: do not")
    from unittest.mock import patch

    with patch.object(prx, "open_bytes", side_effect=refusal):
        msg = _error_of(prx._fromfile_impl(str(p), "GTiff", True))
    assert "TEST raster" in msg and "Fix:" in msg


def test_a_policy_refusal_when_opening_the_header_is_an_error_tile_not_null(tmp_path):
    """The virtual default answers a driver-policy refusal with its reason and Fix, as the materialized path does."""
    from unittest.mock import patch

    from databricks.labs.gbx import _gdal_policy

    p = tmp_path / "x.tif"
    _source_tif(str(p))
    refusal = _gdal_policy.DriverPolicyError("x is a TEST raster. Fix: do not")
    with patch.object(prx, "open_data", side_effect=refusal):
        msg = _error_of(prx._fromfile_impl(str(p), "GTiff", False))
    assert "TEST raster" in msg and "Fix:" in msg


def test_column_api_materialized_vrt_through_spark(spark, tmp_path):
    from pyspark.sql import functions as f

    tif, vrt = tmp_path / "e.tif", tmp_path / "e.vrt"
    data = _source_tif(str(tif))
    _vrt_over(tif, vrt)
    gone_tif, miss = tmp_path / "gone.tif", tmp_path / "miss.vrt"
    _source_tif(str(gone_tif))
    _vrt_over(gone_tif, miss)
    gone_tif.unlink()
    df = spark.createDataFrame([(str(vrt),)], ["path"])
    tile = df.select(
        rx.rst_fromfile("path", f.lit("GTiff"), materialize=True).alias("t")
    ).first()["t"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        assert ds.driver == "GTiff" and np.array_equal(ds.read(), data)
    gone = spark.createDataFrame([(str(miss),)], ["path"])
    err = gone.select(
        rx.rst_fromfile("path", f.lit("GTiff"), materialize=True).alias("t")
    ).first()["t"]
    assert err is not None and err["raster"] is None
    assert err["metadata"]["last_error"]
