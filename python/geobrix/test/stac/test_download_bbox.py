import os
import sys

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.stac._download import fetch_validate_publish


def _write_gtiff(path, width=8, height=8):
    # extent: origin (0, 8), 1.0 px -> x[0,8], y[0,8]
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write(np.arange(width * height, dtype="uint8").reshape(1, height, width))


def test_windowed_fetch_clips_to_bbox(tmp_path):
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(2, 2, 5, 6)
    )
    assert res == os.path.join(str(out_dir), "win.tif")
    with rasterio.open(res) as ds:
        b = ds.bounds
        assert (b.left, b.bottom, b.right, b.top) == (2, 2, 5, 6)
        assert (ds.width, ds.height) == (3, 4)


def test_windowed_fetch_north_overhang_clips(tmp_path):
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(2, 2, 10, 12)  # E+N overhang
    )
    with rasterio.open(res) as ds:
        assert ds.bounds.top == 8  # clipped to dataset top, not 12
        assert ds.bounds.right == 8


def test_windowed_fetch_no_overlap_raises(tmp_path):
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src), str(out_dir), "win.tif", bbox=(20, 20, 21, 21), max_tries=1
    )
    assert res is None  # no overlap -> all attempts fail -> None (no file published)
    assert not os.path.exists(os.path.join(str(out_dir), "win.tif"))


def test_no_bbox_path_unchanged(tmp_path):
    # bbox=None must keep the byte-faithful download path.
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"

    def get(href, timeout=None, stream=None):
        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield open(str(src), "rb").read()

        return R()

    res = fetch_validate_publish(
        lambda: "http://93.184.216.34/ok.tif", str(out_dir), "ok.tif", get=get
    )
    assert res == os.path.join(str(out_dir), "ok.tif")
    assert os.path.getsize(res) == os.path.getsize(
        str(src)
    )  # byte-identical (no window)


# Module-scoped Spark fixture for the client-level test.
# The stac test directory has no conftest.py, so we define it here.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def spark():
    import logging

    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-stac-bbox-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_windowed_fetch_decimates_to_max_mpp(tmp_path):
    """max_mpp=2.0 on a 1px/unit src with a 4x4 window -> factor=2 -> out shape (2,2)."""
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src),
        str(out_dir),
        "d.tif",
        bbox=(2, 2, 6, 6),
        max_mpp=2.0,
    )
    assert res is not None, "fetch_validate_publish returned None"
    with rasterio.open(res) as ds:
        # bounds must be preserved (georef-clean)
        b = ds.bounds
        assert abs(b.left - 2) < 1e-6
        assert abs(b.bottom - 2) < 1e-6
        assert abs(b.right - 6) < 1e-6
        assert abs(b.top - 6) < 1e-6
        # pixel size must be ~2x the native 1.0
        assert (
            abs(abs(ds.transform.a) - 2.0) < 1e-6
        ), f"unexpected res x: {ds.transform.a}"
        assert (
            abs(abs(ds.transform.e) - 2.0) < 1e-6
        ), f"unexpected res y: {ds.transform.e}"
        # output shape: ceil(4/2)=2 wide, ceil(4/2)=2 tall
        assert ds.width == 2, f"expected width=2 got {ds.width}"
        assert ds.height == 2, f"expected height=2 got {ds.height}"


def test_max_mpp_none_is_full_res(tmp_path):
    """max_mpp=None -> full-res windowed read, same as no decimation."""
    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    res = fetch_validate_publish(
        lambda: str(src),
        str(out_dir),
        "full.tif",
        bbox=(2, 2, 6, 6),
        max_mpp=None,
    )
    assert res is not None
    with rasterio.open(res) as ds:
        # native 1px/unit, 4-unit window -> 4x4
        assert ds.width == 4, f"expected width=4 got {ds.width}"
        assert ds.height == 4, f"expected height=4 got {ds.height}"
        # pixel size stays ~1.0
        assert abs(abs(ds.transform.a) - 1.0) < 1e-6


def test_client_download_threads_bbox(spark, tmp_path):
    # End-to-end via StacClient.download with a local file as the (unsigned) href.
    from databricks.labs.gbx.stac.client import StacClient

    src = tmp_path / "src.tif"
    _write_gtiff(str(src))
    out_dir = tmp_path / "out"
    df = spark.createDataFrame(
        [("item1", "image", str(src))], ["item_id", "asset_name", "href"]
    )
    client = StacClient.__new__(StacClient)  # bypass __init__ (no network/catalog)
    client.sign = None  # resolve_signer(None) -> identity signer
    res = client.download(df, str(out_dir), bbox=(2, 2, 5, 6)).collect()
    assert len(res) == 1 and res[0]["is_out_file_valid"]
    with rasterio.open(res[0]["out_file_path"]) as ds:
        assert (ds.width, ds.height) == (3, 4)
