"""Offline unit tests for WorldPopDownloader (injected _download; no network)."""

from __future__ import annotations

import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.sample.worldpop import (  # noqa: E402
    USA_2020_CONSTRAINED,
    WorldPopDownloader,
)

BBOX = [-101.5, 35.0, -100.0, 36.0]
BBOX2 = [-100.0, 35.0, -98.5, 36.0]


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("worldpop-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


class _FakeDownload:
    """Stands in for ``stac._download.windowed_download``; writes a 2x2 GeoTIFF."""

    def __init__(self, raises=None, fail_after=None):
        self.calls = []
        self.raises = raises
        self.fail_after = fail_after  # calls made before every later one fails

    def __call__(self, href, outpath, bbox, **kw):
        self.calls.append((href, outpath, tuple(bbox), kw))
        if self.raises:
            raise self.raises
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            with open(outpath, "wb") as fh:  # a partial file, then the failure
                fh.write(b"partial")
            raise OSError("network dropped")
        import numpy as np
        import rasterio
        from rasterio.transform import from_origin

        with rasterio.open(
            outpath,
            "w",
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(bbox[0], bbox[3], 0.5, 0.5),
        ) as dst:
            dst.write(np.ones((2, 2), dtype="float32"), 1)
        return outpath


def test_href_is_the_verified_worldpop_constrained_2020_usa_file():
    assert USA_2020_CONSTRAINED == (
        "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/"
        "2020/BSGM/USA/usa_ppp_2020_constrained.tif"
    )


def test_download_passes_bbox_and_allowed_host(spark, tmp_path):
    fake = _FakeDownload()
    res = (
        WorldPopDownloader(_download=fake)
        .download(BBOX, str(tmp_path / "wp"), spark=spark)
        .collect()[0]
    )
    href, outpath, bbox, kw = fake.calls[0]
    assert href == USA_2020_CONSTRAINED
    assert bbox == tuple(BBOX)
    assert kw["bbox_crs"] == "EPSG:4326"
    assert kw["allowed_hosts"] == ("data.worldpop.org",)
    # the window is written to a scratch path, then published under the bbox-tagged name
    assert os.path.basename(res["out_file_path"]) == os.path.basename(outpath)
    assert ".gbx_scratch" in outpath and ".gbx_scratch" not in res["out_file_path"]
    assert res["is_out_file_valid"] is True
    assert res["out_file_sz"] == os.path.getsize(res["out_file_path"]) > 0
    assert outpath.endswith(".tif")


def test_columns_match_stac_download(spark, tmp_path):
    df = WorldPopDownloader(_download=_FakeDownload()).download(
        BBOX, str(tmp_path / "c"), spark=spark
    )
    assert df.columns == [
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    ]


def test_bbox_outside_the_asset_is_invalid_not_a_crash(spark, tmp_path):
    fake = _FakeDownload(raises=ValueError("bbox does not overlap"))
    with pytest.warns(UserWarning, match="does not overlap"):
        res = (
            WorldPopDownloader(_download=fake)
            .download(BBOX, str(tmp_path / "o"), spark=spark)
            .collect()[0]
        )
    assert res["is_out_file_valid"] is False
    assert not os.path.exists(res["out_file_path"])


def test_other_errors_propagate_and_leave_no_partial_file(spark, tmp_path):
    fake = _FakeDownload(raises=OSError("network down"))
    with pytest.raises(OSError):
        WorldPopDownloader(_download=fake).download(
            BBOX, str(tmp_path / "p"), spark=spark
        )
    assert not list((tmp_path / "p").glob("*.tif"))


def test_repair_skips_a_valid_file_and_redownloads_a_missing_one(spark, tmp_path):
    d = str(tmp_path / "r")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    row = dl.download(BBOX, d, spark=spark).collect()[0]
    row = dl.repair(BBOX, d, spark=spark).collect()[0]
    assert len(fake.calls) == 1 and row["is_out_file_valid"] is True
    os.remove(row["out_file_path"])
    dl.repair(BBOX, d, spark=spark)
    assert len(fake.calls) == 2


def test_read_uses_raster_gbx(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    d = str(tmp_path / "rd")
    dl = WorldPopDownloader(_download=_FakeDownload())
    dl.download(BBOX, d, spark=spark)
    assert dl.read(d, spark=spark).count() == 1


def test_exports_from_sample():
    from databricks.labs.gbx.sample import WorldPopDownloader as W
    from databricks.labs.gbx.sample import download_worldpop_aoi

    assert W is WorldPopDownloader
    assert callable(download_worldpop_aoi)


def test_two_bboxes_in_one_dir_get_different_files(spark, tmp_path):
    d = str(tmp_path / "two")
    dl = WorldPopDownloader(_download=_FakeDownload())
    a = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    b = dl.download(BBOX2, d, spark=spark).collect()[0]["out_file_path"]
    assert a != b and os.path.exists(a) and os.path.exists(b)
    # deterministic: the same bbox names the same file
    assert dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"] == a


def test_repair_checks_the_file_of_this_bbox(spark, tmp_path):
    d = str(tmp_path / "rp")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    dl.download(BBOX, d, spark=spark)  # only bbox 1 exists
    dl.repair(BBOX, d, spark=spark)
    assert len(fake.calls) == 1  # bbox 1 is on disk: no download
    row = dl.repair(BBOX2, d, spark=spark).collect()[0]
    assert len(fake.calls) == 2  # bbox 2 was NOT covered by bbox 1's file
    assert row["is_out_file_valid"] is True and os.path.exists(row["out_file_path"])


def test_failed_second_call_leaves_the_first_file_intact(spark, tmp_path):
    d = str(tmp_path / "keep")
    fake = _FakeDownload(fail_after=1)
    dl = WorldPopDownloader(_download=fake)
    path = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    before = open(path, "rb").read()
    with pytest.raises(OSError):
        dl.download(BBOX, d, spark=spark)
    assert open(path, "rb").read() == before
    assert os.listdir(d) == [os.path.basename(path)]  # no scratch or partial left


@pytest.mark.parametrize(
    "bad",
    [
        [1, 2, 3],
        [-101, 35, float("nan"), 36],
        [-101, 35, float("inf"), 36],
        [-100, 35, -101, 36],
        [-101, 36, -100, 35],
        "bbox",
        None,
    ],
)
def test_invalid_bbox_raises_with_fix(spark, tmp_path, bad):
    fake = _FakeDownload()
    with pytest.raises(ValueError, match="Fix:"):
        WorldPopDownloader(_download=fake).download(
            bad, str(tmp_path / "b"), spark=spark
        )
    assert fake.calls == []


def test_max_mpp_is_passed_through_and_changes_the_name(spark, tmp_path):
    d = str(tmp_path / "m")
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    a = dl.download(BBOX, d, spark=spark, max_mpp=0.01).collect()[0]["out_file_path"]
    assert fake.calls[0][3]["max_mpp"] == 0.01
    b = dl.download(BBOX, d, spark=spark).collect()[0]["out_file_path"]
    assert "max_mpp" not in fake.calls[1][3] and a != b


def test_window_over_the_decoded_budget_is_refused(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.core.budget.decoded_budget_bytes",
        lambda strategy: 1 << 20,  # 1 MiB; BBOX is about 8 MiB decoded
    )
    fake = _FakeDownload()
    dl = WorldPopDownloader(_download=fake)
    with pytest.raises(ValueError, match="Fix:.*max_mpp"):
        dl.download(BBOX, str(tmp_path / "big"), spark=spark)
    assert fake.calls == []
    # decimating to ~0.01 degree brings the same bbox under the cap
    dl.download(BBOX, str(tmp_path / "big"), spark=spark, max_mpp=0.01)
    assert len(fake.calls) == 1


def test_read_with_bbox_reads_only_that_window(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    d = str(tmp_path / "rb")
    dl = WorldPopDownloader(_download=_FakeDownload())
    dl.download(BBOX, d, spark=spark)
    dl.download(BBOX2, d, spark=spark)
    assert dl.read(d, spark=spark).count() == 2
    assert dl.read(d, spark=spark, bbox=BBOX2).count() == 1
