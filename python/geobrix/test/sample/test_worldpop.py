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

    def __init__(self, raises=None):
        self.calls = []
        self.raises = raises

    def __call__(self, href, outpath, bbox, **kw):
        self.calls.append((href, outpath, tuple(bbox), kw))
        if self.raises:
            raise self.raises
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
    assert res["out_file_path"] == outpath
    assert res["is_out_file_valid"] is True
    assert res["out_file_sz"] == os.path.getsize(outpath) > 0
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
