"""Offline unit tests for TropomiDownloader (injected StacClient, no network)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.sample.tropomi import TropomiDownloader  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("tropomi-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_discover_filters_to_ch4_asset(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [
            ("S5P_1", "ch4", [10.0, 49.0, 12.0, 51.0], "https://x/ch4.nc"),
            ("S5P_1", "co", [10.0, 49.0, 12.0, 51.0], "https://x/co.nc"),
        ],
        ["item_id", "asset_name", "item_bbox", "href"],
    )
    fake.search.return_value = search_df
    dl = TropomiDownloader(_stac_client=fake)
    out = dl.discover([10.0, 49.0, 12.0, 51.0], spark=spark)
    assert out.count() == 1
    assert out.first()["asset_name"] == "ch4"


def test_download_delegates_to_stacclient(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [("S5P_1", "ch4", "https://x/ch4.nc")],
        ["item_id", "asset_name", "href"],
    )
    fake.search.return_value = search_df
    fake.download.return_value = spark.createDataFrame(
        [("S5P_1", "ch4", "/vol/ch4.nc", 123, True)],
        ["item_id", "asset_name", "out_file_path", "out_file_sz", "is_out_file_valid"],
    )
    dl = TropomiDownloader(_stac_client=fake)
    res = dl.download([10.0, 49.0, 12.0, 51.0], "/vol/out", spark=spark)
    assert res.count() == 1
    fake.download.assert_called_once()


def test_exports_from_sample():
    from databricks.labs.gbx.sample import TropomiDownloader, download_tropomi_aoi

    assert TropomiDownloader is not None
    assert callable(download_tropomi_aoi)
