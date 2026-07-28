"""Offline unit tests for NasaNexDownloader (injected StacClient, no network)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.sample.nasanex import NasaNexDownloader  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("nasanex-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_nasanex_download_filters_to_requested_variables(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [
            ("nex_item_1", "tas", "https://x/tas.nc"),
            ("nex_item_1", "pr", "https://x/pr.nc"),
            ("nex_item_1", "tasmax", "https://x/tasmax.nc"),
        ],
        ["item_id", "asset_name", "href"],
    )
    fake.search.return_value = search_df
    fake.download.return_value = spark.createDataFrame(
        [("nex_item_1_tas.nc", 512, True)],
        ["out_file_path", "out_file_sz", "is_out_file_valid"],
    )
    dl = NasaNexDownloader(_stac_client=fake)
    res = dl.download(
        [10.0, 49.0, 12.0, 51.0], "/vol/out", variables=("tas",), spark=spark
    )
    assert res.count() == 1
    # check StacClient.download was called with only the 'tas' href
    call_args = fake.download.call_args
    granules_df = call_args[0][0]
    selected_assets = [row["asset_name"] for row in granules_df.collect()]
    assert selected_assets == ["tas"]


def test_nasanex_download_multiple_variables(spark):
    fake = MagicMock()
    search_df = spark.createDataFrame(
        [
            ("nex_item_1", "tas", "https://x/tas.nc"),
            ("nex_item_1", "pr", "https://x/pr.nc"),
            ("nex_item_1", "hurs", "https://x/hurs.nc"),
        ],
        ["item_id", "asset_name", "href"],
    )
    fake.search.return_value = search_df
    fake.download.return_value = spark.createDataFrame(
        [
            ("nex_item_1_tas.nc", 512, True),
            ("nex_item_1_pr.nc", 512, True),
        ],
        ["out_file_path", "out_file_sz", "is_out_file_valid"],
    )
    dl = NasaNexDownloader(_stac_client=fake)
    res = dl.download(
        [10.0, 49.0, 12.0, 51.0],
        "/vol/out",
        variables=("tas", "pr"),
        spark=spark,
    )
    assert res.count() == 2
    call_args = fake.download.call_args
    granules_df = call_args[0][0]
    selected_assets = sorted([row["asset_name"] for row in granules_df.collect()])
    assert selected_assets == ["pr", "tas"]


def test_exports_from_sample():
    from databricks.labs.gbx.sample import NasaNexDownloader, download_nasanex_aoi

    assert NasaNexDownloader is not None
    assert callable(download_nasanex_aoi)
