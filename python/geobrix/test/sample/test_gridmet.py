"""Offline tests for GridmetDownloader (injected fetch; netcdf_gbx on a local file)."""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.core import _http  # noqa: E402
from databricks.labs.gbx.sample import GridmetDownloader, download_gridmet  # noqa: E402
from databricks.labs.gbx.sample import gridmet as G  # noqa: E402

from .test_http_assets import Fetch, write_nc  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("gridmet-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(s, only=["netcdf_gbx"])
    # register_sources registers no function: gbx_rst_numbands needs its own registration (the Python level)
    gbx_manager.register_family(
        s, ("gbx_rst_", "gbx_pmtiles_"), only=["gbx_rst_numbands"], level="L3"
    )
    yield s
    s.stop()


def test_assets_name_one_file_per_season():
    a = GridmetDownloader().assets([2022, 2023])
    assert a == [
        (
            "vs_2022",
            "vs",
            "https://www.northwestknowledge.net/metdata/data/vs_2022.nc",
            "vs_2022.nc",
        ),
        (
            "vs_2023",
            "vs",
            "https://www.northwestknowledge.net/metdata/data/vs_2023.nc",
            "vs_2023.nc",
        ),
    ]


def test_base_url_host_is_in_the_allow_list_and_reaches_the_fetch(spark, tmp_path):
    host = G.BASE.split("/")[2].lower()
    assert _http._allowed(host, G._ALLOWED_HOSTS)
    assert not _http._allowed("evil.example.com", G._ALLOWED_HOSTS)
    f = Fetch(write_nc(str(tmp_path / "src.nc"), days=3))
    GridmetDownloader(_fetch=f).download([2023], str(tmp_path / "gridmet"), spark=spark)
    assert [(u.split("/")[2], tuple(h)) for u, h in f.calls] == [
        (host, G._ALLOWED_HOSTS)
    ]


def test_download_then_read_one_band_per_day(spark, tmp_path):
    f = Fetch(write_nc(str(tmp_path / "src.nc"), days=5))
    out = str(tmp_path / "gridmet")
    res = GridmetDownloader(_fetch=f).download([2023], out, spark=spark).collect()
    assert res[0]["is_out_file_valid"] and res[0]["out_file_path"].endswith(
        "/vs_2023.nc"
    )
    df = GridmetDownloader().read(spark, out, (-94.0, 41.9, -93.6, 42.2), season=2023)
    row = df.selectExpr("gbx_rst_numbands(tile) AS n").collect()
    assert [r["n"] for r in row] == [5]


def test_repair_refetches_the_invalid_season(spark, tmp_path):
    src = write_nc(str(tmp_path / "src.nc"), days=5)
    out = str(tmp_path / "gridmet")
    GridmetDownloader(_fetch=Fetch(src)).download([2022], out, spark=spark)
    target = GridmetDownloader(_fetch=Fetch(src, truncate=True)).download(
        [2022, 2023], out, spark=spark
    )
    good = Fetch(src)
    fixed = (
        GridmetDownloader(_fetch=good)
        .repair(target, [2022, 2023], out, spark=spark)
        .collect()
    )
    assert [c[0] for c in good.calls] == [
        "https://www.northwestknowledge.net/metdata/data/vs_2023.nc"
    ]
    assert [r["item_id"] for r in fixed] == ["vs_2023"] and fixed[0][
        "is_out_file_valid"
    ]


DEMO_BBOX = (
    -94.2434,
    41.7965,
    -93.154,
    42.2734,
)  # Story + Boone + 5 km (the series' demo area)


@pytest.mark.integration
def test_real_vs_2023(spark, tmp_path):
    import numpy as np
    from rasterio.io import MemoryFile

    out = str(tmp_path / "g")
    res = download_gridmet(spark, [2023], out).collect()
    assert (
        res[0]["is_out_file_valid"] and res[0]["out_file_sz"] > 50_000_000
    )  # 59,825,061 bytes at the time of writing
    tile = GridmetDownloader().read(spark, out, DEMO_BBOX, season=2023).first()["tile"]
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as rds:
        assert (
            rds.count == 365 and rds.width * rds.height <= 30 * 15
        )  # a county-sized cut, not CONUS
        day0 = rds.read(1, masked=True)
    # netcdf_gbx returns the scaled values (m/s): raw uint16 counts would average about 18.9 here
    assert 1.0 < float(np.ma.mean(day0)) < 3.0
