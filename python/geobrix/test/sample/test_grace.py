"""Offline tests for GraceDaDownloader (injected earthaccess)."""

import os
from datetime import date

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from databricks.labs.gbx.sample import GraceDaDownloader  # noqa: E402
from databricks.labs.gbx.sample import grace as G  # noqa: E402

from .test_hls import _EA, _G, BBOX  # noqa: E402
from .test_http_assets import write_nc  # noqa: E402

URL = "https://hydro1.gesdisc.eosdis.nasa.gov/data/GRACEDA/GRACEDADM_CLSM0125US_7D.4.0/2023/GRACEDADM_CLSM0125US_7D.A20230501.040.nc4"


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("grace-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_week_date_and_assets():
    assert G.week_date_of("/v/GRACEDADM_CLSM0125US_7D.A20230501.040.nc4") == date(
        2023, 5, 1
    )
    assert G.asset_of(URL) == "grace" and G.asset_of(URL + ".xml") is None


def _writer(granules, local_path):
    import netCDF4

    os.makedirs(local_path, exist_ok=True)
    for url in granules:
        dest = os.path.join(local_path, os.path.basename(url))
        write_nc(dest, var="gws_inst", days=1)
        with netCDF4.Dataset(dest, "a") as ds:
            ds.createVariable("sfsm_inst", "f4", ("day", "lat", "lon"))[:] = 1.0


WINDOW = ("2023-05-01", "2023-09-30")


def test_download_validates_netcdf_with_both_variables(spark, tmp_path):
    ea = _EA({G.SHORT: [_G([URL])]}, _writer)
    res = (
        GraceDaDownloader(_earthaccess=ea)
        .download(BBOX, str(tmp_path / "g"), temporal=WINDOW, spark=spark)
        .collect()
    )
    assert (
        len(res) == 1 and res[0]["is_out_file_valid"] and ea.searches[0][1] == G.VERSION
    )


def test_repair_refetches_the_invalid_week(spark, tmp_path):
    ea = _EA({G.SHORT: [_G([URL])]}, _writer)
    dl, out = GraceDaDownloader(_earthaccess=ea), str(tmp_path / "g")
    res = dl.download(BBOX, out, temporal=WINDOW, spark=spark)
    target = res.join(
        dl.discover(BBOX, WINDOW, spark), ["item_id", "asset_name"]
    ).withColumn(  # repair needs href
        "is_out_file_valid", F.lit(False)
    )
    fetched = []
    ea.writer = lambda granules, local_path: (
        fetched.extend(granules),
        _writer(granules, local_path),
    )
    fixed = dl.repair(target, spark=spark, out_dir=out).collect()
    assert fetched == [URL] and len(fixed) == 1 and fixed[0]["is_out_file_valid"]


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("EARTHDATA_TOKEN"), reason="needs EARTHDATA_TOKEN"
)
def test_real_week_date_matches_the_file_time(spark, tmp_path):
    import netCDF4

    res = (
        GraceDaDownloader()
        .download(
            BBOX,
            str(tmp_path / "g"),
            temporal=("2023-05-01", "2023-05-14"),
            spark=spark,
        )
        .collect()
    )
    paths = [r["out_file_path"] for r in res if r["is_out_file_valid"]]
    assert paths
    for p in paths:
        with netCDF4.Dataset(p) as ds:
            t = ds.variables["time"]
            first = netCDF4.num2date(t[0], t.units, getattr(t, "calendar", "standard"))
        assert date(first.year, first.month, first.day) == G.week_date_of(p), (
            p,
            str(first),
        )
