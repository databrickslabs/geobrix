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

URL = "https://hydro1.gesdisc.eosdis.nasa.gov/data/GRACEDA/GRACEDADM_CLSM0125US_7D.4.0/2023/GRACEDADM_CLSM0125US_7D.A20230501.040.nc4"


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("grace-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(s, only=["netcdf_gbx"])
    # register_sources registers no function: gbx_rst_numbands needs its own registration (the Python level)
    gbx_manager.register_family(
        s,
        ("gbx_rst_", "gbx_pmtiles_"),
        only=[
            "gbx_rst_numbands",
            "gbx_rst_width",
            "gbx_rst_height",
            "gbx_rst_georeference",
        ],
        level="L3",
    )
    yield s
    s.stop()


def test_week_date_and_assets():
    assert G.week_date_of("/v/GRACEDADM_CLSM0125US_7D.A20230501.040.nc4") == date(
        2023, 5, 1
    )
    assert G.asset_of(URL) == "grace" and G.asset_of(URL + ".xml") is None


def _write_week(dest, date_stamp="2023-05-01"):
    """A weekly file shaped like the real one: (time, lat, lon) with a time dimension of size 1."""
    import netCDF4
    import numpy as np

    with netCDF4.Dataset(dest, "w") as ds:
        ds.createDimension("time", 1)
        ds.createDimension("lat", 4)
        ds.createDimension("lon", 5)
        t = ds.createVariable("time", "f8", ("time",))
        t.units, t.calendar = f"days since {date_stamp} 00:00:00", "standard"
        t[:] = [0.0]
        ds.createVariable("lat", "f8", ("lat",))[:] = [42.2, 42.1, 42.0, 41.9]
        ds.createVariable("lon", "f8", ("lon",))[:] = [
            -94.0,
            -93.9,
            -93.8,
            -93.7,
            -93.6,
        ]
        for i, name in enumerate(G.VARIABLES):
            ds.createVariable(name, "f4", ("time", "lat", "lon"))[:] = (
                np.arange(20, dtype="f4").reshape(1, 4, 5) + i
            )


def _writer(granules, local_path):
    os.makedirs(local_path, exist_ok=True)
    for url in granules:
        _write_week(os.path.join(local_path, os.path.basename(url)))


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


def test_read_gives_one_one_band_tile_per_week(spark, tmp_path):
    out = str(tmp_path / "g")
    dl = GraceDaDownloader(_earthaccess=_EA({G.SHORT: [_G([URL])]}, _writer))
    assert dl.download(BBOX, out, temporal=WINDOW, spark=spark).collect()[0][
        "is_out_file_valid"
    ]
    bbox = (-93.95, 41.95, -93.65, 42.15)
    rows = (
        dl.read(spark, out, "gws_inst", bbox)
        .selectExpr(
            "source",
            "gbx_rst_numbands(tile) AS n",
            "gbx_rst_width(tile) AS w",
            "gbx_rst_height(tile) AS h",
            "gbx_rst_georeference(tile) AS gt",
        )
        .collect()
    )
    r = rows[0]
    assert len(rows) == 1 and r["n"] == 1 and r["w"] > 0 and r["h"] > 0
    g, eps = r["gt"], 1e-6
    east = g["upperLeftX"] + r["w"] * g["scaleX"]
    south = g["upperLeftY"] + r["h"] * g["scaleY"]
    assert (
        g["upperLeftX"] >= bbox[0] - eps and east <= bbox[2] + eps
    )  # the clip is inside the bbox
    assert south >= bbox[1] - eps and g["upperLeftY"] <= bbox[3] + eps
    assert rows[0]["source"].startswith("NETCDF:") and rows[0]["source"].endswith(
        ":gws_inst"
    )
    path = rows[0]["source"].split('"')[1]
    assert G.week_date_of(path) == date(2023, 5, 1)


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
