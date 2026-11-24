"""Offline tests for HlsDownloader (injected earthaccess; no network)."""

import os
from datetime import date

import numpy as np
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from databricks.labs.gbx.sample import HlsDownloader  # noqa: E402
from databricks.labs.gbx.sample import hls as HL  # noqa: E402

BBOX = (-94.17, 41.86, -93.23, 42.21)
ROOT = "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected"
L = f"{ROOT}/HLSL30.020/HLS.L30.T15TVG.2023213T165345.v2.0/HLS.L30.T15TVG.2023213T165345.v2.0"
S = f"{ROOT}/HLSS30.020/HLS.S30.T15TVG.2023215T170901.v2.0/HLS.S30.T15TVG.2023215T170901.v2.0"

S2 = f"{ROOT}/HLSS30.020/HLS.S30.T15TVG.2023220T170901.v2.0/HLS.S30.T15TVG.2023220T170901.v2.0"


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("hls-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


class _G:
    def __init__(self, links):
        self._links = links

    def data_links(self, access=None, in_region=False):
        return self._links


class _EA:
    def __init__(self, by_short, writer=None):
        self.by_short, self.writer, self.logins, self.searches = by_short, writer, 0, []

    def login(self, strategy="all", persist=False):
        self.logins += 1
        return object()

    def search_data(
        self,
        short_name=None,
        version=None,
        bounding_box=None,
        temporal=None,
        count=-1,
        **kw,
    ):
        self.searches.append((short_name, version, bounding_box, temporal))
        return list(self.by_short.get(short_name, []))

    def download(self, granules, local_path=None, threads=8, **kw):
        if self.writer:
            self.writer(granules, local_path)
        return []


def _cog(path):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=64,
            height=64,
            count=1,
            dtype="int16",
            crs="EPSG:32615",
            transform=from_origin(446000, 4650000, 30, 30),
        ) as ds:
            ds.write(np.ones((64, 64), "int16"), 1)
        open(path, "wb").write(mf.read())


def test_asset_of_is_sensor_aware():
    assert HL.asset_of(L + ".B04.tif") == "red" and HL.asset_of(L + ".B05.tif") == "nir"
    assert (
        HL.asset_of(S + ".B8A.tif") == "nir" and HL.asset_of(S + ".B05.tif") is None
    )  # S30 B05 is red edge
    assert (
        HL.asset_of(L + ".Fmask.tif") == "fmask" and HL.asset_of(L + ".B8A.tif") is None
    )
    assert HL.asset_of(L + ".B04.tif.xml") is None


def test_parse_name():
    p = HL.parse_name("/x/HLS.L30.T15TVG.2023213T165345.v2.0.B04.tif")
    assert p == {
        "scene_id": "HLS.L30.T15TVG.2023213T165345.v2.0",
        "sensor": "L30",
        "mgrs_tile": "T15TVG",
        "collect_date": date(2023, 8, 1),
        "asset_name": "red",
    }


def _writer(granules, local_path):
    os.makedirs(local_path, exist_ok=True)
    for url in granules:
        _cog(os.path.join(local_path, os.path.basename(url)))


def test_download_searches_both_collections_and_reads_complete_scenes(spark, tmp_path):
    links = {
        "HLSL30": [
            _G([L + ".B04.tif", L + ".B05.tif", L + ".Fmask.tif", L + ".B06.tif"])
        ],
        "HLSS30": [
            _G([S + ".B04.tif", S + ".B8A.tif"]),  # this S30 scene lacks its Fmask
            _G([S2 + ".B04.tif", S2 + ".B8A.tif", S2 + ".Fmask.tif", S2 + ".B05.tif"]),
        ],
    }
    ea = _EA(links, _writer)
    out = str(tmp_path / "hls")
    res = (
        HlsDownloader(_earthaccess=ea)
        .download(BBOX, out, temporal=("2023-08-01", "2023-08-31"), spark=spark)
        .collect()
    )
    assert ea.logins == 1 and [s[0] for s in ea.searches] == ["HLSL30", "HLSS30"]
    assert {s[1] for s in ea.searches} == {"2.0"}
    assert len(res) == 8 and all(r["is_out_file_valid"] for r in res)
    scenes = HlsDownloader(_earthaccess=ea).read_scenes(spark, out).collect()
    assert [r["scene_id"] for r in scenes] == [
        "HLS.L30.T15TVG.2023213T165345.v2.0",
        "HLS.S30.T15TVG.2023220T170901.v2.0",
    ]
    l30, s30 = scenes
    assert l30["red_path"].endswith(".B04.tif") and l30["nir_path"].endswith(".B05.tif")
    assert s30["red_path"].endswith(".B04.tif") and s30["nir_path"].endswith(".B8A.tif")
    assert s30["fmask_path"].endswith(".Fmask.tif") and s30["sensor"] == "S30"


def test_repair_refetches_the_invalid_asset(spark, tmp_path):
    ea = _EA(
        {"HLSL30": [_G([L + ".B04.tif", L + ".B05.tif", L + ".Fmask.tif"])]}, _writer
    )
    dl, out, window = (
        HlsDownloader(_earthaccess=ea),
        str(tmp_path / "hls"),
        ("2023-08-01", "2023-08-31"),
    )
    res = dl.download(BBOX, out, temporal=window, spark=spark)
    # EarthdataClient.repair needs href on the target: join the discover frame, then mark NIR invalid
    target = res.join(
        dl.discover(BBOX, window, spark), ["item_id", "asset_name"]
    ).withColumn("is_out_file_valid", F.col("asset_name") != "nir")
    fetched = []
    ea.writer = lambda granules, local_path: (
        fetched.extend(granules),
        _writer(granules, local_path),
    )
    fixed = dl.repair(target, spark=spark, out_dir=out).collect()
    assert fetched == [L + ".B05.tif"]
    assert [r["asset_name"] for r in fixed] == ["nir"] and fixed[0]["is_out_file_valid"]


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("EARTHDATA_TOKEN"), reason="needs EARTHDATA_TOKEN"
)
def test_real_one_august_day(spark, tmp_path):
    res = HlsDownloader().download(
        BBOX, str(tmp_path / "hls"), temporal=("2023-08-01", "2023-08-02"), spark=spark
    )
    assert res.where("is_out_file_valid").count() > 0


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("EARTHDATA_TOKEN"), reason="needs EARTHDATA_TOKEN"
)
def test_real_band_names_per_sensor(spark):
    # real CMR links: every L30 and S30 granule gives exactly red B04, NIR (L30 B05, S30 B8A) and Fmask
    rows = (
        HlsDownloader()
        .discover(BBOX, temporal=("2023-08-01", "2023-08-10"), spark=spark)
        .collect()
    )
    by_scene: dict = {}
    for r in rows:
        scene, band = r["item_id"].rsplit(".", 1)
        by_scene.setdefault(scene, {})[r["asset_name"]] = band
    want = {
        "L30": {"red": "B04", "nir": "B05", "fmask": "Fmask"},
        "S30": {"red": "B04", "nir": "B8A", "fmask": "Fmask"},
    }
    assert {s.split(".")[1] for s in by_scene} == {"L30", "S30"}
    bad = {s: a for s, a in by_scene.items() if a != want[s.split(".")[1]]}
    assert (
        not bad
    ), f"granules without the expected red/NIR/Fmask bands: {sorted(bad)[:5]}"
