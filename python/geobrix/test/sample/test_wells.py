"""Offline unit tests for WellsDownloader (injected _get; no network)."""

from __future__ import annotations

import json
import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.sample.wells import WellsDownloader  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("wells-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def _feat(i):
    return {
        "type": "Feature",
        "properties": {
            "API": 4247500000 + i,
            "CompanyName": "ACME OIL",
            "LeaseName": f"UNIV {i}",
            "County": "Reeves",
        },
        "geometry": {"type": "Point", "coordinates": [-103.6 + i * 0.001, 31.9]},
    }


class _PagedGet:
    """Two-page fake: page 0 (exceededTransferLimit) then page 1 (final)."""

    def __init__(self, total=1500, page_size=1000):
        self.total = total
        self.page_size = page_size
        self.calls = []

    def __call__(self, url, params):
        self.calls.append(dict(params))
        off = int(params["resultOffset"])
        page = [_feat(i) for i in range(off, min(off + self.page_size, self.total))]
        more = (off + self.page_size) < self.total
        return {
            "type": "FeatureCollection",
            "features": page,
            "exceededTransferLimit": more,
        }


BBOX = [-103.9, 31.65, -103.4, 32.15]


def test_download_pages_and_merges(spark, tmp_path):
    get = _PagedGet(total=1500, page_size=1000)
    dl = WellsDownloader(_get=get)
    res = dl.download(
        BBOX, str(tmp_path / "wells"), page_size=1000, spark=spark
    ).collect()[0]
    assert len(get.calls) == 2  # paged twice
    assert res["feature_count"] == 1500
    assert res["is_out_file_valid"] is True
    dest = res["out_file_path"]
    assert dest.endswith("wells.geojson") and os.path.exists(dest)
    with open(dest) as fh:
        assert len(json.load(fh)["features"]) == 1500


def test_read_uses_geojson_gbx(spark, tmp_path):
    from databricks.labs.gbx.ds.register import register

    register(spark)
    d = tmp_path / "wells2"
    dl = WellsDownloader(_get=_PagedGet(total=5, page_size=1000))
    dl.download(BBOX, str(d), spark=spark)
    df = dl.read(str(d), spark=spark)
    assert df.count() == 5
    assert any(f.name.startswith("geom_0") for f in df.schema.fields)


def test_exports_from_sample():
    from databricks.labs.gbx.sample import WellsDownloader, download_wells_aoi

    assert WellsDownloader is not None
    assert callable(download_wells_aoi)
