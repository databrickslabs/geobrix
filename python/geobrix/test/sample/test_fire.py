"""Offline unit tests for FirePerimeterDownloader (injected _get; no network)."""

from __future__ import annotations

import json
import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.sample.fire import (  # noqa: E402
    WFIGS_URL,
    FirePerimeterDownloader,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("fire-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def _feat(i):
    ring = [[-101.0, 35.6], [-100.9, 35.6], [-100.9, 35.7], [-101.0, 35.6]]
    return {
        "type": "Feature",
        "properties": {"poly_IncidentName": "Smokehouse Creek", "part": i},
        "geometry": {"type": "Polygon", "coordinates": [ring]},
    }


class _FakeGet:
    """Fake WFIGS FeatureServer: ``f=geojson`` pages plus ``returnCountOnly`` counts."""

    def __init__(self, total=1, page_size=2000, reported_total=None):
        self.total = total
        self.page_size = page_size
        self.reported_total = total if reported_total is None else reported_total
        self.calls = []

    def __call__(self, url, params):
        self.calls.append((url, dict(params)))
        if params.get("returnCountOnly"):
            return {"count": self.reported_total}
        off = int(params.get("resultOffset", 0))
        n = min(int(params.get("resultRecordCount", self.page_size)), self.page_size)
        page = [_feat(i) for i in range(off, min(off + n, self.total))]
        gj = {"type": "FeatureCollection", "features": page}
        if off + n < self.total:
            gj["properties"] = {"exceededTransferLimit": True}
        return gj

    @property
    def page_calls(self):
        return [p for _, p in self.calls if not p.get("returnCountOnly")]


def test_wfigs_url_is_the_perimeter_query_endpoint():
    assert WFIGS_URL.startswith("https://services3.arcgis.com/")
    assert "WFIGS_Interagency_Perimeters/FeatureServer/0/query" in WFIGS_URL


def test_download_writes_valid_geojson(spark, tmp_path):
    get = _FakeGet(total=1)
    res = (
        FirePerimeterDownloader(_get=get)
        .download("SMOKEHOUSE CREEK", str(tmp_path / "fire"), year=2024, spark=spark)
        .collect()[0]
    )
    assert res["feature_count"] == 1
    assert res["is_out_file_valid"] is True
    dest = res["out_file_path"]
    assert dest.endswith(".geojson") and os.path.exists(dest)
    assert "smokehouse_creek" in os.path.basename(dest)
    with open(dest) as fh:
        gj = json.load(fh)
    assert gj["type"] == "FeatureCollection" and len(gj["features"]) == 1


def test_query_uses_exact_field_names_and_year_window():
    get = _FakeGet(total=1)
    FirePerimeterDownloader(_get=get)._fetch_all("Smokehouse Creek", 2024)
    where = get.page_calls[0]["where"]
    assert "UPPER(poly_IncidentName) = 'SMOKEHOUSE CREEK'" in where
    assert "attr_FireDiscoveryDateTime >= DATE '2024-01-01'" in where
    assert "attr_FireDiscoveryDateTime < DATE '2025-01-01'" in where
    assert get.page_calls[0]["f"] == "geojson"
    assert get.page_calls[0]["outSR"] == "4326"


def test_no_year_omits_the_date_window():
    get = _FakeGet(total=1)
    FirePerimeterDownloader(_get=get)._fetch_all("Smokehouse Creek", None)
    assert "attr_FireDiscoveryDateTime" not in get.page_calls[0]["where"]


def test_incident_name_quotes_are_escaped():
    get = _FakeGet(total=1)
    FirePerimeterDownloader(_get=get)._fetch_all("O'Brien", None)
    assert "'O''BRIEN'" in get.page_calls[0]["where"]


def test_pages_until_no_more():
    get = _FakeGet(total=5, page_size=2)
    feats = FirePerimeterDownloader(_get=get)._fetch_all("X", None, page_size=2)
    assert len(feats) == 5
    assert [p["resultOffset"] for p in get.page_calls] == [0, 2, 4]


def test_short_paged_total_raises():
    get = _FakeGet(total=3, reported_total=4)
    with pytest.raises(RuntimeError, match="Fix:"):
        FirePerimeterDownloader(_get=get)._fetch_all("X", None)


def test_empty_result_marks_invalid_with_reason(spark, tmp_path):
    get = _FakeGet(total=0)
    with pytest.warns(UserWarning, match="No perimeter"):
        res = (
            FirePerimeterDownloader(_get=get)
            .download("NO SUCH FIRE", str(tmp_path / "e"), year=2024, spark=spark)
            .collect()[0]
        )
    assert res["is_out_file_valid"] is False
    assert res["feature_count"] == 0
    assert not os.path.exists(res["out_file_path"])


def test_repair_skips_a_valid_file_and_redownloads_a_missing_one(spark, tmp_path):
    d = str(tmp_path / "r")
    get = _FakeGet(total=1)
    dl = FirePerimeterDownloader(_get=get)
    dl.download("Smokehouse Creek", d, year=2024, spark=spark)
    n = len(get.calls)
    row = dl.repair("Smokehouse Creek", d, year=2024, spark=spark).collect()[0]
    assert row["is_out_file_valid"] is True and len(get.calls) == n  # no network
    os.remove(row["out_file_path"])
    dl.repair("Smokehouse Creek", d, year=2024, spark=spark)
    assert len(get.calls) > n


def test_read_uses_geojson_gbx(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
    d = str(tmp_path / "rd")
    dl = FirePerimeterDownloader(_get=_FakeGet(total=3))
    dl.download("Smokehouse Creek", d, year=2024, spark=spark)
    df = dl.read(d, "Smokehouse Creek", 2024, spark=spark)
    assert df.count() == 3


def test_exports_from_sample():
    from databricks.labs.gbx.sample import FirePerimeterDownloader as F
    from databricks.labs.gbx.sample import download_fire_perimeter_aoi

    assert F is FirePerimeterDownloader
    assert callable(download_fire_perimeter_aoi)
