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
    """Fake ArcGIS FeatureServer shaped like the live ``f=geojson`` responses.

    Live shapes (WellSHL/FeatureServer/0/query):
    - page:  ``{"type": "FeatureCollection", "properties": {"exceededTransferLimit":
      true}, "features": [...]}``; the last page has no flag.
    - count (``returnCountOnly=true&f=json``): ``{"count": 1201}``.
    ``top_level=True`` puts the flag at the top level instead (the ``f=json`` shape).
    """

    def __init__(
        self, total=1500, page_size=1000, top_level=False, reported_total=None
    ):
        self.total = total
        self.page_size = page_size
        self.top_level = top_level
        self.reported_total = total if reported_total is None else reported_total
        self.calls = []

    def __call__(self, url, params):
        self.calls.append(dict(params))
        if params.get("returnCountOnly"):
            return {"count": self.reported_total}
        off = int(params["resultOffset"])
        # the server caps a page at maxRecordCount no matter what the caller asks for
        n = min(int(params["resultRecordCount"]), self.page_size)
        page = [_feat(i) for i in range(off, min(off + n, self.total))]
        gj = {"type": "FeatureCollection", "features": page}
        if (off + n) < self.total:
            if self.top_level:
                gj["exceededTransferLimit"] = True
            else:
                gj["properties"] = {"exceededTransferLimit": True}
        return gj

    @property
    def page_calls(self):
        return [c for c in self.calls if not c.get("returnCountOnly")]


BBOX = [-103.9, 31.65, -103.4, 32.15]


def test_download_pages_and_merges(spark, tmp_path):
    get = _PagedGet(total=1500, page_size=1000)
    dl = WellsDownloader(_get=get)
    res = dl.download(
        BBOX, str(tmp_path / "wells"), page_size=1000, spark=spark
    ).collect()[0]
    assert len(get.page_calls) == 2  # paged twice
    assert res["feature_count"] == 1500
    assert res["is_out_file_valid"] is True
    dest = res["out_file_path"]
    assert dest.endswith("wells.geojson") and os.path.exists(dest)
    with open(dest) as fh:
        assert len(json.load(fh)["features"]) == 1500


def test_read_uses_geojson_gbx(spark, tmp_path):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_sources(spark)
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


def test_pages_past_first_geojson_page_1201_wells():
    """Live repro: 1,201 wells, page 1 = 1,000 with properties.exceededTransferLimit."""
    get = _PagedGet(total=1201, page_size=1000)
    feats = WellsDownloader(_get=get)._fetch_all(BBOX, 1000)
    assert len(feats) == 1201
    assert [c["resultOffset"] for c in get.page_calls] == [0, 1000]


def test_still_honors_top_level_exceeded_flag_for_f_json_shape():
    get = _PagedGet(total=2500, page_size=1000, top_level=True)
    assert len(WellsDownloader(_get=get)._fetch_all(BBOX, 1000)) == 2500


def test_pages_when_server_caps_page_below_requested_size():
    # asks for 5000, the service's maxRecordCount returns 1000 per page
    get = _PagedGet(total=2200, page_size=1000)
    assert len(WellsDownloader(_get=get)._fetch_all(BBOX, 5000)) == 2200
    assert [c["resultOffset"] for c in get.page_calls] == [0, 1000, 2000]


def test_count_check_asks_return_count_only_once():
    get = _PagedGet(total=1201)
    WellsDownloader(_get=get)._fetch_all(BBOX, 1000)
    counts = [c for c in get.calls if c.get("returnCountOnly")]
    assert len(counts) == 1
    assert "resultOffset" not in counts[0]


def test_paged_total_short_of_server_count_raises_with_both_numbers():
    # a service that drops the continuation flag entirely: paging stops short
    class _NoFlag(_PagedGet):
        def __call__(self, url, params):
            gj = super().__call__(url, params)
            gj.pop("properties", None)
            return gj

    get = _NoFlag(total=1201, page_size=1000)
    with pytest.raises(RuntimeError) as ei:
        WellsDownloader(_get=get)._fetch_all(BBOX, 1000)
    msg = str(ei.value)
    assert "1000" in msg and "1201" in msg and "Fix:" in msg


def test_download_does_not_write_a_partial_file(spark, tmp_path):
    get = _PagedGet(total=1500, reported_total=1600)
    with pytest.raises(RuntimeError):
        WellsDownloader(_get=get).download(BBOX, str(tmp_path / "w"), spark=spark)
    assert not (tmp_path / "w" / "wells.geojson").exists()


def test_unavailable_count_warns_and_returns_pages():
    class _NoCount(_PagedGet):
        def __call__(self, url, params):
            if params.get("returnCountOnly"):
                raise OSError("count not supported")
            return super().__call__(url, params)

    with pytest.warns(UserWarning, match="not cross-checked"):
        feats = WellsDownloader(_get=_NoCount(total=1500))._fetch_all(BBOX, 1000)
    assert len(feats) == 1500
