"""Offline unit tests for EarthdataClient (injected earthaccess; no network)."""

from __future__ import annotations

import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.gbx.earthdata import EarthdataClient  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("earthdata-test")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


class _FakeGranule:
    def __init__(self, links):
        self._links = links

    def data_links(self, access=None, in_region=False):
        return self._links


class _FakeEarthaccess:
    def __init__(self, granules, writer=None):
        self._granules = granules
        self._writer = writer
        self.login_calls = 0

    def login(self, strategy="all", persist=False):
        self.login_calls += 1

    def search_data(
        self,
        short_name=None,
        version=None,
        bounding_box=None,
        temporal=None,
        count=-1,
        **kw,
    ):
        self.last_temporal = temporal
        return list(self._granules)

    def download(self, granules, local_path=None, threads=8, **kw):
        if self._writer:
            self._writer(granules, local_path)
        return []


def _asset_tif(url):
    return "data" if url.endswith(".tif") else None


def _validate_nonempty(path, asset):
    return os.path.exists(path) and os.path.getsize(path) > 0


BBOX = [-103.9, 31.65, -103.4, 32.15]


def test_norm_temporal_slash_string_becomes_tuple():
    from databricks.labs.gbx.earthdata.client import _norm_temporal

    assert _norm_temporal("2024-08-01/2024-09-30") == ("2024-08-01", "2024-09-30")
    assert _norm_temporal(None) is None
    assert _norm_temporal(("2024-08-01", "2024-09-30")) == (
        "2024-08-01",
        "2024-09-30",
    )
    assert _norm_temporal("2024-08-01") == ("2024-08-01",)


def test_search_normalizes_slash_temporal_for_cmr(spark):
    g = _FakeGranule(["https://x/A_data.tif"])
    fake = _FakeEarthaccess([g])
    client = EarthdataClient(_earthaccess=fake)
    # A STAC-style slash window must reach earthaccess as a (start, end) tuple.
    client.search(
        ["SHORT"], "002", BBOX, "2024-08-01/2024-09-30", _asset_tif, spark=spark
    )
    assert fake.last_temporal == ("2024-08-01", "2024-09-30")


def test_search_returns_classified_rows(spark):
    g = _FakeGranule(["https://x/A_data.tif", "https://x/A_meta.xml"])
    client = EarthdataClient(_earthaccess=_FakeEarthaccess([g]))
    df = client.search(["SHORT"], "002", BBOX, None, _asset_tif, spark=spark)
    rows = df.collect()
    assert [f.name for f in df.schema.fields] == ["item_id", "asset_name", "href"]
    assert len(rows) == 1  # .xml skipped by the classifier
    assert rows[0]["asset_name"] == "data"
    assert rows[0]["href"].endswith("A_data.tif")


def test_download_validates_and_builds_result_frame(spark, tmp_path):
    def _writer(granules, local_path):
        # client passes hrefs as strings (earthaccess.download's str/list[str] form)
        os.makedirs(local_path, exist_ok=True)
        for url in granules:
            dest = os.path.join(local_path, os.path.basename(url))
            with open(dest, "wb") as fh:
                fh.write(b"x" * 100)

    g = _FakeGranule(["https://x/A_data.tif"])
    client = EarthdataClient(_earthaccess=_FakeEarthaccess([g], writer=_writer))
    rows = client.search(["SHORT"], "002", BBOX, None, _asset_tif, spark=spark)
    res = client.download(rows, str(tmp_path / "d"), _validate_nonempty, spark=spark)
    assert [f.name for f in res.schema.fields] == [
        "item_id",
        "asset_name",
        "out_file_path",
        "out_file_sz",
        "is_out_file_valid",
        "last_update",
    ]
    r = res.collect()[0]
    assert r["is_out_file_valid"] and r["out_file_sz"] == 100


def test_download_marks_missing_invalid(spark, tmp_path):
    g = _FakeGranule(["https://x/A_data.tif"])
    client = EarthdataClient(
        _earthaccess=_FakeEarthaccess([g], writer=lambda a, b: None)
    )
    rows = client.search(["SHORT"], "002", BBOX, None, _asset_tif, spark=spark)
    r = client.download(
        rows, str(tmp_path / "d2"), _validate_nonempty, spark=spark
    ).collect()[0]
    assert not r["is_out_file_valid"]
    assert r["out_file_path"] is None


def test_missing_earthaccess_names_the_extra(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "earthaccess":
            raise ModuleNotFoundError("No module named 'earthaccess'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    from databricks.labs.gbx.earthdata.client import EarthdataClient

    with pytest.raises(ImportError, match=r"geobrix\[earthdata\]"):
        EarthdataClient()._ea()  # accessor that imports earthaccess
