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


def test_missing_earthaccess_names_the_package_and_separate_step(monkeypatch):
    import builtins

    # _ea() sets TQDM_DISABLE; register an undo so it does not leak into the session.
    monkeypatch.delenv("TQDM_DISABLE", raising=False)

    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "earthaccess":
            raise ModuleNotFoundError("No module named 'earthaccess'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    from databricks.labs.gbx.earthdata.client import EarthdataClient

    with pytest.raises(
        ImportError, match=r"separate step.*pip install 'earthaccess"
    ) as exc:
        EarthdataClient()._ea()  # accessor that imports earthaccess
    assert "geobrix[earthdata]" not in str(exc.value)  # the extra no longer exists


class EulaNotAccepted(Exception):
    """Stand-in for earthaccess.exceptions.EulaNotAccepted (matched by name)."""


def _one_row_client(fake, spark):
    client = EarthdataClient(_earthaccess=fake)
    rows = client.search(["SHORT"], "002", BBOX, None, _asset_tif, spark=spark)
    return client, rows


def test_download_disables_earthaccess_progress(spark, tmp_path):
    seen = {}

    class _Spy(_FakeEarthaccess):
        def download(self, granules, local_path=None, threads=8, **kw):
            seen.update(kw)
            return []

    client, rows = _one_row_client(
        _Spy([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    client.download(rows, str(tmp_path / "p"), _validate_nonempty, spark=spark)
    assert seen.get("show_progress") is False


def test_download_omits_progress_kwarg_when_unsupported(spark, tmp_path):
    class _Old(_FakeEarthaccess):
        def download(self, granules, local_path=None, threads=8):  # no **kw
            return []

    client, rows = _one_row_client(
        _Old([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    res = client.download(rows, str(tmp_path / "o"), _validate_nonempty, spark=spark)
    assert not res.collect()[0]["is_out_file_valid"]  # no TypeError


def _wrapped(msg, cause):
    err = RuntimeError(msg)
    err.__cause__ = cause
    return err


class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code


class _HttpError(Exception):
    def __init__(self, msg, status_code):
        super().__init__(msg)
        self.response = _Resp(status_code)


@pytest.mark.parametrize(
    "exc",
    [
        EulaNotAccepted("Eula Acceptance Failure for https://x/A_data.tif"),
        RuntimeError("403 Forbidden: You must accept the EULA for this dataset"),
        _HttpError("Forbidden: accept the EULA first", 403),
        RuntimeError("download failed: EulaNotAccepted: Eula Acceptance Failure"),
        _wrapped("pqdm worker failed", EulaNotAccepted("refused")),
        # the class name alone is conclusive (earthaccess raises it only on 401/403)
        RuntimeError("worker error: EulaNotAccepted"),
        RuntimeError("Status code: 403, EULA not accepted for https://x/a.tif"),
    ],
)
def test_download_raises_on_eula_with_fix(spark, tmp_path, exc):
    from databricks.labs.gbx.earthdata import EarthdataAuthorizationError

    class _Eula(_FakeEarthaccess):
        def download(self, granules, local_path=None, threads=8, **kw):
            raise exc

    client, rows = _one_row_client(
        _Eula([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    with pytest.raises(EarthdataAuthorizationError) as ei:
        client.download(rows, str(tmp_path / "e"), _validate_nonempty, spark=spark)
    assert ei.value.__cause__ is exc
    msg = str(ei.value)
    assert "https://urs.earthdata.nasa.gov/profile" in msg
    assert "Fix:" in msg and "NASA GESDISC DATA ARCHIVE" in msg
    assert "valid files are kept" in msg


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("Download failed for https://x/eula_terms.tif. Status code: 500"),
        _HttpError("server error mentioning the EULA page", 500),
        RuntimeError("403 Forbidden for https://x/eulas/A_data.tif"),
        RuntimeError("EULA text fetched, status 500"),
        RuntimeError("Download failed for https://x/eula/403/A.tif. Status code: 500"),
        RuntimeError("EULA terms at https://x/403/terms missing. Status code: 500"),
    ],
)
def test_download_eula_lookalike_stays_invalid_row(spark, tmp_path, exc):
    class _Boom(_FakeEarthaccess):
        def download(self, granules, local_path=None, threads=8, **kw):
            raise exc

    client, rows = _one_row_client(
        _Boom([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    r = client.download(
        rows, str(tmp_path / "l"), _validate_nonempty, spark=spark
    ).collect()[0]
    assert not r["is_out_file_valid"]


def test_download_passes_progress_to_keyword_only_signature(spark, tmp_path):
    seen = {}

    class _KwOnly(_FakeEarthaccess):
        def download(self, granules, local_path=None, *, show_progress=None):
            seen["show_progress"] = show_progress
            return []

    client, rows = _one_row_client(
        _KwOnly([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    client.download(rows, str(tmp_path / "k"), _validate_nonempty, spark=spark)
    assert seen["show_progress"] is False


def test_ea_sets_tqdm_disable_before_importing_earthaccess(monkeypatch):
    import builtins

    monkeypatch.delenv("TQDM_DISABLE", raising=False)
    at_import = {}
    real_import = builtins.__import__

    def spy_import(name, *a, **k):
        if name == "earthaccess":
            at_import["v"] = os.environ.get("TQDM_DISABLE")
            return object()
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", spy_import)
    EarthdataClient()._ea()
    assert at_import["v"] == "1"

    monkeypatch.setenv("TQDM_DISABLE", "0")  # user-set value is kept
    EarthdataClient()._ea()
    assert at_import["v"] == "0"


def test_download_other_failure_stays_invalid_row(spark, tmp_path):
    class _Boom(_FakeEarthaccess):
        def download(self, granules, local_path=None, threads=8, **kw):
            raise RuntimeError("Download failed. Status code: 500")

    client, rows = _one_row_client(
        _Boom([_FakeGranule(["https://x/A_data.tif"])]), spark
    )
    r = client.download(
        rows, str(tmp_path / "b"), _validate_nonempty, spark=spark
    ).collect()[0]
    assert not r["is_out_file_valid"]
