import sys, types
from unittest import mock


def _install_fakes():
    # Fake databricks.labs.gbx.sample so land.py imports without the wheel.
    sample = types.ModuleType("databricks.labs.gbx.sample")
    for name in ("TropomiDownloader", "EmitDownloader", "WellsDownloader"):
        setattr(sample, name, mock.MagicMock())
    sys.modules["databricks"] = types.ModuleType("databricks")
    sys.modules["databricks.labs"] = types.ModuleType("databricks.labs")
    sys.modules["databricks.labs.gbx"] = types.ModuleType("databricks.labs.gbx")
    sys.modules["databricks.labs.gbx.sample"] = sample
    stac = types.ModuleType("databricks.labs.gbx.stac")
    stac.StacClient = mock.MagicMock()
    sys.modules["databricks.labs.gbx.stac"] = stac
    return sample


def test_run_land_s5p_calls_tropomi_download():
    sample = _install_fakes()
    from land.land import run_land
    fake_spark = mock.MagicMock()
    tropomi = sample.TropomiDownloader.return_value
    fake_row = {"out_file_path": "/v/s5p/x.nc", "out_file_sz": 123,
                "is_out_file_valid": True}
    tropomi.download.return_value.select.return_value.collect.return_value = [fake_row]
    run_land(fake_spark, ["s5p"], catalog="c", schema="s", volume="data",
             date_window="2023-07-15/2023-08-20", s5p_temporal="2024-08-23/2024-08-24")
    assert tropomi.download.called
    # staged to the vapor-eyes-lf s5p subtree with the s5p_temporal window
    _, kwargs = tropomi.download.call_args
    assert kwargs.get("temporal") == "2024-08-23/2024-08-24"


def test_run_land_s5p_multi_window():
    # Multiple S5P windows -> one download per window, all into the same subtree;
    # staged count is the sum across windows.
    sample = _install_fakes()
    from land.land import run_land
    fake_spark = mock.MagicMock()
    tropomi = sample.TropomiDownloader.return_value
    fake_row = {"out_file_path": "/v/s5p/x.nc", "out_file_sz": 1,
                "is_out_file_valid": True}
    tropomi.download.return_value.select.return_value.collect.return_value = [fake_row]
    wins = ["2026-04-01/2026-06-30", "2024-08-10/2024-08-26", "2025-09-18/2025-09-25"]
    out = run_land(fake_spark, ["s5p"], catalog="c", schema="s", volume="data",
                   date_window="2023-07-15/2023-08-20",
                   s5p_temporal="ignored-when-windows-given",
                   s5p_windows=wins)
    # one download call per window, each with that window's temporal
    assert tropomi.download.call_count == len(wins)
    seen = [kw.get("temporal") for _, kw in tropomi.download.call_args_list]
    assert seen == wins
    # 1 granule per window, summed
    assert out["s5p"] == len(wins)


def test_run_land_emit_multi_window(monkeypatch):
    # Multiple EMIT windows -> one download per window; staged count summed.
    sample = _install_fakes()
    from land.land import run_land
    import land.land as land_mod
    monkeypatch.setattr(land_mod, "_read_earthdata_token", lambda spark, ref: "tok")
    dl = sample.EmitDownloader.return_value.download
    dl.return_value.count.return_value = 4
    wins = ["2023-06-01/2023-09-01", "2024-08-01/2024-12-15", "2025-09-01/2025-10-01"]
    fake = mock.MagicMock()
    out = run_land(fake, ["emit"], catalog="c", schema="s", volume="data",
                   date_window="ignored", s5p_temporal="x", emit_windows=wins)
    assert dl.call_count == len(wins)
    seen = [kw.get("temporal") for _, kw in dl.call_args_list]
    assert seen == wins
    assert out["emit"] == 4 * len(wins)


def test_run_land_emit_wells():
    sample = _install_fakes()
    from land.land import run_land
    fake = mock.MagicMock()
    sample.EmitDownloader.return_value.download.return_value.count.return_value = 2
    sample.WellsDownloader.return_value.download.return_value.first.return_value = {
        "feature_count": 500}
    out = run_land(fake, ["emit", "wells"], catalog="c", schema="s", volume="data",
                   date_window="2023-07-15/2023-08-20", s5p_temporal="x")
    assert sample.EmitDownloader.return_value.download.called
    assert sample.WellsDownloader.return_value.download.called
    assert out["wells"] == 500


def test_run_land_emit_reads_secret_and_sets_env(monkeypatch):
    # EMIT path reads the UC secret via dbutils and exports EARTHDATA_TOKEN
    # before invoking the downloader.
    sample = _install_fakes()
    import land.land as land_mod
    from land.land import run_land
    monkeypatch.delenv("EARTHDATA_TOKEN", raising=False)

    fake_dbutils = mock.MagicMock()
    fake_dbutils.secrets.get.return_value = "tok-abc123"
    monkeypatch.setattr(land_mod, "_get_dbutils", lambda spark: fake_dbutils)

    seen = {}

    def _dl(bbox, out_dir, temporal=None, spark=None):
        seen["token"] = __import__("os").environ.get("EARTHDATA_TOKEN")
        return mock.MagicMock()

    sample.EmitDownloader.return_value.download.side_effect = _dl
    sample.EmitDownloader.return_value.download.return_value.count.return_value = 3

    fake = mock.MagicMock()
    run_land(fake, ["emit"], catalog="c", schema="s", volume="data",
             date_window="2023-07-15/2023-08-20", s5p_temporal="x",
             earthdata_secret="geospatial_docs.vapor_eyes.earthdata_token")
    # 3-arg UC-secret overload used with the dotted ref split into parts
    fake_dbutils.secrets.get.assert_called_once_with(
        "geospatial_docs", "vapor_eyes", "earthdata_token")
    # token was exported to the env visible to the downloader
    assert seen["token"] == "tok-abc123"


class _FakeResp:
    def __init__(self, items):
        self._items = items

    def raise_for_status(self):
        pass

    def json(self):
        return {"items": self._items}


def _install_fake_requests(pages):
    """Insert a fake `requests` module whose get() returns successive pages and
    records the params it was called with. Returns (module, calls_list)."""
    calls = []
    fake = types.ModuleType("requests")

    def _get(url, headers=None, params=None, timeout=None):
        calls.append({"url": url, "headers": headers, "params": params})
        idx = len(calls) - 1
        return _FakeResp(pages[idx] if idx < len(pages) else [])

    fake.get = _get
    sys.modules["requests"] = fake
    return fake, calls


def test_land_cm_paginates_and_writes_jsonl(tmp_path):
    import json
    from land.land import _land_cm

    # Page 1 = full 1000 (triggers a second page); page 2 = 1 (< limit -> stops).
    page1 = [{"plume_id": f"p{i}", "scene_timestamp": "2025-06-01T12:00:00Z",
              "emission_auto": 100.0 + i,
              "geometry_json": {"type": "Point", "coordinates": [-103.0, 31.5]}}
             for i in range(1000)]
    page2 = [{"plume_id": "pA", "scene_timestamp": "2026-01-02T00:00:00Z",
              "emission_auto": 57.0,
              "geometry_json": {"type": "Point", "coordinates": [-102.0, 31.0]}}]
    _fake, calls = _install_fake_requests([page1, page2])

    n = _land_cm(str(tmp_path), (-104.5, 30.8, -101.0, 33.0),
                 "2024-01-01/2026-07-14", "tok-xyz")

    assert n == 1001
    # bbox went as FOUR repeated params (not a comma string); Bearer token set.
    p = calls[0]["params"]
    bbox_vals = [v for (k, v) in p if k == "bbox"]
    assert bbox_vals == [-104.5, 30.8, -101.0, 33.0]
    assert dict(p)["datetime"] == "2024-01-01/2026-07-14"
    assert dict(p)["plume_gas"] == "CH4"
    assert calls[0]["headers"]["Authorization"] == "Bearer tok-xyz"
    # two pages fetched (offset advanced by limit on the second call)
    assert len(calls) == 2
    assert dict(calls[1]["params"])["offset"] == 1000
    # JSONL written to the window-tagged path, one object per line, geometry stringified
    out = tmp_path / "cm_plumes_2024-01-01_2026-07-14.jsonl"
    assert out.exists()
    lines = out.read_text().splitlines()
    assert len(lines) == 1001
    rec = json.loads(lines[0])
    assert isinstance(rec["geometry_json"], str)
    assert json.loads(rec["geometry_json"])["type"] == "Point"


def test_run_land_cm_reads_secret_and_lands(monkeypatch):
    _install_fakes()
    import land.land as land_mod
    from land.land import run_land
    monkeypatch.setattr(land_mod, "_read_secret", lambda spark, ref: "cm-tok")
    seen = {}

    def _fake_cm(cm_dir, bbox, cm_window, token):
        seen.update(cm_dir=cm_dir, bbox=bbox, cm_window=cm_window, token=token)
        return 7

    monkeypatch.setattr(land_mod, "_land_cm", _fake_cm)
    fake = mock.MagicMock()
    out = run_land(fake, ["cm"], catalog="geospatial_docs", schema="vapor_eyes_lf",
                   volume="data", date_window="x", s5p_temporal="x",
                   cm_secret="geospatial_docs.vapor_eyes.carbon_mapper_token",
                   cm_window="2024-01-01/2026-07-14")
    assert out["cm"] == 7
    # landed under the vapor-eyes-lf/cm subtree with the token + window threaded through
    assert seen["cm_dir"] == (
        "/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/cm")
    assert seen["token"] == "cm-tok"
    assert seen["cm_window"] == "2024-01-01/2026-07-14"


def test_run_land_cm_skips_without_token(monkeypatch):
    _install_fakes()
    import land.land as land_mod
    from land.land import run_land
    monkeypatch.setattr(land_mod, "_read_secret", lambda spark, ref: None)
    fake = mock.MagicMock()
    out = run_land(fake, ["cm"], catalog="c", schema="s", volume="data",
                   date_window="x", s5p_temporal="x")
    assert out["cm"] == 0


def test_run_land_emit_continues_when_secret_absent(monkeypatch):
    # If the secret cannot be read, EMIT download is still attempted (fails
    # loudly on its own); S5P/wells are unaffected. run_land must not raise.
    sample = _install_fakes()
    import land.land as land_mod
    from land.land import run_land
    monkeypatch.delenv("EARTHDATA_TOKEN", raising=False)
    monkeypatch.setattr(land_mod, "_read_earthdata_token", lambda spark, ref: None)
    sample.EmitDownloader.return_value.download.return_value.count.return_value = 0
    fake = mock.MagicMock()
    out = run_land(fake, ["emit"], catalog="c", schema="s", volume="data",
                   date_window="2023-07-15/2023-08-20", s5p_temporal="x")
    assert sample.EmitDownloader.return_value.download.called
    assert "EARTHDATA_TOKEN" not in __import__("os").environ
    assert out["emit"] == 0


def test_subtree_includes_context():
    from land.land import _subtree
    dirs = _subtree("cat", "sch", "vol")
    assert dirs["context"].endswith("/vapor-eyes-lf/context")


def test_eia_plays_url_is_permian_geojson():
    from land.land import _EIA_PLAYS_URL
    assert _EIA_PLAYS_URL.startswith("https://hub.arcgis.com/api/download/v1/items/")
    assert "geojson" in _EIA_PLAYS_URL


def test_tiger_counties_url():
    from land.land import _TIGER_COUNTIES_URL
    assert _TIGER_COUNTIES_URL == (
        "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"
    )


def test_land_context_guarded_on_download_error(tmp_path, monkeypatch):
    """A download failure must not raise — it logs and returns 0."""
    import land.land as L
    def boom(*a, **k):
        raise RuntimeError("network down")
    monkeypatch.setattr(L, "_http_get_to_file", boom)
    n = L._land_context(str(tmp_path))
    assert n == 0


def test_land_context_partial_failure_still_lands_other(tmp_path, monkeypatch):
    """The two sources are guarded INDEPENDENTLY: one failing must not stop the
    other from being attempted. EIA down + TIGER up -> exactly one file lands."""
    import land.land as L

    def selective(url, dst, timeout=180):
        if "arcgis" in url:            # EIA plays -> fail
            raise RuntimeError("EIA down")
        open(dst, "wb").close()        # TIGER counties -> succeed

    monkeypatch.setattr(L, "_http_get_to_file", selective)
    n = L._land_context(str(tmp_path))
    assert n == 1
