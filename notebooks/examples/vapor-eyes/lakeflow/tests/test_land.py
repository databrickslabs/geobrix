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
