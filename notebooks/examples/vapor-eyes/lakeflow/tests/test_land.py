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
