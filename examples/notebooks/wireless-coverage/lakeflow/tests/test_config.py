import types, sys, importlib.util, pathlib


def _load_config():
    p = pathlib.Path(__file__).parents[1] / "transformations" / "_config.py"
    spec = importlib.util.spec_from_file_location("_wc_config", p)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


class FakeConf:
    def __init__(self, d): self.d = d
    def get(self, k, default=None): return self.d.get(k, default)


class FakeSpark:
    def __init__(self, d): self.conf = FakeConf(d)


def test_defaults_full_aoi():
    cfg = _load_config().cfg(FakeSpark({}))
    assert cfg["catalog"] == "geospatial_docs"
    assert cfg["schema"] == "wireless_coverage_lf"
    assert cfg["volume"] == "data"
    assert cfg["full_aoi"] is True
    assert cfg["bbox"] == (-122.55, 37.70, -122.35, 37.85)
    assert cfg["viewshed_res"] == 12 and cfg["surface_bin_res"] == 13
    assert cfg["quickpass_res"] == 10 and cfg["tower_res"] == 9
    assert cfg["breaks_m"] == [0.0, 30.0, 60.0, 90.0, 120.0, 150.0, 180.0, 210.0, 240.0, 270.0, 300.0]


def test_derived_paths_default_lf_volume():
    m = _load_config()
    p = m.paths(FakeSpark({}))
    assert p["laz"] == "/Volumes/geospatial_docs/wireless_coverage_lf/data/wireless-coverage-lf/lidar/sf/laz"


def test_laz_dir_override_shares_series_volume():
    m = _load_config()
    shared = "/Volumes/geospatial_docs/wireless_coverage/data/lidar/sf/laz"
    p = m.paths(FakeSpark({"wireless_coverage.laz_dir": shared}))
    assert p["laz"] == shared


def test_demo_aoi_window():
    cfg = _load_config().cfg(FakeSpark({"wireless_coverage.full_aoi": "false"}))
    assert cfg["full_aoi"] is False
    assert cfg["bbox"] != (-122.55, 37.70, -122.35, 37.85)
