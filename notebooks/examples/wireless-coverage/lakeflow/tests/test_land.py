import importlib.util
import pathlib


def _load():
    p = pathlib.Path(__file__).parents[1] / "land" / "land.py"
    s = importlib.util.spec_from_file_location("_wc_land", p)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


def test_parse_args_defaults():
    a = _load().parse_args(
        [
            "--catalog",
            "c",
            "--schema",
            "s",
            "--volume",
            "v",
            "--full-aoi",
            "true",
            "--laz-dir",
            "",
            "--water-mask-dir",
            "",
        ]
    )
    assert a.catalog == "c" and a.full_aoi == "true"


def test_already_staged_skips(tmp_path, monkeypatch):
    m = _load()
    laz = tmp_path / "laz"
    laz.mkdir()
    (laz / "0-0-0-0.laz").write_bytes(b"x")
    called = {"n": 0}

    def _boom(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(m, "_download_lidar", _boom)
    m.stage_lidar(aoi=(-122.55, 37.70, -122.35, 37.85), out_dir=str(laz), spark=None)
    assert called["n"] == 0  # files present → no download


def test_water_already_staged_skips(tmp_path, monkeypatch):
    m = _load()
    water = tmp_path / "water"
    water.mkdir()
    (water / "water.parquet").write_bytes(b"x")
    called = {"n": 0}

    def _boom(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(m, "_download_water", _boom)
    m.stage_water(bbox=(-122.55, 37.70, -122.35, 37.85), out_dir=str(water), spark=None)
    assert called["n"] == 0  # files present → no download


def test_water_downloads_when_empty(tmp_path, monkeypatch):
    m = _load()
    water = tmp_path / "water"
    water.mkdir()
    called = {"n": 0}

    def _rec(bbox, out_dir, spark):
        called["n"] += 1

    monkeypatch.setattr(m, "_download_water", _rec)
    m.stage_water(bbox=(-122.55, 37.70, -122.35, 37.85), out_dir=str(water), spark=None)
    assert called["n"] == 1  # empty dir → download invoked
