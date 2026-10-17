"""configure_gdal_env: registered PROJ grid dirs + the base PROJ data, as ONE directory.

rasterio (1.5) reads ``PROJ_DATA`` as a single search path: every new ``rasterio.Env``
passes the whole value to ``OSRSetPROJSearchPaths`` as one entry, with no splitting on
``os.pathsep``. A colon-joined ``PROJ_DATA`` therefore points at a directory that does not
exist, so any thread that opens proj.db afterwards fails with "Cannot find proj.db" and
the registered grids are not found either. configure_gdal_env must export one real
directory that holds the base PROJ data and the registered grid files.
"""

import os
import threading
import warnings

import pytest

from databricks.labs.gbx.pyrx import _env


def _mkdir_with(path, *names):
    path.mkdir(parents=True, exist_ok=True)
    for n in names:
        (path / n).write_text(f"{path.name}/{n}")
    return path


def _resolves_to(proj_data, name, owner_dir):
    """True when ``<proj_data>/<name>`` is the file ``<owner_dir>/<name>``."""
    entry = os.path.join(proj_data, name)
    return os.path.exists(entry) and os.path.samefile(entry, owner_dir / name)


@pytest.fixture
def base(monkeypatch, tmp_path):
    """A stand-in bundled PROJ data dir; overlays are built under tmp_path."""
    bundled = _mkdir_with(tmp_path / "bundled", "proj.db", "proj.ini")
    monkeypatch.setenv("PROJ_DATA", str(bundled))
    monkeypatch.delenv("PROJ_LIB", raising=False)
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "local_tmp"))
    monkeypatch.setattr(_env, "_bundled_proj_data", lambda: str(bundled))
    monkeypatch.setattr(_env, "_bundled_gdal_data", lambda: None)
    monkeypatch.setattr(_env, "_register_pyproj_search_dirs", lambda dirs: None)
    monkeypatch.setattr(_env, "_PROJ_DATA_DIRS", {})
    monkeypatch.setattr(_env, "_PROJ_DATA_BASES", {})
    return bundled


def test_registered_dir_and_base_exported_as_one_directory(base, tmp_path):
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    proj_data = os.environ["PROJ_DATA"]
    assert os.pathsep not in proj_data
    assert os.path.isdir(proj_data)
    assert _resolves_to(proj_data, "proj.db", base)
    assert _resolves_to(proj_data, "proj.ini", base)
    assert _resolves_to(proj_data, "a.gsb", grids)


def test_earlier_registered_dirs_win_name_collisions(base, tmp_path):
    a = _mkdir_with(tmp_path / "a", "shared.gsb", "only_a.gsb")
    b = _mkdir_with(tmp_path / "b", "shared.gsb", "only_b.gsb")
    _mkdir_with(base, "shared.gsb")  # registered grids also win over the base
    _env.configure_gdal_env(extra_proj_dirs=[str(a), str(b)])
    proj_data = os.environ["PROJ_DATA"]
    assert _resolves_to(proj_data, "shared.gsb", a)
    assert _resolves_to(proj_data, "only_a.gsb", a)
    assert _resolves_to(proj_data, "only_b.gsb", b)
    assert _resolves_to(proj_data, "proj.db", base)


def test_repeat_calls_reuse_the_same_directory(base, tmp_path):
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    first = os.environ["PROJ_DATA"]
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert os.environ["PROJ_DATA"] == first


def test_newly_registered_dir_is_added_on_a_later_call(base, tmp_path):
    a = _mkdir_with(tmp_path / "a", "a.gsb")
    b = _mkdir_with(tmp_path / "b", "b.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(a)])
    _env.configure_gdal_env(extra_proj_dirs=[str(a), str(b)])
    proj_data = os.environ["PROJ_DATA"]
    assert _resolves_to(proj_data, "a.gsb", a)
    assert _resolves_to(proj_data, "b.gsb", b)
    assert _resolves_to(proj_data, "proj.db", base)


def test_empty_list_is_noop(base):
    _env.configure_gdal_env(extra_proj_dirs=[])
    assert os.environ["PROJ_DATA"] == str(base)


def test_none_reads_registry(monkeypatch, base, tmp_path):
    from databricks.labs.gbx.core import proj_grids

    grids = _mkdir_with(tmp_path / "reg", "r.gsb")
    monkeypatch.setattr(proj_grids, "get_registered_dirs", lambda: [str(grids)])
    _env.configure_gdal_env()  # None → read registry
    assert _resolves_to(os.environ["PROJ_DATA"], "r.gsb", grids)


def test_multi_path_base_is_merged_too(monkeypatch, base, tmp_path):
    extra_base = _mkdir_with(tmp_path / "extra_base", "extra.json")
    monkeypatch.setenv("PROJ_DATA", os.pathsep.join([str(base), str(extra_base)]))
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    proj_data = os.environ["PROJ_DATA"]
    assert os.pathsep not in proj_data
    assert _resolves_to(proj_data, "proj.db", base)
    assert _resolves_to(proj_data, "extra.json", extra_base)


def test_missing_registered_dir_is_skipped(base, tmp_path):
    _env.configure_gdal_env(extra_proj_dirs=[str(tmp_path / "does_not_exist")])
    proj_data = os.environ["PROJ_DATA"]
    assert os.pathsep not in proj_data
    assert _resolves_to(proj_data, "proj.db", base)


def test_unknown_base_leaves_proj_data_unset(monkeypatch, base, tmp_path):
    """No known PROJ data dir: exporting grids alone would hide proj.db, so leave it."""
    monkeypatch.delenv("PROJ_DATA", raising=False)
    monkeypatch.setattr(_env, "_bundled_proj_data", lambda: None)
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    with pytest.warns(RuntimeWarning, match="PROJ data directory"):
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert "PROJ_DATA" not in os.environ


def test_merge_failure_falls_back_to_the_base(monkeypatch, base, tmp_path):
    """If the merged dir cannot be built, keep proj.db reachable (never a joined list)."""

    def _no_symlinks(*_a, **_k):
        raise OSError("symlinks unsupported")

    monkeypatch.setattr(_env.os, "symlink", _no_symlinks)
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    with pytest.warns(RuntimeWarning, match="grid"):
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert os.environ["PROJ_DATA"] == str(base)
    # Per-row callers: the failed build is remembered, not retried (and re-warned).
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert os.environ["PROJ_DATA"] == str(base)


# ---------------------------------------------------------------------------
# End to end against the real rasterio wheel (the failure the light CI run hit).
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
GRID_DIR = os.path.join(
    _REPO_ROOT,
    "sample-data",
    "Volumes",
    "main",
    "geobrix_samples",
    "geobrix-examples",
    "proj-grids",
)
# synthetic.gsb applies a constant +30 arc-second latitude shift inside its box.
GRID_CRS = "+proj=longlat +ellps=GRS80 +nadgrids=synthetic.gsb +no_defs"
EXPECT_LAT = 51.5 + 30.0 / 3600.0


def _in_new_thread(fn):
    """Run ``fn`` on a fresh thread: a new GDAL thread gets a new PROJ context, which
    must find proj.db through the current search path (an already-open one would not).
    """
    out = {}

    def _run():
        try:
            out["value"] = fn()
        except Exception as e:  # surfaced by the assertion below
            out["error"] = e

    t = threading.Thread(target=_run)
    t.start()
    t.join()
    if "error" in out:
        raise out["error"]
    return out["value"]


def test_raster_tier_keeps_proj_db_and_finds_registered_grid(monkeypatch, tmp_path):
    rasterio = pytest.importorskip("rasterio")
    from rasterio.warp import transform

    from databricks.labs.gbx.core.crs import resolve_crs

    assert os.path.isfile(os.path.join(GRID_DIR, "synthetic.gsb"))
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "local_tmp"))
    # Raster tier only: keep pyproj's process-global search path untouched, or the grid
    # stays visible to later pyproj tests in this process (test/pyvx's no-grid control).
    monkeypatch.setattr(_env, "_register_pyproj_search_dirs", lambda dirs: None)
    _env.configure_gdal_env(extra_proj_dirs=[GRID_DIR])

    def _lookup_and_transform():
        with rasterio.Env():  # a new Env re-applies PROJ_DATA as rasterio's search path
            epsg = resolve_crs(32618).to_epsg()
            _, ys = transform(resolve_crs(GRID_CRS), resolve_crs(4326), [0.0], [51.5])
        return epsg, ys[0]

    epsg, lat = _in_new_thread(_lookup_and_transform)
    assert epsg == 32618
    assert lat == pytest.approx(EXPECT_LAT, abs=1e-6)
