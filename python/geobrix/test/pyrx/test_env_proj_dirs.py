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

from databricks.labs.gbx.ds import _listing
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


_REAL_PYPROJ_HOOK = _env._register_pyproj_search_dirs


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
    # GDAL's PROJ search path is process-wide: never point it at this stand-in dir.
    monkeypatch.setattr(_env, "_set_rasterio_proj_search_path", lambda path: None)
    monkeypatch.setattr(_env, "_PROJ_DATA_DIRS", {})
    monkeypatch.setattr(_env, "_PROJ_DATA_BASES", {})
    monkeypatch.setattr(_env, "_PROJ_DATA_RETRY_AT", {})
    monkeypatch.setattr(_env, "_WARNED_UNLISTABLE", set())
    monkeypatch.setattr(_env, "_PROJ_DATA_BUILDING", set())
    monkeypatch.setattr(_listing.time, "sleep", lambda _s: None)  # no retry backoff
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


def test_a_replace_with_fewer_dirs_drops_the_grids_registered_before(
    monkeypatch, base, tmp_path
):
    """register_proj_grids(..., replace=True) with fewer dirs, then none: PROJ_DATA, GDAL's search path and
    pyproj's drop the grids registered before (the base is PROJ_DATA again once none is left).
    """
    import pyproj.datadir

    osr_paths, pyproj_dir = [], {"dir": str(base)}
    monkeypatch.setattr(
        _env, "_set_osr_search_paths", lambda d: osr_paths.append(list(d))
    )
    monkeypatch.setattr(
        _env, "_register_pyproj_search_dirs", _REAL_PYPROJ_HOOK, raising=True
    )
    monkeypatch.setattr(_env, "_PYPROJ_ADDED", [], raising=False)
    monkeypatch.setattr(pyproj.datadir, "get_data_dir", lambda: pyproj_dir["dir"])
    monkeypatch.setattr(
        pyproj.datadir, "set_data_dir", lambda d: pyproj_dir.update(dir=d)
    )
    a = _mkdir_with(tmp_path / "a", "a.gsb")
    b = _mkdir_with(tmp_path / "b", "b.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(a), str(b)])
    _env.configure_gdal_env(extra_proj_dirs=[str(b)])
    assert not os.path.exists(os.path.join(os.environ["PROJ_DATA"], "a.gsb"))
    assert pyproj_dir["dir"].split(os.pathsep) == [str(base), str(b)]
    _env.configure_gdal_env(extra_proj_dirs=[])
    assert os.environ["PROJ_DATA"] == str(base) and osr_paths[-1] == [str(base)]
    assert pyproj_dir["dir"] == str(base)


def test_a_readable_proj_data_of_the_users_is_kept(monkeypatch, base, tmp_path):
    mine = _mkdir_with(tmp_path / "mine", "proj.db")
    monkeypatch.setenv("PROJ_DATA", str(mine))
    _env.configure_gdal_env(extra_proj_dirs=[])
    assert os.environ["PROJ_DATA"] == str(mine)


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


def test_a_users_grid_directory_survives_activation_and_the_registered_union(
    monkeypatch, base, tmp_path
):
    # A user's PROJ_DATA of a grid directory then the PROJ data (/my/grids:/usr/share/proj): the payload's activation
    # keeps the grid directory (it holds no proj.db, but it is readable), and register_proj_grids' union of the
    # registered dirs and that base still reaches the user's grid, the registered one and proj.db.
    from databricks.labs.gbx._native import _rules

    user_grids = _mkdir_with(tmp_path / "my_grids", "user.gsb")
    env = {"PROJ_DATA": os.pathsep.join([str(user_grids), str(base)])}
    _rules.set_data_dirs(env, str(tmp_path / "payload_share"))
    assert env["PROJ_DATA"] == os.pathsep.join([str(user_grids), str(base)])
    monkeypatch.setenv("PROJ_DATA", env["PROJ_DATA"])
    registered = _mkdir_with(tmp_path / "registered", "reg.gsb")
    _env.configure_gdal_env(extra_proj_dirs=[str(registered)])
    proj_data = os.environ["PROJ_DATA"]
    assert _resolves_to(proj_data, "user.gsb", user_grids)
    assert _resolves_to(proj_data, "reg.gsb", registered)
    assert _resolves_to(proj_data, "proj.db", base)


def test_a_joined_proj_data_becomes_one_directory_with_no_registered_grids(
    monkeypatch, base, tmp_path
):
    # The activation leaves a grid directory ahead of the PROJ data as an os.pathsep list (PROJ reads that), but
    # rasterio takes PROJ_DATA as ONE path ("Cannot find proj.db"): with no grid registered, configure_gdal_env
    # still exports one directory.
    user_grids = _mkdir_with(tmp_path / "my_grids", "user.gsb")
    monkeypatch.setenv("PROJ_DATA", os.pathsep.join([str(user_grids), str(base)]))
    rasterio_paths = []
    monkeypatch.setattr(_env, "_set_rasterio_proj_search_path", rasterio_paths.append)
    monkeypatch.setattr(_env, "_RASTERIO_PROJ_PATH", None)
    _env.configure_gdal_env(extra_proj_dirs=[])
    proj_data = os.environ["PROJ_DATA"]
    assert os.pathsep not in proj_data
    assert _resolves_to(proj_data, "user.gsb", user_grids)
    assert _resolves_to(proj_data, "proj.db", base)
    assert rasterio_paths == [proj_data]
    _env.configure_gdal_env(extra_proj_dirs=[])  # per-row callers: same directory
    assert os.environ["PROJ_DATA"] == proj_data


def test_missing_registered_dir_is_skipped(base, tmp_path):
    missing = tmp_path / "does_not_exist"
    with pytest.warns(RuntimeWarning, match="does_not_exist"):
        _env.configure_gdal_env(extra_proj_dirs=[str(missing)])
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


def _fail_listdir(monkeypatch, target, failures):
    """Make ``os.listdir(target)`` raise OSError ``failures`` times (None = always).

    Returns ``(attempts, heal)``: the attempts made on ``target`` and a function that
    makes it list normally again. Other paths list normally throughout.
    """
    real = os.listdir
    attempts = []

    def _listdir(path="."):
        if os.fspath(path) == str(target):
            attempts.append(path)
            if failures is None or len(attempts) <= failures:
                raise FileNotFoundError(2, "transient FUSE miss", str(path))
        return real(path)

    monkeypatch.setattr(os, "listdir", _listdir)
    return attempts, lambda: monkeypatch.setattr(os, "listdir", real)


def test_transient_listdir_failure_is_retried(monkeypatch, base, tmp_path):
    """A one-off listing failure on a registered dir (UC Volume FUSE lag) must not drop
    its grids for the life of the process: the listing is retried like every other
    FUSE read."""
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    attempts, _ = _fail_listdir(monkeypatch, grids, failures=2)
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # recovered: nothing to warn about
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert len(attempts) == 3
    proj_data = os.environ["PROJ_DATA"]
    assert _resolves_to(proj_data, "a.gsb", grids)
    assert _resolves_to(proj_data, "proj.db", base)


def test_unlistable_dir_warns_once_and_is_retried_later(monkeypatch, base, tmp_path):
    """A dir that stays unlistable is named in ONE warning, proj.db stays reachable,
    and the failure is not cached for good: once the dir lists, its grids appear."""
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    attempts, heal = _fail_listdir(monkeypatch, grids, failures=None)
    clock = [1000.0]
    monkeypatch.setattr(_env, "_now", lambda: clock[0])
    with pytest.warns(RuntimeWarning, match="grids") as caught:
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert [str(grids) in str(w.message) for w in caught] == [True]
    degraded = os.environ["PROJ_DATA"]
    assert _resolves_to(degraded, "proj.db", base)
    assert not os.path.exists(os.path.join(degraded, "a.gsb"))

    # Per-row callers shortly afterwards: no new listing attempts, no new warning.
    made = len(attempts)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert len(attempts) == made
    assert os.environ["PROJ_DATA"] == degraded

    # Past the retry window and still unlistable: tried again, but not warned again.
    clock[0] += _env._UNLISTABLE_RETRY_S + 1
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert len(attempts) > made

    # The dir becomes listable and the window has passed again: the next call rebuilds
    # with its grids.
    heal()
    clock[0] += _env._UNLISTABLE_RETRY_S + 1
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    recovered = os.environ["PROJ_DATA"]
    assert recovered != degraded
    assert _resolves_to(recovered, "a.gsb", grids)
    assert _resolves_to(recovered, "proj.db", base)


def test_a_dir_already_warned_about_is_listed_once_on_the_next_try(
    monkeypatch, base, tmp_path
):
    """The long retry (every FUSE read gets it) runs for a dir's first failure. A dir
    already named in the warning is tried once when its retry window comes round, so
    a permanently missing dir costs a quick miss a minute, not ~20 s of sleeping."""
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    attempts, _ = _fail_listdir(monkeypatch, grids, failures=None)
    clock = [1000.0]
    monkeypatch.setattr(_env, "_now", lambda: clock[0])
    with pytest.warns(RuntimeWarning, match="grids"):
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert len(attempts) == 10  # _retry_transient's default
    clock[0] += _env._UNLISTABLE_RETRY_S + 1
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert len(attempts) == 11  # exactly one more


def test_a_second_caller_is_not_blocked_behind_a_slow_relisting(
    monkeypatch, base, tmp_path
):
    """Listing a dir can sit in retries for ~20 s. That must not happen under the
    process-wide lock: while one caller relists, the others get the build they have."""
    grids = _mkdir_with(tmp_path / "grids", "a.gsb")
    attempts, heal = _fail_listdir(monkeypatch, grids, failures=None)
    clock = [1000.0]
    monkeypatch.setattr(_env, "_now", lambda: clock[0])
    with pytest.warns(RuntimeWarning, match="grids"):
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    degraded = os.environ["PROJ_DATA"]
    heal()

    # Past the window: the next listing of `grids` stalls until released.
    clock[0] += _env._UNLISTABLE_RETRY_S + 1
    entered, release = threading.Event(), threading.Event()
    real = os.listdir

    def _stalled(path="."):
        if os.fspath(path) == str(grids):
            entered.set()
            release.wait(20)
        return real(path)

    monkeypatch.setattr(os, "listdir", _stalled)
    seen = {}

    def _relister():
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])

    def _other():
        _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
        seen["proj_data"] = os.environ["PROJ_DATA"]

    first = threading.Thread(target=_relister)
    second = threading.Thread(target=_other)
    try:
        first.start()
        assert entered.wait(10), "the relisting never started"
        second.start()
        second.join(5)
        assert not second.is_alive(), "a caller waited for the relisting"
        assert seen["proj_data"] == degraded
    finally:
        release.set()
        first.join(20)
        second.join(20)

    # The relisting published its result: the healed dir's grids are in.
    _env.configure_gdal_env(extra_proj_dirs=[str(grids)])
    assert _resolves_to(os.environ["PROJ_DATA"], "a.gsb", grids)


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


_PROCESS_ENV_KEYS = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")


def _gdal_proj_search_paths():
    """GDAL's current PROJ search path (it changes only when a rasterio Env starts);
    empty when rasterio is not installed."""
    try:
        from rasterio._env import get_proj_data_search_paths
    except ImportError:
        return []
    return list(get_proj_data_search_paths())


@pytest.fixture(scope="module", autouse=True)
def _module_leaves_process_state_as_found():
    """The tests here call configure_gdal_env, which changes process-wide state that every
    later test module in the same pytest process (all light dirs share one) would see:
    PROJ_DATA / GDAL_DATA, the merged-dir caches, and GDAL's own PROJ search path (set by
    the next rasterio Env)."""
    found = {k: os.environ.get(k) for k in _PROCESS_ENV_KEYS}
    found_dirs = (dict(_env._PROJ_DATA_DIRS), dict(_env._PROJ_DATA_BASES))
    found_paths = _gdal_proj_search_paths()
    yield
    assert {k: os.environ.get(k) for k in _PROCESS_ENV_KEYS} == found
    assert (dict(_env._PROJ_DATA_DIRS), dict(_env._PROJ_DATA_BASES)) == found_dirs
    # A merged dir this module added, not one it found: a module that ran earlier in
    # the same pytest process may have left one, and that is not this module's leak.
    leaked = [
        p
        for p in _gdal_proj_search_paths()
        if _env._MERGED_PREFIX in p and p not in found_paths
    ]
    assert not leaked, f"GDAL's PROJ search path now also points at {leaked}"


@pytest.fixture
def restored_process_state(monkeypatch):
    """Clean per-process caches for the test, and afterwards the original PROJ_DATA /
    GDAL_DATA / PROJ_LIB and GDAL's PROJ search path (re-applied by a final rasterio Env).
    """
    rasterio = pytest.importorskip("rasterio")
    saved = {k: os.environ.get(k) for k in _PROCESS_ENV_KEYS}
    monkeypatch.setattr(_env, "_PROJ_DATA_DIRS", {})
    monkeypatch.setattr(_env, "_PROJ_DATA_BASES", {})
    monkeypatch.setattr(_env, "_PROJ_DATA_RETRY_AT", {})
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    with rasterio.Env():  # applies the restored PROJ_DATA as GDAL's search path
        pass


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


def test_raster_tier_keeps_proj_db_and_finds_registered_grid(
    monkeypatch, tmp_path, restored_process_state
):
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


def test_core_crs_imports_in_a_fresh_interpreter_when_proj_data_is_unlistable(tmp_path):
    """core.crs configures GDAL at import; with a PROJ_DATA this process cannot list, that builds the merged
    dir through ``_list_dir``. If ``_list_dir`` imported ``ds._listing``, the ds package would register every data
    source, one needs the half-imported core.crs, and the import fails with an ImportError (only a fresh
    interpreter shows it: in-process, ``ds`` is already loaded)."""
    import subprocess
    import sys

    import databricks.labs.gbx as gbx

    src = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(gbx.__file__)))
    )
    child = (
        "import sys, time\n"
        f"sys.path.insert(0, {src!r})\n"
        "time.sleep = lambda _s: None\n"  # no transient-retry backoff
        "import databricks.labs.gbx.core.crs\n"
    )
    env = {
        k: v for k, v in os.environ.items() if k not in ("PROJ_LIB", "PYTHONPATH")
    }
    env["PROJ_DATA"] = str(tmp_path / "does-not-exist")
    env["GBX_LOCAL_TEMP_DIR"] = str(tmp_path / "local_tmp")
    run = subprocess.run(
        [sys.executable, "-I", "-c", child],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    assert run.returncode == 0, run.stderr[-2000:]
    assert "ImportError" not in run.stderr, run.stderr[-2000:]
