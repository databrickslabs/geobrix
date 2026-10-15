import os
import shutil
import tempfile
from pathlib import Path

import pytest

from databricks.labs.gbx.pyrx.core.local_temp import (
    gc_stale_local_temp,
    local_temp_root,
    new_local_temp_dir,
    new_local_temp_file,
)


def test_root_defaults_to_gettempdir(monkeypatch):
    monkeypatch.delenv("GBX_LOCAL_TEMP_DIR", raising=False)
    assert local_temp_root() == tempfile.gettempdir()


def test_root_honors_env(monkeypatch, tmp_path):
    target = tmp_path / "gbxtmp"
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(target))
    root = local_temp_root()
    assert root == str(target)
    assert os.path.isdir(root)  # created


def test_root_sanitizes_whitespace_and_bom(monkeypatch, tmp_path):
    target = tmp_path / "gbxtmp"
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", f"﻿  {target}  ​")
    assert local_temp_root() == str(target)


def test_root_empty_after_strip_falls_back(monkeypatch):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", "  ﻿ ​ ")
    assert local_temp_root() == tempfile.gettempdir()


def test_root_unusable_env_warns_and_falls_back(monkeypatch, tmp_path):
    # A path whose parent is a file cannot be made into a dir.
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(blocker / "sub"))
    with pytest.warns(RuntimeWarning):
        assert local_temp_root() == tempfile.gettempdir()


def test_new_local_temp_dir_creates_unique_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    a = new_local_temp_dir("gbx_x_")
    b = new_local_temp_dir("gbx_x_")
    assert a != b
    assert os.path.isdir(a) and os.path.isdir(b)
    assert Path(a).parent == tmp_path and os.path.basename(a).startswith("gbx_x_")


def test_new_local_temp_file_creates_empty_under_root(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    p = new_local_temp_file(suffix=".nc", prefix="gbx_y_")
    assert os.path.isfile(p) and os.path.getsize(p) == 0
    assert p.endswith(".nc") and os.path.basename(p).startswith("gbx_y_")
    assert Path(p).parent == tmp_path
    # fd not leaked: we can reopen and write
    with open(p, "wb") as f:
        f.write(b"data")
    assert os.path.getsize(p) == 4


def test_gc_stale_local_temp_uses_root_and_ages(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    stale = new_local_temp_dir("gbx_gc_")
    old = __import__("time").time() - 10_000
    os.utime(stale, (old, old))
    fresh = new_local_temp_dir("gbx_gc_")
    gc_stale_local_temp("gbx_gc_", ttl_seconds=3600)
    assert not os.path.exists(stale)
    assert os.path.exists(fresh)


def test_gc_stale_local_temp_missing_root_no_raise(monkeypatch, tmp_path):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "does_not_exist"))
    # resolver will create it; removing then GC'ing must not raise
    root = local_temp_root()
    os.rmdir(root)
    gc_stale_local_temp("gbx_gc_")  # no raise


# ---------------------------------------------------------------------------
# StageCache: worker-local copies of source files (light readers)
# ---------------------------------------------------------------------------


def _src(tmp_path, name, nbytes):
    p = tmp_path / "src" / name
    p.parent.mkdir(exist_ok=True)
    p.write_bytes(b"x" * nbytes)
    return str(p)


def _cache(monkeypatch, tmp_path, cap_bytes=None):
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    if cap_bytes is not None:
        monkeypatch.setattr(lt, "_stage_cap_bytes", lambda cache_dir, cached: cap_bytes)
    copies = []

    def copy(src, dst):
        copies.append(src)
        shutil.copy(src, dst)

    return lt.StageCache("gbx_test_stage_", copy), copies


def test_stage_cache_reuses_copy_per_source(monkeypatch, tmp_path):
    cache, copies = _cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.gpkg", 10)
    with cache.use(a) as p1:
        assert Path(p1).read_bytes() == b"x" * 10
    with cache.use(a) as p2:
        pass
    assert p1 == p2 and copies == [a]


def test_stage_cache_dir_names_owner_and_sweeps_dead_owners(monkeypatch, tmp_path):
    """The cache dir carries the owning pid; first use removes dirs of the same
    prefix whose owner process is gone (PySpark ends workers with os._exit, so
    atexit cleanup never runs) and keeps those of live processes."""
    root = tmp_path / "root"
    root.mkdir()
    dead = root / "gbx_test_stage_999999999_old"
    live = root / f"gbx_test_stage_{os.getppid()}_other"
    dead.mkdir()
    live.mkdir()
    cache, _ = _cache(monkeypatch, tmp_path)
    with cache.use(_src(tmp_path, "a.gpkg", 1)) as p:
        assert os.path.basename(os.path.dirname(p)).startswith(
            f"gbx_test_stage_{os.getpid()}_"
        )
    assert not dead.exists() and live.exists()


def test_stage_cache_evicts_least_recently_used_idle_copies(monkeypatch, tmp_path):
    cache, copies = _cache(monkeypatch, tmp_path, cap_bytes=25)
    a, b, c = (_src(tmp_path, f"{n}.gpkg", 10) for n in "abc")
    with cache.use(a) as pa:
        pass
    with cache.use(b):
        pass
    with cache.use(a):  # a is now most recently used
        pass
    with cache.use(c):  # 30 > 25: evict the LRU idle copy (b)
        pass
    assert os.path.exists(pa)
    with cache.use(b):  # b was evicted -> copied again
        pass
    assert copies.count(b) == 2 and copies.count(a) == 1


def test_stage_cache_never_evicts_a_copy_in_use(monkeypatch, tmp_path):
    cache, _ = _cache(monkeypatch, tmp_path, cap_bytes=15)
    a, b = _src(tmp_path, "a.gpkg", 10), _src(tmp_path, "b.gpkg", 10)
    with cache.use(a) as pa:
        with cache.use(b) as pb:  # over the cap, but a is in use
            assert os.path.exists(pa) and os.path.exists(pb)


def test_stage_cache_stages_a_source_larger_than_the_cap(monkeypatch, tmp_path):
    cache, _ = _cache(monkeypatch, tmp_path, cap_bytes=5)
    with cache.use(_src(tmp_path, "big.gpkg", 50)) as p:
        assert os.path.getsize(p) == 50


def test_stage_cap_honors_env(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_STAGE_CACHE_MB", "3")
    assert lt._stage_cap_bytes(str(tmp_path), 0) == 3 * 1024 * 1024
    monkeypatch.delenv("GBX_STAGE_CACHE_MB")
    free = shutil.disk_usage(str(tmp_path)).free
    assert lt._stage_cap_bytes(str(tmp_path), 100) == (free + 100) // 2
