import os
import shutil
import tempfile
from pathlib import Path
from test._volume_safe import open_raster

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


def test_gc_stale_local_temp_pid_owned_keeps_a_live_owner_however_old(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path))
    old = __import__("time").time() - 10_000
    mine = tmp_path / f"gbx_gc_{os.getpid()}_a"
    unowned = tmp_path / "gbx_gc_nopid"
    gone = tmp_path / "gbx_gc_4194303_a"  # above the pid limit of every kernel here
    for d in (mine, unowned, gone):
        d.mkdir()
        os.utime(d, (old, old))
    gc_stale_local_temp("gbx_gc_", ttl_seconds=3600, pid_owned=True)
    assert mine.exists()  # the owner is alive: age says nothing
    assert not gone.exists()  # the owner is gone
    assert not unowned.exists()  # no pid in the name: judged by age


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


def test_stage_cache_sizes_the_local_copy_not_the_source(monkeypatch, tmp_path):
    """Only copy_fn touches the source (readers wrap it in their transient-retry):
    the cache measures the staged local copy, never the remote source path."""
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    real = _src(tmp_path, "real.gpkg", 7)

    def copy(src, dst):  # the "remote" source exists only for copy_fn
        assert src == "/Volumes/c/s/v/remote.gpkg"
        shutil.copy(real, dst)

    cache = lt.StageCache("gbx_test_stage_", copy)
    with cache.use("/Volumes/c/s/v/remote.gpkg") as p:
        assert os.path.getsize(p) == 7


def _rewrite(path, nbytes, fill=b"y"):
    """Replace a source in place with new bytes and a strictly newer mtime."""
    st = os.stat(path)
    Path(path).write_bytes(fill * nbytes)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))


def _sig_cache(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    copies = []

    def copy(src, dst):
        copies.append(src)
        shutil.copy(src, dst)

    return lt.StageCache("gbx_test_stage_", copy, lt.source_signature), copies


def test_stage_cache_restages_a_source_replaced_in_place(monkeypatch, tmp_path):
    """A source replaced at the same path (new size/mtime) is copied again; the
    superseded idle copy is removed."""
    cache, copies = _sig_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.gpkg", 10)
    with cache.use(a) as p1:
        pass
    _rewrite(a, 4)
    with cache.use(a) as p2:
        assert Path(p2).read_bytes() == b"y" * 4
    assert copies == [a, a] and p1 != p2 and not os.path.exists(p1)


def test_stage_cache_reuses_copy_while_source_unchanged(monkeypatch, tmp_path):
    cache, copies = _sig_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.gpkg", 10)
    with cache.use(a) as p1:
        pass
    with cache.use(a) as p2:
        pass
    assert copies == [a] and p1 == p2


def test_stage_cache_keeps_a_superseded_copy_until_released(monkeypatch, tmp_path):
    cache, _ = _sig_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.gpkg", 10)
    with cache.use(a) as old:
        _rewrite(a, 4)
        with cache.use(a) as new:
            assert old != new and os.path.exists(old)
            assert Path(new).read_bytes() == b"y" * 4
        assert os.path.exists(old)  # still in use by the outer reader
    assert not os.path.exists(old)


def test_source_signature_tracks_directory_contents(tmp_path):
    """A .gdb is a directory: a member rewritten in place changes the signature."""
    from databricks.labs.gbx.pyrx.core.local_temp import source_signature

    d = tmp_path / "x.gdb"
    d.mkdir()
    (d / "a0000001.gdbtable").write_bytes(b"x" * 10)
    before = source_signature(str(d))
    _rewrite(str(d / "a0000001.gdbtable"), 10)
    assert source_signature(str(d)) != before


# The staging helpers every StageCache reader (raster_gbx, pmtiles_gbx) shares.


def _fail_first(monkeypatch, module, name, target, failures):
    """Make ``module.name(target, ...)`` raise a FUSE-style FileNotFoundError for its
    first ``failures`` calls; returns the list of calls made for ``target``."""
    real = getattr(module, name)
    calls = []

    def flaky(path, *args, **kwargs):
        if os.fspath(path) == target:
            calls.append(path)
            if len(calls) <= failures:
                raise FileNotFoundError(2, "transient FUSE miss", target)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(module, name, flaky)
    return calls


def test_copy_source_sequential_copies_and_retries_a_transient_miss(
    monkeypatch, tmp_path
):
    import builtins
    import time

    from databricks.labs.gbx.pyrx.core.local_temp import copy_source_sequential

    monkeypatch.setattr(time, "sleep", lambda _s: None)  # no retry backoff
    src = tmp_path / "s.bin"
    src.write_bytes(b"abc" * 1000)
    dst = tmp_path / "d.bin"
    calls = _fail_first(monkeypatch, builtins, "open", str(src), failures=2)
    copy_source_sequential(str(src), str(dst))
    assert len(calls) == 3
    assert dst.read_bytes() == src.read_bytes()


def test_retried_source_signature_matches_source_signature_after_a_miss(
    monkeypatch, tmp_path
):
    import time

    from databricks.labs.gbx.pyrx.core.local_temp import (
        retried_source_signature,
        source_signature,
    )

    monkeypatch.setattr(time, "sleep", lambda _s: None)
    f = tmp_path / "s.bin"
    f.write_bytes(b"x" * 10)
    expected = source_signature(str(f))
    calls = _fail_first(monkeypatch, os, "stat", str(f), failures=3)
    assert retried_source_signature(str(f)) == expected
    assert len(calls) > 3


def test_stage_cache_stages_the_sidecars_with_the_file_and_removes_them_together(
    monkeypatch, tmp_path
):
    """A raster is staged with the files GDAL reads beside it, under the staged name's prefix; evicting or superseding
    the copy removes them too (a stage that kept them would fill the disk)."""
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    cache = lt.StageCache(
        "gbx_test_stage_", lt.copy_source_with_sidecars, lt.raster_source_signature
    )
    a = _src(tmp_path, "a.bil", 10)
    Path(a).with_suffix(".hdr").write_bytes(b"NROWS 1")
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    with cache.use(a) as p:
        staged = sorted(os.listdir(os.path.dirname(p)))
        prefix = os.path.basename(p)[: -len("a.bil")]
        assert staged == sorted(
            [prefix + n for n in ("a.bil", "a.hdr", "a.bil.aux.xml")]
        )
        assert (
            Path(os.path.dirname(p)) / (prefix + "a.hdr")
        ).read_bytes() == b"NROWS 1"
    _rewrite(a, 4)  # the source is replaced: the superseded copy and its sidecars go
    with cache.use(a) as q:
        assert sorted(os.listdir(os.path.dirname(q))) != staged
        assert not any(
            n.startswith(os.path.basename(p)[:33])
            for n in os.listdir(os.path.dirname(q))
        )


def test_a_failed_sidecar_copy_leaves_nothing_staged_and_the_size_counts_the_sidecars(
    monkeypatch, tmp_path
):
    """The size the cache holds a staged raster at includes its sidecars; a copy that fails part way removes the file
    and the sidecars it had staged (they are in no entry of the cache, so nothing else would).
    """
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    a = _src(tmp_path, "a.bil", 10)
    Path(a).with_suffix(".hdr").write_bytes(b"NROWS 1")
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    folder = tmp_path / "stage"
    folder.mkdir()
    ok = str(folder / ("0" * 32 + "_a.bil"))
    sidecars = lt.copy_source_with_sidecars(a, ok)
    assert len(sidecars) == 2
    assert lt._path_bytes(ok, sidecars) == 10 + len(b"NROWS 1") + len(b"<PAMDataset/>")

    bad = str(folder / ("1" * 32 + "_a.bil"))
    real = lt._copy_sidecars

    def _fail_after_one(source_path, local_path):
        real(source_path, local_path)
        raise OSError("sidecar copy failed")

    monkeypatch.setattr(lt, "_copy_sidecars", _fail_after_one)
    with pytest.raises(OSError):
        lt.copy_source_with_sidecars(a, bad)
    assert all(n.startswith("0" * 32) for n in os.listdir(folder))


def _raster_cache(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    copies = []

    def copy(src, dst):
        copies.append(src)
        return lt.copy_source_with_sidecars(src, dst)

    return lt.StageCache("gbx_test_stage_", copy, lt.raster_source_signature), copies


def test_an_edited_sidecar_beside_an_unchanged_file_is_staged_again(
    monkeypatch, tmp_path
):
    """The signature of a raster takes in its staged sidecars: an ``.aux.xml`` or ``.hdr`` changed alone (the main file
    keeps its size and mtime) gives a new staged copy that holds the new sidecar, and the stale one is removed.
    """
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    cache, copies = _raster_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.bil", 10)
    hdr = Path(a).with_suffix(".hdr")
    hdr.write_bytes(b"NROWS 1")
    with cache.use(a) as p1:
        pass
    with cache.use(a) as same:
        assert same == p1  # nothing changed: the copy is reused
    assert copies == [a]
    st = os.stat(hdr)
    hdr.write_bytes(b"NROWS 22")
    os.utime(hdr, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    with cache.use(a) as p2:
        assert p2 != p1 and copies == [a, a]
        prefix = os.path.basename(p2)[: -len("a.bil")]
        assert (Path(p2).parent / (prefix + "a.hdr")).read_bytes() == b"NROWS 22"
    assert not [
        n for n in os.listdir(Path(p2).parent) if n.startswith(Path(p1).name[:33])
    ]
    # a sidecar that appears (seen once the folder's listing is due again: due at once here), and one that goes, also
    # change the signature
    from databricks.labs.gbx.pyrx import _env

    monkeypatch.setattr(_env, "_LISTINGS", _env._FolderListings(relist_interval_s=0))
    monkeypatch.setattr(_env, "_RELIST_COST_FACTOR", 0.0)
    sig = lt.raster_source_signature
    before = sig(a)
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    assert sig(a) != before
    Path(a + ".aux.xml").unlink()
    assert sig(a) == before


def test_the_stage_leaves_an_overview_file_behind(monkeypatch, tmp_path):
    """An ``.ovr`` holds reduced copies of the pixels that no read of the staged copy uses: it is neither copied nor part
    of the signature (the same list ``rst_fromfile`` takes)."""
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    cache, _ = _raster_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.tif", 10)
    Path(a + ".ovr").write_bytes(b"o" * 50)
    before = lt.raster_source_signature(a)
    with cache.use(a) as p:
        assert sorted(os.listdir(Path(p).parent)) == [os.path.basename(p)]
    Path(a + ".ovr").write_bytes(b"o" * 99)
    assert lt.raster_source_signature(a) == before


def test_the_cache_sizes_a_copy_from_its_own_books_and_lists_only_without_the_lock(
    monkeypatch, tmp_path
):
    """Sizing works from the sidecar names the entry holds; eviction and supersede remove the copy once the cache lock
    is released, with the files found by its prefix (so no folder is listed under the lock).
    """
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    cache, _ = _raster_cache(monkeypatch, tmp_path)
    real = lt._staged_sidecars

    def _no_listing_under_the_lock(local):
        assert not cache._lock.locked(), "the cache listed the folder under its lock"
        return real(local)

    a = _src(tmp_path, "a.bil", 10)
    Path(a).with_suffix(".hdr").write_bytes(b"NROWS 1")
    monkeypatch.setattr(lt, "_staged_sidecars", _no_listing_under_the_lock)
    with cache.use(a) as p1:
        assert cache._sizes[a] == 10 + len(b"NROWS 1")
        folder = Path(p1).parent
        assert len(os.listdir(folder)) == 2
    _rewrite(a, 4)  # supersede
    with cache.use(a):
        assert len(os.listdir(folder)) == 2
    cache._evict_to_cap(str(folder), keep="")  # nothing over the cap: kept
    monkeypatch.setattr(lt, "_stage_cap_bytes", lambda d, c: 0)
    with cache._lock:
        cache._evict_to_cap(
            str(folder), keep=""
        )  # over the cap: the idle copy goes with its sidecar, once the lock is released
    assert len(os.listdir(folder)) == 2
    cache._remove_doomed()
    assert os.listdir(folder) == []


def test_a_removed_copy_takes_the_files_gdal_wrote_beside_it(monkeypatch, tmp_path):
    """GDAL writes beside a staged copy under its name (a PAM ``.aux.xml`` with the statistics it computed, a SQLite
    ``-wal`` and ``-shm``): they are in no entry of the cache, and a supersede or an eviction removes them with the copy.
    """
    cache, _ = _raster_cache(monkeypatch, tmp_path)
    a = _src(tmp_path, "a.tif", 10)
    with cache.use(a) as p1:
        for ending in (".aux.xml", "-wal", "-shm"):
            Path(p1 + ending).write_bytes(b"gdal")
    folder = Path(p1).parent
    _rewrite(a, 4)  # supersede
    with cache.use(a) as p2:
        assert sorted(os.listdir(folder)) == [os.path.basename(p2)]


_LONG = (
    "s" * 236 + ".tif"
)  # 240 bytes: with the staged prefix and an .aux.xml it is over NAME_MAX


def test_a_source_name_too_long_to_stage_is_shortened_the_same_way_with_its_sidecars(
    monkeypatch, tmp_path
):
    """A source name whose staged copy (prefix plus name) would pass NAME_MAX (255 bytes) is staged under a shortened
    name that keeps the extension, the same for the same name; its sidecars follow it by the rule GDAL finds them by
    (``<name>.aux.xml``, ``<stem>.hdr``), so the staged copy still opens with them."""
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    os.makedirs(tmp_path / "root")
    a = _src(tmp_path, _LONG, 10)
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    Path(a[: -len(".tif")] + ".hdr").write_bytes(b"NROWS 1")
    short = lt.staged_name(_LONG)
    assert short == lt.staged_name(_LONG) != _LONG and short.endswith(".tif")
    assert lt.staged_name("a.tif") == "a.tif"
    assert (
        lt.staged_name("s" * 236 + "x.tif") != short
    )  # the digest tells names with one start apart
    tmp = lt.new_staged_local_file(_LONG)
    lt.copy_source_with_sidecars(a, tmp)
    names = os.listdir(tmp_path / "root")
    assert len(names) == 3 and max(len(n.encode()) for n in names) <= 255
    assert (Path(tmp + ".aux.xml")).read_bytes() == b"<PAMDataset/>"
    assert (Path(tmp[: -len(".tif")] + ".hdr")).read_bytes() == b"NROWS 1"
    cache, _ = _raster_cache(monkeypatch, tmp_path)
    with cache.use(a) as p:
        assert os.path.basename(p).endswith(short)
        assert sorted(os.listdir(Path(p).parent)) == sorted(
            [
                os.path.basename(p),
                os.path.basename(p) + ".aux.xml",
                os.path.basename(p)[:-4] + ".hdr",
            ]
        )


def test_the_signature_of_a_staged_raster_lists_its_folder_instead_of_a_stat_a_name(
    monkeypatch, tmp_path
):
    """Each use of a staged raster takes its signature. Without the reader's no-sidecar stamp, every name GDAL could
    read beside it (about fifty) used to be a stat, each a FUSE round trip on a Volume; the folder's listing, held for the
    process, now answers which exist: a stat of the file and of each sidecar there."""
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setattr(_env, "_LISTINGS", _env._FolderListings())
    a = _src(tmp_path, "a.tif", 10)
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    before = lt.raster_source_signature(a)
    real, seen = os.stat, []

    def _stat(path, *args, **kwargs):
        seen.append(path)
        return real(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", _stat)
    assert lt.raster_source_signature(a) == before
    assert len(seen) <= 6, seen  # was 51
    assert _env._LISTINGS.listings == 1


def test_remove_staged_takes_the_sidecars_found_by_the_prefix(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(tmp_path / "root"))
    os.makedirs(tmp_path / "root")
    a = _src(tmp_path, "a.bil", 10)
    Path(a).with_suffix(".hdr").write_bytes(b"NROWS 1")
    Path(a + ".ovr").write_bytes(b"o")
    tmp = lt.new_staged_local_file("a.bil")
    assert os.path.basename(tmp).endswith("a.bil")
    lt.copy_source_with_sidecars(a, tmp)
    other = tmp_path / "root" / "keep.txt"
    other.write_bytes(b"k")
    assert len(os.listdir(tmp_path / "root")) == 3  # file, .hdr, the unrelated file
    lt.remove_staged(tmp)
    assert os.listdir(tmp_path / "root") == ["keep.txt"]


# ---------------------------------------------------------------------------
# The callers of the Volume fallback copy remove it with every sidecar beside it
# ---------------------------------------------------------------------------


def _gtiff_source(tmp_path, name="g.tif"):
    import numpy as np
    from affine import Affine

    p = tmp_path / "src" / name
    p.parent.mkdir(exist_ok=True)
    with open_raster(
        p,
        "w",
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=Affine(1, 0, 0, 0, -1, 4),
    ) as ds:
        ds.write(np.arange(16, dtype="uint8").reshape(1, 4, 4))
    return str(p)


def _staging(monkeypatch, tmp_path, module, attr="_stage_local_if_needed"):
    """Make `module`'s Volume fallback copy stage every source: a copy under the staged prefix, with the PAM file GDAL
    can write beside it. Returns the staging folder."""
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setenv("GBX_LOCAL_TEMP_DIR", str(root))

    def stage(path):
        tmp = lt.new_staged_local_file(os.path.basename(path))
        lt.copy_source_with_sidecars(path, tmp)
        Path(tmp + ".aux.xml").write_bytes(b"<PAMDataset/>")
        return tmp, True

    monkeypatch.setattr(module, attr, stage)
    return root


def test_prepare_cogs_removes_the_staged_copy_and_its_sidecars(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import preparer

    root = _staging(monkeypatch, tmp_path, preparer)
    out = preparer.prepare_cogs(
        [_gtiff_source(tmp_path)], str(tmp_path / "out"), verbose=False
    )
    assert out is not None
    assert os.listdir(root) == []


def test_open_tile_removes_the_staged_copy_and_its_sidecars(monkeypatch, tmp_path):
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    root = _staging(monkeypatch, tmp_path, ot)
    with ot.open_tile(VirtualTile(cellid=0, path=_gtiff_source(tmp_path))) as ds:
        assert ds.read(1).shape == (4, 4)
        assert len(os.listdir(root)) == 2  # the copy and its sidecar, while open
    assert os.listdir(root) == []


def test_rst_fromfile_materialized_removes_the_staged_copy_and_its_sidecars(
    monkeypatch, tmp_path
):
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.pyrx import functions as prx

    root = _staging(monkeypatch, tmp_path, file_gbx)
    row = prx._fromfile_impl(_gtiff_source(tmp_path), None, True)
    assert row is not None and row["raster"]
    assert os.listdir(root) == []


def test_a_sidecar_is_staged_by_the_rule_gdal_finds_it_by():
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    assert lt.staged_sidecar_name("a.tif", "a.tif.aux.xml") == "a.tif.aux.xml"
    assert lt.staged_sidecar_name("a.tif", "a.hdr") == "a.hdr"
    assert lt.staged_sidecar_name("a.tif", "b.hdr") is None
    short = lt.staged_name(_LONG)
    assert lt.staged_sidecar_name(_LONG, _LONG + ".aux.xml") == short + ".aux.xml"
    assert lt.staged_sidecar_name(_LONG, _LONG[:-4] + ".tfw") == short[:-4] + ".tfw"


def test_the_listed_signature_counts_only_the_names_gdal_looks_up(
    monkeypatch, tmp_path
):
    """A listed folder answers for the exact names GDAL checks: another spelling (``A.TIF.aux.xml`` beside ``a.tif``) is
    not one, as no by-name stat on a case-sensitive folder would find it."""
    from databricks.labs.gbx.pyrx import _env

    monkeypatch.setattr(_env, "_LISTINGS", _env._FolderListings())
    a = _src(tmp_path, "a.tif", 10)
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    names = {os.path.basename(p) for p in _env.sidecar_files(a, listed=True)}
    assert names == {"a.tif.aux.xml"}
    other = _env._IndexedNames(["a.tif", "a.tif.aux.xml", "A.TIF.AUX.XML"])
    monkeypatch.setattr(_env._LISTINGS, "names", lambda *a, **k: other)
    assert {os.path.basename(p) for p in _env.sidecar_files(a, listed=True)} == {
        "a.tif.aux.xml"
    }


def test_a_long_name_without_an_extension_is_shortened_with_its_digest_alone():
    from databricks.labs.gbx.pyrx.core import local_temp as lt

    for long in (
        "s" * 300,
        "s" * 200 + "." + "e" * 40,
    ):  # no dot; an "extension" too long to be one
        short = lt.staged_name(long)
        assert short == lt.staged_name(long) and "." not in short.rpartition("~")[2]
        assert len(short.encode()) <= lt._STAGED_NAME_MAX and short.startswith("s" * 16)
    assert lt.staged_name("s" * 300) != lt.staged_name("s" * 299 + "t")


def test_a_listed_lookup_falls_back_to_by_name_checks(monkeypatch, tmp_path):
    """A folder held as too large, one that cannot be listed, and a listing that lacks the file (written since) are each
    checked name by name, so their sidecars are still found."""
    from databricks.labs.gbx.pyrx import _env

    a = _src(tmp_path, "a.tif", 10)
    Path(a + ".aux.xml").write_bytes(b"<PAMDataset/>")
    stale = _env._IndexedNames(["b.tif"])
    for held in (_env._TOO_LARGE, None, stale):
        monkeypatch.setattr(_env, "_LISTINGS", _env._FolderListings())
        monkeypatch.setattr(_env._LISTINGS, "names", lambda *a, _h=held, **k: _h)
        got = {os.path.basename(p) for p in _env.sidecar_files(a, listed=True)}
        assert got == {"a.tif.aux.xml"}, held
