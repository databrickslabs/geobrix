import contextlib
import os
import subprocess
import sys

import pytest

from databricks.labs.gbx.pyrx import _env


def test_configure_gdal_env_sets_gdal_data():
    # Clear to prove configure_gdal_env populates from the rasterio wheel.
    os.environ.pop("GDAL_DATA", None)
    _env.configure_gdal_env()
    assert os.environ.get(
        "GDAL_DATA"
    ), "GDAL_DATA should be set from rasterio's bundled data"
    assert os.path.isdir(os.environ["GDAL_DATA"])


def test_configure_gdal_env_is_idempotent_and_respects_existing():
    os.environ["GDAL_DATA"] = "/tmp/preset-gdal-data"
    _env.configure_gdal_env()
    assert os.environ["GDAL_DATA"] == "/tmp/preset-gdal-data"


def test_assert_rasterio_available_returns_versions():
    gdal_ver, rio_ver = _env.assert_rasterio_available()
    assert gdal_ver and rio_ver


@pytest.mark.parametrize("make_dir", [False, True], ids=["missing", "without-proj-db"])
def test_a_proj_data_this_process_cannot_read_still_resolves_epsg_4326(
    tmp_path, make_dir
):
    """DBR 19 exports PROJ_DATA=/databricks/native/proj-data, which the notebook user of a
    standard-access cluster cannot read. rasterio took it as its PROJ search path at
    import, so every light CRS lookup on the driver failed with "Cannot find proj.db".
    A fresh interpreter starts with that environment, as the driver does."""
    unreadable = tmp_path / "proj-data"
    if make_dir:
        unreadable.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("PROJ_DATA", "PROJ_LIB")}
    env["PROJ_DATA"] = str(unreadable)
    code = "\n".join(
        [
            "import os, rasterio",
            "from rasterio.crs import CRS",
            "from databricks.labs.gbx.core.crs import crs_to_canonical, resolve_crs",
            "print(crs_to_canonical(resolve_crs('EPSG:4326')))",
            "print(CRS.from_epsg(27700).to_epsg())",
            "with rasterio.Env():",
            "    print(CRS.from_user_input('EPSG:3857').to_epsg())",
            f"print(os.environ['PROJ_DATA'] != {str(unreadable)!r})",
        ]
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert out.returncode == 0, out.stderr[-3000:]
    assert out.stdout.split() == ["EPSG:4326", "27700", "3857", "True"]


def test_a_registered_grid_reaches_rasterio_after_an_unreadable_proj_data(tmp_path):
    """configure_gdal_env replaces an unreadable PROJ_DATA with the bundled data and then
    merges the registered grid dirs: rasterio's PROJ search path must follow to the merged
    dir, or a transform built without a new rasterio.Env misses the grid. The synthetic
    NTv2 grid shifts latitude by +30 arc-seconds; without it PROJ finds no operation."""
    grids = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "sample-data",
        "Volumes",
        "main",
        "geobrix_samples",
        "geobrix-examples",
        "proj-grids",
    )
    grids = os.path.abspath(grids)
    assert os.path.isfile(os.path.join(grids, "synthetic.gsb"))
    env = {k: v for k, v in os.environ.items() if k not in ("PROJ_DATA", "PROJ_LIB")}
    env["PROJ_DATA"] = str(tmp_path / "unreadable-proj-data")
    code = "\n".join(
        [
            "from databricks.labs.gbx.pyrx import _env",
            "from rasterio.crs import CRS",
            "from rasterio._warp import _transform",
            f"_env.configure_gdal_env(extra_proj_dirs=[{grids!r}])",
            "src = CRS.from_user_input('+proj=longlat +ellps=GRS80 "
            "+nadgrids=synthetic.gsb +no_defs')",
            "xs, ys = _transform(src, CRS.from_user_input('EPSG:4326'), [0.0], [51.5], None)",
            "print(repr(ys[0]))",
        ]
    )
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-2000:]
    assert float(out.stdout.strip().splitlines()[-1]) == pytest.approx(
        51.5 + 30.0 / 3600.0, abs=1e-9
    )


def _cache_max():
    from rasterio.env import get_gdal_config

    return get_gdal_config("GDAL_CACHEMAX")


@pytest.mark.parametrize(
    "outer_env", [False, True], ids=["no outer Env", "inside an Env"]
)
def test_a_block_cache_cap_is_put_back_where_a_rasterio_env_would_leave_it(outer_env):
    """``rasterio.Env(GDAL_CACHEMAX=n)`` sets the cache to n BYTES and, entered inside another
    Env, never sets it back: GDAL work after it ran with a cache too small to hold a block, and
    ``GDALViewshedGenerate`` gave a different answer from call to call. The helper sets the cap
    in bytes and puts the size it found back, in either case."""
    import rasterio

    before = _cache_max()
    cap = 64 * 1024 * 1024
    assert cap != before
    with rasterio.Env() if outer_env else contextlib.nullcontext():
        with _env.capped_block_cache(cap):
            assert _cache_max() == cap
        assert _cache_max() == before
    assert _cache_max() == before


def test_overlapping_block_cache_caps_hold_the_largest_and_end_at_the_original():
    before = _cache_max()
    small, large = 32 * 1024 * 1024, 96 * 1024 * 1024
    with _env.capped_block_cache(small):
        with _env.capped_block_cache(large):
            assert _cache_max() == large
        assert _cache_max() == small
    assert _cache_max() == before


def test_no_module_caps_the_block_cache_through_a_rasterio_env():
    """GDAL_CACHEMAX is named in one module, the helper that restores it. Anywhere else it is
    ``rasterio.Env(GDAL_CACHEMAX=...)`` again: bytes read as megabytes, and never put back.
    """
    import pathlib

    src = pathlib.Path(_env.__file__).parents[1]  # .../databricks/labs/gbx
    offenders = [
        path.relative_to(src).as_posix()
        for path in sorted(src.rglob("*.py"))
        if path.name != "_env.py"
        and "GDAL_CACHEMAX" in path.read_text(encoding="utf-8")
    ]
    assert offenders == [], (
        f"{offenders} set GDAL_CACHEMAX directly. Fix: wrap the block in "
        "databricks.labs.gbx.pyrx._env.capped_block_cache(<bytes>)"
    )


@pytest.fixture
def block_cache():
    """GDAL's block cache size, put back after the test whatever it did."""
    from rasterio.env import set_gdal_config

    before = _cache_max()
    yield before
    set_gdal_config("GDAL_CACHEMAX", before)


def test_a_cap_never_raises_a_cache_that_is_already_smaller(block_cache):
    """A cluster or a user may have lowered the cache to bound memory: the block runs at the
    smaller of the two, and a smaller cap inside it does not take it lower than the first.
    """
    from rasterio.env import set_gdal_config

    lowered = 16 * 1024 * 1024
    set_gdal_config("GDAL_CACHEMAX", lowered)
    with _env.capped_block_cache(128 * 1024 * 1024):
        assert _cache_max() == lowered
        with _env.capped_block_cache(4 * 1024 * 1024):
            assert _cache_max() == lowered
    assert _cache_max() == lowered


def test_a_cache_change_made_while_a_cap_is_active_survives_the_last_exit(block_cache):
    """Something else (user code, another library) resizes the cache inside the block: the last
    cap's exit leaves that value, instead of writing back the size found at the start.
    """
    from rasterio.env import set_gdal_config

    set_gdal_config("GDAL_CACHEMAX", 512 * 1024 * 1024)
    changed = 200 * 1024 * 1024
    with _env.capped_block_cache(32 * 1024 * 1024):
        assert _cache_max() == 32 * 1024 * 1024
        set_gdal_config("GDAL_CACHEMAX", changed)
    assert _cache_max() == changed
    assert _env._CACHE_CAPS == []
    # and the next block starts clean: it records the size found then, and puts it back
    with _env.capped_block_cache(32 * 1024 * 1024):
        assert _cache_max() == 32 * 1024 * 1024
    assert _cache_max() == changed


def test_the_cache_size_comes_back_when_the_block_raises(block_cache):
    with pytest.raises(RuntimeError, match="boom"):
        with _env.capped_block_cache(32 * 1024 * 1024):
            assert _cache_max() == 32 * 1024 * 1024
            raise RuntimeError("boom")
    assert _cache_max() == block_cache
    assert _env._CACHE_CAPS == []


def test_threads_that_overlap_are_held_to_the_largest_cap_until_the_last_ends(
    block_cache,
):
    """A small cap begins on another thread, a large one here, the small one ends first: the
    large one still holds, and the size found comes back only when both are done."""
    import threading

    small, large = 32 * 1024 * 1024, 96 * 1024 * 1024
    inside, release, done = (threading.Event() for _ in range(3))

    def other():
        with _env.capped_block_cache(small):
            inside.set()
            release.wait(10)
        done.set()

    t = threading.Thread(target=other)
    t.start()
    assert inside.wait(10)
    with _env.capped_block_cache(large):
        assert _cache_max() == large
        release.set()
        assert done.wait(10)
        assert _cache_max() == large  # the small one has ended; this one still holds
    t.join(10)
    assert _cache_max() == block_cache


@pytest.fixture
def cap_seen(monkeypatch):
    """``(bytes asked for, cache size inside the block)`` for each ``capped_block_cache`` the
    sites enter, with the real helper doing the work."""
    from contextlib import contextmanager

    from databricks.labs.gbx.pyrx.core import analysis

    seen, real = [], _env.capped_block_cache

    @contextmanager
    def spy(max_bytes):
        with real(max_bytes):
            seen.append((max_bytes, _cache_max()))
            yield

    monkeypatch.setattr(_env, "capped_block_cache", spy)
    monkeypatch.setattr(analysis, "capped_block_cache", spy)
    return seen


def _small_tif(path, size=64):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=size,
        height=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0.0, float(size), 1.0, 1.0),
    ) as dst:
        dst.write(np.arange(size * size, dtype="float32").reshape(1, size, size))
    return str(path)


@pytest.mark.parametrize(
    "site", ["reader window", "reader clip", "stac download", "cog file"]
)
def test_every_site_that_caps_the_block_cache_applies_the_cap(
    tmp_path, cap_seen, block_cache, monkeypatch, site
):
    """Each of the four places that bound GDAL's block cache asks for the size the budget names
    and has it in force while it works; deleting the wrapper from one fails its row. On a classic
    runtime the streaming COG copy's size (its ceiling) differs from the others', so a site that
    drops its ceiling fails too."""
    from rasterio.env import set_gdal_config

    from databricks.labs.gbx.pyrx.core import analysis, budget

    # a cache above every cap, so the cap (not the host's default size) is what is in force
    set_gdal_config("GDAL_CACHEMAX", 1 << 30)
    monkeypatch.setattr(budget, "runtime_kind", lambda: "classic")
    src = _small_tif(tmp_path / "src.tif")
    streaming = budget.block_cache_bytes(
        "auto", ceiling=budget.STREAMING_BLOCK_CACHE_MAX
    )
    assert streaming == 4 * 1024 * 1024 != budget.block_cache_bytes("auto")
    if site == "cog file":
        analysis.cog_convert_file(src, str(tmp_path / "out.tif"), blocksize=256)
        want = streaming
    elif site == "stac download":
        from databricks.labs.gbx.stac._download import windowed_download

        windowed_download(src, str(tmp_path / "win.tif"), (2, 2, 5, 6))
        want = budget.block_cache_bytes("auto")
    else:
        from databricks.labs.gbx.ds.raster import RasterGbxReader

        # tiles with bytes; a whole file would pass through without a decode, so cut windows
        options = {"path": src, "virtualTiles": "false", "tileSize": "32"}
        if site == "reader clip":
            options = {
                "path": src,
                "virtualTiles": "false",
                "clipPolygons": "POLYGON((10 10, 40 10, 40 40, 10 40, 10 10))",
            }
        reader = RasterGbxReader(options)
        assert [row for p in reader.partitions() for row in reader.read(p)]
        want = budget.block_cache_bytes(reader.strategy)
    assert cap_seen and set(cap_seen) == {
        (want, want)
    }  # once per window for the reader
    assert _cache_max() == 1 << 30
