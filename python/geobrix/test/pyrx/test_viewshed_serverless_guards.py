"""Regression guards for the Serverless-safety fixes in ``analysis.viewshed()``.

Each group pins one specific behaviour so a future refactor cannot silently drop it.
Tests are pure-Python, mock the heavy xrspatial computation, and require no cluster,
no Serverless, and no JAR.

  1. ``NUMBA_CACHE_DIR`` guard — set to a ``/tmp/gbx_numba_*`` directory when the
     environment variable is absent (numba cannot cache to the read-only NFS path
     on Serverless); left alone when already set or when JIT is disabled.

  2. ``importlib.import_module`` path — the implementation must obtain the
     xrspatial viewshed MODULE via ``importlib.import_module("xrspatial.viewshed")``
     rather than via the ``xrspatial.viewshed`` package attribute, which resolves to
     the *function* (not the module) due to xrspatial's ``__init__`` re-export.

  3. ``_available_memory_bytes`` monkeypatch — the function is patched to a stub
     that reports the honest per-task cgroup limit (``_cgroup_task_limit_bytes() or
     1 GiB``) during the call and restored in a ``finally`` block regardless of
     outcome; a module-level ``threading.RLock`` serialises concurrent patches
     within the same process.
"""

import importlib
import os
import threading
import types

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

# Skip the whole module if xrspatial is not installed (the tests mock the heavy
# computation, but the module must still be importable to be patchable).
pytest.importorskip("xrspatial")

from databricks.labs.gbx.pyrx import _serde  # noqa: E402 – after importorskip
from databricks.labs.gbx.pyrx.core import analysis  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_numba_env():
    """Save and restore NUMBA_CACHE_DIR and NUMBA_DISABLE_JIT around every test.

    ``analysis.viewshed()`` sets NUMBA_CACHE_DIR as a side effect; without this
    fixture the first test that exercises the guard would pollute the rest.
    """
    _keys = ("NUMBA_CACHE_DIR", "NUMBA_DISABLE_JIT")
    saved = {k: os.environ.get(k) for k in _keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _flat_dem_bytes(size: int = 7) -> bytes:
    """Return a flat (uniform 5.0 elevation) ``size x size`` DEM as GTiff bytes.

    Uses 1 m pixels, origin (0, size), EPSG:32633.  Pixel centres run from
    0.5 to 6.5 in each axis, so an observer at (3.5, 3.5) is safely within bounds.
    """
    dem = np.full((size, size), 5.0, dtype="float64")
    profile = dict(
        driver="GTiff",
        width=size,
        height=size,
        count=1,
        dtype="float64",
        crs="EPSG:32633",
        transform=from_origin(0.0, float(size), 1.0, 1.0),
        nodata=None,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dem, 1)
        return mf.read()


def _center_xy(ds, col: int, row: int):
    """Return world (x, y) at the pixel-centre of (row, col)."""
    from rasterio.transform import xy as _xy

    x, y = _xy(ds.transform, row, col, offset="center")
    return float(x), float(y)


class _FakeViewshedResult:
    """Minimal stand-in for the xrspatial DataArray result.

    Returns all-invisible pixels (values = -1) so the output is a valid all-zero
    uint8 tile — enough to exercise every code path after the viewshed call.
    """

    def __init__(self, shape):
        self.values = np.full(shape, -1.0, dtype="float64")


def _fake_viewshed(da, **kw):
    return _FakeViewshedResult(da.values.shape)


# ---------------------------------------------------------------------------
# Fix 1 — NUMBA_CACHE_DIR guard
# ---------------------------------------------------------------------------
# Exact code in analysis.viewshed() (lines 507-510):
#
#     import os as _os
#     import tempfile as _tempfile
#     if "NUMBA_DISABLE_JIT" not in _os.environ and "NUMBA_CACHE_DIR" not in _os.environ:
#         _os.environ["NUMBA_CACHE_DIR"] = _tempfile.mkdtemp(prefix="gbx_numba_")


def test_numba_cache_dir_set_to_tmp_gbx_prefix_when_absent(monkeypatch):
    """When neither NUMBA_CACHE_DIR nor NUMBA_DISABLE_JIT is set, viewshed
    creates NUMBA_CACHE_DIR pointing to a ``/tmp/gbx_numba_*`` directory."""
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _fake_viewshed)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert "NUMBA_CACHE_DIR" in os.environ, (
        "NUMBA_CACHE_DIR was not set; the numba caching guard is missing"
    )
    val = os.environ["NUMBA_CACHE_DIR"]
    assert val.startswith("/tmp/gbx_numba_"), (
        f"expected a /tmp/gbx_numba_* path, got {val!r}"
    )


def test_numba_cache_dir_not_overridden_when_already_present(monkeypatch):
    """When NUMBA_CACHE_DIR is already in the environment, viewshed respects it
    and does NOT replace it with a new /tmp path."""
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)
    monkeypatch.setitem(os.environ, "NUMBA_CACHE_DIR", "/pre/existing/cache")

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _fake_viewshed)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert os.environ["NUMBA_CACHE_DIR"] == "/pre/existing/cache", (
        "NUMBA_CACHE_DIR was overridden; guard must honour an existing value"
    )


def test_numba_cache_dir_not_set_when_numba_disable_jit_is_present(monkeypatch):
    """When NUMBA_DISABLE_JIT is in the environment (JIT compiled off),
    viewshed does NOT create NUMBA_CACHE_DIR — caching is irrelevant without JIT."""
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.setitem(os.environ, "NUMBA_DISABLE_JIT", "1")

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _fake_viewshed)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert "NUMBA_CACHE_DIR" not in os.environ, (
        "NUMBA_CACHE_DIR was set even though NUMBA_DISABLE_JIT is present; "
        "guard condition must check both variables"
    )


# ---------------------------------------------------------------------------
# Fix 2 — importlib.import_module for the xrspatial.viewshed MODULE
# ---------------------------------------------------------------------------
# Exact code in analysis.viewshed() (lines 558-559):
#
#     import importlib as _importlib
#     _xrs_vs_mod = _importlib.import_module("xrspatial.viewshed")
#
# Rationale (from the inline comment): xrspatial's __init__.py does
#   ``from xrspatial.viewshed import viewshed``, making ``xrspatial.viewshed`` the
#   function, not the module.  Accessing it as a package attribute would give the
#   function, which does not have ``_available_memory_bytes``.  importlib always
#   returns the MODULE object.


def test_xrspatial_viewshed_package_attr_is_function_not_module():
    """Root-cause invariant: ``xrspatial.viewshed`` as a package attribute is the
    viewshed *function*, not the submodule.  Direct attribute access would fail
    to expose ``_available_memory_bytes``, motivating the importlib path."""
    import xrspatial

    assert callable(xrspatial.viewshed), (
        "xrspatial.viewshed is expected to be callable (the function)"
    )
    assert not isinstance(xrspatial.viewshed, types.ModuleType), (
        "xrspatial.viewshed is a module — the importlib workaround may be obsolete; "
        "review whether the direct patch still works"
    )


def test_importlib_import_module_returns_module_with_available_memory_fn():
    """``importlib.import_module('xrspatial.viewshed')`` returns the MODULE object
    that has ``_available_memory_bytes`` — the attribute patched by the guard."""
    mod = importlib.import_module("xrspatial.viewshed")
    assert isinstance(mod, types.ModuleType), (
        "importlib.import_module('xrspatial.viewshed') did not return a module"
    )
    assert hasattr(mod, "_available_memory_bytes"), (
        "xrspatial.viewshed module is missing _available_memory_bytes; "
        "the monkeypatch in analysis.viewshed() would have no effect"
    )
    assert callable(mod._available_memory_bytes), (
        "_available_memory_bytes is not callable"
    )


# ---------------------------------------------------------------------------
# Fix 3 — _available_memory_bytes monkeypatch + finally restore + RLock
# ---------------------------------------------------------------------------
# Exact code in analysis._viewshed_xrspatial() (inner with block):
#
#     with _VIEWSHED_PSUTIL_LOCK:
#         _real_avail_mem_fn = _xrs_vs_mod._available_memory_bytes
#         _xrs_vs_mod._available_memory_bytes = (
#             lambda: _cgroup_task_limit_bytes() or (1024 ** 3)
#         )
#         try:
#             res = _viewshed(...)
#         finally:
#             _xrs_vs_mod._available_memory_bytes = _real_avail_mem_fn


def test_available_memory_bytes_restored_after_successful_call(monkeypatch):
    """On a normal (success) call the finally block restores the original
    ``_available_memory_bytes`` function on the xrspatial.viewshed module."""
    xrs_vs_mod = importlib.import_module("xrspatial.viewshed")
    original_fn = xrs_vs_mod._available_memory_bytes

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _fake_viewshed)
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert xrs_vs_mod._available_memory_bytes is original_fn, (
        "_available_memory_bytes was not restored after a successful viewshed call; "
        "the finally block is missing or broken"
    )


def test_available_memory_bytes_restored_after_viewshed_exception(monkeypatch):
    """When the underlying viewshed call raises, the finally block still restores
    the original ``_available_memory_bytes`` — no attribute is permanently leaked."""
    xrs_vs_mod = importlib.import_module("xrspatial.viewshed")
    original_fn = xrs_vs_mod._available_memory_bytes

    def _raise_viewshed(da, **kw):
        raise RuntimeError("injected failure for restore test")

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _raise_viewshed)
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        with pytest.raises(RuntimeError, match="injected failure for restore test"):
            analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert xrs_vs_mod._available_memory_bytes is original_fn, (
        "_available_memory_bytes was not restored after a viewshed exception; "
        "the finally block is missing or broken"
    )


def test_stub_returns_cgroup_limit_when_available(monkeypatch):
    """During the xrspatial call, ``_available_memory_bytes`` reports the cgroup
    task limit when ``_cgroup_task_limit_bytes()`` returns a value."""
    xrs_vs_mod = importlib.import_module("xrspatial.viewshed")
    observed: list = []

    _known_limit = 4 * 1024**3  # inject a known cgroup limit
    monkeypatch.setattr(analysis, "_cgroup_task_limit_bytes", lambda: _known_limit)

    def _capture_viewshed(da, **kw):
        observed.append(xrs_vs_mod._available_memory_bytes())
        return _FakeViewshedResult(da.values.shape)

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _capture_viewshed)
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert len(observed) == 1, f"expected one viewshed call, got {len(observed)}"
    assert observed[0] == _known_limit, (
        f"stub should report cgroup limit ({_known_limit} bytes), got {observed[0]}"
    )


def test_stub_falls_back_to_1gib_when_no_cgroup_limit(monkeypatch):
    """When ``_cgroup_task_limit_bytes()`` returns None, the stub falls back to
    1 GiB rather than the old hardcoded 8 GiB figure."""
    xrs_vs_mod = importlib.import_module("xrspatial.viewshed")
    observed: list = []

    monkeypatch.setattr(analysis, "_cgroup_task_limit_bytes", lambda: None)

    def _capture_viewshed(da, **kw):
        observed.append(xrs_vs_mod._available_memory_bytes())
        return _FakeViewshedResult(da.values.shape)

    import xrspatial

    monkeypatch.setattr(xrspatial, "viewshed", _capture_viewshed)
    monkeypatch.delitem(os.environ, "NUMBA_CACHE_DIR", raising=False)
    monkeypatch.delitem(os.environ, "NUMBA_DISABLE_JIT", raising=False)

    with _serde.open_tile(_flat_dem_bytes()) as ds:
        ox, oy = _center_xy(ds, col=3, row=3)
        analysis.viewshed(ds, ox, oy, 1.0, 0.0, None)

    assert len(observed) == 1, f"expected one viewshed call, got {len(observed)}"
    expected_fallback = 1024**3  # 1 GiB
    assert observed[0] == expected_fallback, (
        f"stub should fall back to 1 GiB ({expected_fallback} bytes), got {observed[0]}"
    )


def test_viewshed_psutil_lock_is_reentrant_rlock():
    """The module-level ``_VIEWSHED_PSUTIL_LOCK`` is a reentrant ``threading.RLock``
    (not a plain Lock), which allows the same thread to re-acquire it without
    deadlocking — required for Spark Connect's threading model."""
    lock = analysis._VIEWSHED_PSUTIL_LOCK
    # An RLock allows the same thread to acquire it more than once.
    with lock:
        acquired = lock.acquire(blocking=False)
        assert acquired, (
            "_VIEWSHED_PSUTIL_LOCK is not reentrant; it should be threading.RLock, "
            "not threading.Lock"
        )
        lock.release()
