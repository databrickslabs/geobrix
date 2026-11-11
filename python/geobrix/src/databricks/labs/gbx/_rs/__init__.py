"""Loader for the bundled Rust kernels (_gbxrs, abi3): one build per platform dir, pure-Python fallback.

pyspark-free on purpose: the UC sandbox imports this too (L4 batch handlers).
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import platform
import sys
from pathlib import Path
from typing import Optional

_MOD = None
_WHY: Optional[str] = None


def _source_checkout() -> Optional[Path]:
    """The repository root when this package runs from a source checkout (rust/gbxrs sits above it)."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "rust" / "gbxrs" / "Cargo.toml").is_file():
            return parent
    return None


def _rs_dir() -> Optional[Path]:
    """Payload layout: <payload>/_native/<arch>/rs/. On a macOS source checkout: build/rs/dev-darwin/rs/.

    The macOS iteration build (maturin build, never shipped) stays out of the package tree: a stray
    `_native/` directory without the rs module would be an empty namespace package.
    """
    if sys.platform == "darwin":
        root = _source_checkout()
        return root / "build" / "rs" / "dev-darwin" / "rs" if root else None
    # A wheel without the payload module raises "cannot import name" (ImportError), not
    # ModuleNotFoundError.
    try:
        from databricks.labs.gbx import _native
    except ImportError:
        return None
    d = _native.native_dir()
    return Path(d) / "rs" if d else None


def load():
    global _MOD, _WHY
    if _MOD is not None or _WHY is not None:
        return _MOD
    d = _rs_dir()
    found = sorted(d.glob("_gbxrs*.so")) if d else []
    if not found:
        _WHY = f"no Rust build for {platform.machine()} in this wheel"
        return None
    try:
        loader = importlib.machinery.ExtensionFileLoader("_gbxrs", str(found[0]))
        spec = importlib.util.spec_from_loader("_gbxrs", loader)
        mod = importlib.util.module_from_spec(spec)
        loader.exec_module(mod)
        _MOD = mod
    except (
        ImportError,
        OSError,
    ) as exc:  # glibc too old, wrong arch, corrupt file; MemoryError propagates
        _WHY = f"Rust kernels failed to load ({type(exc).__name__}: {exc})"
    return _MOD


def unavailable_reason() -> Optional[str]:
    load()
    return _WHY


def bng_cells(ev, nv, r: int):
    """float64 numpy east/north (NaN = NULL) -> pyarrow StringArray of BNG cell ids."""
    import numpy as np
    import pyarrow as pa

    mod = load()
    if mod is None:
        raise RuntimeError(f"gbx Rust kernels unavailable: {_WHY}")
    data, offs, valid, nulls = mod.bng_eastnorth_cells(
        np.ascontiguousarray(ev, dtype="<f8").tobytes(),
        np.ascontiguousarray(nv, dtype="<f8").tobytes(),
        int(r),
    )
    return pa.Array.from_buffers(
        pa.string(),
        len(ev),
        [pa.py_buffer(valid), pa.py_buffer(offs), pa.py_buffer(data)],
        null_count=nulls,
    )


class LocateFailure(RuntimeError):
    """The TIN kernel's point location did not converge (JTS's ``LocateFailureException``)."""


def missing(*names: str) -> Optional[str]:
    """None when this process's Rust build has every kernel named, else why not (no build, or an older one)."""
    mod = load()
    if mod is None:
        return _WHY
    absent = [n for n in names if not hasattr(mod, n)]
    return f"this Rust build has no {', '.join(absent)}" if absent else None


def tin_delaunay(x, y, env, tol: float):
    """JTS 1.20's incremental Delaunay triangulation of the sites x/y (float64, inserted in this order), framed on
    ``env`` (minx, miny, maxx, maxy) with tolerance ``tol`` -> (M, 3) int64 site indices, each row ascending, the rows
    ascending. Raises :class:`LocateFailure` where JTS's point location fails."""
    import numpy as np

    mod = load()
    if mod is None:
        raise RuntimeError(f"gbx Rust kernels unavailable: {_WHY}")
    try:
        out = mod.tin_delaunay(
            np.ascontiguousarray(x, dtype="<f8").tobytes(),
            np.ascontiguousarray(y, dtype="<f8").tobytes(),
            tuple(float(v) for v in env),
            float(tol),
        )
    except mod.LocateFailure as exc:
        raise LocateFailure(str(exc)) from None
    return np.frombuffer(out, dtype="<i8").reshape(-1, 3).astype(np.int64)


def tin_kdtree(tol: float):
    """JTS's KdTree (``_gbxrs.TinKdTree``): a point within ``tol`` of a node snaps onto it; node ids are insertion
    ranks."""
    mod = load()
    if mod is None:
        raise RuntimeError(f"gbx Rust kernels unavailable: {_WHY}")
    return mod.TinKdTree(float(tol))
