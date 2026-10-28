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
    try:
        from databricks.labs.gbx import _native
    except ImportError:  # no payload module in this wheel: "cannot import name", not ModuleNotFoundError
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
