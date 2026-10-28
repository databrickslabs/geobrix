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


def _rs_dir() -> Optional[Path]:
    """B1 layout: <payload>/_native/<arch>/rs/. On macOS dev hosts: the gitignored _native/dev-darwin/rs/."""
    if sys.platform == "darwin":
        return Path(__file__).resolve().parents[1] / "_native" / "dev-darwin" / "rs"
    try:
        from databricks.labs.gbx import _native

        d = _native.native_dir()
    except (ModuleNotFoundError, AttributeError):  # a wheel without the payload
        return None
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
    except Exception as exc:  # noqa: BLE001 - glibc too old, wrong arch, ...
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
