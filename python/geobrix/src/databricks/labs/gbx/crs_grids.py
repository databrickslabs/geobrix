"""Public helper: register custom PROJ grid-shift directories (both tiers)."""

import os
import warnings
from typing import List, Sequence, Union

from databricks.labs.gbx.core import proj_grids

_GRID_SUFFIXES = (".gsb", ".tif", ".gtx", ".gsa")  # NTv2 / PROJ geoid / NADCON-ish


def _warn_if_unusable(d: str) -> None:
    if not os.path.isdir(d):
        warnings.warn(
            f"register_proj_grids: '{d}' is not an existing directory; "
            f"grid-referencing transforms will silently lose accuracy until it exists.",
            UserWarning,
            stacklevel=3,
        )
        return
    try:
        has_grid = any(f.lower().endswith(_GRID_SUFFIXES) for f in os.listdir(d))
    except OSError:
        has_grid = True  # unreadable now (e.g. Volume eventual-consistency); don't cry wolf
    if not has_grid:
        warnings.warn(
            f"register_proj_grids: '{d}' contains no recognizable grid file "
            f"({', '.join(_GRID_SUFFIXES)}).",
            UserWarning,
            stacklevel=3,
        )


def _reregister_active_light_tiers(spark) -> None:
    """Re-run register() for whichever light tiers are already registered so live
    SQL bindings get fresh closures embedding the current grid dirs."""
    if spark is None:
        return
    for modname in (
        "databricks.labs.gbx.pyrx.functions",
        "databricks.labs.gbx.pyvx.functions",
        "databricks.labs.gbx.pygx.functions",
    ):
        try:
            import importlib
            mod = importlib.import_module(modname)
        except Exception:
            continue
        if getattr(mod, "_gbx_registered", False):  # set by register() — Task 4
            mod.register(spark)


def _apply_heavy(spark, result) -> None:
    """Set the heavy-tier JVM grid registry if a JVM is present (best-effort)."""
    jvm = getattr(spark, "_jvm", None)
    if jvm is None:
        return
    try:
        reg = jvm.com.databricks.labs.gbx.operations.ProjGridRegistry
        reg.set(list(result), True)  # push the full de-duped set (idempotent replace)
    except Exception:
        pass  # heavy classes absent (light-only session) → no-op


def register_proj_grids(
    spark,
    dirs: Union[str, Sequence[str]],
    *,
    replace: bool = False,
) -> List[str]:
    """Register one or more Volume dirs of PROJ grid-shift files so CRS transforms
    on both tiers find them. See the spec for the session-start contract."""
    result = proj_grids.set_registered_dirs(dirs, replace=replace)
    for d in (dirs if not isinstance(dirs, str) else [dirs]):
        _warn_if_unusable(d)
    _reregister_active_light_tiers(spark)
    _apply_heavy(spark, result)
    return result
