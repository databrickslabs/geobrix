"""The static, versioned level table: which implementation level serves each SQL name.

PREFERENCE lists the levels to try, best first; `select` takes the first one that is
available here and records one reason per level it had to skip.
"""

from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from databricks.labs.gbx import manifest
from databricks.labs.gbx.manager.capabilities import Capabilities

LEVEL_TABLE_VERSION = manifest.load().level_table_version  # one source: the manifest
# A function with no measured row below tries every level it has, highest first.
PREFERENCE: Dict[str, Tuple[str, ...]] = {
    "gbx_bng_eastnorthasbng": ("L1", "L2", "L3"),
    "gbx_rst_transformcrs": ("L1", "L2", "L3"),
    "gbx_st_makevalid": ("L3",),
}
L3_ORDER: Tuple[str, ...] = ("d", "c1", "c0")
NAMES = {"L1": "heavy (Catalyst)", "L2": "heavy (Java UDF)", "L3": "light"}


class NoLevelAvailable(RuntimeError):
    """No level can serve the function here; `reasons` has one entry per level tried."""

    def __init__(self, name: str, reasons: Tuple[str, ...]):
        super().__init__(f"{name}: no level available ({'; '.join(reasons)})")
        self.name, self.reasons = name, reasons


@dataclass(frozen=True)
class Choice:
    level: str
    variant: str
    reasons: Tuple[str, ...]


def available(fn, level: str, caps: Capabilities) -> Optional[str]:
    """None when `level` can serve `fn` with its full session signature here; else the reason."""
    spec = fn.levels.get(level)
    if spec is None:
        return f"{fn.sql_name} has no {level} implementation"
    if level in ("L1", "L2"):
        if level == "L1" and caps.connect:
            return (
                caps.jar_reason
                if caps.serverless
                else "Spark Connect session (standard access mode)"
            )
        if caps.jar is None:
            return caps.jar_reason
        if spec.get("needs_gdal") and caps.gdal_reason:
            return caps.gdal_reason
        l1_returns = spec.get("session_returns", fn.returns_session)
        if level == "L1" and l1_returns != fn.returns_session:
            return f"{fn.sql_name}: heavy returns {l1_returns}, the session signature returns {fn.returns_session}"
        if level == "L2" and fn.kind != "scalar":
            return f"standard access mode serves scalars only ({fn.sql_name} is a {fn.kind})"
        if level == "L2" and fn.min_args != fn.max_args and "wrapper" not in spec:
            return f"{fn.sql_name}: optional arguments need a SQL wrapper, and this L2 row declares none"
        wrapper = fn.accepts_virtual_tile or (level == "L2" and "bytes_param" in spec)
        if wrapper and not caps.sql_functions:
            return f"{fn.sql_name} needs a SQL function wrapper, and SQL functions are unavailable here"
        return None
    if level == "L3":
        missing = [
            m for m in spec.get("needs", ()) if importlib.util.find_spec(m) is None
        ]
        return f"{fn.sql_name} needs {', '.join(missing)}" if missing else None
    return f"{level} is not a session level"


def select(
    fn, caps: Capabilities, override: Optional[Tuple[str, str]] = None
) -> Choice:
    """First available level in PREFERENCE (or the testing-only override)."""
    if override is not None:
        return Choice(override[0], override[1], ())
    order = PREFERENCE.get(fn.sql_name) or tuple(
        lv for lv in ("L1", "L2", "L3") if lv in fn.levels
    )
    reasons = []
    for level in order:
        if level not in fn.levels:
            continue
        why = available(fn, level, caps)
        if why is None:
            return Choice(level, "-", tuple(reasons))
        reasons.append(f"geobrix: {NAMES[level]} unavailable: {why}")
    raise NoLevelAvailable(fn.sql_name, tuple(reasons))
