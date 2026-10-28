"""Per-surface function manifest: the source of truth for each function's names,
signatures and implementation levels on every surface.

Pure Python (no pyspark), so the UC DDL emitter, the docs generators and the tests
read it anywhere. JSON lives next to this module and ships as package data. The loaded
manifest is immutable (read-only mappings, tuples), so the cached copy cannot be edited
by a caller.
"""

from __future__ import annotations

import functools
import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Optional, Tuple

SCHEMA_VERSION = 1

# The closed vocabulary of keys a function's per-level spec may carry. A mistyped key
# would be read with `.get` and silently drop a guard, so a contract test rejects any key
# outside this table; add a key here in the same change that first uses it.
LEVEL_KEYS = {
    "L1": frozenset({"companion", "needs_gdal", "session_returns"}),
    "L2": frozenset({"class", "returns", "bytes_param", "needs_gdal"}),
    "L3": frozenset(
        {
            "register",
            "variants",
            "factory",
            "factory_args",
            "callable",
            "udtf",
            "returns",
            "needs",
        }
    ),
    "L4": frozenset({"handler", "deps", "tvf_over"}),
}
# Keys every spec of that level must carry.
LEVEL_REQUIRED = {
    "L1": frozenset({"companion"}),
    "L2": frozenset({"class", "returns"}),
    "L3": frozenset({"register"}),
}

# The v2 tile struct as a SQL type (== V2_TILE_SCHEMA.simpleString(); a contract test pins it).
TILE_SQL = (
    "struct<cellid:bigint,raster:binary,path:string,path_mode:string,"
    "window:struct<col_off:int,row_off:int,width:int,height:int>,"
    "clip_polygon:binary,clip_crs:string,crs:string,metadata:map<string,string>>"
)


@dataclass(frozen=True)
class Param:
    name: str
    session: Tuple[str, ...]
    uc: str
    default: Optional[str] = None
    uc_crs: bool = False


# eq=False keeps identity hashing: the level specs are mappings, which do not hash.
@dataclass(frozen=True, eq=False)
class Function:
    sql_name: str
    family: str
    kind: str  # scalar | generator | aggregate
    params: Tuple[Param, ...]
    min_args: int
    returns_session: str
    returns_uc: Optional[str]
    accepts_virtual_tile: bool
    levels: Mapping[str, Mapping[str, Any]]
    oracle: str

    @property
    def max_args(self) -> int:
        return len(self.params)


@dataclass(frozen=True, eq=False)
class Manifest:
    functions: Mapping[str, Function]
    uc_dependencies: Mapping[str, str]
    level_table_version: str


def sql_type(t: str) -> str:
    """Symbolic manifest type -> SQL type text."""
    return TILE_SQL if t == "TILE" else t


def _freeze(obj: Any) -> Any:
    """Deep read-only copy: dict -> mappingproxy, list -> tuple."""
    if isinstance(obj, dict):
        return MappingProxyType({k: _freeze(v) for k, v in obj.items()})
    if isinstance(obj, list):
        return tuple(_freeze(v) for v in obj)
    return obj


@functools.lru_cache(maxsize=1)
def load() -> Manifest:
    raw = json.loads((Path(__file__).with_name("functions.json")).read_text())
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"functions.json schema_version {raw.get('schema_version')!r}, expected {SCHEMA_VERSION}"
        )
    fns = {}
    for r in raw["functions"]:
        params = tuple(
            Param(
                p["name"],
                tuple(p["session"]),
                p.get("uc", ""),
                p.get("default"),
                bool(p.get("uc_crs")),
            )
            for p in r["params"]
        )
        fns[r["sql_name"]] = Function(
            r["sql_name"],
            r["family"],
            r["kind"],
            params,
            int(r["min_args"]),
            r["returns"]["session"],
            r["returns"].get("uc"),
            bool(r.get("accepts_virtual_tile")),
            _freeze(r["levels"]),
            r["oracle"],
        )
    return Manifest(
        MappingProxyType(fns),
        MappingProxyType(dict(raw["uc_dependencies"])),
        raw["level_table_version"],
    )


def resolve_ref(ref: str) -> Any:
    """'pkg.module:attr' -> the object (imports the module)."""
    mod, _, attr = ref.partition(":")
    return getattr(importlib.import_module(mod), attr)
