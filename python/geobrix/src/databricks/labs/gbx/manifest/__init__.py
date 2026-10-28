"""Per-surface manifest (OD-6): the source of truth for the streamline subset.

Pure Python (no pyspark), so the UC DDL emitter, the docs generators and the tests
read it anywhere. JSON lives next to this module and ships as package data.
"""

from __future__ import annotations

import functools
import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

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


@dataclass(frozen=True)
class Function:
    sql_name: str
    family: str
    kind: str  # scalar | generator | aggregate
    params: Tuple[Param, ...]
    min_args: int
    returns_session: str
    returns_uc: Optional[str]
    accepts_virtual_tile: bool
    levels: Dict[str, Dict[str, Any]]
    oracle: str

    @property
    def max_args(self) -> int:
        return len(self.params)


@dataclass(frozen=True)
class Manifest:
    functions: Dict[str, Function]
    uc_dependencies: Dict[str, str]
    level_table_version: str


def sql_type(t: str) -> str:
    """Symbolic manifest type -> SQL type text."""
    return TILE_SQL if t == "TILE" else t


@functools.lru_cache(maxsize=1)
def load() -> Manifest:
    raw = json.loads((Path(__file__).with_name("functions.json")).read_text())
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
            r["levels"],
            r["oracle"],
        )
    return Manifest(fns, dict(raw["uc_dependencies"]), raw["level_table_version"])


def resolve_ref(ref: str) -> Any:
    """'pkg.module:attr' -> the object (imports the module)."""
    mod, _, attr = ref.partition(":")
    return getattr(importlib.import_module(mod), attr)
