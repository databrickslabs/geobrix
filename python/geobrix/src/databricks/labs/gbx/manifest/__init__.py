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
# `engine` marks a level that runs the row's `gdal` block on the GDAL spec engine where that engine can serve,
# and its own implementation (the other keys) elsewhere.
LEVEL_KEYS = {
    "L1": frozenset({"companion", "needs_gdal", "session_returns", "engine"}),
    "L2": frozenset({"class", "returns", "bytes_param", "needs_gdal", "wrapper"}),
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
            "engine",
        }
    ),
    "L4": frozenset({"handler", "deps", "tvf_over", "engine"}),
}
# Keys every spec of that level must carry.
LEVEL_REQUIRED = {
    "L2": frozenset({"class", "returns"}),
    "L3": frozenset({"register"}),
}
# Levels whose spec must carry at least one of these keys.
LEVEL_REQUIRED_ANY = {
    "L1": frozenset({"companion", "engine"}),
    "L4": frozenset({"handler", "tvf_over"}),
}

# The closed vocabulary of a row's `gdal` block (the GDAL spec engine's spec): what `parse_gdal_spec` accepts.
GDAL_KEYS = frozenset({"alg", "in", "out", "write", "args", "needs_crs", "gdal_min"})
GDAL_ARG_KEYS = frozenset(
    {"arg", "const", "from", "type", "map", "null", "with", "bad"}
)
GDAL_IN_KINDS = frozenset({"raster", "rasters", "text"})
GDAL_OUT_KINDS = frozenset({"raster", "features", "json"})
GDAL_BINDERS = frozenset(
    {
        "double",
        "int",
        "bool",
        "choice",
        "crs",
        "geom_wkt",
        "double_list",
        "calc_envelope",
        "text_vsimem",
        "observer_xy",
        "size_from_factor",
    }
)
GDAL_NULL = frozenset({"omit", "null_result", "raise"})
GDAL_BAD = frozenset({"raise", "data"})
# Write profiles: `tile` is a GTiff with the engine's creation options. No profile writes VRT or GDALG.
GDAL_WRITE = frozenset({"tile"})

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
class GdalArg:
    """One argument of a spec's template: a fixed value (`const`; True is a bare flag) or a param bound
    through a binder (`source`, `type`)."""

    arg: str
    const: Any = None
    source: Optional[str] = None
    type: Optional[str] = None
    map: Mapping[str, str] = MappingProxyType({})
    null: str = "null_result"
    with_: Optional[str] = None
    bad: str = "raise"


@dataclass(frozen=True)
class GdalSpec:
    """A row's `gdal` block: the algorithm, its inputs and output, the write profile and the argument
    template. Both tiers turn it into the same argv."""

    alg: str
    inputs: Tuple[Tuple[str, str], ...]  # (param, input kind)
    out_kind: str
    out_dtype: str  # a GDAL type name, or "input" (the first input's band type)
    write: str
    args: Tuple[GdalArg, ...]
    needs_crs: bool = (
        False  # the input must carry a CRS (a tile without one is a data error)
    )
    gdal_min: Optional[str] = None


@dataclass(frozen=True)
class GdalSection:
    """The manifest's top-level `gdal` section: the GDAL release the specs are checked against, the
    arguments no spec or caller may set, and the algorithms (with their arguments) the engine runs.
    """

    pin: str
    deny: frozenset
    allow: Mapping[str, Tuple[str, ...]]
    # The virtual-tile driver policy: a virtual tile opens with every raster driver but these (stored pipelines and
    # the service drivers, GDALManager's GDAL_SKIP list); both tiers read this one list.
    virtual_refuse: frozenset = frozenset()


def _closed(raw: Mapping, keys: frozenset, what: str) -> None:
    extra = sorted(set(raw) - keys)
    if extra:
        raise ValueError(f"gdal spec: unknown {what} key(s) {extra}")


def _one_of(value, allowed: frozenset, what: str) -> str:
    if value not in allowed:
        raise ValueError(f"gdal spec: {what} {value!r} is not one of {sorted(allowed)}")
    return value


def _parse_gdal_arg(raw: Mapping) -> GdalArg:
    _closed(raw, GDAL_ARG_KEYS, "argument")
    name = raw.get("arg")
    if not isinstance(name, str) or not name:
        raise ValueError(f"gdal spec: an argument needs a name ({dict(raw)})")
    if ("const" in raw) == ("from" in raw):
        raise ValueError(
            f"gdal spec: argument {name!r} needs exactly one of const and from"
        )
    if "from" in raw:
        _one_of(raw.get("type"), GDAL_BINDERS, f"binder of {name!r}")
    return GdalArg(
        arg=name,
        const=raw.get("const"),
        source=raw.get("from"),
        type=raw.get("type"),
        map=MappingProxyType(dict(raw.get("map", {}))),
        null=_one_of(
            raw.get("null", "null_result"), GDAL_NULL, f"null rule of {name!r}"
        ),
        with_=raw.get("with"),
        bad=_one_of(raw.get("bad", "raise"), GDAL_BAD, f"bad rule of {name!r}"),
    )


def parse_gdal_spec(raw: Mapping) -> GdalSpec:
    """A row's `gdal` block -> GdalSpec. Refuses any key or word outside the closed vocabulary, so a
    mistyped key can never be read as absent."""
    _closed(raw, GDAL_KEYS, "block")
    out = raw.get("out") or {}
    _closed(out, frozenset({"kind", "dtype"}), "out")
    inputs = tuple(
        (p, _one_of(k, GDAL_IN_KINDS, f"input kind of {p!r}"))
        for p, k in (raw.get("in") or {}).items()
    )
    return GdalSpec(
        alg=str(raw.get("alg", "")),
        inputs=inputs,
        out_kind=_one_of(out.get("kind"), GDAL_OUT_KINDS, "output kind"),
        out_dtype=str(out.get("dtype", "input")),
        write=_one_of(raw.get("write"), GDAL_WRITE, "write profile"),
        args=tuple(_parse_gdal_arg(a) for a in raw.get("args", ())),
        needs_crs=bool(raw.get("needs_crs", False)),
        gdal_min=raw.get("gdal_min"),
    )


def _parse_gdal_section(raw: Optional[Mapping]) -> Optional[GdalSection]:
    if raw is None:
        return None
    _closed(raw, frozenset({"pin", "deny", "allow", "virtual_tile_refuse"}), "section")
    return GdalSection(
        pin=str(raw["pin"]),
        deny=frozenset(raw.get("deny", ())),
        allow=MappingProxyType(
            {alg: tuple(args) for alg, args in raw.get("allow", {}).items()}
        ),
        virtual_refuse=frozenset(raw.get("virtual_tile_refuse", ())),
    )


@dataclass(frozen=True)
class Helper:
    """A top-level DataFrame helper (not a SQL function): where it lives, and how settled it is."""

    name: str
    module: str
    status: str


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
    # A generator's output columns as (name, SQL type): what its UC table function RETURNS.
    columns: Tuple[Tuple[str, str], ...] = ()
    # The GDAL spec engine's spec for this row (its `gdal` block), when the row has one.
    gdal: Optional[GdalSpec] = None

    @property
    def max_args(self) -> int:
        return len(self.params)


@dataclass(frozen=True, eq=False)
class Manifest:
    functions: Mapping[str, Function]
    uc_dependencies: Mapping[str, str]
    level_table_version: str
    helpers: Tuple[Helper, ...] = ()
    gdal: Optional[GdalSection] = None


def sql_type(t: str) -> str:
    """Symbolic manifest type -> SQL type text."""
    if t == "ARRAY<TILE>":
        return f"array<{TILE_SQL}>"
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
            tuple((c[0], c[1]) for c in r.get("columns", [])),
            parse_gdal_spec(r["gdal"]) if "gdal" in r else None,
        )
    return Manifest(
        MappingProxyType(fns),
        MappingProxyType(dict(raw["uc_dependencies"])),
        raw["level_table_version"],
        tuple(
            Helper(h["name"], h["module"], h["status"]) for h in raw.get("helpers", [])
        ),
        _parse_gdal_section(raw.get("gdal")),
    )


def resolve_ref(ref: str) -> Any:
    """'pkg.module:attr' -> the object (imports the module)."""
    mod, _, attr = ref.partition(":")
    return getattr(importlib.import_module(mod), attr)
