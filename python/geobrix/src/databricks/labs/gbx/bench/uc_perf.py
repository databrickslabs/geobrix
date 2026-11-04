"""The Unity Catalog performance measurements (M0-M13): statements for a SQL warehouse, a runner for Serverless env 6.

One plan feeds both surfaces. ``perf_plan`` builds, per measurement, the probe functions and tables it needs, the timed
statements and the DROPs, from the manifest and this wheel; ``uc_perf_sql`` flattens it into ``(cid, sql)`` pairs for
``gbx-uc-sql`` (a warehouse), ``run_uc_perf`` runs it from a Serverless notebook, and ``run_proc_warm`` measures the
procedure CALL, warm and cold. Nothing here is run by a test against Databricks.

Rules the plan keeps:

* Probe functions and tables are named ``gbx_mvp_perf_*`` (functions built from a product row use
  ``ddl.function_ddl(..., name=...)``), beside the product functions and never over them. Only ``gbx_mvp_`` routines and
  ``gbx_mvp_perf_`` tables are dropped. A probe copy of a SQL kernel creates its helpers as ``gbx_mvp_perf_`` routines of
  its own, so no statement of a probe creates, replaces or drops a product routine.
* A measurement whose product function does not exist in this wheel yet (the manifest holds no UC form of it) is not
  built: the plan records ``skipped`` with the missing name, and the same call builds it once the function lands.
* Inputs are generated on Databricks (``range()``, tiles written to the Volume folder by ``perf_stage_tiles``), never
  uploaded. A statement is one statement: no ``;``, no blank line, no leading ``--``.
"""

from __future__ import annotations

import dataclasses
import json
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Tuple

PERF = "gbx_mvp_perf_"
TAG = "T28 uc-perf"
SURFACES = ("w", "s6")

# Row counts and tile counts of each measurement (the design's table); ``sizes=`` overrides any key, so a smoke run is
# one call with small numbers. m3 adds 100M rows on Serverless.
DEFAULT_SIZES: Dict[str, Tuple[int, ...]] = {
    "m1": (1_000_000, 10_000_000),
    "m2": (1_000_000, 10_000_000),
    "m2_diff": (100_000,),
    "m3": (10_000_000,),
    "m3_s6": (10_000_000, 100_000_000),
    "m4_rows": (100_000,),
    "m5": (10, 100, 1000),
    "m6": (1_000, 100_000, 1_000_000),
    "m6b": (1_000, 100_000),
    "m7": (100_000, 1_000_000, 10_000_000),
    "m8": (10, 50, 200),
    "m10": (16, 64, 256),
    "m11": (100, 100),  # zones, tiles
    "m12": (100,),
    "m13": (10_000,),
}
UNITS = tuple(f"m{i}" for i in range(14) if i != 9)  # m0 .. m13; m9 is run_proc_warm's
M10_MIB = (16, 64, 256)
REFERENCE_MIB = 1  # the 512 x 512 Float32 reference tile
TILE_CRS = "EPSG:32633"
TILE_ORIGIN = (500000.0, 5000000.0)
H3_STATS = ("avg", "count", "max", "min", "stddev", "sum", "variance")
BIG_KEY_TILE = (
    "perf_4band.tif"  # 512 x 512 x 4 Float32, the aggregate tile of m8 and m12
)


def _n(n: int) -> str:
    """A row count in a label: 1000000 -> 1m, 100000 -> 100k, 100 -> 100."""
    for div, suffix in ((1_000_000, "m"), (1_000, "k")):
        if n >= div and n % div == 0:
            return f"{n // div}{suffix}"
    return str(n)


@dataclass
class Leg:
    """One statement of a measurement. ``timed=False`` is a preparation (a DROP of a target before a CALL)."""

    label: str
    sql: str
    surfaces: Tuple[str, ...] = SURFACES
    timed: bool = True


@dataclass
class Unit:
    """One measurement: what it creates, what it times, what it could not build and why, what it drops."""

    id: str
    title: str
    setup: List[Tuple[str, str]] = field(default_factory=list)
    legs: List[Leg] = field(default_factory=list)
    drops: List[Tuple[str, str]] = field(default_factory=list)
    skipped: Dict[str, str] = field(default_factory=dict)
    session_fns: Tuple[
        str, ...
    ] = ()  # session functions a Serverless leg calls (registered before the unit)


@dataclass
class Ctx:
    catalog: str
    schema: str
    q: str
    wheel_uri: str
    jar_path: Optional[str]
    tile_dir: str
    sizes: Dict[str, Tuple[int, ...]]
    surface: str
    man: object
    version: str


# ---- building blocks -------------------------------------------------------------------------------------------


def _has_uc(c: Ctx, *names: str) -> List[str]:
    """The names among ``names`` that have no UC form in the manifest (empty: all there)."""
    return [
        n
        for n in names
        if n not in c.man.functions or "L4" not in c.man.functions[n].levels
    ]


def _need(u: Unit, c: Ctx, key: str, *names: str) -> bool:
    """True when every named UC function exists; else record why ``key`` was not built."""
    missing = _has_uc(c, *names)
    if missing:
        u.skipped[key] = f"needs {', '.join(missing)} (no UC form in this wheel yet)"
    return not missing


def _hash(expr: str) -> str:
    return f"pmod(xxhash64({expr}), 2147483647)"


def _tile_select(c: Ctx, name: str) -> str:
    """A one-row SELECT of ``tile``: the staged GeoTIFF ``name`` wrapped in the v2 tile struct (as the P2 probe does)."""
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import quote_str

    return (
        "SELECT CAST(named_struct('cellid', CAST(0 AS BIGINT), 'raster', content, 'path', NULL, 'path_mode', NULL, "
        "'window', NULL, 'clip_polygon', NULL, 'clip_crs', NULL, "
        f"'crs', '{TILE_CRS}', 'metadata', CAST(map() AS map<string,string>)) AS {TILE_SQL}) AS tile "
        f"FROM read_files({quote_str(f'{c.tile_dir}/{name}')}, format => 'binaryFile')"
    )


def _tiles_table(
    c: Ctx, n: int, name: str = "perf_1MiB.tif", short: str = "tiles"
) -> Tuple[str, str, str]:
    """``(table, setup label, CREATE)``: ``n`` copies of one staged tile, ``(i, tile)``."""
    table = f"{c.q}.{PERF}{short}_{n}"
    return (
        table,
        f"create_{short}_{n}",
        f"CREATE OR REPLACE TABLE {table} AS SELECT i, tile FROM "
        f"(SELECT explode(sequence(1, {n})) AS i) CROSS JOIN ({_tile_select(c, name)})",
    )


_CREATED = re.compile(r"CREATE OR REPLACE FUNCTION ((?:`[^`]*`|[^\s(`])+)\(")
_HELPER_CALL = re.compile(r"\{q\}gbx__([a-z0-9_]+)\(")


def _drops_for(stmts: Sequence[str]) -> List[Tuple[str, str]]:
    """DROPs of the probe routines ``stmts`` create (only ``gbx_mvp_`` routines: a product routine is never dropped)."""
    out = []
    for s in stmts:
        m = _CREATED.match(s)
        if m and m.group(1).split(".")[-1].strip("`").startswith("gbx_mvp_"):
            out.append(
                (
                    f"drop_{m.group(1).split('.')[-1].strip('`')}",
                    f"DROP FUNCTION IF EXISTS {m.group(1)}",
                )
            )
    return out


def _probe_helpers(fn):
    """``fn`` with its SQL kernel's helpers renamed ``gbx__<x>`` -> ``gbx_mvp_perf_<x>``: the probe copy creates (and
    drops) helpers of its own and never replaces the product's. A row with no SQL kernel is returned as it is.
    """
    from databricks.labs.gbx.uc import sqlbody

    src = sqlbody.l4_source(fn)
    if src is None:
        return fn

    def rename(text: str) -> str:
        return _HELPER_CALL.sub(lambda m: f"{{q}}{PERF}{m.group(1)}(", text)

    body, helpers = src
    helpers = tuple(
        {
            **h,
            "name": f"{PERF}{h['name'][len('gbx__'):]}",
            "sql": rename(h["sql"]),
        }
        for h in helpers
    )
    l4 = {**fn.levels["L4"], "sql": rename(body)}
    if helpers:
        l4["sql_helpers"] = helpers
    return dataclasses.replace(fn, levels={**fn.levels, "L4": l4})


def _copy(c: Ctx, u: Unit, product: str, short: str, **kw) -> Optional[str]:
    """Create the probe copy ``gbx_mvp_perf_<short>`` of a product scalar in the form ``kw`` names; its qualified name,
    or None (recorded in ``u.skipped``) when the row cannot be that form. A SQL kernel's helpers are created as
    ``gbx_mvp_perf_`` routines of the probe, never over the product's."""
    from databricks.labs.gbx.uc import ddl

    fn = c.man.functions[product]
    form = kw.get("form")
    if form in (None, "sql"):
        fn = _probe_helpers(fn)
    try:
        stmts = ddl.function_ddl(
            fn,
            c.catalog,
            c.schema,
            c.wheel_uri,
            c.man.uc_dependencies,
            c.version,
            name=f"{PERF}{short}",
            jar_uri=c.jar_path if form == "scala" else None,
            **kw,
        )
    except ValueError as exc:
        u.skipped[short] = str(exc)
        return None
    for i, s in enumerate(stmts):
        u.setup.append((f"create_{short}" + (f"_{i}" if i else ""), s))
    u.drops += [d for d in _drops_for(stmts) if d not in u.drops]
    return f"{c.q}.{PERF}{short}"


def _tail(c: Ctx, key: str) -> Tuple[int, ...]:
    return tuple(c.sizes[key])


# ---- M0 floors -------------------------------------------------------------------------------------------------


def _m0(c: Ctx) -> Unit:
    from databricks.labs.gbx.uc.ddl import quote_str

    u = Unit(
        "m0",
        "per-statement floors: SELECT 1, a trivial Python function, one with the geobrix environment, a SQL function",
    )
    env = quote_str(
        json.dumps(
            [f"geobrix @ {c.wheel_uri}"]
            + [f"{k}=={v}" for k, v in c.man.uc_dependencies.items()]
        )
    )
    py = f"{c.q}.{PERF}py_noop"
    pe = f"{c.q}.{PERF}py_env"
    sq = f"{c.q}.{PERF}sql_noop"
    u.setup += [
        (
            "create_py_noop",
            f"CREATE OR REPLACE FUNCTION {py}(x INT) RETURNS INT LANGUAGE PYTHON DETERMINISTIC AS $$\nreturn x\n$$",
        ),
        (
            "create_py_env",
            f"CREATE OR REPLACE FUNCTION {pe}(x INT) RETURNS INT LANGUAGE PYTHON DETERMINISTIC\n"
            f"ENVIRONMENT (dependencies = {env}, environment_version = '6')\nAS $$\nreturn x\n$$",
        ),
        (
            "create_sql_noop",
            f"CREATE OR REPLACE FUNCTION {sq}(x INT) RETURNS INT LANGUAGE SQL DETERMINISTIC RETURN x + 1",
        ),
    ]
    u.drops += [
        (f"drop_{n}", f"DROP FUNCTION IF EXISTS {f}")
        for n, f in (("py_noop", py), ("py_env", pe), ("sql_noop", sq))
    ]
    u.legs += [
        Leg("select1", "SELECT 1"),
        Leg("py_noop", f"SELECT {py}(1)"),
        Leg("py_env", f"SELECT {pe}(1)"),
        Leg("sql_noop", f"SELECT {sq}(1)"),
    ]
    return u


# ---- M1 is a SQL function inlined ------------------------------------------------------------------------------

_QRES = "bit_and(shiftright({c}, 52), 31)"  # the quadbin resolution field, the control SQL body


def _m1(c: Ctx) -> Unit:
    u = Unit(
        "m1",
        "is a SQL UDF inlined into the caller's plan (plan and time against the inline expression)",
    )
    f = f"{c.q}.{PERF}qres"
    u.setup.append(
        (
            "create_qres",
            f"CREATE OR REPLACE FUNCTION {f}(c BIGINT) RETURNS INT LANGUAGE SQL DETERMINISTIC RETURN {_QRES.format(c='c')}",
        )
    )
    u.drops.append(("drop_qres", f"DROP FUNCTION IF EXISTS {f}"))
    u.legs += [
        Leg(
            "explain_sql",
            f"EXPLAIN FORMATTED SELECT sum({f}(shiftleft(id % 27, 52))) FROM range(1000)",
        ),
        Leg(
            "explain_inline",
            f"EXPLAIN FORMATTED SELECT sum({_QRES.format(c='shiftleft(id % 27, 52)')}) FROM range(1000)",
        ),
    ]
    for n in _tail(c, "m1"):
        gen = f"(SELECT shiftleft(id % 27, 52) AS c FROM range({n}))"
        u.legs += [
            Leg(
                f"t_sql_{_n(n)}", f"SELECT count(*), sum({_hash(f'{f}(c)')}) FROM {gen}"
            ),
            Leg(
                f"t_inline_{_n(n)}",
                f"SELECT count(*), sum({_hash(_QRES.format(c='c'))}) FROM {gen}",
            ),
        ]
    return u


# ---- M2 / M3 SQL vs Scala vs Python vs batch ---------------------------------------------------------------------

_BNG_CELLS = (
    "(SELECT concat('TQ', lpad(CAST(id % 100 AS STRING), 2, '0'), "
    "lpad(CAST((id DIV 100) % 100 AS STRING), 2, '0')) AS c FROM range({n}))"
)
_GRID_FNS = {
    "bng_eastnorthasbng": (
        "gbx_bng_eastnorthasbng",
        "(SELECT rand(7) * 700000 AS e, rand(8) * 1300000 AS n FROM range({n}))",
        "{f}(e, n, '3')",
        False,
    ),
    "bng_cellarea": ("gbx_bng_cellarea", _BNG_CELLS, "{f}(c)", True),
}


def _has_batch(fn) -> bool:
    import importlib

    handler = fn.levels.get("L4", {}).get("handler")
    if (
        not handler
    ):  # a SQL-kernel or table-function row has no Python handler, so no batch twin
        return False
    mod, _, attr = handler.partition(":")
    try:
        return hasattr(importlib.import_module(mod), f"{attr}_batch")
    except (ImportError, ValueError):
        return False


def _forms(
    c: Ctx, u: Unit, short: str, product: str, want_batch: bool = True
) -> Dict[str, str]:
    """The probe copies of one grid scalar by form: python (always), sql (when the row has a SQL kernel), scala (with a
    JAR and a Scala handler), batch (when the handler has a ``_batch`` twin)."""
    fn = c.man.functions[product]
    out: Dict[str, str] = {}
    py = _copy(c, u, product, f"{short}_py", form="python")
    if py:
        out["py"] = py
    from databricks.labs.gbx.uc import sqlbody

    if sqlbody.l4_source(fn) is not None:
        sql = _copy(c, u, product, f"{short}_sql", form="sql")
        if sql:
            out["sql"] = sql
    else:
        u.skipped[f"{short}_sql"] = "no SQL kernel for this row yet"
    if c.jar_path and "scala" in fn.levels["L4"]:
        sc = _copy(c, u, product, f"{short}_scala", form="scala")
        if sc:
            out["scala"] = sc
    else:
        u.skipped[f"{short}_scala"] = (
            "no JAR given"
            if "scala" in fn.levels["L4"]
            else "no Scala handler for this row"
        )
    if want_batch and _has_batch(fn):
        b = _copy(c, u, product, f"{short}_batch", form="python", style="batch")
        if b:
            out["batch"] = b
    elif want_batch:
        u.skipped[f"{short}_batch"] = "the handler has no _batch twin"
    return out


def _m2(c: Ctx) -> Unit:
    u = Unit(
        "m2",
        "SQL vs Scala vs Python vs batched Python for a grid scalar, plus equality of each form with the Python one",
    )
    for short, (product, gen, call, is_double) in _GRID_FNS.items():
        if not _need(u, c, short, product):
            continue
        forms = _forms(c, u, short, product)
        for n in _tail(c, "m2"):
            for form, f in forms.items():
                u.legs.append(
                    Leg(
                        f"t_{short}_{form}_{_n(n)}",
                        f"SELECT count(*), sum({_hash(call.format(f=f))}) FROM {gen.format(n=n)}",
                    )
                )
        if "py" not in forms:
            u.skipped[f"diff_{short}"] = (
                "no Python copy to compare the other forms with"
            )
            continue
        for n in _tail(c, "m2_diff"):
            for form, f in forms.items():
                if form == "py":
                    continue
                a, b = call.format(f=forms["py"]), call.format(f=f)
                bad = (
                    f"abs(({a}) - ({b})) > 1e-9"
                    if is_double
                    else f"({a}) IS DISTINCT FROM ({b})"
                )
                u.legs.append(
                    Leg(
                        f"diff_{short}_{form}_vs_py_{_n(n)}",
                        f"SELECT count(*) FROM {gen.format(n=n)} WHERE {bad}",
                    )
                )
    return u


def _m3(c: Ctx) -> Unit:
    u = Unit(
        "m3",
        "batched vs row Python at scale (an east/north point to a cell; the polygon fill when it has a batch handler)",
    )
    short = "bng_eastnorthasbng"
    product, gen, call, _is_double = _GRID_FNS[short]
    key = "m3_s6" if c.surface == "s6" else "m3"
    if _need(u, c, short, product):
        py = _copy(c, u, product, f"m3_{short}_py", form="python")
        fn = c.man.functions[product]
        b = (
            _copy(c, u, product, f"m3_{short}_batch", form="python", style="batch")
            if _has_batch(fn)
            else None
        )
        if not b:
            u.skipped[f"{short}_batch"] = "the handler has no _batch twin"
        for n in _tail(c, key):
            for form, f in (("py", py), ("batch", b)):
                if f:
                    u.legs.append(
                        Leg(
                            f"t_{short}_{form}_{_n(n)}",
                            f"SELECT count(*), sum({_hash(call.format(f=f))}) FROM {gen.format(n=n)}",
                        )
                    )
    if not _need(u, c, "polyfill", "gbx_bng_polyfill"):
        return u
    poly = c.man.functions["gbx_bng_polyfill"]
    if _has_batch(poly):
        pp = _copy(c, u, "gbx_bng_polyfill", "m3_polyfill_py", form="python")
        pb = _copy(
            c, u, "gbx_bng_polyfill", "m3_polyfill_batch", form="python", style="batch"
        )
        call_p = lambda fn_name: f"size({fn_name}(g, '3'))"  # noqa: E731
        pts = (
            "(SELECT CAST(st_geomfromwkt(concat('POINT(', 530000 + (id % 1000), ' ', 180000 + (id DIV 1000) % 1000, ')'), 27700) "
            "AS GEOMETRY(ANY)) AS g FROM range({n}))"
        )
        for n in _tail(c, key):
            for form, f in (("py", pp), ("batch", pb)):
                if f:
                    u.legs.append(
                        Leg(
                            f"t_polyfill_{form}_{_n(n)}",
                            f"SELECT count(*), sum({_hash(call_p(f))}) FROM {pts.format(n=n)}",
                        )
                    )
    else:
        u.skipped["polyfill_batch"] = "gbx_bng_polyfill has no _batch handler"
    return u


# ---- M4 sandbox sharing ------------------------------------------------------------------------------------------

# (short, product, row-wise call over `i`, one-row call, needs a tile): six functions with distinct dependency lists.
_M4_FNS = (
    ("kring", "gbx_bng_kring", "{f}('TQ3080', 1 + i % 2)", False),
    (
        "pointascell",
        "gbx_quadbin_pointascell",
        "{f}(-0.1 + i * 0.000001, 51.5, '10')",
        False,
    ),
    (
        "explainvalidity",
        "gbx_st_explainvalidity",
        "{f}(CAST(st_geomfromwkt(concat('POINT(', i % 180, ' 1)'), 4326) AS GEOMETRY(ANY)))",
        False,
    ),
    ("width", "gbx_rst_width", "{f}(tile)", True),
    ("hillshade", "gbx_rst_hillshade", "{f}(tile, 315.0, 45.0, 1.0, 1.0, 1.0)", True),
    ("ndvi", "gbx_rst_ndvi", "{f}(tile, 1, 1)", True),
)


def _m4(c: Ctx) -> Unit:
    u = Unit(
        "m4",
        "sandbox sharing: 1, 3 and 6 distinct Python functions in one statement, per-function vs shared dependency lists, a repeated call, header cost by tile size",
    )
    avail = [x for x in _M4_FNS if not _has_uc(c, x[1])]
    for short, product, _call, _t in avail:
        _copy(c, u, product, f"sh_{short}", form="python", shared_deps=True)
    tile_cte = f"WITH t AS ({_tile_select(c, 'perf_1MiB.tif')}) "

    def stmt(variant: str, count: int, rows: int) -> Optional[str]:
        chosen = avail[:count]
        if len(chosen) < count:
            return None
        tile_needed = any(x[3] for x in chosen)
        if tile_needed and rows != 1:
            return None
        parts = []
        for short, product, call, _t in chosen:
            f = f"{c.q}.{product}" if variant == "a" else f"{c.q}.{PERF}sh_{short}"
            parts.append(f"length(CAST({call.format(f=f)} AS STRING))")
        body = " + ".join(parts)
        if tile_needed:
            return f"{tile_cte}SELECT sum({body}) FROM t CROSS JOIN (SELECT CAST(1 AS BIGINT) AS i)"
        return f"SELECT sum({body}) FROM (SELECT id AS i FROM range({rows}))"

    for rows in dict.fromkeys(
        (1, *_tail(c, "m4_rows"))
    ):  # 1 once, whatever m4_rows holds
        for count in (1, 3, 6):
            for variant in ("a", "b"):
                s = stmt(variant, count, rows)
                if s:
                    u.legs.append(Leg(f"{variant}_{count}fn_{_n(rows)}r", s))
    if len(avail) < 6:
        u.skipped["6fn"] = f"{6 - len(avail)} of the six functions have no UC form"
    if avail:
        short, product, call, _t = avail[0]
        f = f"{c.q}.{product}"
        for rows in _tail(c, "m4_rows"):
            row = call.format(f=f)
            gen = f"(SELECT id AS i FROM range({rows}))"
            u.legs += [
                Leg(f"c_once_{_n(rows)}r", f"SELECT sum(size({row})) FROM {gen}"),
                Leg(
                    f"c_twice_{_n(rows)}r",
                    f"SELECT sum(size({row}) + size({row})) FROM {gen}",
                ),
            ]
    if not _has_uc(c, "gbx_rst_width"):
        for mib in (1, 64):
            u.legs.append(
                Leg(
                    f"b_width_{mib}mib",
                    f"SELECT {c.q}.gbx_rst_width(tile) FROM ({_tile_select(c, f'perf_{mib}MiB.tif')})",
                )
            )
    return u


# ---- M5 raster to grid ----------------------------------------------------------------------------------------


def _m5(c: Ctx) -> Unit:
    u = Unit(
        "m5",
        "raster to grid: one partial kernel serving seven statistics vs seven TVFs; across tiles, recipe F1 vs the per-tile TVF plus GROUP BY",
    )
    partial = "gbx_rst_h3_rastertogrid_partial"
    tvfs = tuple(f"gbx_rst_h3_rastertogrid{s}" for s in H3_STATS)
    if not _need(u, c, "m5", partial, *tvfs):
        return u
    for n in _tail(c, "m5"):
        table, label, create = _tiles_table(c, n)
        u.setup.append((label, create))
        u.drops.append((f"drop_tiles_{n}", f"DROP TABLE IF EXISTS {table}"))
        p = f"{c.q}.{partial}"
        part = f"SELECT p.* FROM {table} r LATERAL VIEW inline({p}(r.tile, 7)) p"
        u.legs.append(
            Leg(
                f"fused_{n}t",
                f"WITH p AS ({part}) SELECT count(*), sum({_hash('band, cellid, sum / count, count, sum, min, max, m2 / count, sqrt(m2 / count)')}) FROM p",
            )
        )
        for s in H3_STATS:
            u.legs.append(
                Leg(
                    f"single_{s}_{n}t",
                    f"SELECT count(*), sum({_hash('t.band, t.cellid, t.measure')}) FROM {table} r, "
                    f"LATERAL {c.q}.gbx_rst_h3_rastertogrid{s}(r.tile, 7) AS t",
                )
            )
        f1 = (
            f"WITH p AS ({part}), "
            "w AS (SELECT *, sum(sum) OVER (PARTITION BY band, cellid) / sum(count) OVER (PARTITION BY band, cellid) AS mu FROM p), "
            "g AS (SELECT band, cellid, sum(count) AS count, sum(sum) AS sum, min(min) AS min, max(max) AS max, "
            "sum(sum) / sum(count) AS avg, (sum(m2) + sum(CASE WHEN count > 0 THEN count * pow(sum / count - mu, 2) END)) / sum(count) AS variance "
            "FROM w GROUP BY band, cellid) "
            f"SELECT count(*), sum({_hash('band, cellid, count, avg, variance')}) FROM g"
        )
        u.legs += [
            Leg(f"f1_{n}t", f1),
            Leg(
                f"tvf_groupby_{n}t",
                f"SELECT count(*), sum({_hash('band, cellid, m')}) FROM (SELECT t.band, t.cellid, avg(t.measure) AS m FROM {table} r, "
                f"LATERAL {c.q}.gbx_rst_h3_rastertogridavg(r.tile, 7) AS t GROUP BY t.band, t.cellid)",
            ),
        ]
    return u


# ---- M6 cell fill and unions -------------------------------------------------------------------------------------


def _c1(src: str) -> str:
    """Recipe C1 (H3 fill at table scale, native joins) over ``src(grp, cellid, value)``, as a checksum."""
    return (
        f"WITH g AS (SELECT grp, cellid, max(value) AS value FROM {src} GROUP BY grp, cellid), "
        "holes AS (SELECT grp, cellid FROM g WHERE value IS NULL), "
        "nb AS (SELECT h.grp, h.cellid, n.cellid AS nb, n.distance AS d FROM holes h "
        "LATERAL VIEW inline(h3_kringdistances(h.cellid, 1)) n WHERE n.distance >= 1), "
        "f AS (SELECT nb.grp, nb.cellid, avg(g.value) AS mean_v FROM nb JOIN g ON g.grp = nb.grp AND g.cellid = nb.nb "
        "AND g.value IS NOT NULL GROUP BY nb.grp, nb.cellid) "
        "SELECT count(*), sum(coalesce(g.value, f.mean_v)) FROM g LEFT JOIN f ON f.grp = g.grp AND f.cellid = g.cellid"
    )


def _m6(c: Ctx) -> Unit:
    u = Unit(
        "m6",
        "cell fill: the SQL array form vs the Python array form vs the native recipe; quadbin and BNG unions vs their native recipes",
    )
    from databricks.labs.gbx.uc import ddl

    have_twin = "gbx__h3_cellfill_cells" in c.man.private
    if have_twin:
        pf = dataclasses.replace(
            c.man.private["gbx__h3_cellfill_cells"], name="gbx_mvp_perf_h3_cellfill"
        )
        twin_sql = ddl.private_ddl(
            pf, c.catalog, c.schema, c.wheel_uri, c.man.uc_dependencies, c.version
        )
        u.setup.append(("create_py_twin", twin_sql))
        u.drops += _drops_for([twin_sql])
    else:
        u.skipped["py_twin"] = (
            "needs the private routine gbx__h3_cellfill_cells (not in this wheel)"
        )
    have_sql = _need(u, c, "sql_array", "gbx_h3_cellfill_array")
    for n in _tail(c, "m6"):
        table = f"{c.q}.{PERF}cells_{n}"
        u.setup.append(
            (
                f"create_cells_{_n(n)}",
                f"CREATE OR REPLACE TABLE {table} AS SELECT 0 AS grp, cellid, max(value) AS value FROM "
                f"(SELECT h3_longlatash3(-0.5 + (id % 1000) * 0.006, 51.3 + (id DIV 1000) * 0.005, 8) AS cellid, "
                f"CASE WHEN id % 10 = 0 THEN CAST(NULL AS DOUBLE) ELSE rand(3) END AS value FROM range({n})) GROUP BY cellid",
            )
        )
        u.drops.append((f"drop_cells_{_n(n)}", f"DROP TABLE IF EXISTS {table}"))
        arr = "collect_list(struct(cellid, value))"
        if have_sql:
            u.legs.append(
                Leg(
                    f"sql_array_{_n(n)}",
                    f"SELECT grp, size({c.q}.gbx_h3_cellfill_array({arr}, 1, 'mean', 2.0)) FROM {table} GROUP BY grp",
                )
            )
        if have_twin:
            u.legs.append(
                Leg(
                    f"py_twin_{_n(n)}",
                    f"SELECT grp, size({c.q}.gbx_mvp_perf_h3_cellfill({arr}, 1, 'mean', 2.0)) FROM {table} GROUP BY grp",
                )
            )
        u.legs.append(Leg(f"c1_{_n(n)}", _c1(table)))
    # M6b: quadbin cells, the Python union twin vs a native union (recipe C2)
    if _need(
        u,
        c,
        "m6b",
        "gbx_quadbin_cellunion",
        "gbx_quadbin_aswkb",
        "gbx_quadbin_pointascell",
    ):
        for n in _tail(c, "m6b"):
            table = f"{c.q}.{PERF}qcells_{n}"
            u.setup.append(
                (
                    f"create_qcells_{_n(n)}",
                    f"CREATE OR REPLACE TABLE {table} AS SELECT 0 AS grp, {c.q}.gbx_quadbin_pointascell("
                    f"-0.5 + (id % 1000) * 0.001, 51.3 + (id DIV 1000) * 0.001, '12') AS cell FROM range({n})",
                )
            )
            u.drops.append((f"drop_qcells_{_n(n)}", f"DROP TABLE IF EXISTS {table}"))
            u.legs += [
                Leg(
                    f"b_twin_{_n(n)}",
                    f"SELECT grp, length({c.q}.gbx_quadbin_cellunion(collect_list(cell))) FROM {table} GROUP BY grp",
                ),
                Leg(
                    f"b_c2_{_n(n)}",
                    f"SELECT grp, st_area(st_union_agg(st_geomfromwkb({c.q}.gbx_quadbin_aswkb(cell), 4326))) FROM {table} GROUP BY grp",
                ),
            ]
    # M6c: BNG chips, recipe C3 vs the SQL fold
    if _need(
        u,
        c,
        "m6c",
        "gbx_bng_cellunion_array",
        "gbx_bng_aswkb",
        "gbx_bng_tessellateexplode",
    ):
        chips = f"{c.q}.{PERF}chips"
        poly = "POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"
        a, b = poly.format(x0=520000, y0=170000, x1=540000, y1=190000), poly.format(
            x0=530000, y0=180000, x1=550000, y1=200000
        )
        u.setup.append(
            (
                "create_chips",
                f"CREATE OR REPLACE TABLE {chips} AS SELECT t.cellid, t.core, t.chip FROM "
                f"(SELECT CAST(st_geomfromwkt(w, 27700) AS GEOMETRY(ANY)) AS g FROM VALUES ('{a}'), ('{b}') AS v(w)) s, "
                f"LATERAL {c.q}.gbx_bng_tessellateexplode(s.g, '1km', false) AS t",
            )
        )
        u.drops.append(("drop_chips", f"DROP TABLE IF EXISTS {chips}"))
        u.legs += [
            Leg(
                "c_c3",
                f"SELECT count(*), sum(length(u)) FROM (SELECT cellid, CASE WHEN bool_or(core) THEN {c.q}.gbx_bng_aswkb(cellid) "
                f"ELSE st_asbinary(st_union_agg(st_geomfromwkb(chip, 27700))) END AS u FROM {chips} GROUP BY cellid)",
            ),
            Leg(
                "c_fold",
                f"SELECT count(*), sum(length(u.chip)) FROM (SELECT {c.q}.gbx_bng_cellunion_array(collect_list(struct(core, chip))) AS u "
                f"FROM {chips} GROUP BY cellid)",
            ),
        ]
    else:
        u.skipped["c_python_twin"] = "no Python union twin of the chips yet"
    return u


# ---- M7 binpoints -------------------------------------------------------------------------------------------------


def _m7(c: Ctx) -> Unit:
    u = Unit(
        "m7",
        "points to a raster: the partial/finalize recipe vs the array twin (the twin has a ceiling; a refusal is a result)",
    )
    canvas = "0D, 0D, 1024D, 1024D, 1024, 1024"
    d1 = _has_uc(c, "gbx_rst_binpoints_partial", "gbx_rst_binpoints_finalize")
    if d1:
        u.skipped["d1"] = f"needs {', '.join(d1)} (no UC form in this wheel yet)"
    for n in _tail(c, "m7"):
        table = f"{c.q}.{PERF}pts_{n}"
        u.setup.append(
            (
                f"create_pts_{_n(n)}",
                f"CREATE OR REPLACE TABLE {table} AS SELECT rand(5) * 1024 AS x, rand(6) * 1024 AS y, rand(7) AS z FROM range({n})",
            )
        )
        u.drops.append((f"drop_pts_{_n(n)}", f"DROP TABLE IF EXISTS {table}"))
        u.legs.append(
            Leg(
                f"twin_{_n(n)}",
                f"SELECT length({c.q}.gbx_rst_binpoints(collect_list(x), collect_list(y), collect_list(z), {canvas}, 32633, 'max').raster) FROM {table}",
            )
        )
        if not d1:
            u.legs.append(
                Leg(
                    f"d1_{_n(n)}",
                    f"WITH b AS (SELECT {c.q}.gbx_rst_binpoints_partial(x, y, {canvas}) AS px, z FROM {table}), "
                    "c AS (SELECT px.col AS col, px.row AS row, max(z) AS value FROM b WHERE px IS NOT NULL AND z IS NOT NULL "
                    "GROUP BY px.col, px.row) "
                    f"SELECT length({c.q}.gbx_rst_binpoints_finalize(collect_list(struct(col, row, value)), {canvas}, 32633).raster) FROM c",
                )
            )
    return u


# ---- M8 tree merge ------------------------------------------------------------------------------------------------


def _m8(c: Ctx) -> Unit:
    u = Unit(
        "m8",
        "merge by key: the tree procedure vs the 10-tile array twin vs the session aggregate (Serverless only)",
    )
    u.session_fns = ("gbx_rst_merge_agg",)
    tree = "gbx_combine_by_key" in _procedure_names()
    if not tree:
        u.skipped["tree"] = (
            "needs the tree procedures (gbx_combine_by_key is not in this wheel yet)"
        )
    for n in _tail(c, "m8"):
        table = f"{c.q}.{PERF}keyed_{n}"
        u.setup.append(
            (
                f"create_keyed_{n}",
                f"CREATE OR REPLACE TABLE {table} AS SELECT 'k1' AS tile_key, i AS tile_id, tile FROM "
                f"(SELECT explode(sequence(1, {n})) AS i) CROSS JOIN ({_tile_select(c, BIG_KEY_TILE)})",
            )
        )
        u.drops.append((f"drop_keyed_{n}", f"DROP TABLE IF EXISTS {table}"))
        u.legs.append(
            Leg(
                f"twin_{n}t",
                f"SELECT tile_key, length({c.q}.gbx_rst_merge(collect_list(tile)).raster) FROM {table} GROUP BY tile_key",
                ("w", "s6"),
            )
        )
        u.legs.append(
            Leg(
                f"session_agg_{n}t",
                f"SELECT tile_key, length(gbx_rst_merge_agg(tile).raster) FROM {table} GROUP BY tile_key",
                ("s6",),
            )
        )
        if tree:
            plain = table.replace("`", "")
            for proc, extra in (
                ("gbx_mosaic_by_key", ""),
                ("gbx_combine_by_key", ", op => 'avg'"),
            ):
                tgt = f"{c.q}.{PERF}tgt_{proc[4:]}_{n}"
                u.drops.append(
                    (f"drop_tgt_{proc[4:]}_{n}", f"DROP TABLE IF EXISTS {tgt}")
                )
                u.legs += [
                    Leg(
                        f"prep_{proc[4:]}_{n}t",
                        f"DROP TABLE IF EXISTS {tgt}",
                        timed=False,
                    ),
                    Leg(
                        f"{proc[4:]}_{n}t",
                        f"CALL {c.q}.{proc}(source_table => '{plain}', target_table => '{tgt.replace(chr(96), '')}'{extra})",
                    ),
                ]
    return u


def _procedure_names() -> Tuple[str, ...]:
    from databricks.labs.gbx.uc import procedures

    return tuple(procedures.RECIPES)


# ---- M10 decode cap ----------------------------------------------------------------------------------------------

_CLIP_BOX = "POLYGON((500100 4999900, 500400 4999900, 500400 4999600, 500100 4999600, 500100 4999900))"


def _m10(c: Ctx) -> Unit:
    u = Unit(
        "m10",
        "decode cap: a header read, a clip and a window read (retile) on tiles of 16, 64 and 256 MiB decoded",
    )
    for mib in _tail(c, "m10"):
        t = f"({_tile_select(c, f'perf_{mib}MiB.tif')})"
        u.legs += [
            Leg(f"avg_{mib}mib", f"SELECT {c.q}.gbx_rst_avg(tile) FROM {t}"),
            Leg(
                f"clip_{mib}mib",
                f"SELECT length({c.q}.gbx_rst_clip(tile, CAST(st_geomfromwkt('{_CLIP_BOX}', 32633) AS GEOMETRY(ANY)), true).raster) FROM {t}",
            ),
        ]
        if not _has_uc(c, "gbx_rst_retile"):
            u.legs.append(
                Leg(
                    f"retile_{mib}mib",
                    f"SELECT count(*), sum(length(r.raster)) FROM {t} s, LATERAL {c.q}.gbx_rst_retile(s.tile, 512, 512) AS r",
                )
            )
        else:
            u.skipped["retile"] = "needs gbx_rst_retile (no UC form in this wheel yet)"
    return u


# ---- M11 zonal, M12 chain, M13 MVT ---------------------------------------------------------------------------------


def _m11(c: Ctx) -> Unit:
    u = Unit(
        "m11",
        "zonal statistics: the fused partial vs summary of clip, over zones x tiles",
    )
    zones_n, tiles_n = (list(c.sizes["m11"]) + [100, 100])[:2]
    table, label, create = _tiles_table(c, tiles_n)
    zones = f"{c.q}.{PERF}zones_{zones_n}"
    u.setup += [
        (label, create),
        (
            f"create_zones_{zones_n}",
            f"CREATE OR REPLACE TABLE {zones} AS SELECT CAST(id AS INT) AS zone_id, st_geomfromwkt(concat('POLYGON((', "
            "500000 + (id % 10) * 500, ' ', 4999000 + (id DIV 10) % 10 * 100, ', ', 500000 + (id % 10) * 500 + 400, ' ', "
            "4999000 + (id DIV 10) % 10 * 100, ', ', 500000 + (id % 10) * 500 + 400, ' ', 4999000 + (id DIV 10) % 10 * 100 + 80, ', ', "
            "500000 + (id % 10) * 500, ' ', 4999000 + (id DIV 10) % 10 * 100 + 80, ', ', 500000 + (id % 10) * 500, ' ', "
            f"4999000 + (id DIV 10) % 10 * 100, '))'), 32633) AS geom FROM range({zones_n})",
        ),
    ]
    u.drops += [
        ("drop_tiles", f"DROP TABLE IF EXISTS {table}"),
        ("drop_zones", f"DROP TABLE IF EXISTS {zones}"),
    ]
    geom = "CAST(z.geom AS GEOMETRY(ANY))"
    u.legs.append(
        Leg(
            "summary_of_clip",
            f"SELECT count(*), sum(length({c.q}.gbx_rst_summary({c.q}.gbx_rst_clip(t.tile, {geom}, true)))) "
            f"FROM {zones} z CROSS JOIN {table} t",
        )
    )
    if _need(u, c, "partial", "gbx_rst_zonalstats_partial"):
        u.legs.append(
            Leg(
                "zonalstats_partial",
                f"SELECT z.zone_id, s.band, sum(s.count), sum(s.sum) / sum(s.count), min(s.min), max(s.max) FROM {zones} z "
                f"CROSS JOIN {table} t LATERAL VIEW inline({c.q}.gbx_rst_zonalstats_partial(t.tile, {geom})) s GROUP BY z.zone_id, s.band",
            )
        )
    return u


def _m12(c: Ctx) -> Unit:
    u = Unit(
        "m12",
        "the cost of a chain: clip then ndvi in one statement vs the steps apart (the GeoTIFF re-encode between calls)",
    )
    if not _need(u, c, "m12", "gbx_rst_ndvi", "gbx_rst_clip", "gbx_rst_index"):
        return u
    u.session_fns = ("gbx_rst_ndvi", "gbx_rst_clip")
    (n,) = _tail(c, "m12")[:1]
    table, label, create = _tiles_table(c, n, BIG_KEY_TILE, "tiles4")
    clipped = f"{c.q}.{PERF}clipped_{n}"
    box = f"CAST(st_geomfromwkt('{_CLIP_BOX}', 32633) AS GEOMETRY(ANY))"
    u.setup += [
        (label, create),
        (
            f"create_clipped_{n}",
            f"CREATE OR REPLACE TABLE {clipped} AS SELECT i, {c.q}.gbx_rst_clip(tile, {box}, true) AS tile FROM {table}",
        ),
    ]
    u.drops += [
        ("drop_tiles4", f"DROP TABLE IF EXISTS {table}"),
        ("drop_clipped", f"DROP TABLE IF EXISTS {clipped}"),
    ]
    u.legs += [
        Leg(
            "chain_clip_ndvi",
            f"SELECT sum(length({c.q}.gbx_rst_ndvi({c.q}.gbx_rst_clip(tile, {box}, true), 3, 4).raster)) FROM {table}",
        ),
        Leg(
            "clip_only",
            f"SELECT sum(length({c.q}.gbx_rst_clip(tile, {box}, true).raster)) FROM {table}",
        ),
        Leg(
            "ndvi_on_clipped",
            f"SELECT sum(length({c.q}.gbx_rst_ndvi(tile, 3, 4).raster)) FROM {clipped}",
        ),
        Leg(
            "index_on_clipped",
            f"SELECT sum(length({c.q}.gbx_rst_index(tile, 'ndvi', map('red', 3, 'nir', 4)).raster)) FROM {clipped}",
        ),
        Leg(
            "session_chain",
            f"SELECT sum(length(gbx_rst_ndvi(gbx_rst_clip(tile, st_asewkb({box}), true), 3, 4).raster)) FROM {table}",
            ("s6",),
        ),
    ]
    return u


def _m13(c: Ctx) -> Unit:
    u = Unit(
        "m13",
        "MVT finalize: the share of the feature re-encode in gbx_features_to_mvt (call, pyramid alone, finalize alone)",
    )
    if not _need(u, c, "m13", "gbx_st_asmvt_pyramid"):
        return u
    (n,) = _tail(c, "m13")[:1]
    feats = f"{c.q}.{PERF}feats_{n}"
    tgt = f"{c.q}.{PERF}mvt_tgt"
    u.setup.append(
        (
            f"create_feats_{_n(n)}",
            f"CREATE OR REPLACE TABLE {feats} (geom GEOMETRY(4326), attrs STRUCT<n: INT>)",
        )
    )
    u.setup.append(
        (
            f"fill_feats_{_n(n)}",
            f"INSERT INTO {feats} SELECT st_geomfromwkt(concat('POINT (', rand(1) * 340 - 170, ' ', rand(2) * 160 - 80, ')'), 4326), "
            f"named_struct('n', CAST(id AS INT)) FROM range({n})",
        )
    )
    u.drops += [
        ("drop_feats", f"DROP TABLE IF EXISTS {feats}"),
        ("drop_mvt_tgt", f"DROP TABLE IF EXISTS {tgt}"),
    ]
    pyramid = (
        f"SELECT p.z, p.x, p.y, p.mvt_bytes FROM {feats} s, "
        f"LATERAL {c.q}.gbx_st_asmvt_pyramid(st_asbinary(s.geom), to_json(s.attrs), 0, 10, 'layer', 4096) AS p"
    )
    # the CALL and the private finalize routine exist only where install(procedures=True) created them
    procs = "gbx_features_to_mvt" in _procedure_names()
    if procs:
        u.legs += [
            Leg("prep_call", f"DROP TABLE IF EXISTS {tgt}", timed=False),
            Leg(
                "call",
                f"CALL {c.q}.gbx_features_to_mvt(source_table => '{feats.replace(chr(96), '')}', "
                f"target_table => '{tgt.replace(chr(96), '')}', mvt_layer => 'layer', min_zoom => 0, max_zoom => 10)",
            ),
        ]
    else:
        u.skipped["call"] = (
            "needs the procedure gbx_features_to_mvt (install(procedures=True))"
        )
    u.legs.append(
        Leg("pyramid_only", f"SELECT count(*), sum(length(mvt_bytes)) FROM ({pyramid})")
    )
    if procs and "gbx__st_asmvt_features" in c.man.private:
        u.legs.append(
            Leg(
                "finalize_only",
                f"SELECT count(*), sum(length(m)) FROM (SELECT {c.q}.gbx__st_asmvt_features(collect_list(mvt_bytes), 'layer', 4096) AS m "
                f"FROM ({pyramid}) GROUP BY z, x, y)",
            )
        )
    else:
        u.skipped["finalize_only"] = (
            "needs the private gbx__st_asmvt_features that install(procedures=True) creates"
        )
    return u


_BUILDERS: Dict[str, Callable[[Ctx], Unit]] = {
    "m0": _m0,
    "m1": _m1,
    "m2": _m2,
    "m3": _m3,
    "m4": _m4,
    "m5": _m5,
    "m6": _m6,
    "m7": _m7,
    "m8": _m8,
    "m10": _m10,
    "m11": _m11,
    "m12": _m12,
    "m13": _m13,
}


# ---- the plan ------------------------------------------------------------------------------------------------------


def perf_plan(
    catalog: str,
    schema: str,
    *,
    jar_path: Optional[str] = None,
    wheel_path: Optional[str] = None,
    tile_dir: Optional[str] = None,
    surface: str = "w",
    sizes: Optional[Dict[str, Sequence[int]]] = None,
    only: Optional[Sequence[str]] = None,
) -> List[Unit]:
    """The measurements as units, in order (m9 is ``run_proc_warm``). ``surface`` is ``w`` (a warehouse) or ``s6``
    (Serverless env 6): it picks the legs that run there and m3's scale. ``only`` names units (``["m0", "m4"]``).
    """
    from databricks.labs.gbx import manifest, uc
    from databricks.labs.gbx.uc import ddl

    if surface not in SURFACES:
        raise ValueError(f"surface must be one of {SURFACES} (got {surface!r})")
    if wheel_path is None:
        wheel_path = f"/Volumes/<catalog>/<schema>/<volume>/{uc._env_wheel_name()}"
    ddl.quote_ident("catalog", catalog)
    ddl.quote_ident("schema", schema)
    merged = {**DEFAULT_SIZES, **{k: tuple(v) for k, v in (sizes or {}).items()}}
    wanted = [u.lower() for u in (only or UNITS)]
    unknown = [u for u in wanted if u not in UNITS]
    if unknown:
        raise ValueError(
            f"only= names no unit of this plan: {unknown} (m9 is run_proc_warm)"
        )
    c = Ctx(
        catalog,
        schema,
        f"{ddl.quote_ident('catalog', catalog)}.{ddl.quote_ident('schema', schema)}",
        "file://" + wheel_path,
        jar_path,
        tile_dir or f"{os.path.dirname(wheel_path)}/perf_tiles",
        merged,
        surface,
        manifest.load(),
        uc._public_version(),
    )
    units = []
    for uid in UNITS:
        if uid in wanted and uid in _BUILDERS:
            u = _BUILDERS[uid](c)
            u.legs = [leg for leg in u.legs if surface in leg.surfaces]
            units.append(u)
    return units


def flatten(units: Sequence[Unit]) -> List[Tuple[str, str]]:
    """``(cid, sql)`` pairs for ``gbx-uc-sql``: every unit's setup, then its legs, then all the DROPs at the end. A
    setup statement two units share (a tile table) is sent once."""
    out: List[Tuple[str, str]] = []
    seen = set()
    for u in units:
        for label, sql in u.setup:
            if sql not in seen:
                seen.add(sql)
                out.append((f"{u.id}:{label}", sql))
        out += [(f"{u.id}:{leg.label}", leg.sql) for leg in u.legs]
    drops = set()
    for u in units:
        for label, sql in u.drops:
            if sql not in drops:
                drops.add(sql)
                out.append((f"{u.id}:{label}", sql))
    return out


def uc_perf_sql(
    catalog: str,
    schema: str,
    jar_path: Optional[str],
    wheel_path: str,
    *,
    tile_dir: Optional[str] = None,
    sizes: Optional[Dict[str, Sequence[int]]] = None,
    only: Optional[Sequence[str]] = None,
) -> List[Tuple[str, str]]:
    """The warehouse statements of M0-M8 and M10-M13 as ``(cid, sql)`` for ``gbx-uc-sql ... --repeat 3``.

    ``wheel_path`` is the env 6 wheel on a Volume and ``jar_path`` the JAR with dependencies (None: no Scala leg).
    The tiles the statements read are written by ``perf_stage_tiles`` (a Serverless ``run_uc_perf`` stages them first, or
    call it on a cluster) into ``tile_dir`` (default ``<wheel folder>/perf_tiles``). The product functions the legs name
    must be installed in ``catalog.schema``; the file creates and drops the ``gbx_mvp_perf_*`` probes itself.
    """
    return flatten(
        perf_plan(
            catalog,
            schema,
            jar_path=jar_path,
            wheel_path=wheel_path,
            tile_dir=tile_dir,
            surface="w",
            sizes=sizes,
            only=only,
        )
    )


def skipped(units: Sequence[Unit]) -> Dict[str, Dict[str, str]]:
    """What the plan could not build, by unit: ``{unit: {leg or form: reason}}``."""
    return {u.id: dict(u.skipped) for u in units if u.skipped}


# ---- tiles on the Volume ----------------------------------------------------------------------------------------


def _side(mib: int) -> int:
    """The side of a square Float32 raster that decodes to ``mib`` MiB."""
    return math.isqrt(mib * 1024 * 1024 // 4)


def _write_gradient(path: str, side: int, bands: int = 1) -> int:
    """A ZSTD GeoTIFF of ``side`` x ``side`` x ``bands`` Float32 (a gradient, so the file is small and the decode is not),
    written a block row at a time; the file's size."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from rasterio.windows import Window

    block = min(512, side)
    with rasterio.open(
        path + ".part",
        "w",
        driver="GTiff",
        width=side,
        height=side,
        count=bands,
        dtype="float32",
        crs=TILE_CRS,
        transform=from_origin(*TILE_ORIGIN, 10.0, 10.0),
        compress="zstd",
        tiled=True,
        blockxsize=block,
        blockysize=block,
    ) as dst:
        cols = np.arange(side, dtype="float32")
        for row in range(0, side, block):
            h = min(block, side - row)
            rows = np.arange(row, row + h, dtype="float32")[:, None]
            for b in range(1, bands + 1):
                dst.write(
                    rows * 0.001 + cols[None, :] * 0.0001 + b,
                    b,
                    window=Window(0, row, side, h),
                )
    os.replace(path + ".part", path)
    return os.path.getsize(path)


def perf_stage_tiles(
    tile_dir: str, mibs: Sequence[int] = (REFERENCE_MIB, 64, *M10_MIB)
) -> Dict[str, dict]:
    """Write the GeoTIFFs the statements read into ``tile_dir``: ``perf_<n>MiB.tif`` (decoded size ``n`` MiB, one band),
    and ``perf_4band.tif`` (512 x 512 x 4). A file that is already there is kept. Returns ``{name: {path, bytes, side}}``.
    """
    os.makedirs(tile_dir, exist_ok=True)
    out = {}
    jobs = [(f"perf_{m}MiB.tif", _side(m), 1) for m in dict.fromkeys(mibs)] + [
        (BIG_KEY_TILE, 512, 4)
    ]
    for name, side, bands in jobs:
        path = f"{tile_dir}/{name}"
        size = (
            os.path.getsize(path)
            if os.path.exists(path)
            else _write_gradient(path, side, bands)
        )
        out[name] = {"path": path, "bytes": size, "side": side, "bands": bands}
    return out


# ---- the runners --------------------------------------------------------------------------------------------------


class _Run:
    """Timed steps with the notebook's progress lines: ``[T28 uc-perf] <unit>:<step> START (cap Ns)`` and ``DONE <s>s``,
    a RUNNING line every 60 s (the heartbeat), and the error of a step recorded, never raised: it is the measurement.
    """

    def __init__(self, tag: str, phase: str, total: int, cap_s: float):
        from databricks.labs.gbx.bench import liveness

        self.tag, self.total, self.cap_s, self.n = tag, total, cap_s, 0
        self.hb = liveness.Heartbeat(
            os.path.join(tempfile.gettempdir(), f"{phase}-heartbeat.json"),
            interval=30,
            phase=phase,
            print_every=60,
        ).start()

    def step(
        self, name: str, fn: Callable[[], object]
    ) -> Tuple[object, Optional[str], float]:
        self.hb.begin(name, self.n, self.total, cap_s=self.cap_s, set_name=self.tag)
        self.n += 1
        print(f"[{self.tag}] {name} START (cap {self.cap_s:g}s)", flush=True)
        t0 = time.time()
        val, err = None, None
        try:
            val = fn()
        except Exception as exc:  # noqa: BLE001 - the error is the measurement
            err = f"{type(exc).__name__}: {' '.join(str(exc).split())[:600]}"
        wall = time.time() - t0
        print(
            f"[{self.tag}] {name} DONE {wall:.1f}s {'ok' if err is None else 'ERROR ' + err[:160]}",
            flush=True,
        )
        self.hb.end(status=None if err is None else "error")
        return val, err, wall

    def stop(self) -> None:
        self.hb.stop()


def _first_cell(rows) -> Optional[str]:
    if not rows:
        return None
    r = rows[0]
    return str(r[0])[:200] if len(r) else None


def run_uc_perf(
    spark,
    catalog: str,
    schema: str,
    volume_dir: str,
    jar_path: Optional[str] = None,
    only: Optional[Sequence[str]] = None,
    sizes: Optional[Dict[str, Sequence[int]]] = None,
    repeats: int = 3,
    cap_s: float = 1800.0,
) -> dict:
    """Run the measurements M0-M8 and M10-M13 on Serverless env 6 (the Python/SQL statements of ``uc_perf_sql``, plus the
    session legs). ``volume_dir`` holds the one env 6 wheel; the tiles go to ``<volume_dir>/perf_tiles``. The product UC
    functions must be installed in ``catalog.schema``. ``jar_path`` (or the JAR in ``volume_dir``) adds the Scala legs.

    Each statement runs ``repeats`` times (a preparation once) and its wall times are recorded. Prints
    ``[T28 uc-perf] <unit>:<step> START (cap <s>)`` and ``DONE <s>s ok|ERROR ...`` per step, and a RUNNING line every 60 s
    through ``liveness.Heartbeat``. The probes are dropped at the end, also after an error.

    Returns ``{"wheel", "wheel_sha256", "jar", "tiles", "units": {id: {"title", "setup": {label: {...}}, "legs": {label:
    {"wall_s": [...], "value", "error"}}, "skipped"}}, "dropped"}``.
    """
    from databricks.labs.gbx import register, uc
    from databricks.labs.gbx.bench import probes

    wheel = uc._single_wheel(volume_dir)
    if jar_path is None:
        try:
            jar_path = uc._single_jar(volume_dir)
        except RuntimeError:
            jar_path = None
    tile_dir = f"{volume_dir.rstrip('/')}/perf_tiles"
    units = perf_plan(
        catalog,
        schema,
        jar_path=jar_path,
        wheel_path=wheel,
        tile_dir=tile_dir,
        surface="s6",
        sizes=sizes,
        only=only,
    )
    m10 = tuple((sizes or {}).get("m10", DEFAULT_SIZES["m10"]))
    # the tile staging, per unit its register step (when it has session functions), setup and leg runs, and the one
    # cleanup step that drops everything
    total = (
        1
        + sum(
            bool(u.session_fns)
            + len(u.setup)
            + sum(1 + (leg.timed and repeats > 1) * (repeats - 1) for leg in u.legs)
            for u in units
        )
        + 1
    )
    run = _Run(TAG, "uc-perf", total, cap_s)
    out = {
        "wheel": wheel,
        "wheel_sha256": probes._sha256(wheel),
        "jar": jar_path,
        "units": {},
        "dropped": [],
    }
    try:
        tiles, err, _ = run.step(
            "stage:tiles", lambda: perf_stage_tiles(tile_dir, (REFERENCE_MIB, 64, *m10))
        )
        out["tiles"] = tiles if err is None else err
        for u in units:
            rec = out["units"][u.id] = {
                "title": u.title,
                "setup": {},
                "legs": {},
                "skipped": dict(u.skipped),
            }
            if u.session_fns:
                run.step(
                    f"{u.id}:register",
                    lambda fns=u.session_fns: register(spark, only=list(fns)),
                )
            for label, sql in u.setup:
                _, err, wall = run.step(
                    f"{u.id}:{label}", lambda s=sql: spark.sql(s).collect()
                )
                rec["setup"][label] = {"wall_s": round(wall, 2), "error": err}
            for leg in u.legs:
                reps = repeats if leg.timed else 1
                walls, value, error = [], None, None
                for i in range(reps):
                    rows, err, wall = run.step(
                        f"{u.id}:{leg.label}" + (f"#{i + 1}" if reps > 1 else ""),
                        lambda s=leg.sql: spark.sql(s).collect(),
                    )
                    walls.append(round(wall, 2))
                    if err:
                        error = err
                        break
                    value = _first_cell(rows)
                rec["legs"][leg.label] = {
                    "wall_s": walls,
                    "value": value,
                    "error": error,
                }
    finally:

        def drop_all():
            for u in units:
                for label, sql in u.drops:
                    try:
                        spark.sql(sql)
                        out["dropped"].append(label)
                    except Exception as exc:  # noqa: BLE001 - keep dropping the rest
                        out["dropped"].append(f"{label}: ERROR {type(exc).__name__}")

        run.step("cleanup:drop", drop_all)
        run.stop()
    return out


def proc_warm_plan(
    catalog: str, schema: str, polygons: int = 1000
) -> Dict[str, List[Tuple[str, str]]]:
    """The statements of M9 by phase: ``setup``, ``call`` (the CALL, run cold, warm, and after an idle gap), ``create``
    (a target created from a ``WHERE false`` copy of a Python select vs from an explicit column list) and ``drops``.
    """
    from databricks.labs.gbx.uc import ddl

    q = f"{ddl.quote_ident('catalog', catalog)}.{ddl.quote_ident('schema', schema)}"
    src, tgt = f"{q}.{PERF}warm_src", f"{q}.{PERF}warm_tgt"
    old, new = f"{q}.{PERF}warm_old", f"{q}.{PERF}warm_new"
    side = 250
    wkt = (
        "concat('POLYGON((', 530000 + (id % 40) * 300, ' ', 180000 + (id DIV 40) * 300, ', ', "
        f"530000 + (id % 40) * 300 + {side}, ' ', 180000 + (id DIV 40) * 300, ', ', 530000 + (id % 40) * 300 + {side}, ' ', "
        f"180000 + (id DIV 40) * 300 + {side}, ', ', 530000 + (id % 40) * 300, ' ', 180000 + (id DIV 40) * 300 + {side}, ', ', "
        "530000 + (id % 40) * 300, ' ', 180000 + (id DIV 40) * 300, '))')"
    )
    call = (
        f"CALL {q}.gbx_polygons_to_bng(source_table => '{src.replace('`', '')}', "
        f"target_table => '{tgt.replace('`', '')}', bng_resolution => '100m')"
    )
    select = f"SELECT id, {q}.gbx_bng_kring('TQ3080', 1) AS ring FROM {src}"
    return {
        "setup": [
            (
                "create_src",
                f"CREATE OR REPLACE TABLE {src} AS SELECT CAST(id AS INT) AS id, st_geomfromwkt({wkt}, 27700) AS geom FROM range({polygons})",
            ),
        ],
        "call": [("drop_tgt", f"DROP TABLE IF EXISTS {tgt}"), ("call", call)],
        "create": [
            ("drop_old", f"DROP TABLE IF EXISTS {old}"),
            (
                "create_old",
                f"CREATE TABLE IF NOT EXISTS {old} AS SELECT * FROM ({select}) WHERE false",
            ),
            ("drop_new", f"DROP TABLE IF EXISTS {new}"),
            (
                "create_new",
                f"CREATE TABLE IF NOT EXISTS {new} (id INT, ring ARRAY<STRING>)",
            ),
        ],
        "drops": [
            (f"drop_{t.split('.')[-1]}", f"DROP TABLE IF EXISTS {t}")
            for t in (src, tgt, old, new)
        ],
    }


def run_proc_warm(
    spark,
    catalog: str,
    schema: str,
    volume_dir: Optional[str] = None,
    polygons: int = 1000,
    idle_s: float = 600.0,
    calls: int = 3,
    cap_s: float = 1800.0,
) -> dict:
    """M9: the same ``CALL gbx_polygons_to_bng`` ``calls`` times back to back, then once after ``idle_s`` seconds idle,
    and the two ways to create a target table (a ``WHERE false`` copy of a select that calls a Python function, vs an
    explicit column list), on ``polygons`` polygons. ``volume_dir`` makes the run install the functions and procedures
    first; without it they must be installed in ``catalog.schema``.

    Prints ``[T28 uc-perf] m9:<step> START (cap <s>)`` and ``DONE <s>s ...``, with a RUNNING line every 60 s (also while
    idle). Returns ``{"installed", "call_s": [..], "after_idle_s", "idle_s", "create_old_s", "create_new_s", "errors",
    "dropped"}``; the target is dropped before each CALL, so every CALL creates it.
    """
    from databricks.labs.gbx import uc

    plan = proc_warm_plan(catalog, schema, polygons)
    total = (
        1
        + len(plan["setup"])
        + 2 * (calls + 1)
        + 1
        + len(plan["create"])
        + len(plan["drops"])
    )
    run = _Run(TAG, "uc-perf-m9", total, cap_s)
    out: dict = {
        "installed": None,
        "call_s": [],
        "after_idle_s": None,
        "idle_s": idle_s,
        "errors": {},
        "dropped": [],
    }

    def timed(name, sql):
        val, err, wall = run.step(f"m9:{name}", lambda: spark.sql(sql).collect())
        if err:
            out["errors"][name] = err
        return wall

    try:
        if volume_dir is not None:
            stmts, err, _ = run.step(
                "m9:install",
                lambda: uc.install(
                    catalog, schema, volume_dir, spark=spark, procedures=True
                ),
            )
            out["installed"] = len(stmts) if err is None else err
        for label, sql in plan["setup"]:
            timed(label, sql)
        for i in range(calls):
            timed(f"drop_tgt_{i + 1}", plan["call"][0][1])
            out["call_s"].append(round(timed(f"call_{i + 1}", plan["call"][1][1]), 2))
        run.step("m9:idle", lambda: time.sleep(idle_s))
        timed("drop_tgt_idle", plan["call"][0][1])
        out["after_idle_s"] = round(timed("call_after_idle", plan["call"][1][1]), 2)
        walls = {label: timed(label, sql) for label, sql in plan["create"]}
        out["create_old_s"], out["create_new_s"] = round(walls["create_old"], 2), round(
            walls["create_new"], 2
        )
    finally:

        def drop_all():
            for label, sql in plan["drops"]:
                try:
                    spark.sql(sql)
                    out["dropped"].append(label)
                except Exception as exc:  # noqa: BLE001 - keep dropping the rest
                    err = f"{type(exc).__name__}: {' '.join(str(exc).split())[:300]}"
                    out["errors"][f"drop:{label}"] = err
                    out["dropped"].append(f"{label}: ERROR {type(exc).__name__}")

        run.step("m9:cleanup", drop_all)
        run.stop()
    return out


def write_uc_perf_notebooks(
    out_dir: str,
    wheel: str,
    catalog: str,
    schema: str,
    volume_dir: str,
    jar_path: Optional[str] = None,
    only: Optional[Sequence[str]] = None,
    idle_s: float = 600.0,
) -> Dict[str, str]:
    """Write the two Serverless notebooks, ``uc_perf.ipynb`` (``run_uc_perf``) and ``uc_proc_warm.ipynb``
    (``run_proc_warm``), in the form ``probes.write_notebook`` writes for the other probes (install the staged env wheel,
    restart, print the result). ``wheel`` is the staged env 6 wheel. Returns ``{"uc_perf": path, "proc_warm": path}``.
    """
    from databricks.labs.gbx.bench import probes

    os.makedirs(out_dir, exist_ok=True)
    perf = {"catalog": catalog, "schema": schema, "volume_dir": volume_dir}
    if jar_path:
        perf["jar_path"] = jar_path
    if only:
        perf["only"] = list(only)
    paths = {
        "uc_perf": os.path.join(out_dir, "uc_perf.ipynb"),
        "proc_warm": os.path.join(out_dir, "uc_proc_warm.ipynb"),
    }
    probes.write_notebook(paths["uc_perf"], "run_uc_perf", perf, wheel)
    probes.write_notebook(
        paths["proc_warm"],
        "run_proc_warm",
        {
            "catalog": catalog,
            "schema": schema,
            "volume_dir": volume_dir,
            "idle_s": idle_s,
        },
        wheel,
    )
    return paths
