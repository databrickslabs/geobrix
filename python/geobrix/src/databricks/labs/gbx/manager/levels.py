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
# L0 (a SQL kernel the session installs as a temporary SQL function) is never a default: a name takes it only where a
# row here lists it first, after a measurement; a function whose only level is L0 is served by it.
PREFERENCE: Dict[str, Tuple[str, ...]] = {
    # The SQL kernel (L0) runs in the JVM with no Python and needs no JAR: over 1M rows on a local[4] session it takes
    # 0.28 s against 2.43 s for light (L3). That is the only measurement of L0 against light (local, not on a cluster).
    # The Catalyst level (L1) of this function is gone: on dedicated compute it measured 1.17x and 1.35x slower than
    # the light batch form at 4M and 16M rows. There is no Java UDF level (L2) any more.
    "gbx_bng_eastnorthasbng": ("L0", "L3"),
    # cellarea: L0 0.19 s, L1 0.19 s, light 0.76 s over 1M rows locally (same session).
    "gbx_bng_cellarea": ("L1", "L0", "L3"),
    # transformcrs: the JVM level is 1.6x slower than light (47.7 s against 29.7 s over 1000 tiles) and the Java UDF
    # is 1.67x slower than the engine, so it is light. Its bespoke JVM expression and Java UDF are gone; the row's L1
    # block is the GDAL spec engine's alone, and that switch records its own order here.
    "gbx_rst_transformcrs": ("L3",),
    # The CRS functions have JVM companions that are not timed against light yet, so they take the highest
    # available level; the other vector scalars have no companion (the legacy decode is light by family).
    "gbx_st_crs": ("L1", "L3"),
    "gbx_st_setcrs": ("L1", "L3"),
    "gbx_st_transformcrs": ("L1", "L3"),
    # tessellate and its explode: the JVM form takes 0.47x and 0.42x of the best light form at 4x rows and 0.46x and 0.45x
    # at 16x (dedicated, 1 + 4 x m5d.2xlarge, DBR 19). kring: 0.82x at 4x and 0.79x at 16x (3 iterations), that is 1.21x
    # and 1.27x faster, past the 1.2x line at both scales. cellfill: 0.71x and 0.73x (3 iterations). The other grid arrays and
    # the cell aggregates stay light by family.
    "gbx_bng_tessellate": ("L1", "L3"),
    "gbx_bng_tessellateexplode": ("L1", "L3"),
    "gbx_bng_kring": ("L1", "L3"),
    "gbx_bng_cellfill": ("L1", "L3"),
    # The five BNG explodes are 1.5x to 2.7x faster on the JVM on a cluster (0.37x to 0.67x). The other explodes
    # (quadbin, custom, H3) are light by family: unmeasured, and the quadbin companions are not conformed.
    "gbx_bng_kringexplode": ("L1", "L3"),
    "gbx_bng_kloopexplode": ("L1", "L3"),
    "gbx_bng_geomkringexplode": ("L1", "L3"),
    "gbx_bng_geomkloopexplode": ("L1", "L3"),
    # The TIN generators: triangulate is 1.7x faster on the JVM (0.60x) and the bbox grid 0.75x (both within the 0.8
    # rule); the origin grid is 0.88x and the MVT pyramid is unmeasured (light).
    "gbx_st_triangulate": ("L1", "L3"),
    "gbx_st_interpolateelevationbbox": ("L1", "L3"),
    # Quadbin cell math is at parity with light on a cluster (0.89x to 1.33x), so it stays on light; the other
    # grid scalars (custom grid, H3) are L1 first through their family. The quadbin arrays are light by family.
    "gbx_quadbin_pointascell": ("L3",),
    "gbx_quadbin_aswkb": ("L3",),
    "gbx_quadbin_centroid": ("L3",),
    "gbx_quadbin_resolution": ("L3",),
    # distance has an SQL kernel: L0 0.30 s against light 1.30 s over 1M rows on a local[4] session.
    "gbx_quadbin_distance": ("L0", "L3"),
    # The rings: over 1M cells with k = 1 locally, kring L0 2.23 s against light 5.15 s, kloop 2.78 s against 4.86 s.
    "gbx_quadbin_kring": ("L0", "L3"),
    "gbx_quadbin_kloop": ("L0", "L3"),
    "gbx_quadbin_cellunion": ("L3",),
    # The cell-fill aggregates are light by family, except H3's: 0.57x on a cluster (inside the 0.8 rule), and its
    # Catalyst level returns the session's BINARY payload and the deterministic largest value, as light does.
    "gbx_h3_cellfill": ("L1", "L3"),
    # The BNG rasterize aggregate: the JVM form took 0.40x of the light grouped aggregate with the cells as input and 0.50x
    # with the groups as input at 16M rows (dedicated). The raster aggregates stay light by family: merge has no JVM level.
    "gbx_rst_bng_rasterize_agg": ("L1", "L3"),
    # The rasterize family is light: gridfrompoints measured 1.78x and 1.91x slower on the JVM than on light at 4x and 16x
    # rows (3 iterations, equal output), and binpoints 1.85x and 1.68x slower, so neither keeps a Catalyst expression.
    # The terrain model is 18x faster on light, and rasterize is 15x faster in the pure-core timing (no cluster number;
    # default, unmeasured). Neither keeps a JVM level.
    "gbx_rst_rasterize": ("L3",),
    "gbx_rst_dtmfromgeoms": ("L3",),
    # The GDAL spec engine's rows: each level runs the engine where it can, its own implementation elsewhere.
    "gbx_rst_slope": ("L1", "L3"),
    "gbx_rst_mapalgebra": ("L1", "L3"),
    # The file functions: color_relief has only a pure-core timing (default, unmeasured: highest available).
    # cog_convert (its JVM level measured 1.25x and 1.46x slower than light at 4x and 16x rows, so it has none),
    # viewshed (its JVM level cannot take a WKT observer behind the dispatch wrapper) and buildoverviews (light measured
    # 2.2x faster, so it has no JVM level) take the family order, light.
    "gbx_rst_color_relief": ("L1", "L3"),
    # contour (0.10x at 4x rows) and isoband (0.39x and 0.42x at 4x and 16x rows) are faster on the JVM, and they pass the
    # 1.2x line, but a virtual tile at L1 raises VirtualTileException, and a virtual tile is the light default input. A SQL
    # wrapper that would send it to light cannot sit under explode (UNSUPPORTED_SQL_UDF_USAGE in Generate). Light first until
    # the JVM level reads a virtual tile; the L1 blocks stay for a forced L1.
    "gbx_rst_contour": ("L3",),
    "gbx_rst_isoband": ("L3",),
    # The array merge: the JVM level refuses a group over the memory budget with an error tile (its merge holds the group),
    # where light streams the members through a VRT and gives the mosaic, so a hot key on a JAR cluster came back as a NULL
    # raster. On the dedicated finalize shape (`gbx_rst_merge(collect_list(tile))`, 250 rows) the JVM level is no faster:
    # 34.3 s against 37.5 s at 1x, 103.8 s against 62.5 s at 4x, 297 s against 310 s at 16x (none within the 0.8 rule).
    # Light first; the JVM block stays for a forced L1.
    "gbx_rst_merge": ("L3",),
    # The seven combine functions have no JVM level: it ran at 0.93x and 0.67x the speed of the best lightweight SQL form
    # at 4x and 16x rows (131.56 s against 122.16 s, 576.44 s against 387.95 s), and the DataFrame `combine_by_key` form is
    # 2.2x faster than the JVM form at 16x (261.07 s). frombands, the array family's other JVM level, is not measured
    # yet and takes the family order.
    "gbx_rst_combineavg": ("L3",),
    "gbx_rst_combinecount": ("L3",),
    "gbx_rst_combinemax": ("L3",),
    "gbx_rst_combinemedian": ("L3",),
    "gbx_rst_combinemin": ("L3",),
    "gbx_rst_combinestddev": ("L3",),
    "gbx_rst_combinesum": ("L3",),
    # The accessors that return one value (a number, a string or a polygon's WKB) are JVM first: reading a header is
    # 0.26x (boundingbox) and 0.18x (width) of the light time at 4x rows, and 0.24x for both at 16x (dedicated,
    # 1 + 4 x m5d.2xlarge, DBR 19, equal output). A value of this kind cannot sit under `explode`, so the SQL dispatch
    # wrapper that sends a virtual tile to light (the JVM raises `VirtualTileException` for one) is usable. Accessors that
    # return an `ARRAY`, a `MAP` or a `STRUCT` can sit under `explode` and `inline`, where that wrapper is refused
    # (UNSUPPORTED_SQL_UDF_USAGE), so they are light only and keep no JVM level; they take the family order.
    **{
        f"gbx_rst_{n}": ("L1", "L3")
        for n in (
            "boundingbox",
            "crs",
            "format",
            "height",
            "memsize",
            "numbands",
            "pixelheight",
            "pixelwidth",
            "rastertoworldcoordx",
            "rastertoworldcoordy",
            "rotation",
            "scalex",
            "scaley",
            "skewx",
            "skewy",
            "srid",
            "upperleftx",
            "upperlefty",
            "width",
            "worldtorastercoordx",
            "worldtorastercoordy",
        )
    },
}
# The order a function with no PREFERENCE row takes, by its manifest family (measured on-cluster where a record or
# the benchmark tables have the family; the spec's "highest available" where not). Per-name exceptions are
# PREFERENCE rows. L3 is always there: every function has a light level.
FAMILY_PREFERENCE: Dict[str, Tuple[str, ...]] = {
    "grid-scalar": ("L1", "L3"),
    "grid-array": ("L3",),
    "grid-generator": ("L3",),
    "cell-aggregate": ("L3",),
    "vector-scalar": ("L3",),
    "vector-aggregate": ("L3",),
    "vector-generator": ("L3",),
    "raster-accessor": ("L3",),
    "raster-stat": ("L3",),
    "raster-tile-op": ("L3",),
    "raster-constructor": ("L1", "L3"),
    "raster-rasterize": ("L3",),
    "raster-array-op": ("L1", "L3"),
    "raster-aggregate": ("L3",),
    "raster-vectorize": ("L1", "L3"),
    # the raster tessellates are light: the BNG one's JVM form ran at 1.03x and 0.97x the speed of light at 4x and 16x rows
    # (410.74 s against 422.05 s, 1591.42 s against 1540.71 s), so it has no JVM level; the others are light by family (unmeasured)
    "raster-generator": ("L3",),
    "raster-to-grid": ("L3",),
    "web-tiles": ("L3",),
    "raster-file-op": ("L3",),
}
L3_ORDER: Tuple[str, ...] = ("d", "c1", "c0")
NAMES = {
    "L0": "SQL",
    "L1": "heavy (Catalyst)",
    "L3": "light",
}
# The variant of a level that runs the row's `gdal` block on the GDAL spec engine. A forced L3 may name the
# engine's row-wise UDF alone ("engine-c0") or its batch UDF behind the wrapper ("engine-c1", the default).
ENGINE = "engine"
ENGINE_L3 = ("engine", "engine-c0", "engine-c1")


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


def engine_unavailable(fn, level: str, caps: Capabilities) -> Optional[str]:
    """None when `level` can run `fn`'s `gdal` block on the GDAL spec engine here; else why not.

    L1 needs the JVM's GDAL at the manifest's pin (the release ``KernelGdal.probe`` reports); L3 needs this
    process's ``osgeo`` to be rasterio's GDAL at the pin (``engine.run.unavailable_reason``).
    """
    spec = fn.levels.get(level) or {}
    if fn.gdal is None or not spec.get("engine"):
        return f"{fn.sql_name} has no GDAL spec engine form at {level}"
    if level == "L1":
        why = _jvm_unavailable(fn, level, spec, caps)
        if why:
            return why
        if fn.accepts_virtual_tile and not caps.sql_functions:
            return f"{fn.sql_name} needs a SQL function wrapper, and SQL functions are unavailable here"
        if fn.accepts_virtual_tile:
            # the wrapper sends virtual tiles to the light engine (its c0 UDF), which must run in this process
            from databricks.labs.gbx.engine import run

            why = run.unavailable_reason()
            if why:
                return f"{fn.sql_name} sends virtual tiles to the light GDAL spec engine, which cannot run here ({why})"
        pin = manifest.load().gdal.pin
        if caps.jvm_gdal != pin:
            return f"the JVM's GDAL is {caps.jvm_gdal or 'unknown'}; the GDAL spec engine needs {pin}"
        # Registered PROJ grids reach the engine: every GDALManager.init puts a configuration's grid directories that
        # are not yet on PROJ's search path there, also after an earlier init (the capability probe's) without them.
        return None
    if level == "L3":
        from databricks.labs.gbx.engine import run

        return run.unavailable_reason()
    return f"the GDAL spec engine does not serve {level}"


def _jvm_unavailable(fn, level: str, spec, caps: Capabilities) -> Optional[str]:
    """Why a JVM level cannot run at all here (Connect for L1, no JAR, no GDAL in the JVM); None when it can."""
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
    return None


def no_sql_functions(fn) -> str:
    """The refusal for a row whose level is a SQL wrapper on a session that cannot create SQL functions."""
    return f"{fn.sql_name} needs a SQL function wrapper, and SQL functions are unavailable here"


def available(fn, level: str, caps: Capabilities) -> Optional[str]:
    """None when `level` can serve `fn` with its full session signature here; else the reason."""
    spec = fn.levels.get(level)
    if spec is None:
        return f"{fn.sql_name} has no {level} implementation"
    if spec.get("engine") and engine_unavailable(fn, level, caps) is None:
        return None
    if level == "L1" and "companion" not in spec:  # the engine form is the only one
        return engine_unavailable(fn, level, caps)
    if level == "L1":
        why = _jvm_unavailable(fn, level, spec, caps)
        if why:
            return why
        l1_returns = spec.get("session_returns", fn.returns_session)
        if l1_returns != fn.returns_session:
            return f"{fn.sql_name}: heavy returns {l1_returns}, the session signature returns {fn.returns_session}"
        # A tile function reaches its Catalyst companion only through a SQL dispatch wrapper. Where SQL functions
        # cannot be created there is no wrapper: L1 is unavailable and `select` falls back to the next level,
        # naming this reason in the manager's one-line message.
        if fn.accepts_virtual_tile and not caps.sql_functions:
            return no_sql_functions(fn)
        return None
    if level == "L0":  # a temporary SQL function: nothing to import, no JVM
        return None if caps.sql_functions else no_sql_functions(fn)
    if level == "L3":
        missing = [
            m for m in spec.get("needs", ()) if importlib.util.find_spec(m) is None
        ]
        return f"{fn.sql_name} needs {', '.join(missing)}" if missing else None
    return f"{level} is not a session level"


def order(fn) -> Tuple[str, ...]:
    """The levels to try for ``fn``, best first: its PREFERENCE row, else its family's, else every level it has,
    highest first. A level the row does not have is skipped by ``select``; a family order that names none of the
    row's levels falls through to L3 there (every row has L3)."""
    return (
        PREFERENCE.get(fn.sql_name)
        or FAMILY_PREFERENCE.get(fn.family)
        or tuple(lv for lv in ("L1", "L3") if lv in fn.levels)
    )


def select(
    fn,
    caps: Capabilities,
    override: Optional[Tuple[str, str]] = None,
    prefer: Optional[str] = None,
) -> Choice:
    """First available level in ``order(fn)`` (or the testing-only override). ``prefer`` puts one level first when
    ``fn`` has a row there; unlike ``override`` it is not forced, so an unavailable level falls back to the next.
    """
    if override is not None:
        return Choice(override[0], override[1], ())
    order_ = order(fn)
    if prefer in fn.levels:
        order_ = (prefer,) + tuple(lv for lv in order_ if lv != prefer)
    if not any(lv in fn.levels for lv in order_):
        order_ = (
            ("L3",)
            if "L3" in fn.levels
            else tuple(lv for lv in ("L0", "L1") if lv in fn.levels)
        )
    reasons = []
    for level in order_:
        if level not in fn.levels:
            continue
        why = available(fn, level, caps)
        if why is None:
            on_engine = fn.levels[level].get("engine") and not engine_unavailable(
                fn, level, caps
            )
            return Choice(level, ENGINE if on_engine else "-", tuple(reasons))
        reasons.append(f"geobrix: {NAMES[level]} unavailable: {why}")
    raise NoLevelAvailable(fn.sql_name, tuple(reasons))
