"""The static, versioned level table: which implementation level serves each SQL name.

PREFERENCE lists the levels to try, best first; `select` takes the first one that is
available here and records one reason per level it had to skip.
"""

from __future__ import annotations

import importlib.util
import weakref
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from databricks.labs.gbx import manifest
from databricks.labs.gbx.manager import capabilities, messages
from databricks.labs.gbx.manager.capabilities import Capabilities

LEVEL_TABLE_VERSION = manifest.load().level_table_version  # one source: the manifest
# A function with no measured row below tries every level it has, highest first.
PREFERENCE: Dict[str, Tuple[str, ...]] = {
    "gbx_bng_eastnorthasbng": ("L1", "L2", "L3"),
    "gbx_rst_transformcrs": ("L1", "L2", "L3"),
    "gbx_st_makevalid": ("L3",),
    # The CRS functions have JVM companions that are not timed against light yet, so they take the highest
    # available level; the other vector scalars have no companion (the legacy decode is light by family).
    "gbx_st_crs": ("L1", "L3"),
    "gbx_st_setcrs": ("L1", "L3"),
    "gbx_st_transformcrs": ("L1", "L3"),
    "gbx_rst_retile": ("L3",),
    "gbx_bng_tessellate": ("L1", "L3"),
    "gbx_bng_tessellateexplode": ("L1", "L3"),
    # Quadbin cell math is at parity with light on a cluster (0.89x to 1.33x), so it stays on light; the other
    # grid scalars (custom grid, H3) are L1 first through their family. The quadbin arrays are light by family.
    "gbx_quadbin_pointascell": ("L3",),
    "gbx_quadbin_aswkb": ("L3",),
    "gbx_quadbin_centroid": ("L3",),
    "gbx_quadbin_resolution": ("L3",),
    "gbx_quadbin_distance": ("L3",),
    "gbx_quadbin_cellunion": ("L3",),
    "gbx_rst_merge_agg": ("L3",),
    "gbx_rst_bng_rasterize_agg": ("L3",),
    # The rasterize family is L1 first (gridfrompoints is 5x to 16x faster on the JVM on a cluster; binpoints has no
    # timing, so the highest level). Two rows are light: the terrain model is 18x faster on light, and rasterize is
    # 15x faster in the pure-core timing (no cluster number; default, unmeasured). Neither keeps a JVM level.
    "gbx_rst_rasterize": ("L3",),
    "gbx_rst_dtmfromgeoms": ("L3",),
    # The GDAL spec engine's rows: each level runs the engine where it can, its own implementation elsewhere.
    "gbx_rst_slope": ("L1", "L3"),
    "gbx_rst_mapalgebra": ("L1", "L3"),
    # MVP-IO's decode of a virtual tile: the JVM's (Catalyst) where there is a JAR, else the light UDF.
    "gbx_mvp_rst_materialize": ("L1", "L3"),
    # The file functions: cog_convert is 2.6x faster on the JVM on a cluster (0.38x); color_relief has only a pure-core
    # timing (default, unmeasured: highest available); viewshed is light (R-2 (c); its JVM level cannot take a WKT
    # observer behind the dispatch wrapper). buildoverviews (2.2x light faster) takes its family order.
    "gbx_rst_cog_convert": ("L1", "L3"),
    "gbx_rst_color_relief": ("L1", "L3"),
    "gbx_rst_viewshed": ("L3",),
    # A DataFrame helper, not a SQL name: its levels are L3 (all light), L1-steps (the viewshed and the
    # histogram are Catalyst steps) and JVM-DF (one Scala DataFrame step). The levels leg measures them.
    "rst_viewshed_towers": ("L3",),
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
    "raster-rasterize": ("L1", "L3"),
    "raster-array-op": ("L1", "L3"),
    "raster-aggregate": ("L3",),
    "raster-vectorize": ("L1", "L3"),
    "raster-generator": ("L3",),
    "raster-to-grid": ("L3",),
    "web-tiles": ("L3",),
    "raster-file-op": ("L3",),
}
TOWERS_LEVELS: Tuple[str, ...] = ("L3", "L1-steps", "JVM-DF")
# The Catalyst steps L1-steps calls by their private session names.
TOWERS_STEPS: Tuple[Tuple[str, str], ...] = (
    (
        "gbx__l1_rst_viewshed",
        "com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Viewshed",
    ),
    (
        "gbx__l1_rst_histogram",
        "com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Histogram",
    ),
    (
        "gbx__l1_rst_width",
        "com.databricks.labs.gbx.rasterx.expressions.accessors.RST_Width",
    ),
    (
        "gbx__l1_rst_height",
        "com.databricks.labs.gbx.rasterx.expressions.accessors.RST_Height",
    ),
)
L3_ORDER: Tuple[str, ...] = ("d", "c1", "c0")
NAMES = {"L1": "heavy (Catalyst)", "L2": "heavy (Java UDF)", "L3": "light"}
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


def no_connect_handler(fn) -> str:
    """The refusal for an L2 row with no Java handler (`connect_class`) on a standard-access-mode session."""
    return f"{fn.sql_name} has no Java handler for standard access mode"


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
    if level in ("L1", "L2"):
        why = _jvm_unavailable(fn, level, spec, caps)
        if why:
            return why
        l1_returns = spec.get("session_returns", fn.returns_session)
        if level == "L1" and l1_returns != fn.returns_session:
            return f"{fn.sql_name}: heavy returns {l1_returns}, the session signature returns {fn.returns_session}"
        if level == "L2" and caps.connect and "connect_class" not in spec:
            return no_connect_handler(fn)
        if level == "L2" and fn.kind != "scalar":
            return f"standard access mode serves scalars only ({fn.sql_name} is a {fn.kind})"
        if level == "L2" and fn.min_args != fn.max_args and "wrapper" not in spec:
            return f"{fn.sql_name}: optional arguments need a SQL wrapper, and this L2 row declares none"
        if (
            level == "L2"
            and fn.levels.get("L3", {}).get("factory_args") == "proj_grid_dirs"
        ):
            from databricks.labs.gbx.core import proj_grids

            if proj_grids.get_registered_dirs():  # a Java UDF has no channel for them
                return f"{fn.sql_name}: PROJ grids are registered and a Java UDF cannot read them"
        wrapper = fn.accepts_virtual_tile or (
            level == "L2" and ("bytes_param" in spec or "arg_types" in spec)
        )
        # A row with `arg_types` (or `bytes_param`) reaches its Java UDF only through a SQL wrapper that casts (or
        # unwraps) the arguments. Where SQL functions cannot be created there is no wrapper, and the bare Java UDF
        # fails its argument casts on some sessions: L2 is unavailable and `select` falls back to the next level,
        # naming this reason in the manager's one-line message. `jvm.install_l2` refuses the same way for a forced L2.
        if wrapper and not caps.sql_functions:
            return no_sql_functions(fn)
        return None
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
        or tuple(lv for lv in ("L1", "L2", "L3") if lv in fn.levels)
    )


def select(
    fn, caps: Capabilities, override: Optional[Tuple[str, str]] = None
) -> Choice:
    """First available level in ``order(fn)`` (or the testing-only override)."""
    if override is not None:
        return Choice(override[0], override[1], ())
    order_ = order(fn)
    if not any(lv in fn.levels for lv in order_):
        order_ = ("L3",)
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


def heavy_unavailable(caps: Capabilities) -> str:
    """Why this session cannot run a heavy step directly (a Catalyst companion, or a Scala DataFrame step through
    py4j); "" on a classic session with a matching JAR and loadable GDAL."""
    if caps.connect:
        return (
            caps.jar_reason
            if caps.serverless
            else "Spark Connect session (standard access mode)"
        )
    return caps.jar_reason if caps.jar is None else caps.gdal_reason


_READY: "weakref.WeakKeyDictionary" = (
    weakref.WeakKeyDictionary()
)  # session -> the heavy towers levels set up there


def _ready(spark) -> set:
    try:
        return _READY.setdefault(spark, set())
    except (
        TypeError
    ):  # a session that cannot be weakly referenced is checked every time
        return set()


def towers_level(spark, requested: Optional[str] = None) -> str:
    """The level `rst_viewshed_towers` runs at: `requested` (testing only), else the first level of its
    PREFERENCE row. A heavy level this session cannot serve says why, once, and the light one runs. A heavy
    level is checked and set up once per session, so a later call (a timed one) does none of that.
    """
    if requested is not None and requested not in TOWERS_LEVELS:
        raise ValueError(
            f"_levels must be one of {', '.join(TOWERS_LEVELS)}; got {requested!r}"
        )
    for level in (requested,) if requested else PREFERENCE["rst_viewshed_towers"]:
        if level == "L3":
            break
        if level in _ready(spark):
            return level
        why = heavy_unavailable(capabilities.detect(spark, need_jvm=True))
        if not why and level == "L1-steps":
            from databricks.labs.gbx.manager import jvm

            why = next(
                filter(
                    None,
                    (jvm.install_private_l1(spark, fqn, n) for n, fqn in TOWERS_STEPS),
                ),
                "",
            )
        if not why:
            _ready(spark).add(level)
            return level
        messages.say(spark, f"geobrix: heavy unavailable: {why}; using light")
    return "L3"
