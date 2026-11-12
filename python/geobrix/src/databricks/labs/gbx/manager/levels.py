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
    # transformcrs is 5.7x and 6.4x faster on the JVM at 4x and 16x rows (dedicated compute). crs and setcrs have no
    # JVM level: it took 0.77x and 0.59x (crs, at 1x and 4x) and 1.00x, 0.88x and 0.85x (setcrs, at 1x, 4x and 16x) of
    # the light time (Task 25), so their JVM expressions are deleted. The other vector scalars have no companion.
    "gbx_st_transformcrs": ("L1", "L3"),
    # The legacy Mosaic decode: the JVM form is 3.86x and 5.81x faster than light at 1x and 4x rows (Task 25, dedicated,
    # 1 + 4 x m5d.2xlarge, DBR 19, medians of 3, equal decoded output).
    "gbx_st_legacyaswkb": ("L1", "L3"),
    # tessellate and its explode: the JVM form takes 0.47x and 0.42x of the best light form at 4x rows and 0.46x and 0.45x
    # at 16x (dedicated, 1 + 4 x m5d.2xlarge, DBR 19). kring: 0.82x at 4x and 0.79x at 16x (3 iterations), that is 1.21x
    # and 1.27x faster, past the 1.2x line at both scales. cellfill: 0.71x and 0.73x (3 iterations). The grid arrays and
    # cell aggregates below are measured in the Task 25 pass; the others stay light by family.
    "gbx_bng_tessellate": ("L1", "L3"),
    "gbx_bng_tessellateexplode": ("L1", "L3"),
    "gbx_bng_kring": ("L1", "L3"),
    "gbx_bng_cellfill": ("L1", "L3"),
    # The Task 25 JVM pass (dedicated, 1 + 4 x m5d.2xlarge, DBR 19, medians of 3, equal decoded output), as how many times
    # faster the JVM form is than the best light form at 1x and 4x rows; each passes the 1.2x line.
    # The BNG arrays: geomkloop 5.44x / 5.01x, geomkring 4.66x / 5.97x, kloop 1.87x / 1.69x, polyfill 4.97x / 5.05x.
    "gbx_bng_geomkloop": ("L1", "L3"),
    "gbx_bng_geomkring": ("L1", "L3"),
    "gbx_bng_kloop": ("L1", "L3"),
    "gbx_bng_polyfill": ("L1", "L3"),
    # The custom-grid arrays: geomkloop 4.30x / 5.05x, geomkring 3.13x / 4.95x, kloop 5.20x / 7.45x, kring 4.60x / 8.12x,
    # polyfill 4.19x / 9.80x; the explodes: geomkloopexplode 4.26x / 5.02x, geomkringexplode 4.12x / 5.11x; the cell
    # fill aggregate 1.69x / 2.10x (Task 25).
    "gbx_custom_geomkloop": ("L1", "L3"),
    "gbx_custom_geomkring": ("L1", "L3"),
    "gbx_custom_kloop": ("L1", "L3"),
    "gbx_custom_kring": ("L1", "L3"),
    "gbx_custom_polyfill": ("L1", "L3"),
    "gbx_custom_geomkloopexplode": ("L1", "L3"),
    "gbx_custom_geomkringexplode": ("L1", "L3"),
    "gbx_custom_cellfill": ("L1", "L3"),
    # The five BNG explodes are 1.5x to 2.7x faster on the JVM on a cluster (0.37x to 0.67x). The quadbin and H3 explodes
    # are light by family (no JVM level); the custom ones are above.
    "gbx_bng_kringexplode": ("L1", "L3"),
    "gbx_bng_kloopexplode": ("L1", "L3"),
    "gbx_bng_geomkringexplode": ("L1", "L3"),
    "gbx_bng_geomkloopexplode": ("L1", "L3"),
    # The TIN generators: triangulate is 1.7x faster on the JVM (0.60x) and the bbox grid 0.75x (both within the 0.8
    # rule); the origin grid is 0.88x and the MVT pyramid is unmeasured (light).
    "gbx_st_triangulate": ("L1", "L3"),
    "gbx_st_interpolateelevationbbox": ("L1", "L3"),
    # Quadbin cell math is at parity with light on a cluster (0.89x to 1.33x), so it stays on light; the other
    # grid scalars (BNG, custom grid) are L1 first through their family. The quadbin arrays are light by family.
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
    # Catalyst level returns the session's BINARY payload and the deterministic largest value, as light does. The BNG
    # and custom-grid ones are above.
    "gbx_h3_cellfill": ("L1", "L3"),
    # The BNG rasterize aggregate: the JVM form took 0.40x of the light grouped aggregate with the cells as input and 0.50x
    # with the groups as input at 16M rows (dedicated). The other cell rasterize aggregates are faster on the JVM than the
    # best light form at 1x and 4x rows (Task 25): custom 4.36x / 4.50x, quadbin 2.46x / 2.41x, H3 1.10x / 2.24x. The
    # other raster aggregates stay light by family: merge has no JVM level.
    "gbx_rst_bng_rasterize_agg": ("L1", "L3"),
    "gbx_rst_custom_rasterize_agg": ("L1", "L3"),
    "gbx_rst_quadbin_rasterize_agg": ("L1", "L3"),
    "gbx_rst_h3_rasterize_agg": ("L1", "L3"),
    # The web tile runs on the JVM first. Against the light form that carries tiles as Arrow batches, its JVM form took
    # 1.25x less time at 4x tiles and 1.22x less at 16x (dedicated DBR 19, 5 iterations, equal output; it was 2.84x
    # against the pickled light form). Its dispatch wrapper (manifest L1 dispatch "bytes_to_jvm") sends only a tile with
    # bytes to the JVM; a virtual, NULL or error tile goes to light, which gives the transparent image for it.
    "gbx_rst_tilexyz": ("L1", "L3"),
    # The rasterize family is light: gridfrompoints measured 1.78x and 1.91x slower on the JVM than on light at 4x and 16x
    # rows (3 iterations, equal output), and binpoints 1.85x and 1.68x slower, so neither keeps a Catalyst expression.
    # The terrain model is 18x faster on light, and rasterize is 15x faster in the pure-core timing (no cluster number;
    # default, unmeasured). Neither keeps a JVM level.
    "gbx_rst_rasterize": ("L3",),
    "gbx_rst_dtmfromgeoms": ("L3",),
    # The GDAL spec engine's rows are light, with no JVM level. The engine's Catalyst level took 6.19 s against the light
    # engine's 5.90 s (slope) and 15.17 s against 13.16 s (mapalgebra) at 16x tiles (dedicated, DBR 19), nowhere near the
    # 1.2x line. For transformcrs it passed 1.2x at 16x only, and returned NULL error tiles under load where the light
    # forms returned none. The JVM expressions are deleted. The light level is the engine alone (no numpy form).
    "gbx_rst_slope": ("L3",),
    "gbx_rst_mapalgebra": ("L3",),
    # The DEM functions, measured against the best light form (c0 or c1) on dedicated compute (DBR 19), at 4x and 16x tiles
    # with equal output. TRI took 1.10x at 4x and 1.16x at 16x (5 iterations), under the 1.2x line, so its JVM expression is
    # deleted and it takes the family order, light, like aspect (1.19x, 1.17x), TPI (1.12x, 1.14x) and roughness
    # (1.03x, 1.05x). Hillshade has no JVM level.
    "gbx_rst_aspect": ("L3",),
    "gbx_rst_tpi": ("L3",),
    "gbx_rst_roughness": ("L3",),
    # The file functions: color_relief is light and has no JVM level. The JVM form read the colour table from a path, which a
    # UC Volume path cannot reach (the credential lives in the Python worker, so it raised FileNotFoundException on JAR
    # compute); it measured about 1.06x in a smoke run, nowhere near the 1.2x line.
    # cog_convert (its JVM level measured 1.25x and 1.46x slower than light at 4x and 16x rows, so it has none),
    # viewshed (its JVM level cannot take a WKT observer behind the dispatch wrapper) and buildoverviews (light measured
    # 2.2x faster, so it has no JVM level) take the family order, light.
    "gbx_rst_color_relief": ("L3",),
    # contour (0.10x at 4x rows) and isoband (0.39x and 0.42x at 4x and 16x rows) were faster on the JVM, and they pass the
    # 1.2x line, but a virtual tile at L1 raises VirtualTileException, and a virtual tile is the light default input. A SQL
    # wrapper that would send it to light cannot sit under explode (UNSUPPORTED_SQL_UDF_USAGE in Generate). Both JVM
    # expressions are deleted, so contour and isoband have the light level alone.
    "gbx_rst_contour": ("L3",),
    "gbx_rst_isoband": ("L3",),
    # The array merge: the JVM level refuses a group over the memory budget with an error tile (its merge holds the group),
    # where light streams the members through a VRT and gives the mosaic, so a hot key on a JAR cluster came back as a NULL
    # raster. On the dedicated finalize shape (`gbx_rst_merge(collect_list(tile))`, 250 rows) the JVM level is no faster:
    # 34.3 s against 37.5 s at 1x, 103.8 s against 62.5 s at 4x, 297 s against 310 s at 16x (none within the 0.8 rule).
    # The JVM expression is deleted, so merge has the light level alone.
    "gbx_rst_merge": ("L3",),
    # The seven combine functions have no JVM level: it ran at 0.93x and 0.67x the speed of the best lightweight SQL form
    # at 4x and 16x rows (131.56 s against 122.16 s, 576.44 s against 387.95 s), and the DataFrame `combine_by_key` form is
    # 2.2x faster than the JVM form at 16x (261.07 s). frombands, the array family's other JVM level, took 0.62x and
    # 0.57x of the light time at 1x and 4x (Task 25), so its JVM expression is deleted and the family is light.
    "gbx_rst_combineavg": ("L3",),
    "gbx_rst_combinecount": ("L3",),
    "gbx_rst_combinemax": ("L3",),
    "gbx_rst_combinemedian": ("L3",),
    "gbx_rst_combinemin": ("L3",),
    "gbx_rst_combinestddev": ("L3",),
    "gbx_rst_combinesum": ("L3",),
    # percentile_stretch has no JVM level: it took the nearest-rank percentile and rounded the rescale where light
    # interpolates and truncates (8 pixels of a test tile differed by up to 1.24 levels), it was never measured against
    # light, and a decimated large-band percentile cannot be matched to the level. It takes the family order, light.
    "gbx_rst_percentile_stretch": ("L3",),
    # filter has no JVM level: its mean filter of a gray + alpha tile left a different NoData mask at the empty window
    # than light, the cause was not traced, and it was never measured against light. It takes the family order, light.
    "gbx_rst_filter": ("L3",),
    # Two raster accessors that return one value are JVM first. Against the light form that moves tiles as Arrow batches
    # (dedicated, 1 + 4 x m5d.2xlarge, DBR 19, equal output), boundingbox took 1.27x less time on the JVM at 4x rows and
    # worldtorastercoordy 1.22x less at 16x rows. A value of this kind cannot sit under `explode`, so the SQL dispatch
    # wrapper that sends a virtual tile to light (the JVM raises `VirtualTileException` for one) is usable. Both levels
    # are bound by the scan of the tile, and 19 other one-value accessors took 1.05x to just under 1.2x less time on the
    # JVM at 4x and 16x rows, under the 1.2x line at both: crs, format, height, memsize, numbands, pixelheight, pixelwidth,
    # rotation, scalex, scaley, skewx, skewy, srid, upperleftx, upperlefty, width, rastertoworldcoordx,
    # rastertoworldcoordy and worldtorastercoordx. Their JVM expressions are deleted and they take the family order,
    # light. Accessors that return an `ARRAY`, a `MAP` or a `STRUCT` can sit under `explode` and `inline`, where that
    # wrapper is refused (UNSUPPORTED_SQL_UDF_USAGE), so they are light only and keep no JVM level.
    "gbx_rst_boundingbox": ("L1", "L3"),
    "gbx_rst_worldtorastercoordy": ("L1", "L3"),
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
    "raster-array-op": ("L3",),
    "raster-aggregate": ("L3",),
    "raster-vectorize": ("L1", "L3"),
    # the raster tessellates are light: the BNG one's JVM form ran at 1.03x and 0.97x the speed of light at 4x and 16x rows
    # (410.74 s against 422.05 s, 1591.42 s against 1540.71 s), so it has no JVM level; the others are light by family (unmeasured)
    "raster-generator": ("L3",),
    "raster-to-grid": ("L3",),
    "web-tiles": ("L3",),
    "raster-file-op": ("L3",),
    "general": ("L3",),
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
# The light form each engine row measured fastest, where it is not the default (the batch form behind the wrapper).
# Mapalgebra: the row-wise form took 13.16 s against 13.48 s for the batch form at 16x tiles (dedicated, DBR 19).
# Slope (5.90 s batch against 6.07 s row-wise) and transformcrs (75.6 s batch against 104.0 s row-wise at 4x) take the
# default.
# The DEM functions, from the 16x cluster rows (dedicated, DBR 19, medians of 3, best of c0 and c1): TRI and roughness take
# the row-wise form (c0); aspect and TPI take the batch form (c1, the default, so no entry). Hillshade keeps its local
# pick (c0: 3.4 s against 4.9 s, 1024 px tiles, 384 tiles).
# Color relief took the row-wise form in the local pass (1024 px tiles, a four-stop ramp, median of 3 runs, c0 against c1
# at 24, 96 and 384 tiles): 0.4 / 1.4 / 5.8 s against 1.5 / 2.4 / 7.3 s, 1.26x at 384.
# The band-math functions took the row-wise form in the local pass (512 px four-band tiles, median of 3 runs, 384 tiles,
# c0 against c1): NDVI 3.5 s against 5.2, NDWI 3.4 against 4.7, NBR 3.8 against 5.4, SAVI 4.2 against 5.7, EVI 3.9
# against 5.0, rst_index 3.6 against 5.0, threshold 7.5 against 9.1.
# Contour returns an array of lines, which has no batch form: every engine form registers the row-wise UDF.
# The warp functions took the row-wise form in the local pass (1024 px Float32 tiles in EPSG:4326 with NoData holes, median
# of 3 runs, 384 tiles, c0 against c1): transform 5.7 s against 8.3, web mercator 6.8 against 9.1, resample 5.7 against 6.0,
# resample_to_size 5.2 against 6.4, resample_to_res 4.7 against 6.8.
# The fill took the row-wise form in the local pass (1024 px Float32 tiles in EPSG:4326 with NoData holes, median of 3 runs,
# three passes, c0 against c1): 0.5 to 1.2 s against 1.0 to 2.0 s at 24 tiles, 1.8 to 2.3 s against 2.8 to 3.0 s at 96, and
# 6.4 to 9.2 s against 7.9 to 12.6 s at 384 (c1 was the faster in one of the three passes at 384, by 7 percent).
# The summary returns a string, which has no batch form: every engine form registers the row-wise UDF.
# Polygonize is a table function, which has one engine form per Python UDTF kind: the row UDTF (c0) and the Arrow-optimized UDTF
# (c1). They took the same time in the local pass (1024 px Byte tiles of six classes, median of 3 runs, 24 / 96 / 384 tiles):
# 1.1 / 3.4 / 12.6 s against 1.1 / 3.3 / 12.3 s (and 1.3 / 3.8 / 14.5 against 1.3 / 3.8 / 13.8 in the pass before). The row
# UDTF is the default: it needs no Arrow capability. The rasterio UDTF it replaced took 2.1 / 6.9 / 25.7 s (2.0x).
# The viewshed took the row-wise form in the local pass (1024 px DEM tiles, an observer at the centre of each, median of 3 runs,
# two passes, c0 against c1 against a floor): 0.5 / 1.5 to 2.0 / 5.9 to 6.1 s against 0.6 to 1.3 / 2.0 to 2.3 / 6.0 to 6.1 s at 24 /
# 96 / 384 tiles. The two meet the floor at 384 (the call itself costs about as much as reading the tile); c0 is faster at the
# smaller rungs and needs no wrapper. The batch wrapper also types the geometry by its first session type (BINARY).
# The generic GDAL command (`gbx_rst_gdal`) is unmeasured: it takes the row-wise form, which needs no wrapper and no Arrow capability.
ENGINE_L3_FORM: Dict[str, str] = {
    "gbx_rst_mapalgebra": "engine-c0",
    **{
        f"gbx_rst_{n}": "engine-c0"
        for n in (
            "hillshade",
            "tri",
            "roughness",
            "ndvi",
            "ndwi",
            "nbr",
            "savi",
            "evi",
            "index",
            "threshold",
            "color_relief",
            "contour",
            "transform",
            "to_webmercator",
            "resample",
            "resample_to_size",
            "resample_to_res",
            "fillnodata",
            "summary",
            "polygonize",
            "viewshed",
            "gdal",
        )
    },
}


def engine_form(fn) -> str:
    """The variant of the engine's light level that serves `fn` when no form is forced."""
    return ENGINE_L3_FORM.get(fn.sql_name, ENGINE)


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

    The engine is a light level (L3): it needs this process's ``osgeo`` to be rasterio's GDAL at the pin
    (``engine.run.unavailable_reason``). It has no JVM level.
    """
    spec = fn.levels.get(level) or {}
    if fn.gdal is None or not spec.get("engine"):
        return f"{fn.sql_name} has no GDAL spec engine form at {level}"
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


# An L3 spec with one of these keys has a light form of its own; without one the engine is its only light form.
# (`file_ref: "engine"` is the engine's own FILE-aware form, not another light form.)
_L3_OTHER_FORMS = frozenset({"variants", "factory", "callable", "udtf", "file_ref"})
ENGINE_FILE_REF = "engine"


def engine_only(fn, level: str) -> bool:
    """True when the GDAL spec engine is the only form `fn` has at the light level."""
    spec = fn.levels.get(level) or {}
    other = _L3_OTHER_FORMS & set(spec)
    if spec.get("file_ref") == ENGINE_FILE_REF:
        other = other - {"file_ref"}
    return level == "L3" and bool(spec.get("engine")) and not other


def available(fn, level: str, caps: Capabilities) -> Optional[str]:
    """None when `level` can serve `fn` with its full session signature here; else the reason."""
    spec = fn.levels.get(level)
    if spec is None:
        return f"{fn.sql_name} has no {level} implementation"
    if spec.get("engine") and engine_unavailable(fn, level, caps) is None:
        return None
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
        if engine_only(fn, level):
            return engine_unavailable(
                fn, level, caps
            )  # the engine is this row's only light form
        needs = spec.get("needs", ())
        if (
            "osgeo" in needs
        ):  # a row that calls the engine inside its own UDF (rst_chm): the engine's own reason
            from databricks.labs.gbx.engine import run

            why = run.unavailable_reason()
            if why:
                return why
        missing = [
            m for m in needs if m != "osgeo" and importlib.util.find_spec(m) is None
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
        if override == ("L3", "-") and engine_only(fn, "L3"):
            # a pin to the light level of a row whose only light form is the engine: that form, or why not
            why = engine_unavailable(fn, "L3", caps)
            if why:
                raise NoLevelAvailable(
                    fn.sql_name, (f"geobrix: {NAMES['L3']} unavailable: {why}",)
                )
            return Choice("L3", ENGINE, ())
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
