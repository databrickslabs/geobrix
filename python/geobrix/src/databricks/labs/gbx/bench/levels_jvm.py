"""JV: the functions whose JVM level's fate is not yet measured, each at its JVM level against its light forms.

The keep rule (dedicated DBR 19): a JVM level (L1) stays, and is the default, only where it is at least 1.2x faster than the
best light form (c0 or c1, `l3` where the light level has no variants) at 4x OR 16x rows, with equal decoded output. The
set holds every function with an L1 block that has no such record: the ones the level order never picks, and the ones it
picks first without a dedicated 4x and 16x measurement. TRI is in the `DEM` set, not here.

Each function gets, over ONE input every one of its forms reads:

- `l1@<tag>`: the JVM level (forced, dedicated only);
- one form per light variant of the manifest row (`c0@<tag>`, `c1@<tag>`; `l3@<tag>` for a row with no variants);
- `floor@<tag>` last: the shell without the work (a constant Arrow UDF of the output's type, the input tile back, or a scan).

Every non-floor form carries a value fingerprint (`vsql`: the decoded pixels, the normalised geometry, the rounded doubles,
the sorted arrays) and a NULL report (`nullsql`), so the levels are compared by what they return, and an error tile under
load is a counted, named NULL, not a fast wrong row. A run picks one function with `--levels-forms <fn>:<variant>,...`
(`levels_forms.select_forms`), and the local pass with the `JV-<name>` sets of `bench.local_timing`.
"""

from __future__ import annotations

import dataclasses
from typing import Dict, List, Optional, Tuple

from databricks.labs.gbx.bench.levels import Form

MVP = "JV"
_ME = "databricks.labs.gbx.bench.levels_jvm:"
_M = "2147483647"  # the modulus of every fingerprint sum (`value_fp._MOD`)

# --- the shapes ------------------------------------------------------------------------------------------------------
# Each answers one row (n, fp, n_out). `{call}` is the function call; `{{src}}` the input table.
_SCALAR = (
    "SELECT count(*) AS n, sum(pmod(xxhash64(c), " + _M + ")) AS fp, count(c) AS n_out "
    "FROM (SELECT {call} AS c FROM {{src}})"
)
# a tile output: an error tile (NULL raster) counts as 0 bytes, so a form whose every output failed still ends with its row
_TILE = (
    "SELECT count(*) AS n, sum(coalesce(length(t.raster), 0)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {call} AS t FROM {{src}})"
)
# an aggregate over `key`: n is the input rows (a scalar subquery: a grouped pandas UDF cannot sit beside count(*))
_AGG = (
    "SELECT (SELECT count(*) FROM {{src}}) AS n, sum(coalesce(length(x), 0)) AS fp, "
    "CASE WHEN count(x) = count(*) THEN (SELECT count(*) FROM {{src}}) ELSE count(x) END AS n_out "
    "FROM (SELECT {call} AS x FROM {{src}} GROUP BY key)"
)
# a generator: n is the rows it was given, fp the rows it made (`{call}` names the input row `s`)
_GEN = (
    "SELECT (SELECT count(*) FROM {{src}}) AS n, count(*) AS fp, (SELECT count(*) FROM {{src}}) AS n_out "
    "FROM {{src}} s, LATERAL {call} t"
)
_SHAPES = {"scalar": _SCALAR, "tile": _TILE, "agg": _AGG, "gen": _GEN}

# --- the value fingerprints ------------------------------------------------------------------------------------------
# `{v}` is the expression hashed per output (`c` a scalar, `t` a tile, `x` an aggregate, `t` a generator row).
_VALUE = {
    "hash": "xxhash64({o})",
    "round": "xxhash64(round({o}, 6))",
    "sorted": "xxhash64(array_sort({o}))",
    "wkb": "gbx_bench_vfp_wkb({o})",
    # an EWKB: the normalised geometry and the four SRID bytes after the type word
    "ewkb": "xxhash64(gbx_bench_vfp_wkb({o}), substring({o}, 6, 4))",
    "bbox": "xxhash64(round({o}.xmin, 6), round({o}.ymin, 6), round({o}.xmax, 6), round({o}.ymax, 6))",
    "chip": "xxhash64({o}.cellid, {o}.core, gbx_bench_vfp_wkb({o}.chip))",
    "raster": "gbx_bench_vfp_raster({o}.raster)",
    "png": "gbx_bench_vfp_raster({o})",
    "pmtiles": "gbx_bench_vfp_pmtiles({o})",
}
# the inputs that replicate a few distinct rows: a decode is done once per distinct input row, not per replica
_REPLICATED = {
    "dem_1024_mem": "xxhash64(tile.raster)",
    "se_pair_512_mem": "xxhash64(tiles[0].raster, tiles[1].raster)",
    "jv_nc_512_mem": "xxhash64(tile.raster)",
}
# the few corpus tiles a decode reads: those whose path hashes to 0 mod 8 (every form reads the same tiles; no sort of
# 16 MiB rows)
_CORPUS_SAMPLE = "pmod(xxhash64(tile.path), 8) = 0"
# the input rows a generator's value fingerprint reads (one in 16, by a key of the row's content): its UDF decodes each row
_GEN_SAMPLE = {
    "jv_bng_cells_gen": "pmod(xxhash64(cellid), 16) = 0",
    "bng_polys": "pmod(xxhash64(wkt), 16) = 0",
    "jv_custom_polys": "pmod(xxhash64(wkt), 16) = 0",
    "tin_points": "pmod(xxhash64(points), 16) = 0",
}


@dataclasses.dataclass(frozen=True)
class Spec:
    fn: str
    inp: str  # key in `levels_forms.INPUTS`
    tag: str  # the input's label in the form names (`l1@<tag>`)
    shape: str  # scalar | tile | agg | gen
    call: str  # the call, `{fn}` standing for the SQL name
    value: str  # a key of `_VALUE`: what the value fingerprint hashes
    floor: str  # "udf:<ddl>" (a constant of that type), "tile" (the input tile back) or "scan:<column>"
    floor_args: str = (
        ""  # the input columns the floor UDF takes (default: the call's first argument)
    )
    gen_value: str = (
        ""  # a generator's per-row expression, `t.<column>` (default: `t.cellid`)
    )


_G = "jv_custom_grid"  # the custom grid column of the custom-grid inputs
# the same grid as a literal struct (what `gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)` folds to)
_GRID_LIT = (
    "named_struct('bound_x_min', 0L, 'bound_x_max', 1000000L, 'bound_y_min', 0L, 'bound_y_max', 1000000L, "
    "'cell_splits', 2, 'root_cell_size_x', 1000, 'root_cell_size_y', 1000, 'srid', 27700)"
)
_NULLS_D, _NULLS_I = "CAST(NULL AS DOUBLE)", "CAST(NULL AS INT)"

SPECS: List[Spec] = [
    # -- the raster accessors that return one value: a header read (the 15 one-argument ones over the corpus tiles, as the
    # width and bounding-box record; the four coordinate ones over the replicated DEM tiles, whose grid is known)
    *[
        Spec(f"gbx_rst_{n}", "tiles_n", "n", "scalar", "{fn}(tile)", v, f"udf:{t}", "tile")
        for n, t, v in (
            ("crs", "string", "hash"),
            ("format", "string", "hash"),
            ("height", "int", "hash"),
            ("memsize", "bigint", "hash"),
            ("numbands", "int", "hash"),
            ("pixelheight", "double", "round"),
            ("pixelwidth", "double", "round"),
            ("rotation", "double", "round"),
            ("scalex", "double", "round"),
            ("scaley", "double", "round"),
            ("skewx", "double", "round"),
            ("skewy", "double", "round"),
            ("srid", "int", "hash"),
            ("upperleftx", "double", "round"),
            ("upperlefty", "double", "round"),
        )
    ],
    # pixel (500, 300) of a DEM tile, and the world point at its centre (origin 400000 5760000, 10 m pixels)
    *[
        Spec(f"gbx_rst_{n}", "dem_1024_mem", "dem", "scalar", c, v, f"udf:{t}", "tile")
        for n, c, t, v in (
            ("rastertoworldcoordx", "{fn}(tile, 500, 300)", "double", "round"),
            ("rastertoworldcoordy", "{fn}(tile, 500, 300)", "double", "round"),
            ("worldtorastercoordx", "{fn}(tile, 405005D, 5756995D)", "int", "hash"),
            ("worldtorastercoordy", "{fn}(tile, 405005D, 5756995D)", "int", "hash"),
        )
    ],
    # -- the tile operations, over the replicated 1024 px DEM tiles (as TRI and the engine rows)
    Spec("gbx_rst_asformat", "dem_1024_mem", "dem", "tile", "{fn}(tile, 'COG')", "raster", "tile"),
    Spec("gbx_rst_band", "dem_1024_mem", "dem", "tile", "{fn}(tile, 1)", "raster", "tile"),
    Spec(
        "gbx_rst_convolve", "dem_1024_mem", "dem", "tile",
        "{fn}(tile, array(array(0D, 0.125D, 0D), array(0.125D, 0.5D, 0.125D), array(0D, 0.125D, 0D)))",
        "raster", "tile",
    ),  # fmt: skip
    Spec(
        "gbx_rst_derivedband", "dem_1024_mem", "dem", "tile",
        "{fn}(tile, 'def double(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, "
        "**kwargs):\\n    out_ar[:] = in_ar[0] * 2\\n', 'double')",
        "raster", "tile",
    ),  # fmt: skip
    Spec("gbx_rst_getsubdataset", "jv_nc_512_mem", "nc", "tile", "{fn}(tile, 'Band2')", "raster", "tile"),
    Spec("gbx_rst_initnodata", "dem_1024_mem", "dem", "tile", "{fn}(tile)", "raster", "tile"),
    Spec("gbx_rst_setcrs", "dem_1024_mem", "dem", "tile", "{fn}(tile, 'EPSG:32633')", "raster", "tile"),
    Spec("gbx_rst_setsrid", "dem_1024_mem", "dem", "tile", "{fn}(tile, 32633)", "raster", "tile"),
    Spec("gbx_rst_updatetype", "dem_1024_mem", "dem", "tile", "{fn}(tile, 'Float64')", "raster", "tile"),
    # -- an array of single-band tiles to one multi-band tile (the 512 px pairs of the map-algebra input)
    Spec("gbx_rst_frombands", "se_pair_512_mem", "pair", "tile", "{fn}(tiles)", "raster", "tile", "tiles[0]"),
    # -- the web tiles: the DEM tiles lie in z12 tile (2088, 1384) of web mercator (EPSG:3857)
    Spec(
        "gbx_rst_tilexyz", "dem_1024_mem", "dem", "scalar", "{fn}(tile, 12, 2088, 1384, 'PNG', 256, 'bilinear', 'auto')",
        "png", "udf:binary", "tile",
    ),  # fmt: skip
    Spec(
        "gbx_rst_xyzpyramid", "dem_1024_mem", "dem", "gen", "{fn}(s.tile, 11, 12, 'PNG', 256, 'bilinear', 'auto')",
        "png", "scan:tile.raster", gen_value="t.bytes",
    ),  # fmt: skip
    Spec(
        "gbx_pmtiles_agg", "jv_pm_tiles", "pm", "agg", "{fn}(bytes, z, x, y, '{{\"name\": \"bench\"}}')", "pmtiles",
        "scan:bytes",
    ),  # fmt: skip
    # -- the grid-cell aggregates and the cell rasterize aggregates, over (key, cell, value) rows in 200 keys
    Spec("gbx_h3_cellfill", "jv_h3_cells", "h3", "agg", "{fn}(cellid, value, 1, 'mean', 2.0)", "hash", "scan:cellid"),
    # the custom aggregates take the grid as a constant (the JVM level evaluates it once, with no row: a grid column
    # raises INTERNAL_ERROR "Cannot evaluate expression" there), as a user's `gbx_custom_grid(...)` literal is
    Spec(
        "gbx_custom_cellfill", "jv_custom_cells", "cg", "agg", "{fn}(cellid, value, " + _GRID_LIT + ", 1, 'mean', 2.0)",
        "hash", "scan:cellid",
    ),  # fmt: skip
    Spec(
        "gbx_rst_h3_rasterize_agg", "jv_h3_cells", "h3", "agg",
        "{fn}(cellid, value, 4326, " + ", ".join([_NULLS_D] * 5 + [_NULLS_I] * 2) + ", 'centroids', 1)",
        "png", "scan:cellid",
    ),  # fmt: skip
    Spec(
        "gbx_rst_quadbin_rasterize_agg", "jv_quadbin_cells", "qb", "agg",
        "{fn}(cellid, value, 4326, " + ", ".join([_NULLS_D] * 5 + [_NULLS_I] * 2) + ", 'centroids', 0)",
        "png", "scan:cellid",
    ),  # fmt: skip
    Spec(
        "gbx_rst_custom_rasterize_agg", "jv_custom_cells", "cg", "agg",
        "{fn}(cellid, value, " + _GRID_LIT + ", 27700, " + ", ".join([_NULLS_D] * 5 + [_NULLS_I] * 2) + ", 'centroids', 1)",
        "png", "scan:cellid",
    ),  # fmt: skip
    # -- the BNG cell scalars (1 km cells in TQ), the point one over points in Great Britain, the chip ones over chip pairs
    *[
        Spec(f"gbx_bng_{n}", "bng_cells", "cells", "scalar", c, v, f"udf:{t}", "cellid")
        for n, c, t, v in (
            ("aswkb", "{fn}(cellid)", "binary", "wkb"),
            ("aswkt", "{fn}(cellid)", "string", "hash"),
            ("cellarea", "{fn}(cellid)", "double", "round"),
            ("centroid", "{fn}(cellid)", "binary", "wkb"),
            ("distance", "{fn}(cellid, 'TQ5050')", "bigint", "hash"),
            ("euclideandistance", "{fn}(cellid, 'TQ5050')", "bigint", "hash"),
            ("kloop", "{fn}(cellid, 2)", "array<string>", "sorted"),
        )
    ],
    Spec("gbx_bng_pointascell", "jv_bng_points_wkt", "wktpts", "scalar", "{fn}(wkt, 3)", "hash", "udf:string", "wkt"),
    Spec("gbx_bng_cellunion", "jv_bng_chips", "chips", "scalar", "{fn}(left, right)", "chip", "udf:binary", "left"),
    Spec(
        "gbx_bng_cellintersection", "jv_bng_chips", "chips", "scalar", "{fn}(left, right)", "chip", "udf:binary",
        "left",
    ),  # fmt: skip
    # the BNG polygon arrays (rectangles of about ten 1 km cells, at the 1 km resolution)
    *[
        Spec(f"gbx_bng_{n}", "bng_polys", "polys", "scalar", c, "sorted", "udf:array<string>", "wkt")
        for n, c in (
            ("geomkring", "{fn}(wkt, 3, 1)"),
            ("geomkloop", "{fn}(wkt, 3, 1)"),
            ("polyfill", "{fn}(wkt, 3)"),
        )
    ],
    # the BNG explodes (k = 1 rings of 1 km cells, and the polygon rings)
    Spec("gbx_bng_kringexplode", "jv_bng_cells_gen", "cells", "gen", "{fn}(s.cellid, 1)", "hash", "scan:cellid"),
    Spec("gbx_bng_kloopexplode", "jv_bng_cells_gen", "cells", "gen", "{fn}(s.cellid, 1)", "hash", "scan:cellid"),
    Spec("gbx_bng_geomkringexplode", "bng_polys", "polys", "gen", "{fn}(s.wkt, 3, 1)", "hash", "scan:wkt"),
    Spec("gbx_bng_geomkloopexplode", "bng_polys", "polys", "gen", "{fn}(s.wkt, 3, 1)", "hash", "scan:wkt"),
    # -- the custom grid (0..1e6 m, 1 km root cells, two splits, EPSG:27700): cells at resolution 0 in a 100 km square
    *[
        Spec(f"gbx_custom_{n}", "jv_custom_cells", "cg", "scalar", c, v, f"udf:{t}", "cellid")
        for n, c, t, v in (
            ("cellaswkb", "{fn}(cellid, " + _G + ")", "binary", "wkb"),
            ("cellaswkt", "{fn}(cellid, " + _G + ")", "string", "hash"),
            ("centroid", "{fn}(cellid, " + _G + ")", "binary", "wkb"),
            ("distance", "{fn}(cellid, " + _G + ", cellid2)", "bigint", "hash"),
            ("kring", "{fn}(cellid, " + _G + ", 2)", "array<bigint>", "sorted"),
            ("kloop", "{fn}(cellid, " + _G + ", 2)", "array<bigint>", "sorted"),
            # the grid constructor: a bound from the row, so it is not one constant the optimizer folds
            ("grid", "{fn}(0, 1000000 + pmod(cellid, 7), 0, 1000000, 2, 1000, 1000, 27700)", "binary", "hash"),
        )
    ],
    Spec(
        "gbx_custom_pointascell", "jv_custom_cells", "cg", "scalar", "{fn}(point, " + _G + ", 0)", "hash", "udf:bigint",
        "point",
    ),  # fmt: skip
    *[
        Spec(f"gbx_custom_{n}", "jv_custom_polys", "cgpolys", "scalar", c, "sorted", "udf:array<bigint>", "wkt")
        for n, c in (
            ("geomkring", "{fn}(wkt, " + _G + ", 0, 1)"),
            ("geomkloop", "{fn}(wkt, " + _G + ", 0, 1)"),
            ("polyfill", "{fn}(wkt, " + _G + ", 0)"),
        )
    ],
    Spec(
        "gbx_custom_geomkringexplode", "jv_custom_polys", "cgpolys", "gen", "{fn}(s.wkt, s." + _G + ", 0, 1)", "hash",
        "scan:wkt",
    ),  # fmt: skip
    Spec(
        "gbx_custom_geomkloopexplode", "jv_custom_polys", "cgpolys", "gen", "{fn}(s.wkt, s." + _G + ", 0, 1)", "hash",
        "scan:wkt",
    ),  # fmt: skip
    # -- H3: the cell's bounding box (resolution 7 cells around London)
    Spec("gbx_h3_cell_bbox", "jv_h3_cells", "h3", "scalar", "{fn}(cellid, 4326, 'centroids', 0)", "bbox", "udf:binary"),
    # -- the vector scalars: EWKT polygons in EPSG:27700, and the Mosaic legacy struct of a polygon
    Spec("gbx_st_crs", "jv_srid_polys", "srid", "scalar", "{fn}(wkt)", "hash", "udf:string", "wkt"),
    Spec("gbx_st_setcrs", "jv_srid_polys", "srid", "scalar", "{fn}(wkt, 'EPSG:4326')", "ewkb", "udf:binary", "wkt"),
    Spec("gbx_st_legacyaswkb", "jv_legacy_polys", "legacy", "scalar", "{fn}(geom)", "wkb", "udf:binary", "id"),
    # -- the TIN elevation grid over a bounding box: 16 x 16 cells of each TIN of 60 points
    Spec(
        "gbx_st_interpolateelevationbbox", "tin_points", "tin", "gen",
        "{fn}(s.points, CAST(NULL AS ARRAY<STRING>), 0D, 0D, 'NONENCROACHING', 0D, 0D, 100D, 100D, 16, 16, 0, "
        "'constrained')",
        "wkb", "scan:points", gen_value="t.elevation_point",
    ),  # fmt: skip
]

FUNCTIONS: Tuple[str, ...] = tuple(s.fn for s in SPECS)


# --- the inputs ------------------------------------------------------------------------------------------------------
BASE_ROWS = {
    "jv_nc_512_mem": 128,
    "jv_bng_cells_gen": 250_000,
    "jv_bng_points_wkt": 1_000_000,
    "jv_bng_chips": 250_000,
    "jv_custom_cells": 1_000_000,
    "jv_custom_polys": 100_000,
    "jv_h3_cells": 1_000_000,
    "jv_quadbin_cells": 1_000_000,
    "jv_srid_polys": 250_000,
    "jv_legacy_polys": 250_000,
    "jv_pm_tiles": 20_000,
}
_KEYS = 200  # the groups of a (key, cell, value) input, as `bng_cells`
_PM_KEYS = 20  # archives of the PMTiles input
# the custom grid the custom-grid inputs carry, as the struct `gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)`
# returns (a column, not a call: the call would be timed with every form)
GRID = (0, 1_000_000, 0, 1_000_000, 2, 1000, 1000, 27700)
_GRID_SQL = (
    "named_struct('bound_x_min', CAST({} AS BIGINT), 'bound_x_max', CAST({} AS BIGINT), "
    "'bound_y_min', CAST({} AS BIGINT), 'bound_y_max', CAST({} AS BIGINT), 'cell_splits', CAST({} AS INT), "
    "'root_cell_size_x', CAST({} AS INT), 'root_cell_size_y', CAST({} AS INT), 'srid', CAST({} AS INT))"
).format(*GRID)


def _key(F, seed):
    return F.floor(F.rand(seed) * _KEYS).cast("string").alias("key")


def _nc_tile(size: int, seed: int) -> dict:
    """The bytes of a NetCDF file whose three variables `Band1` to `Band3` are three DEM surfaces (a materialized tile)."""
    import os
    import shutil
    import tempfile

    import numpy as np

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.levels_forms import _se_dem_tile

    def surface(k):
        with open_bytes(_se_dem_tile(size, seed + k)["raster"]) as ds:
            return ds.read(1)

    bands = np.stack([surface(k) for k in range(3)])
    d = tempfile.mkdtemp(prefix="gbx-jv-nc-")
    try:
        tif, nc = os.path.join(d, "b.tif"), os.path.join(d, "b.nc")
        with open(tif, "wb") as fh:
            fh.write(oracles._gtiff(bands, "EPSG:3857"))
        oracles.to_netcdf(tif, nc)
        with open(nc, "rb") as fh:
            return oracles._tile(fh.read())
    finally:
        shutil.rmtree(d, ignore_errors=True)


def _bng_points_wkt(spark, n, corpus, schema):
    """`n` WKT points at random places in Great Britain (EPSG:27700)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_bng_points_wkt_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            F.format_string(
                "POINT(%f %f)",
                F.rand(61) * 600000 + 50000,
                F.rand(62) * 1100000 + 50000,
            ).alias("wkt")
        ),
        rows=n,
    )


def _bng_cells_gen(spark, n, corpus, schema):
    """The BNG cells of `bng_cells` at the explodes' smaller base (each makes 9 rows)."""
    from databricks.labs.gbx.bench import levels_forms as lf

    return lf.INPUTS["bng_cells"](spark, n, corpus, schema)


def _bng_chips(spark, n, corpus, schema):
    """`n` pairs of chips of one 1 km cell: the cell's chip of a rectangle of `bng_polys` and of the rectangle moved 400 m
    east and north (both tessellated at the 1 km resolution with their core geometry, paired by rectangle and cell).
    """
    from databricks.labs.gbx import register
    from databricks.labs.gbx.bench import levels_forms as lf

    polys = lf.INPUTS["bng_polys"](spark, max(1000, n // 6), corpus, schema)
    register(spark, only=["gbx_bng_tessellateexplode"])
    _register_shift(spark)
    chip = "named_struct('cellid', t.cellid, 'core', t.core, 'chip', t.chip)"
    sql = (
        "SELECT a.c AS left, b.c AS right FROM "
        f"(SELECT p.wkt AS k, t.cellid AS cellid, {chip} AS c FROM {polys} p, "
        "LATERAL gbx_bng_tessellateexplode(p.wkt, 3, true) t) a JOIN "
        f"(SELECT s.k AS k, t.cellid AS cellid, {chip} AS c FROM "
        f"(SELECT wkt AS k, gbx_jv_shift(wkt) AS wkt FROM {polys}) s, "
        "LATERAL gbx_bng_tessellateexplode(s.wkt, 3, true) t) b "
        f"ON a.k = b.k AND a.cellid = b.cellid LIMIT {int(n)}"
    )
    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_bng_chips_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.sql(sql),
        rows=n,
    )


def _shift_wkt(wkt):
    """A WKT geometry moved 400 m east and north."""
    import pandas as pd
    import shapely
    from shapely import affinity

    return pd.Series(
        [
            shapely.to_wkt(
                affinity.translate(shapely.from_wkt(w), 400.0, 400.0),
                rounding_precision=3,
            )
            for w in wkt
        ],
        index=wkt.index,
    )


def _register_shift(spark):
    import pandas as pd
    from pyspark.sql import functions as F

    def shift(wkt):
        return _shift_wkt(wkt)

    # the hints as objects: this module's `from __future__ import annotations` makes them strings pandas_udf cannot read
    shift.__annotations__ = {"wkt": pd.Series, "return": pd.Series}
    spark.udf.register("gbx_jv_shift", F.pandas_udf(shift, "string"))


def _custom_cells_of(pdf):
    import pandas as pd

    from databricks.labs.gbx.pygx import _custom

    conf = _custom.CustomGridConf(*GRID)
    xs, ys = pdf["x"].to_numpy(), pdf["y"].to_numpy()
    cells = [
        _custom.point_to_cell_id(conf, float(x), float(y), 0) for x, y in zip(xs, ys)
    ]
    cells2 = [
        _custom.point_to_cell_id(conf, float(x) + 5000.0, float(y) + 3000.0, 0)
        for x, y in zip(xs, ys)
    ]
    return pd.DataFrame({"cellid": cells, "cellid2": cells2}, index=pdf.index)


def _custom_cells(spark, n, corpus, schema):
    """`(key, cellid, cellid2, value, point, jv_custom_grid)`: random 1 km cells of the custom grid in the 100 km square
    500..600 km east, 100..200 km north (as `bng_cells` in TQ), a second cell 5 km east and 3 km north, and the point.
    """
    import pandas as pd
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    def ids(x, y):
        return _custom_cells_of(pd.DataFrame({"x": x, "y": y}))

    ids.__annotations__ = {"x": pd.Series, "y": pd.Series, "return": pd.DataFrame}
    to_ids = F.pandas_udf(ids, "cellid bigint, cellid2 bigint")

    def build():
        pts = spark.range(n).select(
            _key(F, 63),
            (F.rand(64) * 100000 + 500000).alias("x"),
            (F.rand(65) * 100000 + 100000).alias("y"),
            (F.rand(66) * 100).alias("value"),
        )
        return pts.select(
            "key",
            to_ids("x", "y").alias("c"),
            "value",
            F.format_string("POINT(%f %f)", "x", "y").alias("point"),
        ).select(
            "key",
            "c.cellid",
            "c.cellid2",
            "value",
            "point",
            F.expr(_GRID_SQL).alias(_G),
        )

    return lf._table(
        spark, schema, f"mvp_in_jv_custom_cells_v{lf.RECIPE_VERSION}_{n}", build, rows=n
    )


def _custom_polys(spark, n, corpus, schema):
    """The rectangles of `bng_polys` (inside the custom grid's bounds) with the custom grid as a column."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    polys = lf.INPUTS["bng_polys"](spark, n, corpus, schema)
    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_custom_polys_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.table(polys).select("wkt", F.expr(_GRID_SQL).alias(_G)),
        rows=n,
    )


def _h3_ids(lat, lng):
    import h3
    import pandas as pd

    return pd.Series(
        [
            h3.str_to_int(h3.latlng_to_cell(float(a), float(b), 7))
            for a, b in zip(lat, lng)
        ],
        index=lat.index,
    )


def _h3_cells(spark, n, corpus, schema):
    """`(key, cellid, value)`: random resolution 7 H3 cells around London (51..52 N, -1..1 E), 200 keys."""
    import pandas as pd
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    def ids(lat, lng):
        return _h3_ids(lat, lng)

    ids.__annotations__ = {"lat": pd.Series, "lng": pd.Series, "return": pd.Series}
    to_id = F.pandas_udf(ids, "bigint")
    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_h3_cells_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            _key(F, 67),
            to_id(F.rand(68) + 51.0, F.rand(69) * 2.0 - 1.0).alias("cellid"),
            (F.rand(70) * 100).alias("value"),
        ),
        rows=n,
    )


def _quadbin_cells(spark, n, corpus, schema):
    """`(key, cellid, value)`: random zoom 13 quadbin cells around London (51..52 N, -1..1 E), 200 keys."""
    import pandas as pd
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    def ids(lng, lat):
        from databricks.labs.gbx.pygx import _quadbin

        return pd.Series(
            _quadbin.point_as_cell_vec(lng.to_numpy(), lat.to_numpy(), 13),
            index=lng.index,
        )

    ids.__annotations__ = {"lng": pd.Series, "lat": pd.Series, "return": pd.Series}
    to_id = F.pandas_udf(ids, "bigint")
    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_quadbin_cells_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            _key(F, 71),
            to_id(F.rand(72) * 2.0 - 1.0, F.rand(73) + 51.0).alias("cellid"),
            (F.rand(74) * 100).alias("value"),
        ),
        rows=n,
    )


def _srid_polys(spark, n, corpus, schema):
    """`n` EWKT rectangles in EPSG:27700 (`SRID=27700;POLYGON(...)`), 2.8 x 1.8 km at random places in Great Britain."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    x = (F.rand(75) * 500000 + 100000).alias("x")
    y = (F.rand(76) * 800000 + 100000).alias("y")
    return lf._table(
        spark,
        schema,
        f"mvp_in_jv_srid_polys_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n)
        .select(x, y)
        .select(
            F.format_string(
                "SRID=27700;POLYGON((%1$f %2$f, %3$f %2$f, %3$f %4$f, %1$f %4$f, %1$f %2$f))",
                "x",
                "y",
                F.col("x") + 2800,
                F.col("y") + 1800,
            ).alias("wkt")
        ),
        rows=n,
    )


def _legacy_polys(spark, n, corpus, schema):
    """`n` Mosaic legacy structs of a square with a square hole (typeId 5, srid 0), at random places, and an `id`."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    def build():
        base = spark.range(n).select(
            "id", (F.rand(77) * 1000).alias("x"), (F.rand(78) * 1000).alias("y")
        )
        ring = "array(array(x, y), array(x + 10D, y), array(x + 10D, y + 10D), array(x, y + 10D), array(x, y))"
        hole = (
            "array(array(x + 2D, y + 2D), array(x + 4D, y + 2D), array(x + 4D, y + 4D), array(x + 2D, y + 4D), "
            "array(x + 2D, y + 2D))"
        )
        return base.select(
            "id",
            F.expr(
                f"named_struct('typeId', 5, 'srid', 0, 'boundaries', array({ring}), 'holes', array(array({hole})))"
            ).alias("geom"),
        )

    return lf._table(
        spark, schema, f"mvp_in_jv_legacy_polys_v{lf.RECIPE_VERSION}_{n}", build, rows=n
    )


def _png(seed: int) -> bytes:
    """A 256 px RGB PNG of one colour per seed: what a PMTiles archive of raster tiles holds."""
    import numpy as np
    from rasterio.io import MemoryFile

    arr = np.full((3, 256, 256), 40 * (seed + 1), dtype="uint8")
    with MemoryFile() as mf:
        with mf.open(driver="PNG", width=256, height=256, count=3, dtype="uint8") as ds:
            ds.write(arr)
        return mf.read()


def _pm_tiles(spark, n, corpus, schema):
    """`(key, bytes, z, x, y)`: `n` PNG tiles at zoom 14 in 20 archives, each (z, x, y) once per archive."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf
    from databricks.labs.gbx.bench.clientcap import client_dataframe

    pngs = [(i, _png(i)) for i in range(4)]

    def build():
        four = client_dataframe(
            spark, pngs, "idx int, bytes binary", what="jv_pm_tiles PNGs"
        )
        r = F.floor(F.col("id") / _PM_KEYS)
        return (
            spark.range(n)
            .select(
                (F.col("id") % _PM_KEYS).cast("string").alias("key"),
                (F.col("id") % 4).cast("int").alias("idx"),
                F.lit(14).alias("z"),
                (F.lit(8000) + r % 64).cast("int").alias("x"),
                (F.lit(5000) + F.floor(r / 64)).cast("int").alias("y"),
            )
            .join(F.broadcast(four), "idx")
            .select("key", "bytes", "z", "x", "y")
        )

    return lf._table(
        spark, schema, f"mvp_in_jv_pm_tiles_v{lf.RECIPE_VERSION}_{n}", build, rows=n
    )


# --- the floors ------------------------------------------------------------------------------------------------------
_FLOOR_VALUE = {
    "int": 1,
    "bigint": 1,
    "double": 1.0,
    "string": "x",
    "binary": b"x",
    "array<string>": ["x"],
    "array<bigint>": [1],
}


def floor_name(ddl: str) -> str:
    """The session name of the floor UDF that returns a constant of type `ddl` (or the input tile back for "tile")."""
    slug = ddl.replace("array<", "arr_").replace(">", "").replace("<", "_")
    return f"gbx_jv_floor_{slug}"


def _floor_udf(ddl: str):
    import pandas as pd

    def floor(*cols):
        if ddl == "tile":
            return cols[0]
        return pd.Series([_FLOOR_VALUE[ddl]] * len(cols[0]), index=cols[0].index)

    floor.__annotations__ = {"cols": pd.Series, "return": pd.Series}
    return floor


def setup_floors(spark):
    """Register every floor UDF (`gbx_jv_floor_<type>`), once, outside the timed region."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import TILE_SQL

    for ddl in sorted({*_FLOOR_VALUE, "tile"}):
        out = TILE_SQL if ddl == "tile" else ddl
        spark.udf.register(floor_name(ddl), F.pandas_udf(_floor_udf(ddl), out))


def setup_pmtiles_fp(spark):
    """Register `gbx_bench_vfp_pmtiles`: an archive by its tiles, ((z, x, y), a digest of the bytes), sorted."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    @F.udf(returnType=LongType())
    def _fp(blob):
        return None if blob is None else pmtiles_fp(blob)

    spark.udf.register("gbx_bench_vfp_pmtiles", _fp)


def pmtiles_fp(blob) -> int:
    import hashlib

    from databricks.labs.gbx.bench import value_fp
    from databricks.labs.gbx.pmtiles._directory import iter_tiles

    tiles = sorted(
        (zxy, hashlib.blake2b(bytes(b), digest_size=8).hexdigest())
        for zxy, b in iter_tiles(bytes(blob))
    )
    return value_fp._digest(repr(tiles).encode())


# --- the forms -------------------------------------------------------------------------------------------------------


def _l3_variants(fn) -> List[str]:
    return list((fn.levels.get("L3", {}) or {}).get("variants", {}) or []) or ["l3"]


def _first_arg(call: str) -> str:
    inner = call.split("(", 1)[1]
    return inner.split(",", 1)[0].split(")", 1)[0].strip()


def _sql(s: Spec, name: str) -> str:
    return _SHAPES[s.shape].format(call=s.call.replace("{fn}", name))


def _vsql(s: Spec, name: str) -> str:
    """The value query: (n, fp, n_out) with fp the sum of the decoded value's hash, over the rows a decode can afford."""
    call = s.call.replace("{fn}", name)
    if s.shape == "gen":
        sample = _GEN_SAMPLE.get(s.inp)
        src = (
            "(SELECT first(tile) AS tile FROM {src} GROUP BY "
            + _REPLICATED[s.inp]
            + ")"
            if s.inp in _REPLICATED
            else f"(SELECT * FROM {{src}} WHERE {sample})"
        )
        v = _VALUE[s.value].format(o=s.gen_value or "t.cellid")
        return (
            f"SELECT count(*) AS n, sum(pmod({v}, {_M})) AS fp, count(*) AS n_out "
            f"FROM {src} s, LATERAL {call} t"
        )
    if s.shape == "agg":
        v = _VALUE[s.value].format(o="x")
        return (
            f"SELECT (SELECT count(*) FROM {{src}}) AS n, sum(pmod({v}, {_M})) AS fp, "
            "CASE WHEN count(x) = count(*) THEN (SELECT count(*) FROM {src}) ELSE count(x) END AS n_out "
            f"FROM (SELECT {call} AS x FROM {{src}} GROUP BY key)"
        )
    o = "t" if s.shape == "tile" else "c"
    v = _VALUE[s.value].format(o=o)
    if s.inp in _REPLICATED:
        cols = (
            "first(tiles) AS tiles"
            if s.inp == "se_pair_512_mem"
            else "first(tile) AS tile"
        )
        src = f"(SELECT {cols} FROM {{src}} GROUP BY {_REPLICATED[s.inp]})"
    elif s.inp == "tiles_n" and s.value in ("raster", "png"):
        src = f"(SELECT * FROM {{src}} WHERE {_CORPUS_SAMPLE})"
    else:
        src = "{src}"
    out = "t.raster" if s.shape == "tile" else "c"
    return (
        f"SELECT count(*) AS n, sum(pmod({v}, {_M})) AS fp, count({out}) AS n_out "
        f"FROM (SELECT {call} AS {o} FROM {src})"
    )


_ERR_TEXT = "substr(translate(replace(coalesce(t.metadata['last_error'], '<no last_error>'), chr(10), ' '), ';,', '  '), 1, 160)"


def _nullsql(s: Spec, name: str) -> str:
    """(n_null, digest, first): how many outputs are NULL; for a tile output, the distinct `last_error` texts with their
    counts (an error tile under load names its reason)."""
    call = s.call.replace("{fn}", name)
    if s.shape == "tile":
        return (
            "SELECT coalesce(sum(c), 0) AS n_null, md5(concat_ws('|', array_sort(collect_list(txt)))) AS s, "
            "slice(array_sort(collect_list(concat(c, ' x ', txt))), 1, 8) AS first FROM "
            f"(SELECT {_ERR_TEXT} AS txt, count(*) AS c FROM (SELECT {call} AS t FROM {{src}}) "
            "WHERE t.raster IS NULL GROUP BY 1)"
        )
    if s.shape == "scalar":
        return (
            "SELECT count(*) AS n_null, '' AS s, CAST(array() AS ARRAY<STRING>) AS first "
            f"FROM (SELECT {call} AS c FROM {{src}}) WHERE c IS NULL"
        )
    if s.shape == "agg":
        return (
            "SELECT count(*) AS n_null, '' AS s, CAST(array() AS ARRAY<STRING>) AS first "
            f"FROM (SELECT {call} AS x FROM {{src}} GROUP BY key) WHERE x IS NULL"
        )
    return ""  # a generator has no NULL output row


def _floor(s: Spec, common) -> Form:
    kind, _, arg = s.floor.partition(":")
    if kind == "scan":
        if (
            s.shape == "agg"
        ):  # the grouping without the work: the largest value of each group
            sql = _AGG.format(call=f"CAST(max({arg}) AS STRING)")
        else:
            sql = f"SELECT count(*) AS n, sum(length(CAST({arg} AS STRING))) AS fp, count(*) AS n_out FROM {{src}}"
        return Form(
            level="L3", variant=f"floor@{s.tag}", sql=sql, register=False, **common
        )
    ddl = "tile" if kind == "tile" else arg
    args = s.floor_args or _first_arg(s.call)
    call = f"{floor_name(ddl)}({args})"
    shape = _TILE if ddl == "tile" else _SCALAR
    return Form(
        level="L3", variant=f"floor@{s.tag}", sql=shape.format(call=call), setup=_ME + "setup_floors",
        register=False, **common,
    )  # fmt: skip


def jv_forms() -> List[Form]:
    """The forms of every function of `SPECS`: `l1@<tag>`, each light variant, then `floor@<tag>`."""
    from databricks.labs.gbx.manifest import load

    rows = load().functions
    out: List[Form] = []
    for s in SPECS:
        row = rows.get(s.fn)
        if row is None or "L1" not in row.levels:
            continue  # culled since (the manifest-pin test names the drift)
        common = dict(mvp=MVP, fn=s.fn, input=s.inp)
        extra = {"setup": _ME + "setup_pmtiles_fp"} if s.value == "pmtiles" else {}
        out.append(
            Form(
                level="L1", variant=f"l1@{s.tag}", sql=_sql(s, s.fn), vsql=_vsql(s, s.fn),
                nullsql=_nullsql(s, s.fn), computes=("dedicated",), allows_null=True, **common, **extra,
            )
        )  # fmt: skip
        for v in _l3_variants(row):
            out.append(
                Form(
                    level="L3", variant=f"{v}@{s.tag}", sql=_sql(s, s.fn), vsql=_vsql(s, s.fn),
                    nullsql=_nullsql(s, s.fn), allows_null=True, **common, **extra,
                )
            )  # fmt: skip
        out.append(_floor(s, common))
    return out


def variants_of(fn: str) -> Tuple[str, ...]:
    """The form names of `fn` in order (JVM, light variants, floor)."""
    return tuple(f.variant for f in jv_forms() if f.fn == fn)


def spec_of(fn: str) -> Optional[Spec]:
    return next((s for s in SPECS if s.fn == fn), None)


def install(lf) -> None:
    """Add the JV inputs and `FORMS["JV"]` to `levels_forms` (called once, at its import)."""
    lf.INPUTS.update(
        {
            "jv_nc_512_mem": lf._se_dems_replicated(512, _nc_tile, "jvnc", parts=32),
            "jv_bng_cells_gen": _bng_cells_gen,
            "jv_bng_points_wkt": _bng_points_wkt,
            "jv_bng_chips": _bng_chips,
            "jv_custom_cells": _custom_cells,
            "jv_custom_polys": _custom_polys,
            "jv_h3_cells": _h3_cells,
            "jv_quadbin_cells": _quadbin_cells,
            "jv_srid_polys": _srid_polys,
            "jv_legacy_polys": _legacy_polys,
            "jv_pm_tiles": _pm_tiles,
        }
    )
    lf.BASE_ROWS.update(BASE_ROWS)
    lf.FORMS[MVP] = jv_forms()


def by_input() -> Dict[str, List[str]]:
    """`{input: [functions]}`: the functions that share one prepared input (a run part reads one input)."""
    out: Dict[str, List[str]] = {}
    for s in SPECS:
        out.setdefault(s.inp, []).append(s.fn)
    return out


# --- the run parts ---------------------------------------------------------------------------------------------------
# One cluster lease per part, about 20 to 30 min at 1x and 4x each: the functions of a part share one prepared input
# where they can (an input is built once per scale and read by every form of the part), and a part times the floor of its
# first function per input only (the floors of one input and shape cost the same).
PARTS: Dict[str, Tuple[str, ...]] = {
    "acc1": tuple(f"gbx_rst_{n}" for n in ("crs", "format", "height", "memsize", "numbands")),
    "acc2": tuple(f"gbx_rst_{n}" for n in ("pixelheight", "pixelwidth", "rotation", "scalex", "scaley")),
    "acc3": tuple(f"gbx_rst_{n}" for n in ("skewx", "skewy", "srid", "upperleftx", "upperlefty")),
    "dem1": tuple(
        f"gbx_rst_{n}"
        for n in (
            "rastertoworldcoordx", "rastertoworldcoordy", "worldtorastercoordx", "worldtorastercoordy",
            "tilexyz", "xyzpyramid", "band",
        )
    ),  # fmt: skip
    "dem2": tuple(
        f"gbx_rst_{n}" for n in ("asformat", "convolve", "derivedband", "initnodata", "setcrs", "setsrid", "updatetype")
    ),
    "tiles": ("gbx_rst_getsubdataset", "gbx_rst_frombands", "gbx_pmtiles_agg"),
    "cells": (
        "gbx_h3_cellfill", "gbx_rst_h3_rasterize_agg", "gbx_h3_cell_bbox", "gbx_custom_cellfill",
        "gbx_rst_custom_rasterize_agg", "gbx_rst_quadbin_rasterize_agg",
    ),  # fmt: skip
    "bngscalar": tuple(
        f"gbx_bng_{n}"
        for n in (
            "aswkb", "aswkt", "cellarea", "centroid", "distance", "euclideandistance", "kloop", "pointascell",
            "cellunion", "cellintersection",
        )
    ),  # fmt: skip
    "cgscalar": tuple(
        f"gbx_custom_{n}" for n in ("cellaswkb", "cellaswkt", "centroid", "distance", "kring", "kloop", "grid", "pointascell")
    ),
    "bngpolys": tuple(f"gbx_bng_{n}" for n in ("geomkring", "geomkloop", "polyfill")),
    "bngexplode": tuple(f"gbx_bng_{n}" for n in ("geomkringexplode", "geomkloopexplode")),
    "cgpolys": tuple(f"gbx_custom_{n}" for n in ("geomkring", "geomkloop", "polyfill")),
    "cgexplode": tuple(f"gbx_custom_{n}" for n in ("geomkringexplode", "geomkloopexplode")),
    "vector": (
        "gbx_bng_kringexplode", "gbx_bng_kloopexplode", "gbx_st_crs", "gbx_st_setcrs", "gbx_st_legacyaswkb",
        "gbx_st_interpolateelevationbbox",
    ),  # fmt: skip
}


def part_forms(fns, floors: bool = True) -> List[str]:
    """The qualified form names (`<fn>:<variant>`, for `--levels-forms`) of `fns`: each function's JVM and light forms, and
    the floor of the first function per input (with `floors`)."""
    out: List[str] = []
    floored = set()
    for fn in fns:
        s = spec_of(fn)
        if s is None:
            raise KeyError(f"{fn} is not in the JV set")
        for v in variants_of(fn):
            if v.startswith("floor@"):
                if not floors or s.inp in floored:
                    continue
                floored.add(s.inp)
            out.append(f"{fn}:{v}")
    return out


def main(argv=None) -> int:
    """`python -m databricks.labs.gbx.bench.levels_jvm --part <name>` (or `--fns a,b`): print the `--levels-forms` value."""
    import argparse

    ap = argparse.ArgumentParser(
        description="the --levels-forms value of a JV run part"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--part", choices=sorted(PARTS))
    g.add_argument("--fns", help="comma-separated SQL names")
    ap.add_argument(
        "--no-floor",
        action="store_true",
        help="leave the floors out (a 16x rerun of a part's functions)",
    )
    a = ap.parse_args(argv)
    fns = (
        PARTS[a.part]
        if a.part
        else tuple(x.strip() for x in a.fns.split(",") if x.strip())
    )
    print(",".join(part_forms(fns, floors=not a.no_floor)))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
