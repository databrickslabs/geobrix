"""Forms + inputs per set of functions (S, G, A, H); each set appends its forms. Inputs are cached Delta tables.

An input builder is ``f(spark, n, corpus, schema) -> table name`` (None when the input cannot
be built here). With ``schema`` it is a Delta table, created once and reused by every form and
run; without one, a temp view. The seeds are fixed, so a form sees the same rows every time.
"""

from __future__ import annotations

import os
from typing import Callable, Dict, List, Optional

from databricks.labs.gbx._gdal_policy import open_data
from databricks.labs.gbx.bench.clientcap import (
    check_estimate,
    client_dataframe,
    size_of,
)
from databricks.labs.gbx.bench.levels import Form

# Bump when an input recipe (a seed or a shape) changes: it is part of the cached table's name,
# so a stale table from an older recipe is never reused.
RECIPE_VERSION = 2
BASE_ROWS = {
    "bng_points": 1_000_000,
    "invalid_polys": 250_000,
    "tiles_mat": 250,
    "tiles_virt": 250,
    # DataFrame helpers (H): groups of tiles (merge: two offset copies of a corpus tile, combine: two aligned copies)
    # and the corpus tiles on the British National Grid (the others have no BNG cells).
    "tiles_grouped_mat": 250,
    "tiles_grouped_virt": 250,
    "tiles_aligned_mat": 250,
    "tiles_gb_mat": 250,
    "tiles_gb_virt": 250,
    # Generators (G): BNG polygons that each cover a handful of cells, and a few big tiles (4096 x 4096)
    "bng_polys": 100_000,
    "tiles_big": 4,
    # Aggregates (A): tiles in groups of about 50, and (key, BNG cell, value) rows; each also with half of its rows in one key
    "tiles_grouped": 2000,
    "tiles_grouped_skew": 112,  # half of it is one key: 56 tiles, 896 MiB decoded
    "bng_cells": 1_000_000,
    "bng_cells_skew": 1_000_000,
    # DataFrame helpers (H): towers on a DSM of 16 tiles (the tiles are the same at every scale)
    "dsm_towers": 64,
}


def _table(spark, schema: Optional[str], name: str, build) -> str:
    if schema is None:
        build().createOrReplaceTempView(name)
        return name
    full = f"{schema}.{name}"
    if not spark.catalog.tableExists(full):
        build().write.format("delta").saveAsTable(full)
    return full


def _bng_points(spark, n, corpus, schema):
    from pyspark.sql import functions as F

    return _table(
        spark,
        schema,
        f"mvp_in_bng_points_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            (F.rand(7) * 700000).alias("e"), (F.rand(8) * 1300000).alias("n")
        ),
    )


def _invalid_polys(spark, n, corpus, schema):
    from pyspark.sql import functions as F

    x, y = (F.rand(3) * 1000).alias("x"), (F.rand(4) * 1000).alias("y")
    # A bow-tie ring: self-intersecting, so every polygon needs repairing.
    return _table(
        spark,
        schema,
        f"mvp_in_invalid_polys_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n)
        .select(x, y)
        .select(
            F.format_string(
                "POLYGON((%1$f %2$f, %3$f %4$f, %3$f %2$f, %1$f %4$f, %1$f %2$f))",
                "x",
                "y",
                F.col("x") + 1,
                F.col("y") + 1,
            ).alias("wkt")
        ),
    )


def _bng_polys(spark, n, corpus, schema):
    """2800 x 1800 m rectangles at random places inside Great Britain (EPSG:27700): each one covers
    about six 1 km cells, so a tessellation of them makes about six rows per input row.
    """
    from pyspark.sql import functions as F

    x = (F.rand(5) * 500000 + 100000).alias("x")
    y = (F.rand(6) * 800000 + 100000).alias("y")
    return _table(
        spark,
        schema,
        f"mvp_in_bng_polys_v{RECIPE_VERSION}_{n}",
        lambda: spark.range(n)
        .select(x, y)
        .select(
            F.format_string(
                "POLYGON((%1$f %2$f, %3$f %2$f, %3$f %4$f, %1$f %4$f, %1$f %2$f))",
                "x",
                "y",
                F.col("x") + 2800,
                F.col("y") + 1800,
            ).alias("wkt")
        ),
    )


SMALL_INPUT_ROWS = 64  # a tile input of at most this many rows is built from the first files, not by a limit


def _tiles(virtual: bool, subdir: str = "rows", name: Optional[str] = None):
    """The corpus tiles under `subdir` as a one-column `tile` table (`name` labels the cached table)."""

    def build(spark, n, corpus, schema):
        rows = corpus.rstrip("/") + "/" + subdir
        if not os.path.isdir(rows):
            return None
        from databricks.labs.gbx.ds.register import register

        register(spark)  # the light raster reader
        label = f"mvp_in_tiles_{name or ('virt' if virtual else 'mat')}_v{RECIPE_VERSION}_{n}"
        first = (
            _corpus_rows(corpus) if subdir == "rows" and n <= SMALL_INPUT_ROWS else None
        )
        if first:
            # A few rows (the smoke): the first n files, not `limit(n)` over the whole directory, which reads every
            # file (one row each) before it keeps n of them: minutes for a table of 16 tiles.
            pairs = [(str(i), p) for i, p in enumerate(first[:n])]
            return _table(
                spark,
                schema,
                label,
                lambda: _tile_column(spark, pairs, virtual, keyed=False),
            )
        return _table(
            spark,
            schema,
            label,
            lambda: spark.read.format("raster_gbx")
            .option("virtualTiles", str(virtual).lower())
            .load(rows)
            .select("tile")
            .limit(n),
        )

    return build


def _corpus_rows(corpus: str) -> Optional[List[str]]:
    """The corpus' row tiles, sorted; None when the corpus has none."""
    rows = corpus.rstrip("/") + "/rows"
    if not os.path.isdir(rows):
        return None
    from databricks.labs.gbx.ds._listing import list_files

    return list_files(
        rows, filter_regex=r".*\.tif$", recursive=False, raise_on_empty=False
    )


def _tile_column(spark, pairs, virtual: bool, keyed: bool):
    """A DataFrame of ``[key,] tile`` from ``(key, path)`` pairs: virtual tiles, or tiles read now."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    if virtual:
        rows = [(k, VirtualTile(cellid=0, path=p).to_row()) for k, p in pairs]
        df = client_dataframe(
            spark, rows, f"key string, tile {TILE_SQL}", what="virtual tile rows"
        )
    else:
        from databricks.labs.gbx.pyrx import functions as prx

        df = client_dataframe(
            spark, pairs, "key string, path string", what="tile path rows"
        ).select("key", prx.rst_fromfile(F.col("path"), "GTiff", True).alias("tile"))
    return df if keyed else df.select("tile")


def _grouped(recipe: str, virtual: bool):
    """Groups of tiles for the by-key helpers: each corpus row tile with the members the multi-tile
    recipe of ``bench.synth`` writes beside it (merge: two offset copies; combineavg: two aligned
    copies), keyed by the recipe directory's hash. The members are files under the corpus, written once.
    """

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None
        from databricks.labs.gbx.bench import synth

        pairs = []
        for src in sources[:n]:
            out = synth.synth_dir(corpus, "rows/" + os.path.basename(src), recipe)
            pairs += [
                (os.path.basename(out), m) for m in synth.synthesize(src, recipe, out)
            ]
        kind = "virt" if virtual else "mat"
        return _table(
            spark,
            schema,
            f"mvp_in_tiles_grouped_{recipe}_{kind}_v{RECIPE_VERSION}_{n}",
            lambda: _tile_column(spark, pairs, virtual, keyed=True),
        )

    return build


def _gb_tiles(virtual: bool):
    """The corpus row tiles that are EPSG:27700 (the BNG cells of the others fall outside Britain)."""

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None

        gb = []
        for p in sources:
            with open_data(p) as ds:
                if ds.crs is not None and ds.crs.to_epsg() == 27700:
                    gb.append(p)
            if len(gb) >= n:
                break
        if not gb:
            return None
        kind = "virt" if virtual else "mat"
        return _table(
            spark,
            schema,
            f"mvp_in_tiles_gb_{kind}_v{RECIPE_VERSION}_{n}",
            lambda: _tile_column(spark, [(p, p) for p in gb], virtual, keyed=False),
        )

    return build


_GROUP_SIZE = 50  # tiles a group holds, at most
_REST_GROUP_SIZE = 4  # tiles a group holds, at most, in the half of the skew input that is not the hot key
_CELL_KEYS = 200  # groups of the BNG cell rows


def _plan_grouped_tiles(sources, n, skew):
    """`(key, path)` pairs for the first `n` of the corpus row tiles `sources`, every group of ONE CRS.

    The corpus cycles four systems over four places (two cities), so a group of mixed ones is a mosaic of
    several places: terabytes of NoData for the light merge, and one tile for the heavy one. Same-system tiles
    share their place, so a group merges to one tile and what the shapes differ on is how they hold the members.
    The groups are sized from the tiles there are (`n` is only an upper bound), so the 1x, 4x and 16x inputs
    have groups of one size until the corpus runs out.

    With `skew`, half the rows are ONE key of one system (`hot`) and the other half are small groups of up to 4
    tiles, so the hot key is 14 times the typical group at the base size and more beyond it. The base size
    (`BASE_ROWS`) makes `hot` 56 tiles, 896 MiB decoded (16 MiB each): the skew is in how the rows are spread,
    not one group too big to hold. At 4x and 16x `hot` is 3.5 and 14 GiB, which no merge shape can hold: those
    points are the memory cliff, not a comparison.
    """

    n = min(n, len(sources))
    half = n // 2
    size = _REST_GROUP_SIZE if skew else _GROUP_SIZE
    by_crs: Dict[Optional[int], List[str]] = {}
    hot_crs = None
    for p in sources:
        with open_data(p) as ds:
            crs = ds.crs.to_epsg() if ds.crs else None
        hot_crs = crs if not by_crs else hot_crs
        by_crs.setdefault(crs, []).append(p)
        have = sum(map(len, by_crs.values()))
        if have >= n and (not skew or len(by_crs[hot_crs]) >= half):
            break
    pairs = []
    if skew:
        hot, by_crs[hot_crs] = by_crs[hot_crs][:half], by_crs[hot_crs][half:]
        pairs += [("hot", p) for p in hot]
    left = n - len(pairs)
    for crs, paths in sorted(by_crs.items(), key=lambda kv: str(kv[0])):
        for i, p in enumerate(paths[: max(0, left)]):
            pairs.append((f"{crs}-{i // size}", p))
        left -= len(paths)
    return pairs


def _grouped_tiles(skew: bool):
    """`(key, tile)`: the corpus row tiles in groups of one CRS (see :func:`_plan_grouped_tiles`)."""

    def build(spark, n, corpus, schema):
        sources = _corpus_rows(corpus)
        if not sources:
            return None
        n = min(n, len(sources))
        pairs = _plan_grouped_tiles(sources, n, skew)
        kind = "skew" if skew else "even"
        return _table(
            spark,
            schema,
            f"mvp_in_tiles_grouped_{kind}_v{RECIPE_VERSION}_{n}",
            lambda: _tile_column(spark, pairs, False, keyed=True),
        )

    return build


def _bng_cells(skew: bool):
    """`(key, cellid, value)`: random 1 km cells inside the 100 km square TQ, 200 keys; with `skew`, half the rows
    are key 0 (and so the same few thousand cells over and over)."""

    def build(spark, n, corpus, schema):
        from pyspark.sql import functions as F

        key = F.floor(F.rand(5) * _CELL_KEYS)
        if skew:
            key = F.when(F.rand(6) < 0.5, F.lit(0)).otherwise(key)
        cell = F.format_string(
            "TQ%02d%02d", F.floor(F.rand(7) * 100), F.floor(F.rand(8) * 100)
        )
        return _table(
            spark,
            schema,
            f"mvp_in_bng_cells_{'skew' if skew else 'even'}_v{RECIPE_VERSION}_{n}",
            lambda: spark.range(n).select(
                key.cast("string").alias("key"),
                cell.alias("cellid"),
                (F.rand(9) * 100).alias("value"),
            ),
        )

    return build


_DSM_GRID, _DSM_TILE_PX = (
    4,
    2000,
)  # tiles per side, and pixels per tile side (1 m pixels)
_TOWER_RADIUS, _TOWER_MAST = 500.0, 30.0  # metres


def _dsm_towers(spark, n, corpus, schema):
    """`n` towers on a DSM of `_DSM_GRID` x `_DSM_GRID` tiles of `_DSM_TILE_PX` pixels (EPSG:3857, x0 = y0 = 0), the
    towers at fixed random places at least a radius inside it. The tiles are files under the corpus, written once;
    their `(tx, ty, path)` manifest is the table beside the towers', named `<towers table>_tiles`.
    """
    if not os.path.isdir(corpus):
        return None
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import oracles

    tiles = oracles.dsm_tile_rows(
        os.path.join(corpus, "_synth", "dsm_towers", f"v{RECIPE_VERSION}"),
        _DSM_GRID,
        _DSM_TILE_PX,
    )
    name = f"mvp_in_dsm_towers_v{RECIPE_VERSION}_{n}"
    _table(
        spark,
        schema,
        name + "_tiles",
        lambda: client_dataframe(
            spark, tiles, "tx int, ty int, path string", what="tower tile paths"
        ),
    )
    reach = _DSM_GRID * _DSM_TILE_PX - 2 * _TOWER_RADIUS
    return _table(
        spark,
        schema,
        name,
        lambda: spark.range(n).select(
            F.concat(F.lit("t"), F.col("id").cast("string")).alias("tower_cellid"),
            (F.rand(11) * reach + _TOWER_RADIUS).alias("x"),
            (F.rand(12) * reach + _TOWER_RADIUS).alias("y"),
        ),
    )


INPUTS: Dict[str, Callable] = {
    "bng_points": _bng_points,
    "invalid_polys": _invalid_polys,
    "tiles_mat": _tiles(False),
    "tiles_virt": _tiles(True),
    "tiles_grouped_mat": _grouped("merge", False),
    "tiles_grouped_virt": _grouped("merge", True),
    "tiles_aligned_mat": _grouped("combineavg", False),
    "tiles_gb_mat": _gb_tiles(False),
    "tiles_gb_virt": _gb_tiles(True),
    "bng_polys": _bng_polys,
    "tiles_grouped": _grouped_tiles(False),
    "tiles_grouped_skew": _grouped_tiles(True),
    "bng_cells": _bng_cells(False),
    "bng_cells_skew": _bng_cells(True),
    "dsm_towers": _dsm_towers,
    "tiles_big": _tiles(
        False, "big", "big"
    ),  # absent until the 4096 x 4096 tiles are copied to {corpus}/big
}

# Each query answers one row (n, fp, n_out): the output count, an order-independent fingerprint,
# and count(<the output>), which skips NULLs: n_out != n means some rows came out NULL. The hash
# is reduced before it is summed: a sum of raw 64-bit hashes overflows under ANSI.
_BNG = (
    "SELECT count(*) AS n, sum(pmod(xxhash64(c), 2147483647)) AS fp, count(c) AS n_out "
    "FROM (SELECT {fn}(e, n, 3) AS c FROM {src})"
)
_TCRS = (
    "SELECT count(*) AS n, sum(length(t.raster)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {fn}(tile, 'EPSG:3857') AS t FROM {src})"
)
_MV = (
    "SELECT count(*) AS n, sum(length(g)) AS fp, count(g) AS n_out "
    "FROM (SELECT {fn}(wkt) AS g FROM {src})"
)
_MV_UC = (
    "SELECT count(*) AS n, sum(length(st_asewkb(g))) AS fp, count(g) AS n_out FROM "
    "(SELECT {fn}(CAST(st_geomfromwkt(wkt, 4326) AS GEOMETRY(ANY))) AS g FROM {src})"
)


_FLOOR = "TQ3080"


def register_floor(spark):
    """The BNG function's Arrow UDF shell with nothing inside it: the same three input columns
    and string output as ``gbx_bng_eastnorthasbng``, and a constant body. Registered once, by the
    form's ``setup``, outside the timed region."""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    const = _FLOOR

    def floor(e, n, res):
        return pd.Series(const, index=e.index)

    # Type hints as objects, not the strings this module's `from __future__ import annotations` would make: the
    # pandas UDF reads them to pick the Series -> Series form (no deprecated PandasUDFType argument).
    floor.__annotations__ = {
        "e": pd.Series,
        "n": pd.Series,
        "res": pd.Series,
        "return": pd.Series,
    }
    spark.udf.register("gbx_bng_floor", F.pandas_udf(floor, StringType()))


def floor_bng(spark, src):
    """The floor query. Its median is what the shell costs without a kernel, so
    ``floor_median_s`` beside a variant's median says how much of that variant is the kernel
    (the part a faster kernel can win back)."""
    df = spark.sql(_BNG.replace("{fn}", "gbx_bng_floor").format(src=src))
    r = df.collect()[0]
    return r[0], r[1], r[2], df


def _f(level, variant, fn, inp, sql, uc=False, **kw):
    name = ("{uc}." + fn) if uc else fn  # an L4 function is addressed by its UC schema
    return Form(
        "S", fn, level, variant, inp, sql.replace("{fn}", name), register=not uc, **kw
    )


B, T, M = "gbx_bng_eastnorthasbng", "gbx_rst_transformcrs", "gbx_st_makevalid"
FORMS: Dict[str, List[Form]] = {
    "S": [
        _f("L1", "-", B, "bng_points", _BNG, computes=("dedicated",)),
        _f("L2", "-", B, "bng_points", _BNG, computes=("dedicated", "standard")),
        _f("L3", "c0", B, "bng_points", _BNG),
        _f("L3", "c1", B, "bng_points", _BNG),
        _f("L3", "d", B, "bng_points", _BNG),
        Form(
            "S",
            B,
            "L3",
            "floor",
            "bng_points",
            run="databricks.labs.gbx.bench.levels_forms:floor_bng",
            setup="databricks.labs.gbx.bench.levels_forms:register_floor",
            register=False,
        ),
        _f(
            "L4",
            "uc-scalar",
            B,
            "bng_points",
            _BNG,
            uc=True,
            computes=("serverless", "dedicated"),
        ),
        _f("L1", "-", T, "tiles_mat", _TCRS, computes=("dedicated",)),
        _f("L1", "dispatch-virtual", T, "tiles_virt", _TCRS, computes=("dedicated",)),
        _f("L2", "-", T, "tiles_mat", _TCRS, computes=("dedicated", "standard")),
        _f("L3", "-", T, "tiles_mat", _TCRS),
        _f("L3", "virtual", T, "tiles_virt", _TCRS),
        _f("L3", "-", M, "invalid_polys", _MV),
        _f(
            "L4",
            "uc-scalar",
            M,
            "invalid_polys",
            _MV_UC,
            uc=True,
            computes=("serverless", "dedicated"),
        ),
    ]
}


# --- H, the DataFrame helpers: against the per-row UDTFs and the grouped aggregates --------------
#
# Forms run in this order: the light ones first, then the JVM ones, because both register the same
# `gbx_rst_*` names and a registration lasts for the session (a setup runs once).
# Each timed query reads the whole output: tile-returning forms hand it to `gbx_rst_transformcrs`
# (a downstream op that reads the pixels), and the fingerprint is `sum(length(raster))` with
# `count(raster)` as n_out, so a NULL raster shows. The retile size is the same on both sides.

_RETILE_PX = 512
_DOWN = "gbx_rst_transformcrs(tile, 'EPSG:3857')"
_TILE_FP = (
    "SELECT count(*) AS n, sum(length(x.raster)) AS fp, count(x.raster) AS n_out FROM "
    "(SELECT {down} AS x FROM {{src}}, LATERAL {udtf} AS t)"
)
_RETILE_UDTF = _TILE_FP.format(
    down="gbx_rst_transformcrs(struct(t.*), 'EPSG:3857')",
    udtf=f"gbx_rst_retile(tile, {_RETILE_PX}, {_RETILE_PX})",
)
_TESS_UDTF = _TILE_FP.format(
    down="gbx_rst_transformcrs(struct(t.*), 'EPSG:3857')",
    udtf="gbx_rst_bng_tessellate(tile, 3)",
)
# The light aggregate returns the mosaic as BINARY; the heavy one returns a tile struct.
_MERGE_L3 = (
    "SELECT count(*) AS n, sum(length(x)) AS fp, count(x) AS n_out FROM "
    "(SELECT gbx_rst_merge_agg(tile) AS x FROM {src} GROUP BY key)"
)
_MERGE_L1 = (
    "SELECT count(*) AS n, sum(length(x.raster)) AS fp, count(x.raster) AS n_out FROM "
    "(SELECT gbx_rst_merge_agg(tile) AS x FROM {src} GROUP BY key)"
)
_COMBINE_SQL = (
    "SELECT count(*) AS n, sum(length(x.raster)) AS fp, count(x.raster) AS n_out FROM "
    "(SELECT gbx_rst_combineavg(collect_list(tile)) AS x FROM {src} GROUP BY key)"
)


def _register_light(spark, *names):
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark, only=list(names))


def register_h_downstream(spark):
    _register_light(spark, "gbx_rst_transformcrs")


def register_h_retile_udtf(spark):
    _register_light(spark, "gbx_rst_transformcrs", "gbx_rst_retile")


def register_h_tess_udtf(spark):
    _register_light(spark, "gbx_rst_transformcrs", "gbx_rst_bng_tessellate")


def register_h_merge_l3(spark):
    _register_light(spark, "gbx_rst_merge_agg")


def register_h_combine_l3(spark):
    _register_light(spark, "gbx_rst_combineavg")


def register_h_heavy(spark):
    """The JVM names (RST_MergeAgg, RST_CombineAvg): needs the staged JAR. Replaces the light ones."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)


def _stats(df, tile_expr: Optional[str]):
    """(n, fp, n_out, df): the row count, the summed raster length and the non-NULL raster count of
    `tile_expr` over `df` (or of its `raster` column), read in full. `df` here is the query that ran.
    """
    from pyspark.sql import functions as F

    if tile_expr is not None:
        df = df.select(F.expr(tile_expr).alias("x")).select(
            F.col("x.raster").alias("raster")
        )
    out = df.agg(F.count(F.lit(1)), F.sum(F.length("raster")), F.count("raster"))
    r = out.collect()[0]
    return r[0], r[1], r[2], out


def h_retile_helper(spark, src):
    from databricks.labs.gbx.pyrx import helpers

    tiles = helpers.retile_virtual(spark.table(src), "tile", _RETILE_PX, _RETILE_PX)
    return _stats(tiles, _DOWN)


def h_tess_helper(spark, src):
    from databricks.labs.gbx.pyrx import helpers

    return _stats(helpers.bng_tessellate_virtual(spark.table(src), "tile", 3), _DOWN)


def h_merge_fold(spark, src):
    from databricks.labs.gbx.pyrx import helpers

    return _stats(helpers.merge_by_key(spark.table(src), ["key"], method="fold"), None)


def h_merge_vrt(spark, src):
    from databricks.labs.gbx.pyrx import helpers

    return _stats(helpers.merge_by_key(spark.table(src), ["key"], method="vrt"), None)


def h_combine_avg(spark, src):
    from databricks.labs.gbx.pyrx import helpers

    return _stats(helpers.combine_by_key(spark.table(src), ["key"], stat="avg"), None)


def _towers(spark, src, level):
    """(n, fp, n_out, df) of `rst_viewshed_towers` at `level` over the towers of `src` and the DSM beside them: the
    tower count, the summed visible pixels, and the non-NULL count of them. The counts differ between levels
    where their GDAL builds do, so the fingerprints are compared within a level, not across them.
    """
    from pyspark.sql import functions as F

    from databricks.labs.gbx.pyrx.functions import rst_viewshed_towers

    done = rst_viewshed_towers(
        spark.table(src), spark.table(src + "_tiles"), _TOWER_RADIUS, _TOWER_MAST,
        x0=0.0, y0=0.0, tile_m=float(_DSM_TILE_PX), _levels=level,
    )  # fmt: skip
    out = done.agg(F.count(F.lit(1)), F.sum("visible_px"), F.count("visible_px"))
    r = out.collect()[0]
    return r[0], r[1], r[2], out


def h_towers_l3(spark, src):
    return _towers(spark, src, "L3")


def h_towers_l1_steps(spark, src):
    return _towers(spark, src, "L1-steps")


def h_towers_jvm_df(spark, src):
    return _towers(spark, src, "JVM-DF")


def _set_up_towers_level(spark, level):
    """Set `level` up for `rst_viewshed_towers` (once, outside the timed region). A session that cannot run it
    would fall back to L3 and time that under this level's name: that is an error here, with the reason.
    """
    from databricks.labs.gbx.manager import levels

    if levels.towers_level(spark, level) != level:
        raise RuntimeError(
            f"this session cannot run rst_viewshed_towers at {level}; it would time L3 (see the line above)"
        )


def setup_h_towers_l1_steps(spark):
    """The Catalyst steps of the L1-steps form under their private names (needs the staged JAR)."""
    _set_up_towers_level(spark, "L1-steps")


def setup_h_towers_jvm_df(spark):
    """The capability check of the JVM-DF form, done once here and not in the timed region."""
    _set_up_towers_level(spark, "JVM-DF")


_H = "databricks.labs.gbx.bench.levels_forms:"


def _h(fn, level, variant, inp, *, run="", sql="", setup="", **kw):
    return Form(
        "H",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        run=_H + run if run else "",
        setup=_H + setup if setup else "",
        register=False,
        **kw,
    )


FORMS["H"] = [
    _h(
        "retile_virtual",
        "DF",
        "h_retile_helper",
        "tiles_virt",
        run="h_retile_helper",
        setup="register_h_downstream",
    ),
    _h(
        "gbx_rst_retile",
        "L3",
        "h_retile_udtf",
        "tiles_mat",
        sql=_RETILE_UDTF,
        setup="register_h_retile_udtf",
    ),
    _h(
        "bng_tessellate_virtual",
        "DF",
        "h_tess_helper",
        "tiles_gb_virt",
        run="h_tess_helper",
        setup="register_h_downstream",
    ),
    _h(
        "gbx_rst_bng_tessellate",
        "L3",
        "h_tess_udtf",
        "tiles_gb_mat",
        sql=_TESS_UDTF,
        setup="register_h_tess_udtf",
    ),
    _h("merge_by_key", "DF", "h_merge_fold", "tiles_grouped_mat", run="h_merge_fold"),
    # The merge inputs overlap (offset copies), and where members overlap the vrt winner is the last
    # path, the fold winner the last bytes: compare the times of fold, vrt and the aggregate, not
    # their pixels. The vrt result is a COG, so its fingerprint differs from fold's anyway.
    _h("merge_by_key", "DF", "h_merge_vrt", "tiles_grouped_virt", run="h_merge_vrt"),
    _h(
        "gbx_rst_merge_agg",
        "L3",
        "h_merge_l3_agg",
        "tiles_grouped_mat",
        sql=_MERGE_L3,
        setup="register_h_merge_l3",
    ),
    _h(
        "combine_by_key",
        "DF",
        "h_combine_avg",
        "tiles_aligned_mat",
        run="h_combine_avg",
    ),
    _h(
        "gbx_rst_combineavg",
        "L3",
        "h_combine_l3",
        "tiles_aligned_mat",
        sql=_COMBINE_SQL,
        setup="register_h_combine_l3",
    ),
    _h("rst_viewshed_towers", "L3", "h_towers_l3", "dsm_towers", run="h_towers_l3"),
    # The JVM comparators (the staged JAR, a dedicated cluster); last, because they replace the light names.
    _h(
        "gbx_rst_merge_agg",
        "L1",
        "h_merge_l1_agg",
        "tiles_grouped_mat",
        sql=_MERGE_L1,
        setup="register_h_heavy",
        computes=("dedicated",),
    ),
    _h(
        "gbx_rst_combineavg",
        "L1",
        "h_combine_l1",
        "tiles_aligned_mat",
        sql=_COMBINE_SQL,
        setup="register_h_heavy",
        computes=("dedicated",),
    ),
    # The towers' viewshed and histogram as heavy steps, and the whole pipeline as one Scala DataFrame step through
    # py4j: neither runs on a Connect session, so both are dedicated only.
    _h(
        "rst_viewshed_towers",
        "L1-steps",
        "h_towers_l1_steps",
        "dsm_towers",
        run="h_towers_l1_steps",
        setup="setup_h_towers_l1_steps",
        computes=("dedicated",),
    ),
    _h(
        "rst_viewshed_towers",
        "JVM-DF",
        "h_towers_jvm_df",
        "dsm_towers",
        run="h_towers_jvm_df",
        setup="setup_h_towers_jvm_df",
        computes=("dedicated",),
    ),
]


# --- G, generators: one form per shape --------------------------------------------------------------
#
# Each query answers (n, fp, n_out) over the OUTPUT rows. The tessellate fingerprint is the summed (reduced)
# hash of the cell id: the same cells give the same fingerprint in every shape at every level. The retile
# fingerprint is the summed length of the encoded pieces, and each level encodes with its own writer (rasterio
# on L3, GDAL with the tile's own options on L1, plain GDAL GTiff on L2), so it agrees between the shapes of ONE
# level and not between levels: across levels compare `n` (the window count) and the oracle rows.
# A shape that is not a product function runs under a `gbx_mvp_*` name that its `setup` registers (outside the
# timed region).

_RT_CELL = (
    128,
    128,
)  # (window width, height): the corpus' small tiles get a handful of windows each
_RT_BIG = (128, 256)  # a 4096 x 4096 tile gets 32 x 16 = 512 windows
_RT_FP = "SELECT count(*) AS n, sum(length({x})) AS fp, count({x}) AS n_out FROM "
_RT_LATERAL = (
    _RT_FP.format(x="t.raster")
    + "{{src}} s, LATERAL {fn}(s.tile, {size}) t"  # the light UDTF: the tile fields
)
_RT_LATERAL_L1 = (
    _RT_FP.format(x="t.tile.raster")
    + "{{src}} s, LATERAL {fn}(s.tile, {size}) t"  # the Catalyst generator: one `tile` column
)
_RT_ARRAY = (
    _RT_FP.format(x="x.raster")
    + "(SELECT explode(gbx_mvp_rst_retile_array(tile, {size})) AS x FROM {{src}})"
)
_RT_PART = (
    _RT_FP.format(x="x.raster")
    + "(SELECT gbx_mvp_rst_retile_part(tile, {size}, i) AS x FROM {{src}} "
    "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count(tile, {size}) - 1)) v AS i)"
)
# The Java UDFs take raster bytes and give bytes back.
_RT_ARRAY_L2 = (
    _RT_FP.format(x="b")
    + "(SELECT explode(gbx_mvp_rst_retile_array_l2(tile.raster, {size})) AS b FROM {{src}})"
)
_RT_PART_L2 = (
    _RT_FP.format(x="b")
    + "(SELECT gbx_mvp_rst_retile_part_l2(tile.raster, {size}, i) AS b FROM {{src}} "
    "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count_l2(tile.raster, {size}) - 1)) v AS i)"
)
# The session TVF over the array form: a SQL body, so no Python between the rows and the plan.
_RT_TVF = (
    _RT_FP.format(x="t.tile.raster")
    + "{{src}} s, LATERAL gbx_mvp_rst_retile_tvf(s.tile, {w}, {h}) t"
)

_TE_FP = "SELECT count(*) AS n, sum(pmod(xxhash64({x}), 2147483647)) AS fp, count({x}) AS n_out FROM "
_TE_LATERAL = _TE_FP.format(x="t.cellid") + "{{src}} s, LATERAL {fn}(s.wkt, 3) t"
_TE_ARRAY = _TE_FP.format(x="c.cellid") + "(SELECT inline({fn}(wkt, 3)) FROM {{src}}) c"
_TE_ARRAY_L2 = (
    _TE_FP.format(x="c.cellid")
    + "(SELECT inline(gbx_mvp_bng_tessellate_l2(wkt, 3, false)) FROM {{src}}) c"
).format()
_TE_UC = (
    _TE_FP.format(x="t.cellid")
    + "{{src}} s, LATERAL {{uc}}.gbx_bng_tessellateexplode(CAST(st_geomfromwkt(s.wkt, 27700) AS GEOMETRY(ANY)), '3') t"
).format()


def setup_g_probes(spark):
    """The light probe forms (array, count+part, Arrow UDTFs)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_probe_forms(spark)


def setup_g_l2(spark):
    """The Java UDF facades under probe names (needs the JAR)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l2_probes(spark)


def setup_g_tvf(spark):
    """The session SQL table function over the array probe; its body calls the probe UDF."""
    from databricks.labs.gbx.manifest import TILE_SQL

    setup_g_probes(spark)
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY FUNCTION gbx_mvp_rst_retile_tvf(tile {TILE_SQL}, w INT, h INT) "
        f"RETURNS TABLE (tile {TILE_SQL}) "
        "RETURN SELECT explode(gbx_mvp_rst_retile_array(tile, w, h)) AS tile"
    )


_G = "databricks.labs.gbx.bench.levels_forms:"
RT, TE, TS = "gbx_rst_retile", "gbx_bng_tessellateexplode", "gbx_bng_tessellate"


def _g(fn, level, variant, inp, sql, *, setup="", register=False, **kw):
    # `register` runs the manager's own registration of `fn` at `level`; a probe shape sets `setup` instead.
    return Form(
        "G",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        setup=_G + setup if setup else "",
        register=register,
        **kw,
    )


def _retile(sql, window, **fmt):
    w, h = window
    return sql.format(size=f"{w}, {h}", w=w, h=h, **fmt)


FORMS["G"] = [
    # retile on the corpus' small tiles
    _g(
        RT,
        "L3",
        "udtf",
        "tiles_mat",
        _retile(_RT_LATERAL, _RT_CELL, fn=RT),
        register=True,
    ),
    _g(
        RT,
        "L3",
        "udtf-arrow",
        "tiles_mat",
        _retile(_RT_LATERAL, _RT_CELL, fn="gbx_rst_retile_mvp_arrow"),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "array_inline",
        "tiles_mat",
        _retile(_RT_ARRAY, _RT_CELL),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "count_part",
        "tiles_mat",
        _retile(_RT_PART, _RT_CELL),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "session-tvf",
        "tiles_mat",
        _retile(_RT_TVF, _RT_CELL),
        setup="setup_g_tvf",
    ),
    _g(
        RT,
        "L1",
        "tvf",
        "tiles_mat",
        _retile(_RT_LATERAL_L1, _RT_CELL, fn=RT),
        register=True,
        computes=("dedicated",),
    ),
    _g(
        RT,
        "L2",
        "array_inline",
        "tiles_mat",
        _retile(_RT_ARRAY_L2, _RT_CELL),
        setup="setup_g_l2",
        computes=("dedicated", "standard"),
    ),
    _g(
        RT,
        "L2",
        "count_part",
        "tiles_mat",
        _retile(_RT_PART_L2, _RT_CELL),
        setup="setup_g_l2",
        computes=("dedicated", "standard"),
    ),
    # the same three light shapes on big tiles (512 windows each): what the shapes cost in memory
    _g(
        RT,
        "L3",
        "udtf-big",
        "tiles_big",
        _retile(_RT_LATERAL, _RT_BIG, fn=RT),
        register=True,
    ),
    _g(
        RT,
        "L3",
        "array_inline-big",
        "tiles_big",
        _retile(_RT_ARRAY, _RT_BIG),
        setup="setup_g_probes",
    ),
    _g(
        RT,
        "L3",
        "count_part-big",
        "tiles_big",
        _retile(_RT_PART, _RT_BIG),
        setup="setup_g_probes",
    ),
    # BNG tessellate on polygons
    _g(TE, "L3", "udtf", "bng_polys", _TE_LATERAL.format(fn=TE), register=True),
    _g(
        TE,
        "L3",
        "udtf-arrow",
        "bng_polys",
        _TE_LATERAL.format(fn="gbx_bng_tessellateexplode_mvp_arrow"),
        setup="setup_g_probes",
    ),
    _g(
        TE,
        "L1",
        "tvf",
        "bng_polys",
        _TE_LATERAL.format(fn=TE),
        register=True,
        computes=("dedicated",),
    ),
    _g(TS, "L3", "array_inline", "bng_polys", _TE_ARRAY.format(fn=TS), register=True),
    _g(
        TS,
        "L1",
        "array_inline",
        "bng_polys",
        _TE_ARRAY.format(fn=TS),
        register=True,
        computes=("dedicated",),
    ),
    _g(
        TS,
        "L2",
        "array_inline",
        "bng_polys",
        _TE_ARRAY_L2,
        setup="setup_g_l2",
        computes=("dedicated", "standard"),
    ),
    _g(TE, "L4", "uc-tvf", "bng_polys", _TE_UC, computes=("serverless", "dedicated")),
    # form (b): the tessellation as one Scala mapPartitions, called over py4j (no SQL function in between)
    Form(
        "G",
        TE,
        "JVM-DF",
        "scala-mapPartitions",
        "bng_polys",
        run=_G + "g_tessellate_jvm",
        computes=("dedicated",),
        register=False,
    ),
]


def g_tessellate_jvm(spark, src):
    """(n, fp, n_out, df) of `BngTessellateJob.transform` over `src` at resolution 3: the cell count, the same
    cellid fingerprint as the SQL forms, and the non-NULL cellid count."""
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F

    job = spark._jvm.com.databricks.labs.gbx.kernels.df.BngTessellateJob
    cells = DataFrame(
        job.transform(spark._jsparkSession, spark.table(src)._jdf, 3), spark
    )
    out = cells.agg(
        F.count(F.lit(1)),
        F.sum(F.pmod(F.xxhash64("cellid"), F.lit(2147483647))),
        F.count("cellid"),
    )
    r = out.collect()[0]
    return r[0], r[1], r[2], out


# --- A, aggregates: one form per shape and input ----------------------------------------------------------
#
# Each query answers (n, fp, n_out) over the groups: the fingerprint is the summed length of the group's raster
# and n_out counts the rasters that are not NULL, so a form that fails on some groups cannot pass on size.
# The light forms come first and the JVM ones last: both register the same `gbx_rst_*` names, and a `setup`
# runs once. The forms of one aggregate do not agree on the bytes of the result (each level encodes with its
# own writer), so across forms compare `n` and the oracle rows. A form that is not a product function runs
# under a `gbx_mvp_*` name that its `setup` registers (outside the timed region). The light forms are all
# `L3`, so the runner records the peak Python worker memory of each: the table function and the finalizes are
# kept or dropped against the grouped aggregate on that column.

_A = "databricks.labs.gbx.bench.levels_forms:"
_MA, _BR = "gbx_rst_merge_agg", "gbx_rst_bng_rasterize_agg"
# xmin, ymin, xmax, ymax, width, height: the 100 km square TQ (where `bng_cells` puts its cells) at 100 m
_A_CANVAS = (500000.0, 100000.0, 600000.0, 200000.0, 1000, 1000)
_PAIRS = "collect_list(struct(cellid, value))"


def _ma_shapes():
    """The SQL of each shape of the merge over `(key, tile)`. A result that is a tile struct gives its raster."""
    return {
        "grouped": f"SELECT key, {_MA}(tile) AS out FROM {{src}} GROUP BY key",
        "udaf": f"SELECT key, {_MA}(tile).raster AS out FROM {{src}} GROUP BY key",
        "finalize": "SELECT key, gbx_rst_merge(collect_list(tile)).raster AS out FROM {src} GROUP BY key",
        "finalize_l2": "SELECT key, gbx_mvp_rst_merge_array_l2(collect_list(tile.raster)) AS out FROM {src} GROUP BY key",
        "tvf": (
            "SELECT key, raster AS out FROM gbx_mvp_rst_merge_tvf("
            "TABLE(SELECT key, tile FROM {src}) PARTITION BY key)"
        ),
    }


def _br_shapes(canvas=_A_CANVAS):
    """The SQL of each shape of the BNG rasterize over `(key, cellid, value)` on `canvas`."""
    types = ("DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "INT", "INT")
    lits = [f"CAST({v} AS {t})" for v, t in zip(canvas, types)]
    on = ", ".join(lits)
    args = f"cellid, value, 27700, NULL, {on}, 'centroids', 0"
    cols = ", ".join(f"{lit} AS c{i + 4}" for i, lit in enumerate(lits))
    finalize = (
        "SELECT key, {fn}(" + _PAIRS + ", " + on + ") AS out FROM {{src}} GROUP BY key"
    )
    return {
        "grouped": f"SELECT key, {_BR}({args}) AS out FROM {{src}} GROUP BY key",
        "udaf": f"SELECT key, {_BR}({args}).raster AS out FROM {{src}} GROUP BY key",
        "finalize": finalize.format(fn="gbx_mvp_rst_bng_rasterize_cells"),
        "finalize_l2": finalize.format(fn="gbx_mvp_rst_bng_rasterize_cells_l2"),
        # partial canvases per (key, salt), then one per-pixel max per key
        "tree": (
            "SELECT key, gbx_mvp_rst_canvas_max(collect_list(p)) AS out FROM ("
            f"SELECT key, gbx_mvp_rst_bng_rasterize_cells({_PAIRS}, {on}) AS p FROM ("
            "SELECT *, pmod(xxhash64(cellid, value), 2) AS salt FROM {src}) GROUP BY key, salt) GROUP BY key"
        ),
        "tvf": (
            "SELECT key, raster AS out FROM gbx_mvp_rst_bng_rasterize_tvf("
            f"TABLE(SELECT key, cellid, value, {cols} FROM {{src}}) PARTITION BY key)"
        ),
    }


def setup_a_probes(spark):
    """The light probes (finalizes, tree step, table functions) and the light `gbx_rst_merge` finalize."""
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx import functions as prx

    _mvp_forms.register_probe_forms(spark)
    prx.register(spark, only=["gbx_rst_merge"])


def setup_a_l2(spark):
    """The Java UDF finalizes under probe names (needs the JAR)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l2_probes(spark)


def setup_a_udaf(spark):
    """The Catalyst aggregates under their product names (needs the JAR; classic only)."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l1_udafs(spark)


def setup_a_l1_merge(spark):
    """The Catalyst `gbx_rst_merge` over an array of tiles (needs the JAR; replaces the light finalize)."""
    inst = spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    got = str(
        inst.install(
            spark._jsparkSession,
            "gbx_rst_merge",
            "com.databricks.labs.gbx.rasterx.expressions.RST_Merge",
        )
    )
    if got != "ok":
        raise RuntimeError(f"gbx_rst_merge: L1 install gave {got}")


def _a(fn, level, variant, inp, inner, *, setup="", register=False, **kw):
    sql = f"SELECT count(*) AS n, sum(length(out)) AS fp, count(out) AS n_out FROM ({inner})"
    return Form(
        "A",
        fn,
        level,
        variant,
        inp,
        sql=sql,
        setup=_A + setup if setup else "",
        register=register,
        **kw,
    )


def _a_light(fn, shapes, inputs):
    """The light forms of one aggregate over each of its inputs. An input's suffix (`-skew`) labels its forms,
    since a result row carries the form and not the input."""
    out = []
    for inp, sfx in inputs:
        out += [
            _a(fn, "L3", "c0" + sfx, inp, shapes["grouped"], register=True),
            _a(
                fn,
                "L3",
                "finalize" + sfx,
                inp,
                shapes["finalize"],
                setup="setup_a_probes",
            ),
        ]
        if "tree" in shapes:
            out.append(
                _a(fn, "L3", "tree" + sfx, inp, shapes["tree"], setup="setup_a_probes")
            )
        out.append(
            _a(fn, "L3", "tvf" + sfx, inp, shapes["tvf"], setup="setup_a_probes")
        )
    return out


def _a_jvm(fn, shapes, inputs):
    both = ("dedicated", "standard")
    out = []
    for inp, sfx in inputs:
        out += [
            _a(
                fn,
                "L2",
                "finalize" + sfx,
                inp,
                shapes["finalize_l2"],
                setup="setup_a_l2",
                computes=both,
            ),
            _a(
                fn,
                "L1",
                "udaf" + sfx,
                inp,
                shapes["udaf"],
                setup="setup_a_udaf",
                computes=("dedicated",),
            ),
        ]
        if (
            fn == _MA
        ):  # the Catalyst finalize over an array of tiles; the rasterize has none
            out.append(
                _a(
                    fn,
                    "L1",
                    "finalize" + sfx,
                    inp,
                    shapes["finalize"],
                    setup="setup_a_l1_merge",
                    computes=("dedicated",),
                )
            )
    return out


_A_MERGE = (_MA, _ma_shapes(), (("tiles_grouped", ""), ("tiles_grouped_skew", "-skew")))
_A_RASTERIZE = (_BR, _br_shapes(), (("bng_cells", ""), ("bng_cells_skew", "-skew")))
FORMS["A"] = (
    _a_light(*_A_MERGE)
    + _a_light(*_A_RASTERIZE)
    + _a_jvm(*_A_MERGE)
    + _a_jvm(*_A_RASTERIZE)
)


# --- SE, the GDAL spec engine: each of its functions on the engine and on its own implementation --------------------
#
# Inputs: DEM tiles (one band, Float32, EPSG:3857) of 256, 512 and 1024 pixels for slope and the floor; pairs of
# them for map algebra; the corpus tiles for transformcrs (as the S forms). The floor is the engine running
# `raster select --band=1`: the per-row cost of the engine itself (the /vsimem write, the header read, the
# algorithm, one GTiff encode), with next to no raster work. The forms of today's implementations go under private
# names, so a later engine registration never measures them by mistake.

_SE_SIZES = (256, 512, 1024)
BASE_ROWS.update(
    {"se_dem_256": 64, "se_dem_512": 32, "se_dem_1024": 16, "se_pair_512": 32}
)


def _se_dem_tile(size: int, seed: int) -> dict:
    import numpy as np

    from databricks.labs.gbx.bench import oracles

    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:size, 0:size].astype("float64")
    z = (
        100.0
        + 0.5 * xx
        + 0.3 * yy
        + 10.0 * np.sin(xx / (7.0 + seed)) * np.cos(yy / 5.0)
        + rng.normal(0.0, 0.1, (size, size))
    )
    return oracles._tile(oracles._gtiff(z[np.newaxis], "EPSG:3857"))


def _se_dems(size: int):
    def build(spark, n, corpus, schema):
        from databricks.labs.gbx.manifest import TILE_SQL

        tiles = [_se_dem_tile(size, s) for s in range(4)]  # four distinct tiles, cycled
        check_estimate(f"se_dem_{size} x {n}", n, size_of(tiles[0]))
        return _table(
            spark,
            schema,
            f"mvp_in_se_dem{size}_v{RECIPE_VERSION}_{n}",
            lambda: client_dataframe(
                spark,
                [(tiles[i % 4],) for i in range(n)],
                f"tile {TILE_SQL}",
                what=f"se_dem_{size} tiles",
            ).repartition(max(1, min(n, 8))),
        )

    return build


def _se_pairs(size: int):
    def build(spark, n, corpus, schema):
        from databricks.labs.gbx.manifest import TILE_SQL

        tiles = [_se_dem_tile(size, s) for s in range(4)]
        check_estimate(f"se_pair_{size} x {n}", n, 2 * size_of(tiles[0]))
        return _table(
            spark,
            schema,
            f"mvp_in_se_pair{size}_v{RECIPE_VERSION}_{n}",
            lambda: client_dataframe(
                spark,
                [([tiles[i % 4], tiles[(i + 1) % 4]],) for i in range(n)],
                f"tiles array<{TILE_SQL}>",
                what=f"se_pair_{size} tile pairs",
            ).repartition(max(1, min(n, 8))),
        )

    return build


INPUTS.update({f"se_dem_{s}": _se_dems(s) for s in _SE_SIZES})
INPUTS["se_pair_512"] = _se_pairs(512)


def _se_floor_function():
    """The floor's spec: `raster select --band=1` on slope's signature (a bench spec, not a SQL function)."""
    import dataclasses

    from databricks.labs.gbx import manifest

    return dataclasses.replace(
        manifest.load().functions["gbx_rst_slope"],
        sql_name="gbx_bench_engine_floor",
        gdal=manifest.parse_gdal_spec(
            {
                "alg": "raster select",
                "in": {"tile": "raster"},
                "out": {"kind": "raster", "dtype": "input"},
                "write": "tile",
                "args": [{"arg": "band", "const": 1}],
            }
        ),
    )


SE_FLOOR = _se_floor_function()
_SE_FLOOR_REF = "databricks.labs.gbx.bench.levels_forms:SE_FLOOR"


def _se_bench_section():
    """The manifest's allowlist plus the floor's `raster select`: a bench-only extension, never the product's."""
    import dataclasses
    from types import MappingProxyType

    from databricks.labs.gbx import manifest

    section = manifest.load().gdal
    return dataclasses.replace(
        section, allow=MappingProxyType({**section.allow, "raster select": ("band",)})
    )


SE_SECTION = _se_bench_section()
_SE_SECTION_REF = "databricks.labs.gbx.bench.levels_forms:SE_SECTION"


def setup_se_floor(spark):
    """The floor's row-wise and batch UDFs under their own names."""
    from databricks.labs.gbx.engine import l3 as engine_l3

    spark.udf.register(
        "gbx_bench_floor_c0",
        engine_l3.c0(
            "gbx_bench_engine_floor", (), _SE_FLOOR_REF, section_ref=_SE_SECTION_REF
        ),
    )
    spark.udf.register(
        "gbx_bench_floor_c1",
        engine_l3.c1(
            "gbx_bench_engine_floor", (), _SE_FLOOR_REF, section_ref=_SE_SECTION_REF
        ),
    )


def setup_se_slope_today(spark):
    """Today's slope: the Catalyst RST_Slope under its private name, the light numpy UDF under a bench name."""
    from databricks.labs.gbx.manager import jvm
    from databricks.labs.gbx.pyrx import functions as prx

    spark.udf.register("gbx_bench_slope_l3_today", prx._slope_udf)
    why = jvm.install_private_l1(
        spark,
        "com.databricks.labs.gbx.rasterx.expressions.dem.RST_Slope",
        "gbx__l1_rst_slope",
    )
    if why:
        raise RuntimeError(why)


def setup_se_slope_light_today(spark):
    from databricks.labs.gbx.pyrx import functions as prx

    spark.udf.register("gbx_bench_slope_l3_today", prx._slope_udf)


_SE_FP = (
    "SELECT count(*) AS n, sum(length(t.raster)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {call} AS t FROM {{src}})"
)
_SE_SLOPE = _SE_FP.format(
    call="{fn}(tile, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))"
)
_SE_SLOPE_L1_TODAY = _SE_FP.format(call="gbx__l1_rst_slope(tile, 'degrees')")
_SE_SLOPE_L3_TODAY = _SE_FP.format(
    call="gbx_bench_slope_l3_today(tile, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))"
)
_SE_FLOOR_C0 = _SE_FP.format(call="gbx_bench_floor_c0(tile)")
_SE_FLOOR_C1 = (
    "SELECT count(*) AS n, sum(length(r.raster)) AS fp, count(r.raster) AS n_out FROM "
    "(SELECT gbx_bench_floor_c1(tile.raster, 'degrees', CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)) AS r "
    "FROM {src})"
)
_SE_TCRS = _SE_FP.format(call="{fn}(tile, 'EPSG:3857')")
# The call is inserted as it is; the run formats the query with {src}, so the JSON braces are doubled once.
_SE_MA_TODAY = _SE_FP.format(call="""{fn}(tiles, '{{"calc": "(A - B) / (A + B)"}}')""")
_SE_MA_ENGINE = _SE_FP.format(
    call="""{fn}(tiles, '{{"calc": "norm_diff", "dialect": "builtin"}}')"""
)


def _se(fn, level, variant, inp, sql, *, setup="", register=True, **kw):
    return Form(
        "SE",
        fn,
        level,
        variant,
        inp,
        sql.replace("{fn}", fn),
        setup=("databricks.labs.gbx.bench.levels_forms:" + setup) if setup else "",
        register=register,
        **kw,
    )


_SL, _MALG = "gbx_rst_slope", "gbx_rst_mapalgebra"
_DED = ("dedicated",)
# Today's forms first: they live under private names that the engine registrations after them do not replace.
# A label `<variant>@<size>` registers `<variant>` (see levels._run_one) and names the tile size.
FORMS["SE"] = (
    [
        _se(
            _SL,
            "L1",
            f"today@{s}",
            f"se_dem_{s}",
            _SE_SLOPE_L1_TODAY,
            setup="setup_se_slope_today",
            register=False,
            computes=_DED,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"today@{s}",
            f"se_dem_{s}",
            _SE_SLOPE_L3_TODAY,
            setup="setup_se_slope_light_today",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"floor-c0@{s}",
            f"se_dem_{s}",
            _SE_FLOOR_C0,
            setup="setup_se_floor",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(
            _SL,
            "L3",
            f"floor-c1@{s}",
            f"se_dem_{s}",
            _SE_FLOOR_C1,
            setup="setup_se_floor",
            register=False,
        )
        for s in _SE_SIZES
    ]
    + [
        _se(_SL, "L1", f"engine@{s}", f"se_dem_{s}", _SE_SLOPE, computes=_DED)
        for s in _SE_SIZES
    ]
    + [
        _se(_SL, "L3", f"{v}@{s}", f"se_dem_{s}", _SE_SLOPE)
        for v in ("engine-c0", "engine-c1")
        for s in _SE_SIZES
    ]
    + [
        _se(T, "L1", "-", "tiles_mat", _SE_TCRS, computes=_DED),
        _se(T, "L2", "-", "tiles_mat", _SE_TCRS, computes=("dedicated", "standard")),
        _se(T, "L3", "-", "tiles_mat", _SE_TCRS),
        _se(T, "L1", "engine", "tiles_mat", _SE_TCRS, computes=_DED),
        _se(T, "L3", "engine-c0", "tiles_mat", _SE_TCRS),
        _se(T, "L3", "engine-c1", "tiles_mat", _SE_TCRS),
        _se(T, "L1", "engine", "tiles_virt", _SE_TCRS, computes=_DED),
        _se(T, "L3", "engine-c1", "tiles_virt", _SE_TCRS),
        _se(_MALG, "L1", "-", "se_pair_512", _SE_MA_TODAY, computes=_DED),
        _se(_MALG, "L3", "-", "se_pair_512", _SE_MA_TODAY),
        _se(_MALG, "L1", "engine", "se_pair_512", _SE_MA_ENGINE, computes=_DED),
        _se(_MALG, "L3", "engine-c0", "se_pair_512", _SE_MA_ENGINE),
        _se(_MALG, "L3", "engine-c1", "se_pair_512", _SE_MA_ENGINE),
    ]
)


# A form that only sleeps, for proving the launcher's watchdog (``--inject-stall`` of the cluster launcher). It
# is not in any set: ``levels.run_mvp(inject_stall=...)`` adds it after the first form.
INPUTS["none"] = lambda spark, n, corpus, schema: "none"
BASE_ROWS["none"] = 1
STALL_SECONDS = 0.0


def stall_form(spark, src):
    """Sleep on the driver for STALL_SECONDS: a form with no Spark job, so a run that sits in it has no tasks."""
    import time

    time.sleep(STALL_SECONDS)
    return 1, 1, 1


STALL_FORM = Form(
    "S",
    "inject_stall",
    "L3",
    "sleep",
    "none",
    run="databricks.labs.gbx.bench.levels_forms:stall_form",
    register=False,
)


# The inputs a builder makes on the CLIENT (the rest are read or generated on the executors), as (tile pixels,
# tiles per row): the preflight refuses a ladder whose client-built bytes pass the cap before anything is submitted.
CLIENT_TILES = {
    "se_dem_256": (256, 1),
    "se_dem_512": (512, 1),
    "se_dem_1024": (1024, 1),
    "se_pair_512": (512, 2),
}


def client_bytes(input_key: str, rows: int) -> int:
    """The bytes the builder of `input_key` ships from the client for `rows` rows (0 for an executor-side input)."""
    if input_key not in CLIENT_TILES:
        return 0
    size, per_row = CLIENT_TILES[input_key]
    return rows * per_row * size_of(_se_dem_tile(size, 0))
