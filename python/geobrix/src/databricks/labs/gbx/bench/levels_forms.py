"""Forms + inputs per MVP. Each MVP task appends its forms; inputs are cached Delta tables.

An input builder is ``f(spark, n, corpus, schema) -> table name`` (None when the input cannot
be built here). With ``schema`` it is a Delta table, created once and reused by every form and
run; without one, a temp view. The seeds are fixed, so a form sees the same rows every time.
"""

from __future__ import annotations

import os
from typing import Callable, Dict, List, Optional

from databricks.labs.gbx.bench.levels import Form

# Bump when an input recipe (a seed or a shape) changes: it is part of the cached table's name,
# so a stale table from an older recipe is never reused.
RECIPE_VERSION = 1
BASE_ROWS = {
    "bng_points": 1_000_000,
    "invalid_polys": 250_000,
    "tiles_mat": 250,
    "tiles_virt": 250,
    # MVP-H: groups of tiles (merge: two offset copies of a corpus tile, combine: two aligned copies)
    # and the corpus tiles on the British National Grid (the others have no BNG cells).
    "tiles_grouped_mat": 250,
    "tiles_grouped_virt": 250,
    "tiles_aligned_mat": 250,
    "tiles_gb_mat": 250,
    "tiles_gb_virt": 250,
    # MVP-G: BNG polygons that each cover a handful of cells, and a few big tiles (4096 x 4096)
    "bng_polys": 100_000,
    "tiles_big": 4,
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


def _tiles(virtual: bool, subdir: str = "rows", name: Optional[str] = None):
    """The corpus tiles under `subdir` as a one-column `tile` table (`name` labels the cached table)."""

    def build(spark, n, corpus, schema):
        rows = corpus.rstrip("/") + "/" + subdir
        if not os.path.isdir(rows):
            return None
        from databricks.labs.gbx.ds.register import register

        register(spark)  # the light raster reader
        return _table(
            spark,
            schema,
            f"mvp_in_tiles_{name or ('virt' if virtual else 'mat')}_v{RECIPE_VERSION}_{n}",
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
        df = spark.createDataFrame(rows, f"key string, tile {TILE_SQL}")
    else:
        from databricks.labs.gbx.pyrx import functions as prx

        df = spark.createDataFrame(pairs, "key string, path string").select(
            "key", prx.rst_fromfile(F.col("path"), "GTiff", True).alias("tile")
        )
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
        import rasterio

        gb = []
        for p in sources:
            with rasterio.open(p) as ds:
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
    "(SELECT {fn}(st_geomfromwkt(wkt, 4326)) AS g FROM {src})"
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

    def floor(
        e, n, res
    ):  # no annotations: this module's are strings the UDF cannot resolve
        return pd.Series(const, index=e.index)

    spark.udf.register(
        "gbx_bng_floor", F.pandas_udf(floor, StringType(), F.PandasUDFType.SCALAR)
    )


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


# --- MVP-H: the DataFrame helpers against the per-row UDTFs and the grouped aggregates -----------
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
]


# --- MVP-G: generators, one form per shape ----------------------------------------------------------
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
# The session TVF over the array form (E-K2): a SQL body, so no Python between the rows and the plan.
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
    + "{{src}} s, LATERAL {{uc}}.gbx_bng_tessellateexplode(st_geomfromwkt(s.wkt, 27700), '3') t"
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
    """The session SQL table function over the array probe (E-K2); its body calls the probe UDF."""
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
]
