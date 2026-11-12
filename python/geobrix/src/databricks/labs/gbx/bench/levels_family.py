"""One set of forms per function family of the manifest (`--levels <family>`, `--set <family>`).

A family set times one or two representative functions of the family at every level the manifest gives them (L1, the L2
the family keeps, and each L3 variant) beside a floor form, over ONE input that every form reads.

Comparable by construction: the forms of a set share one input of exactly ``n`` rows (a cached table on a cluster; on a
local run the tile input applies ``limit(n)`` -> ``_row`` -> ``repartitionByRange(family_partitions(spark, n), "_row")``
before the work, so every form gets the same rows and the same partitioning). The tile inputs refuse a scale the corpus
cannot supply (`levels_forms.corpus_guard`).

The floor is the function's own shell without its work: an Arrow UDF with the same input columns and a constant body of
the same type (`setup_floors`), or, where the output type has no constant (a struct of arrays), the scan of the same
input. Its cost is the part of every other form that no kernel can win back.

`web-tiles` has no set yet: `gbx_rst_tilexyz` needs, per tile, the z/x/y that covers it, and a fixed z/x/y either renders
nothing (a tile outside the raster) or warps a corpus tile to the whole world's tile (the world tile did not finish in
two minutes on one corpus tile). `WEB_TILES_PENDING` names it for the tests.

Families that the earlier sets already cover with real levels (`REUSED`) take those forms under the family key.
"""

from __future__ import annotations

import dataclasses
import os
import warnings
from typing import Dict, List, Optional, Tuple

from databricks.labs.gbx.bench.levels import Form

_ME = "databricks.labs.gbx.bench.levels_family:"

FAMILY_ROWS_PER_PARTITION = 2  # tiles a partition holds, at most: a UDF holds its partition's tiles in one Arrow batch, with a few copies


def family_partitions(spark, n) -> int:
    """The partition count every form of a tile family reads its `n` tiles with: the compute's slots, but never fewer
    than `ceil(n / FAMILY_ROWS_PER_PARTITION)` (8 materialized 16 MiB tiles per task, with the copies a Python UDF
    makes of them, OOM-killed a local JVM) and never more than `n`. A function of `n` and the compute alone.
    """
    from databricks.labs.gbx.bench.runner import _bench_parallelism

    n = int(n)
    return max(
        1, min(n, max(_bench_parallelism(spark), -(-n // FAMILY_ROWS_PER_PARTITION)))
    )


def spread_evenly(df, partitions, order_by):
    """``df`` in ``partitions`` partitions of whole, equal share (they differ by at most one row).

    ``repartition(k)`` deals the rows round robin from a random start in EACH input partition, so it is even only when
    the rows sit in one or two input partitions (a local session: `createDataFrame` makes `defaultParallelism` of them).
    On a cluster with many slots the n path rows are n one-row input partitions, each dealt to a place of its own, and
    some tasks got three tiles where the cap is two. A row number over the whole (tiny) input, cut by range, gives each
    partition its share on any compute. `order_by` names the columns that order the rows (the numbering only needs to
    be unique, not stable).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    k = int(partitions)
    if k <= 1:
        return df
    numbered = df.withColumn("_gbx_row", F.row_number().over(Window.orderBy(*order_by)))
    return numbered.repartitionByRange(k, "_gbx_row").drop("_gbx_row")


# SQL shapes. Each answers one row (n, fp, n_out): the input rows processed, an order-independent fingerprint of the
# output, and the rows with a non-NULL output (a NULL output is an error unless the form allows it).
_SCALAR = (
    "SELECT count(*) AS n, sum(pmod(xxhash64(c), 2147483647)) AS fp, count(c) AS n_out "
    "FROM (SELECT {call} AS c FROM {{src}})"
)
_TILE = (
    "SELECT count(*) AS n, sum(length(t.raster)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {call} AS t FROM {{src}})"
)
# an aggregate: n is the INPUT rows (a scalar subquery, the same scan for every form: a grouped pandas UDF cannot sit
# beside count(*) in one aggregate), and n_out is n unless a group came out NULL
_AGG = (
    "SELECT (SELECT count(*) FROM {{src}}) AS n, sum(length(x)) AS fp, "
    "CASE WHEN count(x) = count(*) THEN (SELECT count(*) FROM {{src}}) ELSE count(x) END AS n_out "
    "FROM (SELECT {call} AS x FROM {{src}} GROUP BY key)"
)
# The value-level twins (`value_fp`): the same call and input, `fp` the sum of a hash of the DECODED output. A tile output is
# decoded for a few tiles only (the decode costs more than the work); a scalar or an aggregate (few outputs) for all of them.
_VFP_TILE_ROWS = 64
_SCALAR_V = (
    "SELECT count(*) AS n, sum(pmod({udf}(c), 2147483647)) AS fp, count(c) AS n_out "
    "FROM (SELECT {call} AS c FROM {{src}})"
)
# The few tiles are the first `_VFP_TILE_ROWS` BY A KEY OF THE INPUT, not of the scan: an unordered LIMIT takes whichever
# rows the scan meets first (the file order of the table, the task that finishes first), so two levels or two runs hashed
# different tiles and differed in value while every output was equal. The key is a function of the row's own content.
# The rows after the LIMIT sit in one partition, so they are dealt over `_VFP_TILE_ROWS` tasks before the call: the call
# and the decode of 64 tiles in one task took over 6 min for a light form on a cluster. The fingerprint is a sum, so the
# spread does not change it.
_TILE_V = (
    "SELECT count(*) AS n, sum(pmod({udf}(t.raster), 2147483647)) AS fp, count(t.raster) AS n_out "
    "FROM (SELECT {call} AS t FROM (SELECT /*+ REPARTITION("
    + str(_VFP_TILE_ROWS)
    + ") */ * FROM (SELECT * FROM {{src}} ORDER BY {order} LIMIT "
    + str(_VFP_TILE_ROWS)
    + ")))"
)
# the ordering key of each input a tile-producing representative reads (a tile's path, then a hash of its bytes: a
# virtual tile has no bytes, a path-less one has no path)
_VFP_ORDER = {
    "tiles_n": "tile.path, xxhash64(tile.raster)",
    "points_arrays": "xxhash64(x, y, z)",
    "points_wkt": "xxhash64(points, vals)",
    "file_bytes": "xxhash64(content)",
}
_AGG_V = (
    "SELECT (SELECT count(*) FROM {{src}}) AS n, sum(pmod({udf}(x), 2147483647)) AS fp, "
    "CASE WHEN count(x) = count(*) THEN (SELECT count(*) FROM {{src}}) ELSE count(x) END AS n_out "
    "FROM (SELECT {call} AS x FROM {{src}} GROUP BY key)"
)
_VFP_SHAPE = {id(_SCALAR): _SCALAR_V, id(_TILE): _TILE_V, id(_AGG): _AGG_V}
_AGG_FLOOR = _AGG.format(call="CAST(max(length({col})) AS STRING)")
_TILE_SCAN = "SELECT count(*) AS n, sum(length(tile.raster)) AS fp, count(tile) AS n_out FROM {src}"
# a generator: n is the rows it was given, fp the rows it made
_GEN = (
    "SELECT (SELECT count(*) FROM {{src}}) AS n, count(*) AS fp, (SELECT count(*) FROM {{src}}) AS n_out "
    "FROM {{src}} s, LATERAL {call} t"
)

_FLOOR_VALUE = {
    "int": 1,
    "bigint": 1,
    "double": 1.0,
    "string": "x",
    "binary": b"x",
    "boolean": True,
    "array<double>": [1.0],
    "array<string>": ["x"],
}


@dataclasses.dataclass(frozen=True)
class Rep:
    family: str
    fn: str
    inp: str
    shape: str  # one of _SCALAR, _TILE, _AGG, _GEN with {call} filled in by `call`
    call: str  # the function call, `{fn}` standing for the SQL name
    floor_ret: Optional[str] = (
        None  # DDL of the constant a floor UDF returns; "tile": the input tile back; None: scan
    )
    floor_sql: Optional[str] = None  # the scan floor's query when there is no UDF floor
    computes_l3: Tuple[str, ...] = ("dedicated", "standard", "serverless")
    allows_null: bool = False
    l1_virtual: bool = False
    # the kind of output whose decoded value the row's fingerprint hashes (`value_fp.KINDS`); None: the length/hash one
    vfp: Optional[str] = None


WEB_TILES_PENDING = "web-tiles"

_BNG_TILE = "tiles_n"

REPS: List[Rep] = [
    # accessors: a header read, then a column read
    Rep("raster-accessor", "gbx_rst_width", _BNG_TILE, _SCALAR, "{fn}(tile)", "int"),
    Rep(
        "raster-accessor",
        "gbx_rst_boundingbox",
        _BNG_TILE,
        _SCALAR,
        "{fn}(tile)",
        "binary",
        vfp="wkb",
    ),
    # pixel statistics: a full decode
    Rep(
        "raster-stat",
        "gbx_rst_avg",
        _BNG_TILE,
        _SCALAR,
        "{fn}(tile)",
        "array<double>",
        vfp="doubles",
    ),
    # tile to tile
    Rep(
        "raster-tile-op",
        "gbx_rst_align_to",
        _BNG_TILE,
        _TILE,
        "{fn}(tile, tile)",
        "tile",
        vfp="raster",
    ),
    Rep(
        "raster-tile-op",
        "gbx_rst_proximity",
        _BNG_TILE,
        _TILE,
        "{fn}(tile)",
        "tile",
        vfp="raster",
    ),
    Rep(
        "raster-file-op",
        "gbx_rst_cog_convert",
        _BNG_TILE,
        _TILE,
        "{fn}(tile, 'DEFLATE')",
        "tile",
        vfp="raster",
    ),
    # tile to vectors: isoband breaks span the corpus' value range (contour is a GDAL spec engine row: the SE-contour set)
    Rep(
        "raster-vectorize",
        "gbx_rst_isoband",
        _BNG_TILE,
        _SCALAR,
        "{fn}(tile, array(0D, 50D, 100D, 1000D))",
        floor_sql=_TILE_SCAN,
    ),
    # a tile for the world's zoom 0: the whole corpus tile falls inside it
    # arrays of points to a grid
    Rep(
        "raster-rasterize",
        "gbx_rst_binpoints",
        "points_arrays",
        _TILE,
        "{fn}(x, y, z, 0D, 0D, 100D, 100D, 64, 64, 4326, 'mean')",
        floor_sql="SELECT count(*) AS n, sum(size(x)) AS fp, count(x) AS n_out FROM {src}",
        vfp="raster",
    ),
    # inverse-distance weighting of a row's WKT points onto a grid: the power and neighbour count given outright. The function
    # has no JVM level (it measured 1.78x and 1.91x slower than light at 4x and 16x rows), so the forms are light and the floor.
    Rep(
        "raster-rasterize",
        "gbx_rst_gridfrompoints",
        "points_wkt",
        _TILE,
        "{fn}(slice(points, 1, size(points)), vals, 0D, 0D, 100D, 100D, 64, 64, 4326, 2.0D, 12)",
        floor_sql="SELECT count(*) AS n, sum(size(points)) AS fp, count(points) AS n_out FROM {src}",
        vfp="raster",
    ),
    # file bytes to a tile
    Rep(
        "raster-constructor",
        "gbx_rst_fromcontent",
        "file_bytes",
        _TILE,
        "{fn}(content, 'GTiff')",
        floor_sql="SELECT count(*) AS n, sum(length(content)) AS fp, count(content) AS n_out FROM {src}",
        vfp="raster",
    ),
    # BNG cells
    Rep(
        "cell-aggregate",
        "gbx_bng_cellfill",
        "bng_cells",
        _AGG,
        "{fn}(cellid, value, 1, 'mean', 2.0)",
        floor_sql=_AGG_FLOOR.format(col="cellid", src="{src}"),
    ),
    Rep(
        "grid-array",
        "gbx_bng_kring",
        "bng_cells",
        _SCALAR,
        "{fn}(cellid, 2)",
        "array<string>",
    ),
    # the BNG rasterize aggregate on the aggregate family's canvas (n is the input rows, not the groups)
    Rep(
        "raster-aggregate",
        "gbx_rst_bng_rasterize_agg",
        "bng_cells",
        _AGG,
        "{fn}(cellid, value, 27700, NULL, CAST(500000.0 AS DOUBLE), CAST(100000.0 AS DOUBLE), "
        "CAST(600000.0 AS DOUBLE), CAST(200000.0 AS DOUBLE), CAST(200 AS INT), CAST(200 AS INT), 'centroids', 0)",
        floor_sql=_AGG_FLOOR.format(col="cellid", src="{src}"),
        vfp="raster",
    ),
    # vectors
    Rep(
        "vector-scalar",
        "gbx_st_transformcrs",
        "invalid_polys",
        _SCALAR,
        "{fn}(wkt, 'EPSG:3857', 'EPSG:27700')",
        "binary",
        vfp="wkb",
    ),
    Rep(
        "vector-aggregate",
        "gbx_st_asmvt",
        "mvt_rows",
        _AGG,
        "{fn}(wkb, struct(id AS id), 'bench')",
        floor_sql=_AGG_FLOOR.format(col="wkb", src="{src}"),
        vfp="mvt",
    ),
    Rep(
        "vector-generator",
        "gbx_st_triangulate",
        "tin_points",
        _GEN,
        "{fn}(points, CAST(NULL AS ARRAY<STRING>), 0D, 0D, 'NONENCROACHING')",
        floor_sql="SELECT count(*) AS n, sum(size(points)) AS fp, count(points) AS n_out FROM {src}",
    ),
]

# Families whose real levels the earlier sets already time: (family -> [(set, predicate on the form)]).
# The forms keep their names; the family key only re-keys them, and each set gets a scan floor.
_B, _T, _M = "gbx_bng_eastnorthasbng", "gbx_rst_transformcrs", "gbx_st_makevalid"
_REAL_VARIANTS = {
    "udtf", "tvf", "scala-mapPartitions", "uc-tvf", "array_inline", "uc-scalar",
    "h_retile_udtf", "h_tess_udtf", "h_merge_l3_agg", "h_combine_l3", "h_merge_fold", "h_merge_vrt",
    "h_combine_avg",
}  # fmt: skip


def _is_real(f: Form, fns: Tuple[str, ...], extra: Tuple[str, ...] = ()) -> bool:
    return (
        f.fn in fns
        and (
            f.level in ("L1", "L2") or f.variant in _REAL_VARIANTS or f.variant in extra
        )
        and not f.memory_probe
    )


def _reused(lf) -> Dict[str, List[Form]]:
    S, G, H, A = lf.FORMS["S"], lf.FORMS["G"], lf.FORMS["H"], lf.FORMS["A"]

    def pick(forms, fns, extra=()):
        return [f for f in forms if _is_real(f, fns, extra)]

    return {
        "grid-scalar": [f for f in S if f.fn == _B],
        "grid-generator": pick(G, ("gbx_bng_tessellateexplode",)),
        "grid-array": pick(G, ("gbx_bng_tessellate",)),
        "raster-generator": pick(G, ("gbx_rst_retile",))
        + pick(H, ("gbx_rst_retile", "gbx_rst_bng_tessellate")),
        "raster-aggregate": [f for f in A if f.level in ("L1", "L2", "L3")]
        + [f for f in H if f.fn in ("merge_by_key", "gbx_rst_merge_agg")],
        "raster-array-op": [
            f for f in H if f.fn in ("gbx_rst_combineavg", "combine_by_key")
        ],
        "raster-tile-op": [f for f in S if f.fn == _T],
        "vector-scalar": [f for f in S if f.fn == _M],
    }


# floors for reused forms: (fn, input, scan query) per family, the scan of the form's own input (what the shell reads
# before any work). grid-scalar already carries its Arrow floor.
_WKT_SCAN = (
    "SELECT count(*) AS n, sum(length(wkt)) AS fp, count(wkt) AS n_out FROM {src}"
)
# the scan of a virtual-tile input: a virtual tile holds no bytes (`raster` is NULL), only the path it points at
_VIRT_SCAN = "SELECT count(*) AS n, sum(length(tile.path)) AS fp, count(tile) AS n_out FROM {src}"
_REUSED_FLOORS = {
    "grid-generator": [("gbx_bng_tessellateexplode", "bng_polys", _WKT_SCAN)],
    "grid-array": [("gbx_bng_tessellate", "bng_polys", _WKT_SCAN)],
    "raster-generator": [
        ("gbx_rst_retile", "tiles_n", _TILE_SCAN),
        ("gbx_rst_bng_tessellate", "tiles_gb_mat", _TILE_SCAN),
    ],
    "raster-aggregate": [
        ("gbx_rst_merge_agg", "tiles_grouped_family", _TILE_SCAN),
    ],
    "raster-array-op": [("gbx_rst_combineavg", "tiles_aligned_mat", _TILE_SCAN)],
    "raster-tile-op": [("gbx_rst_transformcrs", "tiles_n", _TILE_SCAN)],
    "vector-scalar": [("gbx_st_makevalid", "invalid_polys", _WKT_SCAN)],
}


# --- inputs ------------------------------------------------------------------------------------------------------


def _tiles_n(spark, n, corpus, schema):
    """The corpus' first `n` tiles as one `tile` column of `n` rows over `family_partitions` partitions (see the module doc)."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx.bench import levels_forms as lf

    rows = corpus.rstrip("/") + "/rows"
    if not os.path.isdir(rows):
        return None
    gbx_manager.register_sources(spark)
    held = lf._corpus_rows(corpus) or []
    lf.corpus_guard("tiles", rows, len(held), n)
    only = lf._only_files_regex(held[:n])

    def build():
        return (
            spark.read.format("raster_gbx")
            .option("virtualTiles", "false")
            .option("filterRegex", only)
            .load(rows)
            .select("tile")
            .limit(n)
            .withColumn("_row", F.monotonically_increasing_id())
            .repartitionByRange(family_partitions(spark, n), "_row")
            .select("tile")
        )

    return lf._table(spark, schema, f"mvp_in_tiles_n_v{lf.RECIPE_VERSION}_{n}", build)


def _points_arrays(spark, n, corpus, schema):
    """`n` rows of 400 random points each (x, y in 0..100, z in 0..10), as three arrays."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    return lf._table(
        spark,
        schema,
        f"mvp_in_points_arrays_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            F.expr("transform(sequence(1, 400), i -> rand(21) * 100)").alias("x"),
            F.expr("transform(sequence(1, 400), i -> rand(22) * 100)").alias("y"),
            F.expr("transform(sequence(1, 400), i -> rand(23) * 10)").alias("z"),
        ),
        rows=n,
    )


_WKT_POINTS = 200  # points of one row of the IDW input


def _points_wkt(spark, n, corpus, schema):
    """`n` rows of 200 random WKT points (x, y in 0..100) and a value for each (0..10), as `points` and `vals`."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    return lf._table(
        spark,
        schema,
        f"mvp_in_points_wkt_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            F.expr(
                f"transform(sequence(1, {_WKT_POINTS}), i -> concat('POINT(', cast(rand(51) * 100 as string), ' ',"
                " cast(rand(52) * 100 as string), ')'))"
            ).alias("points"),
            F.expr(f"transform(sequence(1, {_WKT_POINTS}), i -> rand(53) * 10)").alias(
                "vals"
            ),
        ),
        rows=n,
    )


def _file_bytes(spark, n, corpus, schema):
    """The bytes of the corpus' first `n` tiles (`content`), `n` rows over `family_partitions` partitions."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    rows = corpus.rstrip("/") + "/rows"
    if not os.path.isdir(rows):
        return None
    held = lf._corpus_rows(corpus) or []
    lf.corpus_guard("file_bytes", rows, len(held), n)
    paths = held[:n]
    return lf._table(
        spark,
        schema,
        f"mvp_in_file_bytes_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.read.format("binaryFile")
        .load(paths)
        .select("content")
        .limit(n)
        .withColumn("_row", F.monotonically_increasing_id())
        .repartitionByRange(family_partitions(spark, n), "_row")
        .select("content"),
    )


def _point_wkb(x, y):
    """Little-endian WKB of POINT(x y) for two Series of doubles (the geometry as bytes, which every level accepts)."""
    import numpy as np
    import pandas as pd

    rec = np.empty(
        len(x), dtype=[("o", "u1"), ("t", "<u4"), ("x", "<f8"), ("y", "<f8")]
    )
    rec["o"], rec["t"] = 1, 1
    rec["x"], rec["y"] = x.to_numpy(), y.to_numpy()
    return pd.Series([rec[i : i + 1].tobytes() for i in range(len(rec))], index=x.index)


def _mvt_rows(spark, n, corpus, schema):
    """`n` points in tile coordinates (0..4096) as WKB, in 10 groups, each with an id."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    to_wkb = F.pandas_udf(_point_wkb, "binary")
    return lf._table(
        spark,
        schema,
        f"mvp_in_mvt_rows_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            (F.col("id") % 10).cast("string").alias("key"),
            F.col("id"),
            to_wkb(
                (F.rand(31) * 4096).cast("int").cast("double"),
                (F.rand(32) * 4096).cast("int").cast("double"),
            ).alias("wkb"),
        ),
        rows=n,
    )


_TIN_POINTS = 60  # points of one TIN


def _tin_points(spark, n, corpus, schema):
    """`n` rows of 60 random WKT points (with a Z), each row one TIN to build."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench import levels_forms as lf

    return lf._table(
        spark,
        schema,
        f"mvp_in_tin_points_v{lf.RECIPE_VERSION}_{n}",
        lambda: spark.range(n).select(
            F.expr(
                f"transform(sequence(1, {_TIN_POINTS}), i -> "
                "concat('POINT Z (', cast(rand(41) * 100 as string), ' ', cast(rand(42) * 100 as string), ' ',"
                " cast(rand(43) * 10 as string), ')'))"
            ).alias("points")
        ),
        rows=n,
    )


# --- the floor shells --------------------------------------------------------------------------------------------


def _floor_udf(ret: str):
    """A pandas UDF over any arity: the input columns in, a constant of type `ret` (or the first column back) out."""
    import pandas as pd

    def floor(*cols):
        if ret == "tile":
            return cols[0]
        return pd.Series([_FLOOR_VALUE[ret]] * len(cols[0]), index=cols[0].index)

    floor.__annotations__ = {"cols": pd.Series, "return": pd.Series}
    return floor


def setup_floors(spark):
    """Register every family's floor UDF (`gbx_floor_<n>`), once, outside the timed region."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.manifest import TILE_SQL

    for i, rep in enumerate(REPS):
        if rep.floor_ret is None:
            continue
        ddl = TILE_SQL if rep.floor_ret == "tile" else rep.floor_ret
        spark.udf.register(
            f"gbx_floor_{i}", F.pandas_udf(_floor_udf(rep.floor_ret), ddl)
        )


# --- the forms -----------------------------------------------------------------------------------------------------


def _variants(fn) -> List[str]:
    return list((fn.levels.get("L3", {}) or {}).get("variants", {}) or [])


def _label(inp: str) -> str:
    return {
        "tiles_n": "n",
        "points_arrays": "pts",
        "points_wkt": "wktpts",
        "file_bytes": "bytes",
        "bng_cells": "cells",
        "invalid_polys": "polys",
        "mvt_rows": "mvt",
        "tin_points": "tin",
    }.get(inp, inp)


def _manifest_rows():
    from databricks.labs.gbx.manifest import load

    return load().functions


def _sql(rep: Rep, name: str) -> str:
    return rep.shape.format(call=rep.call.replace("{fn}", name))


def _vsql(rep: Rep, name: str) -> str:
    """The value-level query of a representative's form ('' when its output has no decoder yet)."""
    if rep.vfp is None:
        return ""
    from databricks.labs.gbx.bench import value_fp

    order = _VFP_ORDER.get(rep.inp)
    if order is None and rep.shape is _TILE:
        raise KeyError(
            f"{rep.fn}: no ordering key for the input {rep.inp!r} (_VFP_ORDER)"
        )
    return _VFP_SHAPE[id(rep.shape)].format(
        call=rep.call.replace("{fn}", name), udf=value_fp.udf_name(rep.vfp), order=order
    )


def family_forms() -> Dict[str, List[Form]]:
    """`{family: forms}` for the generated representatives (L1, then each L3 variant, then the floor)."""
    rows = _manifest_rows()
    out: Dict[str, List[Form]] = {}
    for i, rep in enumerate(REPS):
        row = rows.get(rep.fn)
        if row is None:
            # a representative the manifest no longer has: this runs when `levels_forms` is imported, so raising would
            # break every set, the preflight and the worker UDF imports. The manifest-pin test names the drift.
            warnings.warn(
                f"levels: {rep.fn} is not in the manifest; its forms are skipped"
            )
            continue
        lv = row.levels
        tag = _label(rep.inp)
        forms: List[Form] = []
        common = dict(fn=rep.fn, input=rep.inp, allows_null=rep.allows_null)
        if "L1" in lv:
            forms.append(
                Form(
                    mvp=rep.family,
                    level="L1",
                    variant=f"l1@{tag}",
                    sql=_sql(rep, rep.fn),
                    vsql=_vsql(rep, rep.fn),
                    computes=("dedicated",),
                    **common,
                )
            )
        for v in _variants(row) or ["l3"]:
            forms.append(
                Form(
                    mvp=rep.family,
                    level="L3",
                    variant=f"{v}@{tag}",
                    sql=_sql(rep, rep.fn),
                    vsql=_vsql(rep, rep.fn),
                    computes=rep.computes_l3,
                    **common,
                )
            )
        if rep.floor_ret is not None:
            floor = Form(
                mvp=rep.family, level="L3", variant=f"floor@{tag}", sql=_sql(rep, f"gbx_floor_{i}"),
                setup=_ME + "setup_floors", register=False, computes=rep.computes_l3, **common,
            )  # fmt: skip
        else:
            floor = Form(
                mvp=rep.family, level="L3", variant=f"floor@{tag}", sql=rep.floor_sql, register=False,
                computes=rep.computes_l3, **common,
            )  # fmt: skip
        out.setdefault(rep.family, []).extend(forms + [floor])
    return out


# the input a reused form reads in its family: the one n-row tile input, and the family-sized grouped tiles
_FAMILY_INPUT = {
    "tiles_mat": "tiles_n",
    "tiles_grouped": "tiles_grouped_family",
    "tiles_grouped_virt": "tiles_grouped_family_virt",
}


def _virtual_inputs(forms: List[Form], fn: str, floored: str) -> List[str]:
    """The virtual-tile inputs `fn`'s forms read (the plain one, not a skew variant of it) that its floor does not."""
    return sorted(
        {
            f.input
            for f in forms
            if f.fn == fn
            and "virt" in f.input
            and "skew" not in f.input
            and f.input != floored
        }
    )


def reused_forms(lf) -> Dict[str, List[Form]]:
    out: Dict[str, List[Form]] = {}
    for fam, forms in _reused(lf).items():
        # the materialized tiles come from the one n-row input every generated form reads
        keep = [
            dataclasses.replace(
                f,
                mvp=fam,
                input=_FAMILY_INPUT.get(f.input, f.input),
                # a JVM level of the BNG scalar is its own form, not a second "-"
                variant=(
                    f"{f.level.lower()}@pts"
                    if fam == "grid-scalar" and f.variant == "-"
                    else f.variant
                ),
            )
            for f in forms
        ]
        for fn, inp, sql in _REUSED_FLOORS.get(fam, []):
            keep.append(
                Form(
                    mvp=fam,
                    fn=fn,
                    level="L3",
                    variant="floor",
                    input=inp,
                    sql=sql,
                    register=False,
                )
            )
            # A floor reads the input of the forms it is the floor of. The forms on virtual tiles (paths, no bytes) read
            # their own input, not the materialized one: a floor over the materialized input builds n whole tiles
            # (past 900 s at 16x) for a run that times only the virtual forms. They get a scan floor of their own.
            for virt in _virtual_inputs(keep, fn, inp):
                keep.append(
                    Form(
                        mvp=fam,
                        fn=fn,
                        level="L3",
                        variant="floor@virt",
                        input=virt,
                        sql=_VIRT_SCAN,
                        register=False,
                    )
                )
        out[fam] = keep
    return out


def install(lf) -> None:
    """Add the family inputs and `FORMS[<family>]` to `levels_forms` (called once, at its import)."""
    lf.INPUTS.update(
        {
            "tiles_n": _tiles_n,
            "tiles_grouped_family": lf._grouped_tiles(False, "_family"),
            "points_arrays": _points_arrays,
            "points_wkt": _points_wkt,
            "file_bytes": _file_bytes,
            "mvt_rows": _mvt_rows,
            "tin_points": _tin_points,
        }
    )
    lf.BASE_ROWS.update(
        {
            "tiles_n": 250,
            "points_arrays": 2000,
            "points_wkt": 2000,
            "file_bytes": 250,
            "mvt_rows": 100_000,
            "tin_points": 2000,
            # the aggregate family reads a tile family's 250 tiles (five groups of about 50), not the 2000 tiles
            # (32 GB, forty groups) the aggregate set reads: that input took a form's whole cap to build. It is its
            # own input, so the aggregate set's `tiles_grouped` (and its stored baselines) keep their 2000
            "tiles_grouped_family": 250,
        }
    )
    for fam, forms in reused_forms(lf).items():
        lf.FORMS[fam] = forms
    for fam, forms in family_forms().items():
        lf.FORMS.setdefault(fam, [])
        lf.FORMS[fam] = lf.FORMS[fam] + forms
