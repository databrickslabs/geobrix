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


def _tiles(virtual: bool):
    def build(spark, n, corpus, schema):
        rows = corpus.rstrip("/") + "/rows"
        if not os.path.isdir(rows):
            return None
        from databricks.labs.gbx.ds.register import register

        register(spark)  # the light raster reader
        return _table(
            spark,
            schema,
            f"mvp_in_tiles_{'virt' if virtual else 'mat'}_v{RECIPE_VERSION}_{n}",
            lambda: spark.read.format("raster_gbx")
            .option("virtualTiles", str(virtual).lower())
            .load(rows)
            .select("tile")
            .limit(n),
        )

    return build


INPUTS: Dict[str, Callable] = {
    "bng_points": _bng_points,
    "invalid_polys": _invalid_polys,
    "tiles_mat": _tiles(False),
    "tiles_virt": _tiles(True),
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
