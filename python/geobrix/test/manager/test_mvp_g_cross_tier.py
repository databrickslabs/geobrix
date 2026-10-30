"""The generators oracle: generators (functions returning many rows per input) through every shape.

One table, one test. A shape is a level plus the SQL that reaches it: the table function form every
tier shares (`LATERAL fn(...)`), the array form with `inline`, and the probe forms the perf matrix
times (count+part with an LRU, Arrow UDTFs, the Java UDF facades). The JVM shapes need the staged JAR
and run under `gbx-test-parity.sh`.
"""

from collections import namedtuple

import pytest

from databricks.labs.gbx import register
from databricks.labs.gbx.bench import oracles

RT, TE, TS = oracles.RT, oracles.TE, oracles.TS
LAT = "SELECT t.* FROM _gbx_oracle, LATERAL {fn}({args}) t"
INLINE = "SELECT inline({fn}({args})) FROM _gbx_oracle"
# The Java UDFs give back bytes. This is what the manager's SQL wrapper makes of them: a tile whose raster is
# the bytes, or an error tile (NULL raster and a reason) when they are NULL.
_ERR = "the heavy kernel could not process this tile (see the executor log)"
_TILE_OF = (
    "named_struct('raster', {b}, 'path', CAST(NULL AS STRING), "
    "'metadata', IF({b} IS NULL, map('last_error', '"
    + _ERR
    + "'), CAST(map() AS map<string,string>)))"
)
_ROWS_OF = (
    "SELECT explode(transform({arr}, b -> "
    + _TILE_OF.format(b="b")
    + ")) AS tile FROM _gbx_oracle"
)

# Shape: the level it exercises ("-" = a probe form registered under its own name), the SQL, the
# functions it answers for (and the case kinds it can judge), the name the SQL calls, whether to fill in
# the default arguments, and the rows it cannot judge.
Shape = namedtuple("Shape", "level sql fns kinds name_of pad skip", defaults=((),))
ANY = ("windows", "windows_batch", "no_rows", "error_tile", "cells", "raises")
PROBE = (
    "windows",
    "windows_batch",
    "no_rows",
    "raises",
)  # the array / count+part probes split a readable tile; they hold no error contract
NAMED = {
    "gbx_mvp_rst_retile_array": "SELECT explode(gbx_mvp_rst_retile_array({args})) AS tile FROM _gbx_oracle",
    "gbx_mvp_rst_retile_part": (
        "SELECT gbx_mvp_rst_retile_part({args}, i) AS tile FROM _gbx_oracle "
        "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count({args}) - 1)) x AS i"
    ),
}
SHAPES = {
    # the table function form: a Python UDTF (L3), or the Catalyst generator installed as a table function (L1)
    "udtf": Shape("L3", LAT, (RT, TE), ANY, None, False),
    "l1_tvf": Shape("L1", LAT, (RT, TE), ANY, None, False),
    # the array form: a scalar returning the chips, exploded by inline
    "array_inline": Shape("L3", INLINE, (TS,), ANY, None, False),
    "l1_array_inline": Shape("L1", INLINE, (TS,), ANY, None, False),
    # probe forms: the light array / count+part UDFs and the Arrow-optimized UDTFs
    "retile_array": Shape(
        "-", NAMED["gbx_mvp_rst_retile_array"], (RT,), PROBE, None, False
    ),
    "retile_count_part": Shape(
        "-", NAMED["gbx_mvp_rst_retile_part"], (RT,), PROBE, None, False
    ),
    "udtf_arrow": Shape("-", LAT, (RT, TE), ANY, lambda f: f + "_mvp_arrow", False),
    # probe forms: the Java UDF facades on bytes (L2). A fixed-arity UDF is called with the defaults filled in. A
    # raster that does not open is one error tile, as on the other levels; an error tile IN is the wrapper's to
    # pass on (the facade only sees bytes, and a tile with no raster has none), so that row is not judged here.
    "l2_array_inline": Shape(
        "-",
        INLINE,
        (TS,),
        ("cells", "no_rows"),
        lambda f: "gbx_mvp_bng_tessellate_l2",
        True,
    ),
    "l2_retile_array": Shape(
        "-",
        _ROWS_OF.format(arr="gbx_mvp_rst_retile_array_l2(c0.raster, c1, c2)"),
        (RT,),
        PROBE + ("error_tile",),
        None,
        False,
        ("rt_error_in",),
    ),
    "l2_retile_count_part": Shape(
        "-",
        "SELECT "
        + _TILE_OF.format(b="p")
        + " AS tile FROM (SELECT gbx_mvp_rst_retile_part_l2(c0.raster, c1, c2, i) AS p FROM _gbx_oracle "
        "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count_l2(c0.raster, c1, c2) - 1)) x AS i)",
        (RT,),
        PROBE + ("error_tile",),
        None,
        False,
        ("rt_error_in",),
    ),
}
JVM = {
    "l1_tvf",
    "l1_array_inline",
    "l2_array_inline",
    "l2_retile_array",
    "l2_retile_count_part",
}


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(k, marks=pytest.mark.integration) if k in JVM else k
        for k in sorted(SHAPES)
    ],
)
def test_mvp_g_oracle(request, tmp_path, shape):
    s = SHAPES[shape]
    spark = request.getfixturevalue("jvm_spark" if shape in JVM else "spark")
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_probe_forms(spark)
    if shape.startswith("l2_"):
        _mvp_forms.register_l2_probes(spark)
    failures = []
    for fn in s.fns:
        if s.level != "-":
            register(spark, only=[fn], _levels={fn: (s.level, "-")})
        rows = [
            c
            for c in oracles.MVP_G
            if c.fn == fn
            and c.kind in s.kinds
            and "session" in c.surfaces
            and c.cid not in s.skip
        ]
        assert rows, f"{shape}: no oracle rows for {fn}"
        ran = oracles.run_rows(
            spark,
            rows,
            shape_sql=s.sql,
            name_of=s.name_of or (lambda f: f),
            tmpdir=str(tmp_path),
            pad=s.pad,
        )
        failures += [m for _, m in ran if m]
    assert failures == []


def test_mvp_g_uc_handlers(tmp_path):
    """The UC table function without Spark: the handler behind it, called with the values UC passes."""
    ran = oracles.run_handlers_rows(oracles.MVP_G, tmpdir=str(tmp_path))
    assert len(ran) == sum(c.fn == TE and "uc" in c.surfaces for c in oracles.MVP_G)
    assert [m for _, m in ran if m] == []


def test_mvp_g_uc_table_function_on_the_warehouse(tmp_path):
    """The warehouse form of the UC table function: one statement per case answers its rows as one JSON cell, and
    the oracle judges the rows read back from it (a missing row fails)."""
    import base64
    import json

    from shapely import box, to_wkb

    pairs = dict(oracles.uc_sql(oracles.MVP_G, "cat", "sch", str(tmp_path)))
    assert " FROM `cat`.`sch`.gbx_bng_tessellateexplode(" in pairs["te_offset"]
    poly = box(530100, 180100, 532900, 181900)
    rows = [
        {
            "cellid": f"TQ{x}{y}",
            "core": False,
            "chip": base64.b64encode(to_wkb(poly & box(*c))).decode(),
        }
        for x, y, c in [
            (
                30 + i,
                80 + j,
                (
                    530000 + 1000 * i,
                    180000 + 1000 * j,
                    531000 + 1000 * i,
                    181000 + 1000 * j,
                ),
            )
            for j in (0, 1)
            for i in (0, 1, 2)
        ]
    ]
    results = {
        "te_offset": {"value": json.dumps(rows)},
        "te_null": {"value": "[]"},
        "te_wrong_srid_uc": {
            "error": "gbx_bng_tessellate: SRID 4326. Fix: use the session function"
        },
    }
    ran = [c for c in oracles.MVP_G if c.cid in results]
    assert oracles.check_uc_results(ran, results, str(tmp_path)) == []
    results["te_offset"]["value"] = json.dumps(rows[1:])
    (failure,) = oracles.check_uc_results(ran, results, str(tmp_path))
    assert failure.startswith("te_offset")


def test_the_probe_cache_holds_one_raster_and_confirms_a_hit_against_its_bytes(
    monkeypatch,
):
    """The oracle's pair goes through a row at a time, so the last piece of A frees A before B arrives; this holds
    the guard itself: a sampled bucket picks the entry, only the same bytes hit, one raster stays, and a raster
    over the decoded budget is not kept."""
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx.core import budget

    a, b = oracles._edit_pair()
    assert _mvp_forms._digest(a) == _mvp_forms._digest(b), "one bucket, two rasters"
    lru = _mvp_forms._OpenLru()
    tile = oracles._tile

    def pixels(raw):
        with lru.opened(tile(raw)) as (ds, release):
            return float(ds.read(1).sum()), ds, release

    sum_a, ds_a, _ = pixels(a)
    again, ds_again, _ = pixels(bytearray(a))  # a fresh copy of the same bytes: a hit
    assert (again, ds_again) == (sum_a, ds_a) and len(lru.items) == 1
    sum_b, ds_b, release = pixels(b)  # same bucket, other bytes: a miss that replaces A
    assert sum_b != sum_a and ds_b is not ds_a and len(lru.items) == 1
    with lru.opened(tile(b)) as (_, release):
        release()  # the last piece of the row
    assert len(lru.items) == 0
    monkeypatch.setattr(budget, "decoded_budget_bytes", lambda strategy: len(a) - 1)
    with lru.opened(tile(a)) as (ds, release):
        assert float(ds.read(1).sum()) == sum_a  # usable for this call
    assert len(lru.items) == 0, "a raster over the budget is opened per call, not kept"
