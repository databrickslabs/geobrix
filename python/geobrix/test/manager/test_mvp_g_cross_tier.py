"""The MVP-G oracle: generators (functions returning many rows per input) through every shape.

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
_ROWS_OF = "SELECT explode(transform({arr}, b -> named_struct('raster', b))) AS tile FROM _gbx_oracle"

# Shape: the level it exercises ("-" = a probe form registered under its own name), the SQL, the
# functions it answers for (and the case kinds it can judge), and the name the SQL calls.
Shape = namedtuple("Shape", "level sql fns kinds name_of pad")
ANY = ("windows", "no_rows", "error_tile", "cells", "raises")
PROBE = (
    "windows",
    "no_rows",
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
    # probe forms: the Java UDF facades on bytes (L2). They take raster bytes, so an error tile has
    # nowhere to ride and is not judged; a fixed-arity UDF is called with the defaults filled in.
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
        PROBE,
        None,
        False,
    ),
    "l2_retile_count_part": Shape(
        "-",
        "SELECT named_struct('raster', gbx_mvp_rst_retile_part_l2(c0.raster, c1, c2, i)) AS tile FROM _gbx_oracle "
        "LATERAL VIEW explode(sequence(0, gbx_mvp_rst_retile_count_l2(c0.raster, c1, c2) - 1)) x AS i",
        (RT,),
        PROBE,
        None,
        False,
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
            if c.fn == fn and c.kind in s.kinds and "session" in c.surfaces
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
    """T0 for the UC table function: the handler behind it, called with the values UC passes."""
    ran = oracles.run_handlers_rows(oracles.MVP_G, tmpdir=str(tmp_path))
    assert len(ran) == sum(c.fn == TE and "uc" in c.surfaces for c in oracles.MVP_G)
    assert [m for _, m in ran if m] == []
