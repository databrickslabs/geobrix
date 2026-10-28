"""The MVP-A oracle: aggregates through every shape.

One table, one test. A shape is a level plus the SQL that reaches it: the grouped aggregate itself (Arrow
UDF on L3, the Catalyst aggregate on L1), the finalize scalar over `collect_list` (light probes, and the Java
UDF facades on L2), a tree of finalizes over salted partial results, and a Python table function over
`TABLE(...) PARTITION BY`. The JVM shapes need the staged JAR and run under `gbx-test-parity.sh`.
"""

from collections import namedtuple

import pytest

from databricks.labs.gbx import register
from databricks.labs.gbx.bench import oracles

MA, BR = oracles.MA, oracles.BR
GROUPED = "SELECT key, {fn}({args}) AS out FROM _gbx_oracle GROUP BY key"
_PAIRS = "collect_list(struct(c0 AS cellid, c1 AS value))"
_BY_CANVAS = "GROUP BY key, c4, c5, c6, c7, c8, c9"
_CANVAS = "c4, c5, c6, c7, c8, c9"


def _finalize(merge: str, rasterize: str, merge_of: str = "c0") -> dict:
    return {
        MA: f"SELECT key, {merge}(collect_list({merge_of})) AS out FROM _gbx_oracle GROUP BY key",
        BR: (
            f"SELECT key, {rasterize}({_PAIRS}, {_CANVAS}) AS out FROM _gbx_oracle {_BY_CANVAS}"
        ),
    }


TREE = (
    "SELECT key, gbx_mvp_rst_canvas_max(collect_list(p)) AS out FROM ("
    f"SELECT key, gbx_mvp_rst_bng_rasterize_cells({_PAIRS}, {_CANVAS}) AS p FROM ("
    "SELECT *, pmod(xxhash64(c0, c1), 2) AS salt FROM _gbx_oracle) "
    "GROUP BY key, salt, c4, c5, c6, c7, c8, c9) GROUP BY key"
)
TVF = {
    MA: (
        "SELECT key, raster AS out FROM gbx_mvp_rst_merge_tvf("
        "TABLE(SELECT key, c0 AS tile FROM _gbx_oracle) PARTITION BY key)"
    ),
    BR: (
        "SELECT key, raster AS out FROM gbx_mvp_rst_bng_rasterize_tvf("
        "TABLE(SELECT key, c0 AS cellid, c1 AS value, c4, c5, c6, c7, c8, c9 FROM _gbx_oracle) "
        "PARTITION BY key)"
    ),
}


def _grouped(spark):
    for fn in (MA, BR):
        register(spark, only=[fn], _levels={fn: ("L3", "c0")})


def _udaf(spark):
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l1_udafs(spark)


def _light(spark):
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx import functions as prx

    _mvp_forms.register_probe_forms(spark)
    prx.register(
        spark, only=["gbx_rst_merge"]
    )  # the light finalize (the heavy one has this name too)


def _l1_merge(spark):
    from databricks.labs.gbx.bench import levels_forms

    levels_forms.setup_a_l1_merge(spark)


def _l2(spark):
    from databricks.labs.gbx.pyrx import _mvp_forms

    _mvp_forms.register_l2_probes(spark)


# Shape: its setup, the SQL per aggregate, and what a NULL value burns on its tier (NaN on the lightweight
# tier, 1.0 on the heavyweight tier: the one thing the tiers do not share, see `oracles.NULL_BURN`).
Shape = namedtuple("Shape", "setup sql null_burn")
SHAPES = {
    # the aggregate itself: an Arrow UDF (L3 c0) and the Catalyst aggregate (L1)
    "grouped": Shape(_grouped, {MA: GROUPED, BR: GROUPED}, oracles.NAN),
    "udaf": Shape(_udaf, {MA: GROUPED, BR: GROUPED}, 1.0),
    # collect_list into a finalize scalar: light (gbx_rst_merge is the product's), and the Java UDFs on bytes
    "collect_finalize": Shape(
        _light,
        _finalize("gbx_rst_merge", "gbx_mvp_rst_bng_rasterize_cells"),
        oracles.NAN,
    ),
    # the Catalyst finalize over an array of tiles (the rasterize has no heavy finalize)
    "collect_finalize_l1": Shape(
        _l1_merge, {MA: _finalize("gbx_rst_merge", "")[MA]}, 1.0
    ),
    "collect_finalize_l2": Shape(
        _l2,
        _finalize(
            "gbx_mvp_rst_merge_array_l2",
            "gbx_mvp_rst_bng_rasterize_cells_l2",
            merge_of="c0.raster",
        ),
        1.0,
    ),
    # partial canvases per (key, salt), merged by a per-pixel max: associative, so exact
    "tree": Shape(_light, {BR: TREE}, oracles.NAN),
    # a Python table function over the whole partition of a key
    "tvf": Shape(_light, TVF, oracles.NAN),
}
JVM = {"udaf", "collect_finalize_l1", "collect_finalize_l2"}


@pytest.mark.parametrize(
    "shape",
    [
        pytest.param(k, marks=pytest.mark.integration) if k in JVM else k
        for k in sorted(SHAPES)
    ],
)
def test_mvp_a_oracle(request, tmp_path, shape):
    s = SHAPES[shape]
    spark = request.getfixturevalue("jvm_spark" if shape in JVM else "spark")
    s.setup(spark)
    failures = []
    for fn, sql in s.sql.items():
        rows = [c for c in oracles.MVP_A if c.fn == fn and "session" in c.surfaces]
        assert rows, f"{shape}: no oracle rows for {fn}"
        ran = oracles.run_groups(spark, rows, sql, str(tmp_path), s.null_burn)
        assert len(ran) == len(rows)
        failures += [m for _, m in ran if m]
    assert failures == []
