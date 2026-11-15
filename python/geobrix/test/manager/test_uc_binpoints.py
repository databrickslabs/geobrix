"""Binpoints as partial / finalize: what the oracle rows (bench/oracle_tables/raster_aggregate.py) do not say. The rows hold
the values, the floor rule's edges, the finalize twin of the session aggregate's rows and the recipe against the session's
reference; this file holds the mechanisms that must fail when reverted (the floor rule, the cell range check), the
recipe against the session aggregate itself (the same bytes), the NULL and NaN z rule pinned, and the shape of both
forms on UC (SQL without Python, a Python handler with a cap that names the aggregate).
"""

import numpy as np
import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracle_tables import raster_aggregate as ra
from databricks.labs.gbx.pyrx.core import binning
from databricks.labs.gbx.uc import ddl, sqlbody, tier_b

BPP, BPF, BP = ra.BPP, ra.BPF, "gbx_rst_binpoints_agg"
MAN = manifest.load()
CANVAS = "0D, 0D, 64D, 64D, 64, 64"
STATS = list(ra.D1_REDUCE)


def _rows(fn):
    return [c for c in oracles.TABLES["RASTER_AGGREGATE"] if c.fn == fn]


def _recipes():
    # the binpoints recipes only; the table also holds the cell-rasterize pre-reduction recipes
    return [
        c
        for c in oracles.TABLES["RASTER_AGGREGATE"]
        if c.sql is not None and c.cid.startswith("recipe_d1_")
    ]


def _raster(out):
    return oracles._raster_bytes(out)


def _pixels(raster):
    from rasterio.io import MemoryFile

    with MemoryFile(raster) as mf, mf.open() as ds:
        return ds.read(1, masked=True)


# --- the forms: SQL without Python, and a Python handler ---------------------------------------------------------------
def test_the_partial_is_one_sql_body_for_the_session_and_uc_with_no_python():
    fn = MAN.functions[BPP]
    assert dict(fn.levels["L4"]) == {"sql": "L0"} and set(fn.levels) == {
        "L0",
        "L3",
        "L4",
    }
    stmts = ddl.function_ddl(fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6")
    assert stmts[-1].count("LANGUAGE SQL") == 1 and "PYTHON" not in "".join(stmts)
    assert "RETURNS struct<col:int,row:int>" in stmts[-1].replace("\n", " ")


def test_the_finalize_is_a_python_handler_that_needs_rasterio():
    fn = MAN.functions[BPF]
    stmts = ddl.function_ddl(fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6")
    # rasterio is vendored in the env wheel (manifest uc_provided): declared, never pinned
    assert "LANGUAGE PYTHON" in stmts[-1] and "rasterio" in fn.levels["L4"]["deps"]
    assert "rasterio==" not in stmts[-1]
    assert fn.levels["L4"]["handler"].endswith(":rst_binpoints_finalize")


# --- mechanism 1: the floor rule is the session's -----------------------------------------------------------------------
def test_the_edge_rows_fail_when_the_floor_rule_is_reverted_to_round(spark, tmp_path):
    body, helpers = sqlbody.l0_source(MAN.functions[BPP])
    assert body.count("floor(") == 2
    rows = _rows(BPP)
    ok = oracles.run_sql_kernels(spark, rows, tmpdir=str(tmp_path))
    assert [m for _, m in ok if m] == []
    reverted = oracles.run_sql_kernels(
        spark,
        rows,
        sources={BPP: (body.replace("floor(", "round("), helpers)},
        tmpdir=str(tmp_path),
    )
    red = {c.cid for c, m in reverted if m}
    # round puts 9.999999 in column 10 (out) and 1.5 in column 2, so the cell rows and the edge row go red
    assert {"bp_partial_cells", "bp_partial_inside_east_edge"} <= red


def test_the_nan_rows_fail_when_the_range_test_is_dropped(spark, tmp_path):
    """floor(NaN) is 0 in SQL: without the test in double space a NaN coordinate lands in column 0."""
    body, helpers = sqlbody.l0_source(MAN.functions[BPP])
    col = "(x - xmin) / (xmax - xmin) * width_px"
    assert f"{col} < width_px" in body
    reverted = body.replace(f"{col} >= 0 AND {col} < width_px AND ", "")
    assert reverted != body
    ran = oracles.run_sql_kernels(
        spark,
        _rows(BPP),
        sources={BPP: (reverted, helpers)},
        tmpdir=str(tmp_path),
    )
    red = {c.cid for c, m in ran if m}
    assert {"bp_partial_nan_x", "bp_partial_outside_x", "bp_partial_negative"} <= red


def test_the_lightweight_partial_gives_the_keys_the_sql_expression_gives(spark):
    """The session has two levels of the partial: one rule, so the same key for every point, edges and non-numbers included."""
    rng = np.random.default_rng(3)
    pts = [(float(a), float(b)) for a, b in rng.uniform(-3, 13, (3000, 2))]
    pts += [
        (x, y)
        for x in (0.0, 5.0, 10.0, 9.999999, -1e-9)
        for y in (0.0, 5.0, 10.0, 0.000001)
    ]
    pts += [(float("nan"), 5.0), (5.0, float("inf")), (None, 5.0), (5.0, None)]
    got = {}
    for level in ("L0", "L3"):
        register(
            spark, only=[BPP], _levels={BPP: (level, "-" if level == "L0" else "c0")}
        )
        spark.createDataFrame(pts, "x double, y double").createOrReplaceTempView(
            "_bp_keys"
        )
        rows = spark.sql(
            f"SELECT x, y, {BPP}(x, y, 0D, 0D, 10D, 10D, 10, 10) AS k FROM _bp_keys"
        ).collect()
        got[level] = [
            None if r["k"] is None else (r["k"]["col"], r["k"]["row"]) for r in rows
        ]
    assert got["L0"] == got["L3"]
    assert sum(k is not None for k in got["L0"]) > 1000


# --- mechanism 2: a cell of another canvas raises ----------------------------------------------------------------------
def test_a_column_past_the_canvas_raises_where_the_flat_index_would_wrap():
    """On a taller canvas col == width is the first pixel of the next row: only the check keeps it from landing there."""
    with pytest.raises(
        ValueError, match="outside the 2 x 2 canvas.*gbx_rst_binpoints_partial"
    ):
        binning.bin_cells([2], [0], [1.0], 0.0, 0.0, 2.0, 2.0, 2, 2, 4326)
    with pytest.raises(ValueError, match="outside the 2 x 2 canvas"):
        binning.bin_cells([0], [-1], [1.0], 0.0, 0.0, 2.0, 2.0, 2, 2, 4326)


def test_a_repeated_pixel_keeps_its_largest_value_and_nan_never_wins():
    got = _pixels(
        binning.bin_cells(
            [0, 0, 0], [0, 0, 0], [1.0, np.nan, 4.0], 0.0, 0.0, 1.0, 1.0, 1, 1, 4326
        )
    )
    assert got.tolist() == [[4.0]]
    assert _pixels(
        binning.bin_cells([0], [0], [np.nan], 0.0, 0.0, 1.0, 1.0, 1, 1, 4326)
    ).mask.all()


# --- the recipe, against the session aggregate itself ------------------------------------------------------------------
def _points(n=20000, seed=7):
    rng = np.random.default_rng(seed)
    x = rng.uniform(-2, 66, n)
    y = rng.uniform(-2, 66, n)
    # points on the edges and corners, which the floor rule must treat alike on both routes
    edge = [
        (0.0, 64.0),
        (64.0, 0.0),
        (64.0, 64.0),
        (0.0, 0.0),
        (0.0, 30.0),
        (30.0, 64.0),
        (30.0, 0.0),
        (64.0, 30.0),
    ]
    x[: len(edge)] = [e[0] for e in edge]
    y[: len(edge)] = [e[1] for e in edge]
    z = np.round(rng.uniform(-50, 200, n), 2)
    return [(float(a), float(b), float(c)) for a, b, c in zip(x, y, z)]


def _session_tile(spark, stat):
    sql = f"SELECT gbx_rst_binpoints_agg(x, y, z, {CANVAS}, 4326, '{stat}') AS o FROM _bp_pts"
    return _raster(spark.sql(sql).collect()[0]["o"])


def _d1_tile(spark, stat):
    sql = ra.d1_sql(stat, source="_bp_pts", canvas=CANVAS).replace("{q}", "")
    return _raster(spark.sql(sql).collect()[0]["out"])


@pytest.fixture
def d1_session(spark):
    register(spark, only=[BP, BPP, BPF], _levels={BPP: ("L0", "-"), BPF: ("L3", "c0")})
    df = spark.createDataFrame(_points(), "x double, y double, z double")
    df.createOrReplaceTempView("_bp_pts")
    return spark


@pytest.mark.parametrize("stat", STATS)
def test_the_recipe_writes_the_tile_the_session_aggregate_writes(d1_session, stat):
    want, got = _session_tile(d1_session, stat), _d1_tile(d1_session, stat)
    a, b = _pixels(want), _pixels(got)
    assert (a.mask == b.mask).all() and a.count() > 1000
    assert np.allclose(a.compressed(), b.compressed(), rtol=1e-6, atol=0)
    assert want == got, f"{stat}: the same pixels, but the bytes differ"


def test_the_recipe_rows_run_on_the_host_through_the_session_forms(spark, tmp_path):
    register(spark, only=[BPP, BPF], _levels={BPP: ("L0", "-"), BPF: ("L3", "c0")})
    recipes = _recipes()
    assert {c.cid for c in recipes} == {
        "recipe_d1_max", "recipe_d1_min", "recipe_d1_mean", "recipe_d1_count", "recipe_d1_median", "recipe_d1_percentile_90",
    }  # fmt: skip
    for c in recipes:
        assert oracles.run_recipe(spark, c, "") is None, c.cid


# --- the NULL and NaN z rule, pinned -----------------------------------------------------------------------------------
def _stat_at_pixel(tile):
    return float(_pixels(tile)[0, 0])


@pytest.mark.parametrize(
    "stat,session_says,recipe_says",
    [
        # max and min ignore a NaN on both routes
        ("max", 5.0, 5.0),
        ("min", 1.0, 1.0),
        # the session counts a NaN point and a NULL z (light reads NULL as NaN); the recipe drops both
        ("count", 4.0, 2.0),
        # a NaN makes the session's mean and median NoData; the recipe takes them over the values it keeps
        ("mean", None, 3.0),
        ("median", None, 3.0),
    ],
)
def test_a_null_or_nan_z_is_dropped_by_the_recipe_and_pinned_against_the_session(
    spark, stat, session_says, recipe_says
):
    register(spark, only=[BP, BPP, BPF], _levels={BPP: ("L0", "-"), BPF: ("L3", "c0")})
    spark.createDataFrame(
        [(0.5, 0.5, 1.0), (0.6, 0.4, 5.0), (0.7, 0.3, float("nan")), (0.8, 0.2, None)],
        "x double, y double, z double",
    ).createOrReplaceTempView("_bp_nan")
    canvas = "0D, 0D, 1D, 1D, 1, 1"
    s = _raster(
        spark.sql(
            f"SELECT gbx_rst_binpoints_agg(x, y, z, {canvas}, 4326, '{stat}') AS o FROM _bp_nan"
        ).collect()[0]["o"]
    )
    r = _raster(
        spark.sql(
            ra.d1_sql(stat, source="_bp_nan", canvas=canvas).replace("{q}", "")
        ).collect()[0]["out"]
    )
    sess, rec = _pixels(s)[0, 0], _pixels(r)[0, 0]
    assert (sess is np.ma.masked) == (session_says is None)
    if session_says is not None:
        assert float(sess) == session_says
    assert float(rec) == recipe_says


# --- the cap, and the shape of the handler -----------------------------------------------------------------------------
def test_a_canvas_over_the_cap_is_refused_with_a_fix_that_names_the_session_aggregate():
    with pytest.raises(
        Exception,
        match=r"Fix: ask for fewer pixels.*run gbx_rst_binpoints_agg in a notebook",
    ):
        tier_b.rst_binpoints_finalize(
            [{"col": 0, "row": 0, "value": 1.0}],
            0.0,
            0.0,
            1.0,
            1.0,
            100000,
            100000,
            4326,
        )


def test_the_handler_gives_the_tile_the_array_twin_gives_for_the_same_points():
    pts = [(0.5, 0.5, 1.0), (0.6, 0.4, 5.0), (1.5, 0.5, 7.0)]
    twin = tier_b.rst_binpoints(
        [p[0] for p in pts],
        [p[1] for p in pts],
        [p[2] for p in pts],
        0.0,
        0.0,
        2.0,
        1.0,
        2,
        1,
        4326,
        "max",
    )
    got = tier_b.rst_binpoints_finalize(
        [{"col": 0, "row": 0, "value": 5.0}, {"col": 1, "row": 0, "value": 7.0}],
        0.0,
        0.0,
        2.0,
        1.0,
        2,
        1,
        4326,
    )
    assert bytes(got["raster"]) == bytes(twin["raster"])


def test_the_m7_legs_exist_once_both_forms_do():
    from databricks.labs.gbx.bench import uc_perf

    plan = {
        u.id: u
        for u in uc_perf.perf_plan("cat", "sch", jar_path="/j.jar", wheel_path="/w.whl")
    }
    assert "d1" not in plan["m7"].skipped
    labels = {leg.label for leg in plan["m7"].legs}
    assert {f"{k}_{n}" for k in ("twin", "d1") for n in ("100k", "1m", "10m")} <= labels
