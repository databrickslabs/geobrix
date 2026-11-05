"""SQL kernels of the quadbin and BNG grid scalars (manifest L0/L4 `sql`): each is judged on a local Spark against the rows
of its oracle table and against the Python core over a sweep. The bodies use only core built-ins (bit operations, string
and array functions, raise_error), so the host proves them; the timing against light is a separate local measurement.
"""

import random

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.uc import ddl, handlers, sqlbody

QDIST = "gbx_quadbin_distance"


def _kernel(name):
    return sqlbody.l4_source(manifest.load().functions[name])


def _install(spark, name):
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
    spark.sql(ddl.local_function_ddl(manifest.load().functions[name], _kernel(name)))


def _drop(spark, name):
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")


def _rows(table, name):
    return [c for c in oracles.TABLES[table] if c.fn == name and "uc" in c.surfaces]


def _lit(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


@pytest.mark.parametrize("name,table", [(QDIST, "GRID_SCALAR")])
def test_each_adopted_row_has_one_sql_text_for_the_session_and_uc(name, table):
    """Fails when a row loses its kernel or its L0 text."""
    fn = manifest.load().functions[name]
    assert fn.levels["L4"]["sql"] == "L0" and "sql" in fn.levels["L0"], name
    assert "handler" in fn.levels["L4"], name  # the Python twin stays the reference
    assert ddl.chosen_form(fn, None) == "sql"


@pytest.mark.parametrize("name,table", [(QDIST, "GRID_SCALAR")])
def test_a_kernel_passes_every_uc_oracle_row_of_its_function(
    spark, tmp_path, name, table
):
    cases = _rows(table, name)
    assert len(cases) >= 4, name
    ran = oracles.run_sql_kernels(spark, cases, tmpdir=str(tmp_path))
    assert len(ran) == len(cases)
    assert [m for _, m in ran if m] == []


# ---- gbx_quadbin_distance -----------------------------------------------------------------------------------------


def _qb_cells(rnd, n):
    import quadbin

    out = []
    for _ in range(n):
        z = rnd.randint(0, 26)
        side = 1 << z
        out.append(
            (quadbin.tile_to_cell((rnd.randrange(side), rnd.randrange(side), z)), z)
        )
    return out


def test_quadbin_distance_equals_the_python_core_at_every_zoom(spark):
    import quadbin

    rnd = random.Random(3)
    pairs = []
    for z in range(27):
        side = 1 << z
        for _ in range(40):
            a = (rnd.randrange(side), rnd.randrange(side))
            b = (
                (
                    min(side - 1, a[0] + rnd.randint(0, 3)),
                    max(0, a[1] - rnd.randint(0, 3)),
                )
                if rnd.random() < 0.5
                else (rnd.randrange(side), rnd.randrange(side))
            )
            pairs.append((quadbin.tile_to_cell((*a, z)), quadbin.tile_to_cell((*b, z))))
        pairs.append(
            (
                quadbin.tile_to_cell((0, 0, z)),
                quadbin.tile_to_cell((side - 1, side - 1, z)),
            )
        )
    _install(spark, QDIST)
    try:
        got = (
            spark.createDataFrame(
                [(i, a, b) for i, (a, b) in enumerate(pairs)],
                "i int, a bigint, b bigint",
            )
            .selectExpr("i", f"{QDIST}(a, b) AS d")
            .collect()
        )
    finally:
        _drop(spark, QDIST)
    wrong = [
        (pairs[r["i"]], r["d"], handlers.quadbin_distance(*pairs[r["i"]]))
        for r in got
        if r["d"] != handlers.quadbin_distance(*pairs[r["i"]])
    ]
    assert len(got) == len(pairs) and wrong[:3] == []


def test_quadbin_distance_is_null_for_an_id_that_is_no_cell_and_raises_across_zooms(
    spark,
):
    import quadbin

    good = quadbin.tile_to_cell((5, 7, 10))
    bad = [
        0,
        -good,
        good ^ (1 << 62),  # no header bit
        good ^ (1 << 59),  # another mode
        good ^ 1,  # the footer bits of a zoom 10 cell are not all set
        (27 << 52) | (1 << 62) | (1 << 59),  # zoom 27
        (1 << 63) - 1,
    ]
    _install(spark, QDIST)
    try:
        for b in bad:
            assert handlers.quadbin_distance(b, good) is None, b
            assert spark.sql(f"SELECT {QDIST}({b}L, {good}L) AS d").first()["d"] is None
            assert spark.sql(f"SELECT {QDIST}({good}L, {b}L) AS d").first()["d"] is None
        other = quadbin.tile_to_cell((5, 7, 9))
        with pytest.raises(ValueError) as want:
            handlers.quadbin_distance(good, other)
        with pytest.raises(Exception) as got:
            spark.sql(f"SELECT {QDIST}({good}L, {other}L) AS d").collect()
        assert str(want.value) in str(got.value)  # the Python text, semicolon included
    finally:
        _drop(spark, QDIST)
