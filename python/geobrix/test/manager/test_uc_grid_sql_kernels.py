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
QKR, QKL = "gbx_quadbin_kring", "gbx_quadbin_kloop"


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


ADOPTED = [(QDIST, "GRID_SCALAR"), (QKR, "GRID_ARRAY"), (QKL, "GRID_ARRAY")]


@pytest.mark.parametrize("name,table", ADOPTED)
def test_each_adopted_row_has_one_sql_text_for_the_session_and_uc(name, table):
    """Fails when a row loses its kernel or its L0 text."""
    fn = manifest.load().functions[name]
    assert fn.levels["L4"]["sql"] == "L0" and "sql" in fn.levels["L0"], name
    assert "handler" in fn.levels["L4"], name  # the Python twin stays the reference
    assert ddl.chosen_form(fn, None) == "sql"


@pytest.mark.parametrize("name,table", ADOPTED)
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


# ---- gbx_quadbin_kring and gbx_quadbin_kloop -------------------------------------------------------------------------


def _inland_cells(rnd, n):
    """Cells whose k <= 3 ring stays inside the world at their zoom."""
    import quadbin

    out = []
    for _ in range(n):
        z = rnd.randint(3, 26)
        side = 1 << z
        out.append(
            quadbin.tile_to_cell(
                (rnd.randint(3, side - 4), rnd.randint(3, side - 4), z)
            )
        )
    return out


@pytest.mark.parametrize("name,k_list", [(QKR, (0, 1, 2, 3)), (QKL, (0, 1, 2, 3))])
def test_quadbin_rings_equal_the_python_core_inland(spark, name, k_list):
    rnd = random.Random(8 if name == QKR else 9)
    rows = [(c, k) for c in _inland_cells(rnd, 60) for k in k_list]
    _install(spark, name)
    try:
        got = (
            spark.createDataFrame(
                [(i, c, k) for i, (c, k) in enumerate(rows)], "i int, c bigint, k int"
            )
            .selectExpr("i", f"{name}(c, k) AS r")
            .collect()
        )
    finally:
        _drop(spark, name)
    py = getattr(handlers, name[len("gbx_") :])
    wrong = [
        (rows[r["i"]], len(r["r"] or []))
        for r in got
        if list(r["r"]) != py(*rows[r["i"]])  # the same cells in the same order
    ]
    assert len(got) == len(rows) and wrong[:3] == []


@pytest.mark.parametrize("name", [QKR, QKL])
def test_quadbin_rings_refuse_a_bad_k_and_a_ring_over_the_cap_with_the_python_text(
    spark, name
):
    import quadbin

    cell = quadbin.tile_to_cell((40000000, 30000000, 26))
    py = getattr(handlers, name[len("gbx_") :])
    _install(spark, name)
    try:
        for k in (-1, 5000, 2000):
            with pytest.raises(Exception) as want:
                py(cell, k)
            with pytest.raises(Exception) as got:
                spark.sql(f"SELECT size({name}({cell}L, {k})) AS n").collect()
            assert str(want.value) in str(got.value), k
        # an id that is no cell is NULL, a NULL argument is NULL
        assert spark.sql(f"SELECT {name}(0L, 1) AS r").first()["r"] is None
        assert (
            spark.sql(f"SELECT {name}({cell}L, CAST(NULL AS INT)) AS r").first()["r"]
            is None
        )
    finally:
        _drop(spark, name)


def test_quadbin_rings_at_the_edge_of_the_world_are_the_documented_divergence(spark):
    """Pinned: the library walks a ring from its top-left corner and crashes (TypeError) when a neighbour is off the
    world; the SQL form returns the cells that exist."""
    import quadbin

    corner = quadbin.tile_to_cell((0, 0, 5))
    with pytest.raises(TypeError):
        handlers.quadbin_kring(corner, 1)
    _install(spark, QKR)
    try:
        got = spark.sql(f"SELECT {QKR}({corner}L, 1) AS r").first()["r"]
    finally:
        _drop(spark, QKR)
    assert sorted(got) == sorted(
        quadbin.tile_to_cell((x, y, 5)) for x in (0, 1) for y in (0, 1)
    )
