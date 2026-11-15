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
BKR, BKL = "gbx_bng_kring", "gbx_bng_kloop"


def _kernel(name):
    """The SQL kernel of a row, or the candidate that waits for its numbers (uc.candidates)."""
    from databricks.labs.gbx.uc import candidates

    return sqlbody.l4_source(manifest.load().functions[name]) or candidates.source(name)


def _install(spark, name):
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
    spark.sql(ddl.local_function_ddl(manifest.load().functions[name], _kernel(name)))


def _drop(spark, name):
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")


def _rows(table, name):
    return [c for c in oracles.TABLES[table] if c.fn == name and "uc" in c.surfaces]


def _lit(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


ADOPTED = [
    (QDIST, "GRID_SCALAR"),
    (QKR, "GRID_ARRAY"),
    (QKL, "GRID_ARRAY"),
]


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


@pytest.mark.parametrize("name", [QKR, QKL])
def test_quadbin_rings_walk_only_the_world_not_the_unclamped_square(spark, name):
    """A huge k at a low zoom passes the size guard (the world is one tile); the loops must be clamped to the world too,
    or Spark builds a (2k+1)^2 square first."""
    import quadbin

    world = quadbin.tile_to_cell((0, 0, 0))
    _install(spark, name)
    try:
        got = spark.sql(f"SELECT {name}({world}L, 1000000) AS r").first()["r"]
    finally:
        _drop(spark, name)
    assert list(got) == ([world] if name == QKR else [])


# ---- gbx_bng_kring and gbx_bng_kloop ---------------------------------------------------------------------------------


def _bng_cells(rnd, n, resolutions=(1, 2, 3, 4, 5, 6, -2, -3, -4, -5, -6)):
    """Cells of every resolution whose k <= 3 ring stays inside the grid (0..700 km by 0..1300 km)."""
    from databricks.labs.gbx.pygx import _bng

    out = []
    for _ in range(n):
        res = rnd.choice(resolutions)
        edge = _bng.get_edge_size(res)
        lo_e, hi_e = 3 * edge, 700000 - 4 * edge
        lo_n, hi_n = 3 * edge, 1300000 - 4 * edge
        e, nn = rnd.randint(lo_e, max(lo_e, hi_e)), rnd.randint(lo_n, max(lo_n, hi_n))
        out.append(_bng.format(_bng.point_to_cell_id(float(e), float(nn), res)))
    return out


def _run_bng(spark, name, rows):
    _install(spark, name)
    try:
        return (
            spark.createDataFrame(
                [(i, c, k) for i, (c, k) in enumerate(rows)], "i int, c string, k int"
            )
            .selectExpr("i", f"{name}(c, k) AS r")
            .collect()
        )
    finally:
        _drop(spark, name)


@pytest.mark.parametrize("name", [BKR, BKL])
def test_bng_rings_equal_the_python_core_inland(spark, name):
    # the kernels are candidates (uc.candidates): equal to the core, but not faster than light locally
    rnd = random.Random(21 if name == BKR else 22)
    rows = [(c, k) for c in _bng_cells(rnd, 80) for k in (0, 1, 2, 3)]
    got = _run_bng(spark, name, rows)
    py = getattr(handlers, name[len("gbx_") :])
    wrong = [
        (rows[r["i"]], r["r"][:3])
        for r in got
        if list(r["r"]) != py(*rows[r["i"]])  # the same cells in the same order
    ]
    assert len(got) == len(rows) and wrong[:3] == []


def _ring_leaves_the_grid(cell, k):
    """True when the square of distance k around the cell has a corner outside 0..700 km by 0..1300 km."""
    from databricks.labs.gbx.pygx import _bng

    digits = _bng.cell_digits(_bng.parse(cell))
    edge = _bng.get_edge_size(_bng.get_resolution_from_digits(digits))
    x, y = _bng.get_x(digits, edge), _bng.get_y(digits, edge)
    return (
        x - k * edge < 0
        or y - k * edge < 0
        or x + (k + 1) * edge > 700000
        or y + (k + 1) * edge > 1300000
    )


def _edge_cells():
    from databricks.labs.gbx.pygx import _bng

    out = ["S", "T", "N", "O", "H", "J", "SV", "TQ", "TA", "HP"]
    for res in (1, 2, 3, 4, 6, -2, -4, -6):
        for x, y in ((0, 0), (699999, 0), (0, 1299999), (699999, 1299999), (350000, 0)):
            out.append(_bng.format(_bng.point_to_cell_id(float(x), float(y), res)))
    return list(dict.fromkeys(out))


@pytest.mark.parametrize("name", [BKR, BKL])
def test_bng_rings_at_the_edge_of_the_grid_are_the_documented_divergence(spark, name):
    """Pinned: a ring that stays on the grid equals the Python core even beside the edge; a ring that leaves it differs,
    because the core's id arithmetic gives letters for a point off the grid (negative or past 700 km) that are no cell, and
    the SQL form clamps the letter index to the grid's own letters. The single-letter 500 km cells are always in that case.
    """
    rows = [(c, k) for c in _edge_cells() for k in (0, 1, 2, 3)]
    got = _run_bng(spark, name, rows)
    py = getattr(handlers, name[len("gbx_") :])
    inside_wrong, off_equal = [], 0
    for r in got:
        cell, k = rows[r["i"]]
        same = list(r["r"]) == py(cell, k)
        if _ring_leaves_the_grid(cell, k):
            off_equal += same
        elif not same:
            inside_wrong.append((cell, k))
    assert inside_wrong == []
    assert off_equal < len(got)  # the divergence exists: it is pinned, not hidden


@pytest.mark.parametrize("name", [BKR, BKL])
def test_bng_rings_refuse_a_ring_over_the_cap_with_the_python_text(spark, name):
    py = getattr(handlers, name[len("gbx_") :])
    _install(spark, name)
    try:
        for k in (3_000_000, 2_000_000_000):
            with pytest.raises(Exception) as want:
                py("TQ3080", k)
            with pytest.raises(Exception) as got:
                spark.sql(f"SELECT size({name}('TQ3080', {k})) AS n").collect()
            assert str(want.value) in str(got.value), k
    finally:
        _drop(spark, name)


def test_bng_kloop_refuses_a_negative_k_and_kring_gives_the_cell_alone(spark):
    _install(spark, BKL)
    _install(spark, BKR)
    try:
        with pytest.raises(Exception) as want:
            handlers.bng_kloop("TQ3080", -1)
        with pytest.raises(Exception) as got:
            spark.sql(f"SELECT {BKL}('TQ3080', -1) AS r").collect()
        assert "k must be a whole number of at least 0; got -1" in str(want.value)
        assert "k must be a whole number of at least 0; got -1" in str(got.value)
        assert handlers.bng_kring("TQ3080", -1) == ["TQ3080"]
        assert spark.sql(f"SELECT {BKR}('TQ3080', -1) AS r").first()["r"] == ["TQ3080"]
        for name in (BKR, BKL):  # a string that is no cell and a NULL argument are NULL
            for arg in ("'ZZ12'", "'TQ30x0'", "''", "CAST(NULL AS STRING)"):
                assert spark.sql(f"SELECT {name}({arg}, 1) AS r").first()["r"] is None
            assert (
                spark.sql(f"SELECT {name}('TQ3080', CAST(NULL AS INT)) AS r").first()[
                    "r"
                ]
                is None
            )
    finally:
        _drop(spark, BKL)
        _drop(spark, BKR)


def test_bng_rings_of_an_odd_digit_string_and_a_cell_finer_than_1_m_are_the_documented_divergence(
    spark,
):
    """Pinned: the core reads an odd number of digits as two overlapping halves and gives the ring of a cell that no
    id names; the SQL form says it is no cell (NULL). A cell finer than 1 m raises with a Fix on SQL, where the core
    crashes with a KeyError."""
    odd = handlers.bng_kring("TQ308", 1)
    assert odd and odd[0] != "TQ308"
    _install(spark, BKR)
    try:
        assert spark.sql(f"SELECT {BKR}('TQ308', 1) AS r").first()["r"] is None
        with pytest.raises(KeyError):
            handlers.bng_kring("TQ308080808080", 1)
        with pytest.raises(Exception, match="finer than 1 m.*Fix:"):
            spark.sql(f"SELECT {BKR}('TQ308080808080', 1) AS r").collect()
    finally:
        _drop(spark, BKR)


@pytest.mark.parametrize("name", [BKR, BKL])
def test_bng_rings_fail_when_a_corner_of_the_walk_changes(spark, name):
    """Fail-on-revert of the walk: moving the first corner of the loop breaks the inland sweep."""
    rows = [(c, 2) for c in _bng_cells(random.Random(5), 12)]
    py = getattr(handlers, name[len("gbx_") :])
    text, helpers = _kernel(name)
    marker = "named_struct('a', (t.x - "
    assert marker in text
    reverted = (text.replace(marker, "named_struct('a', (t.x + ", 1), helpers)

    def run(kernel):
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {name}")
        spark.sql(ddl.local_function_ddl(manifest.load().functions[name], kernel))
        return (
            spark.createDataFrame(
                [(i, c, k) for i, (c, k) in enumerate(rows)], "i int, c string, k int"
            )
            .selectExpr("i", f"{name}(c, k) AS r")
            .collect()
        )

    try:
        assert all(list(r["r"]) == py(*rows[r["i"]]) for r in run((text, helpers)))
        assert any(list(r["r"]) != py(*rows[r["i"]]) for r in run(reverted))
    finally:
        _drop(spark, name)


@pytest.mark.parametrize("name", [BKR, BKL])
def test_bng_ring_candidates_pass_every_grid_array_row(spark, tmp_path, name):
    """The candidate body (uc.candidates) is judged on the function's GRID_ARRAY rows as an adopted kernel would be."""
    from databricks.labs.gbx.uc import candidates

    cases = _rows("GRID_ARRAY", name)
    assert len(cases) >= 2
    ran = oracles.run_sql_kernels(
        spark, cases, sources={name: candidates.source(name)}, tmpdir=str(tmp_path)
    )
    assert len(ran) == len(cases) and [m for _, m in ran if m] == []
