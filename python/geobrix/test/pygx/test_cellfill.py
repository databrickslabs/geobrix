"""Light-tier gbx_<grid>_cellfill grouped aggregator tests.

Mirrors heavy CellFillTest assertions (same inputs/expected values) so
cross-tier parity (Task 11) is satisfiable by construction.

Pure-Python tests drive ``_cellfill.fill`` directly (no Spark).
SQL tests drive the registered ``gbx_<grid>_cellfill`` pandas_udf and decode
the returned BINARY payload via ``_cellfill.decode``.

Column-wrapper regression tests drive the *Python wrapper functions* (h3_cellfill,
bng_cellfill, quadbin_cellfill, custom_cellfill) via df.groupBy().agg() — the path
that had the _col(method) bug.

Four grids, same cell fixtures as CellFillTest.scala:
  H3      — centre = h3.latlng_to_cell(51.5, -0.1, 8) (int form)
  Quadbin — centre = quadbin.point_to_cell(-0.1, 51.5, 10)
  BNG     — centre = _bng.point_to_cell_id(530000, 180000, 4)
  Custom  — centre = _custom.point_to_cell_id(conf, 500000, 500000, 3)
            conf   = 0..1e6 x 0..1e6, splits=2, root=100000, srid=27700
"""

import pytest

h3 = pytest.importorskip("h3", reason="h3 not installed")
quadbin = pytest.importorskip("quadbin", reason="quadbin not installed")
shapely = pytest.importorskip("shapely", reason="shapely not installed")

from pyspark.sql import functions as _f  # noqa: E402

from databricks.labs.gbx.pygx import _bng, _cellfill, _custom  # noqa: E402
from databricks.labs.gbx.pygx import functions as gx  # noqa: E402

# ---------------------------------------------------------------------------
# Grid fixtures (mirrors CellFillTest.scala grids)
# ---------------------------------------------------------------------------

_CUSTOM_CONF = _custom.CustomGridConf(
    bound_x_min=0,
    bound_x_max=1_000_000,
    bound_y_min=0,
    bound_y_max=1_000_000,
    cell_splits=2,
    root_cell_size_x=100_000,
    root_cell_size_y=100_000,
    srid=27700,
)


def _h3_k_loop(cell_id: int, d: int) -> list:
    return [int(c, 16) for c in h3.grid_ring(h3.int_to_str(cell_id), d)]


def _custom_k_loop(cell_id: int, d: int) -> list:
    return _custom.k_loop(_CUSTOM_CONF, cell_id, d)


def _quadbin_k_loop_fn():
    from databricks.labs.gbx.pygx import _quadbin

    return _quadbin.k_loop


def _grids():
    """[(label, center_int, k_loop_fn)] for all four grids."""
    h3_center = int(h3.latlng_to_cell(51.5, -0.1, 8), 16)
    qb_center = quadbin.point_to_cell(-0.1, 51.5, 10)
    bng_center = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    cust_center = _custom.point_to_cell_id(_CUSTOM_CONF, 500000.0, 500000.0, 3)
    return [
        ("H3", h3_center, _h3_k_loop),
        ("QUADBIN", qb_center, _quadbin_k_loop_fn()),
        ("BNG", bng_center, _bng.k_loop),
        ("CUSTOM", cust_center, _custom_k_loop),
    ]


# ---------------------------------------------------------------------------
# Pure Python tests — _cellfill.fill (no Spark)
# ---------------------------------------------------------------------------


def test_fill_mean_k1_fills_null_all_grids():
    """mean k=1 fills a NULL cell with the neighbour mean (all four grids)."""
    for label, center, k_loop in _grids():
        ring1 = k_loop(center, 1)
        assert len(ring1) >= 2, f"{label}: ring1 too small"
        cells = {center: None}
        cells.update({nb: 5.0 for nb in ring1})
        out = dict(
            _cellfill.fill(cells, k=1, method="mean", power=2.0, k_loop_fn=k_loop)
        )
        assert out[center] == pytest.approx(5.0), f"{label}: center not filled"
        for nb in ring1:
            assert out[nb] == pytest.approx(5.0), f"{label}: neighbour changed"


def test_fill_null_with_no_valid_neighbour_stays_null():
    """A NULL cell with no valid neighbour within k rings stays NULL (all four grids)."""
    for label, center, k_loop in _grids():
        cells = {center: None}
        out = dict(
            _cellfill.fill(cells, k=3, method="mean", power=2.0, k_loop_fn=k_loop)
        )
        assert out[center] is None, f"{label}: expected None, got {out[center]}"


def test_fill_idw_k2_power2_exact_value():
    """idw k=2 power=2 weights closer rings higher; exact value (all four grids).

    Exact from CellFillTest:
      mean k=2 = (10 + 20) / 2 = 15.0
      idw k=2 power=2 = (10*1^-2 + 20*2^-2) / (1^-2 + 2^-2)
                       = (10 + 5) / 1.25 = 12.0
    """
    for label, center, k_loop in _grids():
        ring1 = k_loop(center, 1)
        ring2 = k_loop(center, 2)
        nb1 = ring1[0]
        nb2 = ring2[0]
        assert nb1 not in ring2, f"{label}: ring1/ring2 not disjoint"
        assert nb2 not in ring1, f"{label}: ring2/ring1 not disjoint"

        cells = {center: None, nb1: 10.0, nb2: 20.0}

        mean_out = dict(
            _cellfill.fill(cells, k=2, method="mean", power=2.0, k_loop_fn=k_loop)
        )
        assert mean_out[center] == pytest.approx(15.0), f"{label}: mean k=2"

        idw_out = dict(
            _cellfill.fill(cells, k=2, method="idw", power=2.0, k_loop_fn=k_loop)
        )
        assert idw_out[center] == pytest.approx(12.0), f"{label}: idw k=2"


def test_fill_k1_idw_equals_mean():
    """k=1 idw == mean (all four grids) — at d=1 all weights are equal."""
    for label, center, k_loop in _grids():
        ring1 = k_loop(center, 1)
        assert len(ring1) >= 2, f"{label}: ring1 too small"
        cells = {center: None, ring1[0]: 10.0, ring1[1]: 20.0}

        mean_c = dict(
            _cellfill.fill(cells, k=1, method="mean", power=2.0, k_loop_fn=k_loop)
        )[center]
        idw_c = dict(
            _cellfill.fill(cells, k=1, method="idw", power=2.0, k_loop_fn=k_loop)
        )[center]

        assert mean_c == pytest.approx(15.0), f"{label}: mean k=1"
        assert idw_c == pytest.approx(mean_c), f"{label}: k=1 idw != mean"


def test_fill_valid_cells_pass_through_unchanged():
    """Valid (non-NULL) cells pass through unchanged (all four grids)."""
    for label, center, k_loop in _grids():
        ring1 = k_loop(center, 1)
        cells = {center: 7.0}
        cells.update({nb: 5.0 for nb in ring1})

        for method in ("mean", "idw"):
            out = dict(
                _cellfill.fill(cells, k=2, method=method, power=2.0, k_loop_fn=k_loop)
            )
            assert out[center] == pytest.approx(
                7.0
            ), f"{label} {method}: center changed"


def test_fill_k0_is_noop():
    """k=0 performs no fill; all cells — including NULL ones — pass through unchanged."""
    for label, center, k_loop in _grids():
        ring1 = k_loop(center, 1)
        cells = {center: None}
        cells.update({nb: 5.0 for nb in ring1})
        out = dict(
            _cellfill.fill(cells, k=0, method="mean", power=2.0, k_loop_fn=k_loop)
        )
        assert out[center] is None, f"{label}: k=0 should not fill center"
        for nb in ring1:
            assert out[nb] == pytest.approx(5.0), f"{label}: k=0 changed neighbour"


def test_a_repeated_cell_keeps_its_largest_value_in_any_row_order():
    """A repeated cell id fills from its largest non-null value, whatever the row order.

    Drives the four grouped aggregators with every order of the centre cell's three
    rows (7.0, 99.0, NULL) beside five valid neighbours; "the last row wins" would give
    7.0, or a value filled from the neighbours, for some orders.
    """
    import itertools

    import pandas as pd
    from pyspark.sql import Row

    conf = Row(**_CUSTOM_CONF.__dict__)
    udfs = {
        "H3": (gx._h3_cellfill_agg_udf, lambda c: c),
        "QUADBIN": (gx._quadbin_cellfill_agg_udf, lambda c: c),
        "BNG": (gx._bng_cellfill_agg_udf, lambda c: _bng.format(c)),
        "CUSTOM": (gx._custom_cellfill_agg_udf, lambda c: c),
    }
    for label, center, k_loop in _grids():
        udf, spell = udfs[label]
        neighbours = [(nb, 5.0) for nb in k_loop(center, 1)[:5]]
        outcomes = set()
        for order in itertools.permutations([7.0, 99.0, None]):
            rows = [(center, v) for v in order] + neighbours
            args = [
                pd.Series([spell(c) for c, _ in rows]),
                pd.Series([v for _, v in rows], dtype="float64"),
            ]
            if label == "CUSTOM":
                args.append(pd.Series([conf] * len(rows)))
            args += [pd.Series([1] * len(rows)), pd.Series(["mean"] * len(rows))]
            args.append(pd.Series([2.0] * len(rows)))
            out = dict(_cellfill.decode(udf.func(*args)))
            assert out[center] == pytest.approx(99.0), f"{label} {order}: {out[center]}"
            outcomes.add(tuple(sorted(out.items())))
        assert len(outcomes) == 1, f"{label}: the fill depends on the row order"


# ---------------------------------------------------------------------------
# encode/decode roundtrip
# ---------------------------------------------------------------------------


def test_encode_decode_roundtrip():
    """encode -> decode round-trip is lossless."""
    data = [(100, 5.0), (200, None), (300, -3.14)]
    blob = _cellfill.encode(data)
    assert isinstance(blob, bytes)
    decoded = _cellfill.decode(blob)
    assert len(decoded) == 3
    cell_ids = [cid for cid, _ in decoded]
    assert cell_ids == [100, 200, 300]
    assert decoded[0][1] == pytest.approx(5.0)
    assert decoded[1][1] is None
    assert decoded[2][1] == pytest.approx(-3.14)


# ---------------------------------------------------------------------------
# Spark SQL tests — one per grid
# ---------------------------------------------------------------------------


def _decode_result(row_result) -> dict:
    """Decode a grouped-agg BINARY result row to {cellid_int: value_or_None}."""
    blob = row_result[0]
    if blob is None:
        return {}
    return dict(_cellfill.decode(bytes(blob)))


def test_h3_cellfill_sql_mean_k1(spark):
    """gbx_h3_cellfill SQL: mean k=1 fills NULL center from ring1 neighbours."""
    gx.register(spark)
    center_str = h3.latlng_to_cell(51.5, -0.1, 8)
    center = int(center_str, 16)
    ring1 = [int(c, 16) for c in h3.grid_ring(center_str, 1)]

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")
    df.createOrReplaceTempView("h3_fill_test")

    result = spark.sql(
        "SELECT gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0) AS v "
        "FROM h3_fill_test GROUP BY grp"
    ).collect()
    assert len(result) == 1
    decoded = _decode_result(result[0])
    assert decoded[center] == pytest.approx(5.0)
    for nb in ring1:
        assert decoded[nb] == pytest.approx(5.0)


def test_h3_cellfill_sql_no_valid_neighbour_stays_null(spark):
    """gbx_h3_cellfill SQL: NULL cell with no valid neighbour stays NULL."""
    gx.register(spark)
    center = int(h3.latlng_to_cell(51.5, -0.1, 8), 16)
    df = spark.createDataFrame(
        [(1, center, None)], "grp int, cellid long, value double"
    )
    df.createOrReplaceTempView("h3_nonb")
    result = spark.sql(
        "SELECT gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0) AS v FROM h3_nonb GROUP BY grp"
    ).collect()
    decoded = _decode_result(result[0])
    assert decoded[center] is None


def test_quadbin_cellfill_sql_mean_k1(spark):
    """gbx_quadbin_cellfill SQL: mean k=1 fills NULL center from ring1 neighbours."""
    from databricks.labs.gbx.pygx import _quadbin

    gx.register(spark)
    center = quadbin.point_to_cell(-0.1, 51.5, 10)
    ring1 = _quadbin.k_loop(center, 1)

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")
    df.createOrReplaceTempView("qb_fill_test")

    result = spark.sql(
        "SELECT gbx_quadbin_cellfill(cellid, value, 1, 'mean', 2.0) AS v "
        "FROM qb_fill_test GROUP BY grp"
    ).collect()
    assert len(result) == 1
    decoded = _decode_result(result[0])
    assert decoded[center] == pytest.approx(5.0)
    for nb in ring1:
        assert decoded[nb] == pytest.approx(5.0)


def test_bng_cellfill_sql_mean_k1(spark):
    """gbx_bng_cellfill SQL: mean k=1 fills NULL center from ring1 neighbours."""
    gx.register(spark)
    center_int = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    center_str = _bng.format(center_int)
    ring1_int = _bng.k_loop(center_int, 1)
    ring1_str = [_bng.format(c) for c in ring1_int]

    rows = [(1, center_str, None)] + [(1, nb, 5.0) for nb in ring1_str]
    df = spark.createDataFrame(rows, "grp int, cellid string, value double")
    df.createOrReplaceTempView("bng_fill_test")

    result = spark.sql(
        "SELECT gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0) AS v "
        "FROM bng_fill_test GROUP BY grp"
    ).collect()
    assert len(result) == 1
    decoded = _decode_result(result[0])
    # Binary encodes int cell IDs; verify center and ring1 by int key
    assert decoded[center_int] == pytest.approx(5.0)
    for nb_int in ring1_int:
        assert decoded[nb_int] == pytest.approx(5.0)


def test_bng_cellfill_sql_no_valid_neighbour_stays_null(spark):
    """gbx_bng_cellfill SQL: NULL cell with no valid neighbour stays NULL."""
    gx.register(spark)
    center_int = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    center_str = _bng.format(center_int)
    df = spark.createDataFrame(
        [(1, center_str, None)], "grp int, cellid string, value double"
    )
    df.createOrReplaceTempView("bng_nonb")
    result = spark.sql(
        "SELECT gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0) AS v FROM bng_nonb GROUP BY grp"
    ).collect()
    decoded = _decode_result(result[0])
    assert decoded[center_int] is None


def test_custom_cellfill_sql_mean_k1(spark):
    """gbx_custom_cellfill SQL: mean k=1 fills NULL center from ring1 neighbours."""
    gx.register(spark)
    center = _custom.point_to_cell_id(_CUSTOM_CONF, 500000.0, 500000.0, 3)
    ring1 = _custom.k_loop(_CUSTOM_CONF, center, 1)

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")
    df.createOrReplaceTempView("cust_fill_test")

    grid_sql = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 100000, 100000, 27700)"
    result = spark.sql(
        f"SELECT gbx_custom_cellfill(cellid, value, {grid_sql}, 1, 'mean', 2.0) AS v "
        "FROM cust_fill_test GROUP BY grp"
    ).collect()
    assert len(result) == 1
    decoded = _decode_result(result[0])
    assert decoded[center] == pytest.approx(5.0)
    for nb in ring1:
        assert decoded[nb] == pytest.approx(5.0)


def test_custom_cellfill_sql_idw_k2(spark):
    """gbx_custom_cellfill SQL: idw k=2 power=2 gives exact 12.0 for the center cell."""
    gx.register(spark)
    center = _custom.point_to_cell_id(_CUSTOM_CONF, 500000.0, 500000.0, 3)
    ring1 = _custom.k_loop(_CUSTOM_CONF, center, 1)
    ring2 = _custom.k_loop(_CUSTOM_CONF, center, 2)
    nb1, nb2 = ring1[0], ring2[0]

    rows = [(1, center, None), (1, nb1, 10.0), (1, nb2, 20.0)]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")
    df.createOrReplaceTempView("cust_idw_test")

    grid_sql = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 100000, 100000, 27700)"
    result = spark.sql(
        f"SELECT gbx_custom_cellfill(cellid, value, {grid_sql}, 2, 'idw', 2.0) AS v "
        "FROM cust_idw_test GROUP BY grp"
    ).collect()
    decoded = _decode_result(result[0])
    assert decoded[center] == pytest.approx(12.0)


def test_h3_cellfill_sql_idw_k2(spark):
    """gbx_h3_cellfill SQL: idw k=2 power=2 gives exact 12.0 for the center cell."""
    gx.register(spark)
    center_str = h3.latlng_to_cell(51.5, -0.1, 8)
    center = int(center_str, 16)
    ring1 = [int(c, 16) for c in h3.grid_ring(center_str, 1)]
    ring2 = [int(c, 16) for c in h3.grid_ring(center_str, 2)]
    nb1, nb2 = ring1[0], ring2[0]

    rows = [(1, center, None), (1, nb1, 10.0), (1, nb2, 20.0)]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")
    df.createOrReplaceTempView("h3_idw_test")

    result = spark.sql(
        "SELECT gbx_h3_cellfill(cellid, value, 2, 'idw', 2.0) AS v "
        "FROM h3_idw_test GROUP BY grp"
    ).collect()
    decoded = _decode_result(result[0])
    assert decoded[center] == pytest.approx(12.0)


# ---------------------------------------------------------------------------
# Column wrapper regression tests — method string-arg bug
#
# Bug (now fixed): the four cellfill Column wrappers (h3_cellfill,
# bng_cellfill, quadbin_cellfill, custom_cellfill) converted the method
# argument with ``_col(method)``.  Because ``_col`` passes str values through
# unchanged (intended for column *names*), passing method="mean" produced
# ``f.col("mean")`` — a reference to a column named "mean".  At collect()
# Spark raised UNRESOLVED_COLUMN / "cannot resolve 'mean'" on every call that
# used the default method.
#
# Fix: ``_method = f.lit(method) if isinstance(method, str) else _col(method)``
#
# WHY the existing SQL-string tests do NOT catch this:
#   spark.sql("... gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0) ...")
#   The single-quoted 'mean' is a SQL string literal — it never passes
#   through the Python ``_col()`` conversion.  Only the Python Column
#   wrappers (df.groupBy().agg(gx.h3_cellfill(...))) hit the buggy path.
#
# Each test below uses df.groupBy("grp").agg(gx.<grid>_cellfill(...)) —
# the Column wrapper path — with an explicit keyword method= argument.
# Reverting the fix to ``_col(method)`` makes every test here fail with
# AnalysisException / UNRESOLVED_COLUMN at collect() time.
# ---------------------------------------------------------------------------


def _decode_blob(blob) -> dict:
    """Decode a BINARY cellfill payload to {cellid_int: value_or_None}."""
    if blob is None:
        return {}
    return dict(_cellfill.decode(bytes(blob)))


def test_h3_cellfill_wrapper_method_mean(spark):
    """Regression: h3_cellfill Column wrapper with method='mean' executes without UNRESOLVED_COLUMN.

    The old ``_col('mean')`` resolved "mean" as a column name; the fix wraps
    string values with ``f.lit()``.  Asserts non-vacuous output (center filled).
    """
    gx.register(spark)
    center_str = h3.latlng_to_cell(51.5, -0.1, 8)
    center = int(center_str, 16)
    ring1 = [int(c, 16) for c in h3.grid_ring(center_str, 1)]

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")

    # Use the Python Column wrapper path — NOT a raw SQL string.
    result = (
        df.groupBy("grp")
        .agg(gx.h3_cellfill("cellid", "value", k=1, method="mean").alias("v"))
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    assert decoded[center] == pytest.approx(
        5.0
    ), "center not filled; wrapper method arg failed"
    for nb in ring1:
        assert decoded[nb] == pytest.approx(
            5.0
        ), f"ring1 cell {nb} changed unexpectedly"


def test_h3_cellfill_wrapper_method_idw(spark):
    """Regression: h3_cellfill Column wrapper with method='idw' executes without UNRESOLVED_COLUMN.

    Same bug path as mean; verifies both method string values go through f.lit().
    Expected: idw k=2 power=2 with ring1[0]=10 ring2[0]=20 → center=12.0.
    """
    gx.register(spark)
    center_str = h3.latlng_to_cell(51.5, -0.1, 8)
    center = int(center_str, 16)
    ring1 = [int(c, 16) for c in h3.grid_ring(center_str, 1)]
    ring2 = [int(c, 16) for c in h3.grid_ring(center_str, 2)]
    nb1, nb2 = ring1[0], ring2[0]

    rows = [(1, center, None), (1, nb1, 10.0), (1, nb2, 20.0)]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")

    result = (
        df.groupBy("grp")
        .agg(gx.h3_cellfill("cellid", "value", k=2, method="idw", power=2.0).alias("v"))
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    assert decoded[center] == pytest.approx(12.0), "idw k=2 power=2 center value wrong"


def test_bng_cellfill_wrapper_method_mean(spark):
    """Regression: bng_cellfill Column wrapper with method='mean' executes without UNRESOLVED_COLUMN.

    BNG uses string cell IDs — covers the string-cellid grid type alongside H3's int IDs.
    """
    gx.register(spark)
    center_int = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    center_str = _bng.format(center_int)
    ring1_int = _bng.k_loop(center_int, 1)
    ring1_str = [_bng.format(c) for c in ring1_int]

    rows = [(1, center_str, None)] + [(1, nb, 5.0) for nb in ring1_str]
    df = spark.createDataFrame(rows, "grp int, cellid string, value double")

    result = (
        df.groupBy("grp")
        .agg(gx.bng_cellfill("cellid", "value", k=1, method="mean").alias("v"))
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    # BINARY encodes int keys; BNG string IDs are converted to int64 by the UDF.
    assert decoded[center_int] == pytest.approx(
        5.0
    ), "BNG center not filled; wrapper method arg failed"
    for nb_int in ring1_int:
        assert decoded[nb_int] == pytest.approx(
            5.0
        ), f"BNG ring1 cell {nb_int} changed unexpectedly"


def test_bng_cellfill_wrapper_method_idw(spark):
    """Regression: bng_cellfill Column wrapper with method='idw' executes without UNRESOLVED_COLUMN.

    Expected: idw k=2 power=2 with ring1[0]=10 ring2[0]=20 → center=12.0 (BNG string IDs).
    """
    gx.register(spark)
    center_int = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    center_str = _bng.format(center_int)
    ring1_int = _bng.k_loop(center_int, 1)
    ring2_int = _bng.k_loop(center_int, 2)
    nb1_str = _bng.format(ring1_int[0])
    nb2_str = _bng.format(ring2_int[0])

    rows = [(1, center_str, None), (1, nb1_str, 10.0), (1, nb2_str, 20.0)]
    df = spark.createDataFrame(rows, "grp int, cellid string, value double")

    result = (
        df.groupBy("grp")
        .agg(
            gx.bng_cellfill("cellid", "value", k=2, method="idw", power=2.0).alias("v")
        )
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    assert decoded[center_int] == pytest.approx(
        12.0
    ), "BNG idw k=2 power=2 center value wrong"


def test_quadbin_cellfill_wrapper_method_mean(spark):
    """Regression: quadbin_cellfill Column wrapper with method='mean' executes without UNRESOLVED_COLUMN."""
    from databricks.labs.gbx.pygx import _quadbin

    gx.register(spark)
    center = quadbin.point_to_cell(-0.1, 51.5, 10)
    ring1 = _quadbin.k_loop(center, 1)

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")

    result = (
        df.groupBy("grp")
        .agg(gx.quadbin_cellfill("cellid", "value", k=1, method="mean").alias("v"))
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    assert decoded[center] == pytest.approx(
        5.0
    ), "Quadbin center not filled; wrapper method arg failed"
    for nb in ring1:
        assert decoded[nb] == pytest.approx(
            5.0
        ), f"Quadbin ring1 cell {nb} changed unexpectedly"


def test_custom_cellfill_wrapper_method_mean(spark):
    """Regression: custom_cellfill Column wrapper with method='mean' executes without UNRESOLVED_COLUMN.

    The grid argument is built via ``_f.expr(...)`` so the Column wrapper receives
    a real Column object for ``grid``, exercising the full wrapper code path.
    """
    gx.register(spark)
    center = _custom.point_to_cell_id(_CUSTOM_CONF, 500000.0, 500000.0, 3)
    ring1 = _custom.k_loop(_CUSTOM_CONF, center, 1)

    rows = [(1, center, None)] + [(1, nb, 5.0) for nb in ring1]
    df = spark.createDataFrame(rows, "grp int, cellid long, value double")

    # Build the custom-grid struct as a Column expression (all literal args).
    grid_col = _f.expr(
        "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 100000, 100000, 27700)"
    )

    result = (
        df.groupBy("grp")
        .agg(
            gx.custom_cellfill(
                "cellid", "value", grid=grid_col, k=1, method="mean"
            ).alias("v")
        )
        .collect()
    )
    assert len(result) == 1
    decoded = _decode_blob(result[0]["v"])
    assert decoded[center] == pytest.approx(
        5.0
    ), "Custom center not filled; wrapper method arg failed"
    for nb in ring1:
        assert decoded[nb] == pytest.approx(
            5.0
        ), f"Custom ring1 cell {nb} changed unexpectedly"
