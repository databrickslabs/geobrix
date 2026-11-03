"""gbx_<grid>_cellfill cross-tier parity: both tiers return the same BINARY payload.

The payload (documented in ``gridx-functions``): a big-endian Int32 count, then per cell an Int64 id, a present
byte and, when present, a Float64, in ascending id order. A BNG id is the grid's own Int64 number in the payload
in both tiers. ``_cellfill.decode`` reads it in both → ``{int_cellid: value_or_None}``.

Two scenarios per grid:
  - mean/k=1 : center NULL surrounded by valid ring-1 neighbours (all 5.0)
               → center filled with 5.0; isolated NULL (group 2) stays NULL.
  - idw/k=2/power=2 : center NULL, ring1[0]=10.0, ring2[0]=20.0
               → center = (10*1^-2 + 20*2^-2) / (1^-2 + 2^-2) = 12.0;
                  isolated NULL (group 2) stays NULL.

Protocol (mirrors test_parity_quadbin.py):
  Phase 1 — ``gx.register(spark)`` (light PySpark UDFs), collect ALL light results.
  Phase 2 — register all four heavy modules (overwrite the SQL names with JVM
             expressions), collect ALL heavy results.
  Phase 3 — assert parity: same cell set, per-cell value within 1e-9, same NULL set.

Both phases reuse the same temp views (registered before Phase 1).  Heavy register
calls overwrite the catalog entries; temp views are independent of registration.

Heavy requires the geobrix JAR (Scala/JTS). Auto-skips when the JAR is absent under
``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pygx/test_parity_cellfill.py \\
        --log stage3-t11.log
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import pytest

h3 = pytest.importorskip("h3", reason="h3 not installed")
quadbin = pytest.importorskip("quadbin", reason="quadbin not installed")

from databricks.labs.gbx.pygx import _bng, _cellfill, _custom  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pygx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# ---------------------------------------------------------------------------
# Spark fixture (same guard as test_parity_quadbin.py)
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

_GRID_SQL = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 100000, 100000, 27700)"


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:parity --path python/geobrix/test/pygx/test_parity_cellfill.py"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-cellfill-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------


def _decode(blob) -> Dict[int, Optional[float]]:
    """Decode a cellfill BINARY payload → {int_cellid: value_or_None} (the same layout in both tiers)."""
    if blob is None:
        return {}
    return dict(_cellfill.decode(bytes(blob)))


# ---------------------------------------------------------------------------
# Assertion helper
# ---------------------------------------------------------------------------


def _assert_fill_parity(
    light: Dict[int, Optional[float]],
    heavy: Dict[int, Optional[float]],
    ctx: str,
    expected_filled: Dict[int, float],
    expected_null: set,
) -> None:
    """Assert cross-tier parity for one group's fill result.

    Checks:
      1. Same cell-id set (light keys == heavy keys).
      2. Every cell in ``expected_filled`` has the expected value in BOTH tiers
         within 1e-9, and light matches heavy within 1e-9.
      3. Every cell in ``expected_null`` has None in both tiers.

    Deliberately written so a wrong expected value (e.g. 6.0 instead of 5.0)
    causes a pytest.approx failure — the expected constants are mathematically
    derived from the fill semantics and verified against the existing
    CellFillTest.scala/test_cellfill.py unit tests.
    """
    assert set(light) == set(heavy), (
        f"{ctx}: cell-set mismatch\n"
        f"  light_only={sorted(set(light) - set(heavy))}\n"
        f"  heavy_only={sorted(set(heavy) - set(light))}"
    )
    for cid, exp_val in expected_filled.items():
        l_val = light.get(cid)
        h_val = heavy.get(cid)
        assert l_val == pytest.approx(
            exp_val, abs=1e-9
        ), f"{ctx}: light cell {cid} = {l_val!r}; expected {exp_val}"
        assert h_val == pytest.approx(
            exp_val, abs=1e-9
        ), f"{ctx}: heavy cell {cid} = {h_val!r}; expected {exp_val}"
        assert l_val == pytest.approx(h_val, abs=1e-9), (
            f"{ctx}: light/heavy diverge at cell {cid}: light={l_val!r} heavy={h_val!r}"
            " — CROSS-TIER MISMATCH, do not weaken this assertion"
        )
    for cid in expected_null:
        assert (
            light.get(cid) is None
        ), f"{ctx}: light cell {cid} should be None (no valid neighbours); got {light.get(cid)}"
        assert (
            heavy.get(cid) is None
        ), f"{ctx}: heavy cell {cid} should be None (no valid neighbours); got {heavy.get(cid)}"


# ---------------------------------------------------------------------------
# Main parity test — all 4 grids × 2 scenarios in one function
# (light-all-grids first, then heavy-all-grids, to avoid Scala idempotency
# flag preventing re-registration across test boundaries)
# ---------------------------------------------------------------------------


def _register_h3_heavy(spark) -> None:
    """Register gbx_h3_* at L1 (the JAR's expressions) through the manager."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark,
        ("gbx_h3_",),
        only=gbx_manager.names_at_level(("gbx_h3_",), "L1"),
        level="L1",
    )


def test_cellfill_parity_all_grids(spark_with_jar):
    """All 4 grids × 2 scenarios: mean/k=1 and idw/k=2/power=2.

    Phase 1: register light (pygx UDFs), collect ALL results.
    Phase 2: register heavy (JVM, overwrites SQL names), collect ALL results.
    Phase 3: assert parity for each grid × scenario.

    Cell IDs are seeded via each grid's own point/index function so both tiers
    agree on the neighbourhood structure — this is required for the parity to be
    meaningful (cell-set equality, not just value equality).

    H3 note: the heavy leg registers ``gbx_h3_*`` at L1 through ``_register_h3_heavy``.
    """
    from databricks.labs.gbx import manager as gbx_manager

    spark = spark_with_jar

    # -----------------------------------------------------------------------
    # Grid fixtures (cell IDs derived from each grid's own oracle)
    # -----------------------------------------------------------------------

    # H3 — London interior cell at res=8
    h3_center_str = h3.latlng_to_cell(51.5, -0.1, 8)
    h3_center = int(h3_center_str, 16)
    h3_ring1 = [int(c, 16) for c in h3.grid_ring(h3_center_str, 1)]
    h3_ring2 = [int(c, 16) for c in h3.grid_ring(h3_center_str, 2)]
    # Isolated null: a cell from ring 3 — at ring dist 3 from center and not
    # adjacent to any valid cell in group 1 for k<=2.
    h3_isolated = int(h3.grid_ring(h3_center_str, 3)[0], 16)

    # Quadbin — London z10
    from databricks.labs.gbx.pygx import _quadbin

    qb_center = quadbin.point_to_cell(-0.1, 51.5, 10)
    qb_ring1 = _quadbin.k_loop(qb_center, 1)
    qb_ring2 = _quadbin.k_loop(qb_center, 2)
    qb_isolated = _quadbin.k_loop(qb_center, 3)[0]

    # BNG — London 100m cell
    bng_center_int = _bng.point_to_cell_id(530000.0, 180000.0, 4)
    bng_center_str = _bng.format(bng_center_int)
    bng_ring1_int = _bng.k_loop(bng_center_int, 1)
    bng_ring1_str = [_bng.format(c) for c in bng_ring1_int]
    bng_ring2_int = _bng.k_loop(bng_center_int, 2)
    bng_isolated_int = _bng.k_loop(bng_center_int, 3)[0]
    bng_isolated_str = _bng.format(bng_isolated_int)

    # Custom — 500km cell at res=3 within 0..1e6 × 0..1e6 grid
    cust_center = _custom.point_to_cell_id(_CUSTOM_CONF, 500_000.0, 500_000.0, 3)
    cust_ring1 = _custom.k_loop(_CUSTOM_CONF, cust_center, 1)
    cust_ring2 = _custom.k_loop(_CUSTOM_CONF, cust_center, 2)
    cust_isolated = _custom.k_loop(_CUSTOM_CONF, cust_center, 3)[0]

    # -----------------------------------------------------------------------
    # Build temp views
    #
    # Two groups per scenario:
    #   grp=1: center=NULL + neighbours (valid)   →  center gets filled
    #   grp=2: isolated NULL only                 →  stays NULL
    # -----------------------------------------------------------------------

    # H3 mean/k=1: all ring1 = 5.0
    h3_mean_rows = (
        [(1, h3_center, None)]
        + [(1, nb, 5.0) for nb in h3_ring1]
        + [(2, h3_isolated, None)]
    )
    spark.createDataFrame(
        h3_mean_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_h3_mean")

    # H3 idw/k=2/power=2: ring1[0]=10.0, ring2[0]=20.0 → center=12.0
    h3_idw_rows = [
        (1, h3_center, None),
        (1, h3_ring1[0], 10.0),
        (1, h3_ring2[0], 20.0),
        (2, h3_isolated, None),
    ]
    spark.createDataFrame(
        h3_idw_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_h3_idw")

    # Quadbin mean/k=1
    qb_mean_rows = (
        [(1, qb_center, None)]
        + [(1, nb, 5.0) for nb in qb_ring1]
        + [(2, qb_isolated, None)]
    )
    spark.createDataFrame(
        qb_mean_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_qb_mean")

    # Quadbin idw/k=2/power=2
    qb_idw_rows = [
        (1, qb_center, None),
        (1, qb_ring1[0], 10.0),
        (1, qb_ring2[0], 20.0),
        (2, qb_isolated, None),
    ]
    spark.createDataFrame(
        qb_idw_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_qb_idw")

    # BNG mean/k=1 (cellid is STRING)
    bng_mean_rows = (
        [(1, bng_center_str, None)]
        + [(1, nb, 5.0) for nb in bng_ring1_str]
        + [(2, bng_isolated_str, None)]
    )
    spark.createDataFrame(
        bng_mean_rows, "grp int, cellid string, value double"
    ).createOrReplaceTempView("_cf_bng_mean")

    # BNG idw/k=2/power=2
    bng_idw_rows = [
        (1, bng_center_str, None),
        (1, _bng.format(bng_ring1_int[0]), 10.0),
        (1, _bng.format(bng_ring2_int[0]), 20.0),
        (2, bng_isolated_str, None),
    ]
    spark.createDataFrame(
        bng_idw_rows, "grp int, cellid string, value double"
    ).createOrReplaceTempView("_cf_bng_idw")

    # Custom mean/k=1
    cust_mean_rows = (
        [(1, cust_center, None)]
        + [(1, nb, 5.0) for nb in cust_ring1]
        + [(2, cust_isolated, None)]
    )
    spark.createDataFrame(
        cust_mean_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_cust_mean")

    # Custom idw/k=2/power=2
    cust_idw_rows = [
        (1, cust_center, None),
        (1, cust_ring1[0], 10.0),
        (1, cust_ring2[0], 20.0),
        (2, cust_isolated, None),
    ]
    spark.createDataFrame(
        cust_idw_rows, "grp int, cellid long, value double"
    ).createOrReplaceTempView("_cf_cust_idw")

    # -----------------------------------------------------------------------
    # Phase 1: LIGHT — register Python UDFs, collect all results
    # -----------------------------------------------------------------------
    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"), level="L3"
    )

    def _collect(view: str, fn_sql: str) -> Dict[int, Dict[int, Optional[float]]]:
        """Run grouped-fill SQL and return {grp: decoded_dict}."""
        rows = spark.sql(
            f"SELECT grp, {fn_sql} AS fill FROM {view} GROUP BY grp"
        ).collect()
        return {r.grp: _decode(r.fill) for r in rows}

    h3_mean_light = _collect(
        "_cf_h3_mean", "gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    h3_idw_light = _collect(
        "_cf_h3_idw", "gbx_h3_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    qb_mean_light = _collect(
        "_cf_qb_mean", "gbx_quadbin_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    qb_idw_light = _collect(
        "_cf_qb_idw", "gbx_quadbin_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    bng_mean_light = _collect(
        "_cf_bng_mean", "gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    bng_idw_light = _collect(
        "_cf_bng_idw", "gbx_bng_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    cust_mean_light = _collect(
        "_cf_cust_mean",
        f"gbx_custom_cellfill(cellid, value, {_GRID_SQL}, 1, 'mean', 2.0)",
    )
    cust_idw_light = _collect(
        "_cf_cust_idw",
        f"gbx_custom_cellfill(cellid, value, {_GRID_SQL}, 2, 'idw', 2.0)",
    )

    # -----------------------------------------------------------------------
    # Phase 2: HEAVY — register JVM expressions (overwrite the SQL names),
    #          collect all results
    # -----------------------------------------------------------------------
    _register_h3_heavy(spark)
    gbx_manager.register_family(spark, ("gbx_quadbin_",), level="L1")
    gbx_manager.register_family(
        spark,
        ("gbx_bng_",),
        only=gbx_manager.names_at_level(("gbx_bng_",), "L1"),
        level="L1",
    )
    gbx_manager.register_family(spark, ("gbx_custom_",), level="L1")

    def _collect_heavy(view: str, fn_sql: str) -> Dict[int, Dict[int, Optional[float]]]:
        rows = spark.sql(
            f"SELECT grp, {fn_sql} AS fill FROM {view} GROUP BY grp"
        ).collect()
        return {r.grp: _decode(r.fill) for r in rows}

    h3_mean_heavy = _collect_heavy(
        "_cf_h3_mean", "gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    h3_idw_heavy = _collect_heavy(
        "_cf_h3_idw", "gbx_h3_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    qb_mean_heavy = _collect_heavy(
        "_cf_qb_mean", "gbx_quadbin_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    qb_idw_heavy = _collect_heavy(
        "_cf_qb_idw", "gbx_quadbin_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    bng_mean_heavy = _collect_heavy(
        "_cf_bng_mean", "gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0)"
    )
    bng_idw_heavy = _collect_heavy(
        "_cf_bng_idw", "gbx_bng_cellfill(cellid, value, 2, 'idw', 2.0)"
    )
    cust_mean_heavy = _collect_heavy(
        "_cf_cust_mean",
        f"gbx_custom_cellfill(cellid, value, {_GRID_SQL}, 1, 'mean', 2.0)",
    )
    cust_idw_heavy = _collect_heavy(
        "_cf_cust_idw",
        f"gbx_custom_cellfill(cellid, value, {_GRID_SQL}, 2, 'idw', 2.0)",
    )

    # -----------------------------------------------------------------------
    # Phase 3: Assert parity
    #
    # Expected values (deterministic from the fill semantics, verified against
    # CellFillTest.scala and test_cellfill.py unit tests):
    #   mean k=1, all ring1=5.0  → center=5.0
    #   idw k=2 power=2, ring1[0]=10.0 ring2[0]=20.0
    #     → (10*1^-2 + 20*2^-2) / (1^-2 + 2^-2) = (10+5)/(1.0+0.25) = 12.0
    # Isolated NULL (group 2) → stays None (no valid neighbours in group).
    #
    # SANITY: these are exact numeric values; a wrong expected (e.g. 6.0 or 13.0)
    # causes pytest.approx(abs=1e-9) to fail.  The test was verified to fail by
    # temporarily substituting 6.0 for 5.0 and 13.0 for 12.0.
    # -----------------------------------------------------------------------

    # H3 -----------------------------------------------------------------
    _assert_fill_parity(
        h3_mean_light[1],
        h3_mean_heavy[1],
        "H3 mean/k=1 grp=1",
        expected_filled={h3_center: 5.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        h3_mean_light[2],
        h3_mean_heavy[2],
        "H3 mean/k=1 grp=2 (isolated)",
        expected_filled={},
        expected_null={h3_isolated},
    )
    _assert_fill_parity(
        h3_idw_light[1],
        h3_idw_heavy[1],
        "H3 idw/k=2/power=2 grp=1",
        expected_filled={h3_center: 12.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        h3_idw_light[2],
        h3_idw_heavy[2],
        "H3 idw/k=2/power=2 grp=2 (isolated)",
        expected_filled={},
        expected_null={h3_isolated},
    )

    # Quadbin ------------------------------------------------------------
    _assert_fill_parity(
        qb_mean_light[1],
        qb_mean_heavy[1],
        "Quadbin mean/k=1 grp=1",
        expected_filled={qb_center: 5.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        qb_mean_light[2],
        qb_mean_heavy[2],
        "Quadbin mean/k=1 grp=2 (isolated)",
        expected_filled={},
        expected_null={qb_isolated},
    )
    _assert_fill_parity(
        qb_idw_light[1],
        qb_idw_heavy[1],
        "Quadbin idw/k=2/power=2 grp=1",
        expected_filled={qb_center: 12.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        qb_idw_light[2],
        qb_idw_heavy[2],
        "Quadbin idw/k=2/power=2 grp=2 (isolated)",
        expected_filled={},
        expected_null={qb_isolated},
    )

    # BNG — cell IDs compared as int (decoded from BINARY for light, parsed
    # from STRING for heavy via _bng.parse_safe)
    _assert_fill_parity(
        bng_mean_light[1],
        bng_mean_heavy[1],
        "BNG mean/k=1 grp=1",
        expected_filled={bng_center_int: 5.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        bng_mean_light[2],
        bng_mean_heavy[2],
        "BNG mean/k=1 grp=2 (isolated)",
        expected_filled={},
        expected_null={bng_isolated_int},
    )
    _assert_fill_parity(
        bng_idw_light[1],
        bng_idw_heavy[1],
        "BNG idw/k=2/power=2 grp=1",
        expected_filled={bng_center_int: 12.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        bng_idw_light[2],
        bng_idw_heavy[2],
        "BNG idw/k=2/power=2 grp=2 (isolated)",
        expected_filled={},
        expected_null={bng_isolated_int},
    )

    # Custom -------------------------------------------------------------
    _assert_fill_parity(
        cust_mean_light[1],
        cust_mean_heavy[1],
        "Custom mean/k=1 grp=1",
        expected_filled={cust_center: 5.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        cust_mean_light[2],
        cust_mean_heavy[2],
        "Custom mean/k=1 grp=2 (isolated)",
        expected_filled={},
        expected_null={cust_isolated},
    )
    _assert_fill_parity(
        cust_idw_light[1],
        cust_idw_heavy[1],
        "Custom idw/k=2/power=2 grp=1",
        expected_filled={cust_center: 12.0},
        expected_null=set(),
    )
    _assert_fill_parity(
        cust_idw_light[2],
        cust_idw_heavy[2],
        "Custom idw/k=2/power=2 grp=2 (isolated)",
        expected_filled={},
        expected_null={cust_isolated},
    )
