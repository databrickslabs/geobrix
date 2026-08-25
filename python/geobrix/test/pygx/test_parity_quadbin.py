"""Light (pygx) vs heavy (gridx.quadbin) EXACT cross-tier parity for all 10 quadbin functions.

The bar (per the pygx quadbin spec) is EXACT, not tolerant:

- **Cell IDs / sets are bit-exact**: pointascell (same Long), resolution, kring
  (sorted set), distance, polyfill (sorted cell-set), tessellate (cell-set).
- **Geometry EWKB within 1e-6**: aswkb / centroid / cellunion / cellunion_agg /
  tessellate-chips — decode both tiers' EWKB (shapely ``from_wkb``), assert
  ``get_srid == 4326`` in both tiers, and coordinates equal within 1e-6 via
  ``equals_exact`` on the normalized geometries.

Both tiers register the SAME ``gbx_quadbin_*`` SQL names. Light registers
PySpark Python/pandas UDFs; heavy registers JVM expressions (via the
``register_ds`` data source). Both wrapper modules resolve through
``call_function`` to the shared SQL name, so a heavy ``register`` OVERWRITES the
light UDFs in the catalog. We therefore collect EVERY light result first, then
register heavy and collect the heavy results (same pattern as
``test_parity_legacy.py`` / ``test_parity_tin.py``).

Heavy requires the geobrix JAR (Scala/JTS). The JAR is present in the
geobrix-dev Docker container; this test auto-skips when the JAR is not staged
under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_quadbin.py \\
        --with-integration --log parity-quadbin.log
"""

import logging
from pathlib import Path

import pytest

quadbin = pytest.importorskip(
    "quadbin", reason="quadbin package not installed (a light-tier extra or [test])"
)
from shapely import equals_exact, from_wkb, get_srid  # noqa: E402
from shapely import to_wkb as _to_wkb  # noqa: E402
from shapely.geometry import box as _box  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pygx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_quadbin.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-quadbin-parity")
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


# --- geometry helper --------------------------------------------------------------------------


def _assert_geom_parity(light_blob, heavy_blob, ctx=""):
    """Decode both EWKB blobs; assert SRID 4326 in both + coords equal within 1e-6."""
    assert light_blob is not None, f"{ctx}: light geometry is None"
    assert heavy_blob is not None, f"{ctx}: heavy geometry is None"
    lg = from_wkb(bytes(light_blob))
    hg = from_wkb(bytes(heavy_blob))
    assert get_srid(lg) == 4326, f"{ctx}: light SRID {get_srid(lg)} != 4326"
    assert get_srid(hg) == 4326, f"{ctx}: heavy SRID {get_srid(hg)} != 4326"
    # normalize() canonicalizes vertex/ring order so equals_exact compares shape,
    # not winding/start-vertex; 1e-6 is the geometry tolerance from the spec.
    assert equals_exact(
        lg.normalize(), hg.normalize(), 1e-6
    ), f"{ctx}: geometry mismatch beyond 1e-6\n  light={lg.wkt}\n  heavy={hg.wkt}"


# Deterministic fixtures -----------------------------------------------------------------------
# A point well inside a cell (San Francisco) and a small polygon. A cell-id list
# from k_ring drives cellunion / cellunion_agg parity.
_LON, _LAT, _RES = -122.4194, 37.7749, 10
_BOX = _box(-0.05, -0.05, 0.05, 0.05)  # small square straddling meridian/equator
_POLYFILL_RES = 12


# --- the full 10-function parity sweep --------------------------------------------------------


def test_quadbin_full_parity(spark_with_jar):
    """All 10 quadbin functions, light vs heavy, in one session (light first, then heavy)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Seed cell + a k_ring cell-list computed via the lib so the INPUT is
    # tier-independent — both tiers consume the SAME cell ids.
    seed = quadbin.point_to_cell(_LON, _LAT, _RES)
    union_cells = sorted(quadbin.k_ring(seed, 1))
    cell_b = quadbin.point_to_cell(_LON + 1.0, _LAT + 1.0, _RES)  # same-res neighbour
    box_wkb = bytearray(_to_wkb(_BOX))

    df = spark.createDataFrame(
        [(_LON, _LAT, _RES, seed, cell_b, union_cells, box_wkb)],
        "lon double, lat double, res int, seed bigint, cell_b bigint, "
        "cells array<bigint>, geom binary",
    )

    def scalar_row(mod):
        mod.register(spark)
        return df.select(
            mod.quadbin_pointascell(f.col("lon"), f.col("lat"), f.col("res")).alias(
                "pac"
            ),
            mod.quadbin_resolution(f.col("seed")).alias("res_of"),
            mod.quadbin_kring(f.col("seed"), f.lit(1)).alias("kring"),
            mod.quadbin_distance(f.col("seed"), f.col("cell_b")).alias("dist"),
            mod.quadbin_polyfill(f.col("geom"), f.lit(_POLYFILL_RES)).alias("pf"),
            mod.quadbin_aswkb(f.col("seed")).alias("aswkb"),
            mod.quadbin_centroid(f.col("seed")).alias("centroid"),
            mod.quadbin_cellunion(f.col("cells")).alias("cu"),
        ).collect()[0]

    def tess_rows(mod):
        mod.register(spark)
        exploded = df.select(
            f.explode(
                mod.quadbin_tessellate(f.col("geom"), f.lit(_POLYFILL_RES))
            ).alias("chip")
        ).select(f.col("chip.cell").alias("cell"), f.col("chip.geom").alias("geom"))
        return {r["cell"]: r["geom"] for r in exploded.collect()}

    def agg_blob(mod):
        mod.register(spark)
        return (
            df.select(f.explode(f.col("cells")).alias("c"))
            .agg(mod.quadbin_cellunion_agg(f.col("c")).alias("agg"))
            .collect()[0]["agg"]
        )

    # ---- LIGHT first (the heavy register call overwrites the catalog names) ----
    light = scalar_row(gx)
    light_tess = tess_rows(gx)
    light_agg = agg_blob(gx)

    # ---- HEAVY (overwrites the gbx_quadbin_* SQL names) ----
    heavy = scalar_row(hx)
    heavy_tess = tess_rows(hx)
    heavy_agg = agg_blob(hx)

    # === cell-ID / set parity (EXACT) ===
    assert (
        light["pac"] == heavy["pac"]
    ), f"pointascell mismatch: light={light['pac']} heavy={heavy['pac']}"
    assert (
        light["res_of"] == heavy["res_of"]
    ), f"resolution mismatch: light={light['res_of']} heavy={heavy['res_of']}"
    assert sorted(light["kring"]) == sorted(
        heavy["kring"]
    ), f"kring set mismatch:\n  light={sorted(light['kring'])}\n  heavy={sorted(heavy['kring'])}"
    assert (
        light["dist"] == heavy["dist"]
    ), f"distance mismatch: light={light['dist']} heavy={heavy['dist']}"
    assert sorted(light["pf"]) == sorted(
        heavy["pf"]
    ), f"polyfill cell-set mismatch:\n  light={sorted(light['pf'])}\n  heavy={sorted(heavy['pf'])}"
    assert set(light_tess.keys()) == set(
        heavy_tess.keys()
    ), f"tessellate cell-set mismatch:\n  light={sorted(light_tess)}\n  heavy={sorted(heavy_tess)}"

    # === geometry EWKB parity (within 1e-6) ===
    _assert_geom_parity(light["aswkb"], heavy["aswkb"], "aswkb")
    _assert_geom_parity(light["centroid"], heavy["centroid"], "centroid")
    _assert_geom_parity(light["cu"], heavy["cu"], "cellunion")
    _assert_geom_parity(light_agg, heavy_agg, "cellunion_agg")
    for cell in light_tess:
        _assert_geom_parity(
            light_tess[cell], heavy_tess[cell], f"tessellate-chip cell={cell}"
        )


# --- risk-area edge cases (from the spec) -----------------------------------------------------


def test_pointascell_edge_antimeridian_and_pole(spark_with_jar):
    """Risk area 1: pointascell at antimeridian / near-pole edges must be bit-equal.

    Both tiers clamp lon to [-180, 180] and lat to the web-mercator band
    [-85.05112878, 85.05112878] before tiling. Probe near and ON both clamp edges.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    edge_pts = [
        (179.9999, 84.9, 12),  # near +180 lon, near +85 lat
        (-179.9999, -84.9, 12),  # near -180 lon, near -85 lat
        (180.0, 85.05112878, 14),  # exactly on the clamp corner (NE)
        (-180.0, -85.05112878, 14),  # exactly on the clamp corner (SW)
        (179.9999, 89.0, 10),  # lat beyond the merc band -> clamps to LAT_MAX
    ]
    df = spark.createDataFrame(edge_pts, "lon double, lat double, res int")
    col = gx.quadbin_pointascell(f.col("lon"), f.col("lat"), f.col("res")).alias("c")

    gx.register(spark)
    light = [r["c"] for r in df.select(col).collect()]
    hx.register(spark)  # overwrites the SQL name
    heavy = [
        r["c"]
        for r in df.select(
            hx.quadbin_pointascell(f.col("lon"), f.col("lat"), f.col("res")).alias("c")
        ).collect()
    ]

    assert light == heavy, (
        f"antimeridian/pole pointascell mismatch:\n  pts={edge_pts}\n"
        f"  light={light}\n  heavy={heavy}"
    )


def test_polyfill_bbox_corner_tile_boundary(spark_with_jar):
    """Risk area 2: polyfill of a bbox whose corners sit near tile boundaries (off-by-one).

    The SW/NE corner enumeration is the top divergence risk: light ports
    Quadbin.scala's lonLatToTile from the bbox corners, heavy enumerates the same
    way. A box aligned near z=6 lon tile edges (5.625deg steps) probes the corner.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    res = 6  # z=6 lon tile width = 360/64 = 5.625deg
    # NE corner lands a hair past the 11.25 (== 2 tiles) edge; SW a hair below 0.
    box_geom = _box(-0.0001, -0.0001, 11.2501, 11.2501)
    df = spark.createDataFrame([(bytearray(_to_wkb(box_geom)),)], "geom binary")
    col = gx.quadbin_polyfill(f.col("geom"), f.lit(res)).alias("pf")

    gx.register(spark)
    light = sorted(df.select(col).collect()[0]["pf"])
    hx.register(spark)
    heavy = sorted(
        df.select(hx.quadbin_polyfill(f.col("geom"), f.lit(res)).alias("pf")).collect()[
            0
        ]["pf"]
    )

    assert light == heavy, (
        f"corner-boundary polyfill cell-set mismatch:\n"
        f"  light={light}\n  heavy={heavy}\n"
        f"  light_only={set(light) - set(heavy)} heavy_only={set(heavy) - set(light)}"
    )


def test_tessellate_touching_intersection(spark_with_jar):
    """Risk area 3: tessellate of a thin sliver that only TOUCHES some cell borders.

    JTS (heavy) vs shapely (light) can differ on degenerate/touching intersections
    (a shared edge or point). Both tiers must drop empty intersections and agree on
    the surviving chip cell-set, and surviving chip geometries must match within 1e-6.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    sliver = _box(-0.001, -2.0, 0.001, 2.0)  # tall thin strip across the meridian
    res = 8
    df = spark.createDataFrame([(bytearray(_to_wkb(sliver)),)], "geom binary")

    def tess(mod):
        mod.register(spark)
        exploded = df.select(
            f.explode(mod.quadbin_tessellate(f.col("geom"), f.lit(res))).alias("chip")
        ).select(f.col("chip.cell").alias("cell"), f.col("chip.geom").alias("geom"))
        return {r["cell"]: r["geom"] for r in exploded.collect()}

    light_tess = tess(gx)  # light first
    heavy_tess = tess(hx)  # heavy overwrites

    assert set(light_tess.keys()) == set(heavy_tess.keys()), (
        f"touching-intersection chip cell-set mismatch:\n"
        f"  light={sorted(light_tess)}\n  heavy={sorted(heavy_tess)}\n"
        f"  light_only={set(light_tess) - set(heavy_tess)} "
        f"heavy_only={set(heavy_tess) - set(light_tess)}"
    )
    for cell in light_tess:
        _assert_geom_parity(
            light_tess[cell], heavy_tess[cell], f"touching chip cell={cell}"
        )


def test_null_geom_propagation(spark_with_jar):
    """NULL geom -> SQL NULL (not empty array) for polyfill/tessellate, both tiers."""
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar
    df = spark.createDataFrame(
        [(None,), (bytearray(_to_wkb(_box(-0.05, -0.05, 0.05, 0.05))),)], "geom binary"
    )
    df.createOrReplaceTempView("nullv")

    def nulls(mod):
        mod.register(spark)
        rows = spark.sql(
            "SELECT geom IS NULL AS g_null, "
            "gbx_quadbin_polyfill(geom, 8) IS NULL AS pf_null, "
            "gbx_quadbin_tessellate(geom, 8) IS NULL AS te_null FROM nullv"
        ).collect()
        return sorted((r["g_null"], r["pf_null"], r["te_null"]) for r in rows)

    light = nulls(gx)  # light first
    heavy = nulls(hx)  # heavy overwrites the SQL name
    assert (True, True, True) in light  # null geom -> both fns NULL
    assert (False, False, False) in light  # real geom -> not NULL
    assert light == heavy
