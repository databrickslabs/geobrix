"""Light (pygx) vs heavy (gridx.bng) EXACT cross-tier parity for the 23 BNG functions.

The bar (per the pygx BNG spec) is EXACT, not tolerant, for cell ids / sets:

- **Cell IDs / sets are bit-exact** (BNG ids are STRINGS): ``pointascell`` /
  ``eastnorthasbng`` (same STRING), ``cellarea`` (same DOUBLE), ``distance`` /
  ``euclideandistance`` (same LONG), ``kring`` / ``kloop`` (sorted set),
  ``polyfill`` (sorted cell-set), ``tessellate`` (cell-set), ``geomkring`` /
  ``geomkloop`` (sorted set), and the five ``*explode`` UDTFs (light SQL UDTF vs
  heavy ``LATERAL VIEW`` — sorted cellid set).
- **Geometry WKB within 1e-6**: ``aswkb`` / ``aswkt`` / ``centroid`` / surviving
  ``tessellate`` chips / ``cellunion`` / ``cellintersection`` and the two aggs —
  decode both tiers (shapely ``from_wkb`` / ``from_wkt``). BNG carries **NO SRID**
  (heavy ``JTS.toWKB``), so assert ``get_srid == 0`` in BOTH tiers and compare the
  normalized geometries within 1e-6 via ``equals_exact``.

The aggregates are the documented light-agg-struct-return deviation: the light SQL
agg returns plain **BINARY** (the dissolved chip geometry) while heavy returns a
``STRUCT<cellid, core, chip>`` (PySpark grouped-agg pandas UDFs cannot return a
struct). We therefore compare the **decoded chip GEOMETRY** (light BINARY vs the
heavy struct's ``chip`` field), not the struct wrapper.

Both tiers register the SAME ``gbx_bng_*`` SQL names. Light registers PySpark
UDF/UDTF/pandas-agg; heavy registers JVM expressions. Both wrapper modules resolve
through ``call_function`` to the shared SQL name, so a heavy ``register``
OVERWRITES the light functions in the catalog. We therefore collect EVERY light
result first, then register heavy and collect the heavy results (same pattern as
``test_parity_quadbin.py`` / ``test_parity_tin.py``).

Heavy requires the geobrix JAR (Scala/JTS). The JAR is present in the geobrix-dev
Docker container; this test auto-skips when the JAR is not staged under
``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_parity_bng.py \\
        --with-integration --log parity-bng.log
"""

import logging
from pathlib import Path

import pytest
from shapely import equals_exact, from_wkb, from_wkt, get_srid
from shapely import to_wkb as _to_wkb
from shapely.geometry import Point as _Point
from shapely.geometry import box as _box

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
                "gbx:test:python --path python/geobrix/test/pygx/test_parity_bng.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-bng-parity")
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


def _assert_geom_parity(light_blob, heavy_blob, ctx="", decoder=from_wkb):
    """Decode both blobs; assert SRID 0 (BNG has none) in both + coords equal 1e-6."""
    assert light_blob is not None, f"{ctx}: light geometry is None"
    assert heavy_blob is not None, f"{ctx}: heavy geometry is None"
    lg = decoder(bytes(light_blob)) if decoder is from_wkb else decoder(light_blob)
    hg = decoder(bytes(heavy_blob)) if decoder is from_wkb else decoder(heavy_blob)
    # BNG WKB carries NO SRID (heavy JTS.toWKB is 2D, no SRID stamp).
    assert get_srid(lg) == 0, f"{ctx}: light SRID {get_srid(lg)} != 0 (BNG has no SRID)"
    assert get_srid(hg) == 0, f"{ctx}: heavy SRID {get_srid(hg)} != 0 (BNG has no SRID)"
    # normalize() canonicalizes vertex/ring order so equals_exact compares shape,
    # not winding/start-vertex (the shapely-ring-order pitfall). 1e-6 from the spec.
    assert equals_exact(
        lg.normalize(), hg.normalize(), 1e-6
    ), f"{ctx}: geometry mismatch beyond 1e-6\n  light={lg.wkt}\n  heavy={hg.wkt}"


# Deterministic fixtures -----------------------------------------------------------------------
# All coordinates are EPSG:27700 BNG eastings/northings (NOT WGS84). London is
# (530000, 180000). A multi-cell box and a grid-aligned box drive polyfill /
# tessellate. The four 100km NE/NW/SE/SW quadrant points exercise mosaic#434.
_LON_E, _LON_N = 530000.0, 180000.0  # London, EPSG:27700
_RES = 3  # 1km
# A 1km box offset by 500 m so it straddles four 1km cells as BORDER chips with
# NO fully-interior (core) cell. Core cells carry a null chip geometry, and heavy
# BNG_Tessellate(keepCoreGeom=false) crashes serializing a null geom via
# JTS.toWKB (BNG_Tessellate.scala:40 / BNG.scala ~L786) — a real heavy-tier limit.
# An all-border fixture sidesteps that so both registered tiers run their own
# 2-arg/keepCore=false path and emit real chip geometry to compare.
_BOX = _box(530500.0, 180500.0, 531500.0, 181500.0)  # 4 border cells, 0 core
# mosaic#423: edges lie ON exact 1km grid lines (530000/180000) yet extend
# partway into neighbour cells -> border chips along grid lines (the degenerate
# POINT/LINESTRING risk) with NO core cell.
_ALIGNED_BOX = _box(530000.0, 180000.0, 531200.0, 180400.0)
# A 2.5km axis-aligned box whose four edges all sit EXACTLY on 1km grid lines
# (530000/532500 E, 180000/182500 N). At res 3 (1km) this covers a 3x3 block of
# cells, but only the centre cell is a true interior (core) cell; every edge cell
# is grid-aligned so its polygon-vs-cell intersection renormalizes its ring start
# vertex, making heavy's JTS ``equalsExact(cellGeom, 0.1)`` reject promotion -->
# those full-but-grid-aligned cells stay BORDER. geomkring/geomkloop expand the
# border cells, so a wrong core/border split silently drops the SW corner cells.
# This is the bench fixture that surfaced the light-vs-heavy geomkring break.
_GRID_ALIGNED_BIG_BOX = _box(530000.0, 180000.0, 532500.0, 182500.0)
# Quadrant points for the four 100km NE/NW/SE/SW cells (#434). The 100km cell's
# 2-letter prefix is letter-pair-derived; these points land in distinct 100km
# squares whose ids must report resolution 1 (area 10000 km^2), not a quadrant.
_QUAD_PTS = [
    (550000.0, 350000.0),  # central GB
    (250000.0, 550000.0),  # NW-ish
    (450000.0, 150000.0),  # SE-ish
    (150000.0, 850000.0),  # far N
]


def _wkb(geom):
    return bytearray(_to_wkb(geom))


# --- the full BNG parity sweep ----------------------------------------------------------------


def test_bng_full_parity(spark_with_jar):
    """All 23 BNG functions, light vs heavy, in one session (light first, then heavy)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Seed cell + a k_ring cell-list computed via the pure-Python codec so the
    # INPUT is tier-independent: both tiers consume the SAME STRING cell ids.
    seed = _bng.point_as_cell(_LON_E, _LON_N, _RES)
    cell_b = _bng.point_as_cell(_LON_E + 5000.0, _LON_N + 5000.0, _RES)
    pt_wkb = _wkb(_Point(_LON_E, _LON_N))
    box_wkb = _wkb(_BOX)

    df = spark.createDataFrame(
        [(_LON_E, _LON_N, _RES, seed, cell_b, pt_wkb, box_wkb)],
        "e double, n double, res int, seed string, cell_b string, "
        "pt binary, geom binary",
    )

    def scalar_row(mod, tess_keep_core):
        mod.register(spark)
        return df.select(
            mod.bng_pointascell(f.col("pt"), f.col("res")).alias("pac"),
            mod.bng_eastnorthasbng(f.col("e"), f.col("n"), f.col("res")).alias("enb"),
            mod.bng_cellarea(f.col("seed")).alias("area"),
            mod.bng_distance(f.col("seed"), f.col("cell_b")).alias("dist"),
            mod.bng_euclideandistance(f.col("seed"), f.col("cell_b")).alias("edist"),
            mod.bng_aswkb(f.col("seed")).alias("aswkb"),
            mod.bng_aswkt(f.col("seed")).alias("aswkt"),
            mod.bng_centroid(f.col("seed")).alias("centroid"),
            mod.bng_kring(f.col("seed"), f.lit(1)).alias("kring"),
            mod.bng_kloop(f.col("seed"), f.lit(1)).alias("kloop"),
            mod.bng_polyfill(f.col("geom"), f.col("res")).alias("pf"),
            mod.bng_geomkring(f.col("geom"), f.col("res"), f.lit(1)).alias("gkr"),
            mod.bng_geomkloop(f.col("geom"), f.col("res"), f.lit(1)).alias("gkl"),
        ).collect()[0]

    def chip_row(mod):
        """cellintersection / cellunion of two chip structs (same cellid -> union)."""
        mod.register(spark)
        # Build two chip structs over the SAME cellid (seed): a core chip and a
        # full-cell chip, so cell_union/intersection produce the whole cell.
        chip_geom = _bng.cell_id_to_geometry(_bng.parse(seed))
        chip_wkb = _wkb(chip_geom)
        cdf = spark.createDataFrame(
            [(seed, True, chip_wkb, seed, False, chip_wkb)],
            "lid string, lcore boolean, lchip binary, "
            "rid string, rcore boolean, rchip binary",
        )
        left = f.struct(f.col("lid"), f.col("lcore"), f.col("lchip"))
        right = f.struct(f.col("rid"), f.col("rcore"), f.col("rchip"))
        return cdf.select(
            mod.bng_cellunion(left, right).alias("cu"),
            mod.bng_cellintersection(left, right).alias("ci"),
        ).collect()[0]

    def tess_rows(mod, keep_core):
        """Tessellate chips. Both tiers called with keep_core_geom=False so core
        chips have no geometry on either side (parity comparison is clean)."""
        mod.register(spark)
        tess_col = mod.bng_tessellate(f.col("geom"), f.col("res"), f.lit(keep_core))
        exploded = df.select(f.explode(tess_col).alias("chip")).select(
            f.col("chip.cellid").alias("cell"),
            f.col("chip.core").alias("core"),
            f.col("chip.chip").alias("geom"),
        )
        return {r["cell"]: (r["core"], r["geom"]) for r in exploded.collect()}

    def agg_chip_geom(mod):
        """Dissolve a group of identical-cell chips. Light returns BINARY (the chip
        geometry); heavy returns STRUCT<cellid, core, chip> -> take .chip. Compare
        the decoded chip GEOMETRY either way (the load-bearing field)."""
        mod.register(spark)
        chip_geom = _bng.cell_id_to_geometry(_bng.parse(seed))
        chip_wkb = _wkb(chip_geom)
        gdf = spark.createDataFrame(
            [(seed, False, chip_wkb), (seed, False, chip_wkb)],
            "cellid string, core boolean, chip binary",
        )
        chip_struct = f.struct(f.col("cellid"), f.col("core"), f.col("chip"))
        out = (
            gdf.groupBy("cellid")
            .agg(
                mod.bng_cellunion_agg(chip_struct).alias("u"),
                mod.bng_cellintersection_agg(chip_struct).alias("i"),
            )
            .collect()[0]
        )
        return out["u"], out["i"]

    def explode_sets_light():
        """Light registers the five *explode as UDTFs (table functions) -> invoke
        via SQL LATERAL. Collect each as a sorted cellid set."""
        gx.register(spark)
        df.createOrReplaceTempView("bng_src")

        def lateral(call):
            return sorted(
                r["cellid"]
                for r in spark.sql(
                    f"SELECT t.cellid FROM bng_src, LATERAL {call} t"
                ).collect()
            )

        return {
            "kr": lateral("gbx_bng_kringexplode(seed, 1)"),
            "kl": lateral("gbx_bng_kloopexplode(seed, 1)"),
            "gkr": lateral("gbx_bng_geomkringexplode(geom, res, 1)"),
            "gkl": lateral("gbx_bng_geomkloopexplode(geom, res, 1)"),
            "te": lateral("gbx_bng_tessellateexplode(geom, res)"),
        }

    def explode_sets_heavy():
        """Heavy has no table-function *explode (the heavy `gbx_bng_*explode` SQL
        names are not array-returning generators — its Python wrappers explode the
        underlying ARRAY function). The semantic equivalent of light's `*explode`
        UDTF output is exactly the heavy ARRAY function it would explode, so compare
        light's exploded cellid set against the heavy array's cellid set: kringexplode
        <-> gbx_bng_kring, kloopexplode <-> gbx_bng_kloop, geomk*explode <->
        gbx_bng_geomk*, tessellateexplode <-> gbx_bng_tessellate (chip cellids)."""
        hx.register(spark)

        def arr(call):
            row = df.select(call.alias("a")).collect()[0]["a"]
            return sorted(row) if row else []

        # heavy tessellate returns array<struct<cellid, core, chip>> -> chip cellids.
        te_rows = df.select(
            hx.bng_tessellate(f.col("geom"), f.col("res"), f.lit(False)).alias("a")
        ).collect()[0]["a"]
        te = sorted(rec["cellid"] for rec in te_rows) if te_rows else []

        return {
            "kr": arr(f.call_function("gbx_bng_kring", f.col("seed"), f.lit(1))),
            "kl": arr(f.call_function("gbx_bng_kloop", f.col("seed"), f.lit(1))),
            "gkr": arr(
                f.call_function(
                    "gbx_bng_geomkring", f.col("geom"), f.col("res"), f.lit(1)
                )
            ),
            "gkl": arr(
                f.call_function(
                    "gbx_bng_geomkloop", f.col("geom"), f.col("res"), f.lit(1)
                )
            ),
            "te": te,
        }

    # ---- LIGHT first (heavy register OVERWRITES the catalog names) ----
    light = scalar_row(gx, None)
    light_chip = chip_row(gx)
    light_tess = tess_rows(gx, False)  # light: keep_core_geom=False, matches heavy
    light_agg_u, light_agg_i = agg_chip_geom(gx)
    light_expl = explode_sets_light()

    # ---- HEAVY (overwrites the gbx_bng_* SQL names) ----
    heavy = scalar_row(hx, None)
    heavy_chip = chip_row(hx)
    heavy_tess = tess_rows(hx, False)  # heavy: keep_core_geom=False
    heavy_agg_u, heavy_agg_i = agg_chip_geom(hx)
    heavy_expl = explode_sets_heavy()

    # === cell-ID / set parity (EXACT) ===
    assert (
        light["pac"] == heavy["pac"]
    ), f"pointascell mismatch: light={light['pac']} heavy={heavy['pac']}"
    assert (
        light["enb"] == heavy["enb"]
    ), f"eastnorthasbng mismatch: light={light['enb']} heavy={heavy['enb']}"
    assert (
        light["area"] == heavy["area"]
    ), f"cellarea mismatch: light={light['area']} heavy={heavy['area']}"
    assert (
        light["dist"] == heavy["dist"]
    ), f"distance mismatch: light={light['dist']} heavy={heavy['dist']}"
    assert (
        light["edist"] == heavy["edist"]
    ), f"euclideandistance mismatch: light={light['edist']} heavy={heavy['edist']}"
    assert sorted(light["kring"]) == sorted(heavy["kring"]), (
        f"kring set mismatch:\n  light={sorted(light['kring'])}\n"
        f"  heavy={sorted(heavy['kring'])}"
    )
    assert sorted(light["kloop"]) == sorted(heavy["kloop"]), (
        f"kloop set mismatch:\n  light={sorted(light['kloop'])}\n"
        f"  heavy={sorted(heavy['kloop'])}"
    )
    assert sorted(light["pf"]) == sorted(heavy["pf"]), (
        f"polyfill cell-set mismatch:\n  light={sorted(light['pf'])}\n"
        f"  heavy={sorted(heavy['pf'])}"
    )
    assert sorted(light["gkr"]) == sorted(heavy["gkr"]), (
        f"geomkring set mismatch:\n  light={sorted(light['gkr'])}\n"
        f"  heavy={sorted(heavy['gkr'])}"
    )
    assert sorted(light["gkl"]) == sorted(heavy["gkl"]), (
        f"geomkloop set mismatch:\n  light={sorted(light['gkl'])}\n"
        f"  heavy={sorted(heavy['gkl'])}"
    )
    assert set(light_tess.keys()) == set(heavy_tess.keys()), (
        f"tessellate cell-set mismatch:\n  light={sorted(light_tess)}\n"
        f"  heavy={sorted(heavy_tess)}"
    )

    # explode UDTFs (light SQL UDTF vs heavy LATERAL) -- sorted cellid sets
    for key in ("kr", "kl", "gkr", "gkl", "te"):
        assert light_expl[key] == heavy_expl[key], (
            f"{key}explode cellid-set mismatch:\n"
            f"  light={light_expl[key]}\n  heavy={heavy_expl[key]}"
        )

    # === geometry WKB parity (within 1e-6, SRID 0) ===
    _assert_geom_parity(light["aswkb"], heavy["aswkb"], "aswkb")
    _assert_geom_parity(light["aswkt"], heavy["aswkt"], "aswkt", decoder=from_wkt)
    _assert_geom_parity(light["centroid"], heavy["centroid"], "centroid")

    # chip-pair scalar ops: decode the chip struct's geometry (field .chip)
    _assert_geom_parity(
        light_chip["cu"]["chip"], heavy_chip["cu"]["chip"], "cellunion-chip"
    )
    _assert_geom_parity(
        light_chip["ci"]["chip"], heavy_chip["ci"]["chip"], "cellintersection-chip"
    )

    # surviving tessellate chips (border cells carry geometry; core may be None)
    for cell, (lcore, lgeom) in light_tess.items():
        hcore, hgeom = heavy_tess[cell]
        if lgeom is None or hgeom is None:
            # core cell -> no chip geometry in either tier (keep_core=False)
            assert lgeom is None and hgeom is None, (
                f"tessellate-chip cell={cell}: core/None mismatch "
                f"(light={lgeom is None} heavy={hgeom is None})"
            )
            continue
        _assert_geom_parity(lgeom, hgeom, f"tessellate-chip cell={cell}")

    # aggregates: light BINARY vs heavy STRUCT.chip -- compare decoded chip geometry
    light_agg_u_blob = light_agg_u
    heavy_agg_u_blob = heavy_agg_u["chip"] if heavy_agg_u is not None else None
    _assert_geom_parity(light_agg_u_blob, heavy_agg_u_blob, "cellunion_agg")
    light_agg_i_blob = light_agg_i
    heavy_agg_i_blob = heavy_agg_i["chip"] if heavy_agg_i is not None else None
    _assert_geom_parity(light_agg_i_blob, heavy_agg_i_blob, "cellintersection_agg")


# --- mosaic#434 lock-in -----------------------------------------------------------------------


def test_bng_mosaic_434_100km_cells_are_res1(spark_with_jar):
    """mosaic#434 (fixed via #580): 100km NE/NW/SE/SW cells are resolution 1
    (area 10000 km^2 + a 100km-span polygon), NOT quadrants.

    Both tiers must agree on the FIXED behavior: a 100km cell id reports
    ``cellarea == 10000.0`` km^2 and ``aswkb`` decodes to a 100km x 100km polygon.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Derive the four 100km cells via eastnorthasbng at "100km" so the INPUT cell
    # ids are tier-independent.
    cells = [_bng.point_as_cell(e, n, "100km") for (e, n) in _QUAD_PTS]
    df = spark.createDataFrame([(c,) for c in cells], "cellid string")

    def rows(mod):
        mod.register(spark)
        return df.select(
            f.col("cellid"),
            mod.bng_cellarea(f.col("cellid")).alias("area"),
            mod.bng_aswkb(f.col("cellid")).alias("wkb"),
        ).collect()

    light = {r["cellid"]: (r["area"], r["wkb"]) for r in rows(gx)}
    heavy = {r["cellid"]: (r["area"], r["wkb"]) for r in rows(hx)}

    for cell in cells:
        l_area, l_wkb = light[cell]
        h_area, h_wkb = heavy[cell]
        # 100km cell == 100km x 100km = 10000 km^2 (res 1, not a quadrant)
        assert (
            l_area == 10000.0
        ), f"#434 light cellarea({cell})={l_area} != 10000.0 km^2"
        assert (
            h_area == 10000.0
        ), f"#434 heavy cellarea({cell})={h_area} != 10000.0 km^2"
        assert (
            l_area == h_area
        ), f"#434 cellarea mismatch ({cell}): {l_area} vs {h_area}"
        lg = from_wkb(bytes(l_wkb))
        hg = from_wkb(bytes(h_wkb))
        # 100km span on both axes
        lxmin, lymin, lxmax, lymax = lg.bounds
        assert (lxmax - lxmin) == 100000.0 and (
            lymax - lymin
        ) == 100000.0, f"#434 light cell {cell} not 100km span: bounds={lg.bounds}"
        assert get_srid(lg) == 0 and get_srid(hg) == 0, "#434 BNG WKB has no SRID"
        assert equals_exact(
            lg.normalize(), hg.normalize(), 1e-6
        ), f"#434 cell {cell} polygon mismatch: light={lg.wkt} heavy={hg.wkt}"


# --- mosaic#423 lock-in -----------------------------------------------------------------------


def test_bng_mosaic_423_no_degenerate_chips(spark_with_jar):
    """mosaic#423 (fixed): tessellate of a grid-aligned polygon yields NO degenerate
    POINT/LINESTRING chips, and the surviving cell-set is identical light == heavy.

    A box on exact 1km grid lines makes cell boundaries TOUCH polygon edges/vertices;
    the naive intersection would emit POINT/LINESTRING chips. Both tiers filter those
    (heavy: chip type must match input geom type), leaving only areal chips.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    df = spark.createDataFrame([(_wkb(_ALIGNED_BOX), _RES)], "geom binary, res int")

    def tess(mod, keep_core):
        """Both tiers called with explicit keep_core_geom for a clean parity comparison."""
        mod.register(spark)
        tess_col = mod.bng_tessellate(f.col("geom"), f.col("res"), f.lit(keep_core))
        exploded = df.select(f.explode(tess_col).alias("chip")).select(
            f.col("chip.cellid").alias("cell"), f.col("chip.chip").alias("geom")
        )
        return {r["cell"]: r["geom"] for r in exploded.collect()}

    light_tess = tess(gx, False)  # light: keep_core_geom=False, matches heavy
    heavy_tess = tess(hx, False)  # heavy: keep_core_geom=False

    # No surviving chip decodes to a non-areal (POINT/LINESTRING) geometry.
    for tier, chips in (("light", light_tess), ("heavy", heavy_tess)):
        for cell, blob in chips.items():
            if blob is None:
                continue  # core chip, no geometry
            g = from_wkb(bytes(blob))
            assert g.geom_type in (
                "Polygon",
                "MultiPolygon",
            ), f"#423 {tier} degenerate chip cell={cell}: type={g.geom_type}"

    assert set(light_tess.keys()) == set(heavy_tess.keys()), (
        f"#423 grid-aligned tessellate cell-set mismatch:\n"
        f"  light={sorted(light_tess)}\n  heavy={sorted(heavy_tess)}\n"
        f"  light_only={set(light_tess) - set(heavy_tess)} "
        f"heavy_only={set(heavy_tess) - set(light_tess)}"
    )


# --- grid-aligned geomkring / geomkloop lock-in -----------------------------------------------


@pytest.mark.parametrize("k", [1, 2])
def test_bng_geomkring_geomkloop_grid_aligned(spark_with_jar, k):
    """Light == heavy EXACT cell sets for geomkring/geomkloop on a grid-aligned box.

    Regression for the bench failure ``BNG GEOMKRING PARITY: FAIL -- ring set
    mismatch``. The 2.5km box's four edges sit on exact 1km grid lines, so every
    edge cell is fully covered yet grid-aligned. Heavy's JTS ``equalsExact`` keeps
    those cells BORDER (the clipped ring's start vertex is renormalized), so its
    geomkring/geomkloop expand them. Light formerly used an order-insensitive
    near-containment promotion that mislabeled them CORE, shrinking the border set
    and dropping the SW corner cells (e.g. TQ2979/TQ2980/TQ3079 at k=1). Light now
    mirrors heavy's vertex-order-sensitive ``equals_exact`` so the sets match.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    df = spark.createDataFrame(
        [(_wkb(_GRID_ALIGNED_BIG_BOX), _RES, k)], "geom binary, res int, k int"
    )

    def sets(mod):
        mod.register(spark)
        row = df.select(
            mod.bng_geomkring(f.col("geom"), f.col("res"), f.col("k")).alias("ring"),
            mod.bng_geomkloop(f.col("geom"), f.col("res"), f.col("k")).alias("loop"),
        ).collect()[0]
        return sorted(row["ring"]), sorted(row["loop"])

    light_ring, light_loop = sets(gx)
    heavy_ring, heavy_loop = sets(hx)

    assert light_ring == heavy_ring, (
        f"grid-aligned geomkring(k={k}) set mismatch:\n"
        f"  light_only={sorted(set(light_ring) - set(heavy_ring))}\n"
        f"  heavy_only={sorted(set(heavy_ring) - set(light_ring))}"
    )
    assert light_loop == heavy_loop, (
        f"grid-aligned geomkloop(k={k}) set mismatch:\n"
        f"  light_only={sorted(set(light_loop) - set(heavy_loop))}\n"
        f"  heavy_only={sorted(set(heavy_loop) - set(light_loop))}"
    )


# --- geom-encoding consistency ----------------------------------------------------------------


def test_bng_pointascell_all_four_encodings(spark_with_jar):
    """Geom-input consistency: WKB/EWKB/WKT/EWKT of the same point yield the same
    cell, in BOTH tiers (the parse_geom contract is shared across ST/grid)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    pt = _Point(_LON_E, _LON_N)
    wkb = bytearray(_to_wkb(pt))
    ewkb = bytearray(_to_wkb(pt, include_srid=True))  # SRID 0 EWKB
    wkt = pt.wkt
    ewkt = f"SRID=0;{pt.wkt}"

    # All four encodings as a single binary/string column is awkward; use a struct
    # of four geom columns and read pointascell off each.
    df = spark.createDataFrame(
        [(wkb, ewkb, wkt, ewkt, _RES)],
        "wkb binary, ewkb binary, wkt string, ewkt string, res int",
    )

    def cells(mod):
        mod.register(spark)
        return df.select(
            mod.bng_pointascell(f.col("wkb"), f.col("res")).alias("a"),
            mod.bng_pointascell(f.col("ewkb"), f.col("res")).alias("b"),
            mod.bng_pointascell(f.col("wkt"), f.col("res")).alias("c"),
            mod.bng_pointascell(f.col("ewkt"), f.col("res")).alias("d"),
        ).collect()[0]

    light = cells(gx)
    heavy = cells(hx)

    # all four encodings agree within each tier
    for tier, row in (("light", light), ("heavy", heavy)):
        assert row["a"] == row["b"] == row["c"] == row["d"], (
            f"{tier} encoding inconsistency: "
            f"wkb={row['a']} ewkb={row['b']} wkt={row['c']} ewkt={row['d']}"
        )
    # and light == heavy
    assert (
        light["a"] == heavy["a"]
    ), f"pointascell cross-tier mismatch: light={light['a']} heavy={heavy['a']}"
