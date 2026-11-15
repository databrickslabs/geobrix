"""Light (pyvx) vs heavy (vectorx) TIN parity — both modes.

Both tiers implement ``mode="constrained"`` (default, no Steiner points) and
``mode="conforming"``. Light uses scipy/Qhull; heavy uses JTS
``DelaunayTriangulationBuilder`` + Sloan edge-flip constraint recovery.

INVOCATION SURFACES DIFFER PER TIER (important):
  - **Light** registers ``gbx_st_triangulate`` / ``gbx_st_interpolateelevationbbox``
    as PySpark **UDTFs**, invoked via SQL ``LATERAL gbx_st_triangulate(...)``.
  - **Heavy** registers them as JVM **Generator expressions**, invoked as a
    top-level generator Column — ``f.call_function("gbx_st_triangulate", ..., f.lit(mode))``
    inside ``.select(...)``. A single-field struct unwraps, so the selected column
    holds the WKB directly.

Both tiers register under the SAME SQL name and a later registration replaces
the earlier one, so each leg registers its own level (L3 or L1) right before it
runs and its rows are collected before the other leg registers. Each tier is
invoked through its own native surface — light via SQL LATERAL, heavy via the
generator-Column DataFrame API — and the decoded outputs are compared.

Parity posture (per the pyvx TIN spec):
- **No breaklines** → Delaunay is ~unique for points in general position: assert
  same triangle COUNT and matching triangle centroids within 1e-6. Observed: for
  the general-position 7-point set, Qhull (light) and JTS (heavy) produce the
  EXACT same 8 triangles (centroid sets identical) — zero divergence. The
  centroid-match form additionally tolerates cocircular tie-breaks should they
  ever arise, while still proving the same triangle partition.
- **With breaklines** (``mode="constrained"`` both tiers) → assert the constraint
  segment is recovered as a triangle EDGE in BOTH tiers, and the interpolated Z at
  a sample grid matches within 1e-6 — NOT triangle identity (no-Steiner recovery
  may pick different non-constraint diagonals). Observed max interp Z divergence:
  ~1.8e-15 (floating-point noise).
- **mode="conforming"** → both tiers run JTS 1.20's conforming Delaunay (light: a port
  in ``pyvx/_conforming.py``): the triangles are the same, every X and Y bit for bit.

Heavy requires the geobrix JAR (Scala/JTS). The JAR is present in the
geobrix-dev Docker container; this test auto-skips when the JAR is not staged
under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyvx/test_parity_tin.py \\
        --with-integration --log parity-tin.log
"""

import logging
from pathlib import Path

import pytest

pytest.importorskip("scipy")
from shapely import to_wkb, wkb  # noqa: E402
from shapely.geometry import LineString, Point  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyvx -> test -> python/geobrix)
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
                "gbx:test:python --path python/geobrix/test/pyvx/test_parity_tin.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pyvx-tin-parity")
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


# --- helpers ----------------------------------------------------------------------------------


def _pts_wkb(coords):
    return [bytearray(to_wkb(Point(*c))) for c in coords]


def _leg(spark, level):
    """Register ``gbx_st_*`` at ``level`` for the leg that runs next. Both tiers use the same SQL names and a later
    registration replaces an earlier one, so each leg registers right before it runs and its rows are collected
    before the other leg registers."""
    from databricks.labs.gbx import manager as gbx_manager

    names = gbx_manager.names_at_level(("gbx_st_",), level)
    gbx_manager.register_family(spark, ("gbx_st_",), only=names, level=level)


def _light_triangles(spark, view, mode):
    """Light triangles via SQL LATERAL UDTF → list of WKB-bearing rows (col 'triangle')."""
    _leg(spark, "L3")
    return spark.sql(
        "SELECT t.triangle FROM " + view + ", LATERAL "
        "gbx_st_triangulate(pts, bl, mt, st, spf, '" + mode + "') t"
    ).collect()


def _heavy_triangles(df, mode):
    """Heavy triangles via the JVM generator Column. The single-field struct
    unwraps, so the aliased column holds the triangle WKB directly."""
    from pyspark.sql import functions as f

    _leg(df.sparkSession, "L1")
    return df.select(
        f.call_function(
            "gbx_st_triangulate",
            f.col("pts"),
            f.col("bl"),
            f.col("mt"),
            f.col("st"),
            f.col("spf"),
            f.lit(mode),
        ).alias("triangle")
    ).collect()


def _centroid_xy(blob, ndigits=6):
    g = wkb.loads(bytes(blob))
    c = g.centroid
    return (round(c.x, ndigits), round(c.y, ndigits))


def _triangle_centroids(rows, col="triangle"):
    return sorted(_centroid_xy(r[col]) for r in rows)


def _triangle_edges(rows, col="triangle", ndigits=6):
    """Set of undirected XY edges (each a frozenset of two rounded (x,y) tuples)."""
    edges = set()
    for r in rows:
        g = wkb.loads(bytes(r[col]))
        ring = [(round(p[0], ndigits), round(p[1], ndigits)) for p in g.exterior.coords]
        for i in range(len(ring) - 1):
            edges.add(frozenset((ring[i], ring[i + 1])))
    return edges


def _interp_grid_light(spark, view, mode):
    _leg(spark, "L3")
    rows = spark.sql(
        "SELECT t.elevation_point AS p FROM " + view + ", LATERAL "
        "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, w, h, srid, '"
        + mode
        + "') t"
    ).collect()
    return _grid_dict(rows, "p")


def _interp_grid_heavy(df, mode):
    from pyspark.sql import functions as f

    _leg(df.sparkSession, "L1")
    rows = df.select(
        f.call_function(
            "gbx_st_interpolateelevationbbox",
            f.col("pts"),
            f.col("bl"),
            f.col("mt"),
            f.col("st"),
            f.col("spf"),
            f.col("xmin"),
            f.col("ymin"),
            f.col("xmax"),
            f.col("ymax"),
            f.col("w"),
            f.col("h"),
            f.col("srid"),
            f.lit(mode),
        ).alias("p")
    ).collect()
    return _grid_dict(rows, "p")


def _grid_dict(rows, col):
    out = {}
    for r in rows:
        g = wkb.loads(bytes(r[col]))
        out[(round(g.x, 6), round(g.y, 6))] = g.z
    return out


# A 7-point set in general position (no four cocircular) so Delaunay is unique.
_GENERAL_PTS = [
    (0.0, 0.0, 0.0),
    (10.0, 0.0, 5.0),
    (10.0, 10.0, 12.0),
    (0.0, 10.0, 7.0),
    (3.0, 4.0, 3.0),
    (7.0, 2.0, 6.0),
    (4.0, 8.0, 9.0),
]


# --- tests ------------------------------------------------------------------------------------


def test_triangulate_parity_no_breaklines(spark_with_jar):
    """No breaklines: Delaunay ~unique → same triangle count + matching centroids.

    Assertion form: same COUNT, plus every light triangle centroid has a heavy
    centroid within 1e-6 (and vice-versa). Robust to Qhull-vs-JTS cocircular
    tie-breaks. (Observed for this general-position set: centroid sets are
    EXACTLY equal — zero divergence between light scipy and heavy JTS.)
    """
    spark = spark_with_jar

    pts = _pts_wkb(_GENERAL_PTS)
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("tin_nob")

    light_rows = _light_triangles(spark, "tin_nob", "constrained")
    heavy_rows = _heavy_triangles(df, "constrained")

    assert (
        len(light_rows) == len(heavy_rows) > 0
    ), f"triangle count mismatch: light={len(light_rows)} heavy={len(heavy_rows)}"

    lc = _triangle_centroids(light_rows)
    hc = _triangle_centroids(heavy_rows)

    def _unmatched(a, b):
        for cx, cy in a:
            if not any(abs(cx - bx) < 1e-6 and abs(cy - by) < 1e-6 for bx, by in b):
                return (cx, cy)
        return None

    miss_lh = _unmatched(lc, hc)
    assert (
        miss_lh is None
    ), f"light centroid {miss_lh} has no heavy match within 1e-6; heavy={hc}"
    miss_hl = _unmatched(hc, lc)
    assert (
        miss_hl is None
    ), f"heavy centroid {miss_hl} has no light match within 1e-6; light={lc}"


def test_interpolate_parity_surface_closeness(spark_with_jar):
    """Same points + a bbox grid: per-cell interpolated Z must match within 1e-6.

    Join on rounded XY; assert same in-hull cell set and matching Z.
    (Observed max Z divergence: ~1.8e-15 — floating-point noise.)
    """
    spark = spark_with_jar

    pts = _pts_wkb(_GENERAL_PTS)
    df = spark.createDataFrame(
        [(pts, [], 0.0, 0.0, "NONENCROACHING", 0.0, 0.0, 10.0, 10.0, 7, 7, 0)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "xmin double, ymin double, xmax double, ymax double, w int, h int, srid int",
    )
    df.createOrReplaceTempView("tin_interp")

    light = _interp_grid_light(spark, "tin_interp", "constrained")
    heavy = _interp_grid_heavy(df, "constrained")

    assert light.keys() == heavy.keys(), (
        f"in-hull cell mismatch: light_only={set(light) - set(heavy)} "
        f"heavy_only={set(heavy) - set(light)}"
    )
    assert len(light) > 0, "no in-hull cells produced"
    for k in light:
        assert (
            abs(light[k] - heavy[k]) < 1e-6
        ), f"Z mismatch at {k}: light={light[k]} heavy={heavy[k]}"


def test_triangulate_breakline_edges_present_both(spark_with_jar):
    """A single constraint segment between two existing sites must be recovered as a
    triangle edge in BOTH tiers (constrained mode).

    The breakline is the diagonal (0,0)->(10,10) across a 4-corner square. Without
    the constraint the Delaunay diagonal could flip the other way; with it, both
    tiers must carry the (0,0)-(10,10) edge. This configuration is recoverable
    WITHOUT Steiner points in both tiers — a single segment between two mass points
    with a clear flip path. (A breakline whose endpoints are NOT triangulation
    sites, or one that crosses many sites, may be un-recoverable without Steiner
    insertion in the no-Steiner 'constrained' mode; that limitation is documented
    and intentionally not exercised here.)
    """
    spark = spark_with_jar

    corners = [(0.0, 0.0, 0.0), (10.0, 0.0, 0.0), (10.0, 10.0, 0.0), (0.0, 10.0, 0.0)]
    pts = _pts_wkb(corners)
    bl = [bytearray(to_wkb(LineString([(0.0, 0.0), (10.0, 10.0)])))]
    df = spark.createDataFrame(
        [(pts, bl, 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    df.createOrReplaceTempView("tin_bl")

    constraint_edge = frozenset(((0.0, 0.0), (10.0, 10.0)))

    light_rows = _light_triangles(spark, "tin_bl", "constrained")
    heavy_rows = _heavy_triangles(df, "constrained")

    light_edges = _triangle_edges(light_rows)
    heavy_edges = _triangle_edges(heavy_rows)

    assert constraint_edge in light_edges, (
        f"light constrained TIN missing constraint edge {set(constraint_edge)}; "
        f"edges={[set(e) for e in light_edges]}"
    )
    assert constraint_edge in heavy_edges, (
        f"heavy constrained TIN missing constraint edge {set(constraint_edge)}; "
        f"edges={[set(e) for e in heavy_edges]}"
    )


def _exact_triangles(rows, col="triangle"):
    out = set()
    for r in rows:
        ring = list(wkb.loads(bytes(r[col])).exterior.coords)[:-1]
        out.add(frozenset((float(x), float(y)) for x, y in ring))
    return out


@pytest.mark.parametrize(
    "spf,snap", [("NONENCROACHING", 0.0), ("MIDPOINT", 0.0), ("NONENCROACHING", 0.5)]
)
def test_conforming_parity_exact(spark_with_jar, spf, snap):
    """mode='conforming' gives the same triangles on both levels, every X and Y bit for bit, the extra points and the
    snap of points onto break lines included."""
    import numpy as np

    spark = spark_with_jar
    rng = np.random.default_rng(11)
    xy = rng.normal(50, 15, (300, 2))
    pts = _pts_wkb([(x, y, 0.1 * x + 0.05 * y) for x, y in xy])
    lines = [
        [(20.0, 30.0, 4.0), (45.0, 41.0, 6.5), (80.0, 38.0, 9.0)],
        [(30.0, 70.0, 6.0), (70.0, 75.0, 8.0)],
        [(35.0, 10.0, 3.0), (60.0, 25.0, 5.0)],
    ]
    bl = [bytearray(to_wkb(LineString(ln))) for ln in lines]
    df = spark.createDataFrame(
        [(pts, bl, 0.0, snap, spf)],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    view = f"tin_conf_{spf.lower()}_{int(snap * 10)}"
    df.createOrReplaceTempView(view)
    light = _exact_triangles(_light_triangles(spark, view, "conforming"))
    heavy = _exact_triangles(_heavy_triangles(df, "conforming"))
    assert len(light) > 500
    inputs = {(float(x), float(y)) for x, y in xy.tolist()}
    steiner = (
        {v for t in heavy for v in t}
        - inputs
        - {(x, y) for ln in lines for x, y, _ in ln}
    )
    assert steiner, "the break lines should need extra points"
    assert light == heavy, f"{len(light ^ heavy)} triangles differ"


@pytest.mark.parametrize("mode", ["constrained", "conforming"])
@pytest.mark.parametrize("kind", ["grid", "random", "random_bl"])
def test_triangle_wkb_bytes_are_the_same_on_both_levels(spark_with_jar, mode, kind):
    """Each level writes a triangle counter-clockwise from its lowest vertex, so the raw WKB bytes agree, not only the
    vertex sets: a co-circular grid (JTS's own ties), random sites, and random sites with break lines.
    """
    import numpy as np

    spark = spark_with_jar
    rng = np.random.default_rng(23)
    if kind == "grid":
        xy = np.array([(x * 2.0, y * 2.0) for x in range(9) for y in range(9)])
    else:
        xy = rng.random((200, 2)) * 100
    pts = _pts_wkb([(x, y, 0.01 * x) for x, y in xy.tolist()])
    span = float(xy.max())
    lines = (
        [
            [
                (0.13 * span, 0.27 * span, 1.0),
                (0.6 * span, 0.4 * span, 2.0),
                (0.85 * span, 0.7 * span, 1.0),
            ]
        ]
        if kind != "random"
        else []
    )
    bl = [bytearray(to_wkb(LineString(ln))) for ln in lines]
    df = spark.createDataFrame(
        [(pts, bl, 0.0, 0.0, "NONENCROACHING")],
        "pts array<binary>, bl array<binary>, mt double, st double, spf string",
    )
    view = f"tin_bytes_{kind}_{mode}"
    df.createOrReplaceTempView(view)
    light = sorted(bytes(r["triangle"]) for r in _light_triangles(spark, view, mode))
    heavy = sorted(bytes(r["triangle"]) for r in _heavy_triangles(df, mode))
    assert len(light) > 50
    assert light == heavy, f"{len(set(light) ^ set(heavy))} triangle WKBs differ"
