"""
Comprehensive tests for all BNG (British National Grid) functions.

Tests use valid BNG cell IDs and resolutions based on Scala test patterns:
- Valid cell IDs: TQ388791, TQ388792, TQ388793
- Valid resolutions: 1, 3 (grid levels) or "50m", "500m" (meters)
- Valid coordinates: 100000, 200000 (within BNG grid)
"""

import logging
import struct
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=INFO,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def bng_registered(spark):
    """Register BNG functions once for all tests."""
    from databricks.labs.gbx.gridx.bng import functions as bng_funcs

    bng_funcs.register(spark)
    return bng_funcs


@pytest.fixture(scope="session")
def test_data(spark):
    """Create test data with valid BNG cell IDs."""
    return spark.createDataFrame(
        [("TQ388791",), ("TQ388792",), ("TQ388793",)], ["cellId"]
    )


# ====== Coordinate Conversion ======


def test_bng_eastnorthasbng(spark, bng_registered):
    """Test converting east/north coordinates to BNG cell."""
    # Use valid BNG coordinates (100000, 200000) with resolution 3 (from Scala tests)
    result = spark.range(1).select(
        bng_registered.bng_eastnorthasbng(f.lit(100000), f.lit(200000), f.lit(3)).alias(
            "cell_id"
        )
    )
    row = result.collect()[0]
    assert row["cell_id"] is not None
    assert isinstance(row["cell_id"], str)
    assert len(row["cell_id"]) > 0


def test_bng_eastnorthasbng_string_resolution(spark, bng_registered):
    """Test converting east/north coordinates with string resolution."""
    result = spark.range(1).select(
        bng_registered.bng_eastnorthasbng(
            f.lit(100000), f.lit(200000), f.lit("50m")
        ).alias("cell_id")
    )
    row = result.collect()[0]
    assert row["cell_id"] is not None
    assert isinstance(row["cell_id"], str)


# ====== Tessellate ======


def test_bng_tessellate_wkt(spark, bng_registered):
    """Test tessellate with WKT polygon."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit(1), keep_core_geom=True
        ).alias("tessellation")
    )
    row = result.collect()[0]
    assert row["tessellation"] is not None


def test_bng_tessellate_string_resolution(spark, bng_registered):
    """Test tessellate with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit("50m"), keep_core_geom=False
        ).alias("tessellation")
    )
    row = result.collect()[0]
    assert row["tessellation"] is not None


# ====== K-Ring Functions ======


def test_bng_geomkringexplode_mode_column_builds(spark, bng_registered):
    """bng_geomkringexplode with a mode string builds a Column without raising."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    col = bng_registered.bng_geomkringexplode(
        f.lit(polygon_wkt), f.lit(1), f.lit(1), "boundary-out"
    )
    assert hasattr(col, "_jc"), "bng_geomkringexplode(mode=...) did not return a Column"


def test_bng_geomkloopexplode_mode_column_builds(spark, bng_registered):
    """bng_geomkloopexplode with a mode string builds a Column without raising."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    col = bng_registered.bng_geomkloopexplode(
        f.lit(polygon_wkt), f.lit(1), f.lit(1), "boundary-out"
    )
    assert hasattr(col, "_jc"), "bng_geomkloopexplode(mode=...) did not return a Column"


# The documented 3 km x 3 km polygon (EPSG:27700) and its centre cell at 1 km.
_DOC_POLY = "POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))"


@pytest.mark.parametrize(
    "wrapper,args,expected",
    [
        ("bng_kringexplode", ("TQ3080", 1), 9),
        ("bng_kloopexplode", ("TQ3080", 1), 8),
        ("bng_geomkringexplode", (_DOC_POLY, 3, 1), 24),
        ("bng_geomkloopexplode", (_DOC_POLY, 3, 1), 16),
        ("bng_geomkringexplode", (_DOC_POLY, 3, 1, "boundary-out"), 24),
        ("bng_geomkloopexplode", (_DOC_POLY, 3, 1, "boundary-out"), 16),
    ],
)
def test_bng_explode_wrappers_evaluate_to_one_row_per_cell(
    spark, bng_registered, wrapper, args, expected
):
    """The Python explode wrappers evaluate in a select, one ``cellid`` row per cell.

    The first argument is a column of the input DataFrame; the rest are scalars.
    """
    first, *rest = args
    df = spark.createDataFrame([(first,)], "src string")
    result = df.select(getattr(bng_registered, wrapper)(f.col("src"), *rest))
    assert result.columns == ["cellid"]
    cells = [r["cellid"] for r in result.collect()]
    assert len(cells) == expected
    assert len(set(cells)) == expected and all(isinstance(c, str) for c in cells)


def test_bng_tessellate_keep_core_geom_defaults_to_false(spark, bng_registered):
    """bng_tessellate leaves core chips NULL unless keep_core_geom is set, like SQL's 2-arg form."""
    df = spark.createDataFrame([(_DOC_POLY,)], "geom string")

    def chips(col):
        return {c["cellid"]: c for c in df.select(col.alias("t")).collect()[0]["t"]}

    default = chips(bng_registered.bng_tessellate(f.col("geom"), 3))
    kept = chips(bng_registered.bng_tessellate(f.col("geom"), 3, keep_core_geom=True))
    df.createOrReplaceTempView("bng_tess_default")
    sql_two_arg = {
        c["cellid"]: c
        for c in spark.sql(
            "SELECT gbx_bng_tessellate(geom, 3) AS t FROM bng_tess_default"
        ).collect()[0]["t"]
    }

    assert len(default) == 9 and default["TQ3080"]["core"]
    assert default["TQ3080"]["chip"] is None
    assert kept["TQ3080"]["chip"] is not None
    assert default == sql_two_arg


# ====== Aggregator Functions ======


def test_bng_cellintersection_agg(spark, bng_registered, test_data):
    """Test cell intersection aggregator."""
    # For aggregators, we need cells from tessellation/polyfill
    # Use tessellate to get proper cell structures
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    tessellated = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit(1), keep_core_geom=True
        ).alias("cells")
    )

    # Explode the array of cells and aggregate
    result = (
        tessellated.selectExpr("explode(cells) as cell")
        .groupBy()
        .agg(
            bng_registered.bng_cellintersection_agg(f.col("cell")).alias("intersection")
        )
    )
    row = result.collect()[0]
    assert row["intersection"] is not None


def test_bng_cellunion_agg(spark, bng_registered, test_data):
    """Test cell union aggregator."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    tessellated = spark.range(1).select(
        bng_registered.bng_tessellate(
            f.lit(polygon_wkt), f.lit(1), keep_core_geom=True
        ).alias("cells")
    )

    # Explode the array of cells and aggregate
    result = (
        tessellated.selectExpr("explode(cells) as cell")
        .groupBy()
        .agg(bng_registered.bng_cellunion_agg(f.col("cell")).alias("union"))
    )
    row = result.collect()[0]
    assert row["union"] is not None


# ====== Generator/Explode Functions ======
# The Python explode wrappers return the generator itself; select it directly.


def test_bng_kloopexplode(spark, bng_registered, test_data):
    """Test k-loop explode operation using SQL expression."""
    # Register a temp view to use SQL expression (generators work in SQL)
    test_data.limit(1).createOrReplaceTempView("bng_test_cells")
    result = spark.sql(
        "SELECT gbx_bng_kloopexplode(cellId, 1) as cell FROM bng_test_cells"
    )
    rows = result.collect()
    assert len(rows) > 0  # Should explode cell into multiple neighbors
    for row in rows:
        assert row["cell"] is not None
        assert isinstance(row["cell"], str)


def test_bng_kringexplode(spark, bng_registered, test_data):
    """Test k-ring explode operation using SQL expression."""
    test_data.limit(1).createOrReplaceTempView("bng_test_cells2")
    result = spark.sql(
        "SELECT gbx_bng_kringexplode(cellId, 1) as cell FROM bng_test_cells2"
    )
    rows = result.collect()
    assert len(rows) > 0  # Should explode cell into multiple neighbors
    for row in rows:
        assert row["cell"] is not None
        assert isinstance(row["cell"], str)


def test_bng_geomkloopexplode_wkt(spark, bng_registered):
    """Test geometry k-loop explode with WKT using SQL."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.sql(
        f"SELECT gbx_bng_geomkloopexplode('{polygon_wkt}', 1, 1) as cell"
    )
    rows = result.collect()
    assert len(rows) > 0
    for row in rows:
        assert row["cell"] is not None


def test_bng_geomkringexplode_wkt(spark, bng_registered):
    """Test geometry k-ring explode with WKT using SQL."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.sql(
        f"SELECT gbx_bng_geomkringexplode('{polygon_wkt}', 1, 1) as cell"
    )
    rows = result.collect()
    assert len(rows) > 0
    for row in rows:
        assert row["cell"] is not None


def test_bng_tessellateexplode_wkt(spark, bng_registered):
    """Tessellate explode returns cellid, core and chip; the chip is WKB (BINARY) for WKT input too."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    df = spark.createDataFrame([(polygon_wkt,)], "geom string")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", 1, keep_core_geom=True
    ).select("cellid", "core", "chip")
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "binary"),
    ]
    rows = result.collect()
    assert len(rows) > 0
    # keep_core_geom=True: every chip, core or border, carries its geometry.
    assert all(r["cellid"] is not None and r["chip"] is not None for r in rows)


def test_bng_tessellateexplode_wkb_chip_is_binary(spark, bng_registered):
    """A WKB geometry gives a BINARY chip; keep_core_geom=False leaves core chips NULL."""
    # little-endian WKB of a 2.5 km box on exact 1 km grid lines: one core cell, eight border cells
    ring = [
        (530000.0, 180000.0),
        (532500.0, 180000.0),
        (532500.0, 182500.0),
        (530000.0, 182500.0),
        (530000.0, 180000.0),
    ]
    wkb = struct.pack("<BIII", 1, 3, 1, len(ring)) + b"".join(
        struct.pack("<dd", x, y) for x, y in ring
    )
    df = spark.createDataFrame([(bytearray(wkb),)], "geom binary")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", 3, keep_core_geom=False
    ).select("cellid", "core", "chip")
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "binary"),
    ]
    rows = result.collect()
    assert any(r["core"] and r["chip"] is None for r in rows)
    assert any(not r["core"] and r["chip"] is not None for r in rows)


def test_bng_tessellateexplode_keep_core_geom_defaults_to_false(spark, bng_registered):
    """Without keep_core_geom the core chips are NULL, as in the lightweight tier."""
    ring = [
        (530000.0, 180000.0),
        (532500.0, 180000.0),
        (532500.0, 182500.0),
        (530000.0, 182500.0),
        (530000.0, 180000.0),
    ]
    wkb = struct.pack("<BIII", 1, 3, 1, len(ring)) + b"".join(
        struct.pack("<dd", x, y) for x, y in ring
    )
    df = spark.createDataFrame([(bytearray(wkb),)], "geom binary")
    default_rows = (
        bng_registered.bng_tessellateexplode(df, "geom", 3)
        .select("cellid", "core", "chip")
        .collect()
    )
    explicit_rows = (
        bng_registered.bng_tessellateexplode(df, "geom", 3, keep_core_geom=False)
        .select("cellid", "core", "chip")
        .collect()
    )
    assert sorted(default_rows) == sorted(explicit_rows)
    assert any(r["core"] and r["chip"] is None for r in default_rows)

    # the two-argument SQL form (the lightweight tier's signature) is the same call
    df.createOrReplaceTempView("bng_te_two_arg")
    sql_rows = spark.sql(
        "SELECT cellid, core, chip FROM bng_te_two_arg "
        "LATERAL VIEW gbx_bng_tessellateexplode(geom, 3) t AS cellid, core, chip"
    ).collect()
    assert sorted(sql_rows) == sorted(default_rows)


def test_bng_tessellateexplode_string_resolution(spark, bng_registered):
    """Test tessellate explode with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530500 180000, 530500 180500, 530000 180500, 530000 180000))"
    # a str names a column in the DataFrame API, so the "500m" size sits in a column
    df = spark.createDataFrame([(polygon_wkt, "500m")], "geom string, res string")
    result = bng_registered.bng_tessellateexplode(
        df, "geom", "res", keep_core_geom=True
    ).select("cellid", "core", "chip")
    assert result.columns == ["cellid", "core", "chip"]
    rows = result.collect()
    assert len(rows) > 0
