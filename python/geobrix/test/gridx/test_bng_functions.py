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


# ====== Geometry Conversion Functions ======


def test_bng_aswkb(spark, bng_registered, test_data):
    """Test converting BNG cell to WKB format."""
    result = test_data.select(bng_registered.bng_aswkb(f.col("cellId")).alias("wkb"))
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["wkb"] is not None
        assert isinstance(row["wkb"], (bytes, bytearray))
        assert len(row["wkb"]) > 0


def test_bng_aswkt(spark, bng_registered, test_data):
    """Test converting BNG cell to WKT format."""
    result = test_data.select(bng_registered.bng_aswkt(f.col("cellId")).alias("wkt"))
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["wkt"] is not None
        assert isinstance(row["wkt"], str)
        assert "POLYGON" in row["wkt"]


# ====== Cell Properties ======


def test_bng_cellarea(spark, bng_registered, test_data):
    """Test getting cell area."""
    result = test_data.select(
        bng_registered.bng_cellarea(f.col("cellId")).alias("area")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["area"] is not None
        assert row["area"] > 0  # Area should be positive


def test_bng_centroid(spark, bng_registered, test_data):
    """Test getting cell centroid."""
    result = test_data.select(
        bng_registered.bng_centroid(f.col("cellId")).alias("centroid")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["centroid"] is not None
        assert isinstance(row["centroid"], (bytes, bytearray))  # WKB point


# ====== Cell Operations ======


def test_bng_cellintersection(spark, bng_registered, test_data):
    """Test cell intersection operation with proper cell struct."""
    # BNG cell operations expect struct(cellId, isCore, geometry)
    # Create simple WKB for testing
    wkb_bytes = bytes(
        [
            1,
            3,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            5,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            8,
            64,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            8,
            64,
            0,
            0,
            0,
            0,
            0,
            0,
            240,
            63,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            240,
            63,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    )

    result = test_data.select(
        bng_registered.bng_cellintersection(
            f.struct(f.col("cellId"), f.lit(True), f.lit(wkb_bytes)),
            f.struct(f.col("cellId"), f.lit(True), f.lit(wkb_bytes)),
        ).alias("intersection")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["intersection"] is not None


def test_bng_cellunion(spark, bng_registered, test_data):
    """Test cell union operation with proper cell struct."""
    wkb_bytes = bytes(
        [
            1,
            3,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            5,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            8,
            64,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            8,
            64,
            0,
            0,
            0,
            0,
            0,
            0,
            240,
            63,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            240,
            63,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    )

    result = test_data.select(
        bng_registered.bng_cellunion(
            f.struct(f.col("cellId"), f.lit(True), f.lit(wkb_bytes)),
            f.struct(f.col("cellId"), f.lit(True), f.lit(wkb_bytes)),
        ).alias("union")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["union"] is not None


# ====== Distance Functions ======


def test_bng_distance(spark, bng_registered, test_data):
    """Test distance between cells."""
    result = test_data.select(
        bng_registered.bng_distance(f.col("cellId"), f.lit("TQ388791")).alias(
            "distance"
        )
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["distance"] is not None
        assert row["distance"] >= 0


def test_bng_euclideandistance(spark, bng_registered, test_data):
    """Test Euclidean distance between cells."""
    result = test_data.select(
        bng_registered.bng_euclideandistance(f.col("cellId"), f.lit("TQ388791")).alias(
            "euclidean_distance"
        )
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["euclidean_distance"] is not None
        assert row["euclidean_distance"] >= 0


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


def test_bng_pointascell(spark, bng_registered):
    """Test converting point geometry to BNG cell."""
    # Use WKB point at valid BNG coordinates
    wkb_point = bytes([1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 64])
    result = spark.range(1).select(
        bng_registered.bng_pointascell(f.lit(wkb_point), f.lit(1)).alias("cell_id")
    )
    row = result.collect()[0]
    assert row["cell_id"] is not None


# ====== Polyfill ======


def test_bng_polyfill_wkt(spark, bng_registered):
    """Test polyfill with WKT polygon in valid BNG range."""
    # Polygon within BNG grid (around London area)
    # Use a larger polygon to ensure it intersects grid cells
    polygon_wkt = "POLYGON ((530000 180000, 531000 180000, 531000 181000, 530000 181000, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_polyfill(f.lit(polygon_wkt), f.lit(3)).alias("cells")
    )
    row = result.collect()[0]
    assert row["cells"] is not None
    assert isinstance(row["cells"], list)


def test_bng_polyfill_string_resolution(spark, bng_registered):
    """Test polyfill with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_polyfill(f.lit(polygon_wkt), f.lit("50m")).alias("cells")
    )
    row = result.collect()[0]
    assert row["cells"] is not None


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


def test_bng_kloop(spark, bng_registered, test_data):
    """Test k-loop operation."""
    result = test_data.select(
        bng_registered.bng_kloop(f.col("cellId"), f.lit(1)).alias("kloop")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["kloop"] is not None
        assert len(row["kloop"]) > 0  # Should return array of cells


def test_bng_kring(spark, bng_registered, test_data):
    """Test k-ring operation."""
    result = test_data.select(
        bng_registered.bng_kring(f.col("cellId"), f.lit(1)).alias("kring")
    )
    rows = result.collect()
    assert len(rows) == 3
    for row in rows:
        assert row["kring"] is not None
        assert len(row["kring"]) > 0  # Should return array of cells


def test_bng_geomkloop_wkt(spark, bng_registered):
    """Test geometry k-loop with WKT polygon in valid BNG range."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_geomkloop(f.lit(polygon_wkt), f.lit(1), f.lit(1)).alias(
            "geomkloop"
        )
    )
    row = result.collect()[0]
    assert row["geomkloop"] is not None
    assert len(row["geomkloop"]) > 0


def test_bng_geomkring_wkt(spark, bng_registered):
    """Test geometry k-ring with WKT polygon in valid BNG range."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_geomkring(f.lit(polygon_wkt), f.lit(1), f.lit(1)).alias(
            "geomkring"
        )
    )
    row = result.collect()[0]
    assert row["geomkring"] is not None
    assert len(row["geomkring"]) > 0


def test_bng_geomkring_mode_string(spark, bng_registered):
    """bng_geomkring accepts a bare string mode (auto-promoted to literal, not column ref)."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    for mode in ("boundary-out", "boundary-in"):
        col = bng_registered.bng_geomkring(f.lit(polygon_wkt), f.lit(1), f.lit(1), mode)
        assert hasattr(
            col, "_jc"
        ), f"bng_geomkring(mode={mode!r}) did not return a Column"
        row = spark.range(1).select(col.alias("r")).collect()[0]
        assert row["r"] is not None, f"bng_geomkring(mode={mode!r}) returned null"


def test_bng_geomkloop_mode_string(spark, bng_registered):
    """bng_geomkloop accepts a bare string mode (auto-promoted to literal, not column ref)."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    for mode in ("boundary-out", "boundary-in"):
        col = bng_registered.bng_geomkloop(f.lit(polygon_wkt), f.lit(1), f.lit(1), mode)
        assert hasattr(
            col, "_jc"
        ), f"bng_geomkloop(mode={mode!r}) did not return a Column"
        row = spark.range(1).select(col.alias("r")).collect()[0]
        assert row["r"] is not None, f"bng_geomkloop(mode={mode!r}) returned null"


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
# Note: Python functions already wrap with f.explode(), so these work directly


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
    """Tessellate explode returns cellid, core and chip; a WKT geometry gives a STRING chip."""
    polygon_wkt = "POLYGON ((530000 180000, 530100 180000, 530100 180100, 530000 180100, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellateexplode(
            f.lit(polygon_wkt), f.lit(1), keep_core_geom=True
        )
    )
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "string"),
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
    result = df.select(
        bng_registered.bng_tessellateexplode(
            f.col("geom"), f.lit(3), keep_core_geom=False
        )
    )
    assert result.dtypes == [
        ("cellid", "string"),
        ("core", "boolean"),
        ("chip", "binary"),
    ]
    rows = result.collect()
    assert any(r["core"] and r["chip"] is None for r in rows)
    assert any(not r["core"] and r["chip"] is not None for r in rows)


def test_bng_tessellateexplode_string_resolution(spark, bng_registered):
    """Test tessellate explode with string resolution."""
    polygon_wkt = "POLYGON ((530000 180000, 530500 180000, 530500 180500, 530000 180500, 530000 180000))"
    result = spark.range(1).select(
        bng_registered.bng_tessellateexplode(
            f.lit(polygon_wkt), f.lit("500m"), keep_core_geom=True
        )
    )
    assert result.columns == ["cellid", "core", "chip"]
    rows = result.collect()
    assert len(rows) > 0


def test_bng_scalar_literal_args(spark, bng_registered):
    """Plain Python non-string scalars (int) are accepted in place of f.lit(...).

    Verifies ColLike/_col() on gridx/bng: booleans/ints auto-wrap via f.lit,
    so ``bng_pointascell(pt, 1)`` and ``bng_kloop(cell, 1)`` work without
    wrapping. Strings still pass through as pyspark column refs; use f.lit for
    string literals (e.g. resolution="1km").
    """
    polygon_wkt = "POLYGON ((530000 180000, 530500 180000, 530500 180500, 530000 180500, 530000 180000))"
    df = spark.range(1).select(
        bng_registered.bng_pointascell(f.lit("POINT (400000 400000)"), 1).alias(
            "cell_int_res"
        ),
        bng_registered.bng_pointascell(
            f.lit("POINT (400000 400000)"), f.lit("1km")
        ).alias("cell_str_res"),
        bng_registered.bng_polyfill(f.lit(polygon_wkt), 1).alias("cells_int_res"),
        bng_registered.bng_kloop(f.lit("TQ388791"), 1).alias("kloop_int_k"),
    )
    row = df.collect()[0]
    assert row["cell_int_res"] is not None
    assert row["cell_str_res"] is not None
    assert row["cells_int_res"] is not None
    assert row["kloop_int_k"] is not None
