import logging
import pytest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (SparkSession.builder
             .config("spark.driver.extraJavaOptions",
                     "-Dlog4j.rootLogger=ERROR,console "
                     "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native")
             .config("spark.jars", str(JAR))
             .getOrCreate())
    from databricks.labs.gbx.gridx.bng import functions as bng_funcs
    bng_funcs.register(spark)
    return spark


def test_bng_aswkb(spark):
    """Test converting BNG cell ID to WKB."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_aswkb(f.lit("TQ388791")).alias("wkb")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["wkb"] is not None
    assert isinstance(result[0]["wkb"], (bytes, bytearray))


def test_bng_aswkt(spark):
    """Test converting BNG cell ID to WKT."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_aswkt(f.lit("TQ388791")).alias("wkt")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["wkt"] is not None
    assert isinstance(result[0]["wkt"], str)
    assert "POLYGON" in result[0]["wkt"]


def test_bng_cellarea(spark):
    """Test getting BNG cell area."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_cellarea(f.lit("TQ388791")).alias("area")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["area"] is not None
    assert result[0]["area"] > 0


def test_bng_cellintersection(spark):
    """Test BNG cell intersection."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    from shapely import wkt
    import shapely
    
    # Create two cell structs with overlapping geometry
    polygon1_wkb = shapely.to_wkb(wkt.loads("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"))
    polygon2_wkb = shapely.to_wkb(wkt.loads("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"))
    
    df = spark.createDataFrame([
        ("TQ388791", True, polygon1_wkb, "TQ388792", True, polygon2_wkb)
    ], ["cellId1", "core1", "chip1", "cellId2", "core2", "chip2"])
    
    result_df = df.select(
        bng.bng_cellintersection(
            f.struct("cellId1", "core1", "chip1"),
            f.struct("cellId2", "core2", "chip2")
        ).alias("intersection")
    )
    
    result = result_df.collect()
    assert result is not None
    assert result[0]["intersection"] is not None


def test_bng_cellunion(spark):
    """Test BNG cell union."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    from shapely import wkt
    import shapely
    
    # Create two cell structs with adjacent geometry
    polygon1_wkb = shapely.to_wkb(wkt.loads("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"))
    polygon2_wkb = shapely.to_wkb(wkt.loads("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))"))
    
    df = spark.createDataFrame([
        ("TQ388791", True, polygon1_wkb, "TQ388792", True, polygon2_wkb)
    ], ["cellId1", "core1", "chip1", "cellId2", "core2", "chip2"])
    
    result_df = df.select(
        bng.bng_cellunion(
            f.struct("cellId1", "core1", "chip1"),
            f.struct("cellId2", "core2", "chip2")
        ).alias("union")
    )
    
    result = result_df.collect()
    assert result is not None
    assert result[0]["union"] is not None


def test_bng_centroid(spark):
    """Test getting BNG cell centroid."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_centroid(f.lit("TQ388791")).alias("centroid")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["centroid"] is not None


def test_bng_distance(spark):
    """Test BNG distance between cells."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_distance(
            f.lit("TQ388791"),
            f.lit("TQ388792")
        ).alias("distance")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["distance"] is not None
    assert result[0]["distance"] >= 0


def test_bng_euclideandistance(spark):
    """Test BNG Euclidean distance between cells."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_euclideandistance(
            f.lit("TQ388791"),
            f.lit("TQ388792")
        ).alias("euclidean_dist")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["euclidean_dist"] is not None
    assert result[0]["euclidean_dist"] >= 0


def test_bng_eastnorthasbng(spark):
    """Test converting east/north coordinates to BNG."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use coordinates from Scala test (SV grid square)
    df = spark.range(1).select(
        bng.bng_eastnorthasbng(
            f.lit(19922),  # easting
            f.lit(22219),  # northing
            f.lit(3)       # resolution (1km)
        ).alias("bng_cell")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["bng_cell"] is not None
    assert isinstance(result[0]["bng_cell"], str)
    assert result[0]["bng_cell"] == "SV1922"  # Expected result from Scala test


def test_bng_pointasbng(spark):
    """Test converting point geometry to BNG."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use valid BNG coordinates from Scala test
    point_wkt = "POINT(199222 230330)"
    
    df = spark.range(1).select(
        bng.bng_pointasbng(
            f.lit(point_wkt),
            f.lit(3)  # resolution (1km)
        ).alias("bng_cell")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["bng_cell"] is not None
    assert result[0]["bng_cell"] == "SM9930"  # Expected from Scala test


def test_bng_polyfill(spark):
    """Test polyfill operation."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use valid BNG coordinates from Scala test
    polygon_wkt = "POLYGON((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
    
    df = spark.range(1).select(
        bng.bng_polyfill(
            f.lit(polygon_wkt),
            f.lit(3)  # resolution (1km)
        ).alias("cells")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["cells"] is not None
    # Should return an array of BNG cells (45 cells from Scala test)
    assert isinstance(result[0]["cells"], (list, tuple))
    assert len(result[0]["cells"]) == 45


def test_bng_tessellate(spark):
    """Test tessellation of geometry into BNG cells."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use valid BNG coordinates from Scala test
    polygon_wkt = "POLYGON((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
    
    df = spark.range(1).select(
        bng.bng_tessellate(
            f.lit(polygon_wkt),
            f.lit(3)  # resolution (1km)
        ).alias("tessellation")
    )
    
    result = df.collect()
    assert result is not None
    assert result[0]["tessellation"] is not None
    # Tessellation returns an array of structs (cellid, core, chip)
    assert isinstance(result[0]["tessellation"], (list, tuple))
    assert len(result[0]["tessellation"]) > 0


def test_bng_kloop(spark):
    """Test k-loop around a BNG cell."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_kloop(
            f.lit("TQ388791"),
            f.lit(1)
        ).alias("kloop")
    )
    result = df.collect()
    assert result is not None
    assert result[0]["kloop"] is not None


def test_bng_kring(spark):
    """Test k-ring around a BNG cell."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    df = spark.range(1).select(
        bng.bng_kring(
            f.lit("TQ388791"),
            f.lit(1)
        ).alias("kring")
    )
    result = df.collect()
    assert result is not None
    assert result[0]["kring"] is not None


def test_bng_geometrykloop(spark):
    """Test geometry k-loop."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use valid BNG coordinates from Scala test
    polygon_wkt = "POLYGON((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
    
    df = spark.range(1).select(
        bng.bng_geometrykloop(
            f.lit(polygon_wkt),
            f.lit(3),  # resolution (1km)
            f.lit(2)   # k distance
        ).alias("geom_kloop")
    )
    result = df.collect()
    assert result is not None
    assert result[0]["geom_kloop"] is not None


def test_bng_geometrykring(spark):
    """Test geometry k-ring."""
    from databricks.labs.gbx.gridx.bng import functions as bng

    # Use valid BNG coordinates from Scala test
    polygon_wkt = "POLYGON((10000 10000, 20000 10000, 20000 20000, 10000 10000))"

    df = spark.range(1).select(
        bng.bng_geometrykring(
            f.lit(polygon_wkt),
            f.lit(3),  # resolution (1km)
            f.lit(2)   # k distance
        ).alias("geom_kring")
    )
    result = df.collect()
    assert result is not None
    assert result[0]["geom_kring"] is not None
    assert isinstance(result[0]["geom_kring"], (list, tuple))
    assert len(result[0]["geom_kring"]) > 0


def test_bng_aggregations(spark):
    """Test BNG aggregation functions."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    from shapely import wkt
    import shapely

    polygon_wkb = shapely.to_wkb(wkt.loads("POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))"))
    result_df = (
        spark
            .createDataFrame([("TQ388791", 0.01)], ["cellId", "value"])
            .withColumn("gen", f.explode(f.array(f.lit(1), f.lit(2), f.lit(3))))
            .withColumn("dummy", f.lit(1))
            .withColumn("cell", f.struct("cellId", f.lit(True), f.lit(polygon_wkb)))
        .repartition(10)
        .groupBy("dummy")
        .agg(
            bng.bng_cellunion_agg("cell").alias("union_result")
        )
    )

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["union_result"] is not None


def test_bng_explode_functions(spark):
    """Test BNG explode functions."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Use valid BNG coordinates
    polygon_wkt = "POLYGON((10000 10000, 20000 10000, 20000 20000, 10000 10000))"
    
    # Test tessellate explode
    df = spark.range(1).select(
        bng.bng_tessellateexplode(
            f.lit(polygon_wkt),
            f.lit(3)  # resolution (1km)
        ).alias("cell")
    )
    
    result = df.collect()
    assert result is not None
    assert len(result) > 0  # Should have multiple rows after explode
    assert result[0]["cell"] is not None


def test_bng_workflow(spark):
    """Test a complete BNG workflow."""
    from databricks.labs.gbx.gridx.bng import functions as bng
    
    # Start with valid coordinates, convert to BNG, get properties
    df = spark.range(1).select(
        bng.bng_eastnorthasbng(
            f.lit(19922),
            f.lit(22219),
            f.lit(3)  # resolution (1km)
        ).alias("bng_cell")
    )
    
    # Get cell properties
    df = df.select(
        f.col("bng_cell"),
        bng.bng_aswkt(f.col("bng_cell")).alias("wkt"),
        bng.bng_cellarea(f.col("bng_cell")).alias("area"),
        bng.bng_centroid(f.col("bng_cell")).alias("centroid")
    )
    
    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["bng_cell"] == "SV1922"  # Expected result
    assert result[0]["wkt"] is not None
    assert result[0]["area"] > 0
    assert result[0]["centroid"] is not None

