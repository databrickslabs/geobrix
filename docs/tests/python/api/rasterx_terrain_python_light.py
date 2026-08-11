"""
Terrain analysis examples for the light-tier (pyrx) RasterX functions.

All 13 examples use the DEM fixture (SRTM elevation, NYC area, single-band).
Loaded via dem_tile_df(spark) which uses rst_fromcontent (no JAR required).

The light tier returns materialized v2 tiles (raster bytes populated, path null).
"""

from _fixtures import dem_tile_df

# ============================================================================
# Terrain Analysis Functions
# ============================================================================


def rst_slope_python_light_example(spark):
    """Compute slope (degrees or percent rise/run) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_slope("tile", f.lit("degrees"), f.lit(1.0), f.lit(1.0)).alias("tile")
    ).first()
    return result["tile"]


rst_slope_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_aspect_python_light_example(spark):
    """Compute aspect (compass direction of steepest slope) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_aspect("tile", f.lit(False), f.lit(False)).alias("tile")
    ).first()
    return result["tile"]


rst_aspect_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_hillshade_python_light_example(spark):
    """Compute hillshade (8-bit shaded relief) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_hillshade("tile", f.lit(315.0), f.lit(45.0), f.lit(1.0)).alias("tile")
    ).first()
    return result["tile"]


rst_hillshade_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_tri_python_light_example(spark):
    """Compute Terrain Ruggedness Index (TRI) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(rx.rst_tri("tile").alias("tile")).first()
    return result["tile"]


rst_tri_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_tpi_python_light_example(spark):
    """Compute Topographic Position Index (TPI) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(rx.rst_tpi("tile").alias("tile")).first()
    return result["tile"]


rst_tpi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_roughness_python_light_example(spark):
    """Compute Roughness (max neighbour delta in 3x3 window) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(rx.rst_roughness("tile").alias("tile")).first()
    return result["tile"]


rst_roughness_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_color_relief_python_light_example(spark):
    """Apply a color relief mapping (elevation → RGBA) from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from _fixtures import color_table_path

    rx.register(spark)
    df = dem_tile_df(spark)
    clr_path = str(color_table_path())
    result = df.select(
        rx.rst_color_relief("tile", f.lit(clr_path)).alias("tile")
    ).first()
    return result["tile"]


rst_color_relief_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_proximity_python_light_example(spark):
    """Compute per-pixel distance to the nearest non-NoData pixel."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_proximity("tile", f.lit(""), f.lit("PIXEL"), f.lit(100.0)).alias("tile")
    ).first()
    return result["tile"]


rst_proximity_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_contour_python_light_example(spark):
    """Generate contour LineStrings at equal intervals from a DEM tile."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_contour("tile", f.lit([]), f.lit(50.0), f.lit(0.0), f.lit("elev")).alias(
            "contours"
        )
    ).first()
    return result["contours"]


rst_contour_python_light_example_output = """
+--------------------------------------+
|contours                              |
+--------------------------------------+
|[{[BINARY], 50.0}, {[BINARY], 100.0}, {[BINARY], 150.0}, {[BINARY], 200.0}, {[BINARY], 250.0}, {[BINARY], 300.0}]|
+--------------------------------------+
"""


def rst_viewshed_python_light_example(spark):
    """Compute binary viewshed mask from a DEM and observer point."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_viewshed(
            "tile",
            f.lit("POINT(500320 4500320)"),
            f.lit(100.0),
            f.lit(1.6),
            f.lit(500.0),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_viewshed_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_sample_python_light_example(spark):
    """Sample raster pixel values at a POINT geometry."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = dem_tile_df(spark)
    result = df.select(
        rx.rst_sample("tile", f.lit("SRID=32618;POINT(500320 4500320)")).alias("values")
    ).first()
    return result["values"]


rst_sample_python_light_example_output = """
+-------+
|values |
+-------+
|[302.0]|
+-------+
"""


def rst_gridfrompoints_python_light_example(spark):
    """IDW (Inverse Distance Weighting) interpolation from arrays of points and values."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from pyspark.sql.types import ArrayType, BinaryType, DoubleType

    rx.register(spark)

    # Create a synthetic point cloud DataFrame with WKB points and values
    # Using simple WKB POINT(2.0, 2.0) and POINT(4.0, 4.0)
    point_data = [
        (
            [
                bytes.fromhex("010100000000000000000000400000000000000040"),
                bytes.fromhex("010100000000000000000008400000000000000840"),
            ],
            [100.0, 110.0],
        )
    ]
    df = spark.createDataFrame(
        point_data,
        [
            "points_wkb_array",
            "values_array",
        ],
    )

    result = df.select(
        rx.rst_gridfrompoints(
            f.col("points_wkb_array"),
            f.col("values_array"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(1000.0),
            f.lit(1000.0),
            f.lit(256),
            f.lit(256),
            f.lit(32633),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_gridfrompoints_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_dtmfromgeoms_python_light_example(spark):
    """TIN (Triangulated Irregular Network) from Z-valued points via Delaunay interpolation."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from pyspark.sql.types import StructType, StructField, ArrayType, BinaryType

    rx.register(spark)

    # Create a synthetic survey point DataFrame with 4 points inset from the grid
    # edges (Delaunay needs the hull to cover interior cells).
    # POINT Z geometries: (100,100,50), (900,100,80), (900,900,120), (100,900,60)
    # Using explicit schema to avoid type inference errors
    survey_data = [
        (
            [
                bytes.fromhex(
                    "0101000080000000000000594000000000000059400000000000004940"
                ),
                bytes.fromhex(
                    "01010000800000000000208c4000000000000059400000000000005440"
                ),
                bytes.fromhex(
                    "01010000800000000000208c400000000000208c400000000000005e40"
                ),
                bytes.fromhex(
                    "010100008000000000000059400000000000208c400000000000004e40"
                ),
            ],
            [],
        )
    ]
    schema = StructType(
        [
            StructField("points_wkb_array", ArrayType(BinaryType()), True),
            StructField("breaklines_wkb_array", ArrayType(BinaryType()), True),
        ]
    )
    df = spark.createDataFrame(survey_data, schema=schema)

    result = df.select(
        rx.rst_dtmfromgeoms(
            f.col("points_wkb_array"),
            f.col("breaklines_wkb_array"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(1000.0),
            f.lit(1000.0),
            f.lit(100),
            f.lit(100),
            f.lit(32618),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_dtmfromgeoms_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""
