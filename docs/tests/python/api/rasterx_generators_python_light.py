"""
Generator functions examples for the light-tier (pyrx) RasterX functions.

All examples use the single-band fixture (nyc_sentinel2_red_small.tif, 236x161, EPSG:32618)
except rst_separatebands, which uses the multiband fixture (rgb_nir_small.tif, 3 bands).

Generator functions are invoked as UDTFs (User Defined Table Functions) via SQL
LATERAL syntax; light-tier examples raise NotImplementedError when called directly
in df.select (by design — the light tier wraps heavy UDTFs).

Loaded via single_band_tile_df(spark) / multiband_tile_df(spark), which use
rst_fromcontent (no JAR required).
"""

from _fixtures import single_band_tile_df, multiband_tile_df

# ============================================================================
# Generator Functions (UDTFs via LATERAL)
# ============================================================================


def rst_retile_python_light_example(spark):
    """Retile a raster into uniform dimensions (UDTF via LATERAL).

    Generator function: invoke via SQL LATERAL, not df.select.
    Returns one row per sub-tile (each sub-tile is a v2-Tile struct).
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    # Explode raster into 64x64-pixel tiles (returns array of tiles)
    return spark.sql(
        "SELECT t.* FROM rasters, " "LATERAL gbx_rst_retile(tile, 64, 64) t"
    ).take(3)


rst_retile_python_light_example_output = """
+------+--------------+-----+----------------------+
|cellid|raster        |path |...                   |
+------+--------------+-----+----------------------+
|0     |<raster bytes>|...  |{driver -> GTiff, ...}|
+------+--------------+-----+----------------------+
(one row per sub-tile; t.* expands the v2-Tile struct fields)
"""


def rst_tooverlappingtiles_python_light_example(spark):
    """Create overlapping tiles for edge-aware processing (UDTF via LATERAL).

    Overlap percentage is applied to tile dimensions for seamless processing.
    Returns one row per overlapping tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    # 64x64 tiles with 8% overlap
    return spark.sql(
        "SELECT t.* FROM rasters, "
        "LATERAL gbx_rst_tooverlappingtiles(tile, 64, 64, 8) t"
    ).take(3)


rst_tooverlappingtiles_python_light_example_output = """
+------+--------------+-----+----------------------+
|cellid|raster        |path |...                   |
+------+--------------+-----+----------------------+
|0     |<raster bytes>|...  |{driver -> GTiff, ...}|
+------+--------------+-----+----------------------+
(one row per overlapping tile; t.* expands the v2-Tile struct fields)
"""


def rst_separatebands_python_light_example(spark):
    """Separate multi-band raster into individual bands (UDTF via LATERAL).

    Each band is returned as a separate v2-Tile struct.
    Uses the multiband fixture (3 bands: red, NIR, green).
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    multiband_tile_df(spark).createOrReplaceTempView("multiband_rasters")
    # Explode multiband raster into individual band tiles
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, " "LATERAL gbx_rst_separatebands(tile) t"
    ).collect()


rst_separatebands_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(one row per band: 3 rows for a 3-band raster)
"""


def rst_polygonize_python_light_example(spark):
    """Extract polygons from raster regions (UDTF via LATERAL).

    One row per contiguous-value region: (geom_wkb, value). band and
    connectedness (4 or 8) are required arguments in the light tier.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    # Polygonize band 1 with 4-connectivity.
    return spark.sql(
        "SELECT t.* FROM rasters, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
    ).take(3)


rst_polygonize_python_light_example_output = """
+---------+-----+
|geom_wkb |value|
+---------+-----+
|...      |365.0|
+---------+-----+
(one row per contiguous region: geom_wkb is WKB binary, value is the region value)
"""


def rst_maketiles_python_light_example(spark):
    """Subdivide raster into tiles by approximate size (UDTF via LATERAL).

    Size is in MB; the tile grid is derived from the MB budget.
    Returns one row per sub-tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    # Subdivide raster into approximately 1.0 MB tiles
    return spark.sql(
        "SELECT t.* FROM rasters, " "LATERAL gbx_rst_maketiles(tile, 1.0) t"
    ).collect()


rst_maketiles_python_light_example_output = """
+------+--------------+-----+----------------------+
|cellid|raster        |path |...                   |
+------+--------------+-----+----------------------+
|0     |<raster bytes>|...  |{driver -> GTiff, ...}|
+------+--------------+-----+----------------------+
(one row per sub-tile; t.* expands the v2-Tile struct fields)
"""


def rst_rasterize_python_light_example(spark):
    """Burn geometry into a raster tile (column-returning, not UDTF).

    This is NOT a generator — it returns a single tile struct from geometry.
    Takes a WKB/WKT geometry and a burn value; produces a rasterized tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import Row, functions as f

    rx.register(spark)
    # Create a small synthetic geometry DataFrame with WKT polygon
    df = spark.createDataFrame([Row(geom="POLYGON((2 2, 8 2, 8 8, 2 8, 2 2))")])
    # Rasterize a square polygon (value=1.0, extent=(0,0,10,10), 10x10 pixels, EPSG:4326)
    result = df.select(
        rx.rst_rasterize(
            f.col("geom"),
            f.lit(1.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(10.0),
            f.lit(10.0),
            f.lit(10),
            f.lit(10),
            f.lit(4326),
        ).alias("tile")
    ).collect()
    return result[0]["tile"]


rst_rasterize_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(rasterized tile: pixels inside the polygon carry the burn value; outside = NoData)
"""
