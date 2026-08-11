"""
Coordinate transforms & tiling examples for the light-tier (pyrx) RasterX functions.

All 12 examples use the single-band fixture (nyc_sentinel2_red.tif).
- Coordinate transforms use pixel/world coordinate pairs within the raster's bounds.
- Web-Mercator and XYZ tiling functions generate display tiles (PNG bytes).
- Tessellation functions partition the raster into grid cells.

Loaded via single_band_tile_df(spark) which uses rst_fromcontent (no JAR required).

The light tier returns materialized v2 tiles (raster bytes populated, path null).
"""

from _fixtures import single_band_tile_df

# ============================================================================
# Coordinate Transforms
# ============================================================================


def rst_rastertoworldcoord_python_light_example(spark):
    """Convert pixel coordinates (col, row) to world coordinates (x, y)."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # Pixel (100, 80) → world struct {x: <easting>, y: <northing>}
    result = df.select(
        rx.rst_rastertoworldcoord("tile", f.lit(100), f.lit(80)).alias("world_coord")
    ).first()
    return result["world_coord"]


rst_rastertoworldcoord_python_light_example_output = """
+-----------------------------+
|world_coord                  |
+-----------------------------+
|{2122955.0, -10791275.0}     |
+-----------------------------+
(struct with x: DOUBLE, y: DOUBLE)
"""


def rst_rastertoworldcoordx_python_light_example(spark):
    """Convert pixel column to world X coordinate (easting)."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # Pixel col=100 → easting
    result = df.select(
        rx.rst_rastertoworldcoordx("tile", f.lit(100), f.lit(80)).alias("easting")
    ).first()
    return result["easting"]


rst_rastertoworldcoordx_python_light_example_output = """
+---------+
|easting  |
+---------+
|2122955.0|
+---------+
"""


def rst_rastertoworldcoordy_python_light_example(spark):
    """Convert pixel row to world Y coordinate (northing)."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # Pixel row=80 → northing
    result = df.select(
        rx.rst_rastertoworldcoordy("tile", f.lit(100), f.lit(80)).alias("northing")
    ).first()
    return result["northing"]


rst_rastertoworldcoordy_python_light_example_output = """
+------------+
|northing    |
+------------+
|-10791275.0 |
+------------+
"""


def rst_worldtorastercoord_python_light_example(spark):
    """Convert world coordinates (x, y) to pixel coordinates (col, row)."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # World (2122955, -10791275) in the raster CRS → pixel (100, 80) — the exact
    # inverse of the rst_rastertoworldcoord example above.
    result = df.select(
        rx.rst_worldtorastercoord("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_coord"
        )
    ).first()
    return result["pixel_coord"]


rst_worldtorastercoord_python_light_example_output = """
+-----------+
|pixel_coord|
+-----------+
|{100, 80}  |
+-----------+
(struct with x: INT, y: INT)
"""


def rst_worldtorastercoordx_python_light_example(spark):
    """Convert world X coordinate to pixel column."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # World (2122955, -10791275) → pixel column 100
    result = df.select(
        rx.rst_worldtorastercoordx("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_col"
        )
    ).first()
    return result["pixel_col"]


rst_worldtorastercoordx_python_light_example_output = """
+---------+
|pixel_col|
+---------+
|100      |
+---------+
"""


def rst_worldtorastercoordy_python_light_example(spark):
    """Convert world Y coordinate to pixel row."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # World (2122955, -10791275) → pixel row 80
    result = df.select(
        rx.rst_worldtorastercoordy("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_row"
        )
    ).first()
    return result["pixel_row"]


rst_worldtorastercoordy_python_light_example_output = """
+---------+
|pixel_row|
+---------+
|80       |
+---------+
"""


# ============================================================================
# Web-Mercator Tile Output
# ============================================================================


def rst_to_webmercator_python_light_example(spark):
    """Transform raster from its native CRS to Web Mercator (EPSG:3857)."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    df = single_band_tile_df(spark)
    # Reproject to Web Mercator (default bilinear resampling)
    result = df.select(rx.rst_to_webmercator("tile").alias("tile")).first()
    return result["tile"]


rst_to_webmercator_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_tilexyz_python_light_example(spark):
    """Render a single Web-Mercator XYZ tile (z, x, y) as PNG bytes."""
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = single_band_tile_df(spark)
    # Render z=12, x=1234, y=1523 as a 256x256 PNG. rescale="none" keeps the raw
    # dtype mapping (a slippy-map tile off the raster's footprint renders
    # transparent — rst_tilexyz never returns null).
    result = df.select(
        rx.rst_tilexyz(
            "tile",
            f.lit(12),
            f.lit(1234),
            f.lit(1523),
            f.lit("PNG"),
            f.lit(256),
            f.lit("bilinear"),
            f.lit("none"),
        ).alias("png_bytes")
    ).first()
    return result["png_bytes"]


rst_tilexyz_python_light_example_output = """
+----------+
|png_bytes |
+----------+
|[BINARY]  |
+----------+
(PNG image bytes, 256×256 pixels)
"""


def rst_xyzpyramid_python_light_example(spark):
    """Generate XYZ tiles across a zoom range (UDTF: one row per tile).

    rst_xyzpyramid is a Python UDTF — invoke it as a SQL LATERAL table function
    (a plain df.select would raise NotImplementedError).
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    # One row per (z, x, y) tile across zoom 0..1.
    return spark.sql(
        "SELECT t.* FROM rasters, "
        "LATERAL gbx_rst_xyzpyramid(tile, 0, 1, 'PNG', 256, 'bilinear', 'none') t"
    ).take(3)


rst_xyzpyramid_python_light_example_output = """
+---+---+---+--------+
|z  |x  |y  |bytes   |
+---+---+---+--------+
|0  |0  |0  |[BINARY]|
+---+---+---+--------+
(one row per XYZ tile: z, x, y, and the PNG image bytes)
"""


# ============================================================================
# Grid Tessellation
# ============================================================================


def rst_h3_tessellate_python_light_example(spark):
    """Tessellate a raster into H3 hexagonal grid cells (resolution 3).

    rst_h3_tessellate is a Python UDTF — invoke it as a SQL LATERAL table
    function (a plain df.select would raise NotImplementedError). One v2-Tile row
    per H3 cell overlapping the raster, each carrying the H3 cellid.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    return spark.sql(
        "SELECT t.* FROM rasters, LATERAL gbx_rst_h3_tessellate(tile, 3) t"
    ).take(3)


rst_h3_tessellate_python_light_example_output = """
+-------------------+-----------------------------------------------------------+
|cellid             |...                                                        |
+-------------------+-----------------------------------------------------------+
|577586652210266111 |{..., <raster bytes>, ..., {driver -> GTiff, ...}}         |
+-------------------+-----------------------------------------------------------+
(one v2-Tile row per H3 cell, cellid = the H3 index)
"""


def rst_bng_tessellate_python_light_example(spark):
    """Tessellate a raster into British National Grid cells (resolution 3).

    rst_bng_tessellate is a Python UDTF — invoke it as a SQL LATERAL table
    function. The raster is first warped to EPSG:27700 (British National Grid),
    so a raster whose extent falls outside Great Britain yields no cells (an
    empty result, not an error) — as with this NYC-area sample.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    return spark.sql(
        "SELECT t.* FROM rasters, LATERAL gbx_rst_bng_tessellate(tile, 3) t"
    ).take(3)


rst_bng_tessellate_python_light_example_output = """
+------+-----+
|cellid|...  |
+------+-----+
+------+-----+
(one v2-Tile row per BNG cell overlapping the raster after warping to
EPSG:27700; empty here because the NYC-area sample is outside Great Britain)
"""


def rst_quadbin_tessellate_python_light_example(spark):
    """Tessellate a raster into CARTO quadbin grid cells (resolution 5).

    rst_quadbin_tessellate is a Python UDTF — invoke it as a SQL LATERAL table
    function. One v2-Tile row per quadbin cell overlapping the raster.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    single_band_tile_df(spark).createOrReplaceTempView("rasters")
    return spark.sql(
        "SELECT t.* FROM rasters, LATERAL gbx_rst_quadbin_tessellate(tile, 5) t"
    ).take(3)


rst_quadbin_tessellate_python_light_example_output = """
+-------------------+-----------------------------------------------------------+
|cellid             |...                                                        |
+-------------------+-----------------------------------------------------------+
|5250127588525215743|{..., <raster bytes>, ..., {driver -> GTiff, ...}}         |
+-------------------+-----------------------------------------------------------+
(one v2-Tile row per quadbin cell, cellid = the quadbin index)
"""
