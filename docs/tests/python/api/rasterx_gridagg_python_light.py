"""
Python code examples for RasterX rastertogrid functions (light tier).
Single source of truth for docs/docs/api/rasterx-functions.mdx

Light tier uses UDTF-based LATERAL syntax for rastertogrid functions.
Each function returns [band (INT), cellID, measure] columns directly.
"""

from pathlib import Path


def _get_multiband_df(spark):
    """Helper: load multiband fixture for examples."""
    try:
        from . import _fixtures  # noqa: PLC0415
    except (ModuleNotFoundError, ImportError):
        import _fixtures  # noqa: PLC0415

    return _fixtures.multiband_tile_df(spark)


# ============================================================================
# H3 Rastertogrid Functions — Light Tier (UDTF via LATERAL)
# ============================================================================


def rst_h3_rastertogridavg_python_light_example(spark):
    """Aggregate raster values to H3 grid using average (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    # LATERAL UDTF returns [band, cellID, measure] columns directly
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridavg(tile, 4) t"
    ).take(5)


rst_h3_rastertogridavg_python_light_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|123.45 |
|1   |599686042433355776|124.20 |
+----+------------------+-------+
(one row per band × H3 cell)
"""


def rst_h3_rastertogridcount_python_light_example(spark):
    """Count pixels per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridcount(tile, 4) t"
    ).take(5)


rst_h3_rastertogridcount_python_light_example_output = """
+----+------------------------------+-------+
|band|cellID                        |measure|
+----+------------------------------+-------+
|1   |599686042433355775            |256    |
|1   |599686042433355776            |240    |
+----+------------------------------+-------+
(pixel count per band × H3 cell)
"""


def rst_h3_rastertogridmax_python_light_example(spark):
    """Get maximum values per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmax(tile, 4) t"
    ).take(5)


rst_h3_rastertogridmax_python_light_example_output = """
+----+------------------------------+-------+
|band|cellID                        |measure|
+----+------------------------------+-------+
|1   |599686042433355775            |255.0  |
|1   |599686042433355776            |254.0  |
+----+------------------------------+-------+
(max value per band × H3 cell)
"""


def rst_h3_rastertogridmin_python_light_example(spark):
    """Get minimum values per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmin(tile, 4) t"
    ).take(5)


rst_h3_rastertogridmin_python_light_example_output = """
+----+------------------------------+-------+
|band|cellID                        |measure|
+----+------------------------------+-------+
|1   |599686042433355775            |0.0    |
|1   |599686042433355776            |10.0   |
+----+------------------------------+-------+
(min value per band × H3 cell)
"""


def rst_h3_rastertogridmedian_python_light_example(spark):
    """Get median values per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmedian(tile, 4) t"
    ).take(5)


rst_h3_rastertogridmedian_python_light_example_output = """
+----+------------------------------+-------+
|band|cellID                        |measure|
+----+------------------------------+-------+
|1   |599686042433355775            |120.5  |
|1   |599686042433355776            |122.0  |
+----+------------------------------+-------+
(median value per band × H3 cell)
"""


def rst_h3_rastertogridsum_python_light_example(spark):
    """Sum pixel values per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridsum(tile, 4) t"
    ).take(5)


rst_h3_rastertogridsum_python_light_example_output = """
+----+------------------------------+--------+
|band|cellID                        |measure |
+----+------------------------------+--------+
|1   |599686042433355775            |31563.0 |
|1   |599686042433355776            |29488.0 |
+----+------------------------------+--------+
(sum of pixel values per band × H3 cell)
"""


def rst_h3_rastertogridvariance_python_light_example(spark):
    """Get variance per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridvariance(tile, 4) t"
    ).take(5)


rst_h3_rastertogridvariance_python_light_example_output = """
+----+------------------------------+-------+
|band|cellID                        |measure|
+----+------------------------------+-------+
|1   |599686042433355775            |1245.5 |
|1   |599686042433355776            |1389.2 |
+----+------------------------------+-------+
(variance per band × H3 cell)
"""


def rst_h3_rastertogridstddev_python_light_example(spark):
    """Get standard deviation per H3 cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridstddev(tile, 4) t"
    ).take(5)


rst_h3_rastertogridstddev_python_light_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|35.29  |
|1   |599686042433355776|37.27  |
+----+------------------+-------+
(standard deviation per band × H3 cell)
"""


# ============================================================================
# Quadbin Rastertogrid Functions — Light Tier (UDTF via LATERAL)
# ============================================================================


def rst_quadbin_rastertogridavg_python_light_example(spark):
    """Aggregate raster values to Quadbin grid using average (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridavg(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridavg_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |123.45 |
|1   |12346 |124.20 |
+----+------+-------+
(one row per band × Quadbin cell)
"""


def rst_quadbin_rastertogridcount_python_light_example(spark):
    """Count pixels per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridcount(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridcount_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |256    |
|1   |12346 |240    |
+----+------+-------+
(pixel count per band × Quadbin cell)
"""


def rst_quadbin_rastertogridmax_python_light_example(spark):
    """Get maximum values per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmax(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridmax_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |255.0  |
|1   |12346 |254.0  |
+----+------+-------+
(max value per band × Quadbin cell)
"""


def rst_quadbin_rastertogridmin_python_light_example(spark):
    """Get minimum values per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmin(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridmin_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |0.0    |
|1   |12346 |10.0   |
+----+------+-------+
(min value per band × Quadbin cell)
"""


def rst_quadbin_rastertogridmedian_python_light_example(spark):
    """Get median values per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmedian(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridmedian_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |120.5  |
|1   |12346 |122.0  |
+----+------+-------+
(median value per band × Quadbin cell)
"""


def rst_quadbin_rastertogridsum_python_light_example(spark):
    """Sum pixel values per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridsum(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridsum_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |31563.0|
|1   |12346 |29488.0|
+----+------+-------+
(sum of pixel values per band × Quadbin cell)
"""


def rst_quadbin_rastertogridvariance_python_light_example(spark):
    """Get variance per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridvariance(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridvariance_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |1245.5 |
|1   |12346 |1389.2 |
+----+------+-------+
(variance per band × Quadbin cell)
"""


def rst_quadbin_rastertogridstddev_python_light_example(spark):
    """Get standard deviation per Quadbin cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridstddev(tile, 4) t"
    ).take(5)


rst_quadbin_rastertogridstddev_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |35.29  |
|1   |12346 |37.27  |
+----+------+-------+
(standard deviation per band × Quadbin cell)
"""


# ============================================================================
# BNG Rastertogrid Functions — Light Tier (UDTF via LATERAL)
# NOTE: BNG reprojects the raster to EPSG:27700 (British National Grid) before
# binning. For real analysis, use a raster whose extent lies over Britain so the
# BNG cell ids are meaningful; over an arbitrary EPSG:4326 fixture the reprojection
# still yields cells, but their grid-square labels are not geographically sensible.
# ============================================================================


def rst_bng_rastertogridavg_python_light_example(spark):
    """Aggregate raster values to BNG grid using average (light tier UDTF).

    NOTE: BNG reprojects the raster to EPSG:27700 before binning. Use a
    raster whose extent lies over Britain for geographically meaningful cell ids.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridavg(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridavg_python_light_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|77.22222222222223 |
|1   |OW5575|80.66666666666667 |
|2   |OW5574|144.33333333333334|
+----+------+------------------+
(one row per band × BNG cell; cellID is a STRING grid-square label)
"""


def rst_bng_rastertogridcount_python_light_example(spark):
    """Count pixels per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridcount(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridcount_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|9      |
|1   |OW5575|21     |
|2   |OW5574|9      |
+----+------+-------+
(pixel count per band × BNG cell)
"""


def rst_bng_rastertogridmax_python_light_example(spark):
    """Get maximum values per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmax(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridmax_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|106.0  |
|1   |OW5575|118.0  |
|1   |OW5674|107.0  |
+----+------+-------+
(max value per band × BNG cell)
"""


def rst_bng_rastertogridmin_python_light_example(spark):
    """Get minimum values per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmin(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridmin_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|0.0    |
|1   |OW5575|54.0   |
|1   |OW5674|65.0   |
+----+------+-------+
(min value per band × BNG cell)
"""


def rst_bng_rastertogridmedian_python_light_example(spark):
    """Get median values per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmedian(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridmedian_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|88.0   |
|1   |OW5575|80.0   |
|1   |OW5674|81.0   |
+----+------+-------+
(median value per band × BNG cell)
"""


def rst_bng_rastertogridsum_python_light_example(spark):
    """Sum pixel values per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridsum(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridsum_python_light_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|695.0  |
|1   |OW5575|1694.0 |
|1   |OW5674|774.0  |
+----+------+-------+
(sum of pixel values per band × BNG cell)
"""


def rst_bng_rastertogridvariance_python_light_example(spark):
    """Get variance per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridvariance(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridvariance_python_light_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|963.7283950617285 |
|1   |OW5575|464.126984126984  |
|1   |OW5674|196.66666666666666|
+----+------+------------------+
(population variance per band × BNG cell)
"""


def rst_bng_rastertogridstddev_python_light_example(spark):
    """Get standard deviation per BNG cell (light tier UDTF)."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    df.createOrReplaceTempView("multiband_rasters")
    result = spark.sql(
        "SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridstddev(tile, 3) t"
    ).take(5)
    return result


rst_bng_rastertogridstddev_python_light_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|31.043975181373415|
|1   |OW5575|21.543606571950388|
|1   |OW5674|14.023789311975086|
+----+------+------------------+
(population standard deviation per band × BNG cell)
"""


# ============================================================================
# H3 cell utilities — scalar, no raster input
# ============================================================================


def h3_cell_bbox_python_light_example(spark):
    """Bounding box STRUCT for H3 cells in a given CRS (light tier scalar UDF).

    gbx_h3_cell_bbox is a scalar function — call it in a plain select over a
    column of H3 cell ids. Here we build a small DataFrame of NYC-area
    resolution-9 cell ids and request the EPSG:4326 centroid-mode bbox
    (kring_pad omitted → default). Returns STRUCT<xmin, ymin, xmax, ymax>.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.createDataFrame(
        [(617733151020810239,), (617733151085035519,), (617733151021334527,)],
        ["cellid"],
    )
    return df.select(
        "cellid",
        rx.gbx_h3_cell_bbox("cellid", f.lit(4326), f.lit("centroids")).alias("bbox"),
    ).take(3)


h3_cell_bbox_python_light_example_output = """
+------------------+------------------------------+
|cellid            |bbox                          |
+------------------+------------------------------+
|617733151020810239|{-74.02, 40.70, -74.01, 40.71}|
+------------------+------------------------------+
(STRUCT<xmin, ymin, xmax, ymax> per H3 cell, in EPSG:4326)
"""


# ============================================================================
# Custom-Grid Rastertogrid Functions — Light Tier (UDTF via LATERAL)
#
# Each function takes a raster tile, a custom-grid spec struct (produced by
# gbx_custom_grid), and a resolution integer. The custom grid requires INTEGER
# coordinates (all bounds and cell sizes are truncated to int internally).
#
# Fixture: a synthetic single-band raster synthesized with rst_rasterize over a
# 4km × 4km London BNG square (529000-533000 E / 179000-183000 N, EPSG:27700),
# matching the custom grid exactly so all pixel centroids fall within the grid.
# Resolution 0 = one root 4km cell; resolution 1 = four 2km cells.
#
# Registering pygx alongside pyrx makes gbx_custom_grid available in SQL.
# ============================================================================

# SQL inline form of the custom grid (matches the raster extent exactly)
_CUSTOM_GRID_SQL = (
    "gbx_custom_grid(529000, 533000, 179000, 183000, 2, 4000, 4000, 27700)"
)

# WKT polygon for the 4km London square in EPSG:27700 (BNG metres)
_LONDON_4KM_WKT = (
    "POLYGON((529000 179000, 533000 179000, "
    "533000 183000, 529000 183000, 529000 179000))"
)


def _setup_custom_raster_view(spark):
    """Register pyrx + pygx; create a synthetic BNG raster temp view for custom-grid tests."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from databricks.labs.gbx.pygx import functions as gx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    gx.register(spark)
    # Synthesize a small single-band raster over the 4km London BNG square.
    # rst_rasterize produces a raster in EPSG:27700 with integer metre coordinates
    # that exactly match the custom grid bounds below.
    df = spark.range(1).select(
        rx.rst_rasterize(
            f.lit(_LONDON_4KM_WKT),
            f.lit(1.0),
            f.lit(529000.0),
            f.lit(179000.0),
            f.lit(533000.0),
            f.lit(183000.0),
            f.lit(40),
            f.lit(40),
            f.lit(27700),
        ).alias("tile")
    )
    df.createOrReplaceTempView("custom_rasters")


def rst_custom_rastertogridavg_python_light_example(spark):
    """Aggregate raster values to custom-grid cells using average (light tier UDTF).

    Synthesizes a single-band BNG raster (EPSG:27700) over a 4km London square
    and aggregates to one 4km root cell (resolution 0). The raster CRS matches
    the custom grid's native CRS so no reprojection is needed.
    """
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridavg(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridavg_python_light_example_output = """
+----+--------------------+-------+
|band|cellID              |measure|
+----+--------------------+-------+
|1   |<custom cell bigint>|1.0    |
+----+--------------------+-------+
(one row per band × custom-grid cell; cellID is BIGINT, measure is DOUBLE mean)
"""


def rst_custom_rastertogridcount_python_light_example(spark):
    """Count pixels per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridcount(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridcount_python_light_example_output = """
+----+--------------------+-------+
|band|cellID              |measure|
+----+--------------------+-------+
|1   |<custom cell bigint>|1600.0 |
+----+--------------------+-------+
(pixel count per band × custom-grid cell; measure is DOUBLE)
"""


def rst_custom_rastertogridmax_python_light_example(spark):
    """Get maximum values per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridmax(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridmax_python_light_example_output = """
+----+--------------------+-------+
|band|cellID              |measure|
+----+--------------------+-------+
|1   |<custom cell bigint>|1.0    |
+----+--------------------+-------+
(max pixel value per band × custom-grid cell)
"""


def rst_custom_rastertogridmin_python_light_example(spark):
    """Get minimum values per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridmin(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridmin_python_light_example_output = """
+----+--------------------+-------+
|band|cellID              |measure|
+----+--------------------+-------+
|1   |<custom cell bigint>|1.0    |
+----+--------------------+-------+
(min pixel value per band × custom-grid cell)
"""


def rst_custom_rastertogridmedian_python_light_example(spark):
    """Get median values per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridmedian(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridmedian_python_light_example_output = """
+----+--------------------+--------+
|band|cellID              |measure |
+----+--------------------+--------+
|1   |<custom cell bigint>|1.0     |
+----+--------------------+--------+
(median pixel value per band × custom-grid cell)
"""


def rst_custom_rastertogridsum_python_light_example(spark):
    """Sum pixel values per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridsum(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridsum_python_light_example_output = """
+----+--------------------+--------+
|band|cellID              |measure |
+----+--------------------+--------+
|1   |<custom cell bigint>|1600.0  |
+----+--------------------+--------+
(sum of pixel values per band × custom-grid cell)
"""


def rst_custom_rastertogridvariance_python_light_example(spark):
    """Get population variance per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridvariance(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridvariance_python_light_example_output = """
+----+--------------------+----------+
|band|cellID              |measure   |
+----+--------------------+----------+
|1   |<custom cell bigint>|0.0       |
+----+--------------------+----------+
(population variance per band × custom-grid cell; 0.0 when all pixels equal)
"""


def rst_custom_rastertogridstddev_python_light_example(spark):
    """Get population standard deviation per custom-grid cell (light tier UDTF)."""
    _setup_custom_raster_view(spark)
    return spark.sql(
        f"SELECT t.* FROM custom_rasters, "
        f"LATERAL gbx_rst_custom_rastertogridstddev(tile, {_CUSTOM_GRID_SQL}, 0) t"
    ).take(5)


rst_custom_rastertogridstddev_python_light_example_output = """
+----+--------------------+--------+
|band|cellID              |measure |
+----+--------------------+--------+
|1   |<custom cell bigint>|0.0     |
+----+--------------------+--------+
(population standard deviation per band × custom-grid cell; 0.0 when all pixels equal)
"""
