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
