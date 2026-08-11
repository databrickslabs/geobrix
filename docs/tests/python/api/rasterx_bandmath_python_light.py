"""
Band-math / spectral index examples for the light-tier (pyrx) RasterX functions.

All 10 examples use the multiband fixture (3 bands: red=1, NIR=2, green=3; 8x8, UInt16).
Loaded via rst_fromcontent (no JAR required).

The light tier returns materialized v2 tiles (raster bytes populated, path null).
"""

from _fixtures import multiband_tile_df

# ============================================================================
# Spectral Indices
# ============================================================================


def rst_ndvi_python_light_example(spark):
    """Compute Normalized Difference Vegetation Index (NDVI).

    NDVI = (NIR - Red) / (NIR + Red)
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    result = df.select(rx.rst_ndvi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndvi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_evi_python_light_example(spark):
    """Compute Enhanced Vegetation Index (EVI).

    EVI = 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    # EVI requires red (band 1), NIR (band 2), blue (band 3 as proxy)
    result = df.select(
        rx.rst_evi("tile", f.lit(1), f.lit(2), f.lit(3)).alias("tile")
    ).first()
    return result["tile"]


rst_evi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_savi_python_light_example(spark):
    """Compute Soil-Adjusted Vegetation Index (SAVI).

    SAVI = ((NIR - Red) / (NIR + Red + L)) * (1 + L)
    Default L = 0.5. Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    result = df.select(rx.rst_savi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_savi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_ndwi_python_light_example(spark):
    """Compute Normalized Difference Water Index (NDWI).

    NDWI = (Green - NIR) / (Green + NIR)
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    # Use green (band 3) and NIR (band 2)
    result = df.select(rx.rst_ndwi("tile", f.lit(3), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndwi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_nbr_python_light_example(spark):
    """Compute Normalized Burn Ratio (NBR).

    NBR = (NIR - SWIR) / (NIR + SWIR)
    Uses band 2 (NIR) and band 3 (proxied as SWIR for fixture).
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    # Band 2 (NIR), band 3 (SWIR proxy)
    result = df.select(rx.rst_nbr("tile", f.lit(2), f.lit(3)).alias("tile")).first()
    return result["tile"]


rst_nbr_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_index_python_light_example(spark):
    """Compute a named spectral index via formula and band map.

    ``band_map`` is a Spark MAP<STRING, INT> wiring the formula's named bands to
    1-based band indices (built with ``f.create_map``). Output: single-band
    Float32 tile. Uses the multiband fixture (non-default; band 1=red, 2=NIR).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)
    # Compute NDVI via the generic index dispatcher with a red/NIR band map.
    band_map = f.create_map(f.lit("red"), f.lit(1), f.lit("nir"), f.lit(2))
    result = df.select(
        rx.rst_index("tile", f.lit("ndvi"), band_map).alias("tile")
    ).first()
    return result["tile"]


rst_index_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


# ============================================================================
# Multi-tile Operations (require aggregation)
# ============================================================================


def rst_combineavg_python_light_example(spark):
    """Compute per-pixel mean across aligned tiles (NoData-aware).

    Input: array of aligned tiles. Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from _fixtures import multi_band_tiles_df

    rx.register(spark)
    df = multi_band_tiles_df(spark)
    # Aggregate the 3 per-band tiles into one array, then average per-pixel.
    result = (
        df.groupBy("region")
        .agg(rx.rst_combineavg(f.collect_list("tile")).alias("tile"))
        .first()
    )
    return result["tile"]


rst_combineavg_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_derivedband_python_light_example(spark):
    """Apply a user-provided Python pixel-function across the tile's bands.

    ``python_func`` is a source string following GDAL's VRT pixel-function
    signature; ``func_name`` names the callable within it. Output: single-band
    Float64 tile. Uses the multiband fixture (non-default; 3 bands).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)

    # GDAL VRT pixel-function that doubles band 1's pixel values in place.
    python_func = (
        "def double(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0] * 2\n"
    )
    result = df.select(
        rx.rst_derivedband("tile", f.lit(python_func), f.lit("double")).alias("tile")
    ).first()
    return result["tile"]


rst_derivedband_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_mapalgebra_python_light_example(spark):
    """Apply a map-algebra expression across an array of tiles.

    Band 1 of each tile (in array order) binds to A, B, C, …; the expression is
    evaluated with numexpr (safe math only — no arbitrary code). Here a single
    tile is passed, so A = band 1 (red), and ``"A * 2"`` doubles it. Output:
    single-band Float32 tile. Uses the multiband fixture (non-default; 3 bands).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = multiband_tile_df(spark)

    result = df.select(
        rx.rst_mapalgebra(f.array("tile"), f.lit("A * 2")).alias("tile")
    ).first()
    return result["tile"]


rst_mapalgebra_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_merge_python_light_example(spark):
    """Mosaic (merge) multiple aligned tiles into one spanning their union.

    Input: array of aligned tiles (same grid / CRS).
    Output: single merged tile covering the union extent.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from _fixtures import multi_band_tiles_df

    rx.register(spark)
    df = multi_band_tiles_df(spark)

    result = (
        df.groupBy("region")
        .agg(rx.rst_merge(f.collect_list("tile")).alias("tile"))
        .first()
    )
    return result["tile"]


rst_merge_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""
