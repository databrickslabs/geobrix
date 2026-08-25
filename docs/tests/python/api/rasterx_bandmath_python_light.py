"""
Band-math / spectral index examples for the light-tier (pyrx) RasterX functions.

All 10 examples use the multiband fixture (3 bands: red=1, NIR=2, green=3; 8x8, UInt16).
Loaded via rst_fromcontent (no JAR required).

The light tier returns materialized v2 tiles (raster bytes populated, path null).

Single-tile examples read the `multiband_rasters` Setup view via spark.table();
the two multi-tile aggregating examples (rst_combineavg, rst_merge) build their
own multi-row input from the _fixtures helper.
"""

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

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_ndvi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndvi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDVI raster: (NIR-Red)/(NIR+Red))
"""


def rst_evi_python_light_example(spark):
    """Compute Enhanced Vegetation Index (EVI).

    EVI = 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")
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
(single-band EVI raster: G*(NIR-Red)/(NIR+C1*Red-C2*Blue+L))
"""


def rst_savi_python_light_example(spark):
    """Compute Soil-Adjusted Vegetation Index (SAVI).

    SAVI = ((NIR - Red) / (NIR + Red + L)) * (1 + L)
    Default L = 0.5. Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_savi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_savi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band SAVI raster: (NIR-Red)/(NIR+Red+L)*(1+L))
"""


def rst_ndwi_python_light_example(spark):
    """Compute Normalized Difference Water Index (NDWI).

    NDWI = (Green - NIR) / (Green + NIR)
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")
    # Use green (band 3) and NIR (band 2)
    result = df.select(rx.rst_ndwi("tile", f.lit(3), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndwi_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDWI raster: (Green-NIR)/(Green+NIR))
"""


def rst_nbr_python_light_example(spark):
    """Compute Normalized Burn Ratio (NBR).

    NBR = (NIR - SWIR) / (NIR + SWIR)
    Uses band 2 (NIR) and band 3 (proxied as SWIR for fixture).
    Output: single-band Float32 tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")
    # Band 2 (NIR), band 3 (SWIR proxy)
    result = df.select(rx.rst_nbr("tile", f.lit(2), f.lit(3)).alias("tile")).first()
    return result["tile"]


rst_nbr_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NBR raster: (NIR-SWIR)/(NIR+SWIR))
"""


def rst_index_python_light_example(spark):
    """Compute a named spectral index via formula and band map.

    ``band_map`` is a Spark MAP<STRING, INT> wiring the formula's named bands to
    1-based band indices (built with ``f.create_map``). Output: single-band
    Float32 tile. Uses the multiband fixture (non-default; band 1=red, 2=NIR).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")
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
(single-band index raster computed from named formula)
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
(averaged combined raster from 3 input tiles)
"""


def rst_derivedband_python_light_example(spark):
    """Apply a user-provided Python pixel-function across the tile's bands.

    ``python_func`` is a source string following GDAL's VRT pixel-function
    signature; ``func_name`` names the callable within it. Output: single-band
    Float64 tile. Uses the multiband fixture (non-default; 3 bands).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")

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
(raster with derived band from Python UDF)
"""


def rst_mapalgebra_python_light_example(spark):
    """NDVI from two bands of a SINGLE multiband raster — no need to decompose it.

    The spec is the same gdal_calc JSON envelope both tiers accept. The per-
    variable keys map each variable to a raster (``*_index``, 0-based into the
    tiles array) and a 1-based band (``*_band``): here A and B both read raster 0
    (the one tile), A = band 2 (NIR), B = band 1 (Red), giving the classic
    NDVI = (NIR - Red) / (NIR + Red). Direct equivalent of
    ``gdal_calc -A in --A_band=2 -B in --B_band=1 --calc="(A-B)/(A+B)"``.
    Output: single-band Float32 tile. Uses the multiband fixture (rgb_nir_small).
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f

    df = spark.table("multiband_rasters")

    ndvi_spec = (
        '{"calc": "(A - B) / (A + B)", '
        '"A_index": 0, "B_index": 0, "A_band": 2, "B_band": 1}'
    )
    result = df.select(
        rx.rst_mapalgebra(f.array("tile"), f.lit(ndvi_spec)).alias("tile")
    ).first()
    return result["tile"]


rst_mapalgebra_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band Float32 NDVI tile; per-pixel (NIR-Red)/(NIR+Red) in [-1, 1])
"""


def rst_merge_python_light_example(spark):
    """Mosaic (merge) multiple aligned tiles into one spanning their union.

    Input: array of aligned tiles (same grid / CRS).
    Output: single merged tile covering the union extent.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as f
    from _fixtures import multi_band_tiles_df

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
(merged raster from co-registered input tiles)
"""
