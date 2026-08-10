"""
Python code examples for the light (pyrx) tier of RasterX tile-ops and constructor functions.
Single source of truth for the tile-ops & constructors tab in docs/docs/api/raster-functions.mdx.

All examples use the shared canonical fixtures from _fixtures.py (single-band, multiband)
so every function's four tabs show the SAME example — the same fixture, operation, and
argument values expressed in each tier's language.

Fixture assignments
-------------------
SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618, 161x236, 10m pixels):
    rst_asformat, rst_clip, rst_convolve, rst_cog_convert, rst_fillnodata,
    rst_filter, rst_initnodata, rst_resample, rst_resample_to_res, rst_resample_to_size,
    rst_setcrs, rst_setsrid, rst_threshold, rst_transform, rst_transformcrs, rst_updatetype,
    rst_buildoverviews

MULTIBAND (rgb_nir_small.tif, EPSG:4326, 8x8, 3 bands, UInt16):
    rst_band (needs > 1 band), rst_frombands (stacks single-band tiles)

CONSTRUCTORS (produce a tile from bytes/path, not from an existing tile):
    rst_fromcontent, rst_frombands, rst_fromfile
"""

try:
    from databricks.labs.gbx.pyrx import functions as rx
except ImportError:
    rx = None


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py
# ---------------------------------------------------------------------------


def _get_single_band_df(spark):
    from _fixtures import single_band_tile_df  # noqa: PLC0415

    return single_band_tile_df(spark)


def _get_multiband_df(spark):
    from _fixtures import multiband_tile_df  # noqa: PLC0415

    return multiband_tile_df(spark)


# ---------------------------------------------------------------------------
# rst_asformat — convert a raster to another GDAL format
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Output verify: rst_format of result = "GTiff" (re-encoded)
# ---------------------------------------------------------------------------


def rst_asformat_python_light_example(spark):
    """Convert a raster tile to another GDAL format using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_asformat("tile", f.lit("GTiff"))).alias("format")
    ).first()
    return result["format"]


rst_asformat_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(GDAL format name of the re-encoded tile)
"""


# ---------------------------------------------------------------------------
# rst_band — extract a single band from a multi-band raster
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands; extract band 1)
# Output verify: rst_numbands of result = 1
# ---------------------------------------------------------------------------


def rst_band_python_light_example(spark):
    """Extract band 1 from a multi-band raster tile using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) to demonstrate
    band extraction. The result is a new single-band tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    result = df.select(
        rx.rst_numbands(rx.rst_band("tile", f.lit(1))).alias("num_bands")
    ).first()
    return result["num_bands"]


rst_band_python_light_example_output = """
+---------+
|num_bands|
+---------+
|1        |
+---------+
(band count of the extracted single-band tile)
"""


# ---------------------------------------------------------------------------
# rst_buildoverviews — add internal overview pyramid to a raster tile
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_buildoverviews_python_light_example(spark):
    """Add internal overview levels to a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_buildoverviews("tile", f.array(f.lit(2), f.lit(4)))).alias(
            "format"
        )
    ).first()
    return result["format"]


rst_buildoverviews_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile with internal overviews at levels [2, 4])
"""


# ---------------------------------------------------------------------------
# rst_clip — clip a raster to a geometry cutline
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618)
# Clip geometry: EWKT with EPSG:4326 lon/lat over NYC area (auto-reprojected)
# Output verify: rst_format of clipped tile = "GTiff"
# ---------------------------------------------------------------------------


def rst_clip_python_light_example(spark):
    """Clip a raster tile to a geometry cutline using the light pyrx tier.

    Uses a plain WKT polygon in the raster's native CRS (EPSG:32618), covering
    the upper-left half of the raster extent (upperleftx=2121950, width=236px at 10m).
    Omitting an SRID means the geometry is assumed to be in the raster's CRS.
    To clip with a WGS84 geometry, embed the SRID as EWKT: 'SRID=4326;POLYGON(...)'.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    # WKT polygon in raster's native CRS (upper-left quadrant of the tile extent)
    clip_geom = "POLYGON((2121950 -10791280, 2123140 -10791280, 2123140 -10790470, 2121950 -10790470, 2121950 -10791280))"
    result = df.select(
        rx.rst_format(rx.rst_clip("tile", f.lit(clip_geom), f.lit(True))).alias(
            "format"
        )
    ).first()
    return result["format"]


rst_clip_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the clipped tile; polygon is in the raster's native CRS (no SRID = no reprojection))
"""


# ---------------------------------------------------------------------------
# rst_cog_convert — re-layout tile as Cloud Optimized GeoTIFF
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Output verify: rst_format of result = "GTiff" (COG is a valid GeoTIFF)
# ---------------------------------------------------------------------------


def rst_cog_convert_python_light_example(spark):
    """Re-layout a raster tile as a Cloud Optimized GeoTIFF using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_cog_convert("tile")).alias("format")
    ).first()
    return result["format"]


rst_cog_convert_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(a COG is a valid GeoTIFF; use rst_memsize to confirm the tiled internal layout)
"""


# ---------------------------------------------------------------------------
# rst_convolve — apply a convolution kernel to a raster tile
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Kernel: 3x3 identity (pass-through)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_convolve_python_light_example(spark):
    """Apply a 3x3 identity convolution kernel to a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    kernel = f.array(
        f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
        f.array(f.lit(0.0), f.lit(1.0), f.lit(0.0)),
        f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
    )
    result = df.select(
        rx.rst_format(rx.rst_convolve("tile", kernel)).alias("format")
    ).first()
    return result["format"]


rst_convolve_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the convolved tile; kernel is a 3x3 identity)
"""


# ---------------------------------------------------------------------------
# rst_fillnodata — fill NoData pixels via inverse-distance interpolation
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_fillnodata_python_light_example(spark):
    """Fill NoData pixels in a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_fillnodata("tile", f.lit(100.0), f.lit(0))).alias("format")
    ).first()
    return result["format"]


rst_fillnodata_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the filled tile; NoData holes searched within 100 pixels)
"""


# ---------------------------------------------------------------------------
# rst_filter — spatial filter (median / mean / mode)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_filter_python_light_example(spark):
    """Apply a 3x3 median spatial filter to a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_filter("tile", f.lit(3), f.lit("median"))).alias("format")
    ).first()
    return result["format"]


rst_filter_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the filtered tile; 3x3 median filter applied)
"""


# ---------------------------------------------------------------------------
# rst_fromcontent — create a tile from binary content (constructor)
# Fuller example: read bytes via binaryFile reader, construct tile, verify format.
# Note: constructor — no pre-existing tile; example shows the canonical build pattern.
# ---------------------------------------------------------------------------


def rst_fromcontent_python_light_example(spark):
    """Build a tile from binary raster bytes using the light pyrx tier.

    Constructor: reads bytes via Spark's binaryFile reader and constructs a tile
    column via rst_fromcontent. This is the canonical tier-agnostic pattern:
    binaryFile runs in Spark (holds the Volume credential) and works on any
    compute — both lightweight and heavyweight tiers.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from _fixtures import single_band_path  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    path = single_band_path()
    binary_df = spark.read.format("binaryFile").load(path)
    tile_df = binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_fromcontent_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile loaded from binary content via binaryFile reader)
"""


# ---------------------------------------------------------------------------
# rst_frombands — stack band tiles into a multi-band tile (constructor)
# Fuller example: split a tile to its single bands, stack them back.
# Note: constructor producing a tile — example shows the full build round-trip.
# ---------------------------------------------------------------------------


def rst_frombands_python_light_example(spark):
    """Stack an array of single-band tiles into a multi-band tile using the light pyrx tier.

    Constructor: takes an ARRAY<tile> of single-band tiles and stacks them in
    array order (element 0 → band 1). Example splits the multiband fixture into
    per-band tiles via rst_band, then re-stacks them into a 3-band tile.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_multiband_df(spark)
    # Build an array of per-band tiles (band 1, band 2, band 3) then stack
    with_bands = df.select(
        f.array(
            rx.rst_band("tile", f.lit(1)),
            rx.rst_band("tile", f.lit(2)),
            rx.rst_band("tile", f.lit(3)),
        ).alias("bands")
    )
    result = with_bands.select(
        rx.rst_numbands(rx.rst_frombands("bands")).alias("num_bands")
    ).first()
    return result["num_bands"]


rst_frombands_python_light_example_output = """
+---------+
|num_bands|
+---------+
|3        |
+---------+
(band count of the re-stacked 3-band tile)
"""


# ---------------------------------------------------------------------------
# rst_fromfile — load a tile from a file path (constructor, light / SQL only)
# Note: constructor — no pre-existing tile. Light and SQL tiers only.
#       The heavyweight JVM tier cannot read UC Volume FUSE paths — no Scala tab.
# ---------------------------------------------------------------------------


def rst_fromfile_python_light_example(spark):
    """Load a raster tile from a file path using the light pyrx tier.

    Constructor: takes a column of file paths and opens each via rasterio.
    Light and SQL tiers only — there is no Scala/JVM form (the JVM executor
    cannot read UC Volume FUSE paths). For a tier-agnostic alternative use
    binaryFile + rst_fromcontent.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from _fixtures import single_band_path  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    path = single_band_path()
    path_df = spark.createDataFrame([(path,)], ["path"])
    tile_df = path_df.select(rx.rst_fromfile("path", f.lit("GTiff")).alias("tile"))
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_fromfile_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile loaded from a file path; light and SQL tiers only)
"""


# ---------------------------------------------------------------------------
# rst_initnodata — initialize NoData values on a raster tile
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_initnodata_python_light_example(spark):
    """Initialize NoData values on a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(rx.rst_format(rx.rst_initnodata("tile")).alias("format")).first()
    return result["format"]


rst_initnodata_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile with NoData initialized; lightweight tier assigns -9999.0 when absent)
"""


# ---------------------------------------------------------------------------
# rst_resample — resample by a multiplicative factor
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236x161 px)
# factor=2.0 bilinear → 472x322 px
# Output verify: rst_width of result = 472
# ---------------------------------------------------------------------------


def rst_resample_python_light_example(spark):
    """Resample a raster tile by a 2x factor using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_width(rx.rst_resample("tile", f.lit(2.0), f.lit("bilinear"))).alias(
            "width"
        )
    ).first()
    return result["width"]


rst_resample_python_light_example_output = """
+-----+
|width|
+-----+
|472  |
+-----+
(width in pixels of the 2x upsampled tile; source is 236 px wide)
"""


# ---------------------------------------------------------------------------
# rst_resample_to_res — resample to an explicit ground resolution
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10 m pixels, EPSG:32618)
# target res=20.0 m → width = ceil(236*10/20) = 118 px
# Output verify: rst_width of result = 118
# ---------------------------------------------------------------------------


def rst_resample_to_res_python_light_example(spark):
    """Resample a raster tile to an explicit ground resolution using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_width(
            rx.rst_resample_to_res("tile", f.lit(20.0), f.lit(20.0), f.lit("average"))
        ).alias("width")
    ).first()
    return result["width"]


rst_resample_to_res_python_light_example_output = """
+-----+
|width|
+-----+
|118  |
+-----+
(width in pixels after downsampling from 10 m to 20 m resolution)
"""


# ---------------------------------------------------------------------------
# rst_resample_to_size — resample to an explicit pixel grid size
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# target 100x100 px
# Output verify: rst_width of result = 100
# ---------------------------------------------------------------------------


def rst_resample_to_size_python_light_example(spark):
    """Resample a raster tile to an explicit pixel grid size using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_width(
            rx.rst_resample_to_size("tile", f.lit(100), f.lit(100), f.lit("near"))
        ).alias("width")
    ).first()
    return result["width"]


rst_resample_to_size_python_light_example_output = """
+-----+
|width|
+-----+
|100  |
+-----+
(width in pixels of the resampled tile forced to 100x100)
"""


# ---------------------------------------------------------------------------
# rst_setcrs — stamp a CRS string onto a raster without reprojecting
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Stamp with same CRS string to preserve correctness
# Output verify: rst_crs of result = "EPSG:32618"
# ---------------------------------------------------------------------------


def rst_setcrs_python_light_example(spark):
    """Stamp a CRS string onto a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_crs(rx.rst_setcrs("tile", f.lit("EPSG:32618"))).alias("crs")
    ).first()
    return result["crs"]


rst_setcrs_python_light_example_output = """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
(CRS string after stamping; does NOT reproject — use rst_transformcrs to reproject)
"""


# ---------------------------------------------------------------------------
# rst_setsrid — stamp an EPSG SRID integer onto a raster without reprojecting
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — SRID=32618)
# Output verify: rst_srid of result = 32618
# ---------------------------------------------------------------------------


def rst_setsrid_python_light_example(spark):
    """Stamp an EPSG SRID onto a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_srid(rx.rst_setsrid("tile", f.lit(32618))).alias("srid")
    ).first()
    return result["srid"]


rst_setsrid_python_light_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
(EPSG SRID after stamping; does NOT reproject — use rst_transform to reproject)
"""


# ---------------------------------------------------------------------------
# rst_threshold — binarize a raster: pixels matching op/value → 1, others → 0
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0, all pixels are 0)
# threshold > 0 → all pixels become 0 (no pixels pass the threshold)
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_threshold_python_light_example(spark):
    """Binarize a raster tile using a threshold condition using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_threshold("tile", f.lit(">"), f.lit(0.0))).alias("format")
    ).first()
    return result["format"]


rst_threshold_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the binary mask tile; pixels > 0.0 → 1, others → 0)
"""


# ---------------------------------------------------------------------------
# rst_transform — reproject a raster to a target EPSG SRID
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Reproject to EPSG:4326 (WGS84)
# Output verify: rst_srid of result = 4326
# ---------------------------------------------------------------------------


def rst_transform_python_light_example(spark):
    """Reproject a raster tile to a target EPSG SRID using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_srid(rx.rst_transform("tile", f.lit(4326))).alias("srid")
    ).first()
    return result["srid"]


rst_transform_python_light_example_output = """
+----+
|srid|
+----+
|4326|
+----+
(EPSG SRID of the reprojected tile; source is EPSG:32618 (UTM Zone 18N))
"""


# ---------------------------------------------------------------------------
# rst_transformcrs — reproject a raster to a target CRS given as a string
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Reproject to EPSG:3857 (Web Mercator)
# Output verify: rst_crs of result contains "3857"
# ---------------------------------------------------------------------------


def rst_transformcrs_python_light_example(spark):
    """Reproject a raster tile to a CRS string target using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_crs(rx.rst_transformcrs("tile", f.lit("EPSG:3857"))).alias("crs")
    ).first()
    return result["crs"]


rst_transformcrs_python_light_example_output = """
+----------+
|crs       |
+----------+
|EPSG:3857 |
+----------+
(CRS string of the reprojected tile; accepts authority codes, WKT, or PROJ4)
"""


# ---------------------------------------------------------------------------
# rst_updatetype — convert the raster data type
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Convert to Float32
# Output verify: rst_format of result = "GTiff"
# ---------------------------------------------------------------------------


def rst_updatetype_python_light_example(spark):
    """Convert a raster tile's data type using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    df = _get_single_band_df(spark)
    result = df.select(
        rx.rst_format(rx.rst_updatetype("tile", f.lit("Float32"))).alias("format")
    ).first()
    return result["format"]


rst_updatetype_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the type-converted tile; use rst_type to confirm the new data type)
"""
