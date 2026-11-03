"""
Python code examples for RasterX Function Reference documentation.
Single source of truth for docs/docs/api/rasterx-functions.mdx

Imports and registration are in the common setup only. SQL examples are in rasterx_functions_sql.py.
All accessor examples use the shared canonical fixtures from _fixtures.py.
"""

try:
    from databricks.labs.gbx.rasterx import functions as rx
except ImportError:
    rx = None

# Sample data path at runtime (path_config)
from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py (canonical fixture builders)
# ---------------------------------------------------------------------------


def _get_single_band_df_heavy(spark):
    from ._fixtures import single_band_tile_df_heavy  # noqa: PLC0415

    return single_band_tile_df_heavy(spark)


def _get_multiband_df_heavy(spark):
    from ._fixtures import multiband_tile_df_heavy  # noqa: PLC0415

    return multiband_tile_df_heavy(spark)


def _get_netcdf_df_heavy(spark):
    from ._fixtures import netcdf_tile_df_heavy  # noqa: PLC0415

    return netcdf_tile_df_heavy(spark)


def _get_dem_df_heavy(spark):
    from ._fixtures import dem_tile_df_heavy  # noqa: PLC0415

    return dem_tile_df_heavy(spark)


def rasterx_setup_example(spark):
    """Register RasterX and load each sample raster into a `tile` view. Run once.

    Assumes the geobrix JAR is on the cluster (see the Installation guide).
    The examples on this page read from four temp views; this builds all of them.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from ._fixtures import single_band_path, multiband_path, dem_path, netcdf_path

    rx.register(spark)

    GTIFF_SAMPLE_DIR = str(single_band_path())
    GTIFF_MULTI_DIR = str(multiband_path())
    DTM_DIR = str(dem_path())
    NETCDF_DIR = str(netcdf_path())

    def load_tiles(path, reader, view):
        df = spark.read.format(reader).load(path)
        df.createOrReplaceTempView(view)
        return df

    # Load the default single-band raster into the `rasters` view:
    rasters = load_tiles(GTIFF_SAMPLE_DIR, "gtiff_gdal", "rasters")

    # The other three load the same way — GeoTIFFs use "gtiff_gdal", NetCDF uses "netcdf_gdal":
    load_tiles(GTIFF_MULTI_DIR, "gtiff_gdal", "multiband_rasters")
    load_tiles(DTM_DIR, "gtiff_gdal", "dem_rasters")
    load_tiles(NETCDF_DIR, "netcdf_gdal", "netcdf_rasters")
    return rasters


rasterx_setup_example_output = """
RasterX registered. Four temp views created — `rasters` (single-band),
`multiband_rasters`, `dem_rasters`, and `netcdf_rasters` — each with a `tile`
column. Every example on this page reads from one of these views.
"""


# ---------------------------------------------------------------------------
# Shared helper — in-memory GeoTIFF (no sample data needed)
# ---------------------------------------------------------------------------


def _make_geotiff_bytes(width=4, height=3, count=1, epsg=4326):
    """Return in-memory float32 GTiff bytes (width x height, count bands)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def _make_netcdf_bytes(width=4, height=4):
    """Return NETCDF4 bytes with 'temperature' and 'precipitation' variables.

    Two variables cause GDAL to expose them as subdatasets, enabling
    rst_getsubdataset to extract a named layer by name.
    """
    import os
    import tempfile

    import netCDF4 as nc4
    import numpy as np

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as fh:
        nc_path = fh.name
    with nc4.Dataset(nc_path, "w", format="NETCDF4") as ds:
        ds.createDimension("y", height)
        ds.createDimension("x", width)
        for vname in ("temperature", "precipitation"):
            v = ds.createVariable(vname, "f4", ("y", "x"))
            v[:] = np.arange(width * height, dtype="float32").reshape(height, width)
    with open(nc_path, "rb") as fh:
        nc_bytes = fh.read()
    os.unlink(nc_path)
    return nc_bytes


def _make_tagged_geotiff_bytes(width=4, height=3, epsg=4326):
    """Return in-memory float32 GTiff bytes with band-level metadata tags.

    Band 1 carries GDAL_METADATA tags ``units`` and ``source`` so that
    rst_bandmetadata returns a non-empty map.
    """
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
            ds.update_tags(1, units="m", source="synthetic")
        return mf.read()


def _heavy_tagged_tile_df(spark, **kw):
    """One-row heavy-tier tile DataFrame built from a tagged GeoTIFF."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_tagged_geotiff_bytes(**kw)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def _heavy_nc_tile_df(spark, width=4, height=4):
    """One-row heavy-tier tile DataFrame built from NetCDF bytes."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_netcdf_bytes(width=width, height=height)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("netCDF")).alias("tile"))


def _heavy_tile_df(spark, **kw):
    """One-row heavy-tier tile DataFrame built from in-memory synthetic bytes."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# rst_avg — per-band average pixel values (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (heavy tier; returns WKB binary)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_numbands — band count (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_width — pixel width (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236 columns)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_bandmetadata — per-band metadata map (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — has per-band GDAL metadata tags)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_format — GDAL format name (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_georeference — georeference parameters map (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_getnodata — NoData values per band (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_getsubdataset — extract named subdataset (heavy tier)
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_height — raster height in pixels (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 161 rows)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_max — maximum pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_median — median pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_memsize — in-memory size in bytes (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_metadata — raster metadata map (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_min — minimum pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_pixelcount — total pixel count (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8=64 valid pixels per band)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_pixelheight — pixel height in ground units (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_pixelwidth — pixel width in ground units (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_rotation — rotation in radians (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, rotation=0.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_scalex — scale in X (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scalex=10.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_scaley — scale in Y (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scaley=-10.0 for north-up)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_skewx — skew in X (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewx=0.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_skewy — skew in Y (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewy=0.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_srid — spatial reference ID integer (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_crs — CRS as authority string or WKT (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_subdatasets — list of subdataset names (heavy tier)
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_summary — statistical summary as JSON string (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8, 3 bands; single-band is all-NoData)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_type — data type per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — UInt16 per band)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_upperleftx — upper-left corner X coordinate (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperleftx=2121950.0 in EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_upperlefty — upper-left corner Y coordinate (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperlefty=-10790470.0 in EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_isempty — check if raster is empty (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — has real pixel data, not empty)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_tryopen — validate raster can be opened (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — committed, always openable)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_histogram — per-band histogram (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


# ===========================================================================
# Tile ops & constructors family (heavy tier)
# ===========================================================================


# ---------------------------------------------------------------------------
# rst_asformat — convert a raster to another GDAL format (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_band — extract a single band from a multi-band raster (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_buildoverviews — add internal overview pyramid (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_clip — clip a raster to a geometry cutline (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618)
# Clip geometry: EWKT EPSG:4326 lon/lat over NYC area (auto-reprojected)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_cog_convert — re-layout tile as Cloud Optimized GeoTIFF (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_convolve — apply a convolution kernel (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Kernel: 3x3 identity
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_fillnodata — fill NoData pixels via inverse-distance interpolation (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_filter — spatial filter (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_frombands — stack band tiles into a multi-band tile (heavy tier, constructor)
# Fuller example: extract per-band tiles, re-stack them.
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_initnodata — initialize NoData values (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_resample — resample by a multiplicative factor (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236x161 px)
# factor=2.0 bilinear → 472x322 px
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_resample_to_res — resample to an explicit ground resolution (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10 m pixels)
# target res=20.0 m → 118 px wide
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_resample_to_size — resample to an explicit pixel grid size (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# target 100x100 px
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_setcrs — stamp a CRS string onto a raster without reprojecting (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_setsrid — stamp an EPSG SRID integer onto a raster (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — SRID=32618)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_threshold — binarize a raster (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_transform — reproject to a target EPSG SRID (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Reproject to EPSG:4326
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_updatetype — convert the raster data type (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


# ===========================================================================
# Aggregators family (heavy / rasterx tier)
# ===========================================================================


def _get_multi_band_tiles_df_heavy(spark):
    from ._fixtures import multi_band_tiles_df_heavy  # noqa: PLC0415

    return multi_band_tiles_df_heavy(spark)


# ---------------------------------------------------------------------------
# rst_combineavg_agg -- per-pixel mean across aligned tiles (same grid/CRS)
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif, same grid)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_combineavg_agg_python_heavy_example(spark):
    """Average aligned raster tiles per group using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif split by rst_band.
    All 3 tiles share the same grid, satisfying combineavg_agg's alignment requirement.
    Grouped by region, producing 1 averaged tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_combineavg_agg("tile").alias("avg_tile"))
        .first()
    )
    return result["avg_tile"]


rst_combineavg_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|avg_tile                                                   |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_derivedband_agg -- apply a Python pixel function across a group's tiles
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Pixel function: identity (returns band 0, i.e. the first input band)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_derivedband_agg_python_heavy_example(spark):
    """Apply a Python pixel function across a group's band tiles using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif.  Each tile
    contributes one band to the VRT; the pixel function selects the first band.
    Grouped by region, producing 1 derived tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    pyfunc = (
        "def fn(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0]\n"
    )
    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_derivedband_agg("tile", f.lit(pyfunc), f.lit("fn")).alias("derived")
        )
        .first()
    )
    return result["derived"]


rst_derivedband_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|derived                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_frombands_agg -- stack per-band tiles into one multi-band tile
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_frombands_agg_python_heavy_example(spark):
    """Stack per-band tiles into one multi-band tile per group using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif, each with
    band_index=1/2/3.  Grouped by region, stacks ascending by band_index,
    producing 1 three-band tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_frombands_agg("tile", "band_index").alias("stacked"))
        .first()
    )
    return result["stacked"]


rst_frombands_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|stacked                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_rasterize_agg -- burn geometry/value rows into one tile per group
# Fixture: synthesized 3-row DataFrame of WKB polygon + value + extent constants
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_rasterize_agg_python_heavy_example(spark):
    """Burn geometry/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (geom_wkb, burn_value) over a shared 4x4 extent
    in EPSG:4326.  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    # WKB POLYGON((0 0, 4 0, 4 4, 0 4, 0 0)) in EPSG:4326
    poly = bytes.fromhex(
        "0103000000010000000500000000000000000000000000000000000000"
        "0000000000001040000000000000000000000000000010400000000000001040"
        "000000000000000000000000000010400000000000000000"
        "0000000000000000"
    )
    rows = [(poly, 1.0, "R1"), (poly, 2.0, "R1"), (poly, 3.0, "R1")]
    schema = StructType(
        [
            StructField("geom", BinaryType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_rasterize_agg(
                "geom",
                "value",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(4.0),
                f.lit(4.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("burned")
        )
        .first()
    )
    return result["burned"]


rst_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|burned                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_gridfrompoints_agg -- IDW interpolation: one point/value per row -> one tile
# Fixture: synthesized 4 point rows, EPSG:4326 small extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_gridfrompoints_agg_python_heavy_example(spark):
    """IDW-interpolate point/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 4 rows of (WKB point, observation) over a shared [0,0,1,1]
    EPSG:4326 extent.  Grouped by region, producing 1 Float64 IDW tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    def _wkb_point(x, y):
        import struct  # noqa: PLC0415

        return struct.pack("<bIdd", 1, 1, x, y)

    rows = [
        (_wkb_point(0.1, 0.1), 10.0, "R1"),
        (_wkb_point(0.9, 0.1), 20.0, "R1"),
        (_wkb_point(0.1, 0.9), 30.0, "R1"),
        (_wkb_point(0.9, 0.9), 40.0, "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("val", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_gridfrompoints_agg(
                "pt",
                "val",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("idw")
        )
        .first()
    )
    return result["idw"]


rst_gridfrompoints_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|idw                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_dtmfromgeoms_agg -- Delaunay TIN DTM: one Z-point per row -> one tile
# Fixture: synthesized 4 Z-valued WKB points, small EPSG:4326 extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_dtmfromgeoms_agg_python_heavy_example(spark):
    """Build a Delaunay TIN DTM from Z-valued points per group using the heavy rasterx tier.

    Multi-row fixture: 4 rows of WKB POINT Z with elevation values, over a
    [0,0,1,1] EPSG:4326 extent.  Grouped by region, producing 1 DTM tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        StringType,  # noqa: PLC0415
        StructField,
        StructType,
    )

    def _wkb_point_z(x, y, z):
        import struct  # noqa: PLC0415

        return struct.pack("<bIddd", 1, 1001, x, y, z)

    rows = [
        (_wkb_point_z(0.1, 0.1, 100.0), "R1"),
        (_wkb_point_z(0.9, 0.1, 200.0), "R1"),
        (_wkb_point_z(0.1, 0.9, 150.0), "R1"),
        (_wkb_point_z(0.9, 0.9, 250.0), "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_dtmfromgeoms_agg(
                "pt",
                f.lit(None).cast("array<binary>"),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("dtm")
        )
        .first()
    )
    return result["dtm"]


rst_dtmfromgeoms_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|dtm                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_h3_rasterize_agg -- burn H3 cells into one tile per group
# Fixture: synthesized 3 H3 resolution-9 cell rows, burn values 1.0/2.0/3.0
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_h3_rasterize_agg_python_heavy_example(spark):
    """Rasterize H3 cell/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (H3 cell id BIGINT, burn value) at resolution 9
    near lon/lat (0.01, 0.01).  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    import h3  # noqa: PLC0415

    res = 9
    cell_strs = [
        h3.latlng_to_cell(0.01, 0.01, res),
        h3.latlng_to_cell(0.02, 0.01, res),
        h3.latlng_to_cell(0.01, 0.02, res),
    ]
    cells = [h3.str_to_int(c) for c in cell_strs]
    rows = [
        (int(cells[0]), 1.0, "R1"),
        (int(cells[1]), 2.0, "R1"),
        (int(cells[2]), 3.0, "R1"),
    ]
    from pyspark.sql.types import (
        DoubleType,
        LongType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("cellid", LongType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    # cellid + value are the only required args; extent/size/mode/kring and the
    # EPSG:4326 out_srid all take their defaults (same as the lightweight tier).
    result = (
        df.groupBy("region")
        .agg(rx.rst_h3_rasterize_agg(df["cellid"], df["value"]).alias("tile"))
        .first()
    )
    return result["tile"]


rst_h3_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_quadbin_rasterize_agg -- burn quadbin cells into one tile per group
# Fixture: synthesized 3 quadbin zoom-12 cell rows near central London
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_quadbin_rasterize_agg_python_heavy_example(spark):
    """Rasterize quadbin cell/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (quadbin cell id BIGINT, burn value) at zoom 12
    near central London.  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from databricks.labs.gbx.gridx.quadbin import functions as qbx  # noqa: PLC0415

    qbx.register(spark)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _qb_cells_heavy AS
        SELECT region,
               gbx_quadbin_pointascell(cast(lon as double), cast(lat as double), 12) AS cellid,
               cast(val as double) AS value
        FROM (VALUES
            ('R1', -0.10, 51.50, 1.0),
            ('R1', -0.11, 51.51, 2.0),
            ('R1', -0.09, 51.49, 3.0)
        ) AS t(region, lon, lat, val)
    """)
    df = spark.table("_qb_cells_heavy")
    # cellid + value are the only required args; extent/size/mode/kring and the
    # EPSG:4326 out_srid all take their defaults (same as the lightweight tier).
    result = (
        df.groupBy("region")
        .agg(rx.rst_quadbin_rasterize_agg(df["cellid"], df["value"]).alias("tile"))
        .first()
    )
    spark.catalog.dropTempView("_qb_cells_heavy")
    return result["tile"]


rst_quadbin_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null)
"""


# ---------------------------------------------------------------------------
# rst_ndvi -- compute NDVI from red and NIR bands
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_evi -- Enhanced Vegetation Index from red, NIR, blue
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_savi -- Soil-Adjusted Vegetation Index
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_ndwi -- Normalized Difference Water Index
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_nbr -- Normalized Burn Ratio (NIR - SWIR)
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Note: fixture has no SWIR; using green (band 3) as substitute for demo
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_index -- generic dispatcher for named spectral indices
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_combineavg -- combine multiple tiles by averaging
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinecount -- count valid inputs per pixel
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (pixel-count raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinemax -- per-pixel maximum
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinemedian -- per-pixel median
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinemin -- per-pixel minimum
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinestddev -- per-pixel population std-dev
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_combinesum -- per-pixel sum
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# rst_derivedband -- apply Python UDF to produce derived band
# Fixture: multiband_tile_df_heavy(spark) (3 bands)
# Output: tile struct (raster with derived band)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_merge -- merge/mosaic multiple raster tiles
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------






# ============================================================================
# Terrain Analysis Functions
# ============================================================================
# Fixture: dem_tile_df_heavy(spark) (SRTM elevation, NYC area)
# All return tile struct






# ============================================================================
# Coordinate Transforms & Tiling (Heavy Tier)
# ============================================================================










# ============================================================================
# Generator Functions (Heavy Tier)
# ============================================================================


# The 4km London BNG custom grid spec (EPSG:27700 metres, integer bounds).
def _custom_grid_col():
    """Return a Column expression for the 4km London BNG custom grid spec."""
    from pyspark.sql import functions as f  # noqa: PLC0415

    return f.call_function(
        "gbx_custom_grid",
        f.lit(529000), f.lit(533000), f.lit(179000), f.lit(183000),
        f.lit(2), f.lit(4000), f.lit(4000), f.lit(27700),
    )


# ---------------------------------------------------------------------------
# rst_binpoints_agg -- bin one scalar (x, y, z) point per row into a tile
# Fixture: synthesized 3 BNG (EPSG:27700) scalar rows over a 1 km extent
# Output: tile struct (returns a Float32 raster tile)
# ---------------------------------------------------------------------------


def rst_binpoints_agg_python_heavy_example(spark):
    """Bin one scalar (x, y, z) point per row into a Float32 raster tile per group via the heavy rasterx tier.

    Multi-row fixture: 3 rows of scalar BNG (EPSG:27700) x/y/z coordinates over
    a 1 km extent [550000, 180000, 551000, 181000] at 10×10 pixels.
    Grouped by region, producing 1 Float32 tile row where each pixel holds the
    max z-value of points falling in that pixel; empty pixels carry NoData (-9999.0).
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )  # noqa: PLC0415

    rows = [
        ("R1", 550100.0, 180100.0, 42.5),
        ("R1", 550200.0, 180200.0, 45.1),
        ("R1", 550300.0, 180500.0, 38.7),
    ]
    schema = StructType(
        [
            StructField("region", StringType()),
            StructField("x", DoubleType()),
            StructField("y", DoubleType()),
            StructField("z", DoubleType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_binpoints_agg(
                "x",
                "y",
                "z",
                f.lit(550000.0),
                f.lit(180000.0),
                f.lit(551000.0),
                f.lit(181000.0),
                f.lit(10),
                f.lit(10),
                f.lit(27700),
            ).alias("tile")
        )
        .first()
    )
    return result["tile"]


rst_binpoints_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(Float32 10x10 BNG tile; three scalar points binned by max z-value, empty cells = NoData -9999.0)
"""


# ---------------------------------------------------------------------------
# rst_custom_rasterize_agg -- burn custom-grid cells into one tile per group
# Fixture: 3 BNG custom-grid cell rows at resolution 1 (2km cells) in a 4km
#          London extent; each row carries a burn value.
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_custom_rasterize_agg_python_heavy_example(spark):
    """Rasterize custom-grid cell/value rows into one tile per group via the heavy rasterx tier.

    Multi-row fixture: 3 rows of (custom-grid cell id BIGINT, burn value) at
    resolution 1 (2km cells) inside a 4km London BNG extent
    [529000-533000 E, 179000-183000 N, EPSG:27700].  Grouped by region, producing
    1 rasterized GTiff tile in EPSG:27700.  Cell IDs are computed via
    gbx_custom_pointascell for centroids of 3 of the 4 resolution-1 cells.
    """
    if rx is None:
        raise ImportError("rasterx not installed")
    from databricks.labs.gbx.gridx.custom import functions as cx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    cx.register(spark)  # registers gbx_custom_pointascell and gbx_custom_grid

    _GRID_SQL = (
        "gbx_custom_grid(529000, 533000, 179000, 183000, 2, 4000, 4000, 27700)"
    )
    # Compute three resolution-1 cell IDs via SQL (three of the four 2km cells).
    # Use SELECT-from-VALUES: UDFs cannot appear directly in VALUES clauses.
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW _custom_rasterize_cells_heavy AS
        SELECT region,
               gbx_custom_pointascell(wkt, {_GRID_SQL}, 1) AS cellid,
               val AS value
        FROM (VALUES
            ('R1', 'POINT(530000 180000)', cast(1.0 as double)),
            ('R1', 'POINT(532000 180000)', cast(2.0 as double)),
            ('R1', 'POINT(530000 182000)', cast(3.0 as double))
        ) AS t(region, wkt, val)
    """)
    df = spark.table("_custom_rasterize_cells_heavy")

    result = (
        df.groupBy("region")
        .agg(
            rx.rst_custom_rasterize_agg(
                "cellid", _custom_grid_col(), f.col("value"),
                out_srid=f.lit(27700),  # heavy tier requires non-null out_srid
                kring_pad=f.lit(0),     # no padding: avoid expanding outside grid bounds
            ).alias("tile")
        )
        .first()
    )
    spark.catalog.dropTempView("_custom_rasterize_cells_heavy")
    return result["tile"]


rst_custom_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(one v2 Tile per group — raster bytes populated, path null; three 2km custom-grid cells burned)
"""


# ---------------------------------------------------------------------------
# rst_align_to — warp a tile onto a reference tile's grid (heavy tier)
# Fixture: DEM used for both tile and reference_tile (no-op warp)
# Output: tile struct (warped to reference grid)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_chm — Canopy Height Model = clamp(align(DSM→DEM) - DEM, min=0) (heavy tier)
# Fixture: DEM used as both DSM and DEM (degenerate CHM = 0 everywhere)
# Output: tile struct (Float32 CHM tile)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_isoband — reclassify a raster band into elevation bands → polygons (heavy tier)
# Fixture: DEM (dem_small.tif, 0–311 m elevation range, EPSG:32618)
# Output: ARRAY<struct(geom_wkb, band, lower, upper)>
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# pmtiles_agg -- fold (z, x, y, bytes) tile rows into a PMTiles v3 archive (heavy tier)
# Fixture: 9 synthetic tiles at zoom level 2 (x in [0,2], y in [0,2])
# Output: BINARY (PMTiles v3 archive bytes)
# ---------------------------------------------------------------------------




