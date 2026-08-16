"""
Shared test fixture generators for API documentation tests.

The committed .tif/.nc artifacts in src/test/resources/binary/ are the
canonical fixtures; the generator functions here document how they were
produced and can be re-run if a file needs to be regenerated.

NOTE: sample-data/Volumes/main/default/geobrix_samples/ is gitignored
(see .gitignore lines 40-41), so committed test fixtures live under
src/test/resources/binary/ instead.

Fixture paths (all committed under src/test/resources/binary/)
-------------
SINGLE_BAND   nyc_sentinel2_red_small.tif — 236x161 UInt16, EPSG:32618, real
               spatially-varying data. Replaces the all-NoData /Volumes
               placeholder (min=max=0) which silently degraded pixel-dependent
               examples and made tiling generators emit zero rows. Same
               georeferencing as the placeholder so header/coordinate examples
               are unchanged. Regenerate with make_single_band_fixture().
MULTIBAND     rgb_nir_small.tif — 3 bands (red/NIR/green), 8x8 pixels, EPSG:4326,
               per-band metadata tags. Regenerate with make_multiband_fixture().
DEM           dem_small.tif — 64x64 Float32, EPSG:32618, real elevation
               variation (~0-311m). Regenerate with make_dem_fixture().
COLOR_TABLE   elevation.clr — gdaldem color ramp for rst_color_relief.
NETCDF        prAdjust_day_HadGEM2-CC_*.nc — two subdatasets (time_bnds,
               prAdjust); used by rst_subdatasets / rst_getsubdataset.
"""

from pathlib import Path
import sys
import os

# ---------------------------------------------------------------------------
# Path constants (relative paths are relative to repo root)
# ---------------------------------------------------------------------------

# Repo-root-relative paths for committed fixtures
MULTIBAND = "src/test/resources/binary/geotiff-small/rgb_nir_small.tif"
DEM = "src/test/resources/binary/elevation/dem_small.tif"
COLOR_TABLE = "src/test/resources/binary/elevation/elevation.clr"
SINGLE_BAND = "src/test/resources/binary/geotiff-small/nyc_sentinel2_red_small.tif"

# Long filename committed under netcdf-CMIP5/
_NETCDF_FILENAME = (
    "prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc"
    "_rcp45_r1i1p1_20201201-20201231.nc"
)
NETCDF = f"src/test/resources/binary/netcdf-CMIP5/{_NETCDF_FILENAME}"


def _sample_data_base() -> str:
    """Return SAMPLE_DATA_BASE from path_config (lazy import)."""
    try:
        from path_config import SAMPLE_DATA_BASE  # noqa: PLC0415

        return SAMPLE_DATA_BASE
    except ImportError:
        # Fallback for environments where path_config is not on sys.path
        root = os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data")
        return f"{root.rstrip('/')}/geobrix-examples"


def single_band_path() -> Path:
    """Absolute path to the committed single-band GeoTIFF.

    A real 236x161 UInt16 raster (EPSG:32618) with spatially-varying pixel
    values. The /Volumes sample `nyc_sentinel2_red.tif` is an all-NoData
    placeholder (min=max=0), which silently degrades every pixel-dependent
    example and makes tiling generators emit zero rows; the committed fixture
    carries real data with the same georeferencing.
    """
    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    return repo_root / SINGLE_BAND


def dem_path() -> Path:
    """Absolute path to the committed DEM GeoTIFF."""
    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    return repo_root / DEM


def color_table_path() -> Path:
    """Absolute path to the committed gdaldem color table."""
    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    return repo_root / COLOR_TABLE


def multiband_path() -> Path:
    """Absolute path to the committed multiband GeoTIFF."""
    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    return repo_root / MULTIBAND


def netcdf_path() -> Path:
    """Absolute path to the committed NetCDF fixture."""
    repo_root = Path(__file__).parents[4]
    return repo_root / NETCDF


# ---------------------------------------------------------------------------
# Light-tier (pyrx) DataFrame builders
# Each returns a DataFrame with a `tile` column loaded from the canonical file.
# Use rst_fromcontent (binaryFile reader) so no JAR is needed.
# ---------------------------------------------------------------------------


def single_band_tile_df(spark):
    """
    Light-tier (pyrx) one-row DataFrame with `tile` from nyc_sentinel2_red.tif.

    The SHOWN example in each function tab is the bare invocation on this
    DataFrame; the tile is a single-band GeoTIFF (the default fixture for
    most accessor and tile-ops examples).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = str(single_band_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def multiband_tile_df(spark):
    """
    Light-tier (pyrx) one-row DataFrame with `tile` from rgb_nir_small.tif.

    3-band GeoTIFF (red, NIR, green) with per-band metadata tags.
    Used by band-math, rst_numbands, and rst_bandmetadata examples.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = str(multiband_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def dem_tile_df(spark):
    """
    Light-tier (pyrx) one-row DataFrame with `tile` from dem_small.tif.

    Single-band DEM raster with real elevation variation (0–311m, UTM 18N).
    Used by terrain function examples (slope, aspect, hillshade, …).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = str(dem_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def netcdf_tile_df(spark):
    """
    Light-tier (pyrx) one-row DataFrame with `tile` from the CMIP5 NetCDF.

    The file has two subdatasets (time_bnds, prAdjust).
    Used only by rst_subdatasets and rst_getsubdataset examples.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = str(netcdf_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("netCDF")).alias("tile")
    )


# ---------------------------------------------------------------------------
# Heavy-tier (rasterx) DataFrame builders
# Same files; loaded via the rasterx shim with rst_fromcontent.
# ---------------------------------------------------------------------------


def single_band_tile_df_heavy(spark):
    """
    Heavy-tier (rasterx) one-row DataFrame with `tile` from nyc_sentinel2_red.tif.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = str(single_band_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def multiband_tile_df_heavy(spark):
    """
    Heavy-tier (rasterx) one-row DataFrame with `tile` from rgb_nir_small.tif.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = str(multiband_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def dem_tile_df_heavy(spark):
    """
    Heavy-tier (rasterx) one-row DataFrame with `tile` from dem_small.tif.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = str(dem_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )


def netcdf_tile_df_heavy(spark):
    """
    Heavy-tier (rasterx) one-row DataFrame with `tile` from the CMIP5 NetCDF.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = str(netcdf_path())
    binary_df = spark.read.format("binaryFile").load(path)
    return binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("netCDF")).alias("tile")
    )


# ---------------------------------------------------------------------------
# Setup views (rasters / multiband_rasters / dem_rasters / netcdf_rasters)
#
# Every per-function doc example on the raster-functions page starts from one of
# these four temp views (mirroring the page's Setup section). The rendered tabs
# read `spark.table("<view>")`; these helpers create the tier-appropriate views
# in the test session so those examples execute. A test module registers the
# views once via an autouse fixture (see the per-tier setup functions below).
# ---------------------------------------------------------------------------

# view name -> tile-df builder, one mapping per tier. The builder loads the same
# committed fixture the documented named reader would (gtiff_gbx/gtiff_gdal etc.).
_SETUP_VIEWS_LIGHT = {
    "rasters": single_band_tile_df,
    "multiband_rasters": multiband_tile_df,
    "dem_rasters": dem_tile_df,
    "netcdf_rasters": netcdf_tile_df,
}
_SETUP_VIEWS_HEAVY = {
    "rasters": single_band_tile_df_heavy,
    "multiband_rasters": multiband_tile_df_heavy,
    "dem_rasters": dem_tile_df_heavy,
    "netcdf_rasters": netcdf_tile_df_heavy,
}


def create_setup_views_light(spark):
    """Create the four light-tier Setup views (rasters, multiband_rasters,
    dem_rasters, netcdf_rasters) in `spark`. Idempotent (createOrReplace)."""
    for view, builder in _SETUP_VIEWS_LIGHT.items():
        builder(spark).createOrReplaceTempView(view)


def create_setup_views_heavy(spark):
    """Create the four heavy-tier Setup views in `spark`. Idempotent.

    Note: the light and heavy Setup views share names; a single Spark session
    can hold only one tier's views at a time. pytest runs a test module to
    completion before the next, so an autouse module fixture that calls the
    matching tier's creator keeps each file's examples reading the right tier.
    """
    for view, builder in _SETUP_VIEWS_HEAVY.items():
        builder(spark).createOrReplaceTempView(view)


# ---------------------------------------------------------------------------
# Multiband GeoTIFF fixture generator
# ---------------------------------------------------------------------------

# Path relative to repo root — tracked in git (not gitignored).
# band 1 = red, band 2 = NIR, band 3 = green


def make_multiband_fixture(path: "str | Path | None" = None) -> Path:
    """
    Generate a small (8x8) 3-band GeoTIFF suitable for band-math tests.

    Bands
    -----
    1 – red   : values 50–120  (low reflectance)
    2 – NIR   : values 100–200 (high reflectance → positive NDVI)
    3 – green : values 80–150  (intermediate)

    Metadata tags are written per-band so that rst_bandmetadata returns a
    non-empty map.  CRS is EPSG:4326; the affine transform places the tile
    over a small area in the North Sea (no overlap with production sample data).

    Parameters
    ----------
    path : optional override path.  Defaults to MULTIBAND (relative to the
           repo root, resolved from this file's location).

    Returns
    -------
    pathlib.Path pointing at the written file.
    """
    import numpy as np  # noqa: PLC0415
    import rasterio  # noqa: PLC0415
    from rasterio.crs import CRS  # noqa: PLC0415
    from rasterio.transform import from_bounds  # noqa: PLC0415

    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    dest = Path(path) if path else repo_root / MULTIBAND
    dest.parent.mkdir(parents=True, exist_ok=True)

    width, height = 8, 8
    # Affine transform: small area in the North Sea to avoid conflicts
    # Positional: from_bounds(west, south, east, north, width, height)
    transform = from_bounds(2.0, 55.0, 2.01, 55.01, width, height)

    # Distinct per-band data (uint16) — NDVI = (NIR-red)/(NIR+red)
    rng = np.random.default_rng(42)
    red = rng.integers(50, 120, size=(height, width), dtype=np.uint16)
    nir = rng.integers(100, 200, size=(height, width), dtype=np.uint16)
    green = rng.integers(80, 150, size=(height, width), dtype=np.uint16)

    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "width": width,
        "height": height,
        "count": 3,
        "crs": CRS.from_epsg(4326),
        "transform": transform,
    }

    with rasterio.open(dest, "w", **profile) as ds:
        ds.write(red, 1)
        ds.write(nir, 2)
        ds.write(green, 3)
        # Per-band metadata so rst_bandmetadata returns a non-empty map
        ds.update_tags(1, name="red", wavelength_nm="665", band_index="1")
        ds.update_tags(2, name="nir", wavelength_nm="865", band_index="2")
        ds.update_tags(3, name="green", wavelength_nm="560", band_index="3")

    return dest


def make_dem_fixture(path: "str | Path | None" = None) -> Path:
    """(Re)generate the committed DEM fixture (dem_small.tif).

    64x64 Float32 single-band elevation raster, EPSG:32618 (UTM 18N), ~10 m
    pixels, extent 500000-500640 E / 4500000-4500640 N. The surface is a SW→NE
    gradient (0-300 m) plus a central Gaussian hill (+150 m) plus small noise,
    clipped to 0-500 m — real elevation variation so slope/aspect/hillshade/
    contour/color_relief produce meaningful output (the /Volumes sample DEM is a
    2x2 all-zero placeholder). The committed .tif is the artifact examples load.
    """
    import numpy as np  # noqa: PLC0415
    import rasterio  # noqa: PLC0415
    from rasterio.crs import CRS  # noqa: PLC0415
    from rasterio.transform import from_bounds  # noqa: PLC0415

    repo_root = Path(__file__).parents[4]
    dest = Path(path) if path else repo_root / DEM
    dest.parent.mkdir(parents=True, exist_ok=True)

    width, height = 64, 64
    transform = from_bounds(500000.0, 4500000.0, 500640.0, 4500640.0, width, height)
    x = np.linspace(0, 1, width)
    y = np.linspace(0, 1, height)
    xx, yy = np.meshgrid(x, y)
    base = 300 * (xx + yy) / 2
    hill = 150 * np.exp(-((xx - 0.5) ** 2 + (yy - 0.5) ** 2) / 0.05)
    noise = np.random.RandomState(42).normal(0, 5, (height, width))
    dem = np.clip((base + hill + noise).astype(np.float32), 0, 500)

    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "width": width,
        "height": height,
        "count": 1,
        "crs": CRS.from_epsg(32618),
        "transform": transform,
        "nodata": -9999.0,
    }
    with rasterio.open(dest, "w", **profile) as ds:
        ds.write(dem, 1)
    return dest


def make_color_table(path: "str | Path | None" = None) -> Path:
    """(Re)generate the committed gdaldem color table (elevation.clr).

    An elevation ramp spanning the DEM fixture's 0-311 m range, consumed by
    rst_color_relief. gdaldem format: ``elevation R G B`` per line, ``nv`` for
    the NoData color.
    """
    repo_root = Path(__file__).parents[4]
    dest = Path(path) if path else repo_root / COLOR_TABLE
    dest.parent.mkdir(parents=True, exist_ok=True)
    ramp = (
        "0 34 139 34\n"
        "50 50 205 50\n"
        "100 34 139 34\n"
        "150 144 238 144\n"
        "200 255 255 0\n"
        "250 255 200 0\n"
        "300 210 105 30\n"
        "311 255 255 255\n"
        "nv 0 0 0"
    )
    dest.write_text(ramp)
    return dest


def make_single_band_fixture(path: "str | Path | None" = None) -> Path:
    """(Re)generate the committed single-band fixture (nyc_sentinel2_red_small.tif).

    236x161 UInt16 single-band raster with the EXACT georeferencing of the
    /Volumes sample placeholder — EPSG:32618, 10 m pixels, origin
    (2121950, -10790470), nodata=0 — so every header/coordinate assertion
    (width=236, height=161, pixel (100,80) <-> world (2122955,-10791275)) still
    holds. Unlike the all-NoData placeholder, this carries real spatially-varying
    pixel values (a smooth gradient + a Gaussian blob, 1..4000), so pixel
    statistics are meaningful and the tiling generators (rst_retile / maketiles /
    tooverlappingtiles) emit real (non-empty) tiles.
    """
    import numpy as np  # noqa: PLC0415
    import rasterio  # noqa: PLC0415
    from rasterio.crs import CRS  # noqa: PLC0415
    from affine import Affine  # noqa: PLC0415

    repo_root = Path(__file__).parents[4]
    dest = Path(path) if path else repo_root / SINGLE_BAND
    dest.parent.mkdir(parents=True, exist_ok=True)

    width, height = 236, 161
    # Exact placeholder transform: 10 m pixels, origin (2121950, -10790470).
    transform = Affine(10.0, 0.0, 2121950.0, 0.0, -10.0, -10790470.0)

    xx, yy = np.meshgrid(np.linspace(0, 1, width), np.linspace(0, 1, height))
    gradient = 400 + 3000 * (xx + yy) / 2
    blob = 1500 * np.exp(-((xx - 0.55) ** 2 + (yy - 0.4) ** 2) / 0.04)
    noise = np.random.RandomState(7).normal(0, 40, (height, width))
    # Keep values in 1..4000 (avoid 0, which is NoData) so every pixel is valid.
    data = np.clip(gradient + blob + noise, 1, 4000).astype(np.uint16)

    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "width": width,
        "height": height,
        "count": 1,
        "crs": CRS.from_epsg(32618),
        "transform": transform,
        "nodata": 0,
    }
    with rasterio.open(dest, "w", **profile) as ds:
        ds.write(data, 1)
    return dest


# ---------------------------------------------------------------------------
# Multi-tile DataFrame builders (aggregator family)
# Each returns a DataFrame with multiple `tile` rows for use with groupBy+agg.
# ---------------------------------------------------------------------------


def multi_band_tiles_df(spark):
    """
    Light-tier (pyrx) 3-row DataFrame from rgb_nir_small.tif.

    Each row holds one single-band tile extracted from the 3-band multiband
    fixture (band 1, 2, 3 as separate rows).  All three tiles share the same
    grid (extent/shape/CRS), so they are suitable for:
      - rst_combineavg_agg (requires aligned tiles)
      - rst_frombands_agg  (stacks by band_index)
      - rst_derivedband_agg (stacks all bands, applies pixel function)
      - rst_merge_agg       (merges/mosaics; co-registered = exact overlap)

    The returned DataFrame has columns: tile, band_index (1/2/3), region (='R1').
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = str(multiband_path())
    binary_df = spark.read.format("binaryFile").load(path)
    mb_df = binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("mb_tile")
    )
    # Produce 3 rows: one per band extracted from the multi-band tile
    b1 = mb_df.select(rx.rst_band("mb_tile", f.lit(1)).alias("tile")).withColumn(
        "band_index", f.lit(1)
    )
    b2 = mb_df.select(rx.rst_band("mb_tile", f.lit(2)).alias("tile")).withColumn(
        "band_index", f.lit(2)
    )
    b3 = mb_df.select(rx.rst_band("mb_tile", f.lit(3)).alias("tile")).withColumn(
        "band_index", f.lit(3)
    )
    return b1.union(b2).union(b3).withColumn("region", f.lit("R1"))


def multi_band_tiles_df_heavy(spark):
    """
    Heavy-tier (rasterx) 3-row DataFrame from rgb_nir_small.tif.

    Mirror of multi_band_tiles_df for the heavy tier.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = str(multiband_path())
    binary_df = spark.read.format("binaryFile").load(path)
    mb_df = binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("mb_tile")
    )
    b1 = mb_df.select(rx.rst_band("mb_tile", f.lit(1)).alias("tile")).withColumn(
        "band_index", f.lit(1)
    )
    b2 = mb_df.select(rx.rst_band("mb_tile", f.lit(2)).alias("tile")).withColumn(
        "band_index", f.lit(2)
    )
    b3 = mb_df.select(rx.rst_band("mb_tile", f.lit(3)).alias("tile")).withColumn(
        "band_index", f.lit(3)
    )
    return b1.union(b2).union(b3).withColumn("region", f.lit("R1"))


# ---------------------------------------------------------------------------
# VectorX fixture constants (committed binary under src/test/resources/binary/)
# ---------------------------------------------------------------------------

# Elevation shapefile fixtures (BNG EPSG:27700, ~96 k points with Z)
_ELEVATION_DIR = "src/test/resources/binary/elevation"
ELEVATION_DTM_POINT = f"{_ELEVATION_DIR}/sd46_dtm_point.shp"
ELEVATION_DTM_BREAKLINE = f"{_ELEVATION_DIR}/sd46_dtm_breakline.shp"

# WKB POINT Z hex constants (OGC/PostGIS EWKB (Z flag 0x80000000)):
#   geometry type = 0x80000001 (POINTZ, LE) → bytes 01 00 00 80
#   Double = IEEE 754 LE.  10.0 = 0x4024000000000000 BE → 0x0000000000002440 LE
_WKB_POINTZ_0_0_0 = "0101000080000000000000000000000000000000000000000000000000"
_WKB_POINTZ_10_0_0 = "0101000080000000000000244000000000000000000000000000000000"
_WKB_POINTZ_10_10_10 = "0101000080000000000000244000000000000024400000000000002440"
_WKB_POINTZ_0_10_5 = "0101000080000000000000000000000000000024400000000000001440"

# WKB POINT hex constants (2D, no Z):
#   POINT(100, 100) — tile-local coords for MVT examples
_WKB_POINT_100_100 = "010100000000000000000059400000000000005940"
#   POINT(200, 200) — second tile-local point
_WKB_POINT_200_200 = "010100000000000000000069400000000000006940"
#   POINT(13, 42) — WGS84 location (used in CRS examples as WKT)


def elevation_dtm_point_path() -> Path:
    """Absolute path to the committed sd46_dtm_point.shp mass-points shapefile.

    96 k+ POINT Z geometries in BNG (EPSG:27700) with real survey elevations.
    Used as the canonical TIN mass-point fixture.
    """
    repo_root = Path(__file__).parents[4]
    return repo_root / ELEVATION_DTM_POINT


def elevation_dtm_breakline_path() -> Path:
    """Absolute path to the committed sd46_dtm_breakline.shp breaklines shapefile.

    BNG (EPSG:27700) breakline geometries for constrained Delaunay triangulation.
    """
    repo_root = Path(__file__).parents[4]
    return repo_root / ELEVATION_DTM_BREAKLINE


# ---------------------------------------------------------------------------
# VectorX TIN fixture builders
# One row: pts = array<binary> of 4 WKB POINT Z; bl = empty array<binary>.
# The 4 points form a 10×10 square with varying Z (0, 0, 10, 5 m):
#   (0,0,0), (10,0,0), (10,10,10), (0,10,5)  →  2 Delaunay triangles (verified).
# Elevations are genuinely non-flat: range = 10 m.
# ---------------------------------------------------------------------------


def tin_df(spark):
    """Light-tier single-row DataFrame for TIN function examples.

    Columns: ``pts ARRAY<BINARY>`` (4 WKB POINT Z), ``bl ARRAY<BINARY>`` (empty).
    Suitable for ``st_triangulate``, ``st_interpolateelevationbbox``,
    ``st_interpolateelevationgeom`` in the ``constrained`` mode (both tiers).

    The 4 points form a 10×10 m square with elevations 0, 0, 10, 5 m
    (non-degenerate — 2 Delaunay triangles, elevation range = 10 m).
    """
    return spark.sql(f"""
        SELECT array(
            unhex('{_WKB_POINTZ_0_0_0}'),
            unhex('{_WKB_POINTZ_10_0_0}'),
            unhex('{_WKB_POINTZ_10_10_10}'),
            unhex('{_WKB_POINTZ_0_10_5}')
        ) AS pts,
        cast(array() AS array<binary>) AS bl
        """)


def tin_df_heavy(spark):
    """Heavy-tier single-row DataFrame for TIN function examples.

    Same schema and data as ``tin_df``; the heavy-tier column API is used in
    examples but the fixture data itself is tier-agnostic.
    """
    return tin_df(spark)


# ---------------------------------------------------------------------------
# VectorX MVT fixture builders
# Two rows: (z=0, x=0, y=0, geom_wkb BINARY, name STRING, id LONG)
# Geometries are tile-local WKB POINT (pixel space 0..4096 by default).
# ---------------------------------------------------------------------------


def mvt_features_df(spark):
    """Light-tier 2-row DataFrame for MVT function examples.

    Columns: ``z INT``, ``x INT``, ``y INT``, ``geom_wkb BINARY``,
    ``attrs STRUCT<name STRING, id LONG>``.

    Both rows are in the same (z=0, x=0, y=0) tile with tile-local WKB POINT
    geometries: POINT(100, 100) and POINT(200, 200) in pixel coordinates.
    """
    return spark.sql(f"""
        SELECT 0 AS z, 0 AS x, 0 AS y,
               unhex('{_WKB_POINT_100_100}') AS geom_wkb,
               named_struct('name', 'a', 'id', 1L) AS attrs
        UNION ALL
        SELECT 0, 0, 0,
               unhex('{_WKB_POINT_200_200}'),
               named_struct('name', 'b', 'id', 2L)
        """)


def mvt_features_df_heavy(spark):
    """Heavy-tier 2-row DataFrame for MVT function examples.

    Same schema and data as ``mvt_features_df``; heavy tier uses
    ``vectorx.functions`` but the fixture data is tier-agnostic.
    """
    return mvt_features_df(spark)


# ---------------------------------------------------------------------------
# VectorX geometry / CRS fixture builders
# Single row: geom STRING (EWKT) for CRS function examples.
# ---------------------------------------------------------------------------


def geom_ewkt_df(spark):
    """Single-row DataFrame with an EWKT geometry column for CRS examples.

    Column: ``geom STRING`` — ``'SRID=4326;POINT (13 42)'``.
    POINT(13, 42) is in central Italy, inside UTM zone 33N's area of use
    (12°E–18°E), so ``st_transformcrs`` to ``EPSG:32633`` is in-domain.
    """
    return spark.sql("SELECT 'SRID=4326;POINT (13 42)' AS geom")


def geom_ewkt_df_heavy(spark):
    """Heavy-tier single-row DataFrame with EWKT geometry for CRS examples."""
    return geom_ewkt_df(spark)


# ---------------------------------------------------------------------------
# VectorX legacy Mosaic geometry fixture builders
# Single row: geom_legacy STRUCT (InternalGeometry) for st_legacyaswkb.
# ---------------------------------------------------------------------------


def legacy_geom_df(spark):
    """Single-row DataFrame with a legacy Mosaic geometry struct.

    Column: ``geom_legacy STRUCT<typeId INT, srid INT,
    boundaries ARRAY<ARRAY<ARRAY<DOUBLE>>>, holes ARRAY<ARRAY<ARRAY<ARRAY<DOUBLE>>>>>``

    Encodes POINT(13, 42): ``typeId=1`` (POINT), ``srid=0`` (no CRS),
    ``boundaries=[[[13.0, 42.0]]]``, ``holes=[]``.

    ``st_legacyaswkb`` converts this to ISO WKB for
    POINT(13, 42) = ``unhex('01010000000000000000002a400000000000004540')``.
    """
    from pyspark.sql import Row  # noqa: PLC0415
    from pyspark.sql.types import (  # noqa: PLC0415
        ArrayType,
        DoubleType,
        IntegerType,
        StructField,
        StructType,
    )

    legacy_schema = StructType(
        [
            StructField("typeId", IntegerType()),
            StructField("srid", IntegerType()),
            StructField("boundaries", ArrayType(ArrayType(ArrayType(DoubleType())))),
            StructField(
                "holes", ArrayType(ArrayType(ArrayType(ArrayType(DoubleType()))))
            ),
        ]
    )
    schema = StructType([StructField("geom_legacy", legacy_schema)])
    row = Row(geom_legacy=(1, 0, [[[13.0, 42.0]]], []))
    return spark.createDataFrame([row], schema)


def legacy_geom_df_heavy(spark):
    """Heavy-tier single-row DataFrame with a legacy Mosaic geometry struct."""
    return legacy_geom_df(spark)


# ---------------------------------------------------------------------------
# VectorX Setup views
# Maps view name -> builder function. Mirrors the rasterx setup view pattern.
#
# View assignments:
#   tin_survey   — TIN mass-points + breaklines for st_triangulate family
#   mvt_features — tile-local features for st_asmvt / st_asmvt_pyramid
#   vector_geoms — EWKT geometries for st_crs / st_setcrs / st_transformcrs
#   legacy_geoms — legacy Mosaic structs for st_legacyaswkb
# ---------------------------------------------------------------------------

_SETUP_VIEWS_VECTORX_LIGHT = {
    "tin_survey": tin_df,
    "mvt_features": mvt_features_df,
    "vector_geoms": geom_ewkt_df,
    "legacy_geoms": legacy_geom_df,
}
_SETUP_VIEWS_VECTORX_HEAVY = {
    "tin_survey": tin_df_heavy,
    "mvt_features": mvt_features_df_heavy,
    "vector_geoms": geom_ewkt_df_heavy,
    "legacy_geoms": legacy_geom_df_heavy,
}


def create_setup_views_vectorx_light(spark):
    """Create the four light-tier VectorX Setup views. Idempotent (createOrReplace).

    Views: ``tin_survey``, ``mvt_features``, ``vector_geoms``, ``legacy_geoms``.
    Register pyvx *before* calling this so SQL examples can use ``gbx_st_*``.
    """
    for view, builder in _SETUP_VIEWS_VECTORX_LIGHT.items():
        builder(spark).createOrReplaceTempView(view)


def create_setup_views_vectorx_heavy(spark):
    """Create the four heavy-tier VectorX Setup views. Idempotent.

    Views: ``tin_survey``, ``mvt_features``, ``vector_geoms``, ``legacy_geoms``.
    Register vectorx *before* calling this so SQL examples can use ``gbx_st_*``.
    """
    for view, builder in _SETUP_VIEWS_VECTORX_HEAVY.items():
        builder(spark).createOrReplaceTempView(view)


if __name__ == "__main__":
    out = make_multiband_fixture()
    print(f"Written: {out}")
