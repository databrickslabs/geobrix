"""
Shared test fixture generators for API documentation tests.

The committed .tif/.nc artifacts in src/test/resources/binary/ are the
canonical fixtures; the generator functions here document how they were
produced and can be re-run if a file needs to be regenerated.

NOTE: sample-data/Volumes/main/default/geobrix_samples/ is gitignored
(see .gitignore lines 40-41), so committed test fixtures live under
src/test/resources/binary/ instead.

Fixture paths
-------------
SINGLE_BAND   nyc_sentinel2_red.tif — sourced from the sample-data /Volumes mount
               via path_config.SAMPLE_DATA_BASE; resolves to
               /Volumes/main/default/test-data/geobrix-examples/nyc/sentinel2/
               nyc_sentinel2_red.tif in the geobrix-dev container.
MULTIBAND     rgb_nir_small.tif — committed under
               src/test/resources/binary/geotiff-small/ (3 bands: red/NIR/green,
               8x8 pixels, EPSG:4326, per-band metadata tags).
DEM           srtm_n40w073.tif — sourced from the sample-data /Volumes mount
               via path_config.SAMPLE_DATA_BASE; resolves to
               /Volumes/main/default/test-data/geobrix-examples/nyc/elevation/
               srtm_n40w073.tif in the geobrix-dev container.
NETCDF        prAdjust_day_HadGEM2-CC_*.nc — committed under
               src/test/resources/binary/netcdf-CMIP5/; has two subdatasets
               (time_bnds, prAdjust) and is used by rst_subdatasets /
               rst_getsubdataset examples.
"""

from pathlib import Path
import sys
import os

# ---------------------------------------------------------------------------
# Path constants (relative paths are relative to repo root)
# ---------------------------------------------------------------------------

# Repo-root-relative paths for committed fixtures
MULTIBAND = "src/test/resources/binary/geotiff-small/rgb_nir_small.tif"

# Long filename committed under netcdf-CMIP5/
_NETCDF_FILENAME = (
    "prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc"
    "_rcp45_r1i1p1_20201201-20201231.nc"
)
NETCDF = f"src/test/resources/binary/netcdf-CMIP5/{_NETCDF_FILENAME}"

# Single-band and DEM are sourced from the /Volumes mount (available in the
# geobrix-dev container when started with sample-data volumes).
# We compute these lazily from path_config so tests that don't need them
# don't import path_config at module load time.
_SINGLE_BAND: str | None = None
_DEM: str | None = None


def _sample_data_base() -> str:
    """Return SAMPLE_DATA_BASE from path_config (lazy import)."""
    try:
        from path_config import SAMPLE_DATA_BASE  # noqa: PLC0415
        return SAMPLE_DATA_BASE
    except ImportError:
        # Fallback for environments where path_config is not on sys.path
        root = os.environ.get("GBX_SAMPLE_DATA_ROOT", "/Volumes/main/default/test-data")
        return f"{root.rstrip('/')}/geobrix-examples"


def single_band_path() -> str:
    """Absolute path to the canonical single-band GeoTIFF."""
    return f"{_sample_data_base()}/nyc/sentinel2/nyc_sentinel2_red.tif"


def dem_path() -> str:
    """Absolute path to the canonical DEM GeoTIFF."""
    return f"{_sample_data_base()}/nyc/elevation/srtm_n40w073.tif"


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

    path = single_band_path()
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
    Light-tier (pyrx) one-row DataFrame with `tile` from srtm_n40w073.tif.

    Single-band DEM raster (SRTM elevation, NYC area).
    Used by terrain function examples (slope, aspect, hillshade, …).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    path = dem_path()
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

    path = single_band_path()
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
    Heavy-tier (rasterx) one-row DataFrame with `tile` from srtm_n40w073.tif.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    path = dem_path()
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


if __name__ == "__main__":
    out = make_multiband_fixture()
    print(f"Written: {out}")
