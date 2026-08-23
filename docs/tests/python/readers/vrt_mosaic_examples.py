"""VRT-mosaic doc examples — single source of truth for docs/docs/api/vrt-mosaic.mdx.

Code shown on the VRT Mosaics page is imported from here via raw-loader. Tests
exercise the mosaic lifecycle end to end against real sample data:

  file_gbx  ->  cog_gbx writer (vrtMosaic=true)  ->  mosaic.vrt
  mosaic.vrt  ->  raster_gbx reader  ->  one virtual tile row per member
  member tiles  ->  mint_vrt  ->  windowed rasterio read

Light tier (pure Python, no JAR): readers register via ds.register.register,
never rasterx.register.
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_SINGLE = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# ---------------------------------------------------------------------------
# Display constants (payload rendered in docs via raw-loader)
# ---------------------------------------------------------------------------

VRT_PREPARE = r"""from databricks.labs.gbx.ds.register import register
register(spark)

# file_gbx gives one path-reference row per source file
sources = spark.read.format("file_gbx").load("/Volumes/catalog/schema/volume/raw/")

(
    sources
    .write.format("cog_gbx")
    .option("vrtMosaic", "true")   # activate mosaic mode
    .option("tileSize", "1024")    # tile edge in pixels (default 1024)
    .mode("overwrite")
    .save("/Volumes/catalog/schema/volume/mosaic/")
)"""

VRT_READ_EXPAND = r"""from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx.functions import rst_avg
from pyspark.sql.functions import col

register(spark)

# Load the VRT: one whole-file virtual tile per member mini-COG
df = spark.read.format("raster_gbx").load("/Volumes/catalog/schema/vol/mosaic/mosaic.vrt")

# Apply rst_* per tile — exactly as you would for a directory of flat files
result = df.select(
    col("tile.path").alias("member"),
    rst_avg(col("tile")).alias("avg"),
)"""

VRT_MINT = r"""from databricks.labs.gbx.ds._mosaic import mint_vrt

# Build a transient VRT over an explicit tile list
tile_paths = [
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_0_0.tif",
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_0_1.tif",
    "/Volumes/catalog/schema/vol/mosaic/tile_abc_1_0.tif",
]
vrt_path = mint_vrt(tile_paths)

# Open the VRT with rasterio for a windowed read across the mosaic
import rasterio
from rasterio.windows import Window

viewport = Window(col_off=400, row_off=200, width=600, height=400)
with rasterio.open(vrt_path) as vrt_ds:
    data = vrt_ds.read(window=viewport)
    # Only the tiles that intersect the viewport are read"""

# ---------------------------------------------------------------------------
# Test functions (real assertions against real sample data)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def _write_mosaic(spark, src, out_dir, tile_size=128, prune_empty=None):
    """Write a VRT mosaic and return (sorted member tile paths, mosaic.vrt path)."""
    writer = (
        spark.read.format("file_gbx")
        .load(src)
        .write.format("cog_gbx")
        .option("vrtMosaic", "true")
        .option("tileSize", str(tile_size))
    )
    if prune_empty is not None:
        writer = writer.option("pruneEmpty", "true" if prune_empty else "false")
    writer.mode("overwrite").save(out_dir)

    tiles = sorted(
        os.path.join(out_dir, f)
        for f in os.listdir(out_dir)
        if f.startswith("tile_") and f.lower().endswith(".tif")
    )
    return tiles, os.path.join(out_dir, "mosaic.vrt")


def vrt_prepare(spark, src_path=None):
    """cog_gbx mosaic mode splits a source into mini-COGs + a mosaic.vrt index."""
    import rasterio

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    refs = spark.read.format("file_gbx").load(src)
    assert refs.count() >= 1, "file_gbx must find the source raster"

    with tempfile.TemporaryDirectory() as out_dir:
        # A small tileSize forces the single sample raster to split into a grid
        # of mini-COGs; pruneEmpty=false keeps every cell so the VRT
        # reconstitutes the full source extent.
        tiles, mosaic_vrt = _write_mosaic(
            spark, src, out_dir, tile_size=128, prune_empty=False
        )

        assert os.path.exists(mosaic_vrt), "mosaic mode must write mosaic.vrt"
        assert len(tiles) > 1, (
            f"tileSize=128 over the sample raster must yield >1 mini-COG, got {len(tiles)}"
        )

        with rasterio.open(src) as src_ds:
            src_w, src_h, src_crs = src_ds.width, src_ds.height, src_ds.crs
        with rasterio.open(mosaic_vrt) as vrt_ds:
            # The index opens in plain GDAL/rasterio and covers the full source.
            assert abs(vrt_ds.width - src_w) <= 1, (vrt_ds.width, src_w)
            assert abs(vrt_ds.height - src_h) <= 1, (vrt_ds.height, src_h)
            assert vrt_ds.crs == src_crs, (vrt_ds.crs, src_crs)
    return True


def vrt_read_expand(spark, src_path=None):
    """raster_gbx expands mosaic.vrt into one virtual tile row per member mini-COG."""
    from pyspark.sql.functions import col

    from databricks.labs.gbx.pyrx.functions import rst_avg

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        tiles, mosaic_vrt = _write_mosaic(spark, src, out_dir, tile_size=128)
        assert len(tiles) > 1, "expected a multi-member mosaic to expand"

        df = spark.read.format("raster_gbx").load(mosaic_vrt)
        # One row per member mini-COG.
        assert df.count() == len(tiles), (df.count(), len(tiles))

        # rst_* run per tile; each retained member has data, so avg is non-null.
        result = df.select(
            col("tile.path").alias("member"),
            rst_avg(col("tile")).alias("avg"),
        ).collect()
        assert len(result) == len(tiles)
        for row in result:
            assert row["member"] is not None, "member path must be set on a virtual tile"
            assert row["avg"] is not None, f"rst_avg returned null for {row['member']}"
    return df


def vrt_mint_windowed(spark, src_path=None):
    """mint_vrt builds a transient VRT over member tiles for a windowed rasterio read."""
    import rasterio
    from rasterio.windows import Window

    from databricks.labs.gbx.ds._mosaic import mint_vrt, minted_vrt

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    with tempfile.TemporaryDirectory() as out_dir:
        tiles, _ = _write_mosaic(spark, src, out_dir, tile_size=128)
        assert len(tiles) > 1, "expected >1 member tile to mint a VRT over"

        # Primary example: mint a transient VRT over an explicit tile list.
        vrt_path = mint_vrt(tiles)
        try:
            with rasterio.open(vrt_path) as vrt_ds:
                bands, vrt_w, vrt_h = vrt_ds.count, vrt_ds.width, vrt_ds.height
                win_w, win_h = min(64, vrt_w), min(64, vrt_h)
                window = Window(col_off=0, row_off=0, width=win_w, height=win_h)
                data = vrt_ds.read(window=window)
            # A windowed read returns exactly (bands, height, width).
            assert data.shape == (bands, win_h, win_w), data.shape
        finally:
            # mint_vrt leaves the temp dir to the caller; clean it up.
            import shutil

            shutil.rmtree(os.path.dirname(vrt_path), ignore_errors=True)

        # Context-manager form auto-cleans the transient VRT on exit.
        with minted_vrt(tiles) as ctx_vrt:
            assert os.path.exists(ctx_vrt)
            ctx_dir = os.path.dirname(ctx_vrt)
        assert not os.path.exists(ctx_dir), "minted_vrt must remove its temp dir on exit"

    return True
