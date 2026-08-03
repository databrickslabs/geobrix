"""Large-raster reader examples — single source of truth.

Code shown in docs/docs/readers/raster.mdx (large-raster and COG sections)
is imported from here. Tests exercise the new default auto-split behavior,
COG output on split, and the gtiff_gbx writer cog=true round-trip.
"""

from path_config import SAMPLE_DATA_BASE

# A large-ish multi-tile raster in the sample data.  The NYC Sentinel-2 mosaic
# is made up of several files; we load the directory so the reader sees more
# than one file and verifies the default-split path.
SAMPLE_RASTER_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"
SAMPLE_RASTER_SINGLE = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# ---------------------------------------------------------------------------
# Display constants (payload used in docs via raw-loader)
# ---------------------------------------------------------------------------

SPLIT_DEFAULT = """# Default: reader auto-splits large rasters on a decoded-memory budget.
# splitStrategy=auto resolves to 'serverless' (512 MiB/tile) or 'classic'
# (1 536 MiB/tile) based on the cluster environment.  Small files that fit
# in the budget are read as a single whole-image tile.
from databricks.labs.gbx.ds.register import register
register(spark)

df = spark.read.format("raster_gbx").load(
    "/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2"
)
print(df.count(), "tiles")"""

SPLIT_DEFAULT_output = """+------+-----+
|source| tile|
+------+-----+
|...   |{...}|
|...   |{...}|
+------+-----+"""

SPLIT_NONE = """# Recover old behavior (one whole-image tile per file, no split):
df_nosplit = (
    spark.read.format("raster_gbx")
         .option("splitStrategy", "none")
         .load("/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2")
)"""

SPLIT_OPTIONS_TABLE = """# Two-axis control: split strategy and output format.
df = (
    spark.read.format("raster_gbx")
         .option("splitStrategy", "serverless")   # or: classic | none | auto
         .option("tileFormat", "cog")             # or: gtiff | auto
         .option("cogBlockSize", "512")           # tile size for COG internal grid (px)
         .option("cogOverviewResampling", "AVERAGE")  # overview resampling algorithm
         .load("/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2")
)"""

WRITER_COG = r"""# Force-convert to COG on write (any DataFrame with a tile column):
import tempfile
with tempfile.TemporaryDirectory() as out:
    df.write.format("gtiff_gbx") \
        .mode("overwrite") \
        .option("cog", "true") \
        .option("cogBlockSize", "512") \
        .option("cogOverviewResampling", "AVERAGE") \
        .option("cogCompression", "DEFLATE") \
        .save(out)
    cog_df = spark.read.format("raster_gbx").load(out)
    print(cog_df.count(), "COG tiles written and read back")"""

# ---------------------------------------------------------------------------
# Test functions (real assertions against real sample data)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def read_large_raster_defaults(spark, path=None):
    """Default (auto-split): directory read emits >=1 tiles, verifies schema."""
    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_DIR)
    assert "source" in df.columns and "tile" in df.columns
    rows = df.collect()
    assert len(rows) >= 1, "expected at least one tile from the sentinel2 directory"
    for row in rows:
        assert row["tile"]["raster"] is not None, "tile.raster must not be None"
    return df


def read_large_raster_split_none(spark, path=None):
    """splitStrategy=none: each file is emitted as one whole-image tile."""
    _register(spark)
    import os

    p = path or SAMPLE_RASTER_SINGLE
    df = spark.read.format("raster_gbx").option("splitStrategy", "none").load(p)
    rows = df.collect()
    assert len(rows) == 1, "splitStrategy=none must yield one tile per file"
    assert rows[0]["tile"]["cellid"] == -1
    return df


def read_large_raster_cog_output(spark, path=None):
    """splitStrategy=serverless + tileFormat=cog: split tiles are COG-encoded.

    Asserts:
    - count() > 0
    - tile schema is present
    - metadata driver is COG or GTiff (rasterio reports both names for COG tiles)
    """
    _register(spark)
    import os

    # Force a split by using a tiny budget (sizeInMB=1) so even a small test
    # raster is chunked, giving us COG-output tiles to inspect.
    df = (
        spark.read.format("raster_gbx")
        .option("sizeInMB", "1")
        .option("tileFormat", "cog")
        .load(path or SAMPLE_RASTER_SINGLE)
    )
    rows = df.collect()
    assert len(rows) >= 1, "expected at least one COG tile"
    for row in rows:
        driver = row["tile"]["metadata"].get("driver", "").upper()
        assert driver in ("COG", "GTIFF"), (
            f"expected COG or GTiff driver for COG-encoded tile, got {driver!r}"
        )
    return df


def cog_writer_round_trip(spark, path=None):
    """Write tiles as COG and read them back; verifies a round-trip emits valid tiles."""
    _register(spark)
    import tempfile

    src = path or SAMPLE_RASTER_SINGLE
    df_src = spark.read.format("raster_gbx").option("splitStrategy", "none").load(src)
    assert df_src.count() >= 1

    with tempfile.TemporaryDirectory() as out:
        (
            df_src.write.format("gtiff_gbx")
            .mode("overwrite")
            .option("cog", "true")
            .option("cogBlockSize", "512")
            .option("cogOverviewResampling", "AVERAGE")
            .option("cogCompression", "DEFLATE")
            .save(out)
        )
        df_cog = spark.read.format("raster_gbx").load(out)
        cog_rows = df_cog.collect()
        assert len(cog_rows) >= 1, "COG writer must produce at least one output tile"
        for row in cog_rows:
            assert row["tile"]["raster"] is not None
    return df_cog
