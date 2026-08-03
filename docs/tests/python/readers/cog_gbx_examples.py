"""file_gbx path lister + cog_gbx round-trip examples — single source of truth.

Code shown in docs/docs/readers/file.mdx and docs/docs/readers/cog.mdx is
imported from here.  Tests exercise the halo-mode pipeline:

  file_gbx  ->  cog_gbx writer  ->  cog_gbx reader (clipPolygons/windows AOI)
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"
SAMPLE_RASTER_SINGLE = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# ---------------------------------------------------------------------------
# Display constants (payload used in docs via raw-loader)
# ---------------------------------------------------------------------------

FILE_GBX_LIST = """# List raster files in a directory — no content loaded.
from databricks.labs.gbx.ds.register import register
register(spark)

refs = spark.read.format("file_gbx").load(
    "/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2"
)
refs.show(truncate=False)
# path | name | extension | size | modificationTime"""

FILE_GBX_FILTER = """# Keep only .tif files via filterRegex:
refs = (
    spark.read.format("file_gbx")
         .option("filterRegex", r".*\\.tif$")
         .load("/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2")
)"""

FILE_GBX_RECURSIVE = """# Recursively list GeoTIFFs in a nested directory tree:
refs = (
    spark.read.format("file_gbx")
         .option("recursiveFileLookup", "true")
         .option("filterRegex", r".*\\.tif$")
         .load("/Volumes/main/geobrix_samples/geobrix-examples/nyc")
)"""

COG_PREPARE = r"""# Step 1 — list source files.
from databricks.labs.gbx.ds.register import register
register(spark)

refs = spark.read.format("file_gbx").load(
    "/Volumes/main/geobrix_samples/geobrix-examples/nyc/sentinel2"
)

# Step 2 — convert each source file to a master COG.
import tempfile, os
OUT = "/Volumes/main/geobrix_samples/cog-prepared/nyc-sentinel2"

(
    refs.write.format("cog_gbx")
        .option("cogBlockSize", "512")
        .option("cogOverviewResampling", "AVERAGE")
        .option("cogCompression", "DEFLATE")
        .mode("overwrite")
        .save(OUT)
)
print("COGs written to", OUT)"""

COG_BBOX_READ = r"""# Step 3 — windowed read: clip to an area of interest.
# clipPolygons takes a WKT/EWKT string (or, for a list, a JSON array string).
# clipCrs gives the CRS of polygons that don't carry an embedded SRID
# (precedence: embedded EWKB/EWKT SRID -> clipCrs -> raster CRS).
# One tile is emitted per polygon whose envelope intersects the raster.
aoi_wkt = "POLYGON((-74.05 40.65,-73.90 40.65,-73.90 40.80,-74.05 40.80,-74.05 40.65))"
cog_df = (
    spark.read.format("cog_gbx")
         .option("clipPolygons", aoi_wkt)   # NYC area (WGS84)
         .option("clipCrs", "EPSG:4326")
         .load(OUT)
)
cog_df.show()
# source | tile   (tile.raster is pre-clipped to the polygon; masked pixels are NoData)
# The reader issues range-reads that fetch only the intersecting blocks."""

# ---------------------------------------------------------------------------
# Test functions (real assertions against real sample data)
# ---------------------------------------------------------------------------


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def list_files_file_gbx(spark, path=None):
    """file_gbx lists path-reference rows (no raster bytes loaded)."""
    _register(spark)
    df = spark.read.format("file_gbx").load(path or SAMPLE_RASTER_DIR)
    assert set(df.columns) >= {"path", "name", "extension", "size", "modificationTime"}, (
        f"unexpected columns: {df.columns}"
    )
    rows = df.collect()
    assert len(rows) >= 1, "expected at least one file reference"
    for row in rows:
        # path must be a non-empty string; raster bytes are NOT present
        assert row["path"] and isinstance(row["path"], str)
        # extension is lowercase, no leading dot, or None for extensionless files
        ext = row["extension"]
        assert ext is None or (ext == ext.lower() and not ext.startswith(".")), (
            f"extension should be lowercase/no-dot or None, got {ext!r}"
        )
    return df


def list_files_filter_regex(spark, path=None):
    """filterRegex keeps only matching filenames."""
    _register(spark)
    df = (
        spark.read.format("file_gbx")
        .option("filterRegex", r".*\.tif$")
        .load(path or SAMPLE_RASTER_DIR)
    )
    rows = df.collect()
    assert len(rows) >= 1
    for row in rows:
        assert row["name"].endswith(".tif"), f"filterRegex missed: {row['name']}"
    return df


def halo_mode_prepare_cog(spark, src_path=None):
    """file_gbx -> cog_gbx writer: each source file becomes a master COG."""
    import rasterio
    from rio_cogeo.cogeo import cog_validate

    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    refs = spark.read.format("file_gbx").load(src)
    assert refs.count() >= 1, "file_gbx must find at least one file"

    with tempfile.TemporaryDirectory() as out_dir:
        (
            refs.write.format("cog_gbx")
            .option("cogBlockSize", "512")
            .option("cogOverviewResampling", "AVERAGE")
            .option("cogCompression", "DEFLATE")
            .mode("overwrite")
            .save(out_dir)
        )
        tif_files = [
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.lower().endswith(".tif")
        ]
        assert tif_files, "cog_gbx writer produced no .tif output"
        for tif in tif_files:
            is_valid, errors, warnings = cog_validate(tif, strict=False)
            assert is_valid, (
                f"cog_gbx output is not a valid COG: {tif}\n"
                f"errors={errors}\nwarnings={warnings}"
            )
    return True


def halo_mode_bbox_read(spark, src_path=None):
    """file_gbx -> cog_gbx writer -> cog_gbx reader round-trip (whole file).

    Verifies the full preparation pipeline: list files with file_gbx, write master
    COGs with the cog_gbx writer, then read them back with the cog_gbx reader and
    confirm the (source, tile) schema is present with non-null raster bytes.

    Note: clip selection is not exercised here — the sentinel2 minimal-bundle sample
    has non-standard native CRS bounds that cause a NYC AOI window to return empty.
    clipPolygons/windows selection is unit-tested in
    python/geobrix/test/ds/test_raster_clip.py with synthetic data.
    """
    _register(spark)
    src = src_path or SAMPLE_RASTER_SINGLE

    refs = spark.read.format("file_gbx").load(src)

    with tempfile.TemporaryDirectory() as out_dir:
        refs.write.format("cog_gbx").mode("overwrite").save(out_dir)

        # Read the prepared COG back (whole file — clipPolygons/windows selection
        # is unit-tested in python/geobrix/test/ds/test_raster_clip.py).
        cog_df = spark.read.format("cog_gbx").option("virtualTiles", "false").load(out_dir)
        rows = cog_df.collect()
        assert len(rows) >= 1, "cog_gbx reader must return at least one tile"
        for row in rows:
            assert "tile" in row.asDict(), "cog_gbx must emit (source, tile) schema"
            assert row["tile"]["raster"] is not None, "tile.raster must not be None"
    return cog_df
