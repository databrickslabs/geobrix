"""Bronze: Auto Loader file inventory per source. Append-only, exactly-once per
staged file.

These are METADATA / MANIFEST tables: each row is a pointer (`path`) to a payload
file staged on the Volume, plus domain fields hoisted out of the filename
(`observation_date`, `item_id`/`granule_id`/`band`/`product_type`). Silver reads
FROM these tables (join on `path`/`source_file`/`observation_date`) so the real
bronze->silver dependency edges appear in the pipeline DAG and the hoisted fields
have a single source of truth. `_ingested_at` = now.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import paths

# Sensing-date token embedded in the granule filename: ..._YYYYMMDDT......
_S5P_DATE = r".*_(\d{8})T\d{6}.*"

# EMIT filenames embed one acquisition instant (..._YYYYMMDDT<HHMMSS>...).
_YMD_TOKEN = r".*_(\d{8})T\d{6}.*"
# The overpass acquisition id shared across EMIT product types (ENH/PLM/PLMMETA/...):
# the YYYYMMDDTHHMMSS token, e.g. 20230731T191810.
_EMIT_GRANULE = r"_(\d{8}T\d{6})"
# EMIT product type sits between `EMIT_L2B_CH4` and the `_002` version token, e.g.
# CH4ENH / CH4PLM / CH4PLMMETA / CH4SENS / CH4UNCERT. Match PLMMETA before PLM.
_EMIT_PRODUCT = r"EMIT_L2B_CH4(PLMMETA|PLM|ENH|SENS|UNCERT)"
# Sentinel-2 filenames carry TWO tokens — the datatake SENSING start (first) and the
# processing-baseline stamp (last), e.g. B12_S2A_MSIL2A_20230813T172901_..._20240913T093541.tif.
# regexp_extract with a leading greedy `.*_` would capture the LAST (processing) token;
# an un-anchored pattern captures the FIRST (sensing) match — the true observation date,
# and the token the silver s2_plume_cells date is parsed from (keeps the layers consistent).
_S2_DATE_TOKEN = r"_(\d{8})T\d{6}"
# Sentinel-2 SWIR band prefix (B11 / B12) and the product item id after it.
_S2_BAND = r"^(B1[12])_"
_S2_ITEM = r"^B1[12]_(.+)\.tif$"


def _autoload(spark, src_dir, schema_loc, glob):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("pathGlobFilter", glob)
        .load(src_dir)
        .select(
            F.col("path"),
            F.col("length").alias("file_size"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="s5p_granules", comment="Staged Sentinel-5P CH4 granule inventory (metadata)")
def s5p_granules():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["s5p"], f"{p['schema_loc']}/s5p", "*.nc")
    return df.select(
        F.col("path"),
        F.col("source_file"),
        # granule id stem = filename without the .nc extension.
        F.regexp_extract("source_file", r"(.+)\.nc$", 1).alias("item_id"),
        F.to_date(F.regexp_extract("source_file", _S5P_DATE, 1), "yyyyMMdd").alias(
            "observation_date"
        ),
        F.col("file_size"),
        F.col("_ingested_at"),
    )


@dp.table(name="emit_scenes", comment="Staged EMIT L2B CH4 product inventory (metadata)")
def emit_scenes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["emit"], f"{p['schema_loc']}/emit", "*")
    return df.select(
        F.col("path"),
        F.col("source_file"),
        F.regexp_extract("source_file", _EMIT_PRODUCT, 1).alias("product_type"),
        F.regexp_extract("source_file", _EMIT_GRANULE, 1).alias("granule_id"),
        F.to_date(F.regexp_extract("source_file", _YMD_TOKEN, 1), "yyyyMMdd").alias(
            "observation_date"
        ),
        F.col("file_size"),
        F.col("_ingested_at"),
    )


@dp.table(name="wells_raw", comment="Staged TX RRC WellSHL snapshot inventory (metadata)")
def wells_raw():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    # A wells snapshot carries no acquisition date; observation_date = ingest date.
    df = _autoload(spark, p["wells"], f"{p['schema_loc']}/wells", "*.geojson")
    return df.select(
        F.col("path"),
        F.col("source_file"),
        F.to_date(F.col("_ingested_at")).alias("observation_date"),
        F.col("file_size"),
        F.col("_ingested_at"),
    )


@dp.table(
    name="cm_scenes",
    comment="Carbon Mapper annotated CH4 plume detections (one row per plume record)",
)
def cm_scenes():
    """Carbon Mapper rated-plume records, streamed from the JSONL the land task writes
    under cm/. Unlike the other bronze tables these are DATA rows (not file pointers):
    one row per plume detection, with the flat API fields, an `observation_date`
    derived from `scene_timestamp`, `_ingested_at`, and the source `path`.
    `geometry_json` arrives as a JSON STRING (the land task stringifies the nested
    GeoJSON geometry), which silver feeds straight to st_geomfromgeojson."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{p['schema_loc']}/cm")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(p["cm"])
    )
    return df.select(
        F.col("id"),
        F.col("plume_id"),
        F.col("gas"),
        F.col("geometry_json"),
        F.col("scene_id"),
        F.col("scene_timestamp"),
        F.col("instrument"),
        F.col("platform"),
        F.col("emission_auto"),
        F.col("emission_uncertainty_auto"),
        F.col("sector"),
        F.col("plume_quality"),
        F.col("wind_speed_avg_auto"),
        F.col("wind_direction_avg_auto"),
        F.col("is_offshore"),
        F.col("published_at"),
        F.to_date(F.to_timestamp(F.col("scene_timestamp"))).alias("observation_date"),
        F.col("_metadata.file_path").alias("path"),
        F.current_timestamp().alias("_ingested_at"),
    )


@dp.table(name="s2_swir_assets", comment="Staged Sentinel-2 B11/B12 SWIR COG inventory (metadata)")
def s2_swir_assets():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    # S2 SWIR COGs are downloaded in Phase 3; empty until a scene lands but must deploy clean.
    df = _autoload(spark, p["s2"], f"{p['schema_loc']}/s2", "*.tif")
    return df.select(
        F.col("path"),
        F.col("source_file"),
        F.regexp_extract("source_file", _S2_BAND, 1).alias("band"),
        F.regexp_extract("source_file", _S2_ITEM, 1).alias("item_id"),
        F.to_date(F.regexp_extract("source_file", _S2_DATE_TOKEN, 1), "yyyyMMdd").alias(
            "observation_date"
        ),
        F.col("file_size"),
        F.col("_ingested_at"),
    )
