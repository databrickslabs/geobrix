"""Bronze: Auto Loader file inventory per source. Append-only, exactly-once per
staged file. observation_date parsed from the filename; _ingested_at = now."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import paths

# S5P sensing date token in the granule filename: ..._YYYYMMDDT......
_S5P_DATE = r".*_(\d{8})T\d{6}.*"


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


# EMIT / S2 filenames embed the acquisition instant the same way as S5P
# (..._YYYYMMDDT<HHMMSS>...). Confirmed against staged files with `databricks fs
# ls` on the vapor-eyes-lf subtree (see Phase-2 validation).
_YMD_TOKEN = r".*_(\d{8})T\d{6}.*"


@dp.table(name="s5p_granules", comment="Staged Sentinel-5P CH4 granule inventory")
def s5p_granules():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["s5p"], f"{p['schema_loc']}/s5p", "*.nc")
    return df.withColumn(
        "observation_date",
        F.to_date(F.regexp_extract("source_file", _S5P_DATE, 1), "yyyyMMdd"),
    )


@dp.table(name="emit_scenes", comment="Staged EMIT L2B CH4 product inventory")
def emit_scenes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["emit"], f"{p['schema_loc']}/emit", "*")
    return df.withColumn(
        "observation_date",
        F.to_date(F.regexp_extract("source_file", _YMD_TOKEN, 1), "yyyyMMdd"),
    )


@dp.table(name="wells_raw", comment="Staged TX RRC WellSHL snapshot inventory")
def wells_raw():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    # A wells snapshot carries no acquisition date; observation_date = ingest date.
    return (
        _autoload(spark, p["wells"], f"{p['schema_loc']}/wells", "*.geojson")
        .withColumn("observation_date", F.to_date(F.col("_ingested_at")))
    )


@dp.table(name="s2_swir_assets", comment="Staged Sentinel-2 B11/B12 SWIR COG inventory")
def s2_swir_assets():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    # S2 SWIR COGs are downloaded in Phase 3; empty for now but must deploy clean.
    return (
        _autoload(spark, p["s2"], f"{p['schema_loc']}/s2", "*.tif")
        .withColumn(
            "observation_date",
            F.to_date(F.regexp_extract("source_file", _YMD_TOKEN, 1), "yyyyMMdd"),
        )
    )
