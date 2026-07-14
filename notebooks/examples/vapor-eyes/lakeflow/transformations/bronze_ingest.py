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
