"""Bronze layer: Auto Loader streaming table that inventories staged USGS 3DEP
LiDAR .laz EPT nodes.  Produces the ``laz_inventory`` table consumed by
silver."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from _config import paths  # root_path makes transformations/ importable


@dp.table(
    name="laz_inventory",
    comment="Inventory of staged USGS 3DEP LiDAR .laz EPT nodes (metadata).",
)
def laz_inventory():
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{p['schema_loc']}/laz")
        .option("pathGlobFilter", "*.laz")
        .load(p["laz"])
        .select(
            F.col("path"),
            F.col("length").alias("file_size"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
