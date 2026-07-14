"""Silver: the methane cascade, partitioned by observation_date.

Resolves the plan's reader-to-incremental unknown against the live pipeline:

* Source-path column (Task 1.5 Step 1): the netcdf_gbx light reader's vector-mode
  schema is [<variables...>, geom_0, geom_0_srid, geom_0_srid_proj] — it exposes
  NO source-path/filename column. So Branch 2a (join reader points to the bronze
  inventory on a path) is NOT viable. We take Branch 2b: read each staged granule
  scoped by its exact filename via the reader's `filterRegex` and tag its points
  with the observation_date parsed from that filename. We enumerate the staged
  granules with a driver-side os.listdir of the landing dir (the land task stages
  them before this pipeline runs) rather than a collect() on the bronze table —
  bronze is not materialized during flow analysis, so collecting it there yields an
  empty list and the flow fails to resolve. The landing dir is the same source
  bronze inventories, so the two layers stay consistent.

* Streaming vs materialized (Task 1.5 unknown #2): the reader is a batch
  (`spark.read`) Python DataSource, not a streaming source, so a streaming
  `@dp.table` cannot wrap it. This dataset is a `@dp.materialized_view` that
  recomputes from the landing dir while retaining observation_date per row
  (partitioned by observation_date).
"""
import os
import re
from datetime import datetime
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    DateType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.databricks.sql import functions as DBF

from _config import cfg, paths, register_gbx

_S5P_GROUP = "/PRODUCT"
_S5P_VARIABLES = "methane_mixing_ratio_bias_corrected,qa_value"
# S5P granule filename sensing-date token: ..._YYYYMMDDT......
_FILE_DATE = re.compile(r"_(\d{8})T\d{6}")

_HOTSPOT_SCHEMA = StructType(
    [
        StructField("h3_cellid", LongType()),
        StructField("observation_date", DateType()),
        StructField("ch4_mean", DoubleType()),
        StructField("ch4_max", DoubleType()),
        StructField("n_obs", LongType()),
        StructField("geom_wkb", BinaryType()),
        StructField("_ingested_at", TimestampType()),
    ]
)


def _read_s5p_file(spark, s5p_dir, filename, obs_date):
    """Read one staged granule (scoped by its exact filename) and tag it with the
    observation_date parsed from that filename."""
    # filterRegex is matched against the FULL path, so anchor with a leading `.*`
    # to absorb the /Volumes/.../s5p/ prefix (same convention as the gtiff_gbx reader).
    return (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("group", _S5P_GROUP)
        .option("variables", _S5P_VARIABLES)
        .option("filterRegex", r".*" + re.escape(filename) + r"$")
        .load(s5p_dir)
        .withColumn("observation_date", F.lit(obs_date))
    )


@dp.materialized_view(
    name="s5p_hotspots",
    comment="Per-H3-cell CH4 mean/max per overpass (S5P screening surface)",
    partition_cols=["observation_date"],
)
@dp.expect("has_obs", "n_obs > 0")
@dp.expect("ch4_present", "ch4_mean IS NOT NULL")
def s5p_hotspots():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    minx, miny, maxx, maxy = c["bbox"]
    s5p_dir = p["s5p"]

    # Enumerate staged granules on the driver; read each one filename-scoped and tag
    # with the date from its filename (Branch 2b). Resolvable at flow-analysis time.
    try:
        names = sorted(os.listdir(s5p_dir))
    except FileNotFoundError:
        names = []
    frames = []
    for fn in names:
        if not fn.endswith(".nc"):
            continue
        m = _FILE_DATE.search(fn)
        if not m:
            continue
        obs = datetime.strptime(m.group(1), "%Y%m%d").date()
        frames.append(_read_s5p_file(spark, s5p_dir, fn, obs))
    if not frames:
        return spark.createDataFrame([], _HOTSPOT_SCHEMA)

    pts = reduce(lambda a, b: a.unionByName(b), frames)
    pts = pts.filter(
        (F.col("qa_value") >= c["qa_min"])
        & F.col("methane_mixing_ratio_bias_corrected").isNotNull()
    )
    pts = (
        pts.withColumn("_g", DBF.st_geomfromwkb(F.col("geom_0")))
        .withColumn("lon", DBF.st_x("_g")).withColumn("lat", DBF.st_y("_g"))
        .filter((F.col("lon") >= minx) & (F.col("lon") <= maxx))
        .filter((F.col("lat") >= miny) & (F.col("lat") <= maxy))
        .withColumn("h3_cellid", DBF.h3_longlatash3("lon", "lat", F.lit(c["h3_res"])))
    )
    return (
        pts.groupBy("h3_cellid", "observation_date")
        .agg(
            F.mean("methane_mixing_ratio_bias_corrected").alias("ch4_mean"),
            F.max("methane_mixing_ratio_bias_corrected").alias("ch4_max"),
            F.count("*").alias("n_obs"),
        )
        .withColumn("geom_wkb", DBF.h3_centeraswkb("h3_cellid"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
