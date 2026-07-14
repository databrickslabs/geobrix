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
    StringType,
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

# S2 band filename token: gtiff_gbx writes `{asset_name}_{item_id}.tif`, e.g.
# `B12_S2A_MSIL2A_20230730T172901_...tif`. The date lives in the item_id (_YYYYMMDDT).
_S2_TIF_DATE = re.compile(r"_(\d{8})T\d{6}")

_S2_CELLS_SCHEMA = StructType(
    [
        StructField("h3_cellid", LongType()),
        StructField("observation_date", DateType()),
        StructField("stats", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)

# EMIT plume metadata carries no source-path column; observation_date comes from the
# per-plume "UTC Time Observed" field (ISO8601, e.g. 2024-08-23T17:34:10Z) -> date part.
# An AOI with no JPL plume complex has no *CH4PLMMETA*.json, so read_plumes raises
# FileNotFoundError and emit_plumes materializes as this empty schema (a data condition,
# not a failure) — the pipeline deploys clean and plume_quant/attribution stay empty.
_PLUME_SCHEMA = StructType(
    [
        StructField("plume_id", StringType()),
        StructField("observation_date", DateType()),
        StructField("max_conc_ppmm", DoubleType()),
        StructField("emission_rate_kg_hr", DoubleType()),
        StructField("emission_rate_uncert_kg_hr", DoubleType()),
        StructField("wind_speed_ms", DoubleType()),
        StructField("fetch_length_m", DoubleType()),
        StructField("lon_max", DoubleType()),
        StructField("lat_max", DoubleType()),
        StructField("plume_geom", BinaryType()),
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


def _s2_index_cells(spark, s2_dir, date_tok, s2_h3_res):
    """Port NB02 CELL 10 for one S2 scene date: read B12/B11 SWIR tiles scoped to
    this date's filenames, compute the methane-proxy index (B11-B12)/(B11+B12) via
    rst_mapalgebra (band 1 of each tile binds A=B12, B=B11), tessellate the index
    raster into H3 cells (LATERAL UDTF), and summarize per cell. Tagged with the
    observation_date parsed from the S2 filename token."""
    from databricks.labs.gbx.pyrx import functions as rx

    def _band_tile(band):
        return (
            spark.read.format("gtiff_gbx")
            # filterRegex matches the FULL path -> lead with `.*`; scope to this
            # band AND this scene date so multiple staged scenes stay separated.
            .option("filterRegex", rf".*{band}.*_{date_tok}T\d{{6}}.*\.tif$")
            .load(s2_dir)
            .select(F.col("tile").alias(f"tile_{band.lower()}"))
        )

    b12 = _band_tile("B12")
    b11 = _band_tile("B11")
    mbmp = (
        b12.crossJoin(b11)
        .withColumn("tile", rx.rst_mapalgebra(F.array("tile_b12", "tile_b11"),
                                              "(B - A) / (A + B)"))
        .select("tile")
    )
    obs = datetime.strptime(date_tok, "%Y%m%d").date()
    view = f"_mbmp_{date_tok}"
    mbmp.createOrReplaceTempView(view)
    # gbx_rst_h3_tessellate is a UDTF — invoke via SQL LATERAL; rebuild the tile
    # struct for gbx_rst_summary and cast the summary to a JSON string.
    cells = spark.sql(
        f"""
        SELECT
            t.cellid AS h3_cellid,
            CAST(gbx_rst_summary(named_struct('cellid', t.cellid, 'raster', t.raster,
                                              'metadata', t.metadata)) AS STRING) AS stats
        FROM {view} s, LATERAL gbx_rst_h3_tessellate(s.tile, {s2_h3_res}) t
        """
    )
    return (
        cells.withColumn("observation_date", F.lit(obs))
        .withColumn("_ingested_at", F.current_timestamp())
        .select("h3_cellid", "observation_date", "stats", "_ingested_at")
    )


@dp.materialized_view(
    name="s2_plume_cells",
    comment="Sentinel-2 SWIR methane-proxy index shredded to H3 cells over the top hotspot",
    partition_cols=["observation_date"],
)
def s2_plume_cells():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    s2_dir = p["s2"]

    # Enumerate staged S2 band COGs on the driver (resolvable at flow-analysis); one
    # scene date per land run (least-cloudy item), but grouped by date to stay robust
    # if several scenes accumulate. Empty until an S2 scene is staged (data condition).
    try:
        names = sorted(os.listdir(s2_dir))
    except FileNotFoundError:
        names = []
    dates = set()
    for fn in names:
        if not fn.endswith(".tif"):
            continue
        m = _S2_TIF_DATE.search(fn)
        if m:
            dates.add(m.group(1))
    if not dates:
        return spark.createDataFrame([], _S2_CELLS_SCHEMA)

    frames = [_s2_index_cells(spark, s2_dir, d, c["s2_h3_res"]) for d in sorted(dates)]
    return reduce(lambda a, b: a.unionByName(b), frames)


@dp.materialized_view(
    name="emit_plumes",
    comment="EMIT plume outlines + JPL emission-rate estimates (per overpass)",
    partition_cols=["observation_date"],
)
@dp.expect("rate_nonneg", "emission_rate_kg_hr >= 0")
@dp.expect("has_geom", "plume_geom IS NOT NULL")
def emit_plumes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)

    from databricks.labs.gbx.sample import EmitDownloader
    try:
        plumes = EmitDownloader().read_plumes(p["emit"])
    except FileNotFoundError:
        return spark.createDataFrame([], _PLUME_SCHEMA)

    # observation_date from the per-plume UTC observation time (date part). Robust to
    # the trailing 'Z'/time by slicing the leading yyyy-MM-dd (avoids format edge cases).
    return (
        plumes.withColumn(
            "observation_date",
            F.to_date(F.substring(F.col("utc_observed"), 1, 10), "yyyy-MM-dd"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            "plume_id", "observation_date", "max_conc_ppmm", "emission_rate_kg_hr",
            "emission_rate_uncert_kg_hr", "wind_speed_ms", "fetch_length_m",
            "lon_max", "lat_max", "plume_geom", "_ingested_at",
        )
    )


@dp.materialized_view(
    name="plume_quant",
    comment="Per-plume GeoBrix-clipped CH4 enhancement (mean/max ppm-m) vs JPL rate",
    partition_cols=["observation_date"],
)
@dp.expect("gbx_max_present", "gbx_max_ppmm IS NOT NULL")
def plume_quant():
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.sample import EmitDownloader

    plumes = spark.read.table("emit_plumes")
    enh = EmitDownloader().read_enh(p["emit"])  # (source, tile) per overpass segment

    # Clip each plume against every ENH segment, keep — per plume — the segment with
    # the strongest clipped max (edge-segment slivers lose to the containing segment).
    clipped = (
        plumes.crossJoin(enh.select(F.col("tile").alias("scene")))
        .withColumn("clip", rx.rst_clip("scene", "plume_geom", F.lit(True)))
        .withColumn("summary", rx.rst_summary("clip"))
        .withColumn("gbx_mean_ppmm",
                    F.get_json_object("summary", "$.bands[0].mean").cast("double"))
        .withColumn("gbx_max_ppmm",
                    F.get_json_object("summary", "$.bands[0].max").cast("double"))
        .withColumn(
            "_rnk",
            F.row_number().over(
                Window.partitionBy("plume_id").orderBy(
                    F.col("gbx_max_ppmm").desc_nulls_last())
            ),
        )
        .filter(F.col("_rnk") == 1)
        .drop("_rnk")
    )
    return clipped.select(
        "plume_id", "observation_date", "emission_rate_kg_hr",
        "emission_rate_uncert_kg_hr", "max_conc_ppmm", "wind_speed_ms",
        "fetch_length_m", "gbx_mean_ppmm", "gbx_max_ppmm", "plume_geom",
        F.current_timestamp().alias("_ingested_at"),
    )
