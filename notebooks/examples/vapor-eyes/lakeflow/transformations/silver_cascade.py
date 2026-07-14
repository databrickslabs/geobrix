"""Silver: the methane cascade, partitioned by observation_date.

WIRED TO BRONZE METADATA TABLES. Each silver MV reads FROM its bronze inventory
table so a real bronze->silver dependency edge is inferred by Lakeflow (the DAG
edge is inferred only when the MV's RETURNED DataFrame plan references the
upstream table via `spark.read.table(...)`):

* s5p_granules  -> s5p_hotspots
* emit_scenes   -> emit_plumes AND -> plume_quant
* s2_swir_assets-> s2_plume_cells
* wells_raw     -> wells_shl  (already wired via the SCD2 change feed)

Reader mechanics (unchanged from the per-file resolution): the netcdf_gbx and
gtiff_gbx light readers expose NO source-path column, so a pure lazy join from the
reader's rows to the bronze inventory is not possible. We therefore enumerate the
staged payload files (driver-side os.listdir — the files are physically staged by
the land task before this pipeline runs, so they are visible at flow-analysis time,
whereas an eager collect() of a sibling pipeline table at analysis is not reliable),
read each file content-scoped by its exact basename via the reader's `filterRegex`,
tag it with its `source_file`, and then JOIN the union to `spark.read.table(<bronze>)`.
The join is a LAZY plan reference resolved at EXECUTION (after the bronze table has
materialized in this update), so it both (a) places the bronze table in the returned
plan's lineage => the edge is inferred, and (b) sources `observation_date` (and other
hoisted fields) FROM the bronze table — single source of truth, not re-parsed here.

Every empty branch still returns a plan BUILT OFF the bronze table (via `_empty_ref`)
so the dependency edge survives even when no payload files are staged.

* Streaming vs materialized: the readers are batch (`spark.read`) Python DataSources,
  not streaming sources, so these are `@dp.materialized_view`s (partitioned by
  observation_date), not streaming `@dp.table`s.
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

_QUANT_SCHEMA = StructType(
    [
        StructField("plume_id", StringType()),
        StructField("observation_date", DateType()),
        StructField("emission_rate_kg_hr", DoubleType()),
        StructField("emission_rate_uncert_kg_hr", DoubleType()),
        StructField("max_conc_ppmm", DoubleType()),
        StructField("wind_speed_ms", DoubleType()),
        StructField("fetch_length_m", DoubleType()),
        StructField("gbx_mean_ppmm", DoubleType()),
        StructField("gbx_max_ppmm", DoubleType()),
        StructField("plume_geom", BinaryType()),
        StructField("_ingested_at", TimestampType()),
    ]
)


def _empty_ref(src_df, schema):
    """An empty DataFrame shaped to `schema` but BUILT OFF `src_df` so the returned
    plan still references src_df's underlying table. This keeps the DLT dependency
    edge alive even when no payload files are staged (the empty/analysis case)."""
    cols = [F.lit(None).cast(f.dataType).alias(f.name) for f in schema.fields]
    return src_df.select(*cols).where(F.lit(False))


def _read_s5p_file(spark, s5p_dir, filename):
    """Read one staged granule scoped by its exact filename. observation_date is NOT
    tagged here — it is joined in from the s5p_granules bronze table."""
    # filterRegex is matched against the FULL path, so anchor with a leading `.*`
    # to absorb the /Volumes/.../s5p/ prefix (same convention as the gtiff_gbx reader).
    return (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .option("group", _S5P_GROUP)
        .option("variables", _S5P_VARIABLES)
        .option("filterRegex", r".*" + re.escape(filename) + r"$")
        .load(s5p_dir)
        .withColumn("source_file", F.lit(filename))
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

    bronze = spark.read.table("s5p_granules")

    # Enumerate staged granules (driver-side, visible at flow-analysis); read each one
    # filename-scoped and tag with its basename. The bronze JOIN below supplies the
    # date + the dependency edge.
    try:
        names = sorted(os.listdir(s5p_dir))
    except FileNotFoundError:
        names = []
    frames = [
        _read_s5p_file(spark, s5p_dir, fn) for fn in names if fn.endswith(".nc")
    ]
    if not frames:
        return _empty_ref(bronze, _HOTSPOT_SCHEMA)

    pts = reduce(lambda a, b: a.unionByName(b), frames)
    # JOIN to bronze -> observation_date (single source of truth) + inferred edge.
    pts = pts.join(bronze.select("source_file", "observation_date"), "source_file")
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

    bronze = spark.read.table("s2_swir_assets")

    # Enumerate staged S2 band COGs (driver-side, visible at flow-analysis) to derive
    # the scene dates that drive the readers; the bronze JOIN below supplies the edge.
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
        return _empty_ref(bronze, _S2_CELLS_SCHEMA)

    frames = [_s2_index_cells(spark, s2_dir, d, c["s2_h3_res"]) for d in sorted(dates)]
    cells = reduce(lambda a, b: a.unionByName(b), frames)
    # JOIN to bronze distinct scene dates -> inferred edge (1:1 on the same dates).
    return cells.join(
        bronze.select("observation_date").distinct(), "observation_date"
    )


@dp.materialized_view(
    name="emit_plumes",
    comment="EMIT plume outlines + JPL emission-rate estimates (per overpass)",
    partition_cols=["observation_date"],
)
# Concentration is the reliable intensity metric (this pipeline's headline);
# JPL emission_rate is legitimately NULL/NA when no wind data exists, so it is
# only checked non-negative WHEN PRESENT — never treat an absent rate as a
# defect (that would flag ~75% of real plumes).
@dp.expect("conc_present", "max_conc_ppmm > 0")
@dp.expect("rate_valid_when_present", "emission_rate_kg_hr IS NULL OR emission_rate_kg_hr >= 0")
@dp.expect("has_geom", "plume_geom IS NOT NULL")
def emit_plumes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)

    from databricks.labs.gbx.sample import EmitDownloader

    # The CH4PLMMETA rows of the bronze inventory both drive this MV and carry the edge.
    meta_dates = (
        spark.read.table("emit_scenes")
        .filter(F.col("product_type") == "PLMMETA")
        .select("observation_date")
        .distinct()
    )
    try:
        plumes = EmitDownloader().read_plumes(p["emit"])
    except FileNotFoundError:
        return _empty_ref(
            spark.read.table("emit_scenes").filter(F.col("product_type") == "PLMMETA"),
            _PLUME_SCHEMA,
        )

    # observation_date from the per-plume UTC observation time (date part). Robust to
    # the trailing 'Z'/time by slicing the leading yyyy-MM-dd (avoids format edge cases).
    plumes = (
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
    # LEFT-SEMI join to the bronze PLMMETA dates: keeps plume columns/count intact
    # (1 distinct date, no fan-out) while placing emit_scenes in the returned plan's
    # lineage => the emit_scenes -> emit_plumes edge is inferred.
    return plumes.join(F.broadcast(meta_dates), "observation_date", "left_semi")


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
    c = cfg(spark)
    p = paths(spark)

    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.sample import EmitDownloader

    # emit_scenes ENH inventory: carries observation_date per staged ENH COG AND the
    # emit_scenes -> plume_quant dependency edge. Keyed by source_file basename so the
    # join is robust to any URI-prefix difference between the bronze `path` and the
    # raster reader's `source` column.
    enh_bronze = (
        spark.read.table("emit_scenes")
        .filter(F.col("product_type") == "ENH")
        .select("source_file", "observation_date")
    )
    plumes = spark.read.table("emit_plumes")  # existing emit_plumes -> plume_quant edge
    try:
        # (source, tile) per overpass ENH segment.
        enh_raw = EmitDownloader().read_enh(p["emit"])
    except FileNotFoundError:
        return _empty_ref(
            spark.read.table("emit_scenes").filter(F.col("product_type") == "ENH"),
            _QUANT_SCHEMA,
        )

    # Tag each ENH tile with its overpass date from the bronze inventory (join on the
    # source filename basename). This gives every ENH segment an observation_date so
    # each plume can be paired ONLY with the ENH tiles from its OWN overpass.
    enh = (
        enh_raw.withColumn(
            "source_file", F.element_at(F.split(F.col("source"), "/"), -1))
        .join(enh_bronze, "source_file")
        .select("observation_date", F.col("tile").alias("scene"))
    )

    # DATE-SCOPED JOIN (was a crossJoin of every plume x EVERY ENH tile across all
    # years -> Java-heap OOM). A plume's enhancement lives in its own overpass, so join
    # on observation_date: the cross product is now bounded to (plumes x ENH segments)
    # WITHIN a single date. Repartition by plume_id so each task holds only one plume's
    # handful of clips (bounds executor memory; Serverless-safe column repartition).
    paired = plumes.join(enh, "observation_date").repartition(64, "plume_id")

    # EMIT retrieval QC (Gemini tip). We cannot co-align CH4SENS/CH4UNCERT here: the
    # EmitDownloader stages only CH4ENH + CH4PLM (the SENS/UNCERT products are not
    # downloaded), so the full sensitivity/uncertainty mask is out of reach live.
    # Applied instead (the documented minimum): a positive-enhancement / sensitivity
    # floor via rst_threshold -- pixels at/below `emit_enh_floor` (default 0.0) become
    # NoData, which rst_summary then excludes from gbx_max/mean. This drops negative /
    # sub-noise enhancement (retrieval artifacts) before summarizing; the product's own
    # NoData is likewise excluded by rst_summary's valid-pixel statistics.
    floor = c["emit_enh_floor"]
    clipped = (
        paired
        .withColumn("scene_qc", rx.rst_threshold("scene", F.lit(">"), F.lit(floor)))
        .withColumn("clip", rx.rst_clip("scene_qc", "plume_geom", F.lit(True)))
        .withColumn("summary", rx.rst_summary("clip"))
        .withColumn("gbx_mean_ppmm",
                    F.get_json_object("summary", "$.bands[0].mean").cast("double"))
        .withColumn("gbx_max_ppmm",
                    F.get_json_object("summary", "$.bands[0].max").cast("double"))
        # Keep -- per plume -- the segment with the strongest clipped max (edge-segment
        # slivers lose to the containing segment).
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


# ---------------------------------------------------------------------------
# Wells (SHL) as a Slowly-Changing-Dimension (Type 2) + as-of plume attribution.
#
# The TX RRC well surface-hole-location snapshot carries no acquisition date, so each
# staged snapshot's observation_date is its ingest date (see bronze wells_raw). To keep
# a queryable history of the well inventory as new snapshots land, we maintain `wells_shl`
# as an SCD2 table keyed by `api`, sequenced by that snapshot date.
#
# `create_auto_cdc_flow` needs a STREAMING source, but WellsDownloader().read() is a batch
# geojson parse. We bridge the two with a stream-static join: the streaming side is
# `wells_raw` (an Auto Loader streaming table — one row per staged snapshot file, which
# fires the flow when a new snapshot arrives and carries that snapshot's date); the static
# side is the batch-parsed well attributes. The crossJoin re-emits every well tagged with
# the arriving snapshot's date, which is exactly the change feed SCD2 consumes.
# ---------------------------------------------------------------------------


@dp.temporary_view(name="wells_snapshots")
def wells_snapshots():
    """Streaming change feed for `wells_shl`: each staged wells snapshot re-emits the
    full parsed well inventory, tagged with the snapshot's observation_date. Streaming
    trigger = `wells_raw`; well attributes = batch WellsDownloader().read() (NB04 CELL 7
    projection). Stream-static crossJoin keeps the flow a valid streaming source."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)

    from databricks.labs.gbx.sample import WellsDownloader
    attrs = WellsDownloader().read(p["wells"]).select(
        F.col("API").cast("string").alias("api"),
        F.col("CompanyName").alias("operator"),
        F.col("LeaseName").alias("lease"),
        F.col("WellNbr").alias("well_no"),
        F.col("FieldName").alias("field"),
        F.col("County").alias("county"),
        F.col("WellURL").alias("well_url"),
        F.col("geom_0").alias("well_geom"),
    )
    # One row per staged snapshot file (Auto Loader binaryFile) -> the snapshot date.
    snaps = spark.readStream.table("wells_raw").select("observation_date")
    return (
        snaps.crossJoin(F.broadcast(attrs))
        .withColumn("_ingested_at", F.current_timestamp())
    )


dp.create_streaming_table(
    "wells_shl",
    comment="TX RRC well surface-hole locations — SCD2 history keyed by API",
    partition_cols=["api"],
)
dp.create_auto_cdc_flow(
    target="wells_shl",
    source="wells_snapshots",
    keys=["api"],
    sequence_by=F.col("observation_date"),
    stored_as_scd_type=2,
)


@dp.materialized_view(
    name="plume_candidate_wells",
    comment="k nearest TX RRC wells per EMIT plume origin (as-of attribution)",
    partition_cols=["observation_date"],
)
def plume_candidate_wells():
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    k = c["k_candidates"]

    plumes = spark.read.table("emit_plumes")
    # As-of attribution against the SCD2 history. The wells snapshot's observation_date
    # is its INGEST date (wells carry no acquisition date), which postdates the historical
    # EMIT plume dates, so a strict `__START_AT <= plume_date` predicate would exclude every
    # well and yield zero candidates. We therefore attribute against the currently-valid
    # well version (`__END_AT IS NULL`) — the meaningful choice when the inventory postdates
    # the plumes — while still reading the SCD2 table + its temporal bounds.
    wells = (
        spark.read.table("wells_shl")
        .filter(F.col("__END_AT").isNull())
        .select(
            "api", "operator", "lease", "well_no", "field", "county", "well_url",
            F.expr("st_x(st_geomfromwkb(well_geom))").alias("well_lon"),
            F.expr("st_y(st_geomfromwkb(well_geom))").alias("well_lat"),
            F.expr("st_geomfromwkb(well_geom)").alias("well_pt"),
        )
    )
    p_pt = plumes.withColumn("plume_pt", F.expr("st_point(lon_max, lat_max)"))
    paired = p_pt.crossJoin(wells).withColumn(
        "dist_m", F.expr("st_distancesphere(plume_pt, well_pt)")
    )
    candidates = (
        paired.withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("plume_id").orderBy("dist_m")),
        )
        .filter(F.col("rank") <= k)
        .drop("plume_pt", "well_pt")
    )
    return candidates.select(
        "plume_id", "observation_date", "max_conc_ppmm", "emission_rate_kg_hr",
        "emission_rate_uncert_kg_hr", "lon_max", "lat_max",
        "api", "operator", "lease", "well_no", "field", "county", "well_url",
        "well_lon", "well_lat", "dist_m", "rank",
        F.current_timestamp().alias("_ingested_at"),
    )


# ---------------------------------------------------------------------------
# Carbon Mapper rated plumes — the headline authoritative current-detection layer.
#
# cm_scenes (bronze, one row per plume record) -> cm_detections (parsed GEOMETRY +
# hoisted emission-rate/quality/wind fields + H3) -> cm_candidate_wells (nearest-K
# TX RRC wells feeding the operator emissions leaderboard). Carbon Mapper publishes a
# real, non-null emission rate (kg/hr), which revives the kg/hr leaderboard that the
# EMIT source could not carry (JPL rates are frequently NA without wind).
# ---------------------------------------------------------------------------


@dp.materialized_view(
    name="cm_detections",
    comment="Carbon Mapper rated CH4 plumes: GEOMETRY + emission rate (kg/hr) + H3",
    partition_cols=["observation_date"],
)
# Carbon Mapper emission rates are real, non-null estimates -> non-negative always.
@dp.expect("emission_rate_nonneg", "emission_rate_kg_hr >= 0")
@dp.expect("has_geom", "plume_geom IS NOT NULL")
def cm_detections():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)

    cm = spark.read.table("cm_scenes")
    # Parse the GeoJSON geometry to a native GEOMETRY tagged SRID 4326 (coords are
    # WGS84 lon/lat), then derive the plume centroid lon/lat and its H3 cell.
    return (
        cm.withColumn(
            "plume_geom",
            F.expr("st_setsrid(st_geomfromgeojson(geometry_json), 4326)"),
        )
        .withColumn("lon", F.expr("st_x(st_centroid(plume_geom))"))
        .withColumn("lat", F.expr("st_y(st_centroid(plume_geom))"))
        .withColumn(
            "h3_cellid",
            DBF.h3_longlatash3("lon", "lat", F.lit(c["cm_h3_res"])),
        )
        .withColumn("emission_rate_kg_hr", F.col("emission_auto").cast("double"))
        .withColumn(
            "emission_uncertainty_kg_hr",
            F.col("emission_uncertainty_auto").cast("double"),
        )
        .withColumn("wind_speed_ms", F.col("wind_speed_avg_auto").cast("double"))
        .withColumn(
            "wind_direction_deg", F.col("wind_direction_avg_auto").cast("double"))
        .select(
            "plume_id", "observation_date", "scene_id", "scene_timestamp",
            "gas", "instrument", "platform", "sector", "plume_quality",
            "emission_rate_kg_hr", "emission_uncertainty_kg_hr",
            "wind_speed_ms", "wind_direction_deg", "is_offshore",
            "lon", "lat", "h3_cellid", "plume_geom",
            F.current_timestamp().alias("_ingested_at"),
        )
    )


@dp.materialized_view(
    name="cm_candidate_wells",
    comment="k nearest TX RRC wells per Carbon Mapper plume (operator attribution)",
    partition_cols=["observation_date"],
)
def cm_candidate_wells():
    from pyspark.sql import SparkSession
    from pyspark.sql.window import Window
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    k = c["k_candidates"]

    plumes = spark.read.table("cm_detections")
    # Attribute against the currently-valid well version (see plume_candidate_wells for
    # the SCD2 as-of rationale: the wells snapshot postdates historical plume dates).
    wells = (
        spark.read.table("wells_shl")
        .filter(F.col("__END_AT").isNull())
        .select(
            "api", "operator", "lease", "well_no", "field", "county", "well_url",
            F.expr("st_x(st_geomfromwkb(well_geom))").alias("well_lon"),
            F.expr("st_y(st_geomfromwkb(well_geom))").alias("well_lat"),
            F.expr("st_geomfromwkb(well_geom)").alias("well_pt"),
        )
    )
    p_pt = plumes.withColumn("plume_pt", F.expr("st_point(lon, lat)"))
    paired = p_pt.crossJoin(wells).withColumn(
        "dist_m", F.expr("st_distancesphere(plume_pt, well_pt)")
    )
    candidates = (
        paired.withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("plume_id").orderBy("dist_m")),
        )
        .filter(F.col("rank") <= k)
        .drop("plume_pt", "well_pt")
    )
    return candidates.select(
        "plume_id", "observation_date", "emission_rate_kg_hr",
        "emission_uncertainty_kg_hr", "sector", "plume_quality", "lon", "lat",
        "api", "operator", "lease", "well_no", "field", "county", "well_url",
        "well_lon", "well_lat", "dist_m", "rank",
        F.current_timestamp().alias("_ingested_at"),
    )
