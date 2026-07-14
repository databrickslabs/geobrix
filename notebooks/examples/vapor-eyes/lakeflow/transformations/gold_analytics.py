"""Gold: concentration-led analytics materialized views (latest + trend).

CRITICAL DESIGN NOTE (supersedes the plan's original emission-rate framing):
JPL's `emission_rate_kg_hr` / `wind_speed_ms` / `fetch_length_m` are NULL for
both plumes in this AOI/date window (no wind data at overpass time — a
genuine JPL "NA", confirmed against source plume metadata during Phase 3, not
a reader bug). Gold therefore LEADS with CONCENTRATION intensity —
`max_conc_ppmm` (JPL enhancement) and `gbx_max_ppmm` / `gbx_mean_ppmm` (the
GeoBrix `rst_clip` + `rst_summary` cross-check) — as the primary ranking
metric everywhere. `emission_rate_kg_hr` is retained as a nullable
when-available secondary column; it is never used to rank or filter rows.

Every map-facing column here is native `GEOMETRY` (`st_point`/`st_geomfromwkb`
/ `h3_boundaryaswkb`) or a lat/lon double — AI/BI cannot render WKB bytes or
raw H3 cell ids, so gold never exposes those directly on a map-facing MV.

Gold reads its own upstream tables by name (`spark.read.table(...)`); Lakeflow
resolves the MV dependency graph from those references, same as silver's
`plume_candidate_wells` reading `emit_plumes`. No GeoBrix SQL is needed here
(pure Databricks-native `st_*`/`h3_*` + DataFrame aggregation), so
`register_gbx` is intentionally not called.
"""
import math

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from _config import cfg


@dp.materialized_view(
    name="plume_leaderboard_latest",
    comment=(
        "Latest per-plume concentration intensity (max_conc_ppmm / gbx_max_ppmm) "
        "+ leading candidate operator, ranked by peak concentration (map-ready)"
    ),
)
def plume_leaderboard_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    q = spark.read.table("plume_quant").select(
        "plume_id", "observation_date", "max_conc_ppmm", "gbx_max_ppmm",
        "gbx_mean_ppmm", "emission_rate_kg_hr", "wind_speed_ms", "plume_geom",
    )
    lead = (
        spark.read.table("plume_candidate_wells")
        .filter("rank = 1")
        .select(
            "plume_id", "observation_date", "lon_max", "lat_max",
            F.col("operator").alias("lead_operator"),
            F.col("lease").alias("lead_lease"),
            F.col("field").alias("lead_field"),
            F.col("county").alias("lead_county"),
            F.col("dist_m").alias("lead_dist_m"),
        )
    )
    joined = q.join(lead, ["plume_id", "observation_date"], "left")

    # One row per plume_id: keep only its latest observation_date.
    latest = (
        joined.withColumn(
            "_r",
            F.row_number().over(
                Window.partitionBy("plume_id").orderBy(F.col("observation_date").desc())
            ),
        )
        .filter("_r = 1")
        .drop("_r")
    )

    ranked = latest.withColumn(
        "concentration_rank",
        F.row_number().over(Window.orderBy(F.col("max_conc_ppmm").desc_nulls_last())),
    )

    return ranked.select(
        "plume_id", "observation_date",
        "max_conc_ppmm", "gbx_max_ppmm", "gbx_mean_ppmm",
        "emission_rate_kg_hr", "wind_speed_ms",
        "lead_operator", "lead_lease", "lead_field", "lead_county", "lead_dist_m",
        "concentration_rank",
        "lon_max", "lat_max",
        F.expr("st_point(lon_max, lat_max)").alias("origin_geom"),
        F.expr("st_geomfromwkb(plume_geom)").alias("plume_geom_native"),
    )


@dp.materialized_view(
    name="operator_intensity_latest",
    comment="Concentration intensity aggregated by leading candidate operator (latest per plume)",
)
def operator_intensity_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    board = spark.read.table("plume_leaderboard_latest")
    wells = spark.read.table("plume_candidate_wells").select("operator", "api")

    agg = board.groupBy("lead_operator").agg(
        F.count("plume_id").alias("plume_count"),
        F.max("max_conc_ppmm").alias("max_peak_ppmm"),
        F.avg("max_conc_ppmm").alias("avg_peak_ppmm"),
        F.max("gbx_max_ppmm").alias("max_gbx_ppmm"),
        F.sum("emission_rate_kg_hr").alias("total_emission_kg_hr"),
    )
    well_counts = wells.groupBy("operator").agg(
        F.countDistinct("api").alias("well_count")
    )
    joined = agg.join(
        well_counts, agg["lead_operator"] == well_counts["operator"], "left"
    )
    return joined.select(
        agg["lead_operator"].alias("operator"),
        "plume_count", "max_peak_ppmm", "avg_peak_ppmm", "max_gbx_ppmm",
        "total_emission_kg_hr",
        F.coalesce(F.col("well_count"), F.lit(0)).alias("well_count"),
    )


@dp.materialized_view(
    name="field_county_intensity_latest",
    comment="Concentration intensity aggregated by leading candidate field + county (latest per plume)",
)
def field_county_intensity_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    board = spark.read.table("plume_leaderboard_latest")
    return (
        board.groupBy(
            F.col("lead_field").alias("field"), F.col("lead_county").alias("county")
        )
        .agg(
            F.count("plume_id").alias("plume_count"),
            F.max("max_conc_ppmm").alias("max_peak_ppmm"),
        )
    )


@dp.materialized_view(
    name="hotspot_latest",
    comment="Latest-overpass S5P hotspot cells, ranked by peak CH4 (map-ready hexagons)",
)
def hotspot_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    hs = spark.read.table("s5p_hotspots")
    # Whole-table window (no driver collect) to find the latest observation_date,
    # then filter to it — same no-.rdd/no-conf.set constraint as everywhere else.
    tagged = hs.withColumn(
        "_max_date", F.max("observation_date").over(Window.partitionBy(F.lit(1)))
    )
    latest = tagged.filter(F.col("observation_date") == F.col("_max_date")).drop("_max_date")

    ranked = latest.withColumn(
        "hotspot_rank",
        F.row_number().over(Window.orderBy(F.col("ch4_max").desc_nulls_last())),
    )
    return ranked.select(
        "h3_cellid", "observation_date", "ch4_mean", "ch4_max", "n_obs",
        "hotspot_rank",
        F.expr("st_x(st_geomfromwkb(geom_wkb))").alias("center_lon"),
        F.expr("st_y(st_geomfromwkb(geom_wkb))").alias("center_lat"),
        F.expr("st_geomfromwkb(h3_boundaryaswkb(h3_cellid))").alias("hex_geom"),
    )


@dp.materialized_view(
    name="aoi_kpis_latest",
    comment="Single-row AOI KPI summary (latest concentration intensity + inventory scope)",
)
def aoi_kpis_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    c = cfg(spark)
    minx, miny, maxx, maxy = c["bbox"]
    lat_mid = (miny + maxy) / 2.0
    km_per_deg_lon = 111.320 * math.cos(math.radians(lat_mid))
    km_per_deg_lat = 110.574
    area_km2 = round(abs((maxx - minx) * km_per_deg_lon) * abs((maxy - miny) * km_per_deg_lat), 2)

    board = spark.read.table("plume_leaderboard_latest")
    hot = spark.read.table("hotspot_latest")
    # Current (as-of-now) SCD2 version of the well inventory.
    wells = spark.read.table("wells_shl").filter(F.col("__END_AT").isNull())

    plume_kpis = board.agg(
        F.count("plume_id").alias("total_plumes"),
        F.max("max_conc_ppmm").alias("max_concentration_ppmm"),
        F.max("observation_date").alias("latest_observation_date"),
    )
    wells_kpis = wells.agg(F.countDistinct("api").alias("wells_scanned"))
    hotspot_kpis = hot.agg(F.count("h3_cellid").alias("hotspot_cells"))

    return (
        plume_kpis.crossJoin(wells_kpis)
        .crossJoin(hotspot_kpis)
        .withColumn("aoi_area_km2", F.lit(area_km2))
    )


@dp.materialized_view(
    name="regional_ch4_trend_daily",
    comment=(
        "Headline basin-wide CH4-over-time line: one row per S5P observation_date "
        "(regional mean/max/p95, active cell count, total obs) + same-day EMIT plume count"
    ),
)
def regional_ch4_trend_daily():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    hs = spark.read.table("s5p_hotspots")
    daily = hs.groupBy("observation_date").agg(
        F.avg("ch4_mean").alias("regional_ch4_mean"),
        F.max("ch4_max").alias("regional_ch4_max"),
        F.percentile_approx("ch4_max", 0.95).alias("ch4_p95"),
        F.countDistinct("h3_cellid").alias("active_cells"),
        F.sum("n_obs").alias("total_obs"),
    )

    plumes = spark.read.table("emit_plumes")
    plume_counts = plumes.groupBy("observation_date").agg(
        F.count("plume_id").alias("plume_count")
    )

    return (
        daily.join(plume_counts, "observation_date", "left")
        .withColumn("plume_count", F.coalesce(F.col("plume_count"), F.lit(0)))
        .orderBy("observation_date")
    )


@dp.materialized_view(
    name="hotspot_persistence",
    comment=(
        "Chronic-vs-transient emitter analytic: per h3_cellid over the whole "
        "backfilled window, how often it was observed vs elevated (map + ranking)"
    ),
)
def hotspot_persistence():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    hs = spark.read.table("s5p_hotspots")

    # AOI-wide elevated threshold: approx 90th percentile of ch4_max over ALL rows,
    # computed in-body (never hardcoded) and broadcast via crossJoin to a 1-row frame.
    thresh_row = hs.agg(
        F.percentile_approx("ch4_max", 0.90).alias("elevated_threshold")
    )

    tagged = hs.crossJoin(F.broadcast(thresh_row))

    return (
        tagged.groupBy("h3_cellid")
        .agg(
            F.countDistinct("observation_date").alias("dates_observed"),
            F.countDistinct(
                F.when(
                    F.col("ch4_max") >= F.col("elevated_threshold"),
                    F.col("observation_date"),
                )
            ).alias("dates_elevated"),
            F.avg("ch4_mean").alias("mean_ch4"),
            F.max("ch4_max").alias("max_ch4"),
            F.max("geom_wkb").alias("_geom_wkb"),
        )
        .withColumn(
            "persistence_ratio",
            F.col("dates_elevated") / F.col("dates_observed"),
        )
        .withColumn("center_lon", F.expr("st_x(st_geomfromwkb(_geom_wkb))"))
        .withColumn("center_lat", F.expr("st_y(st_geomfromwkb(_geom_wkb))"))
        .withColumn("hex_geom", F.expr("st_geomfromwkb(h3_boundaryaswkb(h3_cellid))"))
        .drop("_geom_wkb")
        .select(
            "h3_cellid", "dates_observed", "dates_elevated", "persistence_ratio",
            "mean_ch4", "max_ch4", "center_lon", "center_lat", "hex_geom",
        )
        .orderBy(F.col("persistence_ratio").desc(), F.col("max_ch4").desc())
    )


@dp.materialized_view(
    name="plume_detection_timeline",
    comment=(
        "EMIT plume detections per overpass observation_date: plume count + peak "
        "concentration intensity (supersedes concentration_trend_daily)"
    ),
)
def plume_detection_timeline():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    q = spark.read.table("plume_quant")
    return q.groupBy("observation_date").agg(
        F.count("plume_id").alias("plume_count"),
        F.max("max_conc_ppmm").alias("max_peak_ppmm"),
        F.avg("max_conc_ppmm").alias("avg_peak_ppmm"),
        F.max("gbx_max_ppmm").alias("max_gbx_ppmm"),
    ).orderBy("observation_date")


@dp.materialized_view(
    name="hotspot_trend",
    comment="Daily per-cell CH4 hotspot trend (map-ready)",
)
def hotspot_trend():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    hs = spark.read.table("s5p_hotspots")
    return hs.select(
        "observation_date", "h3_cellid", "ch4_mean", "ch4_max",
        F.expr("st_x(st_geomfromwkb(geom_wkb))").alias("center_lon"),
        F.expr("st_y(st_geomfromwkb(geom_wkb))").alias("center_lat"),
    )
