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
        F.expr("st_setsrid(st_point(lon_max, lat_max), 4326)").alias("origin_geom"),
        F.expr("st_setsrid(st_geomfromwkb(plume_geom), 4326)").alias("plume_geom_native"),
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
        F.expr("st_geomfromwkb(h3_boundaryaswkb(h3_cellid), 4326)").alias("hex_geom"),
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
        .withColumn("hex_geom", F.expr("st_geomfromwkb(h3_boundaryaswkb(h3_cellid), 4326)"))
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


# ---------------------------------------------------------------------------
# Carbon Mapper "leakiest operators" gold layer (demo headline).
#
# GOOD-CITIZEN / ATTRIBUTION NOTE: every MV below is derived from Carbon Mapper
# rated-plume data (`cm_detections` / `cm_candidate_wells`, silver_cascade.py).
# Per Carbon Mapper's terms, any user-facing visualization (dashboard, map,
# chart) built on this data MUST display "Data © Carbon Mapper" attribution.
# The dashboard-side attribution widget/caption is a follow-up task -- this
# comment is the tripwire so it isn't forgotten when the dashboard is wired up.
# ---------------------------------------------------------------------------


@dp.materialized_view(
    name="cm_plume_attributed",
    comment=(
        "One row per Carbon Mapper plume joined to its rank-1 nearest well "
        "(operator attribution); map + drill-down layer"
    ),
)
def cm_plume_attributed():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    d = spark.read.table("cm_detections")
    lead = (
        spark.read.table("cm_candidate_wells")
        .filter("rank = 1")
        .select(
            "plume_id", "observation_date",
            F.col("operator").alias("lead_operator"),
            F.col("lease").alias("lead_lease"),
            F.col("field").alias("lead_field"),
            F.col("county").alias("lead_county"),
            F.col("dist_m").alias("lead_dist_m"),
        )
    )
    joined = d.join(lead, ["plume_id", "observation_date"], "left")

    return joined.select(
        "plume_id", "observation_date", "scene_timestamp",
        "emission_rate_kg_hr", "emission_uncertainty_kg_hr",
        "plume_quality", "instrument", "sector",
        "lead_operator", "lead_lease", "lead_field", "lead_county", "lead_dist_m",
        "lon", "lat",
        # Re-tag SRID 4326 explicitly: GEOMETRY columns round-tripped through a
        # materialized view have been observed to come back SRID 0 (Phase 6
        # finding), which AI/BI silently refuses to render.
        F.expr("st_setsrid(plume_geom, 4326)").alias("plume_geom"),
    )


@dp.materialized_view(
    name="operator_emissions_leaderboard",
    comment=(
        "THE headline: operators ranked by total Carbon Mapper-attributed CH4 "
        "emission rate (kg/hr) across their nearest-well plumes"
    ),
)
def operator_emissions_leaderboard():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    board = spark.read.table("cm_plume_attributed").withColumn(
        "lead_operator", F.coalesce(F.col("lead_operator"), F.lit("Unattributed"))
    )

    # Trailing-90d window is relative to the latest observation_date actually
    # present in the data (not wall-clock current_date), broadcast via crossJoin
    # -- same no-collect pattern as hotspot_persistence's elevated_threshold.
    max_date_row = board.agg(F.max("observation_date").alias("max_obs_date"))
    recent = (
        board.crossJoin(F.broadcast(max_date_row))
        .filter(F.col("observation_date") >= F.date_sub(F.col("max_obs_date"), 90))
    )

    base = board.groupBy("lead_operator").agg(
        F.sum("emission_rate_kg_hr").alias("total_emission_kg_hr"),
        F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
        F.avg("emission_rate_kg_hr").alias("mean_emission_kg_hr"),
        F.count("plume_id").alias("plume_count"),
        F.min("observation_date").alias("first_detection"),
        F.max("observation_date").alias("last_detection"),
    )
    recent_agg = recent.groupBy("lead_operator").agg(
        F.sum("emission_rate_kg_hr").alias("emission_last_90d_kg_hr")
    )

    wells = (
        spark.read.table("cm_candidate_wells")
        .filter("rank = 1")
        .withColumn("operator", F.coalesce(F.col("operator"), F.lit("Unattributed")))
        .groupBy("operator")
        .agg(F.approx_count_distinct("api").alias("well_count"))
    )

    joined = (
        base.join(recent_agg, "lead_operator", "left")
        .join(wells, base["lead_operator"] == wells["operator"], "left")
        .withColumn(
            "emission_last_90d_kg_hr",
            F.coalesce(F.col("emission_last_90d_kg_hr"), F.lit(0.0)),
        )
        .withColumn("well_count", F.coalesce(F.col("well_count"), F.lit(0)))
    )

    # DEFENSIBLE FRAMING: rank operators by number of high-confidence plume
    # DETECTIONS and report MEAN/MAX per-detection rate. A per-detection rate is an
    # instantaneous kg/hr at one overpass, so summing rates across 2024-2026 is NOT a
    # continuous flow rate and double-counts repeat detections of the same source —
    # so the summed value is retained ONLY as a clearly-labeled secondary context
    # column (cumulative_detected_rate_kg_hr), never the headline/rank.
    ranked = joined.withColumn(
        "detection_rank",
        F.row_number().over(Window.orderBy(F.col("plume_count").desc_nulls_last())),
    )
    return ranked.select(
        F.col("lead_operator").alias("lead_operator"),
        "detection_rank",
        "plume_count",
        "mean_emission_kg_hr", "max_emission_kg_hr",
        "emission_last_90d_kg_hr",
        F.col("total_emission_kg_hr").alias("cumulative_detected_rate_kg_hr"),
        "well_count", "first_detection", "last_detection",
    ).orderBy(F.col("plume_count").desc())


@dp.materialized_view(
    name="cm_monitoring_status",
    comment="Single-row current-status panel: quiet-now vs active, last 90d Carbon Mapper activity",
)
def cm_monitoring_status():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    board = spark.read.table("cm_plume_attributed")

    all_time = board.agg(
        F.max("observation_date").alias("last_detection_date"),
        F.count("plume_id").alias("total_plumes_all_time"),
        F.sum("emission_rate_kg_hr").alias("total_emission_all_time_kg_hr"),
    )

    tagged = board.crossJoin(F.broadcast(all_time.select("last_detection_date")))
    last_90d = (
        tagged.filter(
            F.col("observation_date") >= F.date_sub(F.col("last_detection_date"), 90)
        )
        .agg(
            F.count("plume_id").alias("plumes_last_90d"),
            F.sum("emission_rate_kg_hr").alias("total_emission_last_90d_kg_hr"),
            F.countDistinct("lead_operator").alias("active_operators_last_90d"),
        )
    )

    return (
        all_time.crossJoin(last_90d)
        .withColumn(
            "days_since_last_detection",
            F.datediff(F.current_date(), F.col("last_detection_date")),
        )
        .select(
            "last_detection_date", "days_since_last_detection",
            "plumes_last_90d", "total_emission_last_90d_kg_hr",
            "active_operators_last_90d",
            "total_plumes_all_time", "total_emission_all_time_kg_hr",
        )
    )


@dp.materialized_view(
    name="cm_activity_monthly",
    comment="Per-month Carbon Mapper activity: active-vs-quiet timeline",
)
def cm_activity_monthly():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    board = spark.read.table("cm_plume_attributed").withColumn(
        "lead_operator", F.coalesce(F.col("lead_operator"), F.lit("Unattributed"))
    )

    return (
        board.withColumn("month", F.trunc(F.col("observation_date"), "month"))
        .groupBy("month")
        .agg(
            F.count("plume_id").alias("plume_count"),
            F.sum("emission_rate_kg_hr").alias("total_emission_kg_hr"),
            F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
            F.countDistinct("lead_operator").alias("active_operators"),
        )
        .orderBy("month")
    )


@dp.materialized_view(
    name="emissions_by_play",
    comment="Carbon Mapper plume detections rolled up to EIA shale plays (map-ready)",
)
def emissions_by_play():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    plumes = spark.read.table("cm_plume_attributed").select(
        "plume_id", "emission_rate_kg_hr", "lead_operator",
        F.expr("st_setsrid(st_point(lon, lat), 4326)").alias("pt"),
    )
    plays = spark.read.table("ref_shale_plays")
    # NB: GEOMETRY is not an orderable type, so it cannot be a GROUP BY key. Roll up
    # keyed by play_name, then join the polygon back from the reference table (one
    # geometry per play) so each output row still carries its map-ready geometry.
    agg = (
        plays.join(plumes, F.expr("st_contains(play_geom, pt)"), "left")
        .groupBy("play_name")
        .agg(
            F.count("plume_id").alias("plume_count"),
            F.avg("emission_rate_kg_hr").alias("mean_emission_kg_hr"),
            F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
            F.countDistinct("lead_operator").alias("active_operators"),
        )
    )
    return agg.join(
        plays.select("play_name", "area_sq_km", "play_geom"), "play_name"
    )


@dp.materialized_view(
    name="detections_by_county",
    comment="Carbon Mapper plume detections rolled up to TX/NM counties (map-ready)",
)
def detections_by_county():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    plumes = spark.read.table("cm_plume_attributed").select(
        "plume_id", "emission_rate_kg_hr",
        F.expr("st_setsrid(st_point(lon, lat), 4326)").alias("pt"),
    )
    counties = spark.read.table("ref_counties")
    # GEOMETRY is not orderable, so roll up keyed by geoid (unique per county) and
    # join the county polygon back from the reference table for the choropleth.
    agg = (
        counties.join(plumes, F.expr("st_contains(county_geom, pt)"), "left")
        .groupBy("county_name", "state_fp", "geoid")
        .agg(
            F.count("plume_id").alias("plume_count"),
            F.avg("emission_rate_kg_hr").alias("mean_emission_kg_hr"),
            F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
        )
    )
    return agg.join(
        counties.select("county_name", "state_fp", "geoid", "county_geom"),
        ["county_name", "state_fp", "geoid"],
    )
