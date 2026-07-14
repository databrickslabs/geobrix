"""Tiles / synthesis: fold the latest methane cascade into one shareable vector
tile product — an MVT pyramid, a fanout of bounded per-shard PMTiles archives,
and one light overview archive.

Three layers are assembled from LATEST gold/silver state (mirrors NB05):
  - hotspots  — S5P H3 hotspot cells as hexagons (`h3_boundaryaswkb`)
  - plumes    — EMIT plume outlines (`plume_geom`)
  - wells     — TX RRC surface-hole wells, current SCD2 version (`well_geom`)

Each layer is a `(geom_wkb, attrs)` view; `gbx_st_asmvt_pyramid` (a GeoBrix
light UDTF) bins each geometry into the Web-Mercator tile grid and emits
tile-local MVT per zoom level. `pmtiles_agg` (light grouped aggregator) folds
`(mvt_bytes, z, x, y)` rows into a PMTiles v3 archive (BINARY).

Serverless parallelism for the per-feature UDTF comes ONLY from
`repartition(N, "geom_wkb")` (no `spark.conf.set` / `_jvm` / `.rdd`).

The archive-producing views (pmtiles_shards, overview_manifest) are returned as
LAZY plans that read portfolio_mvt_tiles so Lakeflow infers the dependency and
executes them after that MV is materialized; each archive is written to the
Volume by a scalar UDF (`open(path, "wb")` after `os.makedirs`, FUSE-safe) that
runs during that execution — one row per output file, so no write races.
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

from _config import cfg, paths, register_gbx

# TileJSON `vector_layers` metadata — declares the three MVT source-layers and
# their attribute fields so any viewer (incl. gbx.vizx.plot_pmtiles) can resolve
# and draw each layer. Attribute names/types match the `attrs` structs below.
PORTFOLIO_META = json.dumps({
    "vector_layers": [
        {"id": "hotspots", "fields": {"ch4_max": "Number", "observation_date": "String"}},
        {"id": "plumes", "fields": {
            "plume_id": "String", "max_conc_ppmm": "Number", "observation_date": "String"}},
        {"id": "wells", "fields": {"operator": "String", "api": "String"}},
    ]
})


# ---------------------------------------------------------------------------
# Task 5.1 — portfolio_mvt_tiles: the three-layer MVT pyramid.
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="portfolio_mvt_tiles",
    comment=(
        "Three-layer (hotspots/plumes/wells) MVT pyramid over the latest cascade "
        "state; one (layer, z, x, y, mvt_bytes) row per feature per zoom level"
    ),
)
def portfolio_mvt_tiles():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)  # registers gbx_st_asmvt_pyramid (pyvx UDTF)
    c = cfg(spark)
    min_z, max_z = c["min_z"], c["max_z"]

    # Layer 1 — S5P hotspot hexagons (latest overpass), carrying peak CH4 + date.
    hotspots = spark.read.table("hotspot_latest").selectExpr(
        "h3_boundaryaswkb(h3_cellid) AS geom_wkb",
        "named_struct('ch4_max', ch4_max, "
        "'observation_date', cast(observation_date AS string)) AS attrs",
    )
    # Layer 2 — EMIT plume outlines, carrying id + peak concentration + date.
    plumes = spark.read.table("emit_plumes").selectExpr(
        "plume_geom AS geom_wkb",
        "named_struct('plume_id', plume_id, 'max_conc_ppmm', max_conc_ppmm, "
        "'observation_date', cast(observation_date AS string)) AS attrs",
    )
    # Layer 3 — TX RRC wells, current SCD2 version only (__END_AT IS NULL).
    wells = (
        spark.read.table("wells_shl")
        .filter(F.col("__END_AT").isNull())
        .selectExpr(
            "well_geom AS geom_wkb",
            "named_struct('operator', operator, 'api', api) AS attrs",
        )
    )

    def _pyramid(df, layer, nparts):
        # A column key is required so the per-feature UDTF fans out on Serverless.
        view = f"_v_portfolio_{layer}"
        df.repartition(nparts, "geom_wkb").createOrReplaceTempView(view)
        return spark.sql(
            f"""
            SELECT '{layer}' AS layer, t.z, t.x, t.y, t.mvt_bytes
            FROM {view},
                 LATERAL gbx_st_asmvt_pyramid(geom_wkb, attrs, {min_z}, {max_z}, '{layer}') AS t
            """
        )

    return (
        _pyramid(hotspots, "hotspots", 32)
        .unionByName(_pyramid(plumes, "plumes", 8))
        .unionByName(_pyramid(wells, "wells", 32))
    )


# ---------------------------------------------------------------------------
# Task 5.2 — pmtiles_shards: fanout to bounded per-shard PMTiles archives.
#
# Fully LAZY: the returned plan reads portfolio_mvt_tiles, so Lakeflow infers the
# dependency and executes it AFTER that MV is materialized (an eager collect in the
# body would instead run at flow-analysis against the not-yet-populated upstream and
# capture an empty result). The per-shard archive is written to the Volume by a
# scalar UDF that runs during that execution — one row per shard, distinct paths, so
# no write races. Binary-free fanout (no tile-join).
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="pmtiles_shards",
    comment=(
        "Spatial catalog of bounded per-shard PMTiles archives (fanout at "
        "shard_zoom = min_z); one row per shard with its bbox + archive path"
    ),
)
def pmtiles_shards():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    from databricks.labs.gbx.pmtiles.functions import pmtiles_agg

    c = cfg(spark)
    p = paths(spark)
    sz = c["min_z"]            # shard_zoom == min_z, so every pyramid tile has z >= sz
    n = float(2 ** sz)         # tiles per axis at the shard zoom
    shards_dir = f"{p['tiles']}/shards"

    # Shard key + bbox are computed with executor-native SQL (NOT a UDF over _shard):
    # a UDF referencing _shard's module-level functions is cloudpickled by reference,
    # and _shard is not importable on executors. This SQL mirrors _shard.tile_shard /
    # shard_bounds (the unit-tested reference in tests/test_shard.py) for z >= sz — the
    # only case that occurs here since pyramid tiles start at min_z == sz.
    keyed = (
        spark.read.table("portfolio_mvt_tiles")
        .withColumn("sx", F.expr(f"shiftright(x, z - {sz})"))
        .withColumn("sy", F.expr(f"shiftright(y, z - {sz})"))
        .withColumn("shard_key",
                    F.expr(f"concat({sz}, '/', shiftright(x, z - {sz}), '/', shiftright(y, z - {sz}))"))
    )

    # The self-contained file-writer UDF captures only `shards_dir` (str) + os, so it
    # cloudpickles by value and runs on executors. One row per shard -> distinct paths,
    # no write races; /Volumes is a shared object store (executor FUSE write is safe).
    @F.udf(StringType())
    def _write_shard(shard_key, archive):
        import os
        if archive is None:
            return None
        os.makedirs(shards_dir, exist_ok=True)
        out = f"{shards_dir}/{shard_key.replace('/', '_')}.pmtiles"
        with open(out, "wb") as f:
            f.write(bytes(archive))
        return out

    # Archives and tile-count metadata are aggregated separately then joined — the
    # pmtiles_agg grouped aggregate is not mixed with built-in aggregates in one .agg.
    archives = keyed.groupBy("shard_key").agg(
        pmtiles_agg("mvt_bytes", "z", "x", "y", PORTFOLIO_META).alias("archive")
    )
    counts = keyed.groupBy("shard_key").agg(
        F.first("sx").alias("sx"),
        F.first("sy").alias("sy"),
        F.count("*").cast("long").alias("tile_count"),
        F.min("z").cast("int").alias("min_z"),
        F.max("z").cast("int").alias("max_z"),
        F.sum(F.when(F.col("layer") == "hotspots", 1).otherwise(0)).cast("long").alias("hotspots"),
        F.sum(F.when(F.col("layer") == "plumes", 1).otherwise(0)).cast("long").alias("plumes"),
        F.sum(F.when(F.col("layer") == "wells", 1).otherwise(0)).cast("long").alias("wells"),
    )
    joined = archives.join(counts, "shard_key")

    # XYZ tile -> WGS84 bbox (mirrors _shard.shard_bounds): y increases southward, so
    # row sy is the north edge and sy+1 the south edge. sinh(v) = (e^v - e^-v) / 2.
    north = f"pi() * (1 - 2 * sy / {n})"
    south = f"pi() * (1 - 2 * (sy + 1) / {n})"
    return joined.select(
        F.col("shard_key").alias("shard_id"),
        F.expr(f"sx / {n} * 360 - 180").alias("min_lon"),
        F.expr(f"degrees(atan((exp({south}) - exp(-({south}))) / 2))").alias("min_lat"),
        F.expr(f"(sx + 1) / {n} * 360 - 180").alias("max_lon"),
        F.expr(f"degrees(atan((exp({north}) - exp(-({north}))) / 2))").alias("max_lat"),
        _write_shard("shard_key", "archive").alias("archive_path"),
        F.col("tile_count"),
        F.create_map(
            F.lit("hotspots"), F.col("hotspots"),
            F.lit("plumes"), F.col("plumes"),
            F.lit("wells"), F.col("wells"),
        ).alias("layer_tile_counts"),
        F.col("min_z"),
        F.col("max_z"),
    )
