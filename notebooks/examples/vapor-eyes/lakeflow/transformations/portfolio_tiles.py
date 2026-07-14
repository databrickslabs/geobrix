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
`repartition(N, "geom_wkb")` (no `spark.conf.set` / `_jvm` / `.rdd`). Single
archive writes to the Volume use `open(path, "wb")` after `os.makedirs`, which
is FUSE-safe for sequential driver writes.
"""
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import cfg, register_gbx

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
