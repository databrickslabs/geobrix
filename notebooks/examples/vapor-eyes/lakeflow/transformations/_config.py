"""Shared config for the vapor-eyes Lakeflow pipeline. Parameters come from the
pipeline `configuration` block (spark.conf.get); NO side effects at import time."""


def cfg(spark):
    g = spark.conf.get
    full_aoi = g("vapor_eyes.full_aoi", "true").lower() == "true"
    return {
        "catalog": g("vapor_eyes.catalog", "geospatial_docs"),
        "schema": g("vapor_eyes.schema", "vapor_eyes_lf"),
        "volume": g("vapor_eyes.volume", "data"),
        "full_aoi": full_aoi,
        "bbox": (-104.5, 30.8, -101.0, 33.0) if full_aoi
                else (-103.25, 31.30, -102.85, 31.62),
        "date_window": g("vapor_eyes.date_window", "2023-07-15/2023-08-20"),
        "s5p_temporal": g("vapor_eyes.s5p_temporal", "2024-08-23/2024-08-24"),
        "h3_res": int(g("vapor_eyes.h3_res", "6")),
        "qa_min": float(g("vapor_eyes.qa_min", "0.5")),
        "cloud_max": int(g("vapor_eyes.cloud_max", "20")),
        "s2_h3_res": int(g("vapor_eyes.s2_h3_res", "10")),
        "k_candidates": int(g("vapor_eyes.k_candidates", "5")),
        "min_z": int(g("vapor_eyes.min_z", "6")),
        "max_z": int(g("vapor_eyes.max_z", "13")),
        "overview_max_z": int(g("vapor_eyes.overview_max_z", "12")),
    }


def paths(spark):
    c = cfg(spark)
    root = f"/Volumes/{c['catalog']}/{c['schema']}/{c['volume']}/vapor-eyes-lf"
    return {
        "root": root,
        "s5p": f"{root}/s5p",
        "s2": f"{root}/sentinel2",
        "emit": f"{root}/emit",
        "wells": f"{root}/wells",
        "tiles": f"{root}/tiles",
        "schema_loc": f"{root}/_schema",     # Auto Loader schema locations
    }


def register_gbx(spark):
    """Register GeoBrix light SQL functions + DS readers. Call from function bodies."""
    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.pyvx import functions as vx
    from databricks.labs.gbx.ds.register import register as register_ds
    rx.register(spark)
    vx.register(spark)
    register_ds(spark)
