"""Shared config for the Wireless Coverage Lakeflow pipeline. cfg()/paths() read the
pipeline `configuration` block (spark.conf); register_gbx() installs GeoBrix light SQL +
readers and is called INSIDE dataset function bodies (never at import)."""

_FULL_SF = (-122.55, 37.70, -122.35, 37.85)
_DEMO_GGP = (-122.52, 37.76, -122.45, 37.78)  # Golden Gate Park window


def _g(spark, key, default):
    return spark.conf.get(f"wireless_coverage.{key}", default)


def cfg(spark):
    full = str(_g(spark, "full_aoi", "true")).lower() == "true"
    return {
        "catalog": _g(spark, "catalog", "geospatial_docs"),
        "schema": _g(spark, "schema", "wireless_coverage_lf"),
        "volume": _g(spark, "volume", "data"),
        "full_aoi": full,
        "bbox": _FULL_SF if full else _DEMO_GGP,
        "laz_dir": _g(spark, "laz_dir", ""),
        "water_mask_dir": _g(spark, "water_mask_dir", ""),
        "pixel_m": float(_g(spark, "pixel_m", "1.0")),
        "tile_m": float(_g(spark, "tile_m", "1024.0")),
        "srid": int(_g(spark, "srid", "3857")),
        "h3_res": int(_g(spark, "h3_res", "10")),
        "join_res": int(_g(spark, "join_res", "9")),
        "breaks_m": [float(x) for x in _g(spark, "breaks_m", "0,30,60,90,120,150,180,210,240,270,300").split(",")],
        "tin_max_pts": int(_g(spark, "tin_max_pts", "150000")),
        "tower_res": int(_g(spark, "tower_res", "9")),
        "quickpass_res": int(_g(spark, "quickpass_res", "10")),
        "viewshed_res": int(_g(spark, "viewshed_res", "12")),
        "surface_bin_res": int(_g(spark, "surface_bin_res", "13")),
        "max_dist_m": float(_g(spark, "max_dist_m", "2500")),
        "antenna_above_surface_m": float(_g(spark, "antenna_above_surface_m", "10")),
        "target_h": float(_g(spark, "target_h", "1.6")),
        "quick_thresh_frac": float(_g(spark, "quick_thresh_frac", "0.30")),
        "ground_fill_k": int(_g(spark, "ground_fill_k", "3")),
    }


def paths(spark):
    c = cfg(spark)
    root = f"/Volumes/{c['catalog']}/{c['schema']}/{c['volume']}/wireless-coverage-lf"
    laz = c["laz_dir"] or f"{root}/lidar/sf/laz"
    water = c["water_mask_dir"] or f"{root}/overture-water"
    return {"root": root, "laz": laz, "water": water, "schema_loc": f"{root}/_schema"}


def register_gbx(spark):
    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.pygx import functions as gx
    from databricks.labs.gbx.ds.register import register as register_ds
    rx.register(spark)
    gx.register(spark)
    register_ds(spark)
