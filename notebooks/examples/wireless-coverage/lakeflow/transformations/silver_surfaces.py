"""Silver layer: LiDAR surface rasters for the Wireless Coverage pipeline.

Four materialized views port the shipped Wireless Coverage notebooks' surface
transforms into the declarative pipeline, config-driven via ``_config.cfg``:

  wc_lidar_pts    classified returns, tile-indexed (tx, ty + tile extent) with a
                  res-15 H3 cellid -- ports ``01_pure_lidar_chm`` "Load the point
                  cloud and assign tiles".
  wc_surface_dsm  max-z of all returns per 1 m tile (``bin_points_tiled``) --
                  ports ``01_pure_lidar_chm`` "DSM and DTM" (the DSM half).
  wc_surface_dtm  Delaunay-TIN bare earth over class-2 ground returns, capped at
                  ``tin_max_pts`` per tile (``rst_dtmfromgeoms_agg``) -- ports
                  ``02a_surfaces_dsm_dtm_chm`` "Bare-earth DTM".
  wc_surface_chm  ``rst_chm(dsm, dtm)`` (DSM - DTM, clamped >= 0) -- ports
                  ``02a_surfaces_dsm_dtm_chm`` "CHM".

``register_gbx(spark)`` is called INSIDE each dataset body (never at import);
upstream reads are wired with ``spark.read.table(...)`` so the pipeline sees the
dependency edges. Full-vs-demo is the ``full_aoi`` config toggle (it selects the
``bbox``), replacing the notebooks' DEMO window; the notebook constants
(``X0..Y1``, ``TILE_M``, ``TPX``, ``SRID``, ``TIN_MAX_PTS``) come from ``cfg``.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from _config import cfg, paths, register_gbx  # correct under the pipeline root_path


@dp.materialized_view(
    name="wc_lidar_pts",
    comment="Classified LiDAR returns, tile-indexed with a res-15 H3 cellid (EPSG:3857).",
)
@dp.expect("has_xyz", "x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL")
def wc_lidar_pts():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    from databricks.labs.gbx.sample.lidar import aoi_lonlat_to_3857

    # Dependency edge on the bronze inventory; the .laz are read directly below.
    _ = spark.read.table("laz_inventory")

    srid = c["srid"]
    tile_m = c["tile_m"]
    # AOI bbox (lon/lat) -> EPSG:3857 tile-grid origin + extent. full_aoi selects the
    # bbox (full SF vs Golden Gate demo), replacing the notebook's DEMO +/-2 tile window.
    x0, y0, x1, y1 = aoi_lonlat_to_3857(*c["bbox"])

    # lidar_gbx: one row per return (x, y, z, intensity, return_number,
    # number_of_returns, classification, gps_time) in native EPSG:3857. Clip to the
    # AOI, then stamp each return with its tile key (tx, ty) and that tile's extent.
    pts = (
        spark.read.format("lidar_gbx")
        .load(p["laz"])
        .where(
            (F.col("x") >= x0)
            & (F.col("x") <= x1)
            & (F.col("y") >= y0)
            & (F.col("y") <= y1)
        )
        .withColumn("tx", F.floor((F.col("x") - F.lit(x0)) / F.lit(tile_m)).cast("int"))
        .withColumn("ty", F.floor((F.col("y") - F.lit(y0)) / F.lit(tile_m)).cast("int"))
        .withColumn("t_xmin", F.lit(x0) + F.col("tx") * F.lit(tile_m))
        .withColumn("t_ymin", F.lit(y0) + F.col("ty") * F.lit(tile_m))
        .withColumn("t_xmax", F.col("t_xmin") + F.lit(tile_m))
        .withColumn("t_ymax", F.col("t_ymin") + F.lit(tile_m))
    )

    # Res-15 "H3 pixel" on every return: reproject native x/y to lon/lat and index.
    # Res 15 is the finest baseline -- any downstream step rolls up to a coarser
    # resolution via h3_toparent + a GROUP BY (max z = surface, min z = ground, ...).
    pts = (
        pts.withColumn(
            "_ll4326", F.expr(f"st_transform(st_setsrid(st_point(x, y), {srid}), 4326)")
        )
        .withColumn(
            "cellid_r15", F.expr("h3_longlatash3(st_x(_ll4326), st_y(_ll4326), 15)")
        )
        .drop("_ll4326")
    )
    return pts


@dp.materialized_view(
    name="wc_surface_dsm",
    comment="DSM -- max elevation of all returns per 1 m tile (EPSG:3857).",
)
@dp.expect("tile_present", "dsm IS NOT NULL")
def wc_surface_dsm():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    from databricks.labs.gbx.pyrx import functions as rx

    pts = spark.read.table("wc_lidar_pts")
    tpx = int(c["tile_m"] / c["pixel_m"])
    # bin_points_tiled bins each tile with memory bounded by the raster grid (W x H),
    # NOT the point count -- so a full-resolution tile never OOMs a worker. max z of
    # all returns is the DSM (surface).
    return rx.bin_points_tiled(
        pts,
        x="x",
        y="y",
        z="z",
        by=["tx", "ty"],
        xmin="t_xmin",
        ymin="t_ymin",
        xmax="t_xmax",
        ymax="t_ymax",
        width=tpx,
        height=tpx,
        srid=c["srid"],
        stat="max",
    ).withColumnRenamed("tile", "dsm")


@dp.materialized_view(
    name="wc_surface_dtm",
    comment="DTM -- Delaunay-TIN bare earth over class-2 ground returns (EPSG:3857).",
)
@dp.expect("tile_present", "dtm IS NOT NULL")
def wc_surface_dtm():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    from databricks.labs.gbx.pyrx import functions as rx

    pts = spark.read.table("wc_lidar_pts")
    tpx = int(c["tile_m"] / c["pixel_m"])
    srid = c["srid"]
    tin_max_pts = c["tin_max_pts"]

    # Ground returns (ASPRS class 2) only; cap each TILE at tin_max_pts so every
    # tile's triangulation fits in memory (a per-tile random bounded sample via
    # row_number -- the cap is per-tile, not global).
    ground = pts.where(F.col("classification") == 2).select(
        "tx", "ty", "t_xmin", "t_ymin", "t_xmax", "t_ymax", "x", "y", "z"
    )
    w = Window.partitionBy("tx", "ty").orderBy(F.rand(42))
    ground_s = (
        ground.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") <= tin_max_pts)
        .drop("_rn")
        .withColumn("pt", F.expr("st_asbinary(st_makepoint(x, y, z))"))
    )
    # One Delaunay-TIN DTM tile per (tx, ty) via the streaming rst_dtmfromgeoms_agg
    # aggregator -- a continuous, gap-free bare earth, the production DTM.
    return ground_s.groupBy("tx", "ty").agg(
        rx.rst_dtmfromgeoms_agg(
            "pt",
            F.lit(None),
            F.lit(0.0),
            F.lit(0.0),
            "t_xmin",
            "t_ymin",
            "t_xmax",
            "t_ymax",
            F.lit(tpx),
            F.lit(tpx),
            F.lit(srid),
        ).alias("dtm")
    )


@dp.materialized_view(
    name="wc_surface_chm",
    comment="CHM -- rst_chm(DSM, DTM): canopy/structure height in metres, clamped >= 0.",
)
@dp.expect("tile_present", "chm IS NOT NULL")
def wc_surface_chm():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    from databricks.labs.gbx.pyrx import functions as rx

    dsm = spark.read.table("wc_surface_dsm")
    dtm = spark.read.table("wc_surface_dtm")
    # rst_chm warps the DSM onto the DTM grid, differences them (DSM - DTM), and
    # clamps negatives to 0 -> canopy/structure height in metres.
    return dsm.join(dtm, ["tx", "ty"]).select(
        "tx", "ty", rx.rst_chm("dsm", "dtm").alias("chm")
    )
