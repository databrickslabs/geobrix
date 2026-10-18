"""Silver layer: LiDAR surface rasters for the Wireless Coverage pipeline.

Four materialized views port the shipped Wireless Coverage notebooks' surface
transforms into the declarative pipeline, config-driven via ``_config.cfg``:

  wc_lidar_pts    classified returns, tile-indexed (tx, ty + tile extent) with a
                  res-15 H3 cellid -- ports ``01_pure_lidar_chm`` "Load the point
                  cloud and assign tiles". GeoBrix ``read_paths`` reads exactly the
                  .laz files the bronze ``laz_inventory`` lists.
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

# Tasks the .laz files are spread over when they are read: the same 512-way
# fan-out the surface steps use, so a full-SF update parses its tens of thousands
# of EPT nodes in parallel on Serverless.
_LAZ_TASKS = 512


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
    from databricks.labs.gbx.ds.paths import read_paths
    from databricks.labs.gbx.sample.lidar import aoi_lonlat_to_3857

    srid = c["srid"]
    tile_m = c["tile_m"]
    # AOI bbox (lon/lat) -> EPSG:3857 tile-grid origin + extent. full_aoi selects the
    # bbox (full SF vs Golden Gate demo), replacing the notebook's DEMO +/-2 tile window.
    x0, y0, x1, y1 = aoi_lonlat_to_3857(*c["bbox"])

    # GeoBrix read_paths runs the lidar_gbx reader over exactly the files
    # laz_inventory lists (the bronze table is also the dependency edge): one row
    # per return (x, y, z, intensity, return_number, number_of_returns,
    # classification, gps_time) in native EPSG:3857, plus `_source_path` (the file
    # it came from). schema_path: the LAZ folder, whose first .laz gives the
    # columns (no pipeline table is read while the flow is analyzed).
    # on_error="skip": a file or chunk that cannot be decoded is skipped with a
    # warning instead of failing the update. The inventory has no modification
    # time, so the NULL `_source_mtime` is dropped. Clip to the AOI, then stamp
    # each return with its tile key (tx, ty) and that tile's extent.
    pts = (
        read_paths(
            spark.read.table("laz_inventory"),
            "lidar_gbx",
            schema_path=p["laz"],
            mtime_col=None,
            num_partitions=_LAZ_TASKS,
            on_error="skip",
        )
        .drop("_source_mtime")
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
    #
    # Per-tile fan-out (Serverless-safe): the dsm x dtm join is small enough that
    # Spark would broadcast it (no (tx,ty) shuffle), piling every tile into one
    # task -- that task then holds all tiles' raster bytes AND runs every rst_chm
    # warp, OOMing a light Serverless worker. repartition by (tx,ty) forces one
    # tile per task (a bounded ~tens-of-MB warp each), and scales to full SF
    # (more tiles -> more tasks) without a bigger worker. Mirrors the gold step.
    return (
        dsm.join(dtm, ["tx", "ty"])
        .repartition(512, "tx", "ty")
        .select("tx", "ty", rx.rst_chm("dsm", "dtm").alias("chm"))
    )
