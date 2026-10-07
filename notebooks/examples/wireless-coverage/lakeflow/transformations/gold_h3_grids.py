"""Gold layer: H3-gridded elevation / canopy surfaces for the Wireless Coverage
pipeline.

Three materialized views port notebook ``03a_h3_gridding`` into the declarative
pipeline, config-driven via ``_config.cfg``:

  wc_h3_dem_res10  Bare-earth (DTM) elevation-band H3 cells (res 10) -- 03a's
                   ``_surface_to_h3_cells`` over ``wc_surface_dtm``.
  wc_h3_dsm_res10  Surface (DSM, incl. buildings/canopy) elevation-band H3 cells
                   -- 03a's ``_surface_to_h3_cells`` over ``wc_surface_dsm``.
  wc_h3_chm_res10  Max canopy/structure height per H3 cell -- 03a's
                   ``gbx_rst_h3_rastertogridmax`` path over ``wc_surface_chm``,
                   clamped to MAX_CHM_M and land-masked.

Every H3 table carries a stable res-``join_res`` parent-cell join key
(``parent_cellid`` / ``parent_cellid_res``) so the Part-4 tower analysis joins on
one column; ``@dp.expect_or_fail`` guards it non-null on all three.

``register_gbx(spark)`` is called INSIDE each dataset body (never at import); the
upstream reads use ``spark.read.table`` so the pipeline sees the dependency edges
on the silver surfaces. The helper chain (``_surface_to_h3_cells``,
``_with_join_parent``, ``_land_wkb``) reproduces ``config_nb``'s helpers, which
the pipeline cannot ``%run``-import. Params (``breaks_m``, ``h3_res``,
``join_res``, ``bbox``) and the water-mask dir come from ``cfg``/``paths``;
full-vs-demo is the ``full_aoi`` config toggle.

Note: none of the three MVs is gap-filled, and no MV does an eager action on
UPSTREAM pipeline data, so declarative flow analysis against empty upstreams does
not crash. (``_land_wkb`` does read the staged water file on the driver -- but
that is a staged EXTERNAL reference, present during flow analysis, which is why
the DEM/DSM MVs that also call it analyze fine.) DEM/DSM H3 band cells are
gap-free by construction (``rst_isoband`` -> ``h3_try_coverash3`` tessellates each
elevation band solidly) and land-masked via ``rst_clip``; CHM is
max-canopy-per-cell, clamped and land-masked. 03a's optional k=1 IDW cell
gap-fill is dropped here because it pulled an empty upstream aggregate to the
driver, which does not belong in a ``@dp.materialized_view`` body.
"""

from pyspark import pipelines as dp
from pyspark.databricks.sql import functions as DBF
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from _config import cfg, paths, register_gbx  # correct under the pipeline root_path

# Artifact ceiling for CHM max-canopy before gap-fill. CHM = DSM - DTM bins all
# returns at max z, so towers read as canopy; SF's tallest structure (Salesforce
# Tower) is ~326 m, so 350 m keeps every real object while dropping LiDAR
# artifacts (multi-path reflections, flying objects, DTM underestimates). Ports
# 03a's MAX_CHM_M constant (a fixed physical ceiling, not a cfg knob).
_MAX_CHM_M = 350.0

# Simplify tolerance (DEGREES) for isoband polygons before H3 covering.
# ~3e-5 deg ~= 3 m, FAR below the ~65 m res-10 H3 cell edge, so the covered-cell
# set is unchanged while the dense 1 m pixel-staircase vertex rings collapse. The
# DSM chain spilled ~1 TB covering ~400K isoband polygons of up to ~471K vertices
# each (geometry VOLUME, not an h3-cover explosion); simplifying the polygons
# first removes the spill without changing which cells are produced.
_SIMPLIFY_TOL_DEG = 3e-5

def _land_wkb(bbox, water_dir):
    """AOI box minus Overture water -> EPSG:4326 land multipolygon WKB.

    Ports 03a's land-mask cell: reads the staged Overture base/water GeoParquet
    produced by the land task (``paths["water"]``), subtracts it from the AOI
    box, and returns the land-polygon WKB used as the ``rst_clip`` cutline
    (DEM/DSM) and the ``land_cells`` source (CHM). Falls back to the full AOI box
    WKB when no water is staged (land task not run / dry AOI).
    """
    import glob

    from shapely import wkb as _wkb
    from shapely.geometry import box as _box
    from shapely.ops import unary_union

    from databricks.labs.gbx.sample.overture import OvertureClient

    w, s, e, n = bbox
    if not glob.glob(f"{water_dir}/**/*.parquet", recursive=True):
        return _box(w, s, e, n).wkb
    water = OvertureClient().read(water_dir, theme="base", type="water", bbox=bbox)
    geoms = [
        _wkb.loads(bytes(r["geometry"])) for r in water.select("geometry").collect()
    ]
    land = (
        _box(w, s, e, n).difference(unary_union(geoms)) if geoms else _box(w, s, e, n)
    )
    return land.wkb


def _surface_to_h3_cells(
    surface_df,
    tile_col,
    breaks_array,
    h3_res,
    land_wkb,
    simplify_geom: bool = True,
    clip: bool = True,
):
    """EPSG:3857 surface tiles -> elevation isobands -> H3 cells (EPSG:4326).

    Ports ``config_nb._surface_to_h3_cells``: reproject 3857 -> 4326
    (``h3_try_coverash3`` needs lon/lat WKB), ``rst_clip`` to the land polygon
    (Bay/Pacific -> NoData), ``rst_isoband`` elevation bands, then (when
    ``simplify_geom``, the default) product ``st_simplify`` on the pixel-staircase
    band polygons (see ``_SIMPLIFY_TOL_DEG`` -- << the res-10 cell edge, so covered
    cells are unchanged), then ``h3_try_coverash3`` + explode -> solid, gap-free
    H3 cells per band. Pass ``simplify_geom=False`` for the exact (unsimplified)
    isoband geometry -- the no-simplify baseline. Returns
    (cellid LONG, res INT, band_level INT, elev_lo DOUBLE, elev_hi DOUBLE).
    """
    from databricks.labs.gbx.pyrx import functions as rx

    # One tile per task keeps Python-UDF worker memory bounded at full-SF scale
    # (Serverless fan-out is repartition(N, column), not sparkContext).
    surface_df = surface_df.repartition(512, "tx", "ty")
    t4326 = surface_df.select(
        "tx",
        "ty",
        rx.rst_transform(tile_col, F.lit(4326)).alias("tile_4326"),
    )
    # clip=False skips the raster land-clip (rst_clip SIGABRTs natively on the TIN
    # DTM grid at full-SF scale); callers that skip it mask land in H3 space after
    # covering instead. The DSM path keeps clip=True (works + masks before isoband).
    if clip and land_wkb is not None:
        clipped = t4326.select(
            "tx",
            "ty",
            rx.rst_clip("tile_4326", F.lit(land_wkb), F.lit(True)).alias("tile"),
        )
    else:
        clipped = t4326.withColumnRenamed("tile_4326", "tile")
    patches = clipped.select(
        F.explode(rx.rst_isoband("tile", breaks_array)).alias("p")
    ).select(
        F.col("p.band").alias("band_level"),
        F.col("p.geom_wkb").alias("geom_wkb"),
        F.col("p.lower").alias("elev_lo"),
        F.col("p.upper").alias("elev_hi"),
    )
    # Collapse the 1 m pixel-staircase isoband rings before H3 covering: this is
    # what kept the DSM chain from spilling ~1 TB (~400K polygons up to ~471K
    # vertices each). Product st_simplify resolves on DBR 18.3 serverless and runs
    # in Photon (no per-polygon Python). It is NOT topology-preserving, so it can
    # self-intersect a small fraction of polygons -> h3_try_coverash3 returns NULL
    # and the cellid-not-null filter drops them (a tiny cell loss). The geobrix
    # topology-preserving fallback (a shapely UDF) was tried but OOMs the driver on
    # the 471K-vertex polygons, so product st_simplify is the chosen path.
    # simplify_geom=False skips this to materialize the exact (unsimplified)
    # baseline for the simplify-vs-exact comparison.
    if simplify_geom:
        patches = patches.withColumn(
            "geom_wkb",
            F.expr(
                f"st_asbinary(st_simplify(st_geomfromwkb(geom_wkb), {_SIMPLIFY_TOL_DEG}))"
            ),
        )
    cells = (
        patches.select(
            "band_level",
            "elev_lo",
            "elev_hi",
            F.explode(DBF.h3_try_coverash3(F.col("geom_wkb"), F.lit(h3_res))).alias(
                "cellid"
            ),
        )
        .where(F.col("cellid").isNotNull())
        .distinct()
        .withColumn("res", F.lit(h3_res))
    )
    return cells.select("cellid", "res", "band_level", "elev_lo", "elev_hi")


def _with_join_parent(df, join_res, cell_col="cellid"):
    """Add the stable res-``join_res`` parent-cell join key (ports ``with_join_parent``).

    parent_cellid     = DBF.h3_toparent(<cell_col>, join_res)
    parent_cellid_res = join_res

    Column names are stable so downstream grids keyed at H3 res ``join_res`` join
    on one column; only ``join_res`` changes if the join resolution does.
    """
    return df.withColumn(
        "parent_cellid", DBF.h3_toparent(cell_col, F.lit(join_res))
    ).withColumn("parent_cellid_res", F.lit(join_res))


@dp.materialized_view(
    name="wc_h3_dem_res10",
    comment="DEM (bare-earth) elevation-band H3 cells (res 10), land-masked, with parent join key.",
)
@dp.expect_or_fail("parent_key", "parent_cellid IS NOT NULL")
def wc_h3_dem_res10():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    # DEM = bare-earth DTM elevation bands.
    dtm = spark.read.table("wc_surface_dtm")
    breaks_arr = F.array(*[F.lit(b) for b in c["breaks_m"]])
    land_wkb = _land_wkb(c["bbox"], p["water"])
    # The DTM is a Delaunay TIN: rst_clip SIGABRTs natively on its grid at full-SF
    # scale (the binned DSM clips fine). Skip the raster clip (clip=False) and mask
    # land in H3 space with an inner-join instead -- the TIN already leaves water as
    # NoData (out-of-hull), so the isoband is land-only and the join just trims any
    # stray non-land cells. Same land-mask approach as the CHM MV.
    cells = _surface_to_h3_cells(
        dtm,
        "dtm",
        breaks_arr,
        c["h3_res"],
        land_wkb,
        simplify_geom=c["simplify"],
        clip=False,
    )
    land_cells = (
        spark.range(1)
        .select(
            F.explode(DBF.h3_try_coverash3(F.lit(land_wkb), F.lit(c["h3_res"]))).alias(
                "cellid"
            )
        )
        .where(F.col("cellid").isNotNull())
        .select("cellid")
    )
    cells = cells.join(land_cells, "cellid", "inner")
    return _with_join_parent(cells, c["join_res"])


@dp.materialized_view(
    name="wc_h3_dsm_res10",
    comment="DSM (surface incl. buildings/canopy) elevation-band H3 cells (res 10), land-masked, with parent join key.",
)
@dp.expect_or_fail("parent_key", "parent_cellid IS NOT NULL")
def wc_h3_dsm_res10():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    # DSM = surface model (buildings + canopy) elevation bands.
    dsm = spark.read.table("wc_surface_dsm")
    breaks_arr = F.array(*[F.lit(b) for b in c["breaks_m"]])
    land_wkb = _land_wkb(c["bbox"], p["water"])
    cells = _surface_to_h3_cells(
        dsm, "dsm", breaks_arr, c["h3_res"], land_wkb, simplify_geom=c["simplify"]
    )
    return _with_join_parent(cells, c["join_res"])


@dp.materialized_view(
    name="wc_h3_chm_res10",
    comment="CHM max canopy/structure height per H3 cell (res 10): clamped and land-masked, with parent join key.",
)
@dp.expect_or_fail("parent_key", "parent_cellid IS NOT NULL")
def wc_h3_chm_res10():
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    from databricks.labs.gbx.pyrx import functions as rx

    chm = spark.read.table("wc_surface_chm")
    h3_res = c["h3_res"]
    land_wkb = _land_wkb(c["bbox"], p["water"])

    # rst_h3_rastertogridmax (an H3 UDTF) requires lon/lat EPSG:4326 tiles. Both it
    # and rst_transform are Python ops that load a tile's full band, so tiles MUST
    # be fanned out one-per-task. Repartition the RAW tiles FIRST, THEN reproject,
    # so the transform runs post-shuffle (one tile/task) -- matching the DEM/DSM
    # path in _surface_to_h3_cells; doing the transform inside the pre-repartition
    # select leaves it in the scan stage, reprojecting many tiles per task -> OOM.
    # The pipeline also excludes the InferFiltersFromGenerate optimizer rule (else
    # the LATERAL-UDTF generate is pushed into the scan ahead of the repartition --
    # ref Databricks ES-2236400) and turns AQE coalescePartitions off (else the
    # fanned-out shuffle is merged back by the small inlined-raster row count). See
    # databricks.yml.
    chm_4326 = chm.repartition(512, "tx", "ty").select(
        "tx", "ty", rx.rst_transform("chm", F.lit(4326)).alias("chm_4326")
    )
    chm_4326.createOrReplaceTempView("_wc_chm_4326_tiles")

    # SQL ANSI LATERAL UDTF (the Python wrapper raises NotImplementedError, so
    # invoke via spark.sql). Verified schema (band INT, cellID LONG, measure
    # DOUBLE); band=1 is the single CHM band. Multiple tiles may map the same
    # cellid at tile boundaries -> take the max across tiles.
    chm_cells = (
        spark.sql(
            f"""
        SELECT t.cellID AS cellid, t.measure AS chm_z
        FROM   _wc_chm_4326_tiles,
               LATERAL gbx_rst_h3_rastertogridmax(chm_4326, {h3_res}) t
        WHERE  t.band = 1
        """
        )
        .groupBy("cellid")
        .agg(F.max("chm_z").alias("max_chm_z"))
    )

    # Drop non-physical CHM outliers (> MAX_CHM_M): LiDAR artifacts (multi-path
    # reflections, flying objects, DTM underestimates) above any real structure.
    chm_clean = chm_cells.where(F.col("max_chm_z") <= F.lit(_MAX_CHM_M))

    # Land mask: keep only cells inside the land polygon (drops Bay/Pacific-
    # adjacent cells). spark.range(1) seeds the array explode lazily -- a
    # driver-free seed, so the MV stays lazy for declarative flow analysis.
    land_cells = (
        spark.range(1)
        .select(
            F.explode(DBF.h3_try_coverash3(F.lit(land_wkb), F.lit(h3_res))).alias(
                "cellid"
            )
        )
        .where(F.col("cellid").isNotNull())
        .select("cellid")
    )
    chm_h3 = (
        chm_clean.join(land_cells, "cellid", "inner")
        .withColumn("res", F.lit(h3_res))
        .select("cellid", "res", "max_chm_z")
    )
    return _with_join_parent(chm_h3, c["join_res"])
