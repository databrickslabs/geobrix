"""Wireless Coverage Lakeflow -- siting task (Part 4).

Ports ``notebooks/examples/wireless-coverage/04_tower_viewsheds.ipynb`` into the
DAB job's post-pipeline ``spark_python_task``. It reads the silver surfaces the
pipeline published (``wc_surface_dsm`` / ``wc_surface_dtm``), bins them to H3
"pixels", builds the naive candidate-tower lattice, rules most sites out with a
cheap coarse quick-pass, runs the exact per-tower viewshed on the survivors, and
writes six regular tables the workshop consumes.

Entry point: ``main(argv)`` -- invoked by the DAB job task::

    python siting/siting.py \\
        --catalog geospatial_docs \\
        --schema  wireless_coverage_lf \\
        --volume  data \\
        --full-aoi true

Key difference from notebook 04: the expensive exact (fine-resolution) viewshed
no longer runs as a driver-side ``tower_viewshed`` / ``h3_los_visible`` loop. It
goes through the distributed ``databricks.labs.gbx.pygx.h3_viewshed_towers``
primitive (``mapInPandas``, one tower per task, no ``sparkContext`` / ``.rdd`` /
``_jvm``), so the siting pass scales to the full San Francisco AOI. The cheap,
coarse quick-pass screen stays a driver-side loop by design (that is the whole
point of a quick-pass: rule most sites out before any detailed work).

Algorithm/resolution parameters come from ``transformations/_config.cfg`` so the
siting task and the pipeline agree (``surface_bin_res`` 13 fine bin,
``viewshed_res`` 12 exact + coverage output, ``quickpass_res`` 10 coarse screen,
``tower_res`` 9 candidate lattice). ``catalog`` / ``schema`` / ``full-aoi`` come
from the CLI (the job task does not carry the pipeline's ``spark.conf`` block).

The vendored workflow policy -- quick-pass thresholding, candidate ranking,
required-mast nearest-ground fallback, the H3 binning and the resolution
pyramid -- lives in this file (brief spec section 10). The pure policy helpers
``quick_survivors`` / ``required_mast`` / ``rank_candidates`` are exposed at
module level and unit-tested without a cluster; all Spark / geobrix imports are
deferred into function bodies so the module imports offline.
"""

from __future__ import annotations

import argparse
import math
import sys

import h3

# Constants mirror transformations/_config._FULL_SF / _DEMO_GGP exactly. The CLI
# --full-aoi flag selects the AOI; _config.cfg's spark.conf-driven bbox is not
# used because the job task does not carry the pipeline's configuration block.
_FULL_SF = (-122.55, 37.70, -122.35, 37.85)
_DEMO_GGP = (-122.52, 37.76, -122.45, 37.78)  # Golden Gate Park window

# Output tables (regular Delta tables, overwritten each run).
_T_SURF_R13 = "wc_h3_surf_r13"
_T_GRND_R13 = "wc_h3_grnd_r13"
_T_QUICKPASS = "wc_h3_quickpass_res9"
_T_CANDIDATES = "wc_h3_candidate_sites_res9"
_T_COVERAGE = "wc_h3_coverage_res12"
_T_COVERAGE_TOP10 = "wc_h3_coverage_top10_res12"


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def parse_args(argv):
    """Parse CLI arguments; exposed so tests can call it directly."""
    p = argparse.ArgumentParser(
        description="GeoBrix WC siting task -- Part-4 tower viewsheds at scale"
    )
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    p.add_argument("--schema", required=True, help="UC schema name")
    p.add_argument("--volume", required=True, help="UC volume name")
    p.add_argument(
        "--full-aoi",
        required=True,
        dest="full_aoi",
        help="'true' for full SF AOI; 'false' for the demo GGP window",
    )
    return p.parse_args(argv)


def _resolve_aoi(args):
    """Resolve the AOI bbox from --full-aoi, mirroring _config.cfg()."""
    full = str(args.full_aoi).lower() == "true"
    return _FULL_SF if full else _DEMO_GGP


# --------------------------------------------------------------------------- #
# Vendored policy helpers (pure -- unit-tested without a cluster)
# --------------------------------------------------------------------------- #


def quick_survivors(rows, thresh):
    """Keep candidate rows whose coarse quick-pass fraction clears ``thresh``.

    Ports nb04's survivor filter (``quick_frac >= QUICK_THRESH_FRAC``). ``rows``
    is an iterable of ``(lon, lat, quick_frac)`` tuples; returns the subset at or
    above ``thresh`` (inclusive), preserving order. Most candidates die here, so
    they never pay for the detailed viewshed.
    """
    return [r for r in rows if r[2] >= thresh]


def _nearest_ground(cell, gmap, k_max):
    """Mean bare-earth of the nearest populated H3 ring (ports nb04).

    Expand H3 ``grid_disk`` radius 1..k_max and return the mean ground of the
    first populated radius. ``None`` only when no ground lies within ``k_max``
    rings (genuine no-data, e.g. open water) -- never fabricated.
    """
    for k in range(1, k_max + 1):
        vals = [gmap[c] for c in h3.grid_disk(cell, k) if gmap.get(c) is not None]
        if vals:
            return sum(vals) / len(vals)
    return None


def required_mast(cell, gmap, k):
    """Resolve the bare-earth datum used to size a tower's required mast.

    Returns the exact DTM@``cell`` when present, else the nearest-ground
    ring-fallback mean within ``k`` rings (``_nearest_ground``), else ``None``
    when no ground lies within ``k`` rings -- an honestly uncostable site rather
    than a guessed one. The mast height itself is ``(surface - this) + antenna``
    and is computed by the caller (nb04: ``g = g_exact if g_exact is not None
    else _nearest_ground(...)`` then ``(st - g) + ANTENNA``).
    """
    g = gmap.get(cell)
    if g is not None:
        return g
    return _nearest_ground(cell, gmap, k)


def rank_candidates(rows):
    """Order sited candidates by coverage desc, then required mast asc.

    Ports nb04's ``orderBy(viewshed_cells.desc(), required_mast.asc_nulls_last())``.
    ``rows`` is a list of dicts carrying ``viewshed_cells`` and ``required_mast``
    (``None`` mast sorts last among equal-coverage ties -- uncostable sites rank
    behind costable ones). Returns a new sorted list; input is not mutated.
    """
    return sorted(
        rows,
        key=lambda r: (
            -r["viewshed_cells"],
            r["required_mast"] is None,
            r["required_mast"] if r["required_mast"] is not None else 0.0,
        ),
    )


def _buffer_k(radius_m, res):
    """H3 ring radius covering ``radius_m`` at resolution ``res``.

    Mirrors the buffer sizing inside ``pygx.h3_viewshed_towers`` so the quick-pass
    screen and the viewshed-fraction denominator (``n_targets``) count the same
    neighbourhood the primitive actually tests. Hex centres are ~1.73*edge apart;
    dividing by 1.5 (< 1.73) is a safe over-estimate of k.
    """
    edge_m = h3.average_hexagon_edge_length(int(res), unit="m")
    return max(1, int(math.ceil(float(radius_m) / edge_m / 1.5)))


# --------------------------------------------------------------------------- #
# Spark-stage helpers (lazy imports -> module still loads offline)
# --------------------------------------------------------------------------- #


def _bin_h3(spark, surface_df, col, res, agg):
    """Bin an EPSG:3857 surface-tile column to H3 "pixels" at ``res`` (ports nb04).

    Reproject each tile to EPSG:4326 (the H3 UDTFs require lon/lat), then the
    ANSI-LATERAL ``gbx_rst_h3_rastertogrid{max|min}`` UDTF -> one z per cell:
    ``max`` for the DSM blocker surface, ``min`` for the DTM bare earth. The
    Python wrapper for the UDTF is not implemented, so it is invoked via SQL.
    Returns ``(cellid LONG, z DOUBLE)``. Serverless fan-out is ``repartition(N,
    column)`` -- one tile per task keeps the per-tile raster memory bounded.
    """
    from databricks.labs.gbx.pyrx import functions as rx
    from pyspark.sql import functions as F

    view = f"_wc_siting_bin_{agg}_{res}"
    (
        surface_df.repartition(64, "tx", "ty")
        .select(rx.rst_transform(col, F.lit(4326)).alias("t4326"))
        .createOrReplaceTempView(view)
    )
    return spark.sql(
        f"SELECT t.cellID AS cellid, {agg}(t.measure) AS z "
        f"FROM {view}, LATERAL gbx_rst_h3_rastertogrid{agg}(t4326, {res}) t "
        f"WHERE t.band = 1 GROUP BY t.cellID"
    )


def _downsample(base_r13, res, agg):
    """Downsample the res-13 base to ``res`` (ports nb04's ``_collect`` pyramid).

    ``h3_toparent`` + ``agg`` (``max`` surface / ``min`` ground) -- exact, since
    max/min are associative, so the res-13 raster is read only once. Returns
    ``(cellid LONG, z DOUBLE)`` at ``res``.
    """
    from pyspark.databricks.sql import functions as DBF
    from pyspark.sql import functions as F

    agg_fn = F.max if agg == "max" else F.min
    return base_r13.groupBy(
        DBF.h3_toparent("cellid", F.lit(res)).alias("cellid")
    ).agg(agg_fn("z").alias("z"))


def _zmap(df):
    """Collect a ``(cellid, z)`` DataFrame to a ``{h3_str: z}`` driver dict.

    Bounded at ``viewshed_res`` / ``quickpass_res`` over a city (hundreds of
    thousands of cells) -- the same bounded collect ``h3_viewshed_towers`` does
    internally; keyed by the h3 string form the ``h3`` library is native to.
    """
    return {
        h3.int_to_str(int(r["cellid"])): r["z"]
        for r in df.select("cellid", "z").collect()
    }


def _with_join_parent(df, join_res):
    """Add the stable res-join_res parent-cell join key (ports with_join_parent).

    ``parent_cellid = h3_toparent(cellid, join_res)``; ``parent_cellid_res =
    join_res``. Column names are stable so grids keyed at res ``join_res`` join on
    one column.
    """
    from pyspark.databricks.sql import functions as DBF
    from pyspark.sql import functions as F

    return df.withColumn(
        "parent_cellid", DBF.h3_toparent("cellid", F.lit(join_res))
    ).withColumn("parent_cellid_res", F.lit(join_res))


def _write(df, catalog, schema, name):
    """Overwrite-write ``df`` to the regular table ``catalog.schema.name``."""
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{name}")
    )


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main(argv=None):
    """Run the Part-4 siting pipeline and write the six output tables."""
    import os

    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

    # transformations/_config lives one directory up (sibling of siting/); make it
    # importable for the spark_python_task (tests add it via conftest too).
    _lakeflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _lakeflow_dir not in sys.path:
        sys.path.insert(0, _lakeflow_dir)

    from databricks.labs.gbx.pygx import h3_los_visible, h3_viewshed_towers
    from pyspark.databricks.sql import functions as DBF
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from shapely.geometry import box as _box

    from transformations._config import cfg, register_gbx

    spark = SparkSession.builder.appName("wc-siting").getOrCreate()
    register_gbx(spark)  # install GeoBrix light SQL (gbx_rst_h3_rastertogrid*) + rx

    c = cfg(spark)  # algorithm/resolution params (job task -> _config defaults)
    cat, sch = args.catalog, args.schema
    bbox = _resolve_aoi(args)
    surface_bin_res = c["surface_bin_res"]  # 13 -- finest H3 "pixel" bin
    viewshed_res = c["viewshed_res"]  # 12 -- exact viewshed + coverage output
    quickpass_res = c["quickpass_res"]  # 10 -- coarse quick-pass screen
    tower_res = c["tower_res"]  # 9  -- naive candidate lattice
    join_res = c["join_res"]  # 9  -- coverage parent-cell join key
    max_dist = c["max_dist_m"]
    antenna = c["antenna_above_surface_m"]
    target_h = c["target_h"]
    quick_thresh = c["quick_thresh_frac"]
    ground_fill_k = c["ground_fill_k"]

    # --- Bin the surfaces to H3 and build the resolution pyramid (nb04 cell
    #     "Bin the surfaces to H3 pixels"). The silver surfaces are already AOI-
    #     scoped by the pipeline (full_aoi), so no tile-window filter is needed. ---
    dsm = spark.read.table(f"{cat}.{sch}.wc_surface_dsm")
    dtm = spark.read.table(f"{cat}.{sch}.wc_surface_dtm")

    surf_r13 = _bin_h3(spark, dsm, "dsm", surface_bin_res, "max")
    _write(surf_r13, cat, sch, _T_SURF_R13)
    surf_r13 = spark.table(f"{cat}.{sch}.{_T_SURF_R13}")  # materialize bin once

    grnd_r13 = _bin_h3(spark, dtm, "dtm", surface_bin_res, "min")
    _write(grnd_r13, cat, sch, _T_GRND_R13)
    grnd_r13 = spark.table(f"{cat}.{sch}.{_T_GRND_R13}")

    # Pyramid: res-13 -> viewshed_res (12) and -> quickpass_res (10), max for the
    # surface blocker, min for the bare-earth base. surf12/grnd12 feed the exact
    # viewshed; surf10 feeds the coarse screen.
    surf12_df = _downsample(surf_r13, viewshed_res, "max")
    grnd12_df = _downsample(grnd_r13, viewshed_res, "min")
    surf10_df = _downsample(surf_r13, quickpass_res, "max")

    surf10_map = _zmap(surf10_df)
    surf12_map = _zmap(surf12_df)
    grnd12_map = _zmap(grnd12_df)

    # --- Candidate towers: one per H3 cell centroid over the AOI (nb04 cell
    #     "Candidate towers -- the naive centroid lattice"). ---
    w, s, e, n = bbox
    aoi_wkb = _box(w, s, e, n).wkb
    cand_cells = [
        r["cell"]
        for r in spark.createDataFrame([(1,)], "g int")
        .select(
            F.explode(DBF.h3_try_coverash3(F.lit(aoi_wkb), F.lit(tower_res))).alias(
                "cell"
            )
        )
        .where(F.col("cell").isNotNull())
        .collect()
    ]
    # (lat, lon) centroid of each candidate cell.
    cand_latlng = [h3.cell_to_latlng(h3.int_to_str(c)) for c in cand_cells]

    # --- Quick pass: a cheap coarse-resolution viewshed per candidate; survivors
    #     clear quick_thresh (nb04 cell "Quick pass -- rule out weak sites").
    #     Driver-side by design (coarse -> cheap). Deviation from nb04: the target
    #     buffer is the primitive's H3 grid_disk, not a 3857 euclidean disk, so the
    #     screen and the exact pass share the same neighbourhood definition. ---
    k_quick = _buffer_k(max_dist, quickpass_res)
    scored = []
    for (lat, lon) in cand_latlng:
        tcell = h3.latlng_to_cell(lat, lon, quickpass_res)
        st = surf10_map.get(tcell)
        if st is None:
            continue  # tower on NoData surface
        obs_z = st + antenna
        targets = [cc for cc in h3.grid_disk(tcell, k_quick) if cc in surf10_map]
        # surface-relative line-of-sight: ground == surface (target sits on it).
        vis = h3_los_visible(tcell, obs_z, targets, surf10_map, surf10_map, target_h)
        frac = len(vis) / max(len(targets), 1)
        scored.append((float(lon), float(lat), round(frac, 4)))

    quickpass_df = spark.createDataFrame(
        scored, "lon double, lat double, quick_frac double"
    )
    _write(quickpass_df, cat, sch, _T_QUICKPASS)

    survivors = quick_survivors(scored, quick_thresh)

    # --- Detailed viewsheds on survivors via the DISTRIBUTED h3_viewshed_towers
    #     primitive (replaces nb04's driver-side fine tower_viewshed loop). Build a
    #     towers frame carrying the res-12 tower cell + observer_z (surface@tower +
    #     antenna == DTM@tower + required-mast); keep lon/lat/mast/n_targets for the
    #     site table. ---
    k_fine = _buffer_k(max_dist, viewshed_res)
    towers = []
    for (lo, la, _f) in survivors:
        tcell = h3.latlng_to_cell(la, lo, viewshed_res)
        st = surf12_map.get(tcell)
        if st is None:
            continue  # survivor's fine cell is NoData surface
        obs_z = st + antenna
        earth = required_mast(tcell, grnd12_map, ground_fill_k)
        mast = round((st - earth) + antenna, 1) if earth is not None else None
        n_targets = sum(1 for cc in h3.grid_disk(tcell, k_fine) if cc in surf12_map)
        towers.append(
            (
                h3.str_to_int(tcell),
                float(obs_z),
                float(lo),
                float(la),
                mast,
                int(n_targets),
            )
        )

    towers_df = spark.createDataFrame(
        towers,
        "tower_cellid long, observer_z double, lon double, lat double, "
        "required_mast double, n_targets long",
    )
    visible = h3_viewshed_towers(
        towers_df,
        surf12_df,
        grnd12_df,
        radius_m=max_dist,
        viewshed_res=viewshed_res,
        target_height=target_h,
    )
    visible = visible.cache()  # reused by the site table + both coverage rollups

    # Per-tower visible-cell count (survivors are few -> collect to the driver).
    tcov = {
        int(r["tower_cellid"]): int(r["count"])
        for r in visible.groupBy("tower_cellid").count().collect()
    }

    # --- Two-factor candidate-site table: ranked survivors (nb04 cell "Detailed
    #     viewsheds + the two-factor site table"). ---
    site_rows = []
    for (tcid, obs_z, lo, la, mast, n_targets) in towers:
        n_vis = tcov.get(tcid, 0)
        if n_vis == 0:
            continue  # nb04 skips towers that see nothing
        site_rows.append(
            {
                "cellid": tcid,
                "lon": lo,
                "lat": la,
                "required_mast": mast,
                "viewshed_cells": n_vis,
                "viewshed_pct": round(100.0 * n_vis / max(n_targets, 1), 1),
                "observer_z": obs_z,
            }
        )
    ranked = rank_candidates(site_rows)
    sites_df = spark.createDataFrame(
        [
            (
                r["cellid"],
                r["lon"],
                r["lat"],
                r["required_mast"],
                r["viewshed_cells"],
                r["viewshed_pct"],
                r["observer_z"],
            )
            for r in ranked
        ],
        "cellid long, lon double, lat double, required_mast double, "
        "viewshed_cells long, viewshed_pct double, observer_z double",
    )
    _write(sites_df, cat, sch, _T_CANDIDATES)

    # --- Per-cell coverage depth: how many sited towers can see each cell (nb04
    #     cell "Per-cell coverage"). Distributed groupBy -- the large aggregation
    #     stays in Spark. Both tables carry the res-join_res parent join key. ---
    cov = (
        visible.groupBy("cellid")
        .count()
        .withColumnRenamed("count", "tower_los_count")
        .withColumn("res", F.lit(viewshed_res))
    )
    _write(_with_join_parent(cov, join_res), cat, sch, _T_COVERAGE)

    top10 = [r["cellid"] for r in ranked[:10]]
    cov_top10 = (
        visible.where(F.col("tower_cellid").isin(top10))
        .groupBy("cellid")
        .count()
        .withColumnRenamed("count", "tower_los_count")
        .withColumn("res", F.lit(viewshed_res))
    )
    _write(_with_join_parent(cov_top10, join_res), cat, sch, _T_COVERAGE_TOP10)


if __name__ == "__main__":
    main()
