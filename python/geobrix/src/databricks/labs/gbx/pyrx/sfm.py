"""Distributed sparse Structure-from-Motion orchestration (COLMAP/pycolmap)."""

import json
import shutil
import sqlite3 as _sq
import tempfile
import time
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
from pyspark.sql import functions as F


def image_ids_to_pair_id(image_id1, image_id2):
    if image_id1 > image_id2:
        image_id1, image_id2 = image_id2, image_id1
    return 2147483647 * image_id1 + image_id2


def _extract_features_to_df(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Distributed CPU-SIFT feature extraction (`mapInPandas`).

    Extracted verbatim from the notebook's `extract_features_to_df` (cell
    `bda65812`), minus the `_use_gpu` toggle (this tier is CPU SIFT only).
    Runs pycolmap's native SIFT extraction in a short-lived per-image child
    process: pycolmap's native allocations are not reclaimed by the Python
    GC across the many images one long-lived Spark worker processes, so
    worker RSS climbs until the worker is OOM-killed. The child process lets
    the OS reclaim all native memory on exit, keeping worker RSS flat
    regardless of image count. Full quality is preserved (identical
    `FeatureExtractionOptions`) — do not "optimize" these params.
    """
    import gc
    import os
    import shutil
    import sqlite3
    import subprocess
    import sys
    import tempfile

    import numpy as np
    import pandas as pd
    from PIL import Image

    MAX_IMAGE_SIZE = 2000  # SIFT input cap (quality knob; unchanged)

    # Per-image subprocess isolation. pycolmap's native SIFT allocations are not
    # reclaimed by Python GC across the many images one long-lived Spark worker
    # processes, so worker RSS climbs until the worker is killed (OOM). Running
    # extraction in a short-lived child process lets the OS reclaim ALL native
    # memory on process exit, keeping worker RSS flat regardless of image count.
    # Full quality is preserved (identical FeatureExtractionOptions).
    _child = tempfile.NamedTemporaryFile(
        mode="w", suffix="_extract_child.py", delete=False
    )
    _child.write(
        "import os, sys\n"
        "os.environ['OMP_NUM_THREADS'] = '1'\n"
        "os.environ['MKL_NUM_THREADS'] = '1'\n"
        "from pathlib import Path\n"
        "import pycolmap\n"
        "img_path, db_path, work_dir, max_size, use_gpu = sys.argv[1:6]\n"
        "work = Path(work_dir); work.mkdir(parents=True, exist_ok=True)\n"
        "link = work / Path(img_path).name\n"
        "if not os.path.lexists(str(link)):\n"
        "    os.symlink(img_path, link)\n"
        "db = Path(db_path)\n"
        "if db.exists():\n"
        "    db.unlink()\n"
        "opts = pycolmap.FeatureExtractionOptions()\n"
        "opts.max_image_size = int(max_size)\n"
        "opts.num_threads = 1\n"
        "opts.use_gpu = bool(int(use_gpu))\n"
        "pycolmap.extract_features(db, work, extraction_options=opts)\n"
    )
    _child.close()
    child_script = _child.name

    try:
        for pdf in iterator:
            for _, row in pdf.iterrows():
                img_path = row["source"].replace("file:", "")
                img_name = Path(img_path).name
                worker_db = Path(f"/tmp/ex_{img_name}.db")
                worker_dir = Path(f"/tmp/ed_{img_name}")
                worker_dir.mkdir(parents=True, exist_ok=True)
                if worker_db.exists():
                    worker_db.unlink()
                try:
                    # Isolate the native SIFT extraction in a child process so its
                    # memory is fully reclaimed when the child exits.
                    proc = subprocess.run(
                        [sys.executable, child_script, img_path,
                         str(worker_db), str(worker_dir), str(MAX_IMAGE_SIZE),
                         str(int(False))],
                        capture_output=True, text=True, timeout=300,
                    )
                    if proc.returncode != 0 or not worker_db.exists():
                        print(
                            f"[extract] SKIP {img_name}: child rc={proc.returncode} "
                            f"{proc.stderr.strip()[-300:]}",
                            flush=True,
                        )
                        continue
                    conn = sqlite3.connect(worker_db)
                    kp_data = conn.execute("SELECT rows, cols, data FROM keypoints").fetchone()
                    desc_data = conn.execute("SELECT data FROM descriptors").fetchone()
                    gps_row = conn.execute(
                        "SELECT position, coordinate_system, position_covariance, gravity "
                        "FROM pose_priors LIMIT 1"
                    ).fetchone()
                    if gps_row and gps_row[0]:
                        _pos = np.frombuffer(gps_row[0], dtype=np.float64)
                        gps_pos = bytes(gps_row[0]) if np.all(np.isfinite(_pos)) else None
                        gps_cs = int(gps_row[1]) if gps_pos is not None else 0
                        gps_cov = bytes(gps_row[2]) if gps_row[2] is not None else None
                        gps_grav = bytes(gps_row[3]) if gps_row[3] is not None else None
                    else:
                        gps_pos = None; gps_cs = 0; gps_cov = None; gps_grav = None
                    cam_row = conn.execute(
                        "SELECT model, width, height, params FROM cameras LIMIT 1"
                    ).fetchone()
                    if cam_row:
                        cam_model_val = int(cam_row[0])
                        cam_w_val = int(cam_row[1])
                        cam_h_val = int(cam_row[2])
                        cam_params_val = bytes(cam_row[3])
                    else:
                        cam_model_val = 2; cam_w_val = 4000; cam_h_val = 3000; cam_params_val = None
                    if kp_data and desc_data:
                        with Image.open(img_path) as img:
                            w, h = img.size
                        yield pd.DataFrame([{
                            "source": row["source"],
                            "keypoints": kp_data[2],
                            "kp_rows": kp_data[0],
                            "kp_cols": kp_data[1],
                            "descriptors": desc_data[0],
                            "width": w,
                            "height": h,
                            "gps_pos": gps_pos,
                            "gps_cs": gps_cs,
                            "gps_cov": gps_cov,
                            "gps_grav": gps_grav,
                            "cam_model": cam_model_val,
                            "cam_w": cam_w_val,
                            "cam_h": cam_h_val,
                            "cam_params": cam_params_val,
                        }])
                    conn.close()
                finally:
                    if worker_db.exists():
                        worker_db.unlink()
                    shutil.rmtree(worker_dir, ignore_errors=True)
                    gc.collect()
    finally:
        try:
            os.unlink(child_script)
        except OSError:
            pass


def _match_pairs_dist(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Distributed SIFT feature matching (`mapInPandas`).

    Extracted verbatim from the notebook's `match_pairs_dist` (cell
    `8762ea18`). Rebuilds a scratch per-pair COLMAP DB from the two images'
    extracted keypoints/descriptors, runs `pycolmap.match_exhaustive`, and
    emits verified two-view geometries with >=10 inlier matches.
    """
    import sqlite3

    import numpy as np
    import pandas as pd
    import pycolmap
    from pathlib import Path

    for pdf in iterator:
        results = []
        for _, row in pdf.iterrows():
            pair_hash = abs(hash(row["src1"] + row["src2"]))
            db_path = Path(f"/tmp/p_{pair_hash}.db")
            dummy = Path(f"/tmp/d_{pair_hash}")
            if db_path.exists():
                db_path.unlink()
            try:
                dummy.mkdir(exist_ok=True)
                pycolmap.extract_features(db_path, dummy)
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                for t in ["cameras", "images", "keypoints", "descriptors", "two_view_geometries"]:
                    cursor.execute(f"DELETE FROM {t}")
                _cm = int(row["cam_model"]) if row.get("cam_model") is not None else 2
                _cw = int(row["cam_w"]) if row.get("cam_w") is not None else 4000
                _ch = int(row["cam_h"]) if row.get("cam_h") is not None else 3000
                _cp = bytes(row["cam_params"]) if row.get("cam_params") is not None else \
                    np.array([1733.30, _cw / 2, _ch / 2, 0.0], dtype=np.float64).tobytes()
                cursor.execute(
                    "INSERT INTO cameras (camera_id, model, width, height, params, prior_focal_length) "
                    "VALUES (1, ?, ?, ?, ?, 1)", (_cm, _cw, _ch, _cp))
                cursor.execute(
                    "INSERT INTO images (image_id, name, camera_id) VALUES (1, 'img1', 1), (2, 'img2', 1)")
                cursor.execute(
                    "INSERT INTO keypoints (image_id, rows, cols, data) VALUES (?, ?, ?, ?)",
                    (1, int(row["kp_rows_1"]), int(row["kp_cols_1"]), row["kp1"]))
                cursor.execute(
                    "INSERT INTO keypoints (image_id, rows, cols, data) VALUES (?, ?, ?, ?)",
                    (2, int(row["kp_rows_2"]), int(row["kp_cols_2"]), row["kp2"]))
                d1_rows = len(row["desc1"]) // 128
                d2_rows = len(row["desc2"]) // 128
                cursor.execute(
                    "INSERT INTO descriptors (image_id, rows, cols, data, type) VALUES (?, ?, 128, ?, 0)",
                    (1, d1_rows, row["desc1"]))
                cursor.execute(
                    "INSERT INTO descriptors (image_id, rows, cols, data, type) VALUES (?, ?, 128, ?, 0)",
                    (2, d2_rows, row["desc2"]))
                conn.commit(); conn.close()
                m_opts = pycolmap.FeatureMatchingOptions()
                m_opts.sift.max_ratio = 0.85
                pycolmap.match_exhaustive(database_path=db_path, matching_options=m_opts)
                conn = sqlite3.connect(db_path)
                match_row = conn.execute(
                    "SELECT rows, data, config FROM two_view_geometries LIMIT 1"
                ).fetchone()
                conn.close()
                if match_row and match_row[0] >= 10:
                    results.append({
                        "src1": row["src1"], "src2": row["src2"],
                        "matches": match_row[1], "match_config": match_row[2],
                    })
            except Exception as e:
                print(f"[match_pairs_dist] FAILED {row['src1']} <-> {row['src2']}: {e}")
            finally:
                if db_path.exists(): db_path.unlink()
                if dummy.exists(): dummy.rmdir()
        yield pd.DataFrame(results)


def _assemble_colmap_db(db_path, features, matches, *, camera_defaults=None):
    """Assemble the COLMAP master SQLite DB from extracted features + verified matches.

    Extracted verbatim from the notebook's `run_spark_sfm` Stage 4-5 "Master DB
    assembly" block (cell b07bd8e1): pair-id swap, GPS-prior byte encoding, and
    best-init-pair selection are correctness-critical and left unchanged. The
    caller is responsible for seeding the COLMAP schema on `db_path` (via
    `pycolmap.extract_features` in `run_sfm`, or a DDL fixture in tests) before
    calling this function.

    Returns
    -------
    tuple[int, int, bool]
        `(best_init_id1, best_init_id2, has_gps)`.
    """
    conn = _sq.connect(db_path)
    cursor = conn.cursor()

    local_features_sorted = sorted(features, key=lambda r: Path(r.source).name)
    first_feat = local_features_sorted[0]
    _defaults = camera_defaults or {}
    cam_model_id = (
        int(first_feat.cam_model)
        if first_feat.cam_model is not None
        else _defaults.get("cam_model", 2)
    )
    cam_w_native = (
        int(first_feat.cam_w)
        if first_feat.cam_w is not None
        else _defaults.get("cam_w", 4000)
    )
    cam_h_native = (
        int(first_feat.cam_h)
        if first_feat.cam_h is not None
        else _defaults.get("cam_h", 3000)
    )
    cam_params_bytes = (
        bytes(first_feat.cam_params)
        if first_feat.cam_params is not None
        else _defaults.get(
            "cam_params",
            np.array(
                [1733.30, cam_w_native / 2, cam_h_native / 2, 0.0], dtype=np.float64
            ).tobytes(),
        )
    )
    cursor.execute("DELETE FROM cameras")
    cursor.execute(
        "INSERT INTO cameras (camera_id, model, width, height, params, prior_focal_length) "
        "VALUES (1, ?, ?, ?, ?, 1)",
        (cam_model_id, cam_w_native, cam_h_native, cam_params_bytes),
    )
    cursor.execute(
        "INSERT INTO rigs (rig_id, ref_sensor_id, ref_sensor_type) VALUES (1, 1, 0)"
    )

    img_path_to_id = {}
    has_gps = False
    for row in local_features_sorted:
        img_name = Path(row.source).name
        cursor.execute(
            "INSERT INTO images (name, camera_id) VALUES (?, 1)", (img_name,)
        )
        image_id = cursor.lastrowid
        img_path_to_id[row.source] = image_id
        cursor.execute(
            "INSERT INTO frames (frame_id, rig_id) VALUES (?, 1)", (image_id,)
        )
        cursor.execute(
            "INSERT INTO frame_data (frame_id, data_id, sensor_id, sensor_type) "
            "VALUES (?, ?, 1, 0)",
            (image_id, image_id),
        )
        cursor.execute(
            "INSERT INTO keypoints (image_id, rows, cols, data) VALUES (?, ?, ?, ?)",
            (image_id, row.kp_rows, row.kp_cols, row.keypoints),
        )
        desc_rows = len(row.descriptors) // 128
        cursor.execute(
            "INSERT INTO descriptors (image_id, rows, cols, data, type) VALUES (?, ?, 128, ?, 0)",
            (image_id, desc_rows, row.descriptors),
        )
        gps_pos_bytes = bytes(row.gps_pos) if row.gps_pos else None
        if gps_pos_bytes and len(gps_pos_bytes) == 24:
            _pos = np.frombuffer(gps_pos_bytes, dtype=np.float64)
            if np.all(np.isfinite(_pos)):
                gps_cov_bytes = (
                    bytes(row.gps_cov)
                    if row.gps_cov
                    else np.full(9, np.nan, dtype=np.float64).tobytes()
                )
                gps_grav_bytes = (
                    bytes(row.gps_grav)
                    if row.gps_grav
                    else np.array([0.0, 1.0, 0.0], dtype=np.float64).tobytes()
                )
                cursor.execute(
                    "INSERT INTO pose_priors "
                    "(pose_prior_id, corr_data_id, corr_sensor_id, corr_sensor_type, "
                    " position, coordinate_system, position_covariance, gravity) "
                    "VALUES (?, ?, 1, 0, ?, ?, ?, ?)",
                    (
                        image_id,
                        image_id,
                        gps_pos_bytes,
                        int(row.gps_cs),
                        gps_cov_bytes,
                        gps_grav_bytes,
                    ),
                )
                has_gps = True

    best_match_count = 0
    best_init_id1 = best_init_id2 = -1
    for row in matches:
        id1 = img_path_to_id[row.src1]
        id2 = img_path_to_id[row.src2]
        match_data = bytes(row.matches)
        if id1 > id2:
            id1, id2 = id2, id1
            arr = np.frombuffer(match_data, dtype=np.uint32).reshape(-1, 2)
            match_data = arr[:, ::-1].astype(np.uint32).tobytes()
        pair_id = image_ids_to_pair_id(id1, id2)
        match_rows = len(match_data) // 8
        cursor.execute(
            "INSERT INTO two_view_geometries (pair_id, rows, cols, data, config) "
            "VALUES (?, ?, 2, ?, ?)",
            (pair_id, match_rows, match_data, row.match_config),
        )
        if match_rows > best_match_count:
            best_match_count = match_rows
            best_init_id1, best_init_id2 = id1, id2
    conn.commit()
    conn.close()
    return best_init_id1, best_init_id2, has_gps


def _find_geo_pairs(spark, df_qc, *, max_pair_dist_m):
    """Geospatial pair discovery via ST_DistanceSphere (Databricks built-in).

    Databricks advantage: ``ST_DistanceSphere`` pair filtering keeps pair count
    linear in image count — exhaustive matching would be O(n^2).
    """
    df_qc.createOrReplaceTempView("drone_metadata")
    pairs_df = spark.sql(f"""
        SELECT
            a.source AS src1,
            b.source AS src2,
            ST_DistanceSphere(a.gps_geom, b.gps_geom) AS dist
        FROM drone_metadata a
        JOIN drone_metadata b ON a.source < b.source
        WHERE ST_DistanceSphere(a.gps_geom, b.gps_geom) < {max_pair_dist_m}
    """)
    print(f"Geospatial filtering reduced pairs to: {pairs_df.count()}")
    return pairs_df


def _gps_from_master(db):
    """Read GPS pose priors back out of the assembled master COLMAP DB.

    Extracted verbatim from the notebook's inline `_gps_from_master` def
    (cell b07bd8e1, Stage 4-5) — a module-private helper for `run_sfm`.
    """
    import sqlite3 as __sq
    _c = __sq.connect(str(db)); _o = {}
    for _n, _p in _c.execute("SELECT i.name, p.position FROM images i "
                             "JOIN pose_priors p ON p.pose_prior_id = i.image_id"):
        _o[_n] = np.frombuffer(_p, dtype=np.float64).tolist()
    _c.close(); return _o


def run_sfm(
    spark,
    df_qc,
    image_dir,
    output_dir,
    *,
    group_key=None,
    feature_table,
    match_table,
    max_pair_dist_m,
    force_features=False,
    force_matches=False,
    force_master=False,
    persist_dir=None,
    serialize_extract=False,
):
    """Distributed sparse Structure-from-Motion pipeline for one group of drone images.

    Extracted verbatim from the notebook's `run_spark_sfm` (cell b07bd8e1), with
    `spark` explicit (was a closed-over notebook global), caller-controlled
    `feature_table`/`match_table` (was derived from `group_key`),
    `force_features/matches/master` (was `do_overwrite_features/matches/master`),
    and `max_pair_dist_m` threaded through to `_find_geo_pairs` (was a
    closed-over notebook global `MAX_PAIR_DIST_M`). The Stage 4-5 "Master DB
    assembly" block is delegated to `_assemble_colmap_db` (carved out in Task
    2); `pycolmap` is imported lazily, inside Stage 4-5 only, so this module
    stays importable on the light (pyrx) tier without pycolmap installed.

    Parameters
    ----------
    spark:
        SparkSession.
    df_qc:
        Spark DF with a 'source' column (full path to JPEGs) and optional GPS geometry.
    image_dir:
        POSIX path to the JPEG directory.
    output_dir:
        Persistent storage root for SfM artifacts.
    group_key:
        Optional string label for this group — used only for the
        `master_sfm{_sfx}.db` filename and log labels.
    feature_table, match_table:
        Caller-controlled table names for the extracted-feature / verified-match caches.
    max_pair_dist_m:
        Max geospatial pair distance (meters), passed to `_find_geo_pairs`.
    force_features, force_matches, force_master:
        Force re-extraction / re-matching / re-mapping even when tables/artifacts exist.
    persist_dir:
        Optional durable-storage root (e.g. a Volume) for cross-run sparse-model reuse.
    serialize_extract:
        Force single-partition (coalesce(1)) feature extraction as an OOM fallback.

    Returns
    -------
    dict
        `{"sparse_dir", "gps_json", "models", "registered"}`, plus `"reused": True`
        when short-circuited via the `persist_dir` durable-reuse path.
    """
    _sfx = f"_{group_key}" if group_key else ""

    num_partitions = df_qc.count()
    # Memory-safe (Serverless): one image per mapInPandas Arrow batch, so a Python
    # worker holds at most a single image’s pycolmap working set (per-task RAM ~1-2 GB).
    # SIFT params/quality unchanged. Guarded per the Serverless spark.conf convention.
    try:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1")
    except Exception:
        pass
    t0 = time.perf_counter()

    # ── Stage 1: Distributed feature extraction ───────────────────────────
    t1 = time.perf_counter()
    try:
        _table_ok = (
            spark.catalog.tableExists(feature_table)
            and "gps_pos" in spark.table(feature_table).columns
            and "cam_model" in spark.table(feature_table).columns
        )
        # Empty/stale cache must NOT be reused: 0 cached features yields 0 matches
        # downstream and misreads as a reconstruction failure - re-extract instead.
        _cached_n = spark.table(feature_table).count() if _table_ok else 0
        _reuse = _table_ok and _cached_n > 0 and not force_features
        if _reuse:
            df_features = spark.table(feature_table)
            print(f"[Stage 1] reusing cached features: {_cached_n} images (FORCE_RELOAD=False)")
        else:
            if _table_ok and _cached_n == 0 and not force_features:
                print("[Stage 1] cached features empty - rebuilding (extracting fresh)")
            feature_schema = (
                "source string, keypoints binary, kp_rows int, kp_cols int, "
                "descriptors binary, width int, height int, "
                "gps_pos binary, gps_cs int, gps_cov binary, gps_grav binary, "
                "cam_model int, cam_w int, cam_h int, cam_params binary"
            )
            # serialize_extract: force a single partition so extraction runs one
            # SIFT child at a time (no concurrent SIFT on a node). Retry-loop
            # fallback after worker OOMs; slower but memory-bounded. SIFT quality
            # unchanged. AQE does NOT reliably honor a bare repartition(N) /
            # repartition(1); the respected idioms are repartition(N, col) for
            # fan-out (one task per image, keyed on the unique source path) and
            # coalesce(1) to force a single partition for the serialized fallback.
            if serialize_extract:
                print("[Stage 1] serialized extraction (coalesce 1) - memory-bounded OOM fallback")
            _extract_df = (
                df_qc.select("source").coalesce(1)
                if serialize_extract
                else df_qc.select("source").repartition(num_partitions, "source")
            )
            (
                _extract_df
                .mapInPandas(_extract_features_to_df, schema=feature_schema)
                .write.mode("overwrite").option("overwriteSchema", "true")
                .saveAsTable(feature_table)
            )
            df_features = spark.table(feature_table)
            print(f"[Stage 1] extracted features: {time.perf_counter()-t1:.1f}s | {df_features.count()} images")
    except Exception as e:
        print(f"[ERROR Stage 1] Feature extraction failed after {time.perf_counter()-t1:.1f}s: {e}")
        raise RuntimeError(f"[Stage 1 extract] {e}") from e

    # ── Stage 2–3: Spatial pair discovery + distributed matching ─────────
    t2 = time.perf_counter()
    match_count = 0
    df_verified_matches = None
    try:
        if not spark.catalog.tableExists(match_table) or force_matches:
            df_pairs = _find_geo_pairs(spark, df_qc, max_pair_dist_m=max_pair_dist_m)
            (
                df_pairs
                .join(df_features.alias("f1"), df_pairs.src1 == F.col("f1.source"))
                .join(df_features.alias("f2"), df_pairs.src2 == F.col("f2.source"))
                .select(
                    "src1", "src2",
                    F.col("f1.keypoints").alias("kp1"),
                    F.col("f1.kp_rows").alias("kp_rows_1"),
                    F.col("f1.kp_cols").alias("kp_cols_1"),
                    F.col("f1.descriptors").alias("desc1"),
                    F.col("f2.keypoints").alias("kp2"),
                    F.col("f2.kp_rows").alias("kp_rows_2"),
                    F.col("f2.kp_cols").alias("kp_cols_2"),
                    F.col("f2.descriptors").alias("desc2"),
                    F.col("f1.cam_model").alias("cam_model"),
                    F.col("f1.cam_w").alias("cam_w"),
                    F.col("f1.cam_h").alias("cam_h"),
                    F.col("f1.cam_params").alias("cam_params"),
                )
                .repartition(num_partitions)
                .mapInPandas(
                    _match_pairs_dist,
                    schema="src1 string, src2 string, matches binary, match_config int",
                )
                .write.mode("overwrite").option("overwriteSchema", "true")
                .saveAsTable(match_table)
            )
        df_verified_matches = spark.table(match_table)
        match_count = df_verified_matches.count()
        print(f"[Stage 2-3] Pair matching: {time.perf_counter()-t2:.1f}s | {match_count} verified pairs")
    except Exception as e:
        print(f"[ERROR Stage 2-3] Matching failed after {time.perf_counter()-t2:.1f}s: {e}")
        raise RuntimeError(f"[Stage 2-3 match] {e}") from e

    if match_count == 0:
        _img_n = df_features.count()
        if _img_n == 0:
            raise RuntimeError(
                "Stage 1: 0 features extracted - empty/failed extraction "
                "(if FORCE_RELOAD=False, set True to rebuild the feature cache)."
            )
        raise RuntimeError(
            f"Stage 3: 0 matches from {_img_n} images - check image overlap "
            "or descriptor extraction."
        )

    # ── GPS prior validation ───────────────────────────────────────────────
    # Warn (don't hard-fail) if fewer than 3 images have GPS — incremental
    # mapping will still attempt registration but without GPS-constrained BA.
    gps_check_count = df_features.filter(F.col("gps_pos").isNotNull()).count()
    if gps_check_count < 3:
        print(
            f"[WARN] Only {gps_check_count} images have valid GPS priors "
            "(need ≥3 for GPS-constrained BA). Proceeding without GPS constraints."
        )
    else:
        print(f"  GPS priors available: {gps_check_count} images")

    # ── Stage 4–5: Master DB assembly + incremental mapping ───────────────
    t4 = time.perf_counter()
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        master_db  = Path(output_dir) / f"master_sfm{_sfx}.db"
        sparse_dir = Path(output_dir) / "sparse"
        snap_dir   = Path(output_dir) / "snapshots"

        # Durable reuse (FORCE_RELOAD=False): a prior run persists the sparse model
        # (.bin — safe to copy) and GPS priors (JSON) to persist_dir on the Volume.
        # SfM's SQLite master DB cannot live on a Volume and local scratch is ephemeral
        # per job, so cross-run reuse keys on these durable artifacts, not master_db.
        _psparse   = Path(persist_dir) / "sparse" if persist_dir else None
        _pgps      = Path(persist_dir) / "gps_priors.json" if persist_dir else None
        _gps_local = Path(output_dir) / "gps_priors.json"

        if (not force_master and _psparse is not None
                and _psparse.exists() and _pgps.exists()):
            if sparse_dir.exists(): shutil.rmtree(sparse_dir)
            shutil.copytree(_psparse, sparse_dir)
            shutil.copy2(_pgps, _gps_local)
            print(f"[Stage 4-5] Reusing persisted SfM (FORCE_RELOAD=False) ← {persist_dir} "
                  f"({time.perf_counter()-t0:.0f}s)")
            return {"sparse_dir": str(sparse_dir), "gps_json": str(_gps_local),
                    "models": None, "registered": None, "reused": True}

        if not master_db.exists() or force_master:
            import pycolmap

            if master_db.exists():
                master_db.unlink()

            with tempfile.TemporaryDirectory() as _init_tmp:
                _schema_opts = pycolmap.FeatureExtractionOptions()
                _schema_opts.num_threads = 1
                pycolmap.extract_features(master_db, Path(_init_tmp), extraction_options=_schema_opts)

            local_features = df_features.collect()
            local_matches  = df_verified_matches.collect()

            best_init_id1, best_init_id2, has_gps = _assemble_colmap_db(
                master_db, local_features, local_matches
            )
            local_features_sorted = sorted(local_features, key=lambda r: Path(r.source).name)
            print(f"  Master DB: {len(local_features_sorted)} images, "
                  f"GPS={has_gps}, best init pair: {best_init_id1}/{best_init_id2}")

            # Incremental mapping
            local_map_db = Path("/tmp/master_sfm_map.db")
            if local_map_db.exists():
                local_map_db.unlink()
            shutil.copy2(master_db, local_map_db)

            if sparse_dir.exists(): shutil.rmtree(sparse_dir)
            if snap_dir.exists():   shutil.rmtree(snap_dir)
            sparse_dir.mkdir(parents=True, exist_ok=True)
            snap_dir.mkdir(parents=True, exist_ok=True)

            mapper_opts = pycolmap.IncrementalPipelineOptions()
            mapper_opts.mapper.init_min_tri_angle      = 3.0
            mapper_opts.mapper.init_max_forward_motion = 0.99
            mapper_opts.mapper.init_min_num_inliers    = 50
            mapper_opts.mapper.abs_pose_min_num_inliers  = 15
            mapper_opts.mapper.abs_pose_min_inlier_ratio = 0.1
            mapper_opts.ba_global_max_num_iterations   = 15
            mapper_opts.ba_local_max_num_iterations    = 15
            mapper_opts.init_num_trials                = 50
            mapper_opts.max_runtime_seconds            = 1800
            mapper_opts.snapshot_frames_freq = 10
            mapper_opts.snapshot_path        = snap_dir
            mapper_opts.use_prior_position                = has_gps
            mapper_opts.use_robust_loss_on_prior_position = has_gps
            mapper_opts.init_image_id1 = best_init_id1
            mapper_opts.init_image_id2 = best_init_id2
            mapper_opts.image_names = [Path(r.source).name for r in local_features_sorted]

            _registered = [0]
            _t_map = [time.perf_counter()]

            def _on_init():
                print(f"[Mapping] Initial pair found — registering remaining images...", flush=True)

            def _on_next():
                _registered[0] += 1
                elapsed = time.perf_counter() - _t_map[0]
                rate    = _registered[0] / elapsed if elapsed > 0 else 0
                eta     = (len(local_features_sorted) - _registered[0]) / rate if rate > 0 else float("inf")
                print(
                    f"[Mapping] {_registered[0]}/{len(local_features_sorted)} images "
                    f"({100*_registered[0]/len(local_features_sorted):.0f}%) | "
                    f"elapsed {elapsed:.0f}s | ETA ~{eta:.0f}s",
                    flush=True,
                )

            print(f"Running incremental mapping ({len(local_features_sorted)} images)...", flush=True)
            reconstructions = pycolmap.incremental_mapping(
                database_path=local_map_db,
                image_path=Path(image_dir),
                output_path=sparse_dir,
                options=mapper_opts,
                initial_image_pair_callback=_on_init,
                next_image_callback=_on_next,
            )
            local_map_db.unlink(missing_ok=True)
            if snap_dir.exists():
                shutil.rmtree(snap_dir)
            elapsed_total = time.perf_counter() - t4
            print(
                f"[Stage 4-5] SfM complete: {len(reconstructions)} model(s) in "
                f"{elapsed_total:.0f}s ({_registered[0]}/{len(local_features_sorted)} images registered)."
            )
            with open(_gps_local, "w") as _f:
                json.dump(_gps_from_master(master_db), _f)
            if persist_dir:
                Path(persist_dir).mkdir(parents=True, exist_ok=True)
                if _psparse.exists(): shutil.rmtree(_psparse)
                shutil.copytree(sparse_dir, _psparse)
                shutil.copy2(_gps_local, _pgps)
                print(f"[Stage 4-5] Persisted SfM → {persist_dir} (reusable on FORCE_RELOAD=False)")
            return {
                "sparse_dir": str(sparse_dir),
                "gps_json":   str(_gps_local),
                "models":     len(reconstructions),
                "registered": _registered[0],
            }
        elapsed_total = time.perf_counter() - t0
        print(f"[Stage 4-5] Pre-existing master DB (force_master=False). "
              f"Run monitor notebook for status. ({elapsed_total:.0f}s)")
        with open(_gps_local, "w") as _f:
            json.dump(_gps_from_master(master_db), _f)
        return {"sparse_dir": str(sparse_dir), "gps_json": str(_gps_local), "models": None, "registered": None}
    except Exception as e:
        print(f"[ERROR Stage 4-5] SfM failed after {time.perf_counter()-t4:.1f}s: {e}")
        raise
