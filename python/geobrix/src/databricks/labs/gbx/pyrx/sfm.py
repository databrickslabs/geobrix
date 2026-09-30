"""Distributed sparse Structure-from-Motion orchestration (COLMAP/pycolmap)."""

import sqlite3 as _sq
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
