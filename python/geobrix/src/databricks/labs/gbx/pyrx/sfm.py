"""Distributed sparse Structure-from-Motion orchestration (COLMAP/pycolmap)."""

import sqlite3 as _sq
import time
from pathlib import Path

import numpy as np
from pyspark.sql import functions as F


def image_ids_to_pair_id(image_id1, image_id2):
    if image_id1 > image_id2:
        image_id1, image_id2 = image_id2, image_id1
    return 2147483647 * image_id1 + image_id2


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
