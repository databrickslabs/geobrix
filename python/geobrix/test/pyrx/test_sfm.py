import sqlite3
from types import SimpleNamespace

import numpy as np
import pytest

from databricks.labs.gbx.rasterx.sfm import _assemble_colmap_db, image_ids_to_pair_id

_MAX = 2147483647


def test_pair_id_basic():
    assert image_ids_to_pair_id(1, 2) == _MAX * 1 + 2


def test_pair_id_swap_invariant():
    # id1 > id2 must produce the SAME key as the sorted order
    assert image_ids_to_pair_id(5, 3) == image_ids_to_pair_id(3, 5) == _MAX * 3 + 5


def test_pair_id_equal_ids():
    assert image_ids_to_pair_id(7, 7) == _MAX * 7 + 7


# Minimal COLMAP schema — only the tables/columns the assembly writes.
_SCHEMA = """
CREATE TABLE cameras(camera_id INTEGER PRIMARY KEY, model INT, width INT, height INT, params BLOB, prior_focal_length INT);
CREATE TABLE rigs(rig_id INTEGER PRIMARY KEY, ref_sensor_id INT, ref_sensor_type INT);
CREATE TABLE images(image_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, camera_id INT);
CREATE TABLE frames(frame_id INTEGER PRIMARY KEY, rig_id INT);
CREATE TABLE frame_data(frame_id INT, data_id INT, sensor_id INT, sensor_type INT);
CREATE TABLE keypoints(image_id INT, rows INT, cols INT, data BLOB);
CREATE TABLE descriptors(image_id INT, rows INT, cols INT, data BLOB, type INT);
CREATE TABLE pose_priors(pose_prior_id INT, corr_data_id INT, corr_sensor_id INT, corr_sensor_type INT, position BLOB, coordinate_system INT, position_covariance BLOB, gravity BLOB);
CREATE TABLE two_view_geometries(pair_id INTEGER PRIMARY KEY, rows INT, cols INT, data BLOB, config INT);
"""


def _feat(name, gps=None):
    kp = np.zeros((10, 4), dtype=np.float32).tobytes()
    desc = np.zeros(10 * 128, dtype=np.uint8).tobytes()
    gps_pos = np.array(gps, dtype=np.float64).tobytes() if gps is not None else None
    return SimpleNamespace(
        source=f"/imgs/{name}",
        keypoints=kp,
        kp_rows=10,
        kp_cols=4,
        descriptors=desc,
        gps_pos=gps_pos,
        gps_cs=1,
        gps_cov=None,
        gps_grav=None,
        cam_model=2,
        cam_w=4000,
        cam_h=3000,
        cam_params=None,
    )


def _match(src1, src2, n):
    data = np.zeros((n, 2), dtype=np.uint32).tobytes()  # n correspondences => n rows
    return SimpleNamespace(src1=src1, src2=src2, matches=data, match_config=2)


def test_assemble_colmap_db(tmp_path):
    db = tmp_path / "master.db"
    con = sqlite3.connect(db)
    con.executescript(_SCHEMA)
    con.commit()
    con.close()
    # a.jpg has valid GPS, b.jpg has NaN GPS (skipped), c.jpg has no GPS
    features = [
        _feat("a.jpg", [1.0, 2.0, 3.0]),
        _feat("b.jpg", [np.nan, 0.0, 0.0]),
        _feat("c.jpg", None),
    ]
    # sorted by basename => a=1, b=2, c=3. Strongest pair is a<->c (5 matches).
    matches = [
        _match("/imgs/a.jpg", "/imgs/b.jpg", 2),
        _match("/imgs/c.jpg", "/imgs/a.jpg", 5),
    ]  # src1 id(3) > src2 id(1): swap path

    b1, b2, has_gps = _assemble_colmap_db(db, features, matches)

    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM cameras").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 3
    # only a.jpg has finite GPS => exactly one pose_prior; NaN and missing are skipped
    assert con.execute("SELECT COUNT(*) FROM pose_priors").fetchone()[0] == 1
    # pair-ids stored sorted (id1<id2): (1,2) and (1,3)
    pids = {r[0] for r in con.execute("SELECT pair_id FROM two_view_geometries")}
    assert pids == {image_ids_to_pair_id(1, 2), image_ids_to_pair_id(1, 3)}
    con.close()
    assert has_gps is True
    assert (b1, b2) == (1, 3)  # best init = the 5-match pair, sorted


def test_extract_features_udf_smoke(tmp_path):
    pytest.importorskip("pycolmap")
    from pathlib import Path

    import pandas as pd

    from databricks.labs.gbx.rasterx.sfm import _extract_features_to_df

    # Use a checked-in fixture image if present; else skip (SIFT needs real texture).
    fixtures = list((Path(__file__).parent / "data").glob("*.jpg"))
    if not fixtures:
        pytest.skip("no fixture JPEG for SIFT extraction")
    pdf = pd.DataFrame({"source": [str(fixtures[0])]})
    out = pd.concat(list(_extract_features_to_df(iter([pdf]))), ignore_index=True)
    assert list(out.columns)[:2] == ["source", "keypoints"]
    assert {"descriptors", "cam_model", "cam_w", "cam_h", "gps_pos"} <= set(out.columns)


def test_find_geo_pairs_requires_dbr():
    # ST_DistanceSphere is a Databricks-runtime built-in, absent from OSS Spark,
    # so _find_geo_pairs cannot be unit-tested locally. Assert it imports and is
    # callable; real coverage is the on-cluster nb1a run. (Documented skip.)
    from databricks.labs.gbx.rasterx import sfm

    assert callable(sfm._find_geo_pairs)
    pytest.skip(
        "ST_DistanceSphere is DBR-native; _find_geo_pairs validated on-cluster (nb1a)"
    )


def test_module_imports_without_pycolmap():
    # rasterx.sfm must import on the light tier; run_sfm exists and is keyword-strict.
    import inspect

    from databricks.labs.gbx.rasterx import sfm

    sig = inspect.signature(sfm.run_sfm)
    params = sig.parameters
    assert list(params)[0] == "spark"
    # feature_table / match_table / max_pair_dist_m are REQUIRED keyword-only (no default)
    assert params["feature_table"].default is inspect.Parameter.empty
    assert params["match_table"].default is inspect.Parameter.empty
    assert params["max_pair_dist_m"].default is inspect.Parameter.empty
    assert params["feature_table"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["match_table"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["max_pair_dist_m"].kind is inspect.Parameter.KEYWORD_ONLY
    # force_* replaced do_overwrite_*
    assert {"force_features", "force_matches", "force_master"} <= set(params)
    assert not any(p.startswith("do_overwrite") for p in params)


# --- Volume (dbfs:) sources -------------------------------------------------
# exif_gbx returns `path`/`source` as `dbfs:/Volumes/...` for UC Volume inputs; the
# SIFT child opens a filesystem path, so the scheme must be gone before the hand-off.


def _spy_child(monkeypatch, returncode=1, stderr="stub"):
    import subprocess

    seen = []

    def fake_run(argv, **kw):
        seen.append(argv[2])  # [python, child_script, img_path, db, work, max, gpu]
        return subprocess.CompletedProcess(argv, returncode, "", stderr)

    monkeypatch.setattr(subprocess, "run", fake_run)
    return seen


@pytest.mark.parametrize(
    "source, expected",
    [
        (
            "dbfs:/Volumes/cat/sch/vol/images/G0001.JPG",
            "/Volumes/cat/sch/vol/images/G0001.JPG",
        ),
        (
            "file:/Volumes/cat/sch/vol/images/G0001.JPG",
            "/Volumes/cat/sch/vol/images/G0001.JPG",
        ),
        (
            "/Volumes/cat/sch/vol/images/G0001.JPG",
            "/Volumes/cat/sch/vol/images/G0001.JPG",
        ),
    ],
)
def test_extract_features_hands_the_child_a_local_path(monkeypatch, source, expected):
    import pandas as pd

    from databricks.labs.gbx.rasterx import sfm

    seen = _spy_child(monkeypatch)
    list(sfm._extract_features_to_df(iter([pd.DataFrame({"source": [source]})])))
    assert seen == [expected]


def test_require_extracted_raises_naming_first_path_and_fix(tmp_path):
    from databricks.labs.gbx.rasterx import sfm

    missing = f"dbfs:{tmp_path}/G0001.JPG"
    with pytest.raises(RuntimeError) as ei:
        sfm._require_extracted(0, 3, missing)
    msg = str(ei.value)
    assert "0 of 3 images" in msg
    assert f"{tmp_path}/G0001.JPG" in msg  # the first skipped path
    assert "no such file" in msg  # and why
    assert "Fix:" in msg


def test_require_extracted_reports_the_child_failure_when_the_file_exists(
    tmp_path, monkeypatch
):
    from databricks.labs.gbx.rasterx import sfm

    img = tmp_path / "G0001.JPG"
    img.write_bytes(b"not a jpeg")
    _spy_child(monkeypatch, returncode=2, stderr="ModuleNotFoundError: pycolmap")
    with pytest.raises(RuntimeError) as ei:
        sfm._require_extracted(0, 1, str(img))
    msg = str(ei.value)
    assert str(img) in msg
    assert "pycolmap" in msg
    assert "Fix:" in msg


def test_require_extracted_partial_skip_only_warns(capsys):
    from databricks.labs.gbx.rasterx import sfm

    sfm._require_extracted(2, 3, "/Volumes/c/s/v/a.JPG")
    out = capsys.readouterr().out
    assert "WARN" in out and "2 of 3" in out


def test_require_extracted_all_extracted_is_quiet(capsys):
    from databricks.labs.gbx.rasterx import sfm

    sfm._require_extracted(3, 3, "/Volumes/c/s/v/a.JPG")
    assert capsys.readouterr().out == ""


def test_require_extracted_empty_input_raises_with_fix():
    from databricks.labs.gbx.rasterx import sfm

    with pytest.raises(RuntimeError, match="no rows") as ei:
        sfm._require_extracted(0, 0, None)
    assert "Fix:" in str(ei.value)
