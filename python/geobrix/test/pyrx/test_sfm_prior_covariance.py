"""GPS pose priors carry a position covariance, so COLMAP does not fall back to its 1 m sigma.

pycolmap's EXIF reader writes each GPS prior with an all-NaN covariance. COLMAP then
weights every prior as a hard 1 m constraint and aligns with a ~2.8 m inlier threshold,
which consumer GPS misses. The assembly writes a real covariance: the image's EXIF GPS
accuracy where it has one, else ``prior_sigma_m``.
"""

import inspect
import sqlite3
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from PIL import Image
from PIL.TiffImagePlugin import IFDRational

from databricks.labs.gbx.rasterx import sfm

from .test_sfm import _SCHEMA, _feat, _match

_GPS_IFD = 0x8825
_GPS_DOP = 0x0B
_GPS_H_POSITIONING_ERROR = 0x1F


def _db(tmp_path):
    db = tmp_path / "master.db"
    con = sqlite3.connect(db)
    con.executescript(_SCHEMA)
    con.close()
    return db


def _covariances(db):
    con = sqlite3.connect(db)
    rows = con.execute(
        "SELECT i.name, p.position_covariance FROM images i "
        "JOIN pose_priors p ON p.pose_prior_id = i.image_id"
    ).fetchall()
    con.close()
    return {n: np.frombuffer(c, dtype=np.float64).reshape(3, 3) for n, c in rows}


def _diag(h, v):
    return np.diag([h * h, h * h, v * v])


def _assemble(tmp_path, features, **kw):
    db = _db(tmp_path)
    sfm._assemble_colmap_db(
        db, features, [_match(features[0].source, features[1].source, 3)], **kw
    )
    return _covariances(db)


# --- assembly: every written prior has a finite covariance -------------------------


def test_default_sigma_applies_when_the_feature_has_no_covariance(tmp_path):
    feats = [_feat("a.jpg", [1.0, 2.0, 3.0]), _feat("b.jpg", [1.1, 2.1, 3.0])]
    cov = _assemble(tmp_path, feats)
    h, v = sfm.DEFAULT_PRIOR_SIGMA_M
    assert set(cov) == {"a.jpg", "b.jpg"}
    for c in cov.values():
        assert np.all(np.isfinite(c))
        np.testing.assert_allclose(c, _diag(h, v))


def test_all_nan_covariance_from_the_extractor_gets_the_default(tmp_path):
    # pycolmap's EXIF reader writes 9 NaNs; cached feature tables hold those bytes.
    nan_cov = np.full(9, np.nan, dtype=np.float64).tobytes()
    feats = [_feat("a.jpg", [1.0, 2.0, 3.0]), _feat("b.jpg", [1.1, 2.1, 3.0])]
    for f in feats:
        f.gps_cov = nan_cov
    cov = _assemble(tmp_path, feats)
    for c in cov.values():
        np.testing.assert_allclose(c, _diag(*sfm.DEFAULT_PRIOR_SIGMA_M))


def test_prior_sigma_pair_sets_horizontal_and_vertical(tmp_path):
    feats = [_feat("a.jpg", [1.0, 2.0, 3.0]), _feat("b.jpg", [1.1, 2.1, 3.0])]
    cov = _assemble(tmp_path, feats, prior_sigma_m=(2.0, 7.0))
    np.testing.assert_allclose(cov["a.jpg"], _diag(2.0, 7.0))


def test_prior_sigma_number_is_horizontal_and_vertical_scales(tmp_path):
    feats = [_feat("a.jpg", [1.0, 2.0, 3.0]), _feat("b.jpg", [1.1, 2.1, 3.0])]
    cov = _assemble(tmp_path, feats, prior_sigma_m=3.0)
    np.testing.assert_allclose(
        cov["a.jpg"], _diag(3.0, 3.0 * sfm.GPS_VERTICAL_TO_HORIZONTAL)
    )


def test_a_feature_covariance_from_exif_wins_over_the_default(tmp_path):
    feats = [_feat("a.jpg", [1.0, 2.0, 3.0]), _feat("b.jpg", [1.1, 2.1, 3.0])]
    feats[0].gps_cov = _diag(1.5, 4.0).astype(np.float64).tobytes()
    cov = _assemble(tmp_path, feats, prior_sigma_m=(9.0, 9.0))
    np.testing.assert_allclose(cov["a.jpg"], _diag(1.5, 4.0))
    np.testing.assert_allclose(cov["b.jpg"], _diag(9.0, 9.0))


@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), (5.0, 0.0), (1.0, 2.0, 3.0)])
def test_invalid_prior_sigma_is_refused(bad):
    with pytest.raises(ValueError, match="prior_sigma_m"):
        sfm._prior_sigma_pair(bad)


def test_run_sfm_exposes_prior_sigma_m_with_the_documented_default():
    p = inspect.signature(sfm.run_sfm).parameters["prior_sigma_m"]
    assert p.kind is inspect.Parameter.KEYWORD_ONLY
    assert p.default == sfm.DEFAULT_PRIOR_SIGMA_M


# --- EXIF GPS accuracy ------------------------------------------------------------


def _jpeg(path, gps_tags=None):
    exif = Image.Exif()
    if gps_tags:
        gps = exif.get_ifd(_GPS_IFD)
        for tag, value in gps_tags.items():
            gps[tag] = value
    Image.new("RGB", (8, 6)).save(path, exif=exif)
    return path


def test_exif_horizontal_positioning_error_sets_sigma(tmp_path):
    img = _jpeg(tmp_path / "e.jpg", {_GPS_H_POSITIONING_ERROR: IFDRational(5, 2)})
    assert sfm._exif_gps_sigma(img) == pytest.approx(
        (2.5, 2.5 * sfm.GPS_VERTICAL_TO_HORIZONTAL)
    )


def test_exif_dop_sets_sigma_when_there_is_no_positioning_error(tmp_path):
    img = _jpeg(tmp_path / "d.jpg", {_GPS_DOP: IFDRational(3, 2)})
    h = 1.5 * sfm.GPS_UERE_M
    assert sfm._exif_gps_sigma(img) == pytest.approx(
        (h, h * sfm.GPS_VERTICAL_TO_HORIZONTAL)
    )


def test_positioning_error_wins_over_dop(tmp_path):
    img = _jpeg(
        tmp_path / "b.jpg",
        {_GPS_H_POSITIONING_ERROR: IFDRational(2, 1), _GPS_DOP: IFDRational(9, 1)},
    )
    assert sfm._exif_gps_sigma(img)[0] == pytest.approx(2.0)


@pytest.mark.parametrize(
    "tags", [None, {_GPS_DOP: IFDRational(0, 1)}, {_GPS_H_POSITIONING_ERROR: 0}]
)
def test_no_usable_exif_accuracy_gives_none(tmp_path, tags):
    assert sfm._exif_gps_sigma(_jpeg(tmp_path / "n.jpg", tags)) is None


def test_unreadable_image_gives_none(tmp_path):
    bad = tmp_path / "x.jpg"
    bad.write_bytes(b"not a jpeg")
    assert sfm._exif_gps_sigma(bad) is None


# --- extraction: the worker attaches the EXIF covariance -------------------------
# pycolmap is mocked at the subprocess boundary: the fake child writes the SQLite DB
# pycolmap.extract_features would, with the all-NaN prior covariance it really writes.


def _fake_child(monkeypatch):
    def fake_run(argv, **kw):
        db = Path(argv[3])
        con = sqlite3.connect(db)
        con.executescript(
            "CREATE TABLE keypoints(rows INT, cols INT, data BLOB);"
            "CREATE TABLE descriptors(data BLOB);"
            "CREATE TABLE pose_priors(position BLOB, coordinate_system INT, "
            " position_covariance BLOB, gravity BLOB);"
            "CREATE TABLE cameras(model INT, width INT, height INT, params BLOB);"
        )
        con.execute("INSERT INTO keypoints VALUES (1, 6, ?)", (b"\0" * 24,))
        con.execute("INSERT INTO descriptors VALUES (?)", (b"\0" * 128,))
        con.execute(
            "INSERT INTO pose_priors VALUES (?, 0, ?, NULL)",
            (
                np.array([45.5, -122.6, 120.0]).tobytes(),
                np.full(9, np.nan).tobytes(),
            ),
        )
        con.execute("INSERT INTO cameras VALUES (2, 8, 6, ?)", (np.zeros(4).tobytes(),))
        con.commit()
        con.close()
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)


def _extract(paths):
    pdf = pd.DataFrame({"source": [str(p) for p in paths]})
    out = pd.concat(list(sfm._extract_features_to_df(iter([pdf]))), ignore_index=True)
    return dict(zip(out["source"], out["gps_cov"]))


def test_extraction_attaches_the_exif_covariance(monkeypatch, tmp_path):
    _fake_child(monkeypatch)
    with_acc = _jpeg(
        tmp_path / "acc.jpg", {_GPS_H_POSITIONING_ERROR: IFDRational(3, 1)}
    )
    without = _jpeg(tmp_path / "plain.jpg")
    cov = _extract([with_acc, without])
    got = np.frombuffer(cov[str(with_acc)], dtype=np.float64).reshape(3, 3)
    np.testing.assert_allclose(got, _diag(3.0, 3.0 * sfm.GPS_VERTICAL_TO_HORIZONTAL))
    # no EXIF accuracy: the extractor's NaN covariance is dropped, the assembly default applies
    assert cov[str(without)] is None
