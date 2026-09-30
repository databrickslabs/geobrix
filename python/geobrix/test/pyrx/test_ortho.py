import numpy as np
import pytest

from databricks.labs.gbx.pyrx.ortho import apply_sim3, umeyama_sim3


def test_umeyama_recovers_known_transform():
    rng = np.random.default_rng(0)
    src = rng.normal(size=(20, 3))
    R_true = np.linalg.qr(rng.normal(size=(3, 3)))[0]
    if np.linalg.det(R_true) < 0:
        R_true[:, 0] *= -1
    s_true, t_true = 2.5, np.array([10.0, -5.0, 3.0])
    dst = s_true * (src @ R_true.T) + t_true
    s, R, t = umeyama_sim3(src, dst)
    assert abs(s - s_true) < 1e-6
    assert np.allclose(apply_sim3((s, R, t), src), dst, atol=1e-6)


def test_apply_sim3_identity():
    pts = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    out = apply_sim3((1.0, np.eye(3), np.zeros(3)), pts)
    assert np.allclose(out, pts)


def test_umeyama_degenerate_input_raises():
    # Fewer than 3 point pairs (2 coincident points from the original brief's
    # "degenerate" case) — orchestrator ruling: this must RAISE, not silently
    # return NaN from a 0/0 scale division.
    src_two = np.array([[1.0, 1.0, 1.0], [1.0, 1.0, 1.0]])
    dst_two = np.array([[0.0, 0.0, 0.0], [2.0, 2.0, 2.0]])
    with pytest.raises(ValueError):
        umeyama_sim3(src_two, dst_two)

    # >=3 pairs but zero source variance (all src points coincident).
    src_coincident = np.array([[1.0, 2.0, 3.0]] * 3)
    dst_distinct = np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]])
    with pytest.raises(ValueError):
        umeyama_sim3(src_coincident, dst_distinct)


def test_module_imports_without_pycolmap():
    # Importing the module + using the pure primitives must NOT require pycolmap.
    import importlib

    m = importlib.import_module("databricks.labs.gbx.pyrx.ortho")
    assert hasattr(m, "umeyama_sim3") and hasattr(m, "apply_sim3")
