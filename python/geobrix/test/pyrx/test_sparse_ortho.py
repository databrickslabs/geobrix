import numpy as np
import pytest

from databricks.labs.gbx.pyrx.ortho import _backproject_image


def _nadir_case(height_m=40.0, cam_w=64, cam_h=48, f=50.0):
    # Nadir (straight-down) camera at ENU (0,0,height) over a flat ground z=0.
    # T_cid = identity (ENU == COLMAP world frame): scale=1, R=I, t=0.
    T_cid = (1.0, np.eye(3), np.zeros(3))
    C_enu = np.array([0.0, 0.0, height_m])
    # world->cam: X_cam=+X, Y_cam=-Y, Z_cam=-Z (camera looks down +Z_cam == -Z_world)
    R_cw = np.array([[1.0, 0, 0], [0, -1.0, 0], [0, 0, -1.0]])
    t_cw = -R_cw @ C_enu
    src = np.full(
        (cam_h, cam_w, 3), (200.0, 40.0, 40.0), dtype=np.float32
    )  # uniform red-ish
    # canvas: 2 m span around the footprint, 5 cm px
    gsd_m = 0.05
    e_min, e_max, n_min, n_max = -1.0, 1.0, -1.0, 1.0
    out_w = int((e_max - e_min) / gsd_m)
    out_h = int((n_max - n_min) / gsd_m)
    return dict(
        src_rgb=src,
        cam_f=f,
        cam_cx=cam_w / 2,
        cam_cy=cam_h / 2,
        cam_w=cam_w,
        cam_h=cam_h,
        R_cw=R_cw,
        t_cw=t_cw,
        T_cid=T_cid,
        C_enu=C_enu,
        z_med=0.0,
        e_min=e_min,
        n_max=n_max,
        gsd_m=gsd_m,
        out_w=out_w,
        out_h=out_h,
        blend_gamma=4.0,
    )


def test_backproject_covers_and_preserves_uniform_color():
    r = _backproject_image(**_nadir_case())
    assert r is not None
    px0, px1, py0, py1, colors, weights = r
    assert 0 <= px0 < px1 <= _nadir_case()["out_w"]
    assert 0 <= py0 < py1 <= _nadir_case()["out_h"]
    covered = weights > 0
    assert covered.any(), "nadir camera should cover part of the canvas"
    # bilinear sampling a UNIFORM image returns that color wherever covered
    cov_colors = colors[covered]
    assert np.allclose(cov_colors[:, 0], 200.0, atol=1.0)
    assert np.allclose(cov_colors[:, 1], 40.0, atol=1.0)
    assert weights.max() > 0.0


def _gradient_src(cam_w, cam_h, blue=100.0):
    """A source image whose R channel encodes the image COLUMN (u) and G
    channel encodes the image ROW (v) exactly. Bilinear interpolation of a
    linear ramp reproduces the ramp exactly (no clipping engaged in these
    tests), so the sampled color IS the continuous projected (u, v) at each
    canvas cell -- this lets a test recover and pin the exact projection,
    which a uniform image cannot do."""
    src = np.zeros((cam_h, cam_w, 3), dtype=np.float32)
    src[:, :, 0] = np.arange(cam_w, dtype=np.float32)[None, :]
    src[:, :, 1] = np.arange(cam_h, dtype=np.float32)[:, None]
    src[:, :, 2] = blue
    return src


def test_backproject_known_point_and_monotonic_projection():
    # Nadir camera at height H over ENU origin: the closed-form projection is
    # u(e) = f*e/H + cx, v(n) = cy - f*n/H (derived from _nadir_case's fixed
    # R_cw/t_cw/C_enu geometry). With the R=u, G=v gradient source, the
    # bilinearly-sampled color equals this closed form exactly, so we can pin
    # the projection pointwise -- not just "some plausible color came back".
    case = _nadir_case()
    case["src_rgb"] = _gradient_src(case["cam_w"], case["cam_h"])
    f, cx, cy = case["cam_f"], case["cam_cx"], case["cam_cy"]
    h_above = float(case["C_enu"][2] - case["z_med"])
    e_min, n_max, gsd_m = case["e_min"], case["n_max"], case["gsd_m"]

    r = _backproject_image(**case)
    assert r is not None
    px0, px1, py0, py1, colors, weights = r
    # This nadir camera (40 m up) sees far beyond the 2 m canvas, so the whole
    # canvas is covered -> bbox == full canvas, local index == absolute index.
    assert (px0, px1, py0, py1) == (0, 40, 0, 40)

    def expected_u(p):
        return f * (e_min + (p + 0.5) * gsd_m) / h_above + cx

    def expected_v(q):
        return cy - f * (n_max - (q + 0.5) * gsd_m) / h_above

    # 1) known point: the ground point directly under the camera (ENU origin)
    # projects to image CENTER (cx, cy); the canvas cell nearest ENU (0,0) --
    # (p, q) = (20, 20) here -- samples that center color.
    p_near, q_near = 20, 20
    assert abs(expected_u(p_near) - cx) < 0.1
    assert abs(expected_v(q_near) - cy) < 0.1
    assert colors[q_near, p_near, 0] == pytest.approx(cx, abs=0.5)
    assert colors[q_near, p_near, 1] == pytest.approx(cy, abs=0.5)

    # 2) the WHOLE map matches the closed-form projection exactly -- catches a
    # shift (wrong offset/anchor) anywhere on the canvas, not just at one cell.
    exp_u = expected_u(np.arange(px0, px1))[None, :]
    exp_v = expected_v(np.arange(py0, py1))[:, None]
    np.testing.assert_allclose(
        colors[:, :, 0], np.broadcast_to(exp_u, colors.shape[:2]), atol=0.05
    )
    np.testing.assert_allclose(
        colors[:, :, 1], np.broadcast_to(exp_v, colors.shape[:2]), atol=0.05
    )

    # 3) monotonic in the RIGHT direction: ground-east increases -> sampled u
    # (image column) increases; ground-north increases (row q decreases) ->
    # sampled v increases. A sign-flipped projection reverses one or both.
    assert np.all(np.diff(colors[:, :, 0], axis=1) > 0)
    assert np.all(np.diff(colors[:, :, 1], axis=0) > 0)


def test_backproject_weight_peaks_at_footprint_center():
    r = _backproject_image(**_nadir_case())
    assert r is not None
    px0, px1, py0, py1, colors, weights = r
    assert (px0, px1, py0, py1) == (0, 40, 0, 40)

    row, col = np.unravel_index(np.argmax(weights), weights.shape)
    # peak sits at the canvas cell(s) nearest the sub-camera point (ENU origin)
    assert row in (19, 20) and col in (19, 20)
    center_w = weights.max()
    assert center_w > weights[19, 0]  # west-edge cell, same row as the center
    assert center_w > weights[0, 0]  # corner
    assert center_w > weights[39, 39]  # opposite corner
    # radial-cosine falloff: weight strictly decreases moving away from the
    # center along a single row. An inverted weight profile would reverse this.
    row20 = weights[20, :]
    assert row20[20] > row20[10] > row20[0]


def test_backproject_off_footprint_cells_are_masked():
    # A canvas much larger than this camera's ground footprint: the corners of
    # the returned crop project outside the camera's [0,cam_w) x [0,cam_h)
    # image bounds (in_img=False) even though they sit inside the bbox
    # heuristic's coarse padding, while the crop's center remains covered.
    case = _nadir_case()
    case.update(e_min=-40.0, n_max=30.0, gsd_m=1.0, out_w=80, out_h=60)
    r = _backproject_image(**case)
    assert r is not None
    px0, px1, py0, py1, colors, weights = r
    assert (px0, px1, py0, py1) == (11, 69, 7, 53)

    assert weights[0, 0] == 0.0
    assert weights[-1, -1] == 0.0
    # crop center (absolute canvas px=40, py=30) sits under the camera and IS
    # in the image -> weight > 0.
    assert weights[30 - py0, 40 - px0] > 0.5


def test_backproject_returns_none_when_too_low():
    # camera only 0.5 m above the ground plane -> h_above < 1.0 -> skip
    case = _nadir_case(height_m=0.5)
    assert _backproject_image(**case) is None


def test_ortho_canvas_dims():
    from databricks.labs.gbx.pyrx.ortho import _ortho_canvas_dims

    e0, e1, n0, n1, w, h = _ortho_canvas_dims(0.0, 100.0, 0.0, 50.0, gsd_cm=5.0)
    # 5% margin each side
    assert e0 == -5.0 and e1 == 105.0 and n0 == -2.5 and n1 == 52.5
    # (110 m / 0.05) = 2200 px wide; (55 m / 0.05) = 1100 px tall
    assert w == 2200 and h == 1100


def test_ortho_canvas_dims_tiny_nonzero():
    from databricks.labs.gbx.pyrx.ortho import _ortho_canvas_dims

    _, _, _, _, w, h = _ortho_canvas_dims(0.0, 0.0, 0.0, 0.0, gsd_cm=5.0)
    assert w >= 1 and h >= 1


def test_sparse_orthomosaic_signature_light_import():
    import inspect

    from databricks.labs.gbx.pyrx import ortho

    sig = inspect.signature(ortho.sparse_orthomosaic)
    p = sig.parameters
    assert list(p)[:3] == ["cluster_models", "image_dir", "out_tiff"]
    assert "spark" not in p  # driver-side, no Spark
    assert (
        p["gsd_cm"].default == 3.0
        and p["max_workers"].default == 8
        and p["blend_gamma"].default == 4.0
    )
    for kw in ("gsd_cm", "max_workers", "blend_gamma"):
        assert p[kw].kind is inspect.Parameter.KEYWORD_ONLY


def test_sparse_orthomosaic_end_to_end(tmp_path):
    pytest.importorskip("pycolmap")
    # A meaningful end-to-end needs a real posed reconstruction + overlapping imagery,
    # which is impractical to synthesize minimally. Assert the callable + contract here
    # and defer the true end-to-end to the on-cluster nb1a run (documented integration gate).
    from databricks.labs.gbx.pyrx import ortho

    assert callable(ortho.sparse_orthomosaic)
    pytest.skip(
        "sparse_orthomosaic end-to-end needs a real reconstruction; validated on-cluster (nb1a)"
    )
