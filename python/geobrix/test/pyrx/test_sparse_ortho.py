import numpy as np

from databricks.labs.gbx.pyrx.ortho import _backproject_image


def _nadir_case(height_m=40.0, cam_w=64, cam_h=48, f=50.0):
    # Nadir (straight-down) camera at ENU (0,0,height) over a flat ground z=0.
    # T_cid = identity (ENU == COLMAP world frame): scale=1, R=I, t=0.
    T_cid = (1.0, np.eye(3), np.zeros(3))
    C_enu = np.array([0.0, 0.0, height_m])
    # world->cam: X_cam=+X, Y_cam=-Y, Z_cam=-Z (camera looks down +Z_cam == -Z_world)
    R_cw = np.array([[1.0, 0, 0], [0, -1.0, 0], [0, 0, -1.0]])
    t_cw = -R_cw @ C_enu
    src = np.full((cam_h, cam_w, 3), (200.0, 40.0, 40.0), dtype=np.float32)  # uniform red-ish
    # canvas: 2 m span around the footprint, 5 cm px
    gsd_m = 0.05
    e_min, e_max, n_min, n_max = -1.0, 1.0, -1.0, 1.0
    out_w = int((e_max - e_min) / gsd_m); out_h = int((n_max - n_min) / gsd_m)
    return dict(src_rgb=src, cam_f=f, cam_cx=cam_w/2, cam_cy=cam_h/2, cam_w=cam_w, cam_h=cam_h,
                R_cw=R_cw, t_cw=t_cw, T_cid=T_cid, C_enu=C_enu, z_med=0.0,
                e_min=e_min, n_max=n_max, gsd_m=gsd_m, out_w=out_w, out_h=out_h, blend_gamma=4.0)


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
