"""Tests for plot_point_cloud_poses and poses_to_pdf.

Multi-POSE static LiDAR renders and multi-page PDF export.
All tests use a tiny synthetic point cloud (numpy) — no real LAZ file needed.
Runs headless (Agg backend); no cluster required.

Run with:
  bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/vizx/test_pointcloud_poses.py
"""

import matplotlib

matplotlib.use("Agg")  # headless: must be set before importing pyplot

import numpy as np  # noqa: E402
import pytest  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _synthetic_xyz(n=200, seed=0):
    """Return tiny x/y/z arrays for a synthetic point cloud."""
    rng = np.random.default_rng(seed)
    x = rng.uniform(-10.0, 10.0, n)
    y = rng.uniform(-10.0, 10.0, n)
    z = rng.uniform(0.0, 5.0, n)
    return x, y, z


def _synthetic_df(n=200, seed=0):
    """Return a pandas DataFrame with x/y/z columns."""
    import pandas as pd

    x, y, z = _synthetic_xyz(n, seed)
    return pd.DataFrame({"x": x, "y": y, "z": z})


# ---------------------------------------------------------------------------
# _POSES dict
# ---------------------------------------------------------------------------


def test_poses_dict_has_expected_keys():
    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    expected = {
        "top",
        "iso",
        "oblique_ne",
        "oblique_nw",
        "oblique_se",
        "oblique_sw",
        "front",
        "back",
        "left",
        "right",
    }
    assert set(_POSES.keys()) == expected


def test_poses_dict_values_are_elev_azim_tuples():
    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    for name, (elev, azim) in _POSES.items():
        assert isinstance(elev, (int, float)), f"{name}: elev must be numeric"
        assert isinstance(azim, (int, float)), f"{name}: azim must be numeric"


def test_poses_top_is_overhead():
    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    elev, _ = _POSES["top"]
    assert elev == 90


# ---------------------------------------------------------------------------
# plot_point_cloud_poses — pose resolution
# ---------------------------------------------------------------------------


def test_poses_all_resolves_full_set(monkeypatch):
    """'all' resolves to every pose in _POSES."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = []

    def _fake_render(x, y, z, **kw):
        captured.append(
            kw.get("elev"),
        )
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses="all")
    assert set(result.keys()) == set(pcp._POSES.keys())


def test_poses_single_string(monkeypatch):
    """A single pose name string resolves to exactly that pose."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    call_args = []

    def _fake_render(x, y, z, **kw):
        call_args.append({"elev": kw.get("elev"), "azim": kw.get("azim")})
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses="iso")
    assert set(result.keys()) == {"iso"}
    assert len(call_args) == 1
    assert call_args[0]["elev"] == pcp._POSES["iso"][0]
    assert call_args[0]["azim"] == pcp._POSES["iso"][1]


def test_poses_list_of_names(monkeypatch):
    """A list of pose names resolves to exactly those poses."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    call_args = []

    def _fake_render(x, y, z, **kw):
        call_args.append(kw.get("elev"))
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses=["top", "front"])
    assert set(result.keys()) == {"top", "front"}
    assert len(call_args) == 2


def test_poses_exclude_removes_names(monkeypatch):
    """exclude_poses removes the listed names from the resolved set."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses="all", exclude_poses=["top", "iso"])
    assert "top" not in result
    assert "iso" not in result
    # 10 total - 2 excluded = 8
    assert len(result) == len(pcp._POSES) - 2


def test_poses_exclude_single_string(monkeypatch):
    """exclude_poses accepts a single string (not just a list)."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses="all", exclude_poses="top")
    assert "top" not in result
    assert len(result) == len(pcp._POSES) - 1


def test_poses_unknown_name_raises_value_error():
    """An unknown pose name raises ValueError listing valid names."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp

    df = _synthetic_df()
    with pytest.raises(ValueError, match="unknown pose"):
        pcp.plot_point_cloud_poses(df, poses=["iso", "INVALID_POSE"])


def test_poses_unknown_exclude_raises_value_error():
    """An unknown name in exclude_poses raises ValueError."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp

    df = _synthetic_df()
    with pytest.raises(ValueError, match="unknown pose"):
        pcp.plot_point_cloud_poses(df, poses="all", exclude_poses="NOT_A_POSE")


# ---------------------------------------------------------------------------
# plot_point_cloud_poses — render loop calls render_point_cloud_3d per pose
# ---------------------------------------------------------------------------


def test_render_called_once_per_pose_with_correct_elev_azim(monkeypatch):
    """render_point_cloud_3d is called exactly once per resolved pose, with correct elev/azim."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    call_args = []

    def _fake_render(x, y, z, **kw):
        call_args.append({"elev": kw.get("elev"), "azim": kw.get("azim")})
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    poses_selected = ["top", "iso", "front"]
    pcp.plot_point_cloud_poses(df, poses=poses_selected)

    assert len(call_args) == 3
    seen = {(c["elev"], c["azim"]) for c in call_args}
    expected = {pcp._POSES[p] for p in poses_selected}
    assert seen == expected


def test_load_point_cloud_called_once(monkeypatch):
    """load_point_cloud is called exactly once, regardless of pose count."""
    import databricks.labs.gbx.vizx._pointcloud as pc_mod
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    load_calls = []
    _real_load = pc_mod.load_point_cloud

    def _fake_load(data, **kw):
        load_calls.append(1)
        return _real_load(data, **kw)

    monkeypatch.setattr(pc_mod, "load_point_cloud", _fake_load)

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses=["iso", "top", "front"])
    assert len(load_calls) == 1


# ---------------------------------------------------------------------------
# plot_point_cloud_poses — out_dir writes PNGs
# ---------------------------------------------------------------------------


def test_out_dir_writes_one_png_per_pose(tmp_path, monkeypatch):
    """When out_dir is provided, one PNG per resolved pose is saved."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        fig = plt.figure()
        return fig

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    poses_selected = ["top", "front", "iso"]
    pcp.plot_point_cloud_poses(
        df, poses=poses_selected, out_dir=str(tmp_path), show=None
    )

    for name in poses_selected:
        png = tmp_path / f"{name}.png"
        assert png.exists(), f"Missing {name}.png"
        assert png.stat().st_size > 0, f"Empty {name}.png"


def test_out_dir_filenames_match_pose_names(tmp_path, monkeypatch):
    """PNG filenames are exactly <pose_name>.png (no prefix/suffix mangling)."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses="all", out_dir=str(tmp_path), show=None)

    written = sorted(p.name for p in tmp_path.iterdir() if p.suffix == ".png")
    expected = sorted(f"{k}.png" for k in pcp._POSES.keys())
    assert written == expected


def test_returns_dict_of_figures(monkeypatch):
    """Return value is a dict mapping pose name → matplotlib.figure.Figure."""
    import matplotlib.figure

    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses=["iso", "top"])
    assert isinstance(result, dict)
    for name, fig in result.items():
        assert isinstance(fig, matplotlib.figure.Figure), f"{name} is not a Figure"


# ---------------------------------------------------------------------------
# render_point_cloud_3d — figsize and dpi params
# ---------------------------------------------------------------------------


def test_render_3d_figsize_respected():
    """render_point_cloud_3d with figsize=(6, 4) produces a figure of that size."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    x, y, z = _synthetic_xyz(50)
    fig = render_point_cloud_3d(x, y, z, figsize=(6, 4))
    w, h = fig.get_size_inches()
    assert abs(w - 6.0) < 0.05
    assert abs(h - 4.0) < 0.05


def test_render_3d_dpi_respected():
    """render_point_cloud_3d with dpi=150 sets the figure dpi."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    x, y, z = _synthetic_xyz(50)
    fig = render_point_cloud_3d(x, y, z, dpi=150)
    assert abs(fig.get_dpi() - 150.0) < 1.0


def test_render_3d_default_figsize_unchanged():
    """Without figsize/dpi kwargs, defaults are unchanged (back-compat)."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    x, y, z = _synthetic_xyz(50)
    fig_default = render_point_cloud_3d(x, y, z)
    # Just confirm a Figure is returned — exact size depends on rcParams, but no crash
    import matplotlib.figure

    assert isinstance(fig_default, matplotlib.figure.Figure)


# ---------------------------------------------------------------------------
# render_point_cloud_3d — z_exaggeration
# ---------------------------------------------------------------------------


def test_render_3d_z_exaggeration_explicit_scales_box_z():
    """z_exaggeration=5.0 multiplies the box-aspect z by 5× vs z_exaggeration=1.0."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    # Build a flat cloud: wide XY, narrow Z (mirrors terrain LiDAR)
    rng = np.random.default_rng(0)
    x = rng.uniform(0, 1000.0, 200)
    y = rng.uniform(0, 1000.0, 200)
    z = rng.uniform(0, 10.0, 200)  # Z is 100× smaller than XY

    fig1 = render_point_cloud_3d(x, y, z, z_exaggeration=1.0)
    fig5 = render_point_cloud_3d(x, y, z, z_exaggeration=5.0)

    ax1 = fig1.axes[0]
    ax5 = fig5.axes[0]
    # Box aspect is (x, y, z) — z component of 5× run must be ≥ 5× of 1× run
    box1 = ax1.get_box_aspect()
    box5 = ax5.get_box_aspect()
    assert box5[2] / box1[2] == pytest.approx(5.0, abs=0.05)


def test_render_3d_z_exaggeration_auto_makes_z_visible():
    """Auto z_exaggeration (None) must produce z box-aspect > true-scale for flat cloud."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    rng = np.random.default_rng(1)
    x = rng.uniform(0, 1000.0, 200)
    y = rng.uniform(0, 1000.0, 200)
    z = rng.uniform(0, 5.0, 200)  # very flat

    fig_auto = render_point_cloud_3d(x, y, z, z_exaggeration=None)
    fig_true = render_point_cloud_3d(x, y, z, z_exaggeration=1.0)

    box_auto = fig_auto.axes[0].get_box_aspect()
    box_true = fig_true.axes[0].get_box_aspect()
    # Auto must exaggerate Z more than true scale
    assert box_auto[2] > box_true[2]


def test_render_3d_z_exaggeration_one_equals_true_scale():
    """z_exaggeration=1.0 must match unexaggerated box aspect."""
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    rng = np.random.default_rng(2)
    x = rng.uniform(0, 100.0, 100)
    y = rng.uniform(0, 100.0, 100)
    z = rng.uniform(0, 100.0, 100)  # isotropic cloud — auto ≈ true scale

    fig = render_point_cloud_3d(x, y, z, z_exaggeration=1.0)
    box = fig.axes[0].get_box_aspect()
    # With z_exaggeration=1.0, z component = ptp(z) (no scaling)
    ptp_z = np.ptp(z - z.mean())
    ptp_x = np.ptp(x - x.mean())
    assert box[2] / box[0] == pytest.approx(ptp_z / ptp_x, abs=0.05)


def test_plot_point_cloud_poses_forwards_z_exaggeration(monkeypatch):
    """plot_point_cloud_poses forwards z_exaggeration to render_point_cloud_3d."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = {}

    def _fake_render(x, y, z, **kw):
        captured["z_exaggeration"] = kw.get("z_exaggeration")
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses=["iso"], z_exaggeration=7.0)
    assert captured["z_exaggeration"] == 7.0


def test_plot_point_cloud_poses_z_exaggeration_default_none(monkeypatch):
    """Default z_exaggeration=None (auto) is forwarded as None to render_point_cloud_3d."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = {}

    def _fake_render(x, y, z, **kw):
        captured["z_exaggeration"] = kw.get("z_exaggeration")
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses=["top"])
    assert captured["z_exaggeration"] is None  # default → auto in renderer


# ---------------------------------------------------------------------------
# Coloring: rgb vs cmap selection
# ---------------------------------------------------------------------------


def test_coloring_rgb_used_when_present(monkeypatch):
    """When color='rgb' and source has RGB, rgb is forwarded to render_point_cloud_3d."""
    import pandas as pd

    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = {}

    def _fake_render(x, y, z, **kw):
        captured["rgb"] = kw.get("rgb")
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    # DataFrame WITH RGB columns
    rng = np.random.default_rng(0)
    n = 50
    df = pd.DataFrame(
        {
            "x": rng.uniform(0, 1, n),
            "y": rng.uniform(0, 1, n),
            "z": rng.uniform(0, 1, n),
            "r": rng.integers(0, 255, n),
            "g": rng.integers(0, 255, n),
            "b": rng.integers(0, 255, n),
        }
    )
    pcp.plot_point_cloud_poses(df, poses=["iso"], color="rgb")
    assert (
        captured["rgb"] is not None
    ), "RGB array must be forwarded when source has color"


def test_coloring_rgb_none_when_color_z(monkeypatch):
    """When color='z', rgb is set to None even if source has RGB columns."""
    import pandas as pd

    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    captured = {}

    def _fake_render(x, y, z, **kw):
        captured["rgb"] = kw.get("rgb")
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    rng = np.random.default_rng(0)
    n = 50
    df = pd.DataFrame(
        {
            "x": rng.uniform(0, 1, n),
            "y": rng.uniform(0, 1, n),
            "z": rng.uniform(0, 1, n),
            "r": rng.integers(0, 255, n),
            "g": rng.integers(0, 255, n),
            "b": rng.integers(0, 255, n),
        }
    )
    pcp.plot_point_cloud_poses(df, poses=["iso"], color="z")
    assert (
        captured["rgb"] is None
    ), "rgb must be None when color='z', even if source has RGB"


def test_coloring_fallback_warns_when_rgb_absent(monkeypatch):
    """color='rgb' with no RGB in source issues UserWarning (elevation fallback)."""
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)

    # DataFrame WITHOUT RGB columns
    df = _synthetic_df(50)
    with pytest.warns(UserWarning, match="no RGB"):
        pcp.plot_point_cloud_poses(df, poses=["iso"], color="rgb")


# ---------------------------------------------------------------------------
# poses_to_pdf — multi-page PDF
# ---------------------------------------------------------------------------


def _make_figs_dict(poses=None, n_pts=50):
    """Return {pose_name: Figure} dict using the real renderer (headless)."""
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    if poses is None:
        poses = list(_POSES.keys())

    x, y, z = _synthetic_xyz(n_pts)
    figs = {}
    for name in poses:
        fig = plt.figure()
        fig.add_subplot(111)
        figs[name] = fig
    return figs


def test_poses_to_pdf_creates_file(tmp_path):
    """poses_to_pdf writes a file at the given path."""
    from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf

    figs = _make_figs_dict(["iso", "top", "front"])
    pdf_path = tmp_path / "poses.pdf"
    poses_to_pdf(figs, str(pdf_path))
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 0


def test_poses_to_pdf_page_count_matches_poses(tmp_path):
    """PDF has exactly one page per pose (no title page by default)."""
    from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf

    poses_selected = ["iso", "top", "front", "back"]
    figs = _make_figs_dict(poses_selected)
    pdf_path = tmp_path / "poses.pdf"
    poses_to_pdf(figs, str(pdf_path))

    # Count /Type /Page occurrences in raw PDF bytes, excluding /Type /Pages
    # (the Pages tree node).  A well-formed PDF has exactly one /Type /Page
    # entry per content page.
    data = pdf_path.read_bytes()
    count = data.count(b"/Type /Page") - data.count(b"/Type /Pages")
    assert count == len(
        poses_selected
    ), f"Expected {len(poses_selected)} pages, found {count} /Type/Page markers"


def test_poses_to_pdf_page_count_with_title(tmp_path):
    """With title= set, PDF has one extra title page."""
    from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf

    poses_selected = ["iso", "top"]
    figs = _make_figs_dict(poses_selected)
    pdf_path = tmp_path / "poses_titled.pdf"
    poses_to_pdf(figs, str(pdf_path), title="My Render")

    data = pdf_path.read_bytes()
    count = data.count(b"/Type /Page") - data.count(b"/Type /Pages")
    assert (
        count == len(poses_selected) + 1
    ), f"Expected {len(poses_selected) + 1} pages (including title page), found {count}"


def test_poses_to_pdf_accepts_dict_of_figures(tmp_path):
    """poses_to_pdf works when passed a dict[str, Figure] directly."""
    from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf

    figs = _make_figs_dict(["iso", "front"])
    pdf_path = tmp_path / "out.pdf"
    poses_to_pdf(figs, str(pdf_path))
    assert pdf_path.stat().st_size > 0


def test_poses_to_pdf_creates_parent_dirs(tmp_path):
    """poses_to_pdf creates missing parent directories."""
    from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf

    figs = _make_figs_dict(["iso"])
    nested = tmp_path / "subdir1" / "subdir2" / "poses.pdf"
    poses_to_pdf(figs, str(nested))
    assert nested.exists()


# ---------------------------------------------------------------------------
# Public API: both functions are importable from gbx.vizx
# ---------------------------------------------------------------------------


def test_plot_point_cloud_poses_exported():
    """plot_point_cloud_poses is exported from gbx.vizx."""
    from databricks.labs.gbx import vizx as vz

    assert hasattr(vz, "plot_point_cloud_poses")
    assert callable(vz.plot_point_cloud_poses)


def test_poses_to_pdf_exported():
    """poses_to_pdf is exported from gbx.vizx."""
    from databricks.labs.gbx import vizx as vz

    assert hasattr(vz, "poses_to_pdf")
    assert callable(vz.poses_to_pdf)


# ---------------------------------------------------------------------------
# show='gallery' — always composes a grid regardless of out_dir
# ---------------------------------------------------------------------------


def test_show_gallery_without_out_dir_calls_plot_gallery(monkeypatch):
    """show='gallery' with no out_dir still calls plot_gallery (not a vertical list)."""
    from databricks.labs.gbx.vizx import _gallery as gal
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    gallery_calls: list = []

    def _fake_gallery(path, **kw):
        gallery_calls.append(str(path))

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)
    monkeypatch.setattr(gal, "plot_gallery", _fake_gallery)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses=["iso", "top"], show="gallery")

    assert len(gallery_calls) == 1, "plot_gallery must be called exactly once"


def test_show_gallery_with_out_dir_calls_plot_gallery(monkeypatch, tmp_path):
    """show='gallery' with out_dir saves PNGs and still calls plot_gallery."""
    from databricks.labs.gbx.vizx import _gallery as gal
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    gallery_calls: list = []

    def _fake_gallery(path, **kw):
        gallery_calls.append(str(path))

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)
    monkeypatch.setattr(gal, "plot_gallery", _fake_gallery)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(
        df, poses=["iso", "top"], show="gallery", out_dir=str(tmp_path)
    )

    assert len(gallery_calls) == 1, "plot_gallery must be called exactly once"
    # PNGs must also be written to the permanent out_dir
    assert (tmp_path / "iso.png").exists()
    assert (tmp_path / "top.png").exists()


def test_show_gallery_closes_individual_figures(monkeypatch):
    """show='gallery' plt.close()s individual pose figures so they don't auto-render."""
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx import _gallery as gal
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    def _fake_render(x, y, z, **kw):
        return plt.figure()

    def _fake_gallery(path, **kw):
        pass

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)
    monkeypatch.setattr(gal, "plot_gallery", _fake_gallery)

    df = _synthetic_df()
    result = pcp.plot_point_cloud_poses(df, poses=["iso", "front"], show="gallery")

    for pose_name, fig in result.items():
        assert not plt.fignum_exists(fig.number), (
            f"Figure for pose '{pose_name}' was not closed — "
            "it will auto-render as a list item in a notebook"
        )


def test_show_none_does_not_call_plot_gallery(monkeypatch):
    """show=None skips gallery composition even when out_dir is provided."""
    from databricks.labs.gbx.vizx import _gallery as gal
    from databricks.labs.gbx.vizx import _pointcloud_poses as pcp
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    gallery_calls: list = []

    def _fake_gallery(path, **kw):
        gallery_calls.append(str(path))

    def _fake_render(x, y, z, **kw):
        import matplotlib.pyplot as plt

        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)
    monkeypatch.setattr(gal, "plot_gallery", _fake_gallery)

    df = _synthetic_df()
    pcp.plot_point_cloud_poses(df, poses=["iso"], show=None)

    assert len(gallery_calls) == 0, "plot_gallery must NOT be called when show=None"
