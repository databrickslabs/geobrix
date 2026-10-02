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
