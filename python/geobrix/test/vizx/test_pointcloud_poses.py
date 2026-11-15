"""Tests for plot_point_cloud_poses and poses_to_pdf.

Multi-POSE static LiDAR renders and multi-page PDF export. Tiny synthetic point clouds
(numpy), headless (Agg backend), no real LAZ file and no cluster.
"""

import matplotlib

matplotlib.use("Agg")  # headless: must be set before importing pyplot

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from databricks.labs.gbx.vizx import _gallery as gal  # noqa: E402
from databricks.labs.gbx.vizx import _pointcloud_poses as pcp  # noqa: E402
from databricks.labs.gbx.vizx import _pointcloud_static as pcs  # noqa: E402
from databricks.labs.gbx.vizx._pointcloud_pdf import poses_to_pdf  # noqa: E402


def _xyz(n=200, seed=0, z_max=5.0, span=10.0):
    rng = np.random.default_rng(seed)
    return (
        rng.uniform(-span, span, n),
        rng.uniform(-span, span, n),
        rng.uniform(0.0, z_max, n),
    )


def _df(n=200, rgb=False):
    x, y, z = _xyz(n)
    cols = {"x": x, "y": y, "z": z}
    if rgb:
        rng = np.random.default_rng(1)
        cols.update({c: rng.integers(0, 255, n) for c in "rgb"})
    return pd.DataFrame(cols)


@pytest.fixture
def renders(monkeypatch):
    """Replace the 3-D renderer; the list collects the keyword args of each call."""
    calls = []

    def _fake_render(x, y, z, **kw):
        calls.append(kw)
        return plt.figure()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _fake_render)
    yield calls
    plt.close("all")


def test_pose_selection_all_name_list_and_exclude(renders):
    result = pcp.plot_point_cloud_poses(_df(), poses="all")
    assert {"top", "iso", "front", "back", "left", "right"} <= set(result)
    n_all = len(result)
    assert len(renders) == n_all
    assert all(isinstance(f, plt.Figure) for f in result.values())

    renders.clear()
    assert set(pcp.plot_point_cloud_poses(_df(), poses="iso")) == {"iso"}
    assert (renders[0]["elev"], renders[0]["azim"]) == pcp._POSES["iso"]
    assert set(pcp.plot_point_cloud_poses(_df(), poses=["top", "front"])) == {
        "top",
        "front",
    }
    assert len(renders) == 3  # one render per resolved pose, 1 + 2

    excluded = pcp.plot_point_cloud_poses(
        _df(), poses="all", exclude_poses=["top", "iso"]
    )
    assert "top" not in excluded and "iso" not in excluded
    assert len(excluded) == n_all - 2
    assert len(pcp.plot_point_cloud_poses(_df(), poses="all", exclude_poses="top")) == (
        n_all - 1
    )


def test_unknown_pose_names_raise_for_poses_and_exclude_poses():
    with pytest.raises(ValueError, match="unknown pose"):
        pcp.plot_point_cloud_poses(_df(), poses=["iso", "INVALID_POSE"])
    with pytest.raises(ValueError, match="unknown pose"):
        pcp.plot_point_cloud_poses(_df(), poses="all", exclude_poses="NOT_A_POSE")


def test_the_cloud_is_loaded_once_and_the_render_options_are_forwarded(
    renders, monkeypatch
):
    import databricks.labs.gbx.vizx._pointcloud as pc_mod

    loads = []
    real_load = pc_mod.load_point_cloud
    monkeypatch.setattr(
        pc_mod,
        "load_point_cloud",
        lambda d, **kw: loads.append(1) or real_load(d, **kw),
    )
    pcp.plot_point_cloud_poses(_df(), poses=["iso", "top", "front"], z_exaggeration=7.0)
    assert len(loads) == 1
    assert {c["z_exaggeration"] for c in renders} == {7.0}
    renders.clear()
    pcp.plot_point_cloud_poses(_df(), poses=["top"])
    assert renders[0]["z_exaggeration"] is None  # auto in the renderer


def test_rgb_is_used_only_when_requested_and_present(renders):
    pcp.plot_point_cloud_poses(_df(50, rgb=True), poses=["iso"], color="rgb")
    assert renders[-1]["rgb"] is not None
    pcp.plot_point_cloud_poses(_df(50, rgb=True), poses=["iso"], color="z")
    assert renders[-1]["rgb"] is None
    with pytest.warns(UserWarning, match="no RGB"):
        pcp.plot_point_cloud_poses(_df(50), poses=["iso"], color="rgb")


def test_out_dir_writes_one_png_per_pose_named_after_it(renders, tmp_path):
    result = pcp.plot_point_cloud_poses(
        _df(), poses="all", out_dir=str(tmp_path), show=None
    )
    assert sorted(p.name for p in tmp_path.glob("*.png")) == sorted(
        f"{k}.png" for k in result
    )
    assert all(p.stat().st_size > 0 for p in tmp_path.glob("*.png"))


def test_show_gallery_composes_a_grid_once_and_closes_the_pose_figures(
    renders, monkeypatch, tmp_path
):
    gallery_calls = []
    monkeypatch.setattr(
        gal, "plot_gallery", lambda path, **kw: gallery_calls.append(path)
    )

    result = pcp.plot_point_cloud_poses(_df(), poses=["iso", "front"], show="gallery")
    assert len(gallery_calls) == 1
    # Open pose figures would also auto-render as a list in a notebook.
    assert not any(plt.fignum_exists(f.number) for f in result.values())

    pcp.plot_point_cloud_poses(
        _df(), poses=["iso", "top"], show="gallery", out_dir=str(tmp_path)
    )
    assert len(gallery_calls) == 2
    assert (tmp_path / "iso.png").exists() and (tmp_path / "top.png").exists()

    pcp.plot_point_cloud_poses(_df(), poses=["iso"], show=None)
    assert len(gallery_calls) == 2  # show=None never composes a gallery


def test_render_3d_honours_figsize_dpi_and_z_exaggeration():
    x, y, z = _xyz(50)
    fig = pcs.render_point_cloud_3d(x, y, z, figsize=(6, 4), dpi=150)
    w, h = fig.get_size_inches()
    assert abs(w - 6.0) < 0.05 and abs(h - 4.0) < 0.05
    assert abs(fig.get_dpi() - 150.0) < 1.0

    # A wide, flat cloud (terrain LiDAR): explicit exaggeration scales the box z, auto
    # exaggerates a flat cloud beyond true scale.
    x, y, z = _xyz(200, z_max=10.0, span=500.0)
    box = {
        k: pcs.render_point_cloud_3d(x, y, z, z_exaggeration=k).axes[0].get_box_aspect()
        for k in (1.0, 5.0, None)
    }
    assert box[5.0][2] / box[1.0][2] == pytest.approx(5.0, abs=0.05)
    assert box[None][2] > box[1.0][2]


def test_poses_to_pdf_writes_one_page_per_pose_plus_a_title_page(tmp_path):
    def figs(names):
        return {n: plt.figure() for n in names}

    def pages(path):
        data = path.read_bytes()
        return data.count(b"/Type /Page") - data.count(b"/Type /Pages")

    plain = tmp_path / "sub1" / "sub2" / "poses.pdf"  # missing parents are created
    poses_to_pdf(figs(["iso", "top", "front", "back"]), str(plain))
    assert pages(plain) == 4
    titled = tmp_path / "titled.pdf"
    poses_to_pdf(figs(["iso", "top"]), str(titled), title="My Render")
    assert pages(titled) == 3
    plt.close("all")
