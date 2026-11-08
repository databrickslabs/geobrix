"""Tests for plot_point_cloud_flythrough: camera path, frame count, encoding.

Headless (Agg backend), no cluster. MP4 tests skip if imageio-ffmpeg is not installed;
GIF tests always run (PIL).
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from databricks.labs.gbx.vizx import _pointcloud as pc_mod  # noqa: E402
from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft  # noqa: E402
from databricks.labs.gbx.vizx import _pointcloud_static as pcs  # noqa: E402

_TINY = dict(max_points=50, figsize=(0.2, 0.2), dpi=100)


def _tiny_fig(face="white"):
    fig = plt.figure(figsize=(0.2, 0.2), dpi=100, facecolor=face)
    fig.add_subplot(111).plot([0, 1], [0, 1])
    return fig


def _df(n=50):
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {
            "x": rng.uniform(-10.0, 10.0, n),
            "y": rng.uniform(-10.0, 10.0, n),
            "z": rng.uniform(0.0, 5.0, n),
        }
    )


@pytest.fixture
def camera(monkeypatch):
    """Run the fly-through with a stub renderer; returns the (elev, azim) of every frame."""

    def run(path="orbit", **kw):
        frames = []

        def _spy(x, y, z, **k):
            frames.append((k.get("elev"), k.get("azim")))
            return _tiny_fig()

        monkeypatch.setattr(pcs, "render_point_cloud_3d", _spy)
        kw = {"fps": 5, "seconds": 1.0, "formats": ("gif",), **_TINY, **kw}
        pft.plot_point_cloud_flythrough(_df(), path=path, out_path=None, **kw)
        plt.close("all")
        return frames

    return run


def test_frame_count_comes_from_fps_and_seconds_unless_frames_is_given(camera):
    assert len(camera(fps=10, seconds=3.0)) == 30
    assert len(camera(fps=24, seconds=2.5)) == 60
    assert len(camera(fps=10, seconds=3.0, frames=15)) == 15
    assert len(camera(fps=1, seconds=0.1)) >= 1


def test_orbit_flat_sweeps_the_azimuth_once_at_a_constant_elevation(camera):
    frames = camera("orbit_flat", frames=36, elev=45.0)
    elevs, azims = zip(*frames)
    assert len(frames) == 36 and all(abs(e - 45.0) < 0.01 for e in elevs)
    assert azims[0] == pytest.approx(0.0, abs=0.01)
    assert all(a1 > a0 for a0, a1 in zip(azims, azims[1:]))
    assert 350.0 <= azims[-1] < 360.0  # the last frame does not repeat the first


def test_orbit_rises_to_the_top_and_eases_back_for_a_clean_loop(camera):
    frames = camera("orbit", frames=80, elev=30.0)
    elevs = [e for e, _ in frames]
    assert len(frames) == 80
    assert elevs[0] == pytest.approx(30.0, abs=1.0)
    assert max(elevs) >= 88.0
    assert elevs[-1] == pytest.approx(30.0, abs=5.0)
    orbit_azims = [a for _, a in frames[: round(0.70 * 80)]]
    assert all(a1 >= a0 for a0, a1 in zip(orbit_azims, orbit_azims[1:]))


def test_a_pose_list_flies_through_the_poses_with_cosine_easing(camera):
    frames = camera(["iso", "top", "front"], frames=30)
    assert len(frames) == 30
    # The first frame is exactly the first pose, whatever follows it.
    assert frames[0] == camera(["iso", "front"], frames=30)[0]

    azims = [a for _, a in camera(["front", "back"], frames=20)]
    span = max(azims) - azims[0]
    # Third frame of a 10-frame segment: cosine easing lags the linear 2/9 share.
    assert (azims[2] - azims[0]) / span < 2 / 9 - 0.05

    with pytest.raises(ValueError, match="at least 2"):
        pft.plot_point_cloud_flythrough(_df(), path=["iso"], frames=10, **_TINY)


def test_the_cloud_is_loaded_once_whatever_the_frame_count(camera, monkeypatch):
    loads = []
    real = pc_mod.load_point_cloud
    monkeypatch.setattr(
        pc_mod, "load_point_cloud", lambda d, **kw: loads.append(1) or real(d, **kw)
    )
    assert len(camera("orbit", fps=5, seconds=1.0)) == 5
    assert len(loads) == 1


def test_gif_is_written_with_distinct_frames_and_no_out_path_writes_nothing(
    tmp_path, monkeypatch
):
    from PIL import Image

    n = []

    def _distinct(*a, **kw):
        n.append(1)  # PIL collapses identical consecutive frames: vary them
        return _tiny_fig(face=plt.cm.viridis(len(n) / 6.0))

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _distinct)
    out = pft.plot_point_cloud_flythrough(
        _df(),
        path="orbit",
        fps=5,
        seconds=1.2,
        out_path=str(tmp_path / "orbit"),
        formats=("gif",),
        **_TINY,
    )
    gif = tmp_path / "orbit.gif"
    assert out["gif"] is not None and gif.stat().st_size > 0
    with Image.open(str(gif)) as im:
        assert im.n_frames >= 2
    plt.close("all")

    none = pft.plot_point_cloud_flythrough(
        _df(),
        path="orbit",
        fps=5,
        seconds=0.4,
        out_path=None,
        formats=("gif",),
        **_TINY,
    )
    assert none == {}
    plt.close("all")


def _content_fig(*a, **kw):
    fig = plt.figure(figsize=(0.5, 0.5), dpi=100)
    ax = fig.add_subplot(111)
    ax.set_facecolor("blue")
    ax.scatter([1, 2, 3], [1, 2, 3], c="red")
    return fig


def test_mp4_is_decodable_not_an_empty_container(tmp_path, monkeypatch):
    # The artifact is asserted, not a success counter: an 8-byte stub has no readable frame.
    pytest.importorskip("imageio")
    pytest.importorskip("imageio_ffmpeg")
    import imageio

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _content_fig)
    pft.plot_point_cloud_flythrough(
        _df(),
        path="orbit_flat",
        fps=5,
        seconds=1.0,
        out_path=str(tmp_path / "test"),
        formats=("mp4",),
        max_points=50,
        figsize=(0.5, 0.5),
        dpi=100,
    )
    mp4 = tmp_path / "test.mp4"
    assert mp4.stat().st_size > 1000
    reader = imageio.get_reader(str(mp4))
    frame_count = reader.count_frames()
    reader.close()
    assert frame_count >= 1
    plt.close("all")


def test_both_formats_are_written_in_one_call(tmp_path, monkeypatch):
    pytest.importorskip("imageio_ffmpeg")
    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())
    result = pft.plot_point_cloud_flythrough(
        _df(),
        path="orbit",
        fps=5,
        seconds=1.0,
        out_path=str(tmp_path / "orbit"),
        formats=("mp4", "gif"),
        **_TINY,
    )
    assert (tmp_path / "orbit.mp4").exists() and (tmp_path / "orbit.gif").exists()
    assert result["mp4"] is not None and result["gif"] is not None
    plt.close("all")
