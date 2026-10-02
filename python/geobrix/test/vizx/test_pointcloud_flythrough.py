"""Tests for plot_point_cloud_flythrough — camera path, frame count, encoding.

Headless (Agg backend); no cluster required. MP4 tests skip if
imageio-ffmpeg is not installed; GIF tests always run (PIL).

Run with:
  bash scripts/commands/gbx-test-python.sh \\
    --path python/geobrix/test/vizx/test_pointcloud_flythrough.py
"""

import matplotlib

matplotlib.use("Agg")

import numpy as np  # noqa: E402
import pytest  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tiny_fig():
    """Return a tiny 20×20 px matplotlib figure for fast frame tests."""
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(0.2, 0.2), dpi=100)
    ax = fig.add_subplot(111)
    ax.plot([0, 1], [0, 1])
    return fig


def _synthetic_df(n=50, seed=0):
    import pandas as pd

    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "x": rng.uniform(-10.0, 10.0, n),
            "y": rng.uniform(-10.0, 10.0, n),
            "z": rng.uniform(0.0, 5.0, n),
        }
    )


# ---------------------------------------------------------------------------
# _n_frames: frame-count resolution
# ---------------------------------------------------------------------------


def test_n_frames_from_seconds():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _n_frames

    assert _n_frames(fps=10, seconds=3.0, frames=None) == 30


def test_n_frames_explicit_overrides_seconds():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _n_frames

    assert _n_frames(fps=10, seconds=3.0, frames=15) == 15


def test_n_frames_minimum_one():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _n_frames

    assert _n_frames(fps=1, seconds=0.1, frames=None) >= 1


def test_n_frames_fractional_seconds():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _n_frames

    # fps=24, seconds=2.5 → 60 frames
    assert _n_frames(fps=24, seconds=2.5, frames=None) == 60


# ---------------------------------------------------------------------------
# _build_orbit_path: azimuth sweep
# ---------------------------------------------------------------------------


# --- _build_flat_orbit_path: plain 360° sweep (path='orbit_flat') ---


def test_flat_orbit_path_length():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_flat_orbit_path

    path = _build_flat_orbit_path(n_frames=36)
    assert len(path) == 36


def test_flat_orbit_azimuth_monotonically_increasing():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_flat_orbit_path

    path = _build_flat_orbit_path(n_frames=36)
    azimuths = [az for _, az in path]
    for a0, a1 in zip(azimuths, azimuths[1:]):
        assert a1 > a0, f"azimuth not monotonic: {a0} -> {a1}"


def test_flat_orbit_azimuth_starts_at_zero():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_flat_orbit_path

    path = _build_flat_orbit_path(n_frames=36)
    assert path[0][1] == pytest.approx(0.0, abs=0.01)


def test_flat_orbit_azimuth_covers_full_circle():
    """Last azimuth must be < 360 (endpoint=False) and close to 360."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_flat_orbit_path

    path = _build_flat_orbit_path(n_frames=36)
    last_az = path[-1][1]
    assert last_az < 360.0
    assert last_az >= 350.0  # 36 frames → 350.0 exactly


def test_flat_orbit_elev_constant():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_flat_orbit_path

    path = _build_flat_orbit_path(n_frames=20, elev=45.0)
    elevs = [el for el, _ in path]
    assert all(abs(e - 45.0) < 0.01 for e in elevs)


# --- _build_orbit_path: new default orbit→top→loop ---


def test_orbit_path_length():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_orbit_path

    path = _build_orbit_path(n_frames=80)
    assert len(path) == 80


def test_orbit_path_starts_at_oblique_elev():
    """First frame must be at the oblique elevation."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_orbit_path

    path = _build_orbit_path(n_frames=80, elev_oblique=30.0, elev_top=90.0)
    assert path[0][0] == pytest.approx(30.0, abs=1.0)


def test_orbit_path_reaches_top():
    """Some frame in the rise/hold phase must be at or near elev_top."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_orbit_path

    path = _build_orbit_path(n_frames=80, elev_oblique=30.0, elev_top=90.0)
    elevs = [el for el, _ in path]
    assert max(elevs) >= 88.0, "orbit path never reached top elevation"


def test_orbit_path_phase1_azimuth_monotonic():
    """First 70% of frames (orbit phase) should have increasing azimuth."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_orbit_path

    n = 80
    path = _build_orbit_path(n_frames=n, elev_oblique=30.0, elev_top=90.0)
    orbit_end = round(0.70 * n)
    azimuths = [az for _, az in path[:orbit_end]]
    for a0, a1 in zip(azimuths, azimuths[1:]):
        assert a1 >= a0, f"phase-1 azimuth not non-decreasing: {a0} -> {a1}"


def test_orbit_path_ends_near_oblique():
    """Last frame must ease back down to oblique elevation for clean GIF loop."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_orbit_path

    path = _build_orbit_path(n_frames=80, elev_oblique=30.0, elev_top=90.0)
    assert path[-1][0] == pytest.approx(30.0, abs=5.0)


# ---------------------------------------------------------------------------
# _build_pose_path: eased keyframe interpolation
# ---------------------------------------------------------------------------


def test_pose_path_length():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_pose_path

    path = _build_pose_path(["iso", "top", "front"], n_frames=30)
    assert len(path) == 30


def test_pose_path_starts_at_first_pose():
    """First frame must be exactly at the first pose (t=0 of first segment)."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_pose_path
    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    poses = ["iso", "top", "front"]
    path = _build_pose_path(poses, n_frames=30)
    expected_elev, expected_azim = _POSES[poses[0]]
    assert path[0][0] == pytest.approx(expected_elev, abs=0.5)
    assert path[0][1] == pytest.approx(expected_azim, abs=0.5)


def test_pose_path_cosine_easing_not_linear():
    """Midpoint of a segment must be eased (≠ linear interpolation midpoint)."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import (
        _build_pose_path,
        _ease_cos,
    )

    # Use two poses with a large azimuth gap for easy comparison
    path = _build_pose_path(["front", "back"], n_frames=20)
    # The path loops: front → back → front (2 segments of 10 each)
    # In the first segment (frames 0–9), midpoint is frame ~5
    seg_frames = 10
    mid_idx = seg_frames // 2

    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    az0 = _POSES["front"][1]  # 0
    az1 = _POSES["back"][1]  # 180

    # Linear midpoint would be at az0 + 0.5*(az1-az0) = 90
    # Cosine-eased midpoint uses ease(0.5) = 0.5 → actually same for t=0.5
    # Let's check t=0.25: ease(0.25) = 0.5*(1-cos(pi*0.25)) ≈ 0.146
    # vs linear 0.25 → eased value ≠ linear
    # Check frame at t≈0.25 (frame index ~2 in segment of 10)
    t_quarter = 2 / (seg_frames - 1)
    linear_az = az0 + (az1 - az0) * t_quarter
    eased_t = _ease_cos(t_quarter)
    eased_az = az0 + (az1 - az0) * eased_t

    actual_az = path[2][1]
    # Eased must be closer to actual than linear (or at least different)
    assert abs(actual_az - eased_az) < abs(actual_az - linear_az) + 0.5


def test_pose_path_requires_at_least_two_poses():
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _build_pose_path

    with pytest.raises(ValueError, match="at least 2"):
        _build_pose_path(["iso"], n_frames=10)


# ---------------------------------------------------------------------------
# _fig_to_rgb_array
# ---------------------------------------------------------------------------


def test_fig_to_rgb_array_shape():
    """_fig_to_rgb_array returns HxWx3 uint8 array."""
    from databricks.labs.gbx.vizx._pointcloud_flythrough import _fig_to_rgb_array

    fig = _tiny_fig()
    arr = _fig_to_rgb_array(fig)
    assert arr.ndim == 3
    assert arr.shape[2] == 3
    assert arr.dtype == np.uint8


# ---------------------------------------------------------------------------
# GIF encoding (PIL always available)
# ---------------------------------------------------------------------------


def test_gif_written_to_path(tmp_path, monkeypatch):
    """plot_point_cloud_flythrough writes a non-empty GIF."""
    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())

    out_stem = str(tmp_path / "orbit")
    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=out_stem,
        formats=("gif",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    gif = tmp_path / "orbit.gif"
    assert gif.exists(), "GIF file not created"
    assert gif.stat().st_size > 0


def test_gif_has_multiple_frames(tmp_path):
    """_write_gif produces a multi-frame GIF when given distinct frames."""
    from PIL import Image

    from databricks.labs.gbx.vizx._pointcloud_flythrough import _write_gif

    # Distinct solid-color frames — PIL will NOT collapse these
    rng = np.random.default_rng(0)
    frames = [
        np.full((20, 20, 3), fill_value=rng.integers(0, 256, 3), dtype=np.uint8)
        for _ in range(6)
    ]
    out = tmp_path / "multi.gif"
    _write_gif(frames, out, fps=5)

    with Image.open(str(out)) as im:
        n = im.n_frames
    assert n >= 2, f"Expected multi-frame GIF, got {n} frames"


# ---------------------------------------------------------------------------
# MP4 encoding (skip if imageio-ffmpeg absent)
# ---------------------------------------------------------------------------


def test_mp4_written_to_path(tmp_path, monkeypatch):
    pytest.importorskip("imageio")
    pytest.importorskip("imageio_ffmpeg")

    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())

    out_stem = str(tmp_path / "orbit")
    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit_flat",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=out_stem,
        formats=("mp4",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    mp4 = tmp_path / "orbit.mp4"
    assert mp4.exists(), "MP4 file not created"
    assert (
        mp4.stat().st_size > 1000
    ), f"MP4 is only {mp4.stat().st_size} bytes — looks like an empty container"


def test_mp4_is_decodable_not_empty_container(tmp_path, monkeypatch):
    """MP4 must be decodable (≥1 readable frame), not an 8-byte stub.

    This test asserts the artifact itself, not a success counter.
    """
    pytest.importorskip("imageio")
    pytest.importorskip("imageio_ffmpeg")

    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    # Use tiny but real-content figures (bigger than 20×20 to avoid PIL quantization)
    def _content_fig():
        import matplotlib.pyplot as plt

        fig = plt.figure(figsize=(0.5, 0.5), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_facecolor("blue")
        ax.scatter([1, 2, 3], [1, 2, 3], c="red")
        return fig

    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _content_fig())

    out_stem = str(tmp_path / "test")
    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit_flat",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=out_stem,
        formats=("mp4",),
        figsize=(0.5, 0.5),
        dpi=100,
    )
    mp4 = tmp_path / "test.mp4"
    assert (
        mp4.stat().st_size > 1000
    ), f"MP4 size {mp4.stat().st_size} bytes — empty container"

    # Read back at least 1 frame to confirm it's decodable
    import imageio

    reader = imageio.get_reader(str(mp4))
    frame_count = reader.count_frames()
    reader.close()
    assert frame_count >= 1, f"MP4 has {frame_count} readable frames (expected ≥1)"


def test_both_formats_written(tmp_path, monkeypatch):
    pytest.importorskip("imageio_ffmpeg")

    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())

    out_stem = str(tmp_path / "orbit")
    df = _synthetic_df()
    result = pft.plot_point_cloud_flythrough(
        df,
        path="orbit",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=out_stem,
        formats=("mp4", "gif"),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    assert (tmp_path / "orbit.mp4").exists()
    assert (tmp_path / "orbit.gif").exists()
    assert "mp4" in result and result["mp4"] is not None
    assert "gif" in result and result["gif"] is not None


# ---------------------------------------------------------------------------
# Return value and out_path=None
# ---------------------------------------------------------------------------


def test_returns_none_paths_when_no_out_path(monkeypatch):
    """When out_path=None, returns {} (no files written)."""
    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())

    df = _synthetic_df()
    result = pft.plot_point_cloud_flythrough(
        df,
        path="orbit",
        fps=5,
        seconds=0.4,
        max_points=50,
        out_path=None,
        formats=("gif",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    assert result == {}


# ---------------------------------------------------------------------------
# render_point_cloud_3d called once per frame with correct elev/azim
# ---------------------------------------------------------------------------


def test_render_called_per_frame_with_orbit_flat_angles(monkeypatch):
    """render_point_cloud_3d is called once per frame with monotonic orbit_flat azimuths."""
    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    call_args = []

    def _spy(x, y, z, **kw):
        call_args.append({"elev": kw.get("elev"), "azim": kw.get("azim")})
        return _tiny_fig()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _spy)

    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit_flat",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=None,
        formats=("gif",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    assert len(call_args) == 5  # fps=5, seconds=1.0
    azimuths = [c["azim"] for c in call_args]
    for a0, a1 in zip(azimuths, azimuths[1:]):
        assert a1 > a0, "orbit_flat azimuths not monotonic"


def test_render_called_per_frame_orbit_top_reaches_high_elev(monkeypatch):
    """Default orbit path (orbit→top) must raise elev above 80° at some frame."""
    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    elevs = []

    def _spy(x, y, z, **kw):
        elevs.append(kw.get("elev", 0))
        return _tiny_fig()

    monkeypatch.setattr(pcs, "render_point_cloud_3d", _spy)

    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit",
        fps=5,
        seconds=4.0,  # enough frames to reach top phase
        max_points=50,
        out_path=None,
        formats=("gif",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    assert max(elevs) >= 80.0, f"orbit path never reached high elev; max={max(elevs)}"


def test_load_point_cloud_called_once_in_flythrough(monkeypatch):
    """load_point_cloud is called exactly once regardless of frame count."""
    import databricks.labs.gbx.vizx._pointcloud as pc_mod
    from databricks.labs.gbx.vizx import _pointcloud_flythrough as pft
    from databricks.labs.gbx.vizx import _pointcloud_static as pcs

    load_calls = []
    _real = pc_mod.load_point_cloud

    def _spy_load(data, **kw):
        load_calls.append(1)
        return _real(data, **kw)

    monkeypatch.setattr(pc_mod, "load_point_cloud", _spy_load)
    monkeypatch.setattr(pcs, "render_point_cloud_3d", lambda *a, **kw: _tiny_fig())

    df = _synthetic_df()
    pft.plot_point_cloud_flythrough(
        df,
        path="orbit",
        fps=5,
        seconds=1.0,
        max_points=50,
        out_path=None,
        formats=("gif",),
        figsize=(0.2, 0.2),
        dpi=100,
    )
    assert len(load_calls) == 1


# ---------------------------------------------------------------------------
# Public API export
# ---------------------------------------------------------------------------


def test_plot_point_cloud_flythrough_exported():
    """plot_point_cloud_flythrough is exported from gbx.vizx."""
    from databricks.labs.gbx import vizx as vz

    assert hasattr(vz, "plot_point_cloud_flythrough")
    assert callable(vz.plot_point_cloud_flythrough)
