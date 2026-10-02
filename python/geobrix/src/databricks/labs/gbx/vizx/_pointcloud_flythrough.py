"""Fly-through video export for VizX point clouds.

``plot_point_cloud_flythrough`` renders a smooth camera animation — an
azimuth orbit or an eased path through named poses — encoding frames to
**MP4** (imageio + imageio-ffmpeg) and/or **GIF** (PIL). Reuses
``load_point_cloud`` (loads once) and ``render_point_cloud_3d`` (renders
per frame).

Pure Python, light-tier — no JAR required.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

# ---------------------------------------------------------------------------
# Frame-count helpers
# ---------------------------------------------------------------------------


def _n_frames(fps: float, seconds: float, frames: Optional[int]) -> int:
    """Resolve the number of frames from fps/seconds or an explicit count."""
    if frames is not None:
        return max(1, int(frames))
    return max(1, round(fps * seconds))


# ---------------------------------------------------------------------------
# Camera-path builders
# ---------------------------------------------------------------------------


def _build_flat_orbit_path(
    n_frames: int, elev: float = 30.0
) -> List[Tuple[float, float]]:
    """Return ``n_frames`` (elev, azim) pairs for a plain 360° azimuth orbit.

    Azimuth sweeps 0 → 360 with ``endpoint=False`` so the last frame
    smoothly connects back to the first when looped.  Elevation is
    constant at ``elev``.  Use ``path='orbit_flat'`` to select this path.
    """
    azimuths = np.linspace(0.0, 360.0, n_frames, endpoint=False)
    return [(float(elev), float(az)) for az in azimuths]


def _build_orbit_path(
    n_frames: int,
    elev_oblique: float = 30.0,
    elev_top: float = 90.0,
) -> List[Tuple[float, float]]:
    """Build the **default** orbit path: oblique rotation → top view → loop back.

    Four phases:

    1. **Orbit** (70 %): full 360° azimuth sweep at ``elev_oblique``.
    2. **Rise** (12 %): cosine-eased elevation from oblique up to ``elev_top``.
    3. **Hold** (10 %): stationary top-down view at ``elev_top``.
    4. **Ease back** (8 %): cosine-eased elevation back to ``elev_oblique``,
       reconnecting cleanly to frame 0 for GIF loop.

    The last frame connects visually to the first (same elev, azim≈360→0)
    so ``loop=0`` (infinite repeat) plays smoothly.
    """
    f_orbit = round(0.70 * n_frames)
    f_rise = round(0.12 * n_frames)
    f_top = round(0.10 * n_frames)
    f_fall = max(1, n_frames - f_orbit - f_rise - f_top)

    path: List[Tuple[float, float]] = []

    # Phase 1: orbit at oblique elevation
    for i in range(f_orbit):
        az = 360.0 * i / max(f_orbit, 1)
        path.append((elev_oblique, az))

    # Phase 2: ease up to top-down while azimuth stays at 360
    for i in range(f_rise):
        t = i / max(f_rise - 1, 1)
        elev = elev_oblique + (elev_top - elev_oblique) * _ease_cos(t)
        path.append((elev, 360.0))

    # Phase 3: hold at top-down
    for _ in range(f_top):
        path.append((elev_top, 360.0))

    # Phase 4: ease back down to oblique (clean loop)
    for i in range(f_fall):
        t = i / max(f_fall - 1, 1)
        elev = elev_top + (elev_oblique - elev_top) * _ease_cos(t)
        path.append((elev, 360.0))

    return path[:n_frames]


def _ease_cos(t: float) -> float:
    """Cosine ease-in-out: ``t ∈ [0, 1]`` → smoothed ``[0, 1]``."""
    return 0.5 * (1.0 - np.cos(np.pi * t))


def _angular_diff(a0: float, a1: float) -> float:
    """Shortest signed angular difference from ``a0`` to ``a1`` (degrees)."""
    d = (a1 - a0) % 360.0
    if d > 180.0:
        d -= 360.0
    return d


def _build_pose_path(pose_names: List[str], n_frames: int) -> List[Tuple[float, float]]:
    """Return ``n_frames`` (elev, azim) pairs easing through named poses.

    Loops back to the first pose to close the animation.  Each segment
    between adjacent keyframes uses cosine ease-in/out and takes the
    shortest angular path on the azimuth axis.

    Raises
    ------
    ValueError
        If fewer than 2 pose names are given.
    """
    from databricks.labs.gbx.vizx._pointcloud_poses import _POSES

    if len(pose_names) < 2:
        raise ValueError(
            "pose path needs at least 2 pose names; got "
            f"{len(pose_names)}: {pose_names!r}"
        )

    # Build a closed loop: last segment returns to the first pose
    keyframes = list(pose_names) + [pose_names[0]]
    n_segs = len(keyframes) - 1

    path: List[Tuple[float, float]] = []
    for seg_i in range(n_segs):
        elev0, azim0 = _POSES[keyframes[seg_i]]
        elev1, azim1 = _POSES[keyframes[seg_i + 1]]
        # Shortest angular difference to avoid unnecessary full rotations
        d_azim = _angular_diff(azim0, azim1)
        d_elev = elev1 - elev0

        # Each segment gets a proportional share of the total frames
        f_start = round(seg_i * n_frames / n_segs)
        f_end = round((seg_i + 1) * n_frames / n_segs)
        seg_len = max(1, f_end - f_start)

        for f in range(seg_len):
            t = f / max(seg_len - 1, 1)
            et = _ease_cos(t)
            path.append((elev0 + d_elev * et, azim0 + d_azim * et))

    return path[:n_frames]


# ---------------------------------------------------------------------------
# Frame extraction
# ---------------------------------------------------------------------------


def _fig_to_rgb_array(fig) -> np.ndarray:
    """Convert a matplotlib Figure to an ``H×W×3`` uint8 numpy array.

    Uses the Agg canvas buffer directly (no PNG round-trip) for speed.
    """
    fig.canvas.draw()
    buf = np.frombuffer(fig.canvas.buffer_rgba(), dtype=np.uint8)
    w, h = fig.canvas.get_width_height()
    rgba = buf.reshape(h, w, 4)
    return rgba[:, :, :3].copy()


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def _write_gif(frames: List[np.ndarray], path: pathlib.Path, fps: float) -> None:
    """Write an animated GIF via PIL."""
    from PIL import Image

    duration_ms = max(1, round(1000.0 / fps))
    imgs = [Image.fromarray(f) for f in frames]
    imgs[0].save(
        str(path),
        save_all=True,
        append_images=imgs[1:],
        duration=duration_ms,
        loop=0,
        optimize=False,
    )


def _write_mp4(frames: List[np.ndarray], path: pathlib.Path, fps: float) -> None:
    """Write an MP4 via imageio-ffmpeg.

    Requires ``imageio-ffmpeg`` (declared in the ``[vizx]`` extra).

    Writes to a local temp file first, then copies to the target path.
    Direct writes to FUSE-mounted UC Volumes (``/Volumes/...``) silently
    produce empty containers because imageio-ffmpeg's muxer seeks back to
    update the MP4 header after encoding — a seek that FUSE does not
    support.  The temp-then-copy pattern (per the uc-volumes rule)
    avoids this.
    """
    import shutil
    import tempfile

    import imageio

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        tmp = pathlib.Path(f.name)
    try:
        imageio.mimwrite(str(tmp), frames, fps=float(fps), macro_block_size=None)
        shutil.copy(str(tmp), str(path))
    finally:
        tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plot_point_cloud_flythrough(
    source,
    *,
    path: Union[str, List[str]] = "orbit",
    elev: float = 30.0,
    z_exaggeration=None,
    fps: float = 10.0,
    seconds: float = 8.0,
    frames: Optional[int] = None,
    max_points: int = 200_000,
    out_path: Optional[str] = None,
    formats: Tuple[str, ...] = ("mp4",),
    point_size: float = 1.5,
    figsize: Tuple[float, float] = (8.0, 6.0),
    dpi: int = 100,
    cmap: str = "viridis",
    color: str = "rgb",
    background: str = "#111111",
    seed: int = 0,
    title: Optional[str] = None,
) -> Dict[str, Optional[pathlib.Path]]:
    """Render a smooth fly-through video of a LiDAR point cloud.

    Loads ``source`` **once** via
    :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud`,
    then renders one frame per camera position via
    :func:`~databricks.labs.gbx.vizx._pointcloud_static.render_point_cloud_3d`,
    and encodes the frame sequence to MP4 and/or GIF.

    Parameters
    ----------
    source:
        A LAS/LAZ path, pandas DataFrame with x/y/z columns, or GeoDataFrame —
        anything accepted by ``load_point_cloud``.
    path:
        Camera-path recipe.

        - ``"orbit"`` (default) — oblique 360° orbit, then ease up to a
          top-down view, hold briefly, ease back down, and loop cleanly.
          Use ``elev`` to set the oblique elevation (default 30°).
        - ``"orbit_flat"`` — plain 360° azimuth sweep at a fixed
          elevation (the original orbit; useful for comparison or when
          the top-view is not wanted).
        - A list of pose names (from
          :data:`~databricks.labs.gbx.vizx._pointcloud_poses._POSES`) —
          eased interpolation through those keyframes, looping back.
    elev:
        Camera elevation in degrees for the ``"orbit"`` and
        ``"orbit_flat"`` paths (default 30°).  Ignored for pose paths.
    z_exaggeration:
        Vertical exaggeration forwarded to ``render_point_cloud_3d``; ``None``
        (default) → auto-scale so Z fills ≈ 30 % of the larger XY extent.
    fps:
        Frame rate in frames-per-second (default **10**).  Controls GIF
        inter-frame duration (``1000 / fps`` ms) and MP4 playback speed.
        Lower values → slower playback; the frame count stays the same,
        so GIF file size is unchanged by fps alone.
    seconds:
        Duration in seconds (default 8.0).  Ignored when ``frames`` is given.
    frames:
        Explicit frame count; overrides ``seconds``.
    max_points:
        Point-cloud decimation cap (default 200 000 — lower than the stills
        default for render speed).
    out_path:
        Stem path (without extension) for the output files.  The function
        appends ``.mp4`` and/or ``.gif`` per ``formats``.  Works for local
        paths and FUSE-mounted UC Volume / Workspace paths.  Parent
        directories are created automatically.  ``None`` → render frames
        but do not write any files (returns ``{}``).
    formats:
        Which formats to encode: ``("mp4",)`` (default), ``("gif",)``, or
        ``("mp4", "gif")``.  MP4 requires ``imageio-ffmpeg``; GIF uses PIL.
    point_size:
        Scatter marker size in points (default 1.5).
    figsize:
        Figure size in inches ``(width, height)`` (default ``(8, 6)``).
    dpi:
        Resolution in dots per inch for each frame (default 100 → 800×600).
    cmap:
        Matplotlib colormap for scalar (non-RGB) coloring (default
        ``"viridis"``).
    color:
        Per-point color source: ``"rgb"`` (default) uses true color when
        present, falls back to ``cmap``-on-elevation; ``"z"`` always uses
        elevation cmap.
    background:
        Figure background color (default ``"#111111"``).
    seed:
        Random seed for reproducible decimation (default 0).
    title:
        Optional figure title (same for all frames).

    Returns
    -------
    dict[str, pathlib.Path]
        Mapping from format name to the written file path, for each format
        in ``formats``.  Empty dict when ``out_path`` is ``None``.

    Raises
    ------
    ValueError
        For an invalid ``path`` value or unknown pose name.
    """
    import warnings

    import matplotlib
    import matplotlib.pyplot as plt

    from databricks.labs.gbx.vizx._pointcloud import load_point_cloud
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    # Ensure Agg backend for headless rendering
    if matplotlib.get_backend().lower() != "agg":
        try:
            matplotlib.use("Agg")
        except Exception:  # noqa: BLE001
            pass

    # ---- Resolve frame count ----
    n = _n_frames(fps, seconds, frames)

    # ---- Resolve camera path ----
    if path == "orbit":
        camera_path = _build_orbit_path(n_frames=n, elev_oblique=elev)
    elif path == "orbit_flat":
        camera_path = _build_flat_orbit_path(n_frames=n, elev=elev)
    elif isinstance(path, (list, tuple)):
        camera_path = _build_pose_path(list(path), n_frames=n)
    else:
        raise ValueError(
            f"path must be 'orbit', 'orbit_flat', or a list of pose names; "
            f"got {path!r}"
        )

    # ---- Load point cloud once ----
    column = None if color in ("rgb", "z") else color
    x, y, z, values, rgb, _src_crs = load_point_cloud(
        source, column=column, max_points=max_points, seed=seed
    )
    if color != "rgb":
        rgb = None
    elif rgb is None:
        warnings.warn(
            "plot_point_cloud_flythrough: no RGB in source; falling back to "
            "elevation cmap",
            stacklevel=2,
        )

    # ---- Render frames ----
    frame_arrays: List[np.ndarray] = []
    for frame_elev, frame_azim in camera_path:
        fig = render_point_cloud_3d(
            x,
            y,
            z,
            rgb=rgb,
            values=values,
            cmap=cmap,
            point_size=point_size,
            elev=frame_elev,
            azim=frame_azim,
            background=background,
            title=title,
            max_points=0,  # already decimated
            seed=seed,
            figsize=figsize,
            dpi=dpi,
            z_exaggeration=z_exaggeration,
        )
        frame_arrays.append(_fig_to_rgb_array(fig))
        plt.close(fig)

    # ---- Encode and write ----
    if out_path is None:
        return {}

    stem = pathlib.Path(out_path)
    stem.parent.mkdir(parents=True, exist_ok=True)

    result: Dict[str, pathlib.Path] = {}
    for fmt in formats:
        fmt = fmt.lower()
        out = stem.parent / (stem.name + f".{fmt}")
        if fmt == "mp4":
            _write_mp4(frame_arrays, out, fps)
        elif fmt == "gif":
            _write_gif(frame_arrays, out, fps)
        else:
            raise ValueError(f"unsupported format {fmt!r}; choose 'mp4' or 'gif'")
        result[fmt] = out

    return result
