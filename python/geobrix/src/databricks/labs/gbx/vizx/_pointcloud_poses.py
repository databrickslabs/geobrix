"""Multi-POSE static LiDAR point-cloud renders for VizX.

``plot_point_cloud_poses`` renders the same point cloud from a predefined set
of named camera angles (``_POSES``), optionally saving a PNG per pose and
previewing the collection as a contact-sheet gallery.

This is a pure-Python light-tier helper (no JAR required). It composes the
existing :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud` and
:func:`~databricks.labs.gbx.vizx._pointcloud_static.render_point_cloud_3d`
renderers — each is called once per resolved pose.
"""

from __future__ import annotations

import pathlib
from typing import Dict, List, Optional, Union

import matplotlib.figure

# ---------------------------------------------------------------------------
# Named camera angles: (elev_degrees, azim_degrees)
# ---------------------------------------------------------------------------

_POSES: Dict[str, tuple] = {
    "top": (90, -90),
    "iso": (35, -60),
    "oblique_ne": (30, 45),
    "oblique_nw": (30, 135),
    "oblique_se": (30, 315),
    "oblique_sw": (30, 225),
    "front": (12, 0),
    "back": (12, 180),
    "left": (12, 90),
    "right": (12, 270),
}


def _resolve_poses(
    poses: Union[str, List[str]],
    exclude_poses: Optional[Union[str, List[str]]],
) -> Dict[str, tuple]:
    """Return the ordered subset of ``_POSES`` to render.

    Parameters
    ----------
    poses:
        ``"all"`` → every pose; a ``str`` → that single pose; a ``list[str]``
        → those poses (preserving declaration order).
    exclude_poses:
        A single pose name (str) or list of names to remove from the resolved
        set.  ``None`` → no removal.

    Raises
    ------
    ValueError
        If any name in ``poses`` or ``exclude_poses`` is not in ``_POSES``.
    """
    # Normalise to a list
    if poses == "all":
        selected = list(_POSES.keys())
    elif isinstance(poses, str):
        selected = [poses]
    else:
        selected = list(poses)

    # Normalise exclude
    if exclude_poses is None:
        excluded = []
    elif isinstance(exclude_poses, str):
        excluded = [exclude_poses]
    else:
        excluded = list(exclude_poses)

    # Validate
    all_names = set(_POSES.keys())
    unknown_sel = [n for n in selected if n not in all_names]
    unknown_exc = [n for n in excluded if n not in all_names]
    unknown = unknown_sel + unknown_exc
    if unknown:
        valid = ", ".join(sorted(all_names))
        raise ValueError(f"unknown pose name(s): {unknown!r}. Valid names: {valid}")

    after_exclude = [n for n in selected if n not in set(excluded)]
    return {n: _POSES[n] for n in after_exclude}


def plot_point_cloud_poses(
    source,
    *,
    poses: Union[str, List[str]] = "all",
    exclude_poses: Optional[Union[str, List[str]]] = None,
    color: str = "rgb",
    max_points: int = 400_000,
    point_size: float = 1.5,
    figsize: Optional[tuple] = None,
    dpi: Optional[int] = None,
    out_dir: Optional[str] = None,
    show: Optional[str] = "gallery",
    title: Optional[str] = None,
    cmap: str = "viridis",
    background: str = "#111111",
    seed: int = 0,
) -> Dict[str, matplotlib.figure.Figure]:
    """Render a point cloud from multiple named camera angles.

    Loads ``source`` **once** via
    :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud`, then
    renders one static 3D figure per resolved pose via
    :func:`~databricks.labs.gbx.vizx._pointcloud_static.render_point_cloud_3d`.

    Parameters
    ----------
    source:
        A LAS/LAZ path (str), a pandas DataFrame with x/y/z columns, or a
        GeoDataFrame of 3D points — anything accepted by ``load_point_cloud``.
    poses:
        ``"all"`` (default) → all 10 named poses; a single pose name (str);
        or a list of pose names.  Valid names: ``top``, ``iso``,
        ``oblique_ne``, ``oblique_nw``, ``oblique_se``, ``oblique_sw``,
        ``front``, ``back``, ``left``, ``right``.
    exclude_poses:
        A single pose name or list of names to exclude from ``poses``.
        ``None`` (default) → no exclusion.
    color:
        ``"rgb"`` (default) — true color from the source when present, falling
        back to cmap-on-elevation with a warning when absent.  ``"z"`` — color
        by elevation via ``cmap``.  Any other value → name of a column in
        ``source`` to use as the scalar for ``cmap``.
    max_points:
        Point cloud decimation cap per render (default 400 000 for crisp
        print-quality stills — higher than the interactive default).
    point_size:
        Scatter marker size in points (default 1.5).
    figsize:
        Figure size in inches as ``(width, height)``, e.g. ``(10, 8)``.
        ``None`` (default) → matplotlib rcParams default.
    dpi:
        Figure resolution in dots per inch, e.g. ``200`` for print-quality.
        ``None`` (default) → matplotlib rcParams default.
    out_dir:
        Directory path to save a PNG for each pose
        (``<out_dir>/<pose_name>.png``). Works for local paths, FUSE-mounted
        UC Volume paths (``/Volumes/…``), and Databricks Workspace FUSE paths.
        Parent directory is created if it does not exist.  ``None`` →
        figures are not saved.
    show:
        ``"gallery"`` (default) — when PNGs were saved (``out_dir`` is set),
        preview them via :func:`~databricks.labs.gbx.vizx._gallery.plot_gallery`.
        ``None`` → no preview.
    title:
        Optional per-figure title prefix (the pose name is appended when given).
    cmap:
        Matplotlib colormap name for scalar (non-RGB) coloring (default
        ``"viridis"``).
    background:
        Figure background color (default ``"#111111"`` — near-black).
    seed:
        Random seed for reproducible decimation (default 0).

    Returns
    -------
    dict[str, matplotlib.figure.Figure]
        Mapping from resolved pose name to its rendered Figure.

    Raises
    ------
    ValueError
        If any name in ``poses`` or ``exclude_poses`` is not a valid pose name.
    """
    import warnings

    from databricks.labs.gbx.vizx._pointcloud import load_point_cloud
    from databricks.labs.gbx.vizx._pointcloud_static import render_point_cloud_3d

    # Resolve the pose subset
    pose_map = _resolve_poses(poses, exclude_poses)

    # Load once
    column = None if color in ("rgb", "z") else color
    x, y, z, values, rgb, _src_crs = load_point_cloud(
        source, column=column, max_points=max_points, seed=seed
    )

    # Gate RGB on color= (mirrors plot_point_cloud behaviour)
    if color != "rgb":
        rgb = None
    elif rgb is None:
        warnings.warn(
            "plot_point_cloud_poses: no RGB in source; falling back to elevation cmap",
            stacklevel=2,
        )

    # Render each pose
    figs: Dict[str, matplotlib.figure.Figure] = {}
    for name, (elev, azim) in pose_map.items():
        pose_title = f"{title} — {name}" if title else None
        fig = render_point_cloud_3d(
            x,
            y,
            z,
            rgb=rgb,
            values=values,
            cmap=cmap,
            point_size=point_size,
            elev=elev,
            azim=azim,
            background=background,
            title=pose_title,
            max_points=0,  # already decimated by load_point_cloud
            seed=seed,
            figsize=figsize,
            dpi=dpi,
        )
        figs[name] = fig

    # Save PNGs
    if out_dir is not None:
        out = pathlib.Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        for name, fig in figs.items():
            png_path = out / f"{name}.png"
            save_kw = {}
            if dpi is not None:
                save_kw["dpi"] = dpi
            fig.savefig(str(png_path), bbox_inches="tight", **save_kw)

        if show == "gallery":
            try:
                from databricks.labs.gbx.vizx._gallery import plot_gallery

                plot_gallery(out, mode="all", renderer="photo")
            except Exception:  # noqa: BLE001
                # Gallery is best-effort; don't fail the main function
                pass

    return figs
