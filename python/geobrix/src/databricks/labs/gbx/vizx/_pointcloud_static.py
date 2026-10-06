"""Static matplotlib ``Axes3D`` point-cloud render for VizX.

An oblique 3D scatter -- RGB true-color or ``cmap``-on-scalar -- for the
GitHub-renderable (non-interactive) docs path. Mirrors ``plot_static``: builds
and returns the :class:`matplotlib.figure.Figure` without calling
``pyplot.show()``; the caller (notebook cell / ``plot_static`` compositor)
displays it.
"""

import numpy as np


def render_point_cloud_3d(
    x,
    y,
    z,
    *,
    rgb=None,
    values=None,
    cmap="viridis",
    point_size=2.0,
    elev=30.0,
    azim=-60.0,
    background="#111111",
    title=None,
    max_points=120_000,
    seed=0,
    figsize=None,
    dpi=None,
    z_exaggeration=None,
):
    """Render a decimated, centered point cloud as a static 3D matplotlib Figure.

    ``x``/``y``/``z`` are plain coordinate arrays (see
    :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud` for building
    them from a LAS/LAZ path or a DataFrame). Decimates to ``max_points`` with a
    seeded reproducible sample, keeping ``rgb``/``values`` index-aligned with
    the kept points. The cloud is centered on its own mean on each axis before
    plotting. Color is true RGB (``rgb / 255``) when ``rgb`` is given, else
    ``values`` (elevation ``z`` as a fallback) mapped through ``cmap``.

    An empty cloud (``len(x) == 0``) returns an empty 3D figure without
    raising. A singleton or degenerate (zero-extent) axis falls back to a unit
    box-aspect ratio on that axis instead of dividing by zero.

    ``figsize`` sets the figure size in inches as ``(width, height)``; defaults
    to matplotlib's ``rcParams["figure.figsize"]`` when ``None``. ``dpi``
    sets the figure resolution in dots-per-inch; defaults to
    ``rcParams["figure.dpi"]`` when ``None``. Both are useful for print-quality
    stills (e.g. ``figsize=(12, 9), dpi=300``).

    ``z_exaggeration`` scales the Z axis visually so terrain structure is
    visible when the vertical range is small relative to the XY extent (common
    for airborne LiDAR over wide scenes). ``None`` (default) auto-computes a
    factor so that the Z extent occupies ≈ 30 % of the larger XY extent,
    clamped to a minimum of 1.0 (never squashes Z). Pass ``1.0`` for true
    geographic scale, or a positive float to set an explicit multiplier.

    Does not call ``pyplot.show()`` -- the caller displays the returned
    Figure, consistent with ``plot_static``.
    """
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 -- registers the 3d projection

    x = np.asarray(x, dtype="float64")
    y = np.asarray(y, dtype="float64")
    z = np.asarray(z, dtype="float64")

    n = x.shape[0]
    if max_points and n > max_points:
        idx = np.random.default_rng(seed).choice(n, size=max_points, replace=False)
        idx.sort()
        x, y, z = x[idx], y[idx], z[idx]
        if rgb is not None:
            rgb = rgb[idx]
        if values is not None:
            values = values[idx]
        n = x.shape[0]

    fig_kw = {}
    if figsize is not None:
        fig_kw["figsize"] = figsize
    if dpi is not None:
        fig_kw["dpi"] = dpi
    fig = plt.figure(**fig_kw)
    ax = fig.add_subplot(111, projection="3d")
    fig.set_facecolor(background)
    ax.set_facecolor(background)

    if n == 0:
        if title:
            ax.set_title(title)
        return fig

    x = x - x.mean()
    y = y - y.mean()
    z = z - z.mean()

    scatter_kwargs = {}
    if rgb is not None:
        c = np.asarray(rgb, dtype="float64") / 255.0
    else:
        c = values if values is not None else z
        scatter_kwargs["cmap"] = cmap

    ax.scatter(
        x,
        y,
        z,
        c=c,
        s=point_size,
        marker=".",
        depthshade=False,
        linewidths=0,
        **scatter_kwargs,
    )

    ax.view_init(elev=elev, azim=azim)

    ptp_x = np.ptp(x) or 1
    ptp_y = np.ptp(y) or 1
    ptp_z = np.ptp(z) or 1
    if z_exaggeration is None:
        # Auto: scale Z so it occupies ~30% of the larger XY extent.
        target_z = max(ptp_x, ptp_y) * 0.3
        z_exag = max(1.0, target_z / ptp_z)
    else:
        z_exag = float(z_exaggeration)
    ax.set_box_aspect((ptp_x, ptp_y, ptp_z * z_exag))

    # Clean look: hide panes/ticks so the background reads as a plain canvas.
    ax.xaxis.pane.set_visible(False)
    ax.yaxis.pane.set_visible(False)
    ax.zaxis.pane.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_zticks([])

    if title:
        ax.set_title(title)

    return fig
