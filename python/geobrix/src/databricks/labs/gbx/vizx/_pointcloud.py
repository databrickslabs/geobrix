"""Point-cloud loading for VizX static rendering.

Normalizes a LAS/LAZ path or an in-memory points structure to decimated
``(x, y, z, values, src_crs)`` arrays for the ``point_cloud`` layer branch of
``plot_static``. Kept separate from ``_static_map`` so the laspy/geopandas
handling stays testable and the compositor branch stays thin.
"""

import warnings

import numpy as np


def load_point_cloud(data, *, column=None, max_points=150_000, crs=None, seed=0):
    """Return ``(x, y, z, values, src_crs)`` for a point cloud, decimated to max_points.

    ``data`` is a LAS/LAZ path (read via ``laspy``), a GeoDataFrame of Points
    (elevation from 3D geometry or a ``z`` column), or a pandas DataFrame with
    ``x``/``y``/``z`` columns. ``values`` is the per-point color array: ``column``
    if given (kept in its native dtype so a categorical column stays categorical),
    else elevation ``z``. ``src_crs`` is the override ``crs`` when provided, else the
    source's own CRS (LAZ header / GeoDataFrame ``.crs``), else ``None``.
    """
    src_crs = crs
    if isinstance(data, str):
        import laspy

        las = laspy.read(data)
        x = np.asarray(las.x, dtype="float64")
        y = np.asarray(las.y, dtype="float64")
        z = np.asarray(las.z, dtype="float64")
        if src_crs is None:
            # No CRS VLR / unparseable header → None; the caller may override via crs=.
            try:
                src_crs = las.header.parse_crs()
            except Exception:  # noqa: BLE001
                src_crs = None
        values = np.asarray(getattr(las, column)) if column else z
    else:
        try:
            import geopandas as gpd

            is_gdf = isinstance(data, gpd.GeoDataFrame)
        except ImportError:
            is_gdf = False
        if is_gdf:
            geom = data.geometry
            x = np.asarray(geom.x, dtype="float64")
            y = np.asarray(geom.y, dtype="float64")
            if "z" in data.columns:
                z = np.asarray(data["z"], dtype="float64")
            elif bool(geom.has_z.any()):
                z = np.asarray(geom.z, dtype="float64")
            else:
                raise ValueError(
                    "point_cloud_layer: GeoDataFrame needs 3D (z) geometry or a 'z' column"
                )
            if src_crs is None:
                src_crs = data.crs
            values = np.asarray(data[column]) if column else z
        else:
            # pandas DataFrame / dict-like with x, y, z keys.
            x = np.asarray(data["x"], dtype="float64")
            y = np.asarray(data["y"], dtype="float64")
            z = np.asarray(data["z"], dtype="float64")
            values = np.asarray(data[column]) if column else z

    n = x.shape[0]
    if max_points and n > max_points:
        idx = np.random.default_rng(seed).choice(n, size=max_points, replace=False)
        idx.sort()
        x, y, z, values = x[idx], y[idx], z[idx], values[idx]
        warnings.warn(
            f"point_cloud_layer: {n} points decimated to {max_points} "
            f"(raise max_points to render more).",
            stacklevel=2,
        )
    return x, y, z, values, src_crs
