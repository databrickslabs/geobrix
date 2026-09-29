"""Layer model for the unified VizX viewers (vector / raster / grid / pmtiles)."""

from dataclasses import dataclass
from typing import Any, Optional

_VALID = {"vector", "raster", "grid", "pmtiles", "point_cloud"}


@dataclass
class Layer:
    kind: str
    data: Any
    geom_col: Optional[str] = None
    cellid_col: Optional[str] = None
    column: Optional[str] = None
    grid_system: Optional[str] = None
    grid_conf: Optional[dict] = None
    cmap: str = "viridis"
    category_colors: Optional[dict] = None
    scale: str = "linear"
    opacity: Optional[float] = None
    color: Optional[str] = None
    width: Optional[float] = None
    fill: bool = True
    band: Optional[int] = None
    style: Optional[dict] = None
    simplify: Optional[dict] = None
    label: Optional[str] = None
    max_points: Optional[int] = None
    point_size: Optional[float] = None
    crs: Any = None

    def __post_init__(self):
        if self.kind not in _VALID:
            raise ValueError(f"Layer.kind must be one of {_VALID}, got {self.kind!r}")


def vector_layer(
    data,
    *,
    geom_col=None,
    column=None,
    cmap="viridis",
    category_colors=None,
    scale="linear",
    fill=True,
    color=None,
    width=None,
    opacity=None,
    simplify=None,
    label=None,
):
    """A vector layer colored by ``column`` (through ``cmap``) or a single ``color``.

    ``category_colors`` (with ``column``) overrides ``cmap`` with an explicit
    ``{category_value: color}`` map, so each category renders in its own stable,
    meaningful color and the legend lists the present categories in the map's
    order -- e.g. a land-cover map reading ``{"vegetation": "green",
    "impervious": "gray", ...}`` instead of a colormap's arbitrary (alphabetical)
    assignment. Categories present in the data but absent from the map fall back
    to a neutral gray and are appended to the legend.
    """
    return Layer(
        "vector",
        data,
        geom_col=geom_col,
        column=column,
        cmap=cmap,
        category_colors=category_colors,
        scale=scale,
        fill=fill,
        color=color,
        width=width,
        opacity=opacity,
        simplify=simplify,
        label=label,
    )


def raster_layer(data, *, band=None, cmap="viridis", opacity=1.0, label=None):
    return Layer("raster", data, band=band, cmap=cmap, opacity=opacity, label=label)


def point_cloud_layer(
    data,
    *,
    column=None,
    cmap="viridis",
    category_colors=None,
    max_points=150_000,
    point_size=2.0,
    opacity=None,
    crs=None,
    label=None,
):
    """A LiDAR/point-cloud layer rendered as a decimated 2D scatter (static compositor).

    ``data`` is a LAS/LAZ file path (read via ``laspy``), a GeoDataFrame of Points
    (elevation from 3D geometry or a ``z`` column), or a pandas DataFrame with
    ``x``/``y``/``z`` columns. Points are colored by ``column`` if given, else by
    elevation ``z`` (a continuous ``cmap``); pass ``category_colors`` with a
    categorical ``column`` (e.g. classification) for explicit per-class colors and a
    discrete legend. Clouds larger than ``max_points`` are randomly decimated (seeded)
    so the render stays in memory. ``crs`` overrides the source CRS (the LAZ header /
    GeoDataFrame ``.crs``); points reproject to Web Mercator to compose over raster /
    vector layers, or draw in native coordinates when no CRS is known.

    Point-cloud layers render only in the static compositor (``plot_static``);
    interactive and 3D rendering are a planned follow-on.
    """
    return Layer(
        "point_cloud",
        data,
        column=column,
        cmap=cmap,
        category_colors=category_colors,
        max_points=max_points,
        point_size=point_size,
        opacity=opacity,
        crs=crs,
        label=label,
    )


def grid_layer(
    data,
    *,
    grid_system,
    cellid_col=None,
    column=None,
    cmap="viridis",
    scale="linear",
    opacity=0.7,
    grid_conf=None,
    label=None,
):
    """A grid (H3 / BNG / quadbin) layer, colored per cell by ``column`` through ``cmap``.

    ``scale`` controls the value→color mapping: ``"linear"`` (default) spreads color
    evenly across min..max; ``"quantile"`` spreads by percentile rank, so a skewed
    distribution (e.g. most cells holding a small count with a long tail) stays visually
    distinguishable instead of collapsing into one end of the colormap.
    """
    return Layer(
        "grid",
        data,
        grid_system=grid_system,
        cellid_col=cellid_col,
        column=column,
        cmap=cmap,
        scale=scale,
        opacity=opacity,
        grid_conf=grid_conf,
        label=label,
    )


def pmtiles_layer(
    data, *, style=None, simplify=None, label=None, opacity=None, color=None
):
    """A PMTiles layer (raster or vector, auto-detected from the archive tile type).

    ``opacity`` sets raster-opacity (raster tiles) or fill-opacity (vector tiles);
    ``color`` sets the fill-color for vector tiles. Both default to None → the tier's
    standard defaults (raster-opacity 1.0; vector fill from ``emphasis``). Use a low
    ``opacity`` to make a PMTiles layer a muted underlay/context in a multi-layer overlay.
    """
    return Layer(
        "pmtiles",
        data,
        style=style,
        simplify=simplify,
        label=label,
        opacity=opacity,
        color=color,
    )


def _looks_pmtiles(obj) -> bool:
    if isinstance(obj, (bytes, bytearray)):
        return obj[:7] == b"PMTiles"
    if isinstance(obj, str):
        return obj.endswith(".pmtiles")
    return False


def as_layers(obj) -> list:
    """Coerce a Layer / list[Layer] / bare input into list[Layer]."""
    if isinstance(obj, (list, tuple)) and len(obj) == 0:
        raise ValueError("as_layers: no layers provided")
    if isinstance(obj, Layer):
        return [obj]
    if (
        isinstance(obj, (list, tuple))
        and obj
        and all(isinstance(x, Layer) for x in obj)
    ):
        return list(obj)
    if _looks_pmtiles(obj):
        return [pmtiles_layer(obj)]
    # bare point cloud: a LAS/LAZ path -> point_cloud (else it would wrongly fall
    # through to vector_layer, which cannot read a point-cloud file).
    if isinstance(obj, str) and obj.lower().endswith((".laz", ".las")):
        return [point_cloud_layer(obj)]
    # bare raster: a path to a known raster ext, ndarray, or tile struct -> raster; else vector.
    if isinstance(obj, str) and obj.lower().endswith((".tif", ".tiff", ".cog")):
        return [raster_layer(obj)]
    try:
        import numpy as np

        if isinstance(obj, np.ndarray):
            return [raster_layer(obj)]
    except ImportError:
        pass
    return [vector_layer(obj)]
