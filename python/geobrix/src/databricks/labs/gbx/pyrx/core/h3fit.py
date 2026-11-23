"""The H3 resolution that fits a raster's pixel size (``gbx_rst_h3_fitresolution``).

The pixel area is the geodesic area, on the WGS84 ellipsoid, of the pixel at the tile centre: its four corners go
from the tile CRS to EPSG:4326 and ``core.crs.area_m2`` measures the quadrilateral. A tile with no CRS is read as
EPSG:4326, as ``rastertogrid*`` does. Clip a tile to the area of interest first, so its centre is at that latitude.

Two rules pick the resolution from the average H3 cell areas (h3geo.org, "Tables of Cell Statistics Across
Resolutions"; the same values as h3-py ``average_hexagon_area``):

* ``not_larger`` (default): the coarsest resolution whose average cell area is no larger than the pixel area, so a
  fitted cell never holds more than about one pixel; finer than resolution 15 gives 15.
* ``log_nearest``: the resolution whose average cell area is nearest the pixel area on a log scale (ties to the
  coarser one).

No h3 import: the Unity Catalog handler runs with rasterio, pyproj and shapely only.
"""

import math

# Average H3 cell area in m2 per resolution 0-15: h3-py 4.5.0 h3.average_hexagon_area(res, unit="m^2"), full precision.
H3_AVG_CELL_AREA_M2 = (
    4357449416078.3833,
    609788441794.1332,
    86801780398.99721,
    12393434655.08816,
    1770347654.491307,
    252903858.1819449,
    36129062.164412454,
    5161293.359717191,
    737327.5975944176,
    105332.51342720671,
    15047.50190766435,
    2149.643129451879,
    307.091875631606,
    43.870267947282954,
    6.2671811353243125,
    0.895311590760579,
)
RULES = ("not_larger", "log_nearest")
DEFAULT_RULE = "not_larger"


def check_rule(rule) -> str:
    """Return ``rule`` when it is one of :data:`RULES`; raise ``ValueError`` otherwise (case-sensitive)."""
    if not isinstance(rule, str) or rule not in RULES:
        raise ValueError(f"rule must be 'not_larger' or 'log_nearest'; got {rule!r}")
    return rule


def fit_resolution(pixel_area: float, rule: str = DEFAULT_RULE) -> int:
    """The H3 resolution (0-15) that ``rule`` fits to a pixel of ``pixel_area`` m2."""
    check_rule(rule)
    if not (
        isinstance(pixel_area, (int, float))
        and math.isfinite(pixel_area)
        and pixel_area > 0
    ):
        raise ValueError(
            f"pixel area must be a positive, finite number of m2; got {pixel_area!r}"
        )
    last = len(H3_AVG_CELL_AREA_M2) - 1
    if rule == "not_larger":
        return next(
            (r for r, a in enumerate(H3_AVG_CELL_AREA_M2) if a <= pixel_area), last
        )
    target = math.log(pixel_area)
    return min(
        range(last + 1),
        key=lambda r: (abs(math.log(H3_AVG_CELL_AREA_M2[r]) - target), r),
    )


def pixel_area_m2(ds) -> float:
    """Geodesic area in m2 of the pixel at the centre of ``ds`` (an open rasterio dataset)."""
    from rasterio.warp import transform as warp_points
    from shapely.geometry import Polygon

    from databricks.labs.gbx.core.crs import area_m2

    col, row, t = ds.width // 2, ds.height // 2, ds.transform
    corners = [
        t * (col, row),
        t * (col + 1, row),
        t * (col + 1, row + 1),
        t * (col, row + 1),
    ]
    xs, ys = [c[0] for c in corners], [c[1] for c in corners]
    if ds.crs is not None:
        xs, ys = warp_points(ds.crs, "EPSG:4326", xs, ys)
    return float(area_m2(Polygon(zip(xs, ys)), "EPSG:4326"))


def fit_resolution_of(ds, rule: str = DEFAULT_RULE) -> int:
    """:func:`fit_resolution` of the centre pixel of ``ds``."""
    check_rule(rule)
    return fit_resolution(pixel_area_m2(ds), rule)
