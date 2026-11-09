"""DEM terrain derivatives: slope, aspect, hillshade (Horn's 3x3 method).

Implements the same algorithms as ``gdaldem`` but in pure NumPy — no scipy.
Exact bit-parity with GDAL is NOT guaranteed; the outputs match to within
a rounding tolerance on well-behaved inputs.

Horn's method:
    For each pixel, take the 3x3 neighbourhood (edge-replicated):
        a=NW  b=N   c=NE
        d=W   e=ctr f=E
        g=SW  h=S   i=SE

    dzdx = ((c + 2f + i) - (a + 2d + g)) / (8 * xres)
    dzdy = ((g + 2h + i) - (a + 2b + c)) / (8 * yres)

For slope/aspect/hillshade the horizontal scale is auto-derived from the CRS
(GDAL 3.11 behavior): geographic grids use a latitude-based degree->metre ratio,
projected grids use linear units, and CRS-less grids use unit scale. Override
via the ``xscale``/``yscale`` parameters (both or neither).

Single-band output in every case:
    slope     -> Float32 (degrees or percent)   nodata -9999
    aspect    -> Float32 (compass or trig deg)  nodata -9999
    hillshade -> uint8   valid [1..255], 0=nodata  nodata 0
"""

import math

import numpy as np

from databricks.labs.gbx.pyrx.core._nodata import emit, propagate_invalid, read_masked

_NODATA = -9999.0


def _horn_gradients(ds) -> tuple:
    """Return (dzdx, dzdy, valid) for the first band using Horn's 3x3 kernel.

    Edge pixels are computed using edge-replication (``np.pad(mode='edge')``),
    so the output arrays are the same shape as the source band.  Pixel
    ground sizes come from the absolute values of the affine transform's
    scale components.  ``valid`` is the input validity mask (False where the
    source band is NoData) for downstream NoData propagation.
    """
    xres = abs(ds.transform.a)
    yres = abs(ds.transform.e)

    band, valid = read_masked(ds)
    # Edge-replicate one pixel on each side.
    p = np.pad(band, 1, mode="edge")

    a = p[0:-2, 0:-2]  # NW
    b = p[0:-2, 1:-1]  # N
    c = p[0:-2, 2:]  # NE
    d = p[1:-1, 0:-2]  # W
    f = p[1:-1, 2:]  # E
    g = p[2:, 0:-2]  # SW
    h = p[2:, 1:-1]  # S
    i = p[2:, 2:]  # SE

    dzdx = ((c + 2.0 * f + i) - (a + 2.0 * d + g)) / (8.0 * xres)
    dzdy = ((g + 2.0 * h + i) - (a + 2.0 * b + c)) / (8.0 * yres)

    return dzdx, dzdy, valid


def _gdaldem_scale(ds) -> tuple:
    """Replicate GDAL 3.11 gdaldem auto-scale (xscale, yscale) from the CRS.

    Mirrors apps/gdaldem_lib.cpp GDALDEMProcessing defaulting (triggered when no
    explicit scale): geographic CRS -> anisotropic latitude-based degree->metre;
    projected CRS -> linear units; unknown/no CRS -> (1.0, 1.0).
    """
    crs = ds.crs
    if crs is None:
        return 1.0, 1.0
    try:
        from databricks.labs.gbx.core.crs import to_pyproj_crs

        pcrs = to_pyproj_crs(crs)
    except Exception:
        return 1.0, 1.0
    zunit = 1.0  # vertical unit; GDAL assumes metre when band UnitType is unset
    units = getattr(ds, "units", None)
    if units and units[0]:
        u = units[0].lower()
        if u in ("ft", "foot", "feet", "foot (international)"):
            zunit = 0.3048
        elif u in ("us-ft", "foot_us", "us survey foot"):
            zunit = 1200.0 / 3937.0
    if pcrs.is_geographic:
        ang = math.pi / 180.0  # GetAngularUnits for degree-based geographic CRS
        a = pcrs.ellipsoid.semi_major_metre
        yscale = ang * a / zunit
        mean_lat = (ds.transform.f + ds.height * ds.transform.e / 2.0) * ang
        xscale = yscale * math.cos(mean_lat)
        return xscale, yscale
    if pcrs.is_projected:
        lin = pcrs.axis_info[0].unit_conversion_factor  # metres per linear unit
        s = lin / zunit
        return s, s
    return 1.0, 1.0


# One scale without the other is refused, with the same text as the engine (engine/argv.py).
SLOPE_HALF_SCALE_REFUSAL = (
    "gbx_rst_slope: xscale and yscale must be supplied together (both or neither). "
    "Fix: pass both xscale and yscale, or neither."
)


HILLSHADE_HALF_SCALE_REFUSAL = SLOPE_HALF_SCALE_REFUSAL.replace(
    "gbx_rst_slope", "gbx_rst_hillshade"
)

SLOPE_UNITS = ("degrees", "percent")


def slope_unit_refusal(unit) -> str:
    """The reason of a slope unit that is neither degrees nor percent.

    The head is the wording of the spec engine's enum binder (``unit must be one of 'degrees', 'percent' (got
    'radians')``), so the light, UC and engine surfaces share it; the light core adds the Fix.
    """
    return (
        "gbx_rst_slope: unit must be one of 'degrees', 'percent' "
        f"(got '{unit}'). Fix: pass 'degrees' or 'percent'"
    )


def normalize_slope_unit(unit) -> str:
    """The slope unit as a lower-case word, or ValueError for a unit that is neither degrees nor percent.

    The case and the surrounding spaces do not matter, as in the spec engine's enum binder, so every level agrees.
    """
    word = str(unit).strip().lower()
    if word not in SLOPE_UNITS:
        raise ValueError(slope_unit_refusal(unit))
    return word


def _resolve_scale(ds, xscale, yscale) -> tuple:
    """Explicit (xscale, yscale) when BOTH given, else GDAL-normal auto-scale."""
    if xscale is not None and yscale is not None:
        return float(xscale), float(yscale)
    return _gdaldem_scale(ds)


def slope(ds, unit: str = "degrees", xscale=None, yscale=None) -> bytes:
    """Compute terrain slope (Horn's method), matching gdaldem.

    By default the horizontal scale is auto-derived from the CRS (GDAL 3.11
    behavior): geographic grids use a latitude-based degree->metre ratio,
    projected grids use linear units. Pass explicit ``xscale``/``yscale``
    (both, vertical-units-per-horizontal-unit, e.g. ~111120 for degrees) to
    override.

    Args:
        ds:    Open rasterio DatasetReader. Band 1 is the DEM.
        unit:  ``"degrees"`` (default) or ``"percent"``, in any case; any other value raises.
        xscale, yscale: optional explicit scale overrides (both or neither).

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    unit = normalize_slope_unit(unit)
    if (xscale is None) != (yscale is None):
        raise ValueError(SLOPE_HALF_SCALE_REFUSAL)
    dzdx, dzdy, valid = _horn_gradients(ds)
    xs, ys = _resolve_scale(ds, xscale, yscale)
    magnitude = np.sqrt((dzdx / xs) ** 2 + (dzdy / ys) ** 2)
    slope_rad = np.arctan(magnitude)
    if unit == "percent":
        result = np.tan(slope_rad) * 100.0
    else:
        result = np.degrees(slope_rad)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def aspect(
    ds,
    trigonometric: bool = False,
    zero_for_flat: bool = False,
) -> bytes:
    """Compute terrain aspect (Horn's method), matching gdaldem.

    Aspect is a pure direction (the compass bearing of steepest descent), so it
    is computed from the raw gradient ratio ``atan2(dzdy, -dzdx)``. ``gdaldem
    aspect`` does NOT apply the horizontal CRS scale: scaling dx/dy by equal
    factors leaves the angle unchanged, and GDAL never applies the anisotropic
    geographic scale to aspect (only slope/hillshade, whose output depends on
    gradient magnitude, are scale-aware). pyrx matches that — no ``xscale`` /
    ``yscale`` parameters here, by design.

    Args:
        ds:              Open rasterio DatasetReader. Band 1 used as DEM.
        trigonometric:   If True, math-convention degrees (CCW from east).
        zero_for_flat:   If True, flat cells get 0 instead of -9999.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    dzdx, dzdy, valid = _horn_gradients(ds)
    dx, dy = dzdx, dzdy
    aspect_rad = np.arctan2(dy, -dx)
    aspect_deg = np.degrees(aspect_rad)
    if trigonometric:
        result = aspect_deg % 360.0  # [0, 360), as GDAL gives it
    else:
        result = (90.0 - aspect_deg) % 360.0
    flat_mask = (dx == 0.0) & (dy == 0.0)
    flat_value = 0.0 if zero_for_flat else _NODATA
    result = np.where(flat_mask, flat_value, result)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def _neighbors(band: np.ndarray) -> tuple:
    """Return (center, [n0..n7]) for a 2-D array using edge-replication.

    Neighbor order (matches Wilson 2007 / gdaldem convention):
        n0=NW  n1=N   n2=NE
        n3=W          n4=E
        n5=SW  n6=S   n7=SE
    """
    p = np.pad(band, 1, mode="edge")
    center = p[1:-1, 1:-1]
    neighbors = [
        p[0:-2, 0:-2],  # NW
        p[0:-2, 1:-1],  # N
        p[0:-2, 2:],  # NE
        p[1:-1, 0:-2],  # W
        p[1:-1, 2:],  # E
        p[2:, 0:-2],  # SW
        p[2:, 1:-1],  # S
        p[2:, 2:],  # SE
    ]
    return center, neighbors


def tri(ds) -> bytes:
    """Compute Terrain Ruggedness Index (Riley 1999, ``gdaldem`` default).

    TRI = sqrt of the sum of squared differences between the center cell and
    each of its 8 neighbours (3x3 edge-replicated window).  This matches the
    Riley algorithm that ``gdaldem TRI`` has used by default since GDAL 3.3.
    Flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    result = np.sqrt(
        np.sum(np.stack([(center - n) ** 2 for n in nbrs], axis=0), axis=0)
    )
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def tpi(ds) -> bytes:
    """Compute Topographic Position Index.

    TPI = center - mean(8 neighbours).  Positive values are local highs;
    negative values are local lows; flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    result = center - np.mean(np.stack(nbrs, axis=0), axis=0)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def roughness(ds) -> bytes:
    """Compute terrain roughness (max - min over the 3x3 window).

    Roughness = max(3x3 window including centre) - min(3x3 window including
    centre).  Flat terrain yields 0.

    Args:
        ds: Open rasterio DatasetReader.  Band 1 is used as the DEM.

    Returns:
        Single-band Float32 GTiff bytes; nodata = -9999.
    """
    band, valid = read_masked(ds)
    center, nbrs = _neighbors(band)
    window = np.stack([center] + nbrs, axis=0)  # shape (9, H, W)
    result = np.max(window, axis=0) - np.min(window, axis=0)
    return emit(ds, result, _NODATA, propagate_invalid(valid), "float32")


def hillshade(
    ds,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
    xscale=None,
    yscale=None,
) -> bytes:
    """Compute hillshade matching ``gdaldem hillshade`` (Horn).

    Uses GDAL's exact gradient-magnitude rational form (the cosine-of-the-
    incidence-angle ``cang`` expression from ``GDALHillshadeAlg``), expressed
    in terms of pyrx's Horn gradients (``dzdx``, ``dzdy``).  Azimuth and
    altitude follow GDAL's convention: azimuth is degrees clockwise from north
    (default 315 = NW); altitude is the sun's angle above the horizon in
    degrees (default 45).  ``z_factor`` is vertical exaggeration.  The
    horizontal scale is auto-derived from the CRS by default (GDAL 3.11),
    overridable via ``xscale``/``yscale``.

    Args:
        ds:        Open rasterio DatasetReader.  Band 1 used as DEM.
        azimuth:   Sun azimuth in degrees clockwise from north (default 315).
        altitude:  Sun altitude above horizon in degrees (default 45).
        z_factor:  Vertical exaggeration (default 1.0).
        xscale, yscale: optional explicit scale overrides (both or neither).

    Returns:
        Single-band Byte (uint8) GTiff bytes; valid values [1..255],
        0 reserved for NoData (gdaldem convention).
    """
    if (xscale is None) != (yscale is None):
        raise ValueError(HILLSHADE_HALF_SCALE_REFUSAL)
    dzdx, dzdy, valid = _horn_gradients(ds)
    xs, ys = _resolve_scale(ds, xscale, yscale)
    dzdx = dzdx / xs
    dzdy = dzdy / ys

    z = float(z_factor)
    alt_rad = float(altitude) * np.pi / 180.0
    az_rad = float(azimuth) * np.pi / 180.0
    sin_alt = np.sin(alt_rad)
    cos_alt = np.cos(alt_rad)
    sin_az = np.sin(az_rad)
    cos_az = np.cos(az_rad)

    # GDAL gdaldem hillshade (Horn) rational form, in pyrx gradient terms.
    cang = (sin_alt + cos_alt * z * (dzdy * cos_az - dzdx * sin_az)) / np.sqrt(
        1.0 + z * z * (dzdx * dzdx + dzdy * dzdy)
    )
    # gdaldem scaling: valid pixels map to [1, 255]; cang<=0 floors to 1.
    # 0 is reserved exclusively for NoData.
    hs = np.where(cang <= 0.0, 1.0, 1.0 + 254.0 * cang)

    return emit(ds, hs, 0, propagate_invalid(valid), "uint8")
