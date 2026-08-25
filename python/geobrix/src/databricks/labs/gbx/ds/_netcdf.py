"""CF-convention NetCDF helpers for the netcdf_gbx reader (pure, no Spark).

Classifies a variable's geometry (regular grid / DSG points / curvilinear swath /
unsupported), derives an affine+CRS for grids, and flattens point/swath data to
1-D lon/lat/value arrays. No resampling, no quality filtering — the caller decides.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Tuple

_logger = logging.getLogger(__name__)

GRID = "grid"  # class 1/2 -> raster
POINTS = "points"  # CF discrete sampling geometries -> vector
CURVILINEAR = "curvilinear"  # class 3 (2-D lat/lon) -> vector (per-cell points)
UNSUPPORTED = "unsupported"  # class 4 (sensor geometry + GLT) / unknown

_LAT_NAMES = {"lat", "latitude", "y"}
_LON_NAMES = {"lon", "longitude", "x"}


@contextmanager
def open_dataset(path: str, group: Optional[str]) -> Iterator["object"]:
    """Open a NetCDF file as an xarray.Dataset (netcdf4 engine), optional HDF5 group."""
    import xarray as xr

    kw = {"engine": "netcdf4", "decode_coords": "all", "mask_and_scale": True}
    if group:
        kw["group"] = group
    ds = xr.open_dataset(path, **kw)
    try:
        yield ds
    finally:
        ds.close()


def _is_lat(var) -> bool:
    sn = str(getattr(var, "standard_name", "")).lower()
    un = str(getattr(var, "units", "")).lower()
    return (
        sn == "latitude"
        or un in ("degrees_north", "degree_north")
        or var.name.lower() in _LAT_NAMES
    )


def _is_lon(var) -> bool:
    sn = str(getattr(var, "standard_name", "")).lower()
    un = str(getattr(var, "units", "")).lower()
    return (
        sn == "longitude"
        or un in ("degrees_east", "degree_east")
        or var.name.lower() in _LON_NAMES
    )


def _find_lat_lon(ds):
    """Return (lat_var, lon_var) among ds coords/vars, or (None, None)."""
    lat = lon = None
    for name in list(ds.variables):
        v = ds[name]
        if lat is None and _is_lat(v):
            lat = v
        elif lon is None and _is_lon(v):
            lon = v
    return lat, lon


def _eff_ndim(var) -> int:
    """Dimensionality ignoring size-1 dims (e.g. S5P's leading length-1 `time`)."""
    return int(sum(1 for s in var.shape if s > 1))


def classify(ds, variable: str) -> str:
    lat, lon = _find_lat_lon(ds)
    if lat is None or lon is None:
        return UNSUPPORTED
    # Use the squeezed dimensionality so a (time=1, scanline, ground_pixel) swath
    # (Sentinel-5P) reads as effective 2-D curvilinear, not "unsupported".
    lat_nd, lon_nd = _eff_ndim(lat), _eff_ndim(lon)
    if lat_nd == 2 and lon_nd == 2:
        return CURVILINEAR
    if lat_nd <= 1 and lon_nd <= 1:
        var = ds[variable]
        # DSG points: the value var shares the single obs dimension with lat/lon.
        if _eff_ndim(var) <= 1 and lat.dims == lon.dims == var.dims:
            return POINTS
        # Regular grid: the value var's dims include the lat and lon dims.
        if (
            lat.dims
            and lon.dims
            and lat.dims[0] in var.dims
            and lon.dims[0] in var.dims
        ):
            return GRID
    return UNSUPPORTED


def grid_transform_crs(ds, variable: str) -> Tuple["object", str]:
    """Affine transform (north-up) + CRS string for a regular grid variable."""
    from affine import Affine

    lat, lon = _find_lat_lon(ds)
    lats = lat.values
    lons = lon.values
    px = float(abs(lons[1] - lons[0]))
    py = float(abs(lats[1] - lats[0]))
    ulx = float(min(lons)) - px / 2.0
    uly = float(max(lats)) + py / 2.0
    transform = Affine.translation(ulx, uly) * Affine.scale(px, -py)
    crs = _crs_string(ds)
    return transform, crs


def _crs_string(ds) -> str:
    """Return the CRS string for a CF grid dataset.

    Check order (first match wins):
    1. ``crs_canonical``: authority string ("EPSG:4326", "ESRI:54008") or WKT
       written by the current writer.  Handles non-EPSG CRS that to_epsg()
       would have silently dropped.
    2. ``crs_wkt``: CF standard WKT attribute written by the current writer and
       compliant producers.
    3. ``epsg_code`` / ``spatial_epsg``: legacy integer attribute; returns
       "EPSG:<int>".
    4. Default: "EPSG:4326" (geographic lon/lat).
    """
    for name in list(ds.variables):
        v = ds[name]
        # 1. crs_canonical (authority string or WKT — written by current writer)
        canonical = getattr(v, "crs_canonical", None)
        if canonical is not None:
            return str(canonical)
        # 2. crs_wkt (CF standard; rioxarray and compliant writers emit this)
        wkt = getattr(v, "crs_wkt", None)
        if wkt is not None:
            return str(wkt)
        # 3. Legacy EPSG integer attributes
        epsg = getattr(v, "epsg_code", None) or getattr(v, "spatial_epsg", None)
        if epsg is not None:
            code = _epsg_int(epsg)
            if code is not None:
                return f"EPSG:{code}"
    # Default: geographic lon/lat.
    return "EPSG:4326"


def _epsg_int(epsg) -> Optional[int]:
    """Coerce a CF grid_mapping EPSG attribute to an int code.

    The attribute is stored inconsistently across producers: a bare int
    (``4326``), a numeric string (``"4326"``), or an authority string
    (``"EPSG:4326"``, ``"epsg:4326"``). Extract the trailing digit run; return
    None if no code is present so the caller falls back to EPSG:4326.
    """
    import re

    m = re.search(r"(\d+)\s*$", str(epsg))
    return int(m.group(1)) if m else None


def array_2d(ds, variable: str) -> "object":
    """The variable's 2-D slice as a north-up numpy array (lat descending)."""
    import numpy as np

    da = _decoded_or_raw(ds, variable)
    # Squeeze any leading dims (e.g. time / level): take the first index until 2-D.
    # A leading dim of size > 1 means only its FIRST slice is read and the rest are
    # silently dropped — warn so the data loss is visible. Per-slice fan-out (one
    # tile per time/level, or a multi-band stack) is a planned feature.
    while da.ndim > 2:
        drop_dim = da.dims[0]
        drop_size = int(da.sizes[drop_dim])
        if drop_size > 1:
            _logger.warning(
                "netcdf_gbx: variable %r has leading dimension %r of size %d; "
                "reading only index 0 and dropping the other %d slice(s). "
                "Per-slice fan-out is not yet supported.",
                variable,
                drop_dim,
                drop_size,
                drop_size - 1,
            )
        da = da.isel({drop_dim: 0})
    lat, _ = _find_lat_lon(ds)
    latdim = lat.dims[0]
    # Ensure north-up: descending latitude along the lat dimension.
    latvals = ds[lat.name].values
    if latdim in da.dims and float(latvals[0]) < float(latvals[-1]):
        da = da.isel({latdim: slice(None, None, -1)})
    return np.asarray(da.values)


def _decoded_or_raw(ds, variable: str) -> "object":
    """Return the variable's DataArray, materializing its values.

    Normally the ``mask_and_scale``-decoded array. But some CF integer variables
    (e.g. NOAA coral ``bleaching_alert_area``: uint8 with ``valid_min/max`` and a
    fill of 251) trip xarray's masking decode with "cannot convert float NaN to
    integer". For those we fall back to the RAW (undecoded) values -- which is
    also what the heavy GDAL reader emits (fill kept as its integer sentinel, no
    NaN promotion), so it is the parity-correct behavior. The reopen is targeted
    (only on decode failure) using the variable's own source-file encoding.
    """
    import xarray as xr

    da = ds[variable]
    try:
        # Force the lazy decode now so a masking failure is caught here.
        da.values  # noqa: B018 - intentional materialization to trigger decode
        return da
    except ValueError:
        src = da.encoding.get("source") or ds.encoding.get("source")
        if not src:
            raise
        raw = xr.open_dataset(
            src, engine="netcdf4", decode_coords="all", mask_and_scale=False
        )
        try:
            return raw[variable].load()
        finally:
            raw.close()


def nodata_of(ds, variable: str) -> Optional[float]:
    v = ds[variable]
    for attr in ("_FillValue", "missing_value"):
        val = v.attrs.get(attr)
        if val is not None:
            return float(val)
    enc = v.encoding.get("_FillValue")
    return float(enc) if enc is not None else None


def point_arrays(
    ds, variables: List[str]
) -> Tuple["object", "object", Dict[str, "object"], str]:
    """Flatten point (DSG), grid, or curvilinear data to aligned 1-D arrays.

    - POINTS/CURVILINEAR: lat/lon already align with the value array, so ravel each.
    - GRID: 1-D lat (H) and 1-D lon (W) must be meshgridded to H*W before ravel so
      they align with the 2-D value array's ravel.
    """
    import numpy as np

    lat, lon = _find_lat_lon(ds)
    kind = classify(ds, variables[0])
    if kind == GRID:
        lon2d, lat2d = np.meshgrid(
            np.asarray(ds[lon.name].values), np.asarray(ds[lat.name].values)
        )  # indexing="xy" -> shape (H, W), matching the value array
        lon_flat = lon2d.ravel()
        lat_flat = lat2d.ravel()
    else:  # POINTS or CURVILINEAR
        lon_flat = np.asarray(ds[lon.name].values).ravel()
        lat_flat = np.asarray(ds[lat.name].values).ravel()
    attrs: Dict[str, object] = {}
    for name in variables:
        attrs[name] = np.asarray(ds[name].values).ravel()
    return lon_flat, lat_flat, attrs, "4326"


def readable_variables(ds, mode: str) -> List[str]:
    """Data variables readable in `mode` ('raster' -> GRID; 'vector' -> POINTS/CURVILINEAR).

    Iterates ds.data_vars only: with open_dataset's decode_coords="all", lat/lon,
    grid-mapping, and bounds coordinate variables are xarray coords, not data_vars,
    so they are never surfaced as readable fields.
    """
    keep = {GRID} if mode == "raster" else {POINTS, CURVILINEAR}
    return [name for name in list(ds.data_vars) if classify(ds, name) in keep]


def select_variables(ds, options: Dict[str, str], mode: str) -> List[str]:
    """Auto-enumerate all readable variables, narrowed by an optional variable filter."""
    readable = readable_variables(ds, mode)
    raw = options.get("variables") or options.get("variable")
    if not raw:
        return readable
    requested = [v.strip() for v in str(raw).split(",") if v.strip()]
    readable_set = set(readable)
    return [v for v in requested if v in readable_set]


def np_to_spark(dtype) -> "object":
    import numpy as np
    from pyspark.sql import types as T

    kind = np.dtype(dtype).kind
    itemsize = np.dtype(dtype).itemsize
    if kind == "f":
        return T.FloatType() if itemsize <= 4 else T.DoubleType()
    if kind in ("i", "u"):
        return T.IntegerType() if itemsize <= 4 else T.LongType()
    if kind == "b":
        return T.BooleanType()
    return T.StringType()
