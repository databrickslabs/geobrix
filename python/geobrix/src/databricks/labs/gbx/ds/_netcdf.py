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


def leading_dims(ds, variable: str) -> List[Tuple[str, int]]:
    """Non-spatial leading dimensions of a variable and their sizes.

    Returns each (dim_name, size) in the order the dims appear in the variable,
    excluding the lat and lon spatial dimensions.  A 2-D (lat, lon) variable
    returns an empty list.
    """
    lat, lon = _find_lat_lon(ds)
    spatial_dims: set = set()
    if lat is not None and lat.dims:
        spatial_dims.add(lat.dims[0])
    if lon is not None and lon.dims:
        spatial_dims.add(lon.dims[0])
    var = ds[variable]
    return [(dim, int(var.sizes[dim])) for dim in var.dims if dim not in spatial_dims]


def array_2d(ds, variable: str, sel: Optional[Dict[str, int]] = None) -> "object":
    """The variable's 2-D slice as a north-up numpy array (lat descending).

    Only that slice is read and decoded (``isel`` before ``.values``): a fanout
    or bandDim read decodes one slice per call, never the whole variable, so
    memory stays at one slice and a corrupt chunk fails only the slices in it.

    Args:
        ds:       Open xarray Dataset.
        variable: Variable name.
        sel:      Optional mapping of leading-dim name → integer index.  When a
                  leading dim appears in *sel*, that index is used and no warning
                  is emitted.  Dims absent from *sel* fall back to index 0 (with
                  the existing size>1 warning).  Out-of-range indices raise
                  ValueError.  Pass None (or omit) for the original single-slice
                  behavior.
    """
    import numpy as np

    if sel is None:
        sel = {}

    da = ds[variable]  # lazy: nothing is read or decoded here

    # Early validation: check sel indices are in range for dims present in this var.
    for s_dim, s_idx in sel.items():
        if s_dim in da.dims:
            s_size = int(da.sizes[s_dim])
            if s_idx < 0 or s_idx >= s_size:
                raise ValueError(
                    f"netcdf_gbx: dimIndex {s_dim}={s_idx} is out of range "
                    f"(size={s_size}) for variable {variable!r}."
                )

    # Index the leading dims down to 2-D: the index in *sel* (no warning), else
    # index 0, with a warning when the dim has more than one slice.
    index: Dict[str, object] = {}
    for drop_dim in da.dims[: max(da.ndim - 2, 0)]:
        drop_size = int(da.sizes[drop_dim])
        if drop_dim in sel:
            index[drop_dim] = sel[drop_dim]
            continue
        if drop_size > 1:
            _logger.warning(
                "netcdf_gbx: variable %r has leading dimension %r of size %d; "
                "reading only index 0 and dropping the other %d slice(s). "
                "Use the 'fanout' or 'dimIndex' option to read other slices.",
                variable,
                drop_dim,
                drop_size,
                drop_size - 1,
            )
        index[drop_dim] = 0

    lat, _ = _find_lat_lon(ds)
    latdim = lat.dims[0]
    # Ensure north-up: descending latitude along the lat dimension.
    latvals = ds[lat.name].values
    if (
        latdim in da.dims
        and latdim not in index
        and float(latvals[0]) < float(latvals[-1])
    ):
        index[latdim] = slice(None, None, -1)
    return np.asarray(_decoded_or_raw(ds, variable, index))


def _decoded_or_raw(ds, variable: str, index: Optional[Dict[str, object]] = None):
    """The variable's values at ``index`` (``isel`` indexers; None = all of it),
    reading and decoding only that selection.

    Normally the ``mask_and_scale``-decoded values. But some CF integer variables
    (e.g. NOAA coral ``bleaching_alert_area``: uint8 with ``valid_min/max``, an
    integer ``scale_factor`` and a fill of 251) trip xarray's masking decode with
    "cannot convert float NaN to integer" -- for any selection, since the failure
    comes from the variable's attributes. For those we fall back to the RAW
    (undecoded) values -- which is also what the heavy GDAL reader emits (fill
    kept as its integer sentinel, no NaN promotion), so it is the parity-correct
    behavior. The reopen is targeted (only on decode failure) using the
    variable's own source-file encoding, and reads the same selection only.
    """
    import xarray as xr

    index = index or {}
    da = ds[variable]
    try:
        return da.isel(index).values
    except ValueError:
        src = da.encoding.get("source") or ds.encoding.get("source")
        if not src:
            raise
        raw = xr.open_dataset(
            src, engine="netcdf4", decode_coords="all", mask_and_scale=False
        )
        try:
            return raw[variable].isel(index).values
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
