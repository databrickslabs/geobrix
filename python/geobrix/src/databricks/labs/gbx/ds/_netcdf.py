"""CF-convention NetCDF helpers for the netcdf_gbx reader (pure, no Spark).

Classifies a variable's geometry (regular grid / DSG points / curvilinear swath /
unsupported), derives an affine+CRS for grids, and flattens point/swath data to
1-D lon/lat/value arrays. No resampling, no quality filtering — the caller decides.
"""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Tuple

_logger = logging.getLogger(__name__)

GRID = "grid"  # class 1/2 -> raster
POINTS = "points"  # CF discrete sampling geometries -> vector
CURVILINEAR = "curvilinear"  # class 3 (2-D lat/lon) -> vector (per-cell points)
UNSUPPORTED = "unsupported"  # class 4 (sensor geometry + GLT) / unknown

# Dataset.encoding key holding the HDF5 group a dataset was opened at.
_GROUP_KEY = "gbx_group"

_LAT_NAMES = {"lat", "latitude", "y"}
_LON_NAMES = {"lon", "longitude", "x"}
# What names a GEOGRAPHIC axis: a bare x / y is not enough to call a grid degrees.
_GEO_LAT_NAMES = {"lat", "latitude"}
_GEO_LON_NAMES = {"lon", "longitude"}
# The CF standard names and units of a geographic axis, and the standard names of a projected one.
_LAT_STANDARD, _LON_STANDARD = "latitude", "longitude"
_LAT_UNITS = ("degrees_north", "degree_north")
_LON_UNITS = ("degrees_east", "degree_east")
_PROJ_Y, _PROJ_X = "projection_y_coordinate", "projection_x_coordinate"
# Metres per unit of a projected axis (CF `units`); degrees are never scaled.
_FOOT_M = 0.3048
_US_SURVEY_FOOT_M = 1200.0 / 3937.0
_METRES_PER_UNIT = {
    "m": 1.0,
    "metre": 1.0,
    "metres": 1.0,
    "meter": 1.0,
    "meters": 1.0,
    "km": 1000.0,
    "kilometre": 1000.0,
    "kilometres": 1000.0,
    "kilometer": 1000.0,
    "kilometers": 1000.0,
    "ft": _FOOT_M,
    "foot": _FOOT_M,
    "feet": _FOOT_M,
    "international_foot": _FOOT_M,
    "us_survey_foot": _US_SURVEY_FOOT_M,
    "us_survey_feet": _US_SURVEY_FOOT_M,
}


def classic_truncation(path: str) -> Optional[str]:
    """Why a NetCDF classic file (``CDF`` version 1 or 2) is shorter than the
    data its header declares, or None (also for CDF-5 and NetCDF-4 / HDF5 files,
    which are not checked here: scipy reads neither). Classic files carry no
    checksum: read through netCDF4 the missing data comes back as zeros, so the
    size check is the only sign. scipy maps the file (no data is read) and fails
    to shape a variable that runs past its end."""
    import warnings

    with open(path, "rb") as fh:
        if fh.read(4) not in (b"CDF\x01", b"CDF\x02"):
            return None
    from scipy.io import netcdf_file

    try:
        f = netcdf_file(path, "r", mmap=True)
    except ValueError as e:
        return f"the file is shorter than the data its header declares: {e}"
    except Exception:  # noqa: BLE001 - not this check's error: the reader reports it
        return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # closing a mapped file with live arrays
        f.close()
    return None


@contextmanager
def open_dataset(path: str, group: Optional[str]) -> Iterator["object"]:
    """Open a NetCDF file as an xarray.Dataset (netcdf4 engine), optional HDF5 group."""
    import xarray as xr

    kw = {"engine": "netcdf4", "decode_coords": "all", "mask_and_scale": True}
    if group:
        kw["group"] = group
    ds = xr.open_dataset(path, **kw)
    # xarray records the source file but not the group: keep it, so the raw reopen
    # of a variable that cannot be decoded (see _decoded_or_raw) reads the same group.
    ds.encoding[_GROUP_KEY] = group or None
    try:
        yield ds
    finally:
        ds.close()


def _standard_name(var) -> str:
    return str(getattr(var, "standard_name", "")).lower()


def _units(var) -> str:
    return str(getattr(var, "units", "")).lower()


def _is_lat(var) -> bool:
    return (
        _standard_name(var) in (_LAT_STANDARD, _PROJ_Y)
        or _units(var) in _LAT_UNITS
        or var.name.lower() in _LAT_NAMES
    )


def _is_lon(var) -> bool:
    return (
        _standard_name(var) in (_LON_STANDARD, _PROJ_X)
        or _units(var) in _LON_UNITS
        or var.name.lower() in _LON_NAMES
    )


def _is_geo_lat(var) -> bool:
    """A geographic (degrees) latitude by its CF standard name, units or name (not a bare y)."""
    return (
        _standard_name(var) == _LAT_STANDARD
        or _units(var) in _LAT_UNITS
        or var.name.lower() in _GEO_LAT_NAMES
    )


def _is_geo_lon(var) -> bool:
    """A geographic (degrees) longitude by its CF standard name, units or name (not a bare x)."""
    return (
        _standard_name(var) == _LON_STANDARD
        or _units(var) in _LON_UNITS
        or var.name.lower() in _GEO_LON_NAMES
    )


def _is_cf_axis(var) -> bool:
    """A 1-D coordinate that names itself an axis (CF standard name or degree units), not just by its name."""
    return var.ndim == 1 and (
        _standard_name(var) in (_LAT_STANDARD, _LON_STANDARD, _PROJ_Y, _PROJ_X)
        or _units(var) in _LAT_UNITS + _LON_UNITS
    )


def _pick_axis(candidates):
    """The axis among ``candidates``: a 1-D CF-identified one before a 2-D auxiliary lat / lon (a projected file lists
    both, in any order), else the first."""
    for var in candidates:
        if _is_cf_axis(var):
            return var
    return candidates[0] if candidates else None


def _find_lat_lon(ds):
    """Return (lat_var, lon_var) among ds coords/vars, or (None, None). For a projected grid these are its y / x."""
    lats, lons = [], []
    for name in list(ds.variables):
        v = ds[name]
        if _is_lat(v):
            lats.append(v)
        elif _is_lon(v):
            lons.append(v)
    return _pick_axis(lats), _pick_axis(lons)


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


def _axis_scale(axis, crs_unit_m: float) -> float:
    """What one unit of a projected ``axis`` is in the units of its CRS (``crs_unit_m`` metres each): ``km`` on a metre
    CRS is 1000. An axis with no ``units`` is in the units of the CRS (1.0); a degree axis is left alone (1.0).
    Raises ValueError for a unit it does not know: a guessed scale is a wrong tile."""
    unit = _units(axis).strip()
    if not unit or unit.startswith("degree"):
        return 1.0
    if unit not in _METRES_PER_UNIT:
        raise ValueError(
            f"netcdf_gbx: the axis {axis.name!r} has units {unit!r}, which are none of m, km, ft or US_survey_foot. "
            "Fix: set the axis units to one of these (CF units), or drop them when the values are in the units of the CRS."
        )
    return _METRES_PER_UNIT[unit] / crs_unit_m


def _pixel_size(ds, axis) -> float:
    """The pixel size along a 1-D coordinate ``axis``: the step of its first two values. An axis of one value has no
    step: its CF ``bounds`` variable (the edges of its one cell) gives the size, else the grid is refused with a Fix.
    """
    vals = axis.values
    if len(vals) > 1:
        return float(abs(vals[1] - vals[0]))
    name = axis.attrs.get("bounds") or axis.encoding.get("bounds")
    if name and name in ds.variables:
        edges = ds[name].values.ravel()
        if edges.size >= 2 and edges[0] != edges[1]:
            return float(abs(edges[1] - edges[0]))
    raise ValueError(
        f"netcdf_gbx: the axis {axis.name!r} has one value, so its pixel size is not known: a step needs two values, "
        "and the axis has no CF bounds variable to give the edges of its cell. "
        f"Fix: add a bounds variable to {axis.name!r} (its `bounds` attribute names it), or write the grid with "
        "two or more values along that axis."
    )


def grid_transform_crs(ds, variable: str) -> Tuple["object", str]:
    """Affine transform (north-up) + CRS string for a regular grid variable.

    The CRS is resolved first, so a grid with no CRS is refused before its coordinates are read. The transform
    comes from the 1-D y / x coordinate values. For a projected CRS, wherever it came from, each axis is scaled from
    its CF ``units`` to the units of the CRS (``km`` on a metre CRS -> 1000); a geographic grid is left alone.
    """
    from affine import Affine

    from databricks.labs.gbx.core.crs import projected_unit_metres

    crs = _crs_string(ds, variable)
    lat, lon = _find_lat_lon(ds)
    lats = lat.values
    lons = lon.values
    px = _pixel_size(ds, lon)
    py = _pixel_size(ds, lat)
    ulx = float(min(lons)) - px / 2.0
    uly = float(max(lats)) + py / 2.0
    transform = Affine.translation(ulx, uly) * Affine.scale(px, -py)
    crs_unit_m = projected_unit_metres(crs)
    if crs_unit_m is not None:
        scale = Affine.scale(_axis_scale(lon, crs_unit_m), _axis_scale(lat, crs_unit_m))
        transform = scale * transform
    return transform, crs


def _cf_grid_mapping(ds, variable: Optional[str]) -> Optional[dict]:
    """Attributes of the CF grid-mapping variable of ``variable`` (any data variable when None): the variable named by
    its ``grid_mapping`` attribute that has a ``grid_mapping_name``. xarray moves ``grid_mapping`` to the encoding
    when it decodes coordinates, so both are read. The ``latitude_longitude`` mapping names no projection and is
    not used (a geographic grid keeps its default)."""
    names = [variable] if variable else list(ds.data_vars)
    for name in names:
        da = ds[name]
        raw = da.attrs.get("grid_mapping") or da.encoding.get("grid_mapping")
        if not raw:
            continue
        mapping = (
            str(raw).split(":")[0].split()[0] if ":" in str(raw) else str(raw).strip()
        )
        if mapping in ds.variables:
            attrs = dict(ds[mapping].attrs)
            if (
                attrs.get("grid_mapping_name")
                and attrs["grid_mapping_name"] != "latitude_longitude"
            ):
                return attrs
    return None


def _is_geographic_grid(ds) -> bool:
    """True for a grid whose axes are 1-D geographic latitude and longitude."""
    lat, lon = _find_lat_lon(ds)
    return (
        lat is not None
        and lon is not None
        and lat.ndim == 1
        and lon.ndim == 1
        and _is_geo_lat(lat)
        and _is_geo_lon(lon)
    )


def _crs_string(ds, variable: Optional[str] = None) -> str:
    """The CRS string for a CF grid dataset.

    Check order (first match wins):
    1. ``crs_canonical``: authority string ("EPSG:4326", "ESRI:54008") or WKT
       written by the current writer.  Handles non-EPSG CRS that to_epsg()
       would have silently dropped.
    2. ``crs_wkt``: CF standard WKT attribute written by the current writer and
       compliant producers.
    3. ``epsg_code`` / ``spatial_epsg``: legacy integer attribute; returns
       "EPSG:<int>".
    4. The CF ``grid_mapping`` variable of the data variable (``polar_stereographic``,
       ``lambert_conformal_conic``, ...), through ``core.crs.crs_from_cf``.
    5. Default "EPSG:4326", ONLY for a grid with 1-D geographic latitude / longitude. Any other grid has no
       CRS to give: refused with a Fix, never labelled 4326.
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
    # 4. The CF grid mapping
    attrs = _cf_grid_mapping(ds, variable)
    if attrs is not None:
        from databricks.labs.gbx.core.crs import crs_from_cf

        return crs_from_cf(attrs)
    # 5. Geographic lon/lat only
    if _is_geographic_grid(ds):
        return "EPSG:4326"
    raise ValueError(_no_crs_message(ds, variable))


def _no_crs_message(ds, variable: Optional[str]) -> str:
    """Why a grid has no CRS, and the fix that fits it: vector mode for 2-D lat / lon, a CF grid mapping otherwise."""
    what = f"variable {variable!r}" if variable else "the grid"
    head = (
        f"netcdf_gbx: {what} has no CRS: no crs_canonical, crs_wkt or epsg_code attribute and no CF grid_mapping "
        "variable, and its axes are not 1-D latitude / longitude, so EPSG:4326 would be a guess. "
    )
    lat, _ = _find_lat_lon(ds)
    if lat is not None and _eff_ndim(lat) == 2:
        return (
            head
            + "Fix: read it with mode=vector, which places one point per cell from its 2-D lat / lon."
        )
    fix = (
        "Fix: add a CF grid mapping variable with crs_wkt (or the CF projection attributes) and name it in the "
        "grid_mapping attribute of the variable."
    )
    if variable and _aux_lat_lon(ds, variable) is not None:
        fix += " Or read it with mode=vector, which places one point per cell from its 2-D lat / lon."
    return head + fix


def _epsg_int(epsg) -> Optional[int]:
    """Coerce a CF grid_mapping EPSG attribute to an int code.

    The attribute is stored inconsistently across producers: a bare int
    (``4326``), a numeric string (``"4326"``), or an authority string
    (``"EPSG:4326"``, ``"epsg:4326"``). Extract the trailing digit run; return
    None if no code is present so the caller goes on to the next source.
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


def grid_shape(ds, variable: str) -> Tuple[int, int]:
    """(height, width) of a regular grid variable: the sizes of its lat and lon dimensions."""
    lat, lon = _find_lat_lon(ds)
    var = ds[variable]
    return int(var.sizes[lat.dims[0]]), int(var.sizes[lon.dims[0]])


def array_2d(
    ds,
    variable: str,
    sel: Optional[Dict[str, int]] = None,
    window: Optional[Tuple[int, int, int, int]] = None,
) -> "object":
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
        window:   Optional north-up pixel window (col_off, row_off, width, height)
                  of the lat/lon grid; only that window is read and decoded.
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

    lat, lon = _find_lat_lon(ds)
    latdim, londim = lat.dims[0], lon.dims[0]
    # North-up: descending latitude along the lat dimension.
    latvals = ds[lat.name].values
    ascending = (
        latdim in da.dims
        and latdim not in index
        and float(latvals[0]) < float(latvals[-1])
    )
    if window is None:
        if ascending:
            index[latdim] = slice(None, None, -1)
    else:
        col_off, row_off, width, height = window
        if ascending:  # north-up row r is file row n - 1 - r
            n = int(da.sizes[latdim])
            stop = n - 1 - (row_off + height)
            index[latdim] = slice(n - 1 - row_off, stop if stop >= 0 else None, -1)
        else:
            index[latdim] = slice(row_off, row_off + height)
        index[londim] = slice(col_off, col_off + width)
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
    variable's own source-file encoding, at the group the dataset was opened at,
    and reads the same selection only.
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
        kw = {"engine": "netcdf4", "decode_coords": "all", "mask_and_scale": False}
        group = ds.encoding.get(_GROUP_KEY)
        if group:
            kw["group"] = group
        raw = xr.open_dataset(src, **kw)
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


def fill_kept(arr, nodata: Optional[float]):
    """*arr* with the cells that xarray decoded from the variable's fill value (NaN in a float variable) set back to
    that fill value, so the tile's pixels and its NoData agree and equal what GDAL reads from the file. A variable
    with no fill value, an integer one (no NaN) and a fill value that is itself NaN come back unchanged.
    """
    import numpy as np

    if nodata is None or arr.dtype.kind != "f" or np.isnan(nodata):
        return arr
    gaps = np.isnan(arr)
    if not gaps.any():
        return arr
    return np.where(gaps, arr.dtype.type(nodata), arr)


def point_arrays(
    ds, variables: List[str]
) -> Tuple["object", "object", Dict[str, "object"], str]:
    """Flatten point (DSG), grid, or curvilinear data to aligned 1-D arrays.

    - POINTS/CURVILINEAR: lat/lon already align with the value array, so ravel each.
    - GRID with 2-D aux lat / lon (CF ``coordinates``): those lat / lon, raveled.
    - GRID: 1-D lat (H) and 1-D lon (W) must be meshgridded to H*W before ravel so
      they align with the 2-D value array's ravel.
    """
    import numpy as np

    lat, lon = _find_lat_lon(ds)
    kind = classify(ds, variables[0])
    aux = _aux_lat_lon(ds, variables[0]) if kind == GRID else None
    if aux is not None:  # a projected grid placed by its 2-D lat / lon, cell by cell
        lat_flat = np.asarray(aux[0].values).ravel()
        lon_flat = np.asarray(aux[1].values).ravel()
    elif kind == GRID:
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
        if kind == CURVILINEAR:
            _warn_dropped_slices(ds, name, lat)
        attrs[name] = np.asarray(ds[name].values).ravel()
    return lon_flat, lat_flat, attrs, "4326"


def _warn_dropped_slices(ds, variable: str, lat) -> None:
    """Vector mode places one point per cell of the 2-D lat / lon and reads a variable's values in file order, the
    first one per point. A dimension beside lat / lon longer than one is not split. When every dimension before it
    has size 1, the points get its first slice (index 0): the result is unchanged and a warning says so (the Python
    logger of the reading process, as ``array_2d``'s). After another dimension (``y, time, x``) the values of its
    slices interleave and no slice lines up with the points: that is refused, never read.
    """
    da = ds[variable]
    for pos, dim in enumerate(da.dims):
        size = int(da.sizes[dim])
        if dim in lat.dims or size <= 1:
            continue
        if all(int(da.sizes[earlier]) == 1 for earlier in da.dims[:pos]):
            _logger.warning(
                "netcdf_gbx: variable %r has dimension %r of size %d; vector mode reads one point per cell, so it "
                "takes index 0 of %r and drops the other %d slice(s).",
                variable,
                dim,
                size,
                dim,
                size - 1,
            )
        else:
            raise ValueError(
                f"netcdf_gbx: variable {variable!r} has dimensions {tuple(da.dims)} with {dim!r} (size {size}) "
                "after another dimension longer than one, so its values interleave in file order and no slice "
                "lines up with the 2-D lat / lon points. Fix: select one slice of "
                f"{dim!r} before reading (for example `ncks -d {dim},0 in.nc out.nc`), or put it first "
                f"(`ncpdq -a {dim} in.nc out.nc`). Vector mode has no dimIndex option."
            )


# CF-1.8 discrete sampling geometry (DSG) feature types the vector mode reads, one point per observation.
DSG_TYPES = frozenset({"point", "profile", "trajectory"})


@dataclass(frozen=True)
class Dsg:
    """The layout of a CF-1.8 DSG file: where its points come from and which variables become columns.

    ``obs_dim`` is the dimension of the observations (one point each). ``lat``, ``lon`` and the optional altitude
    ``z`` (the point's Z) sit on it, or, in a ragged array representation, on ``instance_dim`` (the profile or
    trajectory dimension); ``lat`` and ``lon`` may stay on ``obs_dim`` there, as a CF trajectory has them. Indexed ragged: ``index`` is the variable on ``obs_dim`` whose ``instance_dimension``
    attribute names ``instance_dim`` and whose values say which instance each observation belongs to. Contiguous
    ragged: ``counts`` is the variable on ``instance_dim`` whose ``sample_dimension`` attribute names ``obs_dim``
    and whose values count the observations of each instance, which follow one another.
    ``attributes`` are the variables that become columns, the id variable (``cf_role``) first, then by name.
    """

    obs_dim: str
    lat: str
    lon: str
    z: Optional[str]
    index: Optional[str]
    instance_dim: Optional[str]
    attributes: Tuple[str, ...]
    counts: Optional[str] = None


# CF vertical coordinates: by axis, by standard name (altitude is a height, depth its downward counterpart), or by
# the ``positive`` attribute CF requires on every vertical coordinate.
_Z_STANDARD_NAMES = frozenset({"altitude", "depth"})


def _is_z(var) -> bool:
    """True for a CF vertical coordinate. A depth (``positive="down"`` or ``standard_name="depth"``) is one, and its
    values are the point's Z as stored, positive down: the GDAL netCDF driver (``netcdf_ogr``) gives the same Z and
    does not flip the sign, so the two readers agree."""
    return (
        str(var.attrs.get("axis", "")).upper() == "Z"
        or str(var.attrs.get("standard_name", "")).lower() in _Z_STANDARD_NAMES
        or str(var.attrs.get("positive", "")).lower() in ("up", "down")
    )


def dsg_layout(ds) -> Optional[Dsg]:
    """The DSG layout of ``ds`` (a ``featureType`` of point, profile or trajectory), else None.

    A file that carries no ``featureType`` (a swath, a grid, a plain table of points) has none: the other vector
    paths read it. A DSG file the layout cannot place (no lat/lon, or a lat/lon on neither the observation nor the
    instance dimension) has none either.
    """
    if str(ds.attrs.get("featureType", "")).strip().lower() not in DSG_TYPES:
        return None
    lat, lon = _find_lat_lon(ds)
    if lat is None or lon is None or lat.dims != lon.dims or len(lat.dims) != 1:
        return None
    index = counts = instance_dim = None
    for name in ds.variables:
        v = ds[name]
        inst, sample = v.attrs.get("instance_dimension"), v.attrs.get(
            "sample_dimension"
        )
        if inst is not None and len(v.dims) == 1 and str(inst) in ds.sizes:
            index, instance_dim = str(name), str(inst)
            break
        if sample is not None and len(v.dims) == 1 and str(sample) in ds.sizes:
            counts, instance_dim = str(name), v.dims[0]
            obs_dim = str(sample)
            break
    if index is not None:
        obs_dim = ds[index].dims[0]
    elif counts is None:
        obs_dim = lat.dims[0]
    # lat/lon sit on the instance dimension (one position per profile or trajectory) or, as a CF trajectory has them and
    # a profile may, on the observation dimension (a position per observation): either way the instance variables
    # are expanded per observation, and the id, the time and the Z are columns.
    if instance_dim is not None and lat.dims not in ((instance_dim,), (obs_dim,)):
        return None
    z = next(
        (
            str(n)
            for n in ds.variables
            if ds[n].dims in ((obs_dim,), (instance_dim,)) and _is_z(ds[n])
        ),
        None,
    )
    skip = {str(lat.name), str(lon.name), z, index, counts, obs_dim}
    wanted = {obs_dim} | ({instance_dim} if instance_dim else set())
    names = [
        str(n)
        for n in ds.variables
        if str(n) not in skip
        and len(ds[n].dims) == 1
        and ds[n].dims[0] in wanted
        and (ds[n].dims[0] != obs_dim or str(n) != obs_dim)
    ]
    names.sort(key=lambda n: (not ds[n].attrs.get("cf_role"), n))
    return Dsg(
        obs_dim,
        str(lat.name),
        str(lon.name),
        z,
        index,
        instance_dim,
        tuple(names),
        counts,
    )


def dsg_arrays(
    ds, layout: Dsg, variables: List[str]
) -> Tuple["object", "object", Optional["object"], Dict[str, "object"], str]:
    """Per-observation arrays of a DSG file: lon, lat, z (or None) and the ``variables``, an instance-dimension
    variable expanded to its observations through the index variable."""
    import numpy as np

    ix = None  # the instance of each observation
    n_obs = ds.sizes[layout.obs_dim]
    n_inst = ds.sizes[layout.instance_dim] if layout.instance_dim else 0
    if layout.index is not None:
        raw = np.asarray(ds[layout.index].values)
        # a missing index decodes to NaN (a fill value masked), -1 is the usual stand-in: an observation with no
        # instance. It has no profile or trajectory to take its id, time or position from, so the file is refused
        # rather than the observation dropped or, worse, wrapped onto the last instance (index -1).
        bad = ~np.isfinite(raw) if raw.dtype.kind == "f" else np.zeros(raw.shape, bool)
        ix = np.where(bad, -1, raw).astype("int64")
        orphan = bad | (ix < 0) | (ix >= n_inst)
        if orphan.any():
            first = int(np.flatnonzero(orphan)[0])
            raise ValueError(
                f"netcdf_gbx: {layout.index!r} gives observation {first} (of {n_obs}) no instance: its value is "
                f"{raw[first]!r}, not an index in [0, {n_inst}) of {layout.instance_dim!r}; "
                f"{int(orphan.sum())} observation(s) in all. Fix: give every observation a valid instance index "
                f"(CF allows no missing index), or drop the observation(s) from the file."
            )
    elif layout.counts is not None:
        raw = np.asarray(ds[layout.counts].values)
        if raw.dtype.kind == "f" and not np.isfinite(raw).all():
            raise ValueError(
                f"netcdf_gbx: {layout.counts!r} holds a missing count. Fix: give every {layout.instance_dim!r} "
                f"its number of observations (0 for none)."
            )
        sizes = raw.astype("int64")
        if (sizes < 0).any() or int(sizes.sum()) != n_obs:
            raise ValueError(
                f"netcdf_gbx: the counts in {layout.counts!r} add up to {int(sizes.sum())}, but "
                f"{layout.obs_dim!r} holds {n_obs} observations"
                + (", and a count is negative" if (sizes < 0).any() else "")
                + f". Fix: make the counts sum to the size of {layout.obs_dim!r}."
            )
        ix = np.repeat(np.arange(len(sizes)), sizes)

    def per_obs(name: str):
        values = np.asarray(ds[name].values)
        return (
            values[ix]
            if ix is not None and ds[name].dims[0] != layout.obs_dim
            else values
        )

    lon, lat = per_obs(layout.lon), per_obs(layout.lat)
    z = per_obs(layout.z) if layout.z is not None else None
    return lon, lat, z, {name: per_obs(name) for name in variables}, "4326"


def _aux_lat_lon(ds, variable: str):
    """(lat, lon) of a grid variable's 2-D auxiliary geographic coordinates (CF ``coordinates``), else None.

    They place a projected grid cell by cell in vector mode. Both must lie on the variable's last two dimensions, in
    its order, and the variable must have no other dimension with more than one slice, so its values align with them.
    xarray moves ``coordinates`` to the encoding when it decodes coordinates, so both are read.
    """
    da = ds[variable]
    raw = da.attrs.get("coordinates") or da.encoding.get("coordinates") or ""
    names = list(dict.fromkeys(str(raw).split() + [str(c) for c in da.coords]))
    if da.ndim < 2 or _eff_ndim(da) > 2:
        return None
    lat = lon = None
    for name in names:
        if name not in ds.variables:
            continue
        v = ds[name]
        if v.ndim != 2 or tuple(v.dims) != tuple(da.dims[-2:]):
            continue
        if lat is None and _is_geo_lat(v):
            lat = v
        elif lon is None and _is_geo_lon(v):
            lon = v
    return (lat, lon) if lat is not None and lon is not None else None


def _vector_readable(ds, name: str) -> bool:
    kind = classify(ds, name)
    return kind in (POINTS, CURVILINEAR) or (
        kind == GRID and _aux_lat_lon(ds, name) is not None
    )


def readable_variables(ds, mode: str) -> List[str]:
    """Data variables readable in `mode` ('raster' -> GRID; 'vector' -> POINTS/CURVILINEAR, or a GRID with 2-D
    aux lat / lon).

    Iterates ds.data_vars only: with open_dataset's decode_coords="all", lat/lon,
    grid-mapping, and bounds coordinate variables are xarray coords, not data_vars,
    so they are never surfaced as readable fields. A DSG file (``featureType`` point,
    profile or trajectory) is the exception in vector mode: its columns are the variables
    of :func:`dsg_layout` (the id and the time are coordinates there, and are columns).
    """
    if mode != "raster":
        layout = dsg_layout(ds)
        if layout is not None:
            return list(layout.attributes)
        return [name for name in list(ds.data_vars) if _vector_readable(ds, name)]
    return [name for name in list(ds.data_vars) if classify(ds, name) == GRID]


def _refuse_projected_grids(ds, names: List[str], readable: List[str]) -> None:
    """Vector mode: a projected grid with no 2-D lat / lon has no positions to give its points. Refuse it, never an
    empty frame."""
    if dsg_layout(ds) is not None or _is_geographic_grid(ds):
        return
    for name in names:
        if name in ds.data_vars and name not in readable and classify(ds, name) == GRID:
            raise ValueError(
                f"netcdf_gbx: variable {name!r} sits on projected x / y axes with no 2-D lat / lon to place its "
                "points in vector mode; this is a projected grid: read it in raster mode (the default mode)."
            )


def select_variables(ds, options: Dict[str, str], mode: str) -> List[str]:
    """Auto-enumerate all readable variables, narrowed by an optional variable filter. Vector mode refuses a
    projected grid it cannot place (see :func:`_refuse_projected_grids`)."""
    readable = readable_variables(ds, mode)
    raw = options.get("variables") or options.get("variable")
    requested = [v.strip() for v in str(raw).split(",") if v.strip()] if raw else None
    if mode != "raster":
        _refuse_projected_grids(
            ds, requested if requested is not None else list(ds.data_vars), readable
        )
    if requested is None:
        return readable
    readable_set = set(readable)
    return [v for v in requested if v in readable_set]


# CF time units that count days from a base: with whole days and a midnight base the values are
# calendar dates, not instants.
_DAYS_SINCE = "days since"
# the clock of a CF base ("2000-01-01 12:00:00", "2000-01-01T00:00:00Z", "... 00:00:00 +05:00")
_BASE_CLOCK = re.compile(
    r"^\d{1,4}-\d{1,2}-\d{1,2}(?:[ T](\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2})(?:\.\d+)?)?"
    r"\s*(Z|UTC|GMT|[+-]\d{1,2}(?::?\d{2})?)?)?$",
    re.IGNORECASE,
)


def _is_date_axis(var) -> bool:
    """True when a decoded time variable counts whole days from a midnight base, read from its CF
    attributes and its stored type (no data read): ``units`` ``days since <date>`` with no time of
    day or zone offset in the base, stored as an integer with no ``scale_factor`` / ``add_offset``.
    Anything else (a float count, a scaled one, a base with a clock time) can carry a time of day,
    which a date would drop."""
    import numpy as np

    enc = var.encoding
    units = str(enc.get("units", "")).strip().lower()
    if not units.startswith(_DAYS_SINCE):
        return False
    stored = enc.get("dtype")
    if stored is None or np.dtype(stored).kind not in "iu":
        return False
    if enc.get("scale_factor") is not None or enc.get("add_offset") is not None:
        return False
    base = _BASE_CLOCK.match(units[len(_DAYS_SINCE) :].strip())
    if base is None:
        return False
    hour, minute, second, zone = base.groups()
    if any(int(x or 0) for x in (hour, minute, second)):
        return False
    return zone is None or not any(int(x) for x in re.findall(r"\d+", zone))


def column_kind(var) -> Optional[str]:
    """What a decoded variable is beyond its numpy type, read from its CF attributes (no data
    read): ``"timestamp"`` or ``"date"`` for a time variable (``"date"`` only for a whole-day
    count from a midnight base, see :func:`_is_date_axis`), ``"boolean"`` for a variable that
    flags 0 and 1 as ``false true`` (``flag_values`` and ``flag_meanings``, which is how
    ``netcdf_gbx`` writes a BOOLEAN), else ``None``. xarray decodes a time to ``datetime64``
    and, with its fill value masked, a flag to a float; the numpy type alone says neither.
    """
    import numpy as np

    if np.dtype(var.dtype).kind == "M":
        return "date" if _is_date_axis(var) else "timestamp"
    meanings = str(var.attrs.get("flag_meanings", "")).split()
    values = np.atleast_1d(var.attrs.get("flag_values", []))
    if meanings == ["false", "true"]:
        try:
            if [int(v) for v in values] == [0, 1]:
                return "boolean"
        except (ValueError, TypeError, OverflowError):
            pass  # flag_values that is text or holds a NaN or infinity is malformed: not a flag
    return None


def spark_type_of(var) -> "object":
    """Spark type of a decoded variable: a timestamp, date or boolean by :func:`column_kind`,
    else the one of its numpy type (:func:`np_to_spark`)."""
    from pyspark.sql import types as T

    kind = column_kind(var)
    if kind is not None:
        return {
            "timestamp": T.TimestampType,
            "date": T.DateType,
            "boolean": T.BooleanType,
        }[kind]()
    return np_to_spark(var.dtype)


def cell_value(kind: Optional[str], v):
    """The Python value of one element of a decoded variable of ``kind``: a UTC ``datetime``,
    a ``date`` or a ``bool``, ``None`` for NaT / NaN; otherwise numpy's own ``item()``.
    """
    import datetime

    import numpy as np

    if kind in ("timestamp", "date"):
        if np.isnat(v):
            return None
        if kind == "date":
            return v.astype("datetime64[D]").item()
        # xarray decodes float seconds to nanoseconds, a hair under a microsecond boundary
        # (…15.249999999 for …15.25): round to the microsecond, do not cut.
        us = (int(v.astype("datetime64[ns]").astype("int64")) + 500) // 1000
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        return epoch + datetime.timedelta(microseconds=us)
    if kind == "boolean":
        return None if v != v else bool(v)
    return v.item()


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
