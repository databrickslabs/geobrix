"""UC Python handlers for the raster tile functions: a materialized tile in, the session function's answer out.

Import-light, as :mod:`databricks.labs.gbx.uc.handlers`: no pyspark, and a kernel module is imported when its
handler first runs, so a function needs only the dependencies its manifest block declares. Each handler is a thin
function over the pure core of the light kernel (``pyrx/core``), built from a table:

- NULL in is NULL out; an error tile (no raster, no path, ``last_error`` in its metadata) passes through a
  tile-returning function and is NULL for any other.
- A virtual tile (a path, no raster bytes) is refused with a ``Fix:`` that names the session function: virtual
  tiles are session-only.
- A tile decoded over :data:`DECODED_CAP` is refused with a ``Fix:``, never truncated. The cap is the Serverless
  worker's, not the transport's: a tile of 256 MiB reaches a function, but no decoding kernel has been measured above
  this size on a Unity Catalog sandbox.
- Data a function cannot read (corrupt bytes, a pixel outside the grid) is NULL: a UC function has no error tile.
  A parameter error raises with a ``Fix:`` line. Resource exhaustion and a missing dependency are never data.
- A function that returns a tile builds it with :func:`tile_row`: a result decoded over the cap is refused with a
  ``Fix:`` too (a resample or a cast can grow a tile), and an error tile goes through unchanged.
"""

from __future__ import annotations

import importlib
from typing import Callable, Dict, Optional

from databricks.labs.gbx.pyrx.core.budget import (
    UC_DECODED_CAP,
    decoded_bytes,
    grid_bytes,
    size_text,
)
from databricks.labs.gbx.uc.handlers import _field, _geom_bytes, _Refusal, _refuse

# The decoded size above which a tile is refused: one value for every handler (see the module doc).
DECODED_CAP = UC_DECODED_CAP
ERROR_KEY = "last_error"
_CORES = "databricks.labs.gbx.pyrx.core"

HANDLERS: Dict[str, Callable] = {}


class _Parameter(ValueError):
    """A wrong argument: it raises (it is not data), whatever the tile holds."""


def cap_refusal(fn: str, decoded: int, cap: int) -> _Refusal:
    """A tile decoded over the cap: the text of the session's read-cap refusal, and a Fix that names the function."""
    return _Refusal(
        f"{fn}: the tile is {size_text(decoded)} decoded, over the {size_text(cap)} a Unity Catalog function "
        f"decodes. Fix: run {fn} in a notebook or job session, or retile first (gbx_rst_retile) so each tile is smaller"
    )


def _is_error_tile(tile) -> bool:
    return ERROR_KEY in (_field(tile, "metadata") or {})


def raster_of(fn: str, tile) -> Optional[bytes]:
    """The raster bytes of a materialized tile. None for a tile with nothing to read (NULL, an error tile, an empty
    one); a virtual tile is refused."""
    if tile is None:
        return None
    raster = _field(tile, "raster")
    if raster is not None:
        return bytes(raster)
    if _field(tile, "path") is not None:
        raise _refuse(fn, "virtual tiles (a path, no raster bytes) are session-only")
    return None


def _kernel(module: str, attr: str) -> Callable:
    return getattr(importlib.import_module(f"{_CORES}.{module}"), attr)


def with_dataset(fn: str, tile, kernel: Callable, on_empty=None):
    """Open a materialized tile's raster, refuse it over the cap, and return ``kernel(ds)``.

    Unreadable bytes, and a kernel that cannot read the pixels, are NULL. A refusal, a wrong argument, resource
    exhaustion and a missing dependency raise. ``on_empty`` is the answer for an error tile (default NULL).
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes

    raster = raster_of(fn, tile)
    if raster is None:
        return on_empty if tile is not None and _is_error_tile(tile) else None
    try:
        with MemoryFile(raster) as mf, open_bytes(mf) as ds:
            decoded = decoded_bytes(ds)
            if decoded > DECODED_CAP:
                raise cap_refusal(fn, decoded, DECODED_CAP)
            return kernel(ds)
    except (MemoryError, RecursionError, ImportError, _Refusal, _Parameter):
        raise
    except Exception:  # noqa: BLE001 - unreadable data is NULL on UC (no error tile)
        return None


def _scalar(
    sql_name: str, module: str, attr: str, nargs: int = 0, on_empty=None
) -> Callable:
    """A handler ``(tile, *args)`` that answers ``<module>.<attr>(ds, *args)``; a NULL argument is NULL."""
    fn = sql_name

    def handler(tile, *args):
        if tile is None or any(a is None for a in args):
            return None
        return with_dataset(
            fn, tile, lambda ds: _kernel(module, attr)(ds, *args), on_empty
        )

    handler.__name__ = sql_name[len("gbx_") :]
    handler.__doc__ = f"{sql_name} over a materialized tile (core.{module}.{attr})."
    HANDLERS[sql_name] = handler
    return handler


# --- header accessors: size, CRS, geotransform, metadata, pixel <-> world ---------------------------------------
rst_width = _scalar("gbx_rst_width", "accessors", "width")
rst_height = _scalar("gbx_rst_height", "accessors", "height")
rst_numbands = _scalar("gbx_rst_numbands", "accessors", "numbands")
rst_srid = _scalar("gbx_rst_srid", "accessors", "srid")
rst_crs = _scalar("gbx_rst_crs", "accessors", "crs")
rst_pixelwidth = _scalar("gbx_rst_pixelwidth", "accessors", "pixelwidth")
rst_pixelheight = _scalar("gbx_rst_pixelheight", "accessors", "pixelheight")
rst_upperleftx = _scalar("gbx_rst_upperleftx", "accessors", "upperleftx")
rst_upperlefty = _scalar("gbx_rst_upperlefty", "accessors", "upperlefty")
rst_scalex = _scalar("gbx_rst_scalex", "accessors", "scalex")
rst_scaley = _scalar("gbx_rst_scaley", "accessors", "scaley")
rst_skewx = _scalar("gbx_rst_skewx", "accessors", "skewx")
rst_skewy = _scalar("gbx_rst_skewy", "accessors", "skewy")
rst_rotation = _scalar("gbx_rst_rotation", "accessors", "rotation")
rst_boundingbox = _scalar("gbx_rst_boundingbox", "accessors", "boundingbox")
rst_format = _scalar("gbx_rst_format", "accessors", "format")
rst_type = _scalar("gbx_rst_type", "accessors", "type")
rst_getnodata = _scalar("gbx_rst_getnodata", "accessors", "getnodata")
rst_georeference = _scalar("gbx_rst_georeference", "accessors", "georeference")
rst_metadata = _scalar("gbx_rst_metadata", "accessors", "metadata")
rst_subdatasets = _scalar("gbx_rst_subdatasets", "accessors", "subdatasets")
rst_rastertoworldcoord = _scalar(
    "gbx_rst_rastertoworldcoord", "coords", "raster_to_world_coord"
)
rst_rastertoworldcoordx = _scalar(
    "gbx_rst_rastertoworldcoordx", "coords", "raster_to_world_x"
)
rst_rastertoworldcoordy = _scalar(
    "gbx_rst_rastertoworldcoordy", "coords", "raster_to_world_y"
)
rst_worldtorastercoord = _scalar(
    "gbx_rst_worldtorastercoord", "coords", "world_to_raster_coord"
)
rst_worldtorastercoordx = _scalar(
    "gbx_rst_worldtorastercoordx", "coords", "world_to_raster_x"
)
rst_worldtorastercoordy = _scalar(
    "gbx_rst_worldtorastercoordy", "coords", "world_to_raster_y"
)


def rst_bandmetadata(tile, band):
    """gbx_rst_bandmetadata: the tags of a 1-based band; a band outside the tile raises (a wrong argument)."""
    fn = "gbx_rst_bandmetadata"
    if tile is None or band is None:
        return None

    def kernel(ds):
        if not 1 <= int(band) <= ds.count:
            raise _Parameter(
                f"{fn}: band {band} is outside the tile's {ds.count} band(s). "
                f"Fix: pass a band from 1 to {ds.count} (gbx_rst_numbands gives the count)"
            )
        return _kernel("accessors", "bandmetadata")(ds, int(band))

    return with_dataset(fn, tile, kernel)


HANDLERS["gbx_rst_bandmetadata"] = rst_bandmetadata


# --- pixel statistics ----------------------------------------------------------------------------------------
rst_avg = _scalar("gbx_rst_avg", "accessors", "avg")
rst_min = _scalar("gbx_rst_min", "accessors", "minimum")
rst_max = _scalar("gbx_rst_max", "accessors", "maximum")
rst_median = _scalar("gbx_rst_median", "accessors", "median")
rst_pixelcount = _scalar("gbx_rst_pixelcount", "accessors", "pixelcount")
rst_summary = _scalar("gbx_rst_summary", "accessors", "summary")
# an error tile has no valid pixel: it is empty
rst_isempty = _scalar("gbx_rst_isempty", "accessors", "isempty", on_empty=True)


def rst_histogram(tile, n_buckets, min_val, max_val, include_nodata):
    """gbx_rst_histogram: per-band bucket counts; a NULL n_buckets is 256, a NULL range is the band's own."""
    fn = "gbx_rst_histogram"
    if n_buckets is not None and int(n_buckets) < 1:
        raise _Parameter(
            f"{fn}: n_buckets must be >= 1; got {n_buckets}. Fix: pass a bucket count of at least 1"
        )
    if tile is None:
        return None
    return with_dataset(
        fn,
        tile,
        lambda ds: _kernel("accessors", "histogram")(
            ds,
            256 if n_buckets is None else int(n_buckets),
            None if min_val is None else float(min_val),
            None if max_val is None else float(max_val),
            bool(include_nodata) if include_nodata is not None else False,
        ),
    )


HANDLERS["gbx_rst_histogram"] = rst_histogram


def rst_sample(tile, geom, crs):
    """gbx_rst_sample: the value of each band at a POINT (a GEOMETRY, in its own SRID; `crs` names the SRID of a
    point that has none); a point outside the tile is NULL."""
    import shapely

    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.uc.handlers import _geom_bytes

    fn = "gbx_rst_sample"
    if tile is None or geom is None:
        return None
    source = None if crs is None else parse_uc_crs(crs, fn=fn).code
    point = shapely.from_wkb(_geom_bytes(geom))
    if point.geom_type != "Point":
        raise _Parameter(
            f"{fn}: needs a POINT geometry; got {point.geom_type.upper()}. "
            "Fix: pass the point to sample, for example st_point(x, y)"
        )
    return with_dataset(
        fn, tile, lambda ds: _kernel("ops", "sample")(ds, point, geom_crs=source)
    )


HANDLERS["gbx_rst_sample"] = rst_sample


def rst_tryopen(tile):
    """gbx_rst_tryopen: whether the tile's bytes open as a raster. Bytes that do not open, and an error tile, are
    false (the answer, not a missing value); a NULL tile is NULL."""
    fn = "gbx_rst_tryopen"
    if tile is None:
        return None
    # opening is the test (as ops.try_open): bytes that do not open come back from with_dataset as None
    return bool(with_dataset(fn, tile, lambda ds: ds.count is not None, on_empty=False))


HANDLERS["gbx_rst_tryopen"] = rst_tryopen


# --- tile-returning functions -------------------------------------------------------------------------------------
def result_refusal(fn: str, decoded: int, cap: int) -> _Refusal:
    """A result decoded over the cap (an operation can grow a tile): never truncated, refused with a Fix."""
    return _Refusal(
        f"{fn}: the result would be {size_text(decoded)} decoded, over the {size_text(cap)} a Unity Catalog function "
        f"returns. Fix: run {fn} in a notebook or job session, or retile first (gbx_rst_retile) so each tile is smaller"
    )


def _tile_dict(tile, raster, srid, metadata) -> dict:
    """The dict a UC function returns for a tile (the shape of gbx_rst_transformcrs): the cell id of the input tile
    and the raster, with every provenance field empty."""
    return {
        "cellid": int(_field(tile, "cellid") or 0),
        "raster": raster,
        "path": None,
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None if srid is None else str(srid),
        "metadata": metadata,
    }


def error_row(tile) -> dict:
    """An error tile as a dict, with its reason: it goes through a tile-returning function unchanged."""
    return {
        **_tile_dict(
            tile, None, _field(tile, "crs"), dict(_field(tile, "metadata") or {})
        ),
        "path_mode": _field(tile, "path_mode"),
    }


def gate(fn: str, *tiles):
    """``(done, value)`` for the tile arguments of a tile-returning function, once none is NULL: a virtual tile is
    refused, an error tile goes out as itself (the first of them), a tile with nothing else in it is NULL. ``done`` is
    False when every tile has a raster to read."""
    rasters = [raster_of(fn, t) for t in tiles]
    if all(r is not None for r in rasters):
        return False, None
    errors = [
        error_row(t) for t, r in zip(tiles, rasters) if r is None and _is_error_tile(t)
    ]
    return True, errors[0] if errors else None


def tile_row(
    fn: str, tile, raster: Optional[bytes], driver: str = "GTiff"
) -> Optional[dict]:
    """The tile a function returns for the raster bytes it made: the metadata the session tile carries (driver, size,
    band count) and the integer SRID of the raster's CRS. A raster decoded over the cap is refused with a Fix.
    """
    if raster is None:
        return None
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.core.crs import authority_srid_of

    raster = bytes(raster)
    with MemoryFile(raster) as mf, open_bytes(mf) as ds:
        decoded = decoded_bytes(ds)
        if decoded > DECODED_CAP:
            raise result_refusal(fn, decoded, DECODED_CAP)
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
        return _tile_dict(tile, raster, authority_srid_of(ds.crs), meta)


def check_grows(fn: str, width: int, height: int, dtypes) -> None:
    """Refuse, before any pixel is made, a result grid decoded over the cap."""
    decoded = grid_bytes(width, height, dtypes)
    if decoded > DECODED_CAP:
        raise result_refusal(fn, decoded, DECODED_CAP)


def check_bands(fn: str, ds, named: Dict[str, int]) -> None:
    """A band argument the tile does not have (1-based) is a wrong argument: it raises with a Fix."""
    for name, value in named.items():
        if not 1 <= int(value) <= ds.count:
            raise _Parameter(
                f"{fn}: {name} {int(value)} is outside the tile's {ds.count} band(s). "
                f"Fix: pass a band from 1 to {ds.count} (gbx_rst_numbands gives the count)"
            )


def with_tile(fn: str, tile, kernel: Callable, driver: str = "GTiff"):
    """``with_dataset`` for a function that returns a tile: ``kernel(ds)`` makes the raster bytes (None for no
    tile), an error tile goes through unchanged, and the result is cap-checked by :func:`tile_row`.
    """
    on_empty = error_row(tile) if tile is not None and _is_error_tile(tile) else None
    return with_dataset(
        fn, tile, lambda ds: tile_row(fn, tile, kernel(ds), driver), on_empty
    )


def _tile_fn(
    sql_name: str,
    module: str,
    attr: str,
    *,
    optional=(),
    check: Optional[Callable] = None,
    cast: Optional[Callable] = None,
    grows: Optional[Callable] = None,
    driver=None,
    bands: Optional[Dict[str, int]] = None,
) -> Callable:
    """A handler ``(tile, *args)`` that returns the tile ``<module>.<attr>(ds, *args)`` makes.

    NULL in is NULL out for the tile and every argument not named in ``optional`` (positions after the tile).
    ``check(*args)`` raises a ``ValueError`` for a wrong argument, before the tile is read; ``cast(*args)`` gives the
    kernel's arguments; ``grows(ds, *args)`` gives the ``(width, height, dtypes)`` of the result grid, which is
    refused over the cap before the kernel makes it; ``driver(*args)`` names the format of the bytes; ``bands`` maps
    the name of each band argument to its position, and a band the tile does not have raises.
    """
    fn = sql_name
    free = frozenset(optional)

    def handler(tile, *args):
        if tile is None or any(a is None for i, a in enumerate(args) if i not in free):
            return None
        done, early = gate(
            fn, tile
        )  # an error tile passes through, whatever the arguments are
        if done:
            return early
        if check is not None:
            check(*args)
        kargs = cast(*args) if cast is not None else args

        def kernel(ds):
            if bands:
                check_bands(fn, ds, {name: args[i] for name, i in bands.items()})
            if grows is not None:
                check_grows(fn, *grows(ds, *args))
            return _kernel(module, attr)(ds, *kargs)

        return with_tile(fn, tile, kernel, driver(*args) if driver else "GTiff")

    handler.__name__ = sql_name[len("gbx_") :]
    handler.__doc__ = f"{sql_name} over a materialized tile (core.{module}.{attr}); the tile it makes."
    HANDLERS[sql_name] = handler
    return handler


def _check(module: str, attr: str, *names: str) -> Callable:
    """A ``check`` that calls the core check ``<module>.<attr>`` with the handler's arguments: by ``names`` (the core
    checks that take keywords), else by position."""

    def run(*args):
        check = _kernel(module, attr)
        check(**dict(zip(names, args))) if names else check(*args)

    return run


# --- warp and edit: tile to tile ------------------------------------------------------------------------------------
def _no_crs_refusal(fn: str) -> _Refusal:
    return _Refusal(
        f"{fn}: the tile has no CRS, so it cannot be reprojected. Fix: set its CRS first with gbx_rst_setcrs"
    )


def _reproject(fn: str, ds, crs: str, resampling: str = "nearest"):
    """The tile reprojected to the CRS ``crs`` (an authority code). A tile with no CRS is refused (nothing to
    reproject from); a result grid over the cap is refused before the pixels are made.
    """
    from databricks.labs.gbx.pyrx.core import warp

    if warp.source_crs(ds) is None:
        raise _no_crs_refusal(fn)
    width, height = warp.target_grid(ds, crs)
    check_grows(fn, width, height, ds.dtypes[:1] * ds.count)
    return warp.reproject_to_crs(ds, crs, resampling=resampling)


def rst_transform(tile, srid):
    """gbx_rst_transform: the tile reprojected to a CRS given as an SRID integer (an EPSG or ESRI code)."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    fn = "gbx_rst_transform"
    if tile is None or srid is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    crs = parse_uc_crs(srid, fn=fn)
    return with_tile(fn, tile, lambda ds: _reproject(fn, ds, crs.code))


HANDLERS["gbx_rst_transform"] = rst_transform


def rst_to_webmercator(tile, resampling):
    """gbx_rst_to_webmercator: the tile reprojected to EPSG:3857, with the resampling word given."""
    from databricks.labs.gbx.pyrx.core._util import resampling_enum

    fn = "gbx_rst_to_webmercator"
    if tile is None or resampling is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    resampling_enum(
        str(resampling)
    )  # a word that names no resampling raises, also for a tile already in 3857
    return with_tile(
        fn, tile, lambda ds: _reproject(fn, ds, "EPSG:3857", str(resampling))
    )


HANDLERS["gbx_rst_to_webmercator"] = rst_to_webmercator


def rst_setsrid(tile, srid):
    """gbx_rst_setsrid: stamp the CRS from an SRID integer without reprojecting; 0 clears it."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    fn = "gbx_rst_setsrid"
    if tile is None or srid is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    if int(srid) < 0:
        raise _Parameter(
            f"{fn}: SRID must be >= 0; got {int(srid)}. Fix: pass 0 to clear the CRS, or an EPSG or ESRI code"
        )
    code = parse_uc_crs(int(srid), fn=fn).srid if int(srid) > 0 else 0
    return with_tile(fn, tile, lambda ds: _kernel("edit", "set_srid")(ds, code))


HANDLERS["gbx_rst_setsrid"] = rst_setsrid


def rst_setcrs(tile, crs):
    """gbx_rst_setcrs: stamp the CRS without reprojecting; the CRS is an SRID, 'EPSG:<n>' or 'ESRI:<n>'."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    fn = "gbx_rst_setcrs"
    if tile is None or crs is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    code = parse_uc_crs(crs, fn=fn).code
    return with_tile(fn, tile, lambda ds: _kernel("edit", "set_crs")(ds, code))


HANDLERS["gbx_rst_setcrs"] = rst_setcrs


def _grid_of(size: Callable) -> Callable:
    """A ``grows`` hint from ``size(ds, *args) -> (width, height)``: the grid has the tile's bands and types."""

    def grows(ds, *args):
        width, height = size(ds, *args)
        return max(1, int(width)), max(1, int(height)), ds.dtypes

    return grows


rst_resample = _tile_fn(
    "gbx_rst_resample",
    "resample",
    "resample_by_factor",
    check=_check("resample", "check_by_factor", "factor", "algorithm"),
    grows=_grid_of(
        lambda ds, f, a: (round(ds.width * float(f)), round(ds.height * float(f)))
    ),
)
rst_resample_to_size = _tile_fn(
    "gbx_rst_resample_to_size",
    "resample",
    "resample_to_size",
    check=_check("resample", "check_to_size", "width_px", "height_px", "algorithm"),
    grows=_grid_of(lambda ds, w, h, a: (w, h)),
)


def _res_size(ds, x_res, y_res, algorithm):
    left, bottom, right, top = ds.bounds
    return (right - left) / float(x_res), (top - bottom) / float(y_res)


rst_resample_to_res = _tile_fn(
    "gbx_rst_resample_to_res",
    "resample",
    "resample_to_res",
    check=_check("resample", "check_to_res", "x_res", "y_res", "algorithm"),
    grows=_grid_of(_res_size),
)


def rst_clip(tile, geom, cutline_all_touched, clip_crs):
    """gbx_rst_clip: the tile cut to a polygon (a GEOMETRY in its own SRID; `clip_crs` names the SRID of one that has
    none). A polygon that misses the tile, or an empty one, is NULL."""
    import shapely

    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.uc.handlers import _geom_bytes

    fn = "gbx_rst_clip"
    if tile is None or geom is None or cutline_all_touched is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    source = None if clip_crs is None else parse_uc_crs(clip_crs, fn=fn).code
    cutline = shapely.from_wkb(_geom_bytes(geom))
    if cutline.is_empty:
        return None
    return with_tile(
        fn,
        tile,
        lambda ds: _kernel("edit", "clip_to_geom")(
            ds, cutline, bool(cutline_all_touched), geom_crs=source
        ),
    )


HANDLERS["gbx_rst_clip"] = rst_clip


def _type_grid(ds, new_type):
    return ds.width, ds.height, [_kernel("edit", "numpy_type")(new_type)] * ds.count


rst_updatetype = _tile_fn(
    "gbx_rst_updatetype",
    "edit",
    "update_type",
    check=_check("edit", "gdal_type"),
    grows=_type_grid,
)
rst_initnodata = _tile_fn("gbx_rst_initnodata", "edit", "init_nodata")
rst_threshold = _tile_fn(
    "gbx_rst_threshold",
    "edit",
    "threshold",
    check=lambda op, value: _kernel("edit", "check_threshold_op")(op),
)


def rst_band(tile, band_index):
    """gbx_rst_band: one 1-based band as a single-band tile; a band the tile does not have raises."""
    fn = "gbx_rst_band"
    if tile is None or band_index is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    if int(band_index) < 1:
        raise _Parameter(
            f"{fn}: band_index {int(band_index)} is out of range (>=1). Fix: pass a band from 1 to the tile's band count"
        )

    def kernel(ds):
        check_bands(fn, ds, {"band_index": band_index})
        return _kernel("edit", "band")(ds, int(band_index))

    return with_tile(fn, tile, kernel)


HANDLERS["gbx_rst_band"] = rst_band


def rst_asformat(tile, new_format):
    """gbx_rst_asformat: the tile re-encoded as another GDAL raster format, the pixels unchanged."""
    fn = "gbx_rst_asformat"
    if tile is None or new_format is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    _kernel("ops", "driver_name")(new_format)  # a format this GDAL cannot write raises
    driver = _kernel("ops", "written_driver")(new_format)
    return with_tile(
        fn, tile, lambda ds: _kernel("ops", "as_format")(ds, str(new_format)), driver
    )


HANDLERS["gbx_rst_asformat"] = rst_asformat


def rst_getsubdataset(tile, subset_name):
    """gbx_rst_getsubdataset: one subdataset of a container tile (NetCDF, HDF, ...) as a tile; a name the tile does not
    have raises (gbx_rst_subdatasets lists them)."""
    fn = "gbx_rst_getsubdataset"
    if tile is None or subset_name is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early

    def kernel(ds):
        try:
            return _kernel("accessors", "getsubdataset")(ds, str(subset_name))
        except (
            ValueError
        ) as exc:  # the name matches no subdataset (the only ValueError the kernel raises)
            raise _Parameter(
                f"{exc}. Fix: pass a name from gbx_rst_subdatasets"
            ) from exc

    return with_tile(fn, tile, kernel)


HANDLERS["gbx_rst_getsubdataset"] = rst_getsubdataset


def rst_align_to(tile, reference):
    """gbx_rst_align_to: the tile warped onto the grid of a reference tile (its CRS, size and transform), nearest
    neighbour. The reference is read for its header only."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes

    fn = "gbx_rst_align_to"
    if tile is None or reference is None:
        return None
    done, early = gate(fn, tile, reference)
    if done:
        return early
    reference_raster = raster_of(fn, reference)

    def kernel(ds):
        with MemoryFile(reference_raster) as rmf, open_bytes(rmf) as ref:
            check_grows(fn, ref.width, ref.height, ds.dtypes)
            return _kernel("warp", "align_datasets")(ds, ref)

    return with_tile(fn, tile, kernel)


HANDLERS["gbx_rst_align_to"] = rst_align_to


rst_filter = _tile_fn(
    "gbx_rst_filter", "focal", "filt", check=_check("focal", "check_filter")
)
rst_convolve = _tile_fn(
    "gbx_rst_convolve", "focal", "convolve", check=_check("focal", "check_kernel")
)
rst_fillnodata = _tile_fn("gbx_rst_fillnodata", "features", "fill_nodata")


# --- terrain: a DEM tile to a surface tile -------------------------------------------------------------------------
def _slope_args(unit, xscale, yscale):
    """The wrong arguments of gbx_rst_slope: a unit that is not degrees or percent, one scale without the other."""
    if str(unit) not in _kernel("terrain", "SLOPE_UNITS"):
        raise _Parameter(_kernel("terrain", "slope_unit_refusal")(unit))
    if (xscale is None) != (yscale is None):
        raise _Parameter(_kernel("terrain", "SLOPE_HALF_SCALE_REFUSAL"))


def _hillshade_args(azimuth, altitude, z_factor, xscale, yscale):
    if (xscale is None) != (yscale is None):
        raise _Parameter(_kernel("terrain", "HILLSHADE_HALF_SCALE_REFUSAL"))


def _surface(dtype: str) -> Callable:
    """A ``grows`` hint: a terrain result is one band of ``dtype`` on the tile's grid."""
    return lambda ds, *args: (ds.width, ds.height, [dtype])


def _optional_float(*values):
    return tuple(None if v is None else float(v) for v in values)


rst_slope = _tile_fn(
    "gbx_rst_slope",
    "terrain",
    "slope",
    optional=(1, 2),
    check=_slope_args,
    cast=lambda unit, xscale, yscale: (str(unit), *_optional_float(xscale, yscale)),
    grows=_surface("float32"),
)
rst_aspect = _tile_fn(
    "gbx_rst_aspect",
    "terrain",
    "aspect",
    cast=lambda trigonometric, zero_for_flat: (
        bool(trigonometric),
        bool(zero_for_flat),
    ),
    grows=_surface("float32"),
)
rst_hillshade = _tile_fn(
    "gbx_rst_hillshade",
    "terrain",
    "hillshade",
    optional=(3, 4),
    check=_hillshade_args,
    cast=lambda azimuth, altitude, z_factor, xscale, yscale: (
        float(azimuth),
        float(altitude),
        float(z_factor),
        *_optional_float(xscale, yscale),
    ),
    grows=_surface("uint8"),
)
rst_tpi = _tile_fn("gbx_rst_tpi", "terrain", "tpi", grows=_surface("float32"))
rst_tri = _tile_fn("gbx_rst_tri", "terrain", "tri", grows=_surface("float32"))
rst_roughness = _tile_fn(
    "gbx_rst_roughness", "terrain", "roughness", grows=_surface("float32")
)


# --- spectral indices: bands to one index band -----------------------------------------------------------------------
rst_ndvi = _tile_fn(
    "gbx_rst_ndvi",
    "indices",
    "ndvi",
    cast=lambda red, nir: (int(red), int(nir)),
    bands={"red_idx": 0, "nir_idx": 1},
    grows=_surface("float32"),
)
rst_ndwi = _tile_fn(
    "gbx_rst_ndwi",
    "indices",
    "ndwi",
    cast=lambda green, nir: (int(green), int(nir)),
    bands={"green_idx": 0, "nir_idx": 1},
    grows=_surface("float32"),
)
rst_nbr = _tile_fn(
    "gbx_rst_nbr",
    "indices",
    "nbr",
    cast=lambda nir, swir: (int(nir), int(swir)),
    bands={"nir_idx": 0, "swir_idx": 1},
    grows=_surface("float32"),
)
rst_savi = _tile_fn(
    "gbx_rst_savi",
    "indices",
    "savi",
    cast=lambda red, nir, l_val: (int(red), int(nir), float(l_val)),
    bands={"red_idx": 0, "nir_idx": 1},
    grows=_surface("float32"),
)
rst_evi = _tile_fn(
    "gbx_rst_evi",
    "indices",
    "evi",
    cast=lambda red, nir, blue, l_val, c1, c2, g: (
        int(red),
        int(nir),
        int(blue),
        float(l_val),
        float(c1),
        float(c2),
        float(g),
    ),
    bands={"red_idx": 0, "nir_idx": 1, "blue_idx": 2},
    grows=_surface("float32"),
)


def rst_index(tile, formula_name, band_map):
    """gbx_rst_index: a built-in index (ndvi, msavi, ...) from the bands named in `band_map`, one Float32 band."""
    fn = "gbx_rst_index"
    if tile is None or formula_name is None or band_map is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    key, mapped = _kernel("indices", "check_index")(formula_name, band_map)
    needed = {
        f"band_map['{b}']": mapped[b] for b in _kernel("indices", "formula_bands")(key)
    }

    def kernel(ds):
        check_bands(fn, ds, needed)
        check_grows(fn, *_surface("float32")(ds))
        return _kernel("indices", "index")(ds, str(formula_name), dict(band_map))

    return with_tile(fn, tile, kernel)


HANDLERS["gbx_rst_index"] = rst_index


# --- other tile to tile ------------------------------------------------------------------------------------------
def rst_chm(dsm_tile, dem_tile):
    """gbx_rst_chm: the canopy height model, the DSM aligned to the DEM's grid minus the DEM, negatives clamped to 0."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes

    fn = "gbx_rst_chm"
    if dsm_tile is None or dem_tile is None:
        return None
    done, early = gate(fn, dsm_tile, dem_tile)
    if done:
        return early
    dsm, dem = raster_of(fn, dsm_tile), raster_of(fn, dem_tile)

    def kernel(dsm_ds):
        with MemoryFile(dem) as mf, open_bytes(mf) as dem_ds:
            decoded = decoded_bytes(dem_ds)
            if decoded > DECODED_CAP:
                raise cap_refusal(fn, decoded, DECODED_CAP)
            check_grows(fn, dem_ds.width, dem_ds.height, ["float32"])
        return _kernel("chm", "chm")(dsm, dem)

    return with_tile(fn, dsm_tile, kernel)


HANDLERS["gbx_rst_chm"] = rst_chm


def rst_land_cover(tile, method, n_clusters, smooth):
    """gbx_rst_land_cover: Int32 class labels of a tile of 3 or more bands (red, green, blue, ...); a tile of fewer
    bands is refused with a Fix."""
    fn = "gbx_rst_land_cover"
    if tile is None or method is None or n_clusters is None or smooth is None:
        return None
    done, early = gate(fn, tile)
    if done:
        return early
    _kernel("land_cover", "check_args")(method, n_clusters, smooth)

    def kernel(ds):
        check_grows(fn, *_surface("int32")(ds))
        try:
            return _kernel("land_cover", "classify_dataset")(
                ds, method, n_clusters, smooth
            )
        except _kernel("land_cover", "TooFewBands") as exc:
            raise _Refusal(str(exc)) from exc

    return with_tile(fn, tile, kernel)


HANDLERS["gbx_rst_land_cover"] = rst_land_cover

rst_percentile_stretch = _tile_fn(
    "gbx_rst_percentile_stretch",
    "stretch",
    "percentile_stretch",
    check=_check("stretch", "check_percentiles"),
    cast=lambda lo, hi: (float(lo), float(hi)),
)
rst_proximity = _tile_fn(
    "gbx_rst_proximity",
    "analysis",
    "proximity",
    optional=(0, 1, 2),
    check=_check(
        "analysis", "check_proximity", "target_values", "distunits", "max_distance"
    ),
    cast=lambda target_values, dist_units, max_distance: (
        None if target_values is None else str(target_values),
        "GEO" if dist_units is None else str(dist_units),
        *_optional_float(max_distance),
    ),
    grows=_surface("float32"),
)


# --- an ARRAY of tiles: combine, merge, frombands, mapalgebra -------------------------------------------------------
# The session rules for the members of an array (pyrx.core.arrays): a NULL element is skipped, an error tile or a tile
# that cannot be read is skipped and counted in the result's `last_error`, an array of nothing but error tiles answers
# the first of them, and an array with no usable member is NULL. UC adds what a sandbox needs: a virtual member refuses
# the array, and the members' decoded sizes plus the result's are checked against the cap before any pixel is read.
def _agg():
    from databricks.labs.gbx.pyrx.core import agg

    return agg


def array_members(fn: str, tiles):
    """``(members, done, value)`` for an ARRAY<TILE> argument: ``members`` is ``[(tile, raster bytes)]`` for the
    elements that hold a raster, in order. A virtual element refuses the array (before anything is read). With no
    element holding a raster, ``done`` is True and ``value`` is the first error tile when every non-NULL element is
    one, else NULL."""
    if tiles is None:
        return [], True, None
    present = [t for t in tiles if t is not None]
    members = []
    for t in present:
        raster = raster_of(fn, t)
        if raster is not None:
            members.append((t, raster))
    if members:
        return members, False, None
    errors = [error_row(t) for t in present if _is_error_tile(t)]
    if present and len(errors) == len(present):
        return [], True, _kernel("arrays", "first_error")(errors)
    return [], True, None


def usable_members(members):
    """``(tiles, rasters, dropped)``: the members whose bytes open as a raster (the others are counted, not errors)."""
    rasters, kept, dropped = _kernel("arrays", "collect_members")(
        members, lambda m: m[1]
    )
    return [t for t, _ in kept], rasters, dropped


def check_array_cap(fn: str, hdrs, output: int, member_fix: str, total_fix: str):
    """Refuse an array before any pixel is read: a member decoded over the cap, or the members together with the result
    over it. The Fix of each is the session function's route for it."""
    cap = DECODED_CAP
    for h in hdrs:
        if h.decoded > cap:
            raise _Refusal(
                f"{fn}: the tile is {size_text(h.decoded)} decoded, over the {size_text(cap)} a Unity Catalog "
                f"function decodes. Fix: {member_fix}"
            )
    members = sum(h.decoded for h in hdrs)
    if members + output > cap:
        raise _Refusal(
            f"{fn}: the {len(hdrs)} tiles are {size_text(members)} decoded and the result {size_text(output)}, "
            f"together over the {size_text(cap)} a Unity Catalog function decodes. Fix: {total_fix}"
        )


def run_array_kernel(fn: str, kernel: Callable):
    """``kernel()`` with the UC cap as the read cap every gate of the kernel judges by. A refusal of the kernel keeps its
    Fix; a wrong argument, resource exhaustion and a missing dependency raise; bytes the kernel cannot read are NULL.
    """
    from databricks.labs.gbx._gdal_policy import carried

    agg = _agg()
    try:
        with carried(None, DECODED_CAP):
            return kernel()
    except (agg.ReadCapRefused, agg.MosaicTooLarge) as exc:
        raise _Refusal(f"{fn}: {exc}") from exc
    except (ValueError, MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - unreadable data is NULL on UC (no error tile)
        return None


def array_row(fn: str, raster, cellid: int, op: str, dropped: int = 0):
    """The tile an array function returns: the cell id the session gives it, and the note for the members it skipped."""
    row = tile_row(fn, {"cellid": cellid}, raster)
    if row is not None and dropped:
        row["metadata"]["last_error"] = _kernel("arrays", "skipped_note")(op, dropped)
    return row


def _cellid(tile) -> int:
    return int(_field(tile, "cellid") or 0)


def _array_fn(
    sql_name: str, op: str, kernel: Callable, output: Callable, fixes: Callable
):
    """A handler ``(tiles)`` for a function that folds an array into one tile.

    ``kernel(rasters)`` makes the GTiff bytes, ``output(hdrs)`` is the decoded size of the result, ``fixes(hdrs)`` gives
    the ``(member, total)`` Fix of an array over the cap, and the cell id of the result is the session's rule for ``op``.
    """
    fn = sql_name

    def handler(tiles):
        members, done, value = array_members(fn, tiles)
        if done:
            return value
        kept, rasters, dropped = usable_members(members)
        if not rasters:
            return None
        hdrs = _agg()._scan_headers(rasters)
        check_array_cap(fn, hdrs, output(hdrs), *fixes(hdrs))
        made = run_array_kernel(fn, lambda: kernel(rasters))
        return array_row(fn, made, _result_cellid(op, kept), op, dropped)

    handler.__name__ = sql_name[len("gbx_") :]
    handler.__doc__ = (
        f"{sql_name} over an array of materialized tiles ({op}); the tile it makes."
    )
    HANDLERS[sql_name] = handler
    return handler


def _result_cellid(op: str, kept) -> int:
    """The cell id of a result, by the session rule of each function: a combine keeps the cell id its members share (-1
    when they differ), a merge has none (0), a stack of bands keeps the first member's.
    """
    if op == "RST_Merge":
        return 0
    if op == "RST_FromBands":
        return _cellid(kept[0])
    return _kernel("arrays", "shared_cellid")([_cellid(t) for t in kept])


def _same_grid(hdrs) -> int:
    """The decoded size of one band of the first member's grid in its first band's type, times its bands."""
    ref = hdrs[0]
    return grid_bytes(ref.width, ref.height, ref.dtypes[:1] * ref.count)


def _combine_fixes(hdrs):
    route = _agg()._COMBINE_OUT_ROUTE
    return route, route


def _combine(stat: str) -> Callable:
    reducer = "combineavg_tiles" if stat == "avg" else f"combine_{stat}_tiles"
    return _array_fn(
        f"gbx_rst_combine{stat}",
        f"RST_Combine{stat.capitalize()}",
        lambda rasters: getattr(_agg(), reducer)(rasters),
        _same_grid,
        _combine_fixes,
    )


rst_combineavg = _combine("avg")
rst_combinecount = _combine("count")
rst_combinemax = _combine("max")
rst_combinemedian = _combine("median")
rst_combinemin = _combine("min")
rst_combinestddev = _combine("stddev")
rst_combinesum = _combine("sum")


def _session_fix(fn: str):
    return (
        lambda hdrs: (
            (
                f"run {fn} in a notebook or job session, or retile first (gbx_rst_retile) so each tile is smaller"
            ),
        )
        * 2
    )


def _stack_bytes(hdrs) -> int:
    """The decoded size of the stack of every member's bands on the first member's grid."""
    ref = hdrs[0]
    return grid_bytes(ref.width, ref.height, [dt for h in hdrs for dt in h.dtypes])


rst_frombands = _array_fn(
    "gbx_rst_frombands",
    "RST_FromBands",
    lambda rasters: _agg().frombands_tiles(list(enumerate(rasters))),
    _stack_bytes,
    _session_fix("gbx_rst_frombands"),
)


def rst_merge(tiles):
    """gbx_rst_merge: the mosaic of an array of tiles by extent, in the CRS of the lowest EPSG code (tiles in other CRSs
    are reprojected onto it). A mosaic the sandbox cannot build in memory is refused with the session's routes.
    """
    fn = "gbx_rst_merge"
    members, done, value = array_members(fn, tiles)
    if done:
        return value
    kept, rasters, dropped = usable_members(members)
    if not rasters:
        return None
    agg = _agg()
    hdrs = agg._scan_headers(rasters)
    ref_crs = agg._pick_ref_crs(hdrs)
    mixed = agg._mixed_grid(hdrs, ref_crs)
    try:
        union = agg._union_decoded_bytes(hdrs, ref_crs)
    except (
        Exception
    ):  # noqa: BLE001 - the kernel's own size gate falls back the same way
        union = max(h.decoded for h in hdrs)
    check_array_cap(
        fn,
        hdrs,
        union,
        "retile it first (gbx_rst_retile) so each tile is smaller, or "
        + agg._vrt_fix(agg._VRT_ROUTE, mixed),
        agg._vrt_fix(agg._MOSAIC_ROUTE, mixed),
    )
    made = run_array_kernel(fn, lambda: agg.merge_tiles(rasters, stream=False))
    return array_row(fn, made, 0, "RST_Merge", dropped)


HANDLERS["gbx_rst_merge"] = rst_merge


def rst_mapalgebra(tiles, expression):
    """gbx_rst_mapalgebra: an expression over band 1 of each tile (A, B, C, ... in array order), or the bands a JSON spec
    names, as a Float32 tile on the first tile's grid. The expression is evaluated by numexpr (math only). An error
    member goes through; a member that cannot be read is NULL (every member is needed).
    """
    fn = "gbx_rst_mapalgebra"
    if tiles is None or expression is None:
        return None
    members, done, value = array_members(fn, tiles)
    if done:
        return value
    errors = [error_row(t) for t in tiles if t is not None and _is_error_tile(t)]
    if errors:
        return _kernel("arrays", "first_error")(errors)
    try:
        _kernel("mapalgebra", "check_expression")(expression, len(members))
    except ValueError as exc:
        raise _Parameter(f"{fn}: {exc}") from exc
    rasters = [r for _, r in members]
    agg = _agg()
    try:
        hdrs = agg._scan_headers(rasters)
    except Exception:  # noqa: BLE001 - a member that cannot be read: the call is NULL
        return None
    check_array_cap(
        fn,
        hdrs,
        grid_bytes(hdrs[0].width, hdrs[0].height, ["float32"]),
        *_session_fix(fn)(hdrs),
    )
    made = run_array_kernel(
        fn, lambda: _kernel("mapalgebra", "mapalgebra")(rasters, str(expression))
    )
    return array_row(fn, made, _cellid(tiles[0]), "RST_MapAlgebra")


HANDLERS["gbx_rst_mapalgebra"] = rst_mapalgebra


# --- constructors: geometries or points to a tile (no input tile) ------------------------------------------------------
# There is no tile to gate, so the order is: NULL in is NULL out, a wrong argument raises, the grid to be made is checked
# against the cap before any of it is allocated (its working set, not only its pixels), then the kernel runs.
def grid_refusal(
    fn: str, width: int, height: int, decoded: int, working: int
) -> _Refusal:
    """A grid too big for a Unity Catalog function to make: never truncated, refused with a Fix."""
    return _Refusal(
        f"{fn}: the {int(width)} x {int(height)} grid is {size_text(decoded)} decoded and takes about "
        f"{size_text(working)} to make, over the {size_text(DECODED_CAP)} a Unity Catalog function holds. "
        f"Fix: ask for fewer pixels (a smaller width_px and height_px, or one call for each part of the extent), "
        f"or run {fn} in a notebook or job session"
    )


def check_made(fn: str, width, height, dtypes, working: int) -> None:
    """Refuse, before anything is allocated, a grid whose pixels or whose working set are over the cap."""
    decoded = grid_bytes(width, height, dtypes)
    if max(decoded, working) > DECODED_CAP:
        raise grid_refusal(fn, width, height, decoded, working)


def _grid_args(fn: str, xmin, ymin, xmax, ymax, width_px, height_px) -> None:
    """The size and extent of the grid to make: a wrong one raises."""
    try:
        _kernel("_util", "check_grid")(fn, xmin, ymin, xmax, ymax, width_px, height_px)
    except ValueError as exc:
        raise _Parameter(
            f"{exc}. Fix: pass a width_px and height_px above 0 and an extent with xmax above xmin and ymax above ymin"
        ) from exc


def _out_crs(fn: str, out_srid, out_crs):
    """The authority code of the output CRS from ``out_srid`` or ``out_crs`` (not both), else None."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    if out_srid is not None and out_crs is not None:
        raise _Parameter(
            f"{fn}: provide out_srid OR out_crs, not both. Fix: pass one of them, or neither for a tile with no CRS"
        )
    if out_crs is not None:
        return parse_uc_crs(out_crs, fn=fn).code
    if out_srid is not None:
        return parse_uc_crs(int(out_srid), fn=fn).code
    return None


def _constructed(fn: str, made):
    return tile_row(fn, {"cellid": 0}, made)


def rst_rasterize(
    geom, value, xmin, ymin, xmax, ymax, width_px, height_px, out_srid, out_crs
):
    """gbx_rst_rasterize: a Float64 tile over the extent with `value` in the pixels whose centre the geometry covers (a
    GEOMETRY in its own SRID, reprojected to the output CRS) and NoData elsewhere."""
    fn = "gbx_rst_rasterize"
    if any(
        a is None for a in (geom, value, xmin, ymin, xmax, ymax, width_px, height_px)
    ):
        return None
    _grid_args(fn, xmin, ymin, xmax, ymax, width_px, height_px)
    crs = _out_crs(fn, out_srid, out_crs)
    ewkb = _geom_bytes(geom)
    check_made(
        fn,
        width_px,
        height_px,
        ["float64"],
        _kernel("features", "rasterize_working_bytes")(width_px, height_px),
    )
    made = run_array_kernel(
        fn,
        lambda: _kernel("features", "rasterize_geom")(
            ewkb, value, xmin, ymin, xmax, ymax, width_px, height_px, out_crs=crs
        ),
    )
    return _constructed(fn, made)


HANDLERS["gbx_rst_rasterize"] = rst_rasterize


def rst_binpoints(
    x_array,
    y_array,
    z_array,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
    statistic,
):
    """gbx_rst_binpoints: a Float32 tile of the statistic (max, min, mean, median, count, percentile:<p>) of the points in
    each pixel; a point on the east or south edge is left out and an empty pixel is NoData.
    """
    from databricks.labs.gbx.core.crs import parse_uc_crs

    fn = "gbx_rst_binpoints"
    if any(
        a is None
        for a in (y_array, z_array, xmin, ymin, xmax, ymax, width_px, height_px, srid)
    ):
        return None
    if not x_array:
        return None
    _grid_args(fn, xmin, ymin, xmax, ymax, width_px, height_px)
    try:
        _kernel("binning", "check_lengths")(x_array, y_array, z_array)
        _kernel("binning", "check_statistic")("max" if statistic is None else statistic)
    except ValueError as exc:
        raise _Parameter(
            f"{exc}. Fix: pass arrays of one length, and a statistic of max, min, mean, median, count or percentile:<p>"
        ) from exc
    code = parse_uc_crs(int(srid), fn=fn).srid
    check_made(
        fn,
        width_px,
        height_px,
        ["float32"],
        _kernel("binning", "working_bytes")(width_px, height_px, len(x_array)),
    )
    made = run_array_kernel(
        fn,
        lambda: _kernel("binning", "bin_points")(
            x_array,
            y_array,
            z_array,
            float(xmin),
            float(ymin),
            float(xmax),
            float(ymax),
            int(width_px),
            int(height_px),
            code,
            "max" if statistic is None else str(statistic),
        ),
    )
    return _constructed(fn, made)


HANDLERS["gbx_rst_binpoints"] = rst_binpoints


def rst_gridfrompoints(
    points_array,
    values_array,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid,
    power,
    max_pts,
    out_crs,
):
    """gbx_rst_gridfrompoints: a Float64 tile of the inverse-distance-weighted mean of the nearest `max_pts` points (WKB or
    EWKB POINTs, taken to be in the output CRS) in each pixel; NoData where there are none.
    """
    fn = "gbx_rst_gridfrompoints"
    if any(
        a is None
        for a in (
            points_array,
            values_array,
            xmin,
            ymin,
            xmax,
            ymax,
            width_px,
            height_px,
        )
    ):
        return None
    _grid_args(fn, xmin, ymin, xmax, ymax, width_px, height_px)
    try:
        _kernel("tin", "check_lengths")(points_array, values_array)
        _kernel("tin", "check_idw_args")(
            2.0 if power is None else power, 12 if max_pts is None else max_pts
        )
    except ValueError as exc:
        raise _Parameter(
            f"{exc}. Fix: pass as many values as points, a power above 0 and a max_pts above 0"
        ) from exc
    crs = _out_crs(fn, out_srid, out_crs)
    check_made(
        fn,
        width_px,
        height_px,
        ["float64"],
        _kernel("tin", "idw_working_bytes")(
            width_px,
            height_px,
            len(points_array),
            12 if max_pts is None else max_pts,
        ),
    )
    made = run_array_kernel(
        fn,
        lambda: _kernel("tin", "grid_from_points")(
            points_array,
            values_array,
            xmin,
            ymin,
            xmax,
            ymax,
            width_px,
            height_px,
            power=power,
            max_pts=max_pts,
            out_crs=crs,
        ),
    )
    return _constructed(fn, made)


HANDLERS["gbx_rst_gridfrompoints"] = rst_gridfrompoints


def rst_dtmfromgeoms(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid,
    no_data,
    out_crs,
):
    """gbx_rst_dtmfromgeoms: a Float64 tile of the height interpolated in the Delaunay triangle of each pixel (the points
    are WKB or EWKB POINTs with a Z; the break lines' vertices are extra points), `no_data` outside their hull.
    """
    fn = "gbx_rst_dtmfromgeoms"
    if any(
        a is None
        for a in (
            points_array,
            merge_tolerance,
            snap_tolerance,
            xmin,
            ymin,
            xmax,
            ymax,
            width_px,
            height_px,
        )
    ):
        return None
    _grid_args(fn, xmin, ymin, xmax, ymax, width_px, height_px)
    crs = _out_crs(fn, out_srid, out_crs)
    check_made(
        fn,
        width_px,
        height_px,
        ["float64"],
        _kernel("tin", "dtm_working_bytes")(
            width_px,
            height_px,
            len(points_array) + len(breaklines_array or ()),
        ),
    )
    made = run_array_kernel(
        fn,
        lambda: _kernel("tin", "dtm_from_geoms")(
            points_array,
            breaklines_array,
            xmin,
            ymin,
            xmax,
            ymax,
            width_px,
            height_px,
            no_data=no_data,
            out_crs=crs,
        ),
    )
    return _constructed(fn, made)


HANDLERS["gbx_rst_dtmfromgeoms"] = rst_dtmfromgeoms
