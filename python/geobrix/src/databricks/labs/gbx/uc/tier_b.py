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
"""

from __future__ import annotations

import importlib
from typing import Callable, Dict, Optional

from databricks.labs.gbx.pyrx.core.budget import (
    UC_DECODED_CAP,
    decoded_bytes,
    size_text,
)
from databricks.labs.gbx.uc.handlers import _field, _Refusal, _refuse

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
