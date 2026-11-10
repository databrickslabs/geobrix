"""The Unity Catalog handlers of the GDAL spec engine's rows: the session's engine call, under Unity Catalog's rules.

One generic handler (:func:`handler`) serves every manifest row that has a ``gdal`` block and a UC form. It runs
``engine.run.run`` on the arguments as the session's row-wise UDF (``engine.l3.c0``) does: the same argv, the same GDAL
and the same tile, so a UC function answers what its session function answers. On top of that it keeps the rules every
UC raster function has (``uc.tier_b``):

- a virtual tile (a path, no raster bytes) is refused with a ``Fix:`` that names the session function;
- a CRS argument is an integer SRID, ``EPSG:<n>`` or ``ESRI:<n>`` (``core.crs.parse_uc_crs``), refused with a ``Fix:``
  otherwise;
- a ``GEOMETRY`` argument arrives as the sandbox's geometry value and reaches the engine as EWKB, with its SRID;
- a call holds at most :data:`DECODED_CAP` decoded, each input and the inputs with the estimated output together (the
  engine's budget on Serverless). Over it the call is refused with a ``Fix:``, never truncated. A list of features is
  refused over the same size;
- a data error (corrupt bytes, a call GDAL fails) is NULL, because a UC function has no error tile. A parameter error
  raises. An error tile goes through a tile-returning function unchanged.

A generator row (``gbx_rst_polygonize``) is a UC table function over a private array function (manifest ``uc_private``):
:func:`features` is that array, one struct per feature.

Import-light, as :mod:`databricks.labs.gbx.uc.handlers`: no pyspark (``engine.run`` imports none) and no light kernel.
"""

from __future__ import annotations

import functools
from typing import Callable, Dict

from databricks.labs.gbx.engine import _tile as tile_checks
from databricks.labs.gbx.pyrx.core.budget import UC_DECODED_CAP
from databricks.labs.gbx.uc.handlers import _Refusal, _refuse

# The decoded size a call may hold: each input, and its inputs with the estimated output (the D-P2 cap of every UC tile
# function, uc.tier_b.DECODED_CAP).
DECODED_CAP = UC_DECODED_CAP

HANDLERS: Dict[str, Callable] = {}


@functools.lru_cache(maxsize=None)
def _function(sql_name: str):
    from databricks.labs.gbx import manifest

    fn = manifest.load().functions[sql_name]
    if fn.gdal is None:
        raise ValueError(
            f"{sql_name} has no gdal block: the engine handler does not run it"
        )
    return fn


def _tile_dict(tile) -> dict:
    """A tile as a dict whatever form the sandbox hands it in, its metadata a dict (``uc.tier_b._metadata``)."""
    from databricks.labs.gbx.uc.tier_b import _metadata

    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    return {**dict.fromkeys(tile_checks.TILE_FIELDS), **d, "metadata": _metadata(tile)}


def _inputs(fn, first):
    """``(rasters, passthrough, cellid)``, the session's rule (``engine.l3._inputs``) for materialized tiles: NULL, an empty
    tile and an empty array give ``(None, None, None)``; an error tile (the first) gives ``(None, it, None)``; a virtual
    tile is refused with a Fix. Every tile is judged before one is read, as in the session.
    """
    one = fn.gdal.inputs[0][1] == "raster"
    if first is None:
        return None, None, None
    tiles = [first] if one else list(first)
    if not tiles:
        return None, None, None
    dicts = []
    for t in tiles:
        if t is None:
            return None, None, None
        d = _tile_dict(t)
        err = tile_checks.error_tile_of(d)
        if err is not None:
            return None, err, None
        if tile_checks.is_empty(d):
            return None, None, None
        dicts.append(d)
    for d in dicts:
        if d["raster"] is None:
            raise _refuse(
                fn.sql_name, "virtual tiles (a path, no raster bytes) are session-only"
            )
    return [bytes(d["raster"]) for d in dicts], None, tile_checks.cellid(dicts[0])


def _values(fn, args) -> dict:
    """The parameters after the first, by name, as the engine takes them: a GEOMETRY as EWKB, a CRS checked against the
    forms UC takes. A parameter left out takes its DEFAULT (the SQL wrapper passes every one).
    """
    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.engine.spec import sql_default
    from databricks.labs.gbx.uc.handlers import _geom_bytes

    params = fn.params[1:]
    if len(args) > len(params):
        raise TypeError(
            f"{fn.sql_name} takes {len(params) + 1} arguments, got {len(args) + 1}"
        )
    raw = list(args) + [sql_default(p.default) for p in params[len(args) :]]
    values = {}
    for p, v in zip(params, raw):
        if v is not None and (p.uc or "").upper() == "GEOMETRY":
            v = _geom_bytes(v)
        elif v is not None and p.uc_crs:
            parse_uc_crs(
                v, fn=fn.sql_name
            )  # refused with a Fix outside SRID / EPSG:n / ESRI:n
        values[p.name] = v
    return values


def _over_cap(fn, res) -> _Refusal:
    return _Refusal(
        f"{res.error}, or run {fn.sql_name} in a notebook or job session. The limit is the "
        f"{DECODED_CAP >> 20} MiB a Unity Catalog function decodes"
    )


def _check_features(fn, rows) -> None:
    from databricks.labs.gbx.uc.tier_b import check_rows

    check_rows(fn.sql_name, rows)


def _call(fn, first, args):
    """The engine's answer as the session gives it (a tile, a list of feature dicts, a JSON text), or None; an error
    tile (``(None, tile)``) for a tile-returning row given one."""
    from databricks.labs.gbx.engine import run

    rasters, passthrough, cellid = _inputs(fn, first)
    if rasters is None:
        return passthrough if fn.gdal.out_kind == "raster" else None
    values = _values(fn, args)
    res = run.run(fn, rasters, values, cap=DECODED_CAP)
    if res is None:
        return None
    if res.error is not None:
        if res.over_cap:
            raise _over_cap(fn, res)
        return None  # a data error: a UC function has no error tile
    if fn.gdal.out_kind == "raster":
        return tile_checks.engine_tile(res, cellid)
    if fn.gdal.out_kind == "json":
        return res.text
    names = ["geom_wkb"] + [n for n, _, _ in fn.gdal.out_fields]
    try:
        rows = [dict(zip(names, row)) for row in res.features]
    finally:
        close = getattr(res.features, "close", None)
        if close is not None:  # a streamed output removes its file
            close()
    _check_features(fn, rows)
    return rows


def handler(sql_name: str) -> Callable:
    """The UC Python handler of the engine row ``sql_name``: ``(first, *args)`` with the row's parameters in order, the
    first a tile or an array of tiles. The manifest row is read when the handler first runs.
    """

    def call(first, *args):
        return _call(_function(sql_name), first, args)

    call.__name__ = sql_name[len("gbx_") :]
    call.__qualname__ = call.__name__
    call.__doc__ = (
        f"{sql_name} on Unity Catalog: the session's GDAL spec engine call (engine.uc)."
    )
    HANDLERS[sql_name] = call
    return call


def features(sql_name: str) -> Callable:
    """The private array function under the UC table function of the generator row ``sql_name``: every feature of the
    engine's output, in layer order, as an array of structs (the table function's columns). NULL, an empty or error tile
    and a data error give NULL (no rows)."""

    def call(first, *args):
        fn = _function(sql_name)
        if fn.kind != "generator" or fn.gdal.out_kind != "features":
            raise ValueError(
                f"{sql_name}: features() serves a generator row with a features output"
            )
        return _call(fn, first, args)

    call.__name__ = f"{sql_name[len('gbx_') :]}_features"
    call.__qualname__ = call.__name__
    call.__doc__ = f"The features of {sql_name} as an array, for its UC table function (engine.uc)."
    return call


# --- one handler per engine row with a UC form (the manifest's L4 `handler` names it) -------------------------------
rst_aspect = handler("gbx_rst_aspect")
rst_color_relief = handler("gbx_rst_color_relief")
rst_contour = handler("gbx_rst_contour")
rst_evi = handler("gbx_rst_evi")
rst_fillnodata = handler("gbx_rst_fillnodata")
rst_hillshade = handler("gbx_rst_hillshade")
rst_index = handler("gbx_rst_index")
rst_mapalgebra = handler("gbx_rst_mapalgebra")
rst_nbr = handler("gbx_rst_nbr")
rst_ndvi = handler("gbx_rst_ndvi")
rst_ndwi = handler("gbx_rst_ndwi")
rst_resample = handler("gbx_rst_resample")
rst_resample_to_res = handler("gbx_rst_resample_to_res")
rst_resample_to_size = handler("gbx_rst_resample_to_size")
rst_roughness = handler("gbx_rst_roughness")
rst_savi = handler("gbx_rst_savi")
rst_slope = handler("gbx_rst_slope")
rst_summary = handler("gbx_rst_summary")
rst_threshold = handler("gbx_rst_threshold")
rst_to_webmercator = handler("gbx_rst_to_webmercator")
rst_tpi = handler("gbx_rst_tpi")
rst_transform = handler("gbx_rst_transform")
rst_transformcrs = handler("gbx_rst_transformcrs")
rst_tri = handler("gbx_rst_tri")
rst_viewshed = handler("gbx_rst_viewshed")
rst_polygonize_features = features("gbx_rst_polygonize")
