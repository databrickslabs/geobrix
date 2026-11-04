"""UC array forms: a table function over a bounded ARRAY scalar, and the array twins of two aggregates.

Import-light, as :mod:`databricks.labs.gbx.uc.handlers`: no pyspark, no pandas, and a kernel is imported when its
handler first runs. Each handler is a thin function over the pure core of the light kernel:

- ``st_asmvt_pyramid_cells`` is the array behind the table function ``gbx_st_asmvt_pyramid``
  (``pyvx._mvt.pyramid_tiles``);
- ``rst_h3_rasterize`` is ``gbx_rst_h3_rasterize`` (``pyrx.core.cellraster``, the kernel of the session twin of
  ``gbx_rst_h3_rasterize_agg``);
- ``h3_cellfill_cells`` and ``st_asmvt_features`` are private (``gbx__h3_cellfill_cells``, ``gbx__st_asmvt_features``):
  the array twins of ``gbx_h3_cellfill`` and ``gbx_st_asmvt``, called by the procedures only.

An array row is bounded: the transport carries 256 MiB, but a kernel is only measured up to
:data:`~databricks.labs.gbx.uc.tier_b.DECODED_CAP` decoded, so an input or a result over it is refused with a ``Fix:``
that names the session function, never truncated. NULL in is NULL out; a wrong argument raises; resource exhaustion
and a missing dependency are never data.
"""

from __future__ import annotations

import json
from typing import List

from databricks.labs.gbx.pyrx.core.budget import size_text
from databricks.labs.gbx.uc.handlers import (
    WGS84_SRID,
    _check_array,
    _field,
    _geom_bytes,
    _Refusal,
    _refuse,
)
from databricks.labs.gbx.uc.tier_b import DECODED_CAP

_CELL_BYTES = 40  # a cell of an array: the id, the value and the row's overhead, an upper estimate
_TILE_OVERHEAD = 32


def _too_big(fn: str, what: str, size: int, session_fn: str) -> _Refusal:
    return _Refusal(
        f"{fn}: {what} is {size_text(size)}, over the {size_text(DECODED_CAP)} a Unity Catalog function holds. "
        f"Fix: run {session_fn} in a notebook or job session, or split the input into smaller groups"
    )


def _parameter(fn: str, message: str) -> ValueError:
    return ValueError(f"{fn}: {message}")


# --- the MVT pyramid: the array behind the table function ----------------------------------------------------------
def _attrs(fn: str, attrs) -> dict:
    """The feature's attributes: a JSON object text (``to_json(struct(...))``); NULL is no property."""
    if attrs is None:
        return {}
    try:
        value = json.loads(attrs) if isinstance(attrs, (str, bytes)) else attrs
    except ValueError as exc:
        text = attrs.decode("utf-8", "replace") if isinstance(attrs, bytes) else attrs
        if text.strip().startswith("{") and text.strip().endswith("}"):
            # A STRUCT passed as attrs is cast to text such as `{1, a}`: braces, but not JSON.
            raise _parameter(
                fn,
                "attrs looks like a STRUCT cast to text; on Unity Catalog it takes a JSON object string. "
                "Fix: pass to_json(struct(...))",
            ) from exc
        raise _parameter(
            fn,
            f"attrs is not JSON ({exc}). Fix: pass the attributes as a JSON object text, for example "
            "to_json(struct(name, id))",
        ) from exc
    if not isinstance(value, dict):
        raise _parameter(
            fn,
            "attrs must be a JSON object. Fix: pass to_json(struct(...)) or to_json(map(...)), not an array or a value",
        )
    return value


def st_asmvt_pyramid_cells(geom, attrs, min_z, max_z, layer_name, extent):
    """gbx__st_asmvt_pyramid_cells: the ``(z, x, y, mvt_bytes)`` of every tile one EPSG:4326 feature touches, as an
    array. ``gbx_st_asmvt_pyramid`` is the table function over it (``inline`` of the array).
    """
    import shapely

    from databricks.labs.gbx.pyvx import _mvt

    fn = "gbx_st_asmvt_pyramid"
    if geom is None or min_z is None or max_z is None:
        return None
    props = _attrs(fn, attrs)
    layer = "layer" if layer_name is None else str(layer_name)
    ext = _mvt.DEFAULT_EXTENT if extent is None else int(extent)
    try:
        g = shapely.from_wkb(_geom_bytes(geom))
    except (MemoryError, RecursionError, ImportError):
        raise
    except Exception:  # noqa: BLE001 - unreadable geometry is NULL on UC
        return None
    srid = shapely.get_srid(g)
    if srid != WGS84_SRID:
        raise _refuse(
            fn,
            f"the pyramid needs geometry in SRID {WGS84_SRID}; got SRID {srid} (st_transform(geom, {WGS84_SRID}) first)",
        )
    out: List[dict] = []
    size = 0
    # the generator yields one tile at a time: stop at the cap, so a wide feature never fills the worker
    for z, x, y, blob in _mvt.pyramid_tiles(
        shapely.set_srid(g, 0), props, int(min_z), int(max_z), layer, ext
    ):
        size += len(blob) + _TILE_OVERHEAD
        if size > DECODED_CAP:
            raise _too_big(
                fn, "the pyramid of this feature", size, "gbx_st_asmvt_pyramid"
            )
        out.append({"z": z, "x": x, "y": y, "mvt_bytes": blob})
    return out


# --- the H3 cell rasterizer: the array twin of the aggregate ---------------------------------------------------------
def _cells(cells):
    """(ids, values) of an array of ``(cellid, value)`` structs, the NULL cell ids dropped."""
    rows = [
        (_field(c, "cellid"), _field(c, "value"))
        for c in (cells or ())
        if c is not None and _field(c, "cellid") is not None
    ]
    return [r[0] for r in rows], [r[1] for r in rows]


def _check_cells(fn: str, cells, session_fn: str) -> None:
    size = len(cells or ()) * _CELL_BYTES
    if size > DECODED_CAP:
        raise _too_big(fn, "the array of cells", size, session_fn)


def rst_h3_rasterize(
    cells,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    out_crs,
):
    """gbx_rst_h3_rasterize: the same bytes as the aggregate ``gbx_rst_h3_rasterize_agg`` over the same cells (GeoTIFF,
    Float64, NoData -9999); NULL for an array with no cell."""
    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr
    from databricks.labs.gbx.uc import tier_b

    fn = "gbx_rst_h3_rasterize"
    if cells is None:
        return None
    _check_cells(fn, cells, "gbx_rst_h3_rasterize_agg")
    ids, values = _cells(cells)
    if not ids:
        return None
    if out_crs is not None and str(out_crs).strip():
        out_crs = parse_uc_crs(
            out_crs, fn=fn
        ).code  # a CRS form UC takes, as an authority code
    srid = cr.cell_out_srid(fn, out_srid, out_crs)

    def check_canvas(
        w, h
    ):  # the canvas given or derived from the cells, refused before it is allocated
        tier_b.check_made(
            fn,
            w,
            h,
            ["float64"],
            int(w) * int(h) * 8 * 3,
            session_fn="gbx_rst_h3_rasterize_agg",
        )

    _env.configure_gdal_env()
    ad = cr._adapter("h3")
    vals = [None if v is None or v != v else float(v) for v in values]
    cell_values = cr.max_value_per_cell(
        [int(c) for c in ids], vals, ad.to_key, typed=True
    )
    made = cr.rasterize_cells(
        cell_values,
        [int(c) for c in ids],
        ad,
        "h3",
        srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
        check_canvas=check_canvas,
    )
    if (
        made is not None and len(made) > DECODED_CAP
    ):  # the encoded size: a backstop to the canvas check
        raise _too_big(fn, "the raster", len(made), "gbx_rst_h3_rasterize_agg")
    return made


# --- H3 cell fill: the array twin of the aggregate -----------------------------------------------------------------
def _h3_ring(cell_id: int, d: int) -> List[int]:
    import h3

    return [int(c, 16) for c in h3.grid_ring(h3.int_to_str(cell_id), d)]


def _check_h3_ids(fn: str, ids) -> None:
    """Raise, naming the id and a Fix, for a cell id that is not an H3 cell (the h3 error is the ``__cause__``)."""
    import h3

    for c in ids:
        try:
            h3.get_resolution(h3.int_to_str(int(c)))
        except (ValueError, OverflowError, TypeError) as exc:
            raise _parameter(
                fn,
                f"{c!r} is not a valid H3 cell id ({exc}). Fix: pass cell ids as BIGINT H3 cells, "
                "for example from h3_pointash3",
            ) from exc


def _check_fill_work(fn: str, k: int) -> None:
    """Refuse a cell fill whose work for one NULL cell is over the cap, before any ring is built. A NULL cell looks at
    ``(2k+1)^2`` cells in all, built as hollow rings one at a time, and the output is as long as the input (already
    bounded): this is a bound on that work per NULL cell, not a single-ring memory peak. Call it only when a cell is
    NULL; an all-valid array builds no ring."""
    _check_array(
        fn,
        (2 * k + 1) ** 2,
        "the neighbour cells to look at for one NULL cell",
        lower="lower k, or split the cells into smaller groups",
    )


def h3_cellfill_cells(cells, k, method, power):
    """gbx__h3_cellfill_cells: the cells of ``gbx_h3_cellfill`` as an array of ``(cellid, value)`` structs, in
    ascending unsigned cell id order; a valid cell passes through, a NULL one takes the mean (or the inverse-distance
    mean) of its valid neighbours within ``k`` rings. NULL for an array with no cell."""
    from databricks.labs.gbx.pygx import _cellfill

    fn = "gbx_h3_cellfill"
    if cells is None:
        return None
    _check_cells("gbx__h3_cellfill_cells", cells, "gbx_h3_cellfill")
    ids, values = _cells(cells)
    if not ids:
        return None
    _check_h3_ids(fn, ids)
    ring = 1 if k is None else k
    how = "mean" if method is None else str(method)
    exponent = 2.0 if power is None else float(power)
    collected = _cellfill.collect(
        (int(c), None if v is None or v != v else float(v)) for c, v in zip(ids, values)
    )
    # a negative k is the fill's own error, below; an all-valid array builds no ring, so any k passes
    if int(ring) >= 0 and any(v is None for v in collected.values()):
        _check_fill_work(fn, int(ring))
    try:
        filled = _cellfill.fill(collected, int(ring), how, exponent, _h3_ring)
    except ValueError as exc:
        raise _parameter(
            fn,
            f"{exc}. Fix: pass k of at least 0 and method 'mean' or 'idw'; the session aggregate takes the same",
        ) from exc
    return [{"cellid": c, "value": v} for c, v in filled]


# --- cell fill for the grids with no product ring: the array twin of each aggregate ---------------------------------
def _fill_array(fn, session_fn, cells, k, method, power, ring, parse=None, fmt=None):
    """The shared body of ``gbx_<grid>_cellfill_array``: ``cells`` (an array of ``(cellid, value)`` structs) filled with
    the grid's ring function, as an array of ``{cellid, value}`` in ascending id order. NULL for NULL or for an array with
    no cell id; a wrong k or method raises with a Fix; an array over the cap is refused, and so is a k whose ring
    (``(2k+1)^2`` neighbours of one NULL cell) is over the cap, before any ring is built.
    """
    from databricks.labs.gbx.pygx import _cellfill

    if cells is None:
        return None
    _check_cells(fn, cells, session_fn)
    rows = [(_field(c, "cellid"), _field(c, "value")) for c in cells if c is not None]
    if parse is not None:  # a cell id that is no cell is skipped
        rows = [(parse(c), v) for c, v in rows if c is not None]

    try:
        filled = _cellfill.fill_cells(
            ((c, v) for c, v in rows if c is not None),
            k,
            method,
            power,
            ring,
            before_fill=lambda radius: _check_fill_work(fn, radius),
        )
    except _Refusal:
        raise
    except ValueError as exc:
        raise _parameter(
            fn,
            f"{exc}. Fix: pass k of at least 0 and method 'mean' or 'idw'; the aggregate {session_fn} takes the same",
        ) from exc
    if filled is None:
        return None
    return [{"cellid": fmt(c) if fmt else c, "value": v} for c, v in filled]


def quadbin_cellfill_array(cells, k=1, method="mean", power=2.0):
    """gbx_quadbin_cellfill_array: the filled cells of ``gbx_quadbin_cellfill`` as an array of ``(cellid, value)``."""
    from databricks.labs.gbx.pygx import _quadbin

    return _fill_array(
        "gbx_quadbin_cellfill_array",
        "gbx_quadbin_cellfill",
        cells,
        k,
        method,
        power,
        _quadbin.k_loop,
    )


def bng_cellfill_array(cells, k=1, method="mean", power=2.0):
    """gbx_bng_cellfill_array: the filled cells of ``gbx_bng_cellfill`` as an array of ``(cellid, value)``, the ids as
    BNG strings. A string that is no cell is skipped."""
    from databricks.labs.gbx.pygx import _bng

    return _fill_array(
        "gbx_bng_cellfill_array",
        "gbx_bng_cellfill",
        cells,
        k,
        method,
        power,
        _bng.k_loop,
        parse=lambda c: _bng.parse_safe(str(c)),
        fmt=_bng.format,
    )


def custom_cellfill_array(cells, grid, k=1, method="mean", power=2.0):
    """gbx_custom_cellfill_array: the filled cells of ``gbx_custom_cellfill`` as an array of ``(cellid, value)``."""
    from databricks.labs.gbx.pygx import _custom
    from databricks.labs.gbx.uc.handlers import _custom_conf

    fn = "gbx_custom_cellfill_array"
    if cells is None or grid is None:
        return None
    conf = _custom_conf(fn, grid)
    return _fill_array(
        fn,
        "gbx_custom_cellfill",
        cells,
        k,
        method,
        power,
        lambda cid, d: _custom.k_loop(conf, cid, d),
    )


# --- the Python twin of the chip union: a bench probe only -----------------------------------------------------------------
def bng_chips_union_probe(chips):
    """Probe copy (``gbx_mvp_perf_bng_chips_union``, made by the bench, never by an install): the chips of ONE cell
    folded by the session kernel in one Python call, as EWKB with SRID 27700 (as the SQL fold writes it). It is what ``gbx_bng_cellunion_array`` (a SQL fold) is
    timed against; it has no manifest row and no public name."""
    import shapely

    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.uc.handlers import _chip

    fn = "gbx_bng_cellunion_array"
    if chips is None:
        return None
    acc = None
    for c in chips:
        if c is None:
            continue
        cur = _chip(fn, c)
        acc = cur if acc is None else _bng.cell_union(acc, cur)
    if acc is None:
        return None
    cellid, core, geom = acc
    if (geom is None or geom.is_empty) and core and cellid is not None:
        cid = _bng.parse_safe(cellid)
        geom = None if cid is None else _bng.cell_id_to_geometry(cid)
    if geom is None or geom.is_empty:
        return None
    return shapely.to_wkb(shapely.set_srid(geom, 27700), include_srid=True)


# --- vector tile merge: the array twin of the aggregate ------------------------------------------------------------
def st_asmvt_features(tiles, layer_name, extent):
    """gbx__st_asmvt_features: the one-feature tiles of one tile (the ``mvt_bytes`` of ``gbx_st_asmvt_pyramid``, collected
    per z, x, y) as one layer holding all their features. NULL for an array with no feature.
    """
    import mapbox_vector_tile as mvt
    from shapely.geometry import shape

    from databricks.labs.gbx.pyvx import _mvt

    fn = "gbx_st_asmvt"
    if tiles is None:
        return None
    blobs = [bytes(t) for t in tiles if t is not None]
    size = sum(len(b) + _TILE_OVERHEAD for b in blobs)
    if size > DECODED_CAP:
        raise _too_big("gbx__st_asmvt_features", "the array of tiles", size, fn)
    layer = "layer" if layer_name is None else str(layer_name)
    ext = _mvt.DEFAULT_EXTENT if extent is None else int(extent)
    features = []
    for blob in blobs:
        decoded = mvt.decode(blob, default_options={"y_coord_down": True})
        stray = sorted(set(decoded) - {layer})
        if stray:
            raise _parameter(
                fn,
                f"a tile holds layer {stray[0]!r}, not {layer!r}. Fix: pass the layer name the tiles were made with",
            )
        for f in decoded.get(layer, {}).get("features", ()):
            features.append(
                {"geometry": shape(f["geometry"]), "properties": f.get("properties")}
            )
    if not features:
        return None
    return _mvt.encode_layer(features, layer, ext)


HANDLERS = {
    "gbx__st_asmvt_pyramid_cells": st_asmvt_pyramid_cells,
    "gbx_rst_h3_rasterize": rst_h3_rasterize,
    "gbx__h3_cellfill_cells": h3_cellfill_cells,
    "gbx_quadbin_cellfill_array": quadbin_cellfill_array,
    "gbx_bng_cellfill_array": bng_cellfill_array,
    "gbx_custom_cellfill_array": custom_cellfill_array,
    "gbx__st_asmvt_features": st_asmvt_features,
}
