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
    if all(a is not None for a in (xmin, ymin, xmax, ymax, width, height)):
        tier_b.check_made(
            fn, width, height, ["float64"], int(width) * int(height) * 8 * 3
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
    )
    if made is not None and len(made) > DECODED_CAP:
        raise _too_big(fn, "the raster", len(made), "gbx_rst_h3_rasterize")
    return made


# --- H3 cell fill: the array twin of the aggregate -----------------------------------------------------------------
def _h3_ring(cell_id: int, d: int) -> List[int]:
    import h3

    return [int(c, 16) for c in h3.grid_ring(h3.int_to_str(cell_id), d)]


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
    ring = 1 if k is None else k
    how = "mean" if method is None else str(method)
    exponent = 2.0 if power is None else float(power)
    collected = _cellfill.collect(
        (int(c), None if v is None or v != v else float(v)) for c, v in zip(ids, values)
    )
    try:
        filled = _cellfill.fill(collected, int(ring), how, exponent, _h3_ring)
    except ValueError as exc:
        raise _parameter(
            fn,
            f"{exc}. Fix: pass k of at least 0 and method 'mean' or 'idw'; the session aggregate takes the same",
        ) from exc
    return [{"cellid": c, "value": v} for c, v in filled]


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
    "gbx__st_asmvt_features": st_asmvt_features,
}
