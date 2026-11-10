"""UC array forms: a table function over a bounded ARRAY scalar, and the array twins of two aggregates.

Import-light, as :mod:`databricks.labs.gbx.uc.handlers`: no pyspark, no pandas, and a kernel is imported when its
handler first runs. Each handler is a thin function over the pure core of the light kernel:

- ``st_asmvt_pyramid_cells`` is the array behind the table function ``gbx_st_asmvt_pyramid``
  (``pyvx._mvt.pyramid_tiles``);
- ``rst_h3_rasterize``, ``rst_quadbin_rasterize``, ``rst_bng_rasterize`` and ``rst_custom_rasterize`` are the
  ``gbx_rst_<grid>_rasterize`` array twins of the cell rasterize aggregates (``pyrx.core.cellraster``, the kernel of the
  session twins);
- ``h3_cellfill_cells`` and ``st_asmvt_features`` are private (``gbx__h3_cellfill_cells``, ``gbx__st_asmvt_features``):
  the array twins of ``gbx_h3_cellfill`` and ``gbx_st_asmvt``, called by the procedures only.

An array row is bounded: the transport carries 256 MiB, but a kernel is only measured up to
:data:`~databricks.labs.gbx.uc.tier_b.DECODED_CAP` decoded, so an input or a result over it is refused with a ``Fix:``
that names the session function, never truncated. NULL in is NULL out; a wrong argument raises; resource exhaustion
and a missing dependency are never data.
"""

from __future__ import annotations

import importlib
import json
from typing import Callable, List, Optional

from databricks.labs.gbx.pyrx.core.budget import decoded_bytes, grid_bytes, size_text
from databricks.labs.gbx.uc.handlers import (
    WGS84_SRID,
    _check_array,
    _field,
    _geom_bytes,
    _Refusal,
    _refuse,
)
from databricks.labs.gbx.uc.tier_b import (
    _CORES,
    DECODED_CAP,
    WINDOWED_DECODED_CAP,
    _Parameter,
    _tile_dict,
    gate,
    raster_of,
    result_refusal,
)

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


def _extent(fn: str, extent) -> int:
    """The tile extent: a whole number of 1 or more, 4096 when NULL. A bad one is a wrong argument, so it raises."""
    from databricks.labs.gbx.pyvx import _mvt

    if extent is None:
        return _mvt.DEFAULT_EXTENT
    try:
        whole = int(extent)
        ok = not isinstance(extent, bool) and whole == extent and whole >= 1
    except (TypeError, ValueError, OverflowError):
        ok = False
    if not ok:
        raise _parameter(
            fn,
            f"extent must be a whole number of 1 or more; got {extent!r}. "
            "Fix: pass an extent such as 4096, the MVT default, or leave it out",
        )
    return whole


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


def _burn_cells(
    fn,
    session_fn,
    grid,
    adapter,
    srid,
    ids,
    values,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    key=True,
):
    """The shared burn of the cell rasterizers: the canvas is refused over the cap before it is allocated, a repeated
    cell keeps its largest value (a NaN never beats a number), and the encoded raster is held to the cap as a backstop.
    ``key`` False hands the cells to the adapter as they are (the custom grid keys by its own ids).
    """
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr
    from databricks.labs.gbx.uc import tier_b

    def check_canvas(
        w, h
    ):  # the canvas given or derived from the cells, refused before it is allocated
        tier_b.check_made(
            fn,
            w,
            h,
            ["float64"],
            int(w) * int(h) * 8 * 3,
            session_fn=session_fn,
        )

    _env.configure_gdal_env()
    vals = [None if v is None or v != v else float(v) for v in values]
    cell_values = cr.max_value_per_cell(
        ids, vals, adapter.to_key if key else None, typed=True
    )
    made = cr.rasterize_cells(
        cell_values,
        ids,
        adapter,
        grid,
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
        raise _too_big(fn, "the raster", len(made), session_fn)
    return made


def _lonlat_rasterize(
    grid,
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
    """gbx_rst_<h3|quadbin>_rasterize: the cells of a lon/lat grid, the output CRS ``out_srid`` or ``out_crs`` (not both,
    else EPSG:4326)."""
    from databricks.labs.gbx.core.crs import parse_uc_crs
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    fn = f"gbx_rst_{grid}_rasterize"
    session_fn = f"{fn}_agg"
    if cells is None:
        return None
    _check_cells(fn, cells, session_fn)
    ids, values = _cells(cells)
    if not ids:
        return None
    if out_crs is not None and str(out_crs).strip():
        out_crs = parse_uc_crs(
            out_crs, fn=fn
        ).code  # a CRS form UC takes, as an authority code
    srid = cr.cell_out_srid(fn, out_srid, out_crs)
    return _burn_cells(
        fn,
        session_fn,
        grid,
        cr._adapter(grid),
        srid,
        [int(c) for c in ids],
        values,
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
    return _lonlat_rasterize(
        "h3",
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
    )


def rst_quadbin_rasterize(
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
    """gbx_rst_quadbin_rasterize: the same bytes as ``gbx_rst_quadbin_rasterize_agg`` over the same cells; NULL for an
    array with no cell."""
    return _lonlat_rasterize(
        "quadbin",
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
    )


def rst_bng_rasterize(
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
    out_crs=None,
):
    """gbx_rst_bng_rasterize: the same bytes as ``gbx_rst_bng_rasterize_agg`` over the same cells. The output is always
    EPSG:27700 (``out_srid`` is not read) and a non-NULL ``out_crs`` is refused; NULL for an array with no cell.
    """
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    fn = "gbx_rst_bng_rasterize"
    if cells is None:
        return None
    if out_crs is not None:
        raise _parameter(
            fn,
            "out_crs is not supported; the output is always EPSG:27700. "
            "Fix: pass NULL for out_crs, then reproject the tile with gbx_rst_transformcrs.",
        )
    _check_cells(fn, cells, "gbx_rst_bng_rasterize_agg")
    ids, values = _cells(cells)
    if not ids:
        return None
    return _burn_cells(
        fn,
        "gbx_rst_bng_rasterize_agg",
        "bng",
        cr._adapter("bng"),
        27700,
        [str(c) for c in ids],
        values,
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


def rst_custom_rasterize(
    cells,
    grid,
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
):
    """gbx_rst_custom_rasterize: the same bytes as ``gbx_rst_custom_rasterize_agg`` over the same cells. The output CRS
    is ``out_srid``, else the grid's own; a grid with none needs ``out_srid``. NULL for an array with no cell.
    """
    from databricks.labs.gbx.pyrx.core import cellraster as cr
    from databricks.labs.gbx.uc.handlers import _custom_conf

    fn = "gbx_rst_custom_rasterize"
    if cells is None or grid is None:
        return None
    _check_cells(fn, cells, "gbx_rst_custom_rasterize_agg")
    ids, values = _cells(cells)
    if not ids:
        return None
    conf = _custom_conf(fn, grid)
    if out_srid is not None:
        srid = int(out_srid)
    elif conf.srid != -1:
        srid = conf.srid
    else:
        raise _parameter(
            fn, "the grid has no CRS (srid=-1). Fix: supply out_srid explicitly."
        )
    return _burn_cells(
        fn,
        "gbx_rst_custom_rasterize_agg",
        "custom",
        cr._CustomAdapter(conf, fallback_srid=srid),
        srid,
        [int(c) for c in ids],
        values,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
        key=False,
    )


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
    """Probe copy (``gbx_probe_perf_bng_chips_union``, made by the bench, never by an install): the chips of ONE cell
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


# --- vector tile layer and coverage checks: the array twins of three aggregates ----------------------------------------
def _gap_width(fn: str, gap_width) -> float:
    import math

    gw = 0.0 if gap_width is None else float(gap_width)
    if not math.isfinite(gw) or gw < 0.0:
        raise _parameter(
            fn,
            f"gap_width must be a finite number, 0 or more; got {gap_width!r}. "
            "Fix: pass a gap_width of 0 or more, in the units of the polygons' CRS",
        )
    return gw


def _coverage(fn: str, session_fn: str, geoms, gap_width, kernel):
    from shapely.errors import ShapelyError

    from databricks.labs.gbx.pyvx import _coverage as cov

    if geoms is None:
        return None
    gw = _gap_width(fn, gap_width)
    members = [bytes(g) for g in geoms if g is not None]
    size = sum(len(m) + _TILE_OVERHEAD for m in members)  # before any member is parsed
    if size > DECODED_CAP:
        raise _too_big(fn, "the array of polygons", size, session_fn)
    try:
        return getattr(cov, kernel)(members, gw)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        ValueError,
        ShapelyError,
    ):  # a member that is no polygon: unreadable data is NULL on UC
        return None


def st_coverageisvalid_array(geoms, gap_width=None):
    """gbx_st_coverageisvalid_array: the aggregate's answer over an array of WKB polygons."""
    return _coverage(
        "gbx_st_coverageisvalid_array",
        "gbx_st_coverageisvalid",
        geoms,
        gap_width,
        "coverage_is_valid_agg",
    )


def st_coverageinvalidedges_array(geoms, gap_width=None):
    """gbx_st_coverageinvalidedges_array: the aggregate's edges (WKB, EWKB with an SRID) over an array of WKB polygons."""
    return _coverage(
        "gbx_st_coverageinvalidedges_array",
        "gbx_st_coverageinvalidedges",
        geoms,
        gap_width,
        "coverage_invalid_edges_agg",
    )


def st_asmvt_array(features, layer_name, extent=None):
    """gbx_st_asmvt_array: one MVT layer from an array of ``(geom, attrs)`` structs, the layer of the aggregate
    ``gbx_st_asmvt``. ``geom`` is WKB in tile coordinates, ``attrs`` a JSON object text (NULL for none).
    """
    from shapely.errors import ShapelyError

    from databricks.labs.gbx.pyvx import _mvt

    fn = "gbx_st_asmvt_array"
    if features is None or layer_name is None:
        return None
    ext = _extent(fn, extent)  # a wrong argument raises; only unreadable data is NULL
    rows = [(_field(r, "geom"), _field(r, "attrs")) for r in features if r is not None]
    # the attribute text travels with its feature, so it counts toward the cap; checked before any geometry is parsed
    size = sum(len(g or b"") + len(a or "") + _TILE_OVERHEAD for g, a in rows)
    if size > DECODED_CAP:
        raise _too_big(fn, "the array of features", size, "gbx_st_asmvt")
    feats = [
        {"geometry": bytes(g), "properties": _attrs(fn, a)}
        for g, a in rows
        if g is not None
    ]
    try:
        return _mvt.encode_layer(feats, str(layer_name), ext)
    except (MemoryError, RecursionError, ImportError):
        raise
    except (
        ValueError,
        ShapelyError,
    ):  # a geometry that is no geometry: unreadable data is NULL on UC
        return None


# --- the retile family: one tile cut into an array of tiles -------------------------------------------------------
# The window readers (retile, maketiles, tooverlappingtiles) take a tile decoded up to WINDOWED_DECODED_CAP (256 MiB), the
# separatebands routine keeps DECODED_CAP (64 MiB) because it decodes a whole band at once. A window the readers cut is
# held to DECODED_CAP whatever the input, so no step reads more than a pixel function does.
# The private routines behind gbx_rst_retile, gbx_rst_maketiles, gbx_rst_tooverlappingtiles and gbx_rst_separatebands:
# each returns the sub-tiles of one tile as an array, and the table function turns the array into rows. Three bounds
# keep the work and the array under the cap:
#   1. the input, decoded, is under the cap (a refusal that names the session, where the tile is read in windows);
#   2. the array is estimated BEFORE a window is cut, from the window arithmetic alone (the count of tiles and the sum of
#      their pixels, `tiling.window_stats`): the decoded bytes of every tile with 1% for the codec, and per tile the
#      measured file header and the row. It never falls under the bytes the kernel makes, so an array over the cap is
#      refused with no window cut; the price is that it is an upper bound and can refuse an array that would have fitted;
#   3. each tile made is counted again as it is cut (the bytes so far against the cap) and checked against the cap
#      decoded, so a tile or an array the estimate let through is still never returned over the cap.
_TILE_FILE_BYTES = 4096  # a tile's TIFF header: 0.4 KiB for one band, 3.4 KiB for 500 (measured), rounded up
_TILE_BAND_BYTES = 32  # and per band of the tile
_TILE_ROW_BYTES = 512  # the row around the bytes: the other fields and the metadata map
PROBE_DECODED_CAP = (
    512 * 1024 * 1024
)  # what a bench probe copy decodes and returns (see rst_retile_tiles_probe)


def tiles_estimate(tiles: int, decoded: int, bands: int) -> int:
    """An upper bound of the bytes of an array of ``tiles`` tiles of ``bands`` bands each that hold ``decoded`` bytes of
    pixels between them: the pixels with 1% for the codec (zstd adds under 0.4% to data it cannot compress), plus a header
    and a row for every tile."""
    return -(-decoded * 101 // 100) + tiles * (
        _TILE_FILE_BYTES + _TILE_BAND_BYTES * bands + _TILE_ROW_BYTES
    )


def _window_dict(window) -> Optional[dict]:
    if window is None:
        return None
    keys = ("col_off", "row_off", "width", "height")
    if isinstance(window, (tuple, list)):
        return dict(zip(keys, (int(v) for v in window)))
    values = [_field(window, k) for k in keys]
    return None if any(v is None for v in values) else dict(zip(keys, values))


def _sub_tile(
    fn: str,
    tile,
    raster: bytes,
    cap: int,
    window=None,
    band=None,
    whole=False,
    cell=None,
):
    """The tile row of one piece of ``tile``: the session's fields (where it was cut from in ``window``, the band marker),
    with the CRS as an SRID as every UC tile has it. The one piece that covers its whole input IS the input, so it keeps the
    input's own provenance. A piece decoded over ``cap`` is refused with a Fix, never returned.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.core.crs import authority_srid_of

    with MemoryFile(raster) as mf, open_bytes(mf) as ds:
        decoded = decoded_bytes(ds)
        if decoded > cap:
            raise result_refusal(
                fn,
                decoded,
                cap,
                f"run {fn} in a notebook or job session, or ask for smaller tiles",
            )
        meta = {
            "driver": "GTiff",
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
        srid = authority_srid_of(ds.crs)
    parent_band = (_field(tile, "metadata") or {}).get("band")
    if band is not None:
        meta["band"] = str(band)
    elif parent_band is not None:
        meta["band"] = parent_band
    if (
        cell is not None
    ):  # a cell of a grid: its own id, and the grid it belongs to (as the session stamps both)
        meta["gridSystem"] = cell[1]
    row = _tile_dict(tile, raster, srid, meta)
    if cell is not None:
        row["cellid"] = int(cell[0])
    if whole:
        row["path"] = _field(tile, "path")
        row["window"] = _window_dict(_field(tile, "window"))
        row["clip_polygon"] = _field(tile, "clip_polygon")
        row["clip_crs"] = _field(tile, "clip_crs")
    else:
        row["window"] = _window_dict(window)
    return row


def _tile_array(
    fn: str,
    tile,
    cap: int,
    plan: Callable,
    lower: str,
    smaller: str = "pass a smaller tile size",
    piece_cap: Optional[int] = None,
    reads: str = "which reads the tile in windows",
):
    """The pieces of ``tile`` as an array of tile rows, under the three bounds above. NULL for a NULL tile and for data
    that cannot be read (UC has no error tile); an error tile goes out as itself; a virtual tile is refused.

    ``plan(ds, raster)`` returns ``(tiles, pixel_bytes, bands, whole, pieces, largest)``: the count of tiles, the decoded
    bytes of all of them, the bands of each, whether the one tile is the whole input, a function ``pieces(ds)`` that yields
    ``(window, band, raster bytes)`` one piece at a time (a cell of a grid adds a fourth item, ``(cell id, grid)``), and
    the decoded bytes of the largest piece. A plan that cannot count its tiles before it cuts them (a tessellation)
    returns 0 tiles and 0 pixel bytes: the running total then is the only bound on the array. The input and the
    array are held to ``cap``; a piece, which is what is read at once, to ``piece_cap`` (``cap`` or the pixel cap
    :data:`DECODED_CAP`, the lower), refused from the arithmetic before a window is cut.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.uc.handlers import _check_bytes

    done, early = gate(
        fn, tile
    )  # an error tile passes through, a virtual tile is refused
    if done:
        return None if early is None else [early]
    raster = raster_of(fn, tile)
    try:
        with MemoryFile(raster) as mf, open_bytes(mf) as ds:
            decoded = decoded_bytes(ds)
            if decoded > cap:
                raise _Refusal(
                    f"{fn}: the tile is {size_text(decoded)} decoded, over the {size_text(cap)} a Unity Catalog "
                    f"function decodes. Fix: run {fn} in a notebook or job session, {reads}"
                )
            tiles, pixel_bytes, bands, whole, pieces, largest = plan(ds, raster)
            at_once = min(cap, DECODED_CAP) if piece_cap is None else piece_cap
            if largest > at_once:
                raise _Refusal(
                    f"{fn}: a window of {size_text(largest)} decoded would be read at once, over the {size_text(at_once)} "
                    f"a Unity Catalog function holds. Fix: {smaller}, or run {fn} in a notebook or job session"
                )
            _check_bytes(
                fn,
                tiles_estimate(tiles, pixel_bytes, bands),
                "the tiles it returns (an upper estimate)",
                lower=lower,
                cap=cap,
            )
            out, total = [], 0
            for window, band, data, *cell in pieces(ds):
                out.append(
                    _sub_tile(fn, tile, data, at_once, window, band, whole, *cell)
                )
                total += len(data) + _TILE_ROW_BYTES
                _check_bytes(fn, total, "the tiles it returns", lower=lower, cap=cap)
            return out
    except (MemoryError, RecursionError, ImportError, _Refusal, _Parameter):
        raise
    except Exception:  # noqa: BLE001 - unreadable data is NULL on UC (no error tile)
        return None


def _window_plan(windows: Callable, tile_size: Callable, overlap: int = 0) -> Callable:
    """The plan of a function that cuts windows: ``tile_size(ds, raster)`` gives the tile width and height; ``windows`` is
    the core's located iterator over them."""
    tiling = importlib.import_module(f"{_CORES}.tiling")

    def plan(ds, raster):
        tw, th = tile_size(ds, raster)
        tiles, pixels = tiling.window_stats(ds.width, ds.height, tw, th, overlap)
        return (
            tiles,
            pixels * grid_bytes(1, 1, ds.dtypes),
            ds.count,
            tiling.covers_whole_input(ds, tw, th, overlap),
            lambda d: ((win, None, b) for win, b in windows(d, tw, th)),
            min(tw, ds.width) * min(th, ds.height) * grid_bytes(1, 1, ds.dtypes),
        )

    return plan


def _retile_tiles(tile, tile_width, tile_height, cap, piece_cap=None):
    fn = "gbx_rst_retile"
    tiling = importlib.import_module(f"{_CORES}.tiling")
    tiling.require_window(tile_width, tile_height, fn)
    if tile is None or tile_width is None or tile_height is None:
        return None
    w, h = int(tile_width), int(tile_height)
    return _tile_array(
        fn,
        tile,
        cap,
        _window_plan(tiling.iter_retile_located, lambda ds, raster: (w, h)),
        "pass a larger tile_width and tile_height",
        "pass a smaller tile_width and tile_height",
        piece_cap,
    )


def rst_retile_tiles(tile, tile_width, tile_height):
    """gbx__rst_retile_tiles: the non-overlapping windows of a tile as an array of tiles, in the session's order (rows, then
    columns; the edge tiles are smaller). A window of under one pixel raises with a Fix, also for a tile with nothing to read.
    """
    return _retile_tiles(tile, tile_width, tile_height, WINDOWED_DECODED_CAP)


def rst_retile_tiles_probe(tile, tile_width, tile_height):
    """The bench probe copy of :func:`rst_retile_tiles`: the same function with the cap raised to :data:`PROBE_DECODED_CAP`
    for the input, each tile and the array, so a timing can measure what the product function refuses. Created only under
    a ``gbx_probe_perf_`` name; never a product routine."""
    return _retile_tiles(
        tile, tile_width, tile_height, PROBE_DECODED_CAP, PROBE_DECODED_CAP
    )


def rst_tooverlappingtiles_tiles(tile, tile_width, tile_height, overlap):
    """gbx__rst_tooverlappingtiles_tiles: the overlapping windows of a tile (each edge overlaps by ``overlap`` percent of the
    tile size, rounded up) as an array of tiles, in the session's order. A window under one pixel, or an overlap outside 0 to
    below 100, raises with a Fix."""
    fn = "gbx_rst_tooverlappingtiles"
    tiling = importlib.import_module(f"{_CORES}.tiling")
    tiling.require_window(tile_width, tile_height, fn)
    ov = tiling.require_overlap(overlap, fn)
    if tile is None or tile_width is None or tile_height is None or ov is None:
        return None
    w, h = int(tile_width), int(tile_height)
    return _tile_array(
        fn,
        tile,
        WINDOWED_DECODED_CAP,
        _window_plan(
            lambda ds, tw, th: tiling.iter_to_overlapping_tiles_located(ds, tw, th, ov),
            lambda ds, raster: (w, h),
            ov,
        ),
        "pass a larger tile size or a smaller overlap",
        "pass a smaller tile size",
    )


def rst_maketiles_tiles(tile, size_in_mb):
    """gbx__rst_maketiles_tiles: the tile cut into the power-of-4 grid (1, 4, 16 ... at most 256 tiles) whose parts each fit
    ``size_in_mb`` megabytes of the tile's own bytes; 0 or less does not cut. The grid is the session's.
    """
    fn = "gbx_rst_maketiles"
    tiling = importlib.import_module(f"{_CORES}.tiling")
    if tile is None or size_in_mb is None:
        return None
    mb = int(size_in_mb)
    return _tile_array(
        fn,
        tile,
        WINDOWED_DECODED_CAP,
        _window_plan(
            tiling.iter_retile_located,
            lambda ds, raster: tiling.make_tiles_size(ds, mb, size_bytes=len(raster)),
        ),
        "pass a larger size_in_mb",
        "pass a smaller size_in_mb",
    )


def rst_separatebands_tiles(tile):
    """gbx__rst_separatebands_tiles: one single-band tile per band, in band order, each marked with its band number in its
    metadata."""
    fn = "gbx_rst_separatebands"
    tiling = importlib.import_module(f"{_CORES}.tiling")
    if tile is None:
        return None

    def plan(ds, raster):
        return (
            ds.count,
            decoded_bytes(ds),
            1,
            False,
            lambda d: (
                (None, band, b) for band, b in tiling.iter_separate_bands_indexed(d)
            ),
            decoded_bytes(ds) // max(1, ds.count),
        )

    return _tile_array(
        fn, tile, DECODED_CAP, plan, "split the raster by band before it is a tile"
    )


# --- the grid tessellations: one tile cut into the tile of each cell it covers --------------------------------------
# gbx__rst_<grid>_tessellate_tiles return the session's cells as an array of tiles, in the kernel's order, each with the
# cell id (a BNG id as its digit id) and the grid in its metadata. A tessellation is NOT a window reader: it holds a label
# raster of 4 bytes a pixel for the whole tile (and the BNG route warps the tile whole), so its input is held to DECODED_CAP,
# the 64 MiB of a pixel function, where the retile family takes 256 MiB. The tiles of the cells cannot be counted before
# they are cut (a covering cell overlaps its neighbours), so the running total of the tiles made is the only bound on the
# array: each is checked against the cap as it is cut and a tile past it stops the work.
_ASSIGNMENTS = ("covering", "centroid")
_COVERAGES = ("sparse", "complete")


def _tessellate_words(fn: str, assignment, coverage):
    """The assignment and coverage words (the session's defaults for NULL); another word is a wrong argument."""
    a = "centroid" if assignment is None else assignment
    c = "complete" if coverage is None else coverage
    if a not in _ASSIGNMENTS:
        raise _parameter(fn, f"assignment must be one of covering, centroid; got '{a}'")
    if c not in _COVERAGES:
        raise _parameter(fn, f"coverage must be one of sparse, complete; got '{c}'")
    return a, c


def _tessellate_tiles(grid, tile, resolution, assignment, coverage, spec=None):
    from databricks.labs.gbx.uc.handlers import _custom_conf, _res

    fn = f"gbx_rst_{grid}_tessellate"
    if tile is None or resolution is None or (grid == "custom" and spec is None):
        return None
    tess = importlib.import_module(f"{_CORES}.tessellate")
    a, c = _tessellate_words(fn, assignment, coverage)
    conf = None
    if grid == "custom":
        conf = _custom_conf(fn, spec)
        res = int(resolution)
        if not 0 <= res <= conf.max_resolution:
            raise _parameter(
                fn,
                f"resolution must be in [0, {conf.max_resolution}] for this grid; got {res}. "
                "Fix: pass a resolution the grid defines",
            )
    elif grid == "bng":
        res = _res(
            resolution
        )  # an index or a size such as '1km'; one BNG does not define raises
    else:
        try:
            res = tess._resolve_resolution(resolution, grid)
        except ValueError as exc:
            raise _parameter(
                fn, f"{exc}. Fix: pass a resolution the grid defines"
            ) from exc

    def plan(ds, raster):
        def pieces(d):
            for cell, data in tess.iter_tessellate(
                d, res, grid, mode=a, coverage=c, conf=conf
            ):
                if data is None:  # defensive: never a tile with no raster
                    continue
                if grid == "bng":
                    from databricks.labs.gbx.pygx import _bng

                    cell = _bng.parse(cell)
                yield None, None, data, (cell, grid)

        return 0, 0, ds.count, False, pieces, decoded_bytes(ds)

    return _tile_array(
        fn,
        tile,
        DECODED_CAP,
        plan,
        "use a coarser resolution or retile first",
        "retile first (gbx_rst_retile) so each tile is smaller",
        reads="or retile first (gbx_rst_retile) so each tile is smaller",
    )


def rst_h3_tessellate_tiles(tile, resolution, assignment, coverage):
    """gbx__rst_h3_tessellate_tiles: the tile of every H3 cell the tile covers, as an array (the session's cells)."""
    return _tessellate_tiles("h3", tile, resolution, assignment, coverage)


def rst_quadbin_tessellate_tiles(tile, resolution, assignment, coverage):
    """gbx__rst_quadbin_tessellate_tiles: the tile of every quadbin cell the tile covers, as an array."""
    return _tessellate_tiles("quadbin", tile, resolution, assignment, coverage)


def rst_bng_tessellate_tiles(tile, resolution, assignment, coverage):
    """gbx__rst_bng_tessellate_tiles: the tile of every BNG cell the tile covers, as an array. The resolution is an index
    or a size such as '1km'; the tile is warped to EPSG:27700 first when it is in another CRS.
    """
    return _tessellate_tiles("bng", tile, resolution, assignment, coverage)


def rst_custom_tessellate_tiles(tile, grid, resolution, assignment, coverage):
    """gbx__rst_custom_tessellate_tiles: the tile of every cell of the custom grid ``grid`` the tile covers, as an array."""
    return _tessellate_tiles("custom", tile, resolution, assignment, coverage, grid)


# --- the XYZ pyramid: one tile rendered into the web tiles it touches -----------------------------------------------
# The pyramid renders through the kernel of gbx_rst_tilexyz, which UC reads under the 64 MiB pixel cap and the render-size
# cap, so it keeps both. The tile count is not known to be small: the tiles are counted as they are rendered and the
# running total is held to the cap, so a pyramid over it stops there (the session's own guard, 1,000,000 tiles, still
# applies first).
def rst_xyzpyramid_tiles(tile, min_z, max_z, format, size, resampling, rescale):
    """gbx__rst_xyzpyramid_tiles: the ``(z, x, y, bytes)`` of every web tile the tile touches across the zoom range, as an
    array, in the session's order. A NULL, error or unreadable tile has no tiles; a virtual tile is refused.
    """
    import contextlib

    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.uc.tier_b import check_made

    fn = "gbx_rst_xyzpyramid"
    xyzc = importlib.import_module(f"{_CORES}.xyz")
    if min_z is None or max_z is None:
        return None
    try:
        fmt, sz, resamp, rsc = xyzc.render_args(format, size, resampling, rescale)
        lo, hi = xyzc._validate_zoom_range(min_z, max_z)
    except ValueError as exc:
        raise _Parameter(
            f"{exc}. Fix: use a format of PNG, JPEG or WEBP, a size in (0, 4096], a GDAL resampling word, a rescale of "
            "'auto', 'none' or 'min,max', and 0 <= min_z <= max_z <= 20"
        ) from exc
    if tile is None:
        return None
    raster = raster_of(fn, tile)
    if raster is None:
        return None
    with contextlib.ExitStack() as stack:
        try:
            ds = stack.enter_context(
                open_bytes(stack.enter_context(MemoryFile(raster)))
            )
        except (MemoryError, RecursionError, ImportError):
            raise
        except (
            Exception
        ):  # noqa: BLE001 - bytes GDAL cannot open: no tiles, as the session
            return None
        decoded = decoded_bytes(ds)
        if decoded > DECODED_CAP:
            raise _Refusal(
                f"{fn}: the tile is {size_text(decoded)} decoded, over the {size_text(DECODED_CAP)} a Unity Catalog "
                f"function decodes. Fix: run {fn} in a notebook or job session, or retile first (gbx_rst_retile) so "
                "each tile is smaller"
            )
        check_made(
            fn,
            sz,
            sz,
            ["uint8"] * 4,
            xyzc.render_working_bytes(sz, ds.count),
            ask="a smaller size",
        )
        out, total = [], 0
        for z, x, y, blob in xyzc.iter_pyramid(ds, lo, hi, fmt, sz, resamp, rsc):
            total += len(blob) + _TILE_OVERHEAD
            if total > DECODED_CAP:
                raise _Refusal(
                    f"{fn}: the pyramid is {size_text(total)} so far, over the {size_text(DECODED_CAP)} a Unity "
                    f"Catalog function returns. Fix: lower max_z, pass a smaller size, or run {fn} in a notebook "
                    "or job session"
                )
            out.append({"z": z, "x": x, "y": y, "bytes": blob})
        return out


# --- the TIN functions: a surface from points and break lines ---------------------------------------------------------
# gbx__st_triangulate_rows, gbx__st_interpolateelevationbbox_rows and gbx__st_interpolateelevationgeom_rows return the
# session's rows as an array of WKB (a triangle as 2D WKB, an elevation point as ISO WKB with its Z: the warehouse parses
# type 1001 and refuses the EWKB Z flag). The points and break lines are ARRAY<GEOMETRY(ANY)>, whose Geometry values keep
# their Z; the SRID is not carried in the bytes, as the session's Python level leaves it. The arrays are bounded twice:
# the points and break lines held (their WKB bytes) and the rows returned, the rows estimated BEFORE the surface is built
# (at most two triangles per point; one point per grid cell) and counted again as they are made.
_ROW_OVERHEAD = 32  # what an array element carries beside its bytes
_TRIANGLE_ROW_BYTES = (
    73 + _ROW_OVERHEAD
)  # the 2D WKB of a triangle: one ring of four vertices
_POINT_ROW_BYTES = 29 + _ROW_OVERHEAD  # the WKB of a point with Z


def _tin_mode(fn: str, mode) -> None:
    """The mode: 'constrained' (the default) or 'conforming'; any other word is wrong."""
    m = (mode or "constrained").lower()
    if m not in ("constrained", "conforming"):
        raise _parameter(
            fn, f"mode must be 'constrained' or 'conforming'; got {mode!r}"
        )


def _tin_finder(fn: str, split_point_finder) -> None:
    """'NONENCROACHING' or 'MIDPOINT', in any case; any other word is wrong (in both modes, as in the session)."""
    from databricks.labs.gbx.pyvx import _tin

    try:
        _tin.validate_finder(split_point_finder)
    except ValueError as exc:
        raise _parameter(fn, str(exc)) from None


def _tin_rows(fn: str, rows, what: str):
    """The rows within the cap; a conforming triangulation that gives up raises with the function's name and its Fix."""
    from databricks.labs.gbx.pyvx._conforming import ConformingError

    try:
        return _rows_within_cap(fn, rows, what)
    except ConformingError as exc:
        raise _parameter(fn, str(exc)) from None


def _tin_sites(fn: str, points, breaklines):
    """``(coords, break lines)`` of the two Geometry arrays, once their WKB fits the cap."""
    from databricks.labs.gbx.pyvx import _tin
    from databricks.labs.gbx.uc.tier_b import _wkb_array

    pts, bls = _wkb_array(points), _wkb_array(breaklines)
    held = sum(len(b) for b in (pts or []) if b is not None) + sum(
        len(b) for b in (bls or []) if b is not None
    )
    if held > DECODED_CAP:
        raise _too_big(fn, "the points and break lines", held, fn)
    return _tin.sites(pts, bls)


def _rows_within_cap(fn: str, rows, what: str):
    """The rows of a generator as a list, each counted as it is made against the cap."""
    out, total = [], 0
    for blob in rows:
        total += len(blob) + _ROW_OVERHEAD
        if total > DECODED_CAP:
            raise _too_big(fn, what, total, fn)
        out.append(blob)
    return out


def st_triangulate_rows(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    split_point_finder,
    mode,
):
    """gbx__st_triangulate_rows: the 2D WKB triangles of the triangulation of the points and break lines (either mode),
    as an array, in the session's order. NULL points, an empty array or a NULL tolerance give no rows.
    """
    from databricks.labs.gbx.pyvx import _tin
    from databricks.labs.gbx.uc.handlers import _check_bytes

    fn = "gbx_st_triangulate"
    _tin_mode(fn, mode)
    if (
        points_array is None
        or merge_tolerance is None
        or snap_tolerance is None
        or split_point_finder is None
    ):
        return None
    _tin_finder(fn, split_point_finder)
    parsed = _tin_sites(fn, points_array, breaklines_array)
    coords, bls = parsed
    if coords is None:
        return None
    sites = len(coords) + sum(len(b) for b in bls)
    _check_bytes(
        fn,
        2 * sites * _TRIANGLE_ROW_BYTES,
        "the triangles it returns (an upper estimate, two for every point)",
        lower="pass fewer points",
        cap=DECODED_CAP,
    )
    return _tin_rows(
        fn,
        _tin.triangle_wkbs(
            None,
            None,
            merge_tolerance,
            snap_tolerance,
            mode or "constrained",
            split_point_finder,
            parsed=parsed,
        ),
        "the triangles it returns",
    )


def _elevation_rows(fn, points, breaklines, mt, st, spf, mode, cells, cell_iter, srid):
    from databricks.labs.gbx.pyvx import _tin
    from databricks.labs.gbx.uc.handlers import _check_bytes

    _tin_mode(fn, mode)
    if mt is None or st is None or spf is None:
        return None
    _tin_finder(fn, spf)
    _check_bytes(
        fn,
        cells * _POINT_ROW_BYTES,
        "the points it returns (an upper estimate, one for every grid cell)",
        lower="pass a coarser grid",
        cap=DECODED_CAP,
    )
    if points is None:
        return None
    parsed = _tin_sites(fn, points, breaklines)
    if parsed[0] is None:
        return None
    return _tin_rows(
        fn,
        _tin.elevation_wkbs(
            None,
            None,
            mt,
            st,
            cell_iter,
            srid,
            flavor="iso",
            parsed=parsed,
            mode=mode or "constrained",
            split_point_finder=spf,
        ),
        "the points it returns",
    )


def st_interpolateelevationbbox_rows(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    split_point_finder,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
    mode,
):
    """gbx__st_interpolateelevationbbox_rows: the ISO WKB point (with Z) at the centre of every cell of the bounding-box
    grid that is inside the surface, as an array, in the session's order (column by column).
    """
    from databricks.labs.gbx.pyvx import _tin

    fn = "gbx_st_interpolateelevationbbox"
    if None in (xmin, ymin, xmax, ymax, width_px, height_px, srid):
        _tin_mode(fn, mode)
        return None
    cells = max(0, int(width_px)) * max(0, int(height_px))
    return _elevation_rows(
        fn,
        points_array,
        breaklines_array,
        merge_tolerance,
        snap_tolerance,
        split_point_finder,
        mode,
        cells,
        _tin.grid_bbox(
            float(xmin),
            float(ymin),
            float(xmax),
            float(ymax),
            int(width_px),
            int(height_px),
        ),
        int(srid),
    )


def st_interpolateelevationgeom_rows(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    split_point_finder,
    grid_origin,
    grid_cols,
    grid_rows,
    cell_size_x,
    cell_size_y,
    mode,
):
    """gbx__st_interpolateelevationgeom_rows: as the bounding-box form, for a grid anchored at a POINT (its north-west
    corner; a negative cell height steps south)."""
    import shapely

    from databricks.labs.gbx.pyvx import _tin

    fn = "gbx_st_interpolateelevationgeom"
    if None in (grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y):
        _tin_mode(fn, mode)
        return None
    origin = shapely.from_wkb(_geom_bytes(grid_origin))
    cells = max(0, int(grid_cols)) * max(0, int(grid_rows))
    return _elevation_rows(
        fn,
        points_array,
        breaklines_array,
        merge_tolerance,
        snap_tolerance,
        split_point_finder,
        mode,
        cells,
        _tin.grid_geom(
            origin.x,
            origin.y,
            int(grid_cols),
            int(grid_rows),
            float(cell_size_x),
            float(cell_size_y),
        ),
        shapely.get_srid(origin),
    )


HANDLERS = {
    "gbx_st_coverageisvalid_array": st_coverageisvalid_array,
    "gbx_st_coverageinvalidedges_array": st_coverageinvalidedges_array,
    "gbx_st_asmvt_array": st_asmvt_array,
    "gbx__st_asmvt_pyramid_cells": st_asmvt_pyramid_cells,
    "gbx_rst_h3_rasterize": rst_h3_rasterize,
    "gbx_rst_quadbin_rasterize": rst_quadbin_rasterize,
    "gbx_rst_bng_rasterize": rst_bng_rasterize,
    "gbx_rst_custom_rasterize": rst_custom_rasterize,
    "gbx__h3_cellfill_cells": h3_cellfill_cells,
    "gbx_quadbin_cellfill_array": quadbin_cellfill_array,
    "gbx_bng_cellfill_array": bng_cellfill_array,
    "gbx_custom_cellfill_array": custom_cellfill_array,
    "gbx__st_asmvt_features": st_asmvt_features,
    "gbx__rst_retile_tiles": rst_retile_tiles,
    "gbx__rst_maketiles_tiles": rst_maketiles_tiles,
    "gbx__rst_tooverlappingtiles_tiles": rst_tooverlappingtiles_tiles,
    "gbx__rst_separatebands_tiles": rst_separatebands_tiles,
    "gbx__rst_h3_tessellate_tiles": rst_h3_tessellate_tiles,
    "gbx__rst_quadbin_tessellate_tiles": rst_quadbin_tessellate_tiles,
    "gbx__rst_bng_tessellate_tiles": rst_bng_tessellate_tiles,
    "gbx__rst_custom_tessellate_tiles": rst_custom_tessellate_tiles,
    "gbx__rst_xyzpyramid_tiles": rst_xyzpyramid_tiles,
    "gbx__st_triangulate_rows": st_triangulate_rows,
    "gbx__st_interpolateelevationbbox_rows": st_interpolateelevationbbox_rows,
    "gbx__st_interpolateelevationgeom_rows": st_interpolateelevationgeom_rows,
}


# the private routines of raster to grid (one per grid and statistic) live in uc.rastertogrid
from databricks.labs.gbx.uc import rastertogrid as _rastertogrid  # noqa: E402

HANDLERS.update(
    {k: v for k, v in _rastertogrid.HANDLERS.items() if k.startswith("gbx__")}
)
