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


# --- the retile family: one tile cut into an array of tiles -------------------------------------------------------
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
    fn: str, tile, raster: bytes, cap: int, window=None, band=None, whole=False
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
    row = _tile_dict(tile, raster, srid, meta)
    if whole:
        row["path"] = _field(tile, "path")
        row["window"] = _window_dict(_field(tile, "window"))
        row["clip_polygon"] = _field(tile, "clip_polygon")
        row["clip_crs"] = _field(tile, "clip_crs")
    else:
        row["window"] = _window_dict(window)
    return row


def _tile_array(fn: str, tile, cap: int, plan: Callable, lower: str):
    """The pieces of ``tile`` as an array of tile rows, under the three bounds above. NULL for a NULL tile and for data
    that cannot be read (UC has no error tile); an error tile goes out as itself; a virtual tile is refused.

    ``plan(ds, raster)`` returns ``(tiles, pixel_bytes, bands, whole, pieces)``: the count of tiles, the decoded bytes
    of all of them, the bands of each, whether the one tile is the whole input, and a function ``pieces(ds)`` that yields
    ``(window, band, raster bytes)`` one piece at a time.
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
                    f"function decodes. Fix: run {fn} in a notebook or job session, which reads the tile in windows"
                )
            tiles, pixel_bytes, bands, whole, pieces = plan(ds, raster)
            _check_bytes(
                fn,
                tiles_estimate(tiles, pixel_bytes, bands),
                "the tiles it returns (an upper estimate)",
                lower=lower,
                cap=cap,
            )
            out, total = [], 0
            for window, band, data in pieces(ds):
                out.append(_sub_tile(fn, tile, data, cap, window, band, whole))
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
        )

    return plan


def _retile_tiles(tile, tile_width, tile_height, cap):
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
    )


def rst_retile_tiles(tile, tile_width, tile_height):
    """gbx__rst_retile_tiles: the non-overlapping windows of a tile as an array of tiles, in the session's order (rows, then
    columns; the edge tiles are smaller). A window of under one pixel raises with a Fix, also for a tile with nothing to read.
    """
    return _retile_tiles(tile, tile_width, tile_height, DECODED_CAP)


def rst_retile_tiles_probe(tile, tile_width, tile_height):
    """The bench probe copy of :func:`rst_retile_tiles`: the same function with the cap raised to :data:`PROBE_DECODED_CAP`
    for the input, each tile and the array, so a timing can measure what the product function refuses. Created only under
    a ``gbx_mvp_perf_`` name; never a product routine."""
    return _retile_tiles(tile, tile_width, tile_height, PROBE_DECODED_CAP)


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
        DECODED_CAP,
        _window_plan(
            lambda ds, tw, th: tiling.iter_to_overlapping_tiles_located(ds, tw, th, ov),
            lambda ds, raster: (w, h),
            ov,
        ),
        "pass a larger tile size or a smaller overlap",
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
        DECODED_CAP,
        _window_plan(
            tiling.iter_retile_located,
            lambda ds, raster: tiling.make_tiles_size(ds, mb, size_bytes=len(raster)),
        ),
        "pass a larger size_in_mb",
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
        )

    return _tile_array(
        fn, tile, DECODED_CAP, plan, "split the raster by band before it is a tile"
    )


HANDLERS = {
    "gbx__st_asmvt_pyramid_cells": st_asmvt_pyramid_cells,
    "gbx_rst_h3_rasterize": rst_h3_rasterize,
    "gbx__h3_cellfill_cells": h3_cellfill_cells,
    "gbx_quadbin_cellfill_array": quadbin_cellfill_array,
    "gbx_bng_cellfill_array": bng_cellfill_array,
    "gbx_custom_cellfill_array": custom_cellfill_array,
    "gbx__st_asmvt_features": st_asmvt_features,
    "gbx__rst_retile_tiles": rst_retile_tiles,
    "gbx__rst_maketiles_tiles": rst_maketiles_tiles,
    "gbx__rst_tooverlappingtiles_tiles": rst_tooverlappingtiles_tiles,
    "gbx__rst_separatebands_tiles": rst_separatebands_tiles,
}
