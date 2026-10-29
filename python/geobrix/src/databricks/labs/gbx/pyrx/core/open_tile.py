"""The single v1/v2/virtual chokepoint.

open_tile(tile) yields an open rasterio dataset regardless of tile shape:
  - raster present  -> open the bytes (v1 / materialized). Provenance fields
    (window/clip/crs) are informational; the bytes ARE the result.
  - raster None      -> stage path local, read exactly `window` (may span >1
    block), lazy-warp to `crs` if set & different, clip to `clip_polygon` if set.
Function bodies never branch on tile shape — they call open_tile and operate on
an open dataset. This is the ONLY place that knows the three tile shapes.

Lifecycle: the yielded dataset is backed by a rasterio MemoryFile (and possibly
a staged temp file). All layers are held open on a single contextlib.ExitStack
so the dataset stays valid for the caller's entire ``with`` block; the stack
unwinds in reverse (close dataset, close MemoryFile, remove staged temp) only
after the caller exits. This avoids the "dataset closed" trap of yielding out of
a nested ``with`` that has already closed.
"""

import atexit
import os
import shutil
from contextlib import ExitStack, contextmanager
from typing import Iterator, Optional, Tuple

import numpy as np
import rasterio
import rasterio.windows
from rasterio.coords import BoundingBox
from rasterio.io import DatasetReader, MemoryFile
from rasterio.transform import TransformMethodsMixin
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip
from databricks.labs.gbx.pyrx.core import _nodata as _nd
from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core import warp as _warp
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_file
from databricks.labs.gbx.pyrx.core.preparer import _stage_local_if_needed
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.zarr import _tiles as _zarr_tiles  # stdlib + numpy only

# ---------------------------------------------------------------------------
# Pending-instruction metadata keys
# These keys in a virtual tile's metadata map record cheap ops (band-select,
# nodata assign, CRS relabel) that are applied together at the next open_tile
# call, avoiding a pixel read just to record the intent.
# Apply order (fixed, order-independent of call order):
#   band-select -> nodata -> setsrid -> window -> clip -> reproject
# ---------------------------------------------------------------------------

PENDING_NODATA = "pending_nodata"
# Token recorded in PENDING_NODATA by rst_initnodata: "stamp the dtype's fallback
# no-data value when the source declares none" (resolved per dtype at apply time).
# Tiles carrying a numeric value (older producers, hand-built) keep that value.
PENDING_NODATA_AUTO = "auto"
PENDING_SRID = "pending_srid"
PENDING_BANDS = "pending_bands"
PENDING_CRS = "pending_crs"

_PENDING_KEYS = (PENDING_NODATA, PENDING_SRID, PENDING_BANDS, PENDING_CRS)

# ---------------------------------------------------------------------------
# Metadata-key helpers for tile size information (informational, not pending)
# These keys are NOT pending instructions: they persist through operations
# that don't change size, and are refreshed when a tile is re-minted.
# ---------------------------------------------------------------------------


def _read_size_key(metadata, key) -> Optional[int]:
    """Parse a size key from tile metadata.

    Returns the int value if present and parseable, None otherwise.
    Useful for reading ``path_file_size`` (underlying source file size in bytes)
    or ``tile_size`` (materialized raster bytes) from the metadata map.
    """
    if not metadata:
        return None
    value = metadata.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_pending(metadata):
    """Return (bands|None, nodata|None, srid|None, crs_str|None) from tile metadata.

    ``nodata`` is a float, or the string ``PENDING_NODATA_AUTO`` (``"auto"``) meaning
    "the dtype's fallback value" (see ``_pending_nodata_value``).

    ``pending_crs`` (a canonical CRS string e.g. 'ESRI:54008') supersedes
    ``pending_srid`` when both are present — the caller should prefer
    ``pending_crs`` for CRS relabelling to support non-EPSG targets.
    """
    md = metadata or {}
    bands = None
    if md.get(PENDING_BANDS):
        bands = [int(b) for b in str(md[PENDING_BANDS]).split(",") if b.strip()]
    nodata = None
    if md.get(PENDING_NODATA) not in (None, ""):
        raw = md[PENDING_NODATA]
        nodata = PENDING_NODATA_AUTO if raw == PENDING_NODATA_AUTO else float(raw)
    srid = int(md[PENDING_SRID]) if md.get(PENDING_SRID) not in (None, "") else None
    crs_str = md[PENDING_CRS] if md.get(PENDING_CRS) not in (None, "") else None
    return bands, nodata, srid, crs_str


def _has_pending(metadata) -> bool:
    """True when the tile metadata records a pending band-select, nodata, SRID or CRS
    instruction (see :func:`_parse_pending`)."""
    return any(v is not None for v in _parse_pending(metadata))


def _pending_nodata_value(nodata, dtype_str) -> Optional[float]:
    """Value to stamp for a pending nodata on a source that declares none.

    ``"auto"`` resolves to the dtype's fallback (``_nodata.fallback_nodata``, the
    heavyweight tier's ``getNoDataConstant``); a numeric value is used as given.
    Returns None when the value does not fit the dtype (nodata stays unset).
    """
    value = _nd.fallback_nodata(dtype_str) if nodata == PENDING_NODATA_AUTO else nodata
    return value if _nd.nodata_fits_dtype(value, dtype_str) else None


def _without_pending(metadata):
    """Metadata map with all pending_* keys removed (consumed on materialization)."""
    return {k: v for k, v in (metadata or {}).items() if k not in _PENDING_KEYS}


def _epsg_of(crs) -> Optional[int]:
    """Parse 'EPSG:3857' / '3857' -> 3857; None if not a plain EPSG code."""
    s = str(crs).strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None


def _selected_labels(src, indexes):
    """The colour interpretation a window of *src* is written with: its own for all its bands,
    or for the band subset *indexes* (1-based) by the band-subset rule
    (``_nodata.subset_colorinterp``)."""
    if not indexes:
        return list(src.colorinterp)
    return _nd.subset_colorinterp(src.colorinterp, indexes)


def _own_mask_use(src, profile, pending_nodata):
    """How a window of *src* carries the source's own mask band: ``(has_mask, as_nodata)``.

    ``has_mask`` is True for a source masked by its own per-dataset mask band, an internal
    or external mask as JPEG/YCbCr imagery often has (``_nodata.has_own_mask``); an alpha
    band is an ordinary band and needs nothing. Pixels alone do not carry that mask, so a
    window written without it returns every masked pixel as valid.

    ``as_nodata`` is True when a NoData is stamped on the window by a pending
    ``rst_initnodata`` (*pending_nodata* set and *profile* holds a NoData): the masked
    pixels then take that value and no mask is written, as eager ``edit.init_nodata``
    does. Otherwise the mask is written with the window as its own mask band (the
    source's mask, as the bytes of the whole file hold it).
    """
    if not _nd.has_own_mask(src):
        return False, False
    return True, pending_nodata is not None and profile.get("nodata") is not None


def _window_mask(src, window, data, profile, pending_nodata, mark_masked=False):
    """The window's mask band to write with *data*, or None; marks NoData in place.

    See :func:`_own_mask_use`. A pending ``rst_initnodata`` that stamps a NoData sets the
    masked pixels of *data* to it (and returns None). ``mark_masked`` also sets them to
    the NoData the window declares, where a mask is written: a warp ignores a mask band
    once a NoData is set, so the pixels themselves must say they are NoData.
    """
    has_mask, as_nodata = _own_mask_use(src, profile, pending_nodata)
    if not has_mask:
        return None
    # Memory: an own-mask source costs one uint8 per window pixel for the mask, and the
    # boolean ``mask == 0`` below is a second one, next to the window's pixels. A source
    # without an own mask pays nothing.
    mask = src.read_masks(1, window=window)
    if as_nodata or (mark_masked and profile.get("nodata") is not None):
        data[:, mask == 0] = profile["nodata"]
    return None if as_nodata else mask


def _window_dataset_bytes(
    src, window: Window, pending=(None, None, None, None), mark_masked: bool = False
) -> bytes:
    """Read one window into standalone GTiff bytes, applying pending instructions.

    pending = (bands|None, nodata|None, srid|None, crs_str|None); applied in
    fixed order: band-select -> nodata -> CRS relabel (crs_str supersedes srid).

    Nodata "ensure/preserve" semantics (matches edit.init_nodata):
    - If the source already carries a nodata value, PRESERVE it (do not override
      with the pending default).
    - If the source has no nodata AND the pending default fits the output dtype
      range, set it.
    - If the source has no nodata AND the pending default does NOT fit the dtype
      (e.g. -9999 for uint16), leave nodata unset rather than writing an invalid
      value.

    A source's own mask band (``_nodata.has_own_mask``) comes with the window, as the
    bytes of the whole file hold it, so a masked pixel stays invalid. Where a pending
    ``rst_initnodata`` stamps a NoData the masked pixels take it instead, as in eager
    ``edit.init_nodata`` (see :func:`_own_mask_use`). ``mark_masked``, for a window that is
    about to be warped, also sets the masked pixels to a declared NoData.
    """
    # Unpack — tolerate the legacy 3-tuple form (srid only, no crs_str).
    if len(pending) == 4:
        bands, nodata, srid, crs_str = pending
    else:
        bands, nodata, srid = pending
        crs_str = None
    indexes = bands if bands else None  # rasterio: 1-based band list or None=all
    # src.read with indexes as a list always returns 3D (bands, h, w); the
    # 2D-collapse path is unreachable here and has been removed.
    data = src.read(window=window, indexes=indexes)
    profile = _comp.source_profile(src.profile)
    count = len(bands) if bands else src.count
    profile.update(
        driver="GTiff",
        height=int(window.height),
        width=int(window.width),
        count=count,
        transform=src.window_transform(window),
    )
    if nodata is not None:
        if src.nodata is not None:
            # Source already has a nodata value: preserve it (pending_nodata
            # means "ensure exists", not "force override").
            profile["nodata"] = src.nodata
        else:
            # Source has no nodata: apply the pending value ("auto" -> the dtype's
            # fallback) only if it fits.
            out_dtype = profile.get("dtype", src.dtypes[0])
            value = _pending_nodata_value(nodata, out_dtype)
            if value is not None:
                profile["nodata"] = value
            # else: value doesn't fit the dtype; leave nodata unset.
    # CRS relabel: pending_crs (full string) supersedes pending_srid.
    if crs_str is not None:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        profile["crs"] = resolve_crs(crs_str)
    elif srid is not None:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs as _resolve

        profile["crs"] = _resolve(
            srid
        )  # epsg/esri classification, not lenient from_epsg
    mask = _window_mask(src, window, data, profile, nodata, mark_masked)
    # Route through the compression authority: ZSTD + dtype-predictor baseline.
    return _comp.write_gtiff(
        profile, data, alpha_like=_selected_labels(src, indexes), mask=mask
    )


def _warped_mask(wds, vrt):
    """The own mask band of *wds* warped like the pixels of *vrt*: ``(rows, cols)`` uint8.

    See :func:`warp.warped_own_mask`: the mask is warped as data onto the grid of *vrt*,
    so it lands where the pixels do, and a pixel no source pixel maps to stays valid.
    """
    return _warp.warped_own_mask(
        wds,
        dst_crs=vrt.crs,
        transform=vrt.transform,
        width=vrt.width,
        height=vrt.height,
    )


def _warp_bytes(win_bytes: bytes, want_crs) -> bytes:
    """Reproject standalone window bytes to *want_crs*; return GTiff bytes.

    *want_crs* is anything ``WarpedVRT`` takes: an ``"EPSG:n"`` string or a rasterio CRS.
    The window's own mask band (see :func:`_window_dataset_bytes`) goes through the warp
    with the pixels: where the window declares a NoData its masked pixels were set to it
    and the warp carries it; without one the mask is warped to the output grid and
    written as the output's own mask band.
    """
    with MemoryFile(win_bytes) as mf, mf.open() as wds:
        mask_band = _nd.has_own_mask(wds) and wds.nodata is None
        with WarpedVRT(wds, crs=want_crs) as vrt:
            prof = _comp.source_profile(vrt.profile)
            prof.update(driver="GTiff")
            data = vrt.read()
            mask = _warped_mask(wds, vrt) if mask_band else None
            # Route through the compression authority for the reprojected output.
            return _comp.write_gtiff(prof, data, alpha_like=wds.colorinterp, mask=mask)


def _warp_window_bytes(
    src, window: Window, want_epsg: int, pending=(None, None, None)
) -> bytes:
    """Materialize a window, lazily reproject it to want_epsg, return GTiff bytes."""
    win_bytes = _window_dataset_bytes(src, window, pending=pending, mark_masked=True)
    return _warp_bytes(win_bytes, f"EPSG:{want_epsg}")


def _warp_window_bytes_crs(
    src, window: Window, want_crs, pending=(None, None, None, None)
) -> bytes:
    """Materialize a window, lazily reproject it to a CRS object, return GTiff bytes.

    Like ``_warp_window_bytes`` but accepts any rasterio CRS object (not just an
    EPSG int), so non-EPSG targets (ESRI:54008, WKT, PROJ4) work correctly.
    ``want_crs`` must be a ``rasterio.crs.CRS`` instance.
    """
    win_bytes = _window_dataset_bytes(src, window, pending=pending, mark_masked=True)
    return _warp_bytes(win_bytes, want_crs)


def _warp_pending(tile: VirtualTile, src) -> bool:
    """True when *tile*'s target ``crs`` asks for a reprojection of its source *src*.

    The rule ``open_tile`` applies: the target is compared with the source's CRS, or with
    the CRS a pending ``rst_setsrid`` relabels it to; a target the source already has
    asks for nothing.
    """
    if not tile.crs:
        return False
    from databricks.labs.gbx.core.crs import authority_srid_of

    _, _, pending_srid, _ = _parse_pending(tile.metadata)
    want = _epsg_of(tile.crs)
    if want is not None:
        effective = (
            pending_srid if pending_srid is not None else authority_srid_of(src.crs)
        )
        return want != effective
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    effective_crs = resolve_crs(pending_srid) if pending_srid is not None else src.crs
    return effective_crs is None or effective_crs != resolve_crs(tile.crs)


def _tile_window_bytes(tile: VirtualTile, src, window: Window, pending) -> bytes:
    """GTiff bytes of *window* of *src* with *tile*'s pending instructions applied,
    reprojected to the tile's target ``crs`` when :func:`_warp_pending` says it differs.

    The one place that decides between the plain, EPSG-warp and CRS-object-warp reads.
    """
    if not _warp_pending(tile, src):
        return _window_dataset_bytes(src, window, pending=pending)
    want = _epsg_of(tile.crs)
    if want is not None:
        return _warp_window_bytes(src, window, want, pending=pending)
    # Non-EPSG target (e.g. 'ESRI:54008', WKT, PROJ4): resolve to a CRS object.
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    return _warp_window_bytes_crs(src, window, resolve_crs(tile.crs), pending=pending)


def _zarr_tile_bytes(tile: VirtualTile, pending) -> bytes:
    """GTiff bytes of a Zarr-backed tile with its pending instructions applied (not its clip).

    The store's window is decoded into standalone GTiff bytes (``zarr_tile_dataset_bytes``);
    from there the tile is a window of a file: pending band-select / NoData / SRID
    instructions and a target ``crs`` apply to those bytes as :func:`_tile_window_bytes`
    applies them to a window of a source.
    """
    data = _zarr_tiles.zarr_tile_dataset_bytes(tile)
    if not _has_pending(tile.metadata) and not tile.crs:
        return data  # nothing to apply
    with MemoryFile(data) as mf, mf.open() as src:
        window = Window(0, 0, src.width, src.height)
        return _tile_window_bytes(tile, src, window, pending)


def _clip_bytes(tile_bytes: bytes, tile: VirtualTile) -> bytes:
    """``tile_bytes`` clipped to the tile's ``clip_polygon`` (a disjoint clip: a 1x1 NoData)."""
    with MemoryFile(tile_bytes) as mf, mf.open() as wds:
        clipped = _clip.clip_dataset(wds, tile.clip_polygon, tile.clip_crs)
        return _empty_dataset_bytes(wds) if clipped is None else clipped


def _empty_dataset_bytes(ref) -> bytes:
    """A valid 1x1 NoData GTiff mirroring ref's band count / dtype (disjoint clip).

    Built from a CLEAN minimal profile — the source's tiling keys
    (tiled/blockxsize/blockysize) make no sense at 1x1 and copying them risks a
    GDAL GTiff-creation warning/error, so they are deliberately dropped.
    """
    # An alpha band marks the pixel transparent (0) by itself: a NoData declared over it
    # would shadow it.
    nodata = (
        ref.nodata if ref.nodata is not None else (None if _nd.has_alpha(ref) else 0)
    )
    dtype = ref.dtypes[0]
    profile = dict(
        driver="GTiff",
        width=1,
        height=1,
        count=ref.count,
        dtype=dtype,
        crs=ref.crs,
        nodata=nodata,
        transform=ref.transform,  # source origin; valid georeference for the 1x1
    )
    # 1x1 tile: decoded_bytes is tiny; auto picks the highest level (fast, negligible).
    decoded_bytes = ref.count * 1 * 1 * np.dtype(dtype).itemsize
    profile.update(
        _comp.creation_opts(dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    arr = np.full((ref.count, 1, 1), 0 if nodata is None else nodata, dtype=dtype)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            _comp.match_alpha(dst, ref.colorinterp)
            dst.write(arr)
        return mf.read()


def _open_bytes(stack: ExitStack, raster_bytes: bytes) -> DatasetReader:
    """Open standalone raster bytes on the stack so it lives until the stack exits."""
    mf = stack.enter_context(MemoryFile(raster_bytes))
    return stack.enter_context(mf.open())


@contextmanager
def open_tile(tile: VirtualTile, file_ref=None) -> Iterator[DatasetReader]:
    # 1. raster present: the bytes ARE the result; provenance fields are ignored
    #    (including any bogus path). Delegate to the v1 bytes contextmanager.
    if tile.raster is not None:
        with _serde.open_tile(tile.raster) as ds:
            yield ds
        return

    # 2. virtual: try FILE windowed-read first (if file_ref provided), else stage
    #    path locally and read the window with optional warp + clip.
    with ExitStack() as stack:
        pending = _parse_pending(tile.metadata)

        # Zarr-backed tile: its window is decoded from the store (tile.path is the store,
        # not a raster to open), then the pending instructions and the clip apply as for a
        # window of a file.
        if _zarr_tiles.is_zarr_tile(tile):
            data = _zarr_tile_bytes(tile, pending)
            if tile.clip_polygon is not None:
                data = _clip_bytes(data, tile)
            yield _open_bytes(stack, data)
            return

        # FILE branch: try windowed read via FileRef (avoids staging the full file).
        # A FileRef is a handle to the same source file — it does NOT reproject
        # or clip.  This fast-path only applies for the no-clip AND no-warp case:
        #   - clip_polygon is not None → fall through; local-path branch clips.
        #   - warp needed → open_windowed_via_fileref raises FileRefReadError → fall through.
        # On any FileRefReadError fall through to the local-path branch below.
        if file_ref is not None and tile.clip_polygon is None:
            try:
                from databricks.labs.gbx.pyrx._file_ref import (
                    FileRefReadError,
                    open_windowed_via_fileref,
                )

                with open_windowed_via_fileref(
                    file_ref, tile.window, pending, tile_crs=tile.crs
                ) as ds:
                    yield ds
                return
            except FileRefReadError:
                pass  # fall through to local-path branch

        # Local-path branch: shared helper handles file_ref degradation
        # (as_local_file → tile.path) and today's plain tile.path read (file_ref=None).
        local_path = _resolve_local_or_windowed(tile, file_ref, stack)

        with rasterio.open(local_path) as src:
            # Resolve window=None → full extent, mirroring open_header() semantics.
            if tile.window is None:
                window = Window(0, 0, src.width, src.height)
            else:
                c, r, w, h = tile.window
                window = Window(c, r, w, h)
            # The pending_srid relabel counts as the source's CRS in the warp
            # decision (see _warp_pending): a tile with pending_srid=3857 and
            # crs=4326 is warped although the file itself is 4326.
            tile_bytes = _tile_window_bytes(tile, src, window, pending)
        # src is closed here; we hold only standalone bytes.

        wds = _open_bytes(stack, tile_bytes)
        if tile.clip_polygon is None:
            yield wds
            return

        clipped = _clip.clip_dataset(wds, tile.clip_polygon, tile.clip_crs)
        if clipped is None:  # disjoint -> valid empty NoData dataset, not an error
            yield _open_bytes(stack, _empty_dataset_bytes(wds))
        else:
            yield _open_bytes(stack, clipped)


def _safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _resolve_local_or_windowed(tile: VirtualTile, file_ref, stack: ExitStack) -> str:
    """Shared local-path resolver for open_tile, _open, and open_header.

    Returns a staged local path ready for rasterio.open, registering cleanup on
    ``stack`` if ``_stage_local_if_needed`` produced a temp file:

    - ``file_ref`` provided: try ``file_ref.as_local_file()`` first; fall back to
      ``tile.path`` if ``as_local_file`` raises.
    - ``file_ref`` is None: ``tile.path`` directly (today's behavior, unchanged).

    This helper covers the *fallback / degradation* path.  The FILE-first windowed
    read (``open_windowed_via_fileref``) is handled inline by ``open_tile`` before
    this helper is called; if the FILE attempt succeeds, this helper is never
    reached.
    """
    if file_ref is not None:
        try:
            raw = file_ref.as_local_file()
        except Exception:
            raw = tile.path
    else:
        raw = tile.path
    local_path, is_temp = _stage_local_if_needed(raw)
    if is_temp:
        stack.callback(_safe_remove, local_path)
    return local_path


def _to_virtual_tile(tile) -> VirtualTile:
    """Normalize any tile shape to VirtualTile.

    Accepted shapes:
    - ``VirtualTile`` → passthrough.
    - ``bytes`` / ``bytearray`` → materialized tile with ``cellid=0``.
    - dict/Row with a ``path`` or ``window`` key → v2 struct (``VirtualTile.from_row``).
    - dict/Row with a ``raster`` key (no ``path``) → v1 struct (``VirtualTile.from_v1``).
    """
    if isinstance(tile, VirtualTile):
        return tile
    if isinstance(tile, (bytes, bytearray)):
        return VirtualTile(cellid=0, raster=bytes(tile))
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    if "path" in d or "window" in d:
        return VirtualTile.from_row(d)
    return VirtualTile.from_v1(d.get("cellid", 0), d["raster"], d.get("metadata"))


@contextmanager
def _open(tile, file_ref=None):
    """Context manager that yields an open ``DatasetReader`` for any tile shape.

    Accepts a ``VirtualTile``, raw bytes/bytearray, a v1 dict/Row
    ``{cellid, raster, metadata}``, or a v2 dict/Row (9-field struct).
    Normalises to ``VirtualTile`` via ``_to_virtual_tile`` then delegates to
    ``open_tile``; all lifecycle management (MemoryFile, staged temps) is
    handled there.

    ``file_ref``: optional FileRef for FILE-type windowed reads; threaded through
    to ``open_tile``.  Defaults to None (today's behaviour, unchanged).
    """
    with open_tile(_to_virtual_tile(tile), file_ref=file_ref) as ds:
        yield ds


@contextmanager
def _open_all(tiles):
    """Context manager that yields a list of open ``DatasetReader`` objects.

    Opens each tile in *tiles* under a single ``ExitStack`` so all datasets
    stay valid for the caller's entire ``with`` block and are closed together
    on exit. Useful for multi-input operations (map-algebra, aggregation).
    """
    with ExitStack() as stack:
        yield [stack.enter_context(_open(t)) for t in tiles]


class _WindowHeaderView:
    """Read-free header view of a source dataset restricted to a sub-window.

    Presents the window-varying fields (``width``, ``height``, ``transform``,
    ``bounds``, ``profile``) as the sub-window's values, computed purely from the
    source header + the window offset/size (no pixel I/O). Every other attribute
    (``crs``, ``count``, ``nodata``, ``dtypes``, ``driver``, ``tags()``,
    ``subdatasets``, ...) proxies straight through to the source dataset via
    ``__getattr__`` — those are window-invariant. ``read`` is intentionally NOT
    proxied: this view exists precisely to avoid materialising pixels.

    Optional ``pending_count``, ``pending_crs``, and ``pending_nodata`` override
    ``count``, ``crs``, and ``nodata`` to reflect pending band-select, setsrid,
    and init_nodata instructions recorded on the tile.

    For ``pending_nodata``: the same ensure/preserve+dtype-fit logic as
    open_tile applies — if the source already has nodata, source value is
    preserved; if the source has no nodata and the pending value fits the dtype,
    it is shown; otherwise source nodata (None) is preserved.
    """

    def __init__(
        self,
        src,
        window: Window,
        pending_count=None,
        pending_crs=None,
        pending_nodata=None,
    ):
        self._src = src
        self._window = window
        self._transform = src.window_transform(window)
        # Bounds of the window in the source CRS, derived from the source
        # transform + window (no read).
        left, bottom, right, top = rasterio.windows.bounds(window, src.transform)
        self._bounds = BoundingBox(left, bottom, right, top)
        self._pending_count = pending_count
        self._pending_crs = pending_crs
        # Resolve nodata once at construction (no pixel I/O needed).
        if src.nodata is not None:
            # Source already has nodata: preserve it.
            self._resolved_nodata = src.nodata
        elif pending_nodata is not None:
            # Source has no nodata: apply the pending value ("auto" -> the
            # dtype's fallback) only if it fits.
            self._resolved_nodata = _pending_nodata_value(pending_nodata, src.dtypes[0])
        else:
            self._resolved_nodata = None
        self._has_nodata_override = pending_nodata is not None and src.nodata is None

    @property
    def width(self) -> int:
        return int(self._window.width)

    @property
    def height(self) -> int:
        return int(self._window.height)

    @property
    def transform(self):
        return self._transform

    @property
    def bounds(self) -> BoundingBox:
        return self._bounds

    @property
    def count(self) -> int:
        if self._pending_count is not None:
            return self._pending_count
        return self._src.count

    @property
    def crs(self):
        if self._pending_crs is not None:
            return self._pending_crs
        return self._src.crs

    @property
    def nodata(self):
        if self._has_nodata_override:
            return self._resolved_nodata
        return self._src.nodata

    @property
    def profile(self):
        prof = self._src.profile.copy()
        prof.update(
            width=int(self._window.width),
            height=int(self._window.height),
            transform=self._transform,
        )
        if self._pending_count is not None:
            prof["count"] = self._pending_count
        if self._pending_crs is not None:
            prof["crs"] = self._pending_crs
        if self._has_nodata_override:
            prof["nodata"] = self._resolved_nodata
        return prof

    def __getattr__(self, name):
        # Window-invariant attributes/methods proxy to the source dataset. Guard
        # against read to keep the view header-only.
        if name == "read":
            raise AttributeError(
                "_WindowHeaderView is header-only; pixel reads are not supported"
            )
        return getattr(self._src, name)


def _is_full_extent(window, src) -> bool:
    """True if ``window`` (col, row, w, h) covers the full source extent."""
    c, r, w, h = window
    return c == 0 and r == 0 and w == src.width and h == src.height


def _header_pending(metadata):
    """``(bands|None, pending_crs|None, pending_nodata|None)``: what a view of a source
    presents for the tile's pending instructions (see :class:`_WindowHeaderView`).

    ``pending_crs`` is the resolved CRS a pending ``rst_setsrid`` relabels the source to
    (``pending_crs`` supersedes ``pending_srid``). ``pending_nodata`` is the raw pending
    value: the view applies the same ensure/preserve and dtype-fit rule as
    ``_window_dataset_bytes``, so a view and the window's pixels agree on NoData.
    """
    bands, nodata, srid, crs_str = _parse_pending(metadata)
    pending_crs = None
    if crs_str is not None or srid is not None:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        pending_crs = resolve_crs(crs_str if crs_str is not None else srid)
    return bands, pending_crs, nodata


@contextmanager
def open_header(tile, file_ref=None) -> Iterator[DatasetReader]:
    """Context manager yielding an open dataset for **header/metadata access only**.

    Unlike ``open_tile`` / ``_open``, this function never materialises a pixel
    window.  Callers may safely inspect ``.width``, ``.height``, ``.crs``,
    ``.transform``, ``.bounds``, ``.profile``, etc.  They must NOT call
    ``ds.read()`` — that would materialise pixels and defeats the purpose.

    Accepted tile shapes:
    - bytes / bytearray / v1 dict (``raster`` set) → open the bytes via the
      MemoryFile path (``_open``).  The bytes already contain the full result so
      header is trivially available; no window read is performed by this call.
    - virtual dict / v2 struct / VirtualTile (``raster`` None) → resolve local
      path via ``_resolve_local_or_windowed`` (uses ``file_ref.as_local_file()``
      if provided, else ``tile.path``; handles staging for /Volumes paths), then
      open the source file with a plain ``rasterio.open`` (lazy, no pixel I/O).
      If the tile carries a sub-window (present AND not the full source extent),
      yield a read-free ``_WindowHeaderView`` whose
      ``width``/``height``/``transform``/``bounds``/``profile`` reflect the WINDOW
      (consistent with the pixel path and the materialized-equivalent tile) while
      other fields proxy the source. For a whole-file window (None or == full
      extent) yield the source dataset directly. The ExitStack cleans up the
      staged temp on exit.

    ``file_ref``: optional FileRef for FILE-type reads.  For header-only reads,
    ``file_ref.as_local_file()`` is tried (avoids re-implementing pixel
    materialisation); falls back to ``tile.path`` if that raises.  Defaults to
    None (today's behaviour, unchanged).
    """
    vt = _to_virtual_tile(tile)

    if not vt.is_virtual():
        # Bytes path: delegate to _open; no pixel read is performed here.
        with _open(vt) as ds:
            yield ds
        return

    # Virtual path: open the source header — no window read in either branch.
    with ExitStack() as stack:
        if _zarr_tiles.is_zarr_tile(vt):
            # the header of the tile's plane, from its metadata: no chunk is read, and
            # vt.window below picks the tile's part of it, as for a file
            src = _zarr_tiles.zarr_tile_header_view(vt)
        else:
            local_path = _resolve_local_or_windowed(vt, file_ref, stack)
            src = stack.enter_context(rasterio.open(local_path))

        # Parse pending instructions to reflect band-select / setsrid / nodata in header.
        bands, pending_crs, pending_nodata = _header_pending(vt.metadata)
        pending_count = len(bands) if bands else None

        any_pending = (
            pending_count is not None
            or pending_crs is not None
            or pending_nodata is not None
        )

        if vt.window is None or _is_full_extent(vt.window, src):
            # Whole-file: yield a view that reflects pending overrides but
            # otherwise proxies the source directly.
            if not any_pending:
                yield src
            else:
                # Re-use _WindowHeaderView with the full-source window to get
                # the pending overrides; width/height/transform/bounds are
                # identical to the source for a whole-file window.
                full_window = Window(0, 0, src.width, src.height)
                yield _WindowHeaderView(
                    src,
                    full_window,
                    pending_count=pending_count,
                    pending_crs=pending_crs,
                    pending_nodata=pending_nodata,
                )
        else:
            # Sub-window: present the window's dims/extent + pending overrides.
            c, r, w, h = vt.window
            yield _WindowHeaderView(
                src,
                Window(c, r, w, h),
                pending_count=pending_count,
                pending_crs=pending_crs,
                pending_nodata=pending_nodata,
            )


# ---------------------------------------------------------------------------
# Point reads: the pixels of a virtual tile at a few points (rst_sample)
# ---------------------------------------------------------------------------


def _task_attempt_id():
    """The attempt id of the Spark task this code runs in, or None outside a task."""
    try:
        from pyspark import TaskContext

        ctx = TaskContext.get()
    except Exception:  # noqa: BLE001 - no pyspark, or not in a Spark task
        return None
    return None if ctx is None else ctx.taskAttemptId()


class _TaskSources:
    """The open sources of virtual tiles, shared by the point reads of one Spark task.

    A scalar UDF is called once a row, so ``rst_sample`` over N points of one file opens
    the file N times unless something outlives the call. Within one task attempt the opened
    sources are held in an ``OpenResourceLRU`` (``ds.file_gbx``: the per-partition LRU of
    the grouped ops, with its opener, which probes a FUSE path and stages it when direct
    reads fail), keyed by path. A new task attempt closes the last one's sources first, so
    nothing is reused across tasks, where a file may have been rewritten. A staged copy is
    removed from disk as soon as it is open (``unlink_staged``): the handle may be held
    until the worker ends, and a PySpark worker ends with ``os._exit``, which runs no
    ``atexit`` handler. Outside a task (a driver call) each read opens the source and
    closes it.
    """

    def __init__(self):
        self._task = None
        self._lru = None

    @contextmanager
    def source(self, path: str):
        """Yield the open source at *path*: from the task's LRU, or opened for this call."""
        from databricks.labs.gbx.ds.file_gbx import (
            OpenResourceLRU,
            _lru_sizing,
            _OpenerContext,
        )

        task = _task_attempt_id()
        if task != self._task:
            self.reset()
        if task is None:
            opener = _OpenerContext()
            src = opener.open(path)
            try:
                yield src
            finally:
                opener.close(src)
            return
        if self._lru is None:
            from databricks.labs.gbx.pyrx.core.budget import runtime_kind

            # a staged copy comes off the disk once open: the handle outlives the task
            opener = _OpenerContext(unlink_staged=True)
            _, max_bytes, max_count = _lru_sizing(runtime_kind() == "serverless")
            self._lru = OpenResourceLRU(
                max_bytes=max_bytes,
                max_count=max_count,
                opener=opener.open,
                closer=opener.close,
                weigher=opener.weigh,
            )
            self._task = task
        yield self._lru.get(path)

    def reset(self) -> None:
        """Close every held source (a new task, the end of the worker)."""
        if self._lru is not None:
            self._lru.close_all()
        self._lru = None
        self._task = None


_TASK_SOURCES = _TaskSources()
atexit.register(_TASK_SOURCES.reset)


class _WindowPointView(TransformMethodsMixin, _WindowHeaderView):
    """The dataset ``open_tile`` yields for a virtual tile, for reads of a few pixels.

    The header is the window's (``_WindowHeaderView``: dims, transform, and the pending
    band-select / NoData / SRID), ``index`` maps a point to the window's row and column, and
    ``read`` / ``sample`` read the pixels from the source, offset by the window and mapped to
    the selected bands. GDAL then decodes only the blocks that hold the points, where
    ``open_tile`` decodes the whole window and encodes it again as a GeoTIFF. Unmasked reads
    only: a source with its own mask band is not given this view (see :func:`_point_view`).
    """

    def __init__(self, src, window: Window, bands=None, pending_crs=None, nodata=None):
        super().__init__(
            src,
            window,
            pending_count=len(bands) if bands else None,
            pending_crs=pending_crs,
            pending_nodata=nodata,
        )
        self._bands = list(bands) if bands else list(src.indexes)

    @property
    def indexes(self):
        return tuple(range(1, len(self._bands) + 1))

    @property
    def dtypes(self):
        return tuple(self._src.dtypes[b - 1] for b in self._bands)

    def read(self, indexes=None, window=None, masked=False):
        if masked:
            raise NotImplementedError("_WindowPointView reads unmasked pixels only")
        one = isinstance(indexes, int)
        wanted = self.indexes if indexes is None else [indexes] if one else indexes
        win = window or Window(0, 0, self.width, self.height)
        data = self._src.read(
            [self._bands[i - 1] for i in wanted],
            window=Window(
                self._window.col_off + win.col_off,
                self._window.row_off + win.row_off,
                win.width,
                win.height,
            ),
        )
        return data[0] if one else data

    def sample(self, xy, indexes=None, masked=False):
        from rasterio.sample import sample_gen

        if masked:
            raise NotImplementedError("_WindowPointView samples unmasked pixels only")
        return sample_gen(self, xy, indexes=indexes)


def _point_view(tile: VirtualTile, src) -> Optional[_WindowPointView]:
    """A :class:`_WindowPointView` of *tile*'s window of the open *src*, or None when the
    window must be materialised: a reprojection is pending (the pixels are a warp's), the
    source has its own mask band (a pending NoData marks its masked pixels), or the window
    is not inside the source."""
    if _warp_pending(tile, src) or _nd.has_own_mask(src):
        return None
    c, r, w, h = tile.window or (0, 0, src.width, src.height)
    if c < 0 or r < 0 or c + w > src.width or r + h > src.height:
        return None
    bands, pending_crs, nodata = _header_pending(tile.metadata)
    return _WindowPointView(src, Window(c, r, w, h), bands, pending_crs, nodata)


@contextmanager
def open_tile_points(tile) -> Iterator:
    """``open_tile`` for an op that reads the pixels at a few points (``rst_sample``).

    Yields what ``open_tile`` would, with the same pixel values at every point: for a
    virtual tile of a file (no clip; no pending reprojection), a :class:`_WindowPointView`
    of its window of the source, so a point costs the read of the block holding it, and the
    source is opened once per Spark task (:class:`_TaskSources`); for any other tile
    (materialized, Zarr-backed, clipped, reprojected), ``open_tile`` itself.
    """
    vt = _to_virtual_tile(tile)
    if (
        not vt.is_virtual()
        or _zarr_tiles.is_zarr_tile(vt)
        or vt.clip_polygon is not None
    ):
        with open_tile(vt) as ds:
            yield ds
        return
    with _TASK_SOURCES.source(vt.path) as src:
        view = _point_view(vt, src)
        if view is not None:
            yield view
            return
    with open_tile(vt) as ds:
        yield ds


def materialize_to_bytes(tile: VirtualTile) -> VirtualTile:
    """Convert a (possibly virtual) tile to a v2-materialized tile: run open_tile
    on the light side (which CAN read /Volumes), capture the window+warp+clip
    result into `raster`, keep provenance. Output is heavy-consumable. This is
    the single sanctioned light->heavy crossing for virtual tiles.
    """
    if _zarr_tiles.is_zarr_tile(tile):
        return _materialize_zarr(tile)
    with open_tile(tile) as ds:
        data = ds.read()
        profile = _comp.source_profile(ds.profile)
        profile.update(driver="GTiff")
        # A source's own mask band travels with the bytes (the heavy tier reads them).
        mask = ds.read_masks(1) if _nd.has_own_mask(ds) else None
        # Route through the compression authority: ZSTD + dtype-predictor baseline.
        raster = _comp.write_gtiff(profile, data, alpha_like=ds.colorinterp, mask=mask)
    # Pending keys are baked into the produced bytes; strip them so the
    # materialized tile's metadata stays honest (no double-application).
    meta = _without_pending(tile.metadata)
    # Stamp tile_byte_size (materialized raster byte length) into metadata.
    meta["tile_byte_size"] = str(len(raster))
    return VirtualTile(
        cellid=tile.cellid,
        raster=raster,
        path=tile.path,
        window=tile.window,
        clip_polygon=tile.clip_polygon,
        clip_crs=tile.clip_crs,
        crs=tile.crs,
        metadata=meta,
    )


def _materialize_zarr(tile: VirtualTile) -> VirtualTile:
    """``materialize_to_bytes`` of a Zarr-backed tile.

    The window is decoded from the store once (``zarr_tile_dataset_bytes``, whose size check
    is ``materialize_decision``), the pending instructions and the clip are applied to
    those bytes, and the provenance stays: ``path``, ``window``, ``sliceDims``,
    ``zarr_array``, ``zarr_version``, the CF attributes in ``zarr_meta``. The bytes are a
    GeoTIFF, which is what a writer reads from the ``driver`` and ``format`` keys.
    """
    raster = _tile_to_bytes(tile)
    meta = _without_pending(tile.metadata)
    meta["tile_byte_size"] = str(len(raster))
    meta["driver"] = meta["format"] = "GTiff"
    return VirtualTile(
        cellid=tile.cellid,
        raster=raster,
        path=tile.path,
        window=tile.window,
        clip_polygon=tile.clip_polygon,
        clip_crs=tile.clip_crs,
        crs=tile.crs,
        metadata=meta,
    )


def _windowed_materialize_bytes(tile: VirtualTile) -> bytes:
    """Block-streaming equivalent of ``materialize_to_bytes(tile).raster``.

    Opens the source file lazily and writes the output GTiff block-by-block,
    so the full pixel array is never held in RAM.  Returns encoded GTiff bytes
    that are **pixel-array-equal and profile-equal** to ``materialize_to_bytes``
    for the common (no-warp, no-clip) case.

    Profile building exactly mirrors ``_window_dataset_bytes`` / ``materialize_to_bytes``:
    - Same driver, height, width, count, transform.
    - Same nodata ensure/preserve+dtype-fit logic.
    - Same CRS relabel (pending_crs supersedes pending_srid).
    - Same compression authority (``_comp.creation_opts``, "auto", full decoded_bytes).

    **Fallback for warp / clip:**  Reprojection and clipping require full-window
    context that cannot be assembled block-by-block safely.  These cases delegate
    to ``materialize_to_bytes(tile).raster``.  Tests exercise the no-warp / no-clip
    path only and assert ``materialize_to_bytes`` is NOT called there.

    A Zarr-backed tile has no file to stream from; it is decoded a window at a time by the
    store engine, so it takes ``materialize_to_bytes``.
    """
    if _zarr_tiles.is_zarr_tile(tile):
        return materialize_to_bytes(tile).raster
    with ExitStack() as stack:
        pending = _parse_pending(tile.metadata)
        bands, nodata, srid, crs_str = pending

        # Resolve the local path — same as open_tile's local-path branch.
        local_path = _resolve_local_or_windowed(tile, None, stack)

        with rasterio.open(local_path) as src:
            # Resolve window → same semantics as open_tile.
            if tile.window is None:
                window = Window(0, 0, src.width, src.height)
            else:
                c, r, w, h = tile.window
                window = Window(c, r, w, h)

            # Detect whether reprojection is needed.  Warp and clip cannot be
            # done block-by-block (they need full-window context), so fall back.
            needs_warp = _warp_pending(tile, src)
            needs_clip = tile.clip_polygon is not None

            if needs_warp or needs_clip:
                # Warp / clip require full-window context; delegate to the
                # standard materializer.  This branch is not exercised by the
                # size-gate test suite (test tiles are no-warp / no-clip).
                return materialize_to_bytes(tile).raster

            # --- Block-streaming path: no warp, no clip ---
            indexes = bands if bands else None
            count = len(bands) if bands else src.count

            # Build the exact profile that _window_dataset_bytes / materialize_to_bytes
            # would produce.
            profile = _comp.source_profile(src.profile)
            profile.update(
                driver="GTiff",
                height=int(window.height),
                width=int(window.width),
                count=count,
                transform=src.window_transform(window),
            )

            # Nodata: same ensure/preserve + dtype-fit logic as _window_dataset_bytes.
            if nodata is not None:
                if src.nodata is not None:
                    # Source already carries nodata → preserve it.
                    profile["nodata"] = src.nodata
                else:
                    out_dtype_nd = profile.get("dtype", src.dtypes[0])
                    value = _pending_nodata_value(nodata, out_dtype_nd)
                    if value is not None:
                        profile["nodata"] = value

            # CRS relabel: pending_crs (full string) supersedes pending_srid.
            if crs_str is not None:
                from databricks.labs.gbx.pyrx.core.crs import resolve_crs

                profile["crs"] = resolve_crs(crs_str)
            elif srid is not None:
                from databricks.labs.gbx.pyrx.core.crs import resolve_crs

                profile["crs"] = resolve_crs(srid)

            # Compression: same authority as materialize_to_bytes (full decoded size).
            out_dtype = profile.get("dtype", src.dtypes[0])
            decoded_bytes = (
                count
                * int(window.width)
                * int(window.height)
                * np.dtype(out_dtype).itemsize
            )
            profile.update(
                _comp.creation_opts(
                    out_dtype, decoded_bytes=decoded_bytes, compress="auto"
                )
            )

            # A source's own mask band: written block by block as the output's mask, or
            # (a pending rst_initnodata stamping a NoData) set into the pixels, exactly
            # as _window_dataset_bytes does for the whole window.
            has_mask, as_nodata = _own_mask_use(src, profile, nodata)

            # Write block-by-block — at most one output block in RAM at a time.
            # (The mask goes in as an internal mask band; without the option GDAL
            # would put it in a sidecar file a MemoryFile cannot hold on to.)
            with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
                with MemoryFile() as mf:
                    with mf.open(**profile) as dst:
                        _comp.match_alpha(dst, _selected_labels(src, indexes))
                        for _, block_window in dst.block_windows(1):
                            # Translate the output-local (0-based) block_window to
                            # source-file coordinates (offset by the tile window origin).
                            src_block = Window(
                                col_off=int(block_window.col_off) + int(window.col_off),
                                row_off=int(block_window.row_off) + int(window.row_off),
                                width=int(block_window.width),
                                height=int(block_window.height),
                            )
                            data = src.read(window=src_block, indexes=indexes)
                            if has_mask:
                                mask = src.read_masks(1, window=src_block)
                                if as_nodata:
                                    data[:, mask == 0] = profile["nodata"]
                                else:
                                    dst.write_mask(mask, window=block_window)
                            dst.write(data, window=block_window)
                    return mf.read()


def _tile_to_bytes(vt: VirtualTile) -> Optional[bytes]:
    """Return GTiff bytes for a tile, avoiding the double-encode of materialize_to_bytes.

    For materialized tiles (``raster`` set), returns ``bytes(vt.raster)`` unchanged.
    For virtual tiles (``raster`` None), produces bytes via the local-path pipeline
    (``_window_dataset_bytes`` / ``_warp_window_bytes`` / clip) and returns them
    **directly** — without the ``_open_bytes`` → ``ds.read()`` → re-encode cycle
    that ``materialize_to_bytes`` performs.

    Compared to ``materialize_to_bytes(vt).raster``, this saves one ZSTD compress
    + one ZSTD decompress per virtual input tile in the common case (no clip / no
    warp).  The bytes returned are semantically identical: same pixels, same
    georeference, same nodata, same compression.

    **Constraints**:
    - ``file_ref=None`` only (local-path branch; the FILE path is not needed here
      because callers that use FILE already go through ``_uf_*`` variants).
    - Use when only the pixel bytes matter and the full ``VirtualTile`` provenance
      returned by ``materialize_to_bytes`` is not needed.
    """
    if not vt.is_virtual():
        return bytes(vt.raster) if vt.raster is not None else None

    if _zarr_tiles.is_zarr_tile(vt):
        data = _zarr_tile_bytes(vt, _parse_pending(vt.metadata))
        return data if vt.clip_polygon is None else _clip_bytes(data, vt)

    with ExitStack() as stack:
        pending = _parse_pending(vt.metadata)
        local_path = _resolve_local_or_windowed(vt, None, stack)
        with rasterio.open(local_path) as src:
            # Resolve window=None → full extent, mirroring open_tile() semantics.
            # A deferred whole-file virtual tile carries window=None;
            # materialising it must produce the same bytes as an explicit full window.
            if vt.window is None:
                window = Window(0, 0, src.width, src.height)
            else:
                c, r, w, h = vt.window
                window = Window(c, r, w, h)
            tile_bytes = _tile_window_bytes(vt, src, window, pending)

        if vt.clip_polygon is None:
            return tile_bytes

        # Clip path: open tile_bytes temporarily to clip, return clipped bytes directly.
        with MemoryFile(tile_bytes) as mf:
            with mf.open() as wds:
                clipped = _clip.clip_dataset(wds, vt.clip_polygon, vt.clip_crs)
                if clipped is None:
                    return _empty_dataset_bytes(wds)
                return clipped


def materialize_array(tile: VirtualTile) -> Tuple[np.ndarray, "rasterio.Affine", dict]:
    """Convenience wrapper: (array, transform, profile) from open_tile."""
    with open_tile(tile) as ds:
        return ds.read(), ds.transform, ds.profile.copy()


def shape_output(
    tile,
    *,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> VirtualTile:
    """Force the output shape of a tile.

    Accepted input: any tile shape (VirtualTile, bytes, v1/v2 dict/Row);
    normalised via ``_to_virtual_tile`` before processing.

    Rules
    -----
    ``virtualize_dir`` and ``materialize=True`` are mutually exclusive.

    ``virtualize_dir`` set:
      - The tile is already a REFERENCE to backing pixels (``raster`` is None —
        header reads, reader selection): return as-is.
        ``virtualize_dir`` has no meaningful effect here because the tile already
        references real, self-consistent bytes on a backing store.
      - The tile carries PRODUCED pixels (``raster`` set — a pixel-producing op
        such as reproject/merge/combineavg/frombands materialized its result):
        write bytes to
        ``<dir>/[<prefix>_]<cellid>_<col>_<row>_<w>_<h>.tif`` (overwrite),
        FUSE-safe (local temp -> shutil.copyfile), return a VirtualTile with
        ``raster=None`` referencing the written file. This is the ONLY way a
        pixel-producer returns a virtual tile. The returned ``window`` is the
        written file's full extent ``(0, 0, width, height)``; an input
        ``window`` (a part's position in its parent) only names the file. The input's
        ``clip_polygon`` / ``clip_crs`` / ``crs`` are provenance (already applied to
        make its bytes) and are not carried: on the virtual tile they would be
        instructions, applied again on every read.

    ``materialize=True``:
      - Tile virtual → read via ``open_tile`` (lazy), capture bytes,
        return a materialized VirtualTile (delegates to
        ``materialize_to_bytes``).
      - Tile already materialized → no-op (return as-is).

    Neither → return the tile as-is (auto).
    """
    vt = _to_virtual_tile(tile)

    if virtualize_dir is not None and materialize:
        raise ValueError(
            "shape_output: virtualize_dir and materialize=True are mutually exclusive"
        )

    if virtualize_dir is not None:
        # Already virtual — nothing to do.
        if vt.is_virtual():
            return vt

        # The written file's own size (header read only).
        with _serde.open_tile(vt.raster) as ds:
            own_w, own_h = ds.width, ds.height

        # (col, row, w, h) names the file. A part cut from a parent carries its
        # position in the PARENT as ``window``; that tells sibling parts of one
        # cellid apart, so it stays in the name.
        if vt.window is not None:
            col, row, w, h = vt.window
        else:
            col, row, w, h = 0, 0, own_w, own_h

        parts = [str(vt.cellid), str(col), str(row), str(w), str(h)]
        base = "_".join(parts) + ".tif"
        if virtualize_prefix:
            base = f"{virtualize_prefix}_{base}"

        os.makedirs(virtualize_dir, exist_ok=True)
        out_path = os.path.join(virtualize_dir, base)

        # FUSE-safe write: write to a local temp, then copy into place.
        tmp = new_local_temp_file(suffix=".tif")
        try:
            with open(tmp, "wb") as f:
                f.write(vt.raster)
            shutil.copyfile(tmp, out_path)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

        # Pending keys were applied when open_tile read the bytes above; strip
        # them so the produced tile's metadata is pending-free (keys are baked
        # into the written file, no longer pending).
        meta = _without_pending(vt.metadata)
        meta["shape_output"] = "virtualized"
        # Stamp path_file_size (underlying source file size in bytes) into metadata.
        file_size = os.path.getsize(out_path)
        meta["path_file_size"] = str(file_size)
        return VirtualTile(
            cellid=vt.cellid,
            raster=None,
            path=out_path,
            # The window addresses the file just written, which holds only this
            # tile: its full extent. The parent position is not carried over; it
            # would point outside the file.
            window=(0, 0, own_w, own_h),
            # On the materialized input clip_polygon / clip_crs / crs record what was
            # already applied to make its bytes; on this virtual tile they would be
            # instructions, applied a second time to the file just written.
            clip_polygon=None,
            clip_crs=None,
            crs=None,
            metadata=meta,
        )

    if materialize:
        if not vt.is_virtual():
            return vt
        return materialize_to_bytes(vt)

    # Auto: return as-is.
    return vt
