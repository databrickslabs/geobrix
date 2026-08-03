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

import os
import shutil
import tempfile
from contextlib import ExitStack, contextmanager
from typing import Iterator, Optional, Tuple

import numpy as np
import rasterio
import rasterio.windows
from rasterio.coords import BoundingBox
from rasterio.io import DatasetReader, MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip
from databricks.labs.gbx.pyrx.core.edit import _nodata_fits_dtype
from databricks.labs.gbx.pyrx.core.preparer import _stage_local_if_needed
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# Pending-instruction metadata keys
# These keys in a virtual tile's metadata map record cheap ops (band-select,
# nodata assign, CRS relabel) that are applied together at the next open_tile
# call, avoiding a pixel read just to record the intent.
# Apply order (fixed, order-independent of call order):
#   band-select -> nodata -> setsrid -> window -> clip -> reproject
# ---------------------------------------------------------------------------

PENDING_NODATA = "pending_nodata"
PENDING_SRID = "pending_srid"
PENDING_BANDS = "pending_bands"

_PENDING_KEYS = (PENDING_NODATA, PENDING_SRID, PENDING_BANDS)


def _parse_pending(metadata):
    """Return (bands|None, nodata|None, srid|None) from a tile metadata map."""
    md = metadata or {}
    bands = None
    if md.get(PENDING_BANDS):
        bands = [int(b) for b in str(md[PENDING_BANDS]).split(",") if b.strip()]
    nodata = (
        float(md[PENDING_NODATA]) if md.get(PENDING_NODATA) not in (None, "") else None
    )
    srid = int(md[PENDING_SRID]) if md.get(PENDING_SRID) not in (None, "") else None
    return bands, nodata, srid


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


def _window_dataset_bytes(src, window: Window, pending=(None, None, None)) -> bytes:
    """Read one window into standalone GTiff bytes, applying pending instructions.

    pending = (bands|None, nodata|None, srid|None); applied in fixed order:
    band-select (which bands to read) -> nodata (profile) -> setsrid (crs relabel).

    Nodata "ensure/preserve" semantics (matches edit.init_nodata):
    - If the source already carries a nodata value, PRESERVE it (do not override
      with the pending default).
    - If the source has no nodata AND the pending default fits the output dtype
      range, set it.
    - If the source has no nodata AND the pending default does NOT fit the dtype
      (e.g. -9999 for uint16), leave nodata unset rather than writing an invalid
      value.
    """
    import rasterio.crs as _rcrs

    bands, nodata, srid = pending
    indexes = bands if bands else None  # rasterio: 1-based band list or None=all
    # src.read with indexes as a list always returns 3D (bands, h, w); the
    # 2D-collapse path is unreachable here and has been removed.
    data = src.read(window=window, indexes=indexes)
    profile = src.profile.copy()
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
            # Source has no nodata: apply the pending default only if it fits.
            out_dtype = profile.get("dtype", src.dtypes[0])
            if _nodata_fits_dtype(nodata, out_dtype):
                profile["nodata"] = nodata
            # else: default doesn't fit the dtype; leave nodata unset.
    if srid is not None:
        profile["crs"] = _rcrs.CRS.from_epsg(srid)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def _warp_window_bytes(
    src, window: Window, want_epsg: int, pending=(None, None, None)
) -> bytes:
    """Materialize a window, lazily reproject it to want_epsg, return GTiff bytes."""
    win_bytes = _window_dataset_bytes(src, window, pending=pending)
    with MemoryFile(win_bytes) as mf, mf.open() as wds:
        with WarpedVRT(wds, crs=f"EPSG:{want_epsg}") as vrt:
            prof = vrt.profile.copy()
            prof.update(driver="GTiff")
            data = vrt.read()
            with MemoryFile() as out_mf:
                with out_mf.open(**prof) as dst:
                    dst.write(data)
                return out_mf.read()


def _empty_dataset_bytes(ref) -> bytes:
    """A valid 1x1 NoData GTiff mirroring ref's band count / dtype (disjoint clip).

    Built from a CLEAN minimal profile — the source's tiling keys
    (tiled/blockxsize/blockysize) make no sense at 1x1 and copying them risks a
    GDAL GTiff-creation warning/error, so they are deliberately dropped.
    """
    nodata = ref.nodata if ref.nodata is not None else 0
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
    arr = np.full((ref.count, 1, 1), nodata, dtype=dtype)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr)
        return mf.read()


def _open_bytes(stack: ExitStack, raster_bytes: bytes) -> DatasetReader:
    """Open standalone raster bytes on the stack so it lives until the stack exits."""
    mf = stack.enter_context(MemoryFile(raster_bytes))
    return stack.enter_context(mf.open())


@contextmanager
def open_tile(tile: VirtualTile) -> Iterator[DatasetReader]:
    # 1. raster present: the bytes ARE the result; provenance fields are ignored
    #    (including any bogus path). Delegate to the v1 bytes contextmanager.
    if tile.raster is not None:
        with _serde.open_tile(tile.raster) as ds:
            yield ds
        return

    # 2. virtual: stage path, read exactly the window, optional warp + clip.
    with ExitStack() as stack:
        local_path, is_temp = _stage_local_if_needed(tile.path)
        if is_temp:
            stack.callback(_safe_remove, local_path)

        c, r, w, h = tile.window
        window = Window(c, r, w, h)
        pending = _parse_pending(tile.metadata)
        bands, _nodata, pending_srid = pending
        with rasterio.open(local_path) as src:
            src_epsg = src.crs.to_epsg() if src.crs else None
            want = _epsg_of(tile.crs) if tile.crs else None
            # When pending_srid relabels the CRS, use the relabeled EPSG as the
            # "current" CRS for the warp skip-decision.  Without this, a tile
            # with pending_srid=3857 + tile.crs=4326 would skip the warp
            # (want==src_epsg==4326) even though the relabeled CRS is 3857.
            effective_src_epsg = pending_srid if pending_srid is not None else src_epsg
            if want is not None and want != effective_src_epsg:
                tile_bytes = _warp_window_bytes(src, window, want, pending=pending)
            else:
                tile_bytes = _window_dataset_bytes(src, window, pending=pending)
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
def _open(tile):
    """Context manager that yields an open ``DatasetReader`` for any tile shape.

    Accepts a ``VirtualTile``, raw bytes/bytearray, a v1 dict/Row
    ``{cellid, raster, metadata}``, or a v2 dict/Row (8-field struct).
    Normalises to ``VirtualTile`` via ``_to_virtual_tile`` then delegates to
    ``open_tile``; all lifecycle management (MemoryFile, staged temps) is
    handled there.
    """
    with open_tile(_to_virtual_tile(tile)) as ds:
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
            # Source has no nodata: apply pending default only if it fits.
            dtype_str = src.dtypes[0]
            self._resolved_nodata = (
                pending_nodata
                if _nodata_fits_dtype(pending_nodata, dtype_str)
                else None
            )
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


@contextmanager
def open_header(tile) -> Iterator[DatasetReader]:
    """Context manager yielding an open dataset for **header/metadata access only**.

    Unlike ``open_tile`` / ``_open``, this function never materialises a pixel
    window.  Callers may safely inspect ``.width``, ``.height``, ``.crs``,
    ``.transform``, ``.bounds``, ``.profile``, etc.  They must NOT call
    ``ds.read()`` — that would materialise pixels and defeats the purpose.

    Accepted tile shapes:
    - bytes / bytearray / v1 dict (``raster`` set) → open the bytes via the
      MemoryFile path (``_open``).  The bytes already contain the full result so
      header is trivially available; no window read is performed by this call.
    - virtual dict / v2 struct / VirtualTile (``raster`` None) → stage the path
      local if needed (e.g. /Volumes FUSE), open the source file with a plain
      ``rasterio.open`` (lazy, no pixel I/O). If the tile carries a sub-window
      (present AND not the full source extent), yield a read-free
      ``_WindowHeaderView`` whose ``width``/``height``/``transform``/``bounds``/
      ``profile`` reflect the WINDOW (consistent with the pixel path and the
      materialized-equivalent tile) while other fields proxy the source. For a
      whole-file window (None or == full extent) yield the source dataset
      directly. The ExitStack cleans up the staged temp on exit.
    """
    vt = _to_virtual_tile(tile)

    if not vt.is_virtual():
        # Bytes path: delegate to _open; no pixel read is performed here.
        with _open(vt) as ds:
            yield ds
        return

    # Virtual path: open the source header — no window read in either branch.
    with ExitStack() as stack:
        local_path, is_temp = _stage_local_if_needed(vt.path)
        if is_temp:
            stack.callback(_safe_remove, local_path)
        src = stack.enter_context(rasterio.open(local_path))

        # Parse pending instructions to reflect band-select / setsrid / nodata in header.
        bands, raw_nodata, srid = _parse_pending(vt.metadata)
        pending_count = len(bands) if bands else None
        pending_crs = None
        if srid is not None:
            import rasterio.crs as _rcrs

            pending_crs = _rcrs.CRS.from_epsg(srid)

        # pending_nodata is passed to _WindowHeaderView, which applies the same
        # ensure/preserve+dtype-fit logic as _window_dataset_bytes so header and
        # pixel views agree on nodata.
        pending_nodata = raw_nodata  # may be None

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


def materialize_to_bytes(tile: VirtualTile) -> VirtualTile:
    """Convert a (possibly virtual) tile to a v2-materialized tile: run open_tile
    on the light side (which CAN read /Volumes), capture the window+warp+clip
    result into `raster`, keep provenance. Output is heavy-consumable. This is
    the single sanctioned light->heavy crossing for virtual tiles.
    """
    with open_tile(tile) as ds:
        data = ds.read()
        profile = ds.profile.copy()
        profile.update(driver="GTiff")
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            raster = mf.read()
    return VirtualTile(
        cellid=tile.cellid,
        raster=raster,
        path=tile.path,
        window=tile.window,
        clip_polygon=tile.clip_polygon,
        clip_crs=tile.clip_crs,
        crs=tile.crs,
        # Pending keys are baked into the produced bytes; strip them so the
        # materialized tile's metadata stays honest (no double-application).
        metadata=_without_pending(tile.metadata),
    )


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
        pixel-producer returns a virtual tile.

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

        # Determine (col, row, w, h) for the filename.
        if vt.window is not None:
            col, row, w, h = vt.window
        else:
            # Read only the header to get dimensions.
            with _serde.open_tile(vt.raster) as ds:
                w, h = ds.width, ds.height
            col, row = 0, 0

        parts = [str(vt.cellid), str(col), str(row), str(w), str(h)]
        base = "_".join(parts) + ".tif"
        if virtualize_prefix:
            base = f"{virtualize_prefix}_{base}"

        os.makedirs(virtualize_dir, exist_ok=True)
        out_path = os.path.join(virtualize_dir, base)

        # FUSE-safe write: write to a local temp, then copy into place.
        fd, tmp = tempfile.mkstemp(suffix=".tif")
        try:
            os.close(fd)
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
        return VirtualTile(
            cellid=vt.cellid,
            raster=None,
            path=out_path,
            window=(col, row, w, h),
            clip_polygon=vt.clip_polygon,
            clip_crs=vt.clip_crs,
            crs=vt.crs,
            metadata=meta,
        )

    if materialize:
        if not vt.is_virtual():
            return vt
        return materialize_to_bytes(vt)

    # Auto: return as-is.
    return vt
