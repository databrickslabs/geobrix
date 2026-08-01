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
from contextlib import ExitStack, contextmanager
from typing import Iterator, Optional, Tuple

import numpy as np
import rasterio
from rasterio.io import DatasetReader, MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import _clip
from databricks.labs.gbx.pyrx.core.preparer import _stage_local_if_needed
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _epsg_of(crs) -> Optional[int]:
    """Parse 'EPSG:3857' / '3857' -> 3857; None if not a plain EPSG code."""
    s = str(crs).strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None


def _window_dataset_bytes(src, window: Window) -> bytes:
    """Read one window from an open dataset into standalone GTiff bytes."""
    data = src.read(window=window)
    profile = src.profile.copy()
    profile.update(
        driver="GTiff",
        height=int(window.height),
        width=int(window.width),
        transform=src.window_transform(window),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def _warp_window_bytes(src, window: Window, want_epsg: int) -> bytes:
    """Materialize a window, lazily reproject it to want_epsg, return GTiff bytes."""
    win_bytes = _window_dataset_bytes(src, window)
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
        with rasterio.open(local_path) as src:
            src_epsg = src.crs.to_epsg() if src.crs else None
            want = _epsg_of(tile.crs) if tile.crs else None
            if want is not None and want != src_epsg:
                tile_bytes = _warp_window_bytes(src, window, want)
            else:
                tile_bytes = _window_dataset_bytes(src, window)
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
        yield [stack.enter_context(open_tile(_to_virtual_tile(t))) for t in tiles]


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
        metadata=dict(tile.metadata or {}),
    )


def materialize(tile: VirtualTile) -> Tuple[np.ndarray, "rasterio.Affine", dict]:
    """Convenience wrapper: (array, transform, profile) from open_tile."""
    with open_tile(tile) as ds:
        return ds.read(), ds.transform, ds.profile.copy()
