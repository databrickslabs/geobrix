"""Spark-free aggregation reducers for the rasterx ``*_agg`` aggregates. Each reducer
takes plain Python inputs (lists of raster GTiff ``bytes``, ``(band_index, bytes)``
pairs, or ``(wkb, value)`` feature lists plus extent params) and returns the result
raster's GTiff ``bytes``.

These implement the aggregates:
  * ``merge_tiles``           -> gbx_rst_merge_agg (spatial mosaic)
  * ``combineavg_tiles``      -> gbx_rst_combineavg_agg (per-pixel mean, NoData-aware)
  * ``combine_{stat}_tiles``  -> gbx_rst_combine{min,max,sum,count,median,stddev}
  * ``frombands_tiles``       -> gbx_rst_frombands_agg (stack bands, ascending band_index)
  * ``rasterize_features``    -> gbx_rst_rasterize_agg (burn all features into one raster)
  * ``derivedband_tiles``     -> gbx_rst_derivedband_agg (user pyfunc across N tiles-as-bands)
  * ``align_to_tiles``        -> gbx_rst_align_to (warp tile to reference grid)
"""

import logging
import math
import os
import shutil
import uuid
import warnings
from contextlib import contextmanager
from functools import cmp_to_key
from typing import List, Optional, Tuple

import numpy as np
import shapely.wkb
from rasterio.enums import ColorInterp
from rasterio.errors import NodataShadowWarning
from rasterio.features import rasterize as _rasterize
from rasterio.io import MemoryFile
from rasterio.merge import merge as _rio_merge
from rasterio.transform import from_bounds

from databricks.labs.gbx._gdal_policy import (
    allowed_names,
    bytes_refused_light_names,
    open_bytes,
    open_data,
)
from databricks.labs.gbx.pyrx._env import (
    capped_block_cache,
    note_tile_hints,
    readdir_mode,
    readdir_option,
)
from databricks.labs.gbx.pyrx.core import _nodata
from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core import derivedband as _derivedband
from databricks.labs.gbx.pyrx.core import warp as _warp
from databricks.labs.gbx.pyrx.core._util import snap_bounds
from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
from databricks.labs.gbx.pyrx.core.budget import WindowOverReadCap, block_cache_for
from databricks.labs.gbx.pyrx.core.budget import decoded_bytes as _decoded_bytes
from databricks.labs.gbx.pyrx.core.budget import (
    over_read_cap,
    read_cap_refusal,
    size_text,
    tile_read_cap,
)
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

_log = logging.getLogger(__name__)


def _build_mosaic_vrt(*args, **kwargs):
    """``ds.cog_writer._build_mosaic_vrt``, imported where it is used: the writer module needs pyspark, which a Unity
    Catalog sandbox does not have, and only the streamed merge (a session) builds a VRT.
    """
    from databricks.labs.gbx.ds.cog_writer import _build_mosaic_vrt as build

    return build(*args, **kwargs)


_NODATA = -9999.0


def _open_all(rasters: List[bytes], gate: bool = True):
    """Open a list of GTiff byte buffers as rasterio datasets.

    Returns ``(memfiles, datasets)``; callers MUST close both (datasets first). ``gate`` refuses a member decoded
    over the read cap; the callers that read it window by window (:func:`_members`) do not need it.

    On a partial-open failure (e.g. a corrupt tile midway through the group) close
    the buffers opened so far before re-raising -- the caller's ``try/finally`` only
    runs once this returns, so without this a mid-loop failure would leak every
    MemoryFile/dataset already opened.
    """
    memfiles = []
    datasets = []
    try:
        for b in rasters:
            mf = MemoryFile(bytes(b))
            memfiles.append(mf)
            datasets.append(open_bytes(mf))
            # the decode of a member's pixels follows: the gate applies here, where a validity probe only opened it
            reason = over_read_cap(datasets[-1]) if gate else None
            if reason is not None:
                raise ReadCapRefused(reason)
    except Exception:
        _close_all(memfiles, datasets)
        raise
    return memfiles, datasets


def _close_all(memfiles, datasets):
    for ds in datasets:
        ds.close()
    for mf in memfiles:
        mf.close()


def _fill_reads_as_data(src) -> bool:
    """True when the pixels a warp of *src* does not reach would read as data: *src* has a mask
    band of its own (which reads ahead of any NoData) or declares no NoData and has no alpha
    band to carry the fill. A source with a NoData or an alpha band and no mask band is warped
    with it, and the fill is invalid already."""
    if _nodata.has_own_mask(src):
        return True
    return src.nodata is None and not _nodata.has_alpha(src)


def _reproject_dataset(src, dst_crs, target_res=None, anchor=None):
    """Reproject an open rasterio dataset to ``dst_crs``; return (memfile, dataset).

    Used by merge_tiles to reconcile a group whose tiles span multiple CRSs (e.g. a
    UTM zone boundary) before rasterio.merge, which requires a single CRS. Nearest
    resampling preserves the source values; the source NoData is carried through.
    Caller MUST close the returned dataset then memfile.

    Args:
        src:        Open rasterio DatasetReader to reproject.
        dst_crs:    Target CRS (rasterio CRS or any rasterio-accepted form).
        target_res: Optional ``(xres, yres)`` pixel size for the output.  When
                    provided, the reprojected tile is forced onto this pixel grid so
                    that tiles reprojected from different sources all share the same
                    pixel boundaries (fixing sub-pixel drift in the streaming merge
                    path).  ``None`` lets ``calculate_default_transform`` choose the
                    natural resolution.
        anchor:     Optional ``(left, top)`` corner of the reference tile (with
                    ``target_res``). The reprojected tile is put on that tile's pixel grid,
                    so its pixels line up with the reference tile's whatever the merge path:
                    ``rasterio.merge`` places a tile at ``floor(offset + 0.1)`` pixels and the
                    streamed VRT at ``round(offset)``, which differ by one pixel for a tile at a
                    fractional offset (0.5 to 0.9) and agree for a tile on the grid.
    """
    import rasterio
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    _res_kwarg = {"resolution": target_res} if target_res is not None else {}
    transform, width, height = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds, **_res_kwarg
    )
    if target_res is not None and anchor is not None:
        # The grid line at or beyond the tile's top-left corner, the size grown to cover it.
        left, bottom = transform.c, transform.f + height * transform.e
        right, top = left + width * transform.a, transform.f
        left, _, _, top, width, height = snap_bounds(
            left, bottom, right, top, target_res, anchor=anchor, tol=1e-6
        )
        transform = rasterio.Affine(target_res[0], 0.0, left, 0.0, -target_res[1], top)
    out_dtype = src.dtypes[0]
    decoded_bytes = src.count * width * height * np.dtype(out_dtype).itemsize
    profile = _comp.source_profile(src.profile)
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    # A source's own mask band (not an alpha band) is warped to the new grid and written
    # with the pixels; the warp itself leaves the masked pixels as valid zeros. The pixels
    # the warp does not reach are zeros as well, and nothing marks them when the source has
    # no NoData to fill with (or a mask band, which reads ahead of it) and no alpha band:
    # the tile is then written with the footprint of the warp as its mask band.
    mask = None
    if _nodata.has_own_mask(src):
        mask = _warp.warped_own_mask(
            src, dst_crs=dst_crs, transform=transform, width=width, height=height
        )
    if _fill_reads_as_data(src):
        covered = _warp.warped_footprint(
            src, dst_crs=dst_crs, transform=transform, width=width, height=height
        )
        mask = covered if mask is None else np.where(covered != 0, mask, 0)
    mf = MemoryFile()
    with _comp.internal_mask_env(mask):
        dst = mf.open(**profile)
        _comp.match_alpha(dst, src.colorinterp)
        for b in range(1, src.count + 1):
            reproject(
                source=rasterio.band(src, b),
                destination=rasterio.band(dst, b),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=dst_crs,
                src_nodata=src.nodata,
                dst_nodata=src.nodata,
                resampling=Resampling.nearest,
            )
        if mask is not None:
            dst.write_mask(mask)
    return mf, dst


def _merge_pixels(datasets):
    """``rasterio.merge`` of the tiles' pixels, the last tile that is valid at a pixel winning.

    A tile with a NoData declared beside an alpha band makes the merge's own masked read warn that
    the NoData shadows the alpha band. The warning is dropped: the merge only picks which tile's
    pixels land where, and the mosaic's validity is judged afterwards by the rule of both tiers
    (:func:`_nodata.valid_mask`, which reads the alpha values itself).
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NodataShadowWarning)
        return _rio_merge(datasets, method="last")


def _valid_u8(ds) -> np.ndarray:
    """``(rows, cols)`` uint8 (255 valid, 0 invalid) validity of *ds* by THE rule of both tiers,
    :func:`_nodata.valid_mask`: valid when any data band is (its NoData, its own mask band) and, with an
    alpha band, opaque. (``read_masks(1)`` is band 1's mask alone: it calls a pixel invalid whose band 1
    is NoData though another band holds data.)"""
    return np.where(_nodata.valid_mask(ds), 255, 0).astype("uint8")


def _write_masked_tile(ds, path, mask) -> None:
    """Write *ds* to *path* (a file path, or a ``MemoryFile``) as a GTiff whose own mask band is *mask*
    (``None``: no mask band), the alpha label kept, with the canonical creation options: never the
    source's codec, so a JPEG tile is not re-compressed lossily."""
    data = ds.read()
    profile = _comp.source_profile(ds.profile)
    profile.update(driver="GTiff")
    profile.update(
        _comp.creation_opts(
            profile["dtype"], decoded_bytes=data.nbytes, compress="auto"
        )
    )
    out = (
        path.open(**profile)
        if isinstance(path, MemoryFile)
        else open_data(path, "w", **profile)
    )
    with _comp.internal_mask_env(mask), out as dst:
        _comp.match_alpha(dst, ds.colorinterp)
        dst.write(data)
        if mask is not None:
            dst.write_mask(mask)


def _union_own_mask(datasets) -> np.ndarray:
    """``(rows, cols)`` uint8 (255 valid, 0 invalid) own mask of the mosaic ``rasterio.merge`` builds
    from *datasets*: valid where some tile that covers the pixel says it is valid, and a pixel no tile
    covers invalid.

    What a tile says is its validity by the rule of both tiers (:func:`_valid_u8`: any data band valid, its
    own mask band, its NoData, its alpha band), so a tile whose invalid pixels are NoData stays invalid
    there beside a tile with an own mask, and a pixel with data in one band only stays valid.

    Built by merging the tiles' mask rasters with the same geometry as the pixels (``method="max"``), so
    it lands on the pixels' grid. The mask comes from the tiles' masks, not from the merged values: a
    pixel whose value is 0 is not thereby invalid.
    """
    memfiles, masks = [], []
    try:
        for d in datasets:
            mf = MemoryFile()
            memfiles.append(mf)
            with mf.open(
                driver="GTiff",
                count=1,
                dtype="uint8",
                width=d.width,
                height=d.height,
                transform=d.transform,
                crs=d.crs,
            ) as dst:
                dst.write(_valid_u8(d), 1)
            masks.append(open_bytes(mf))
        merged, _ = _rio_merge(masks, method="max")
        return np.where(merged[0] != 0, 255, 0).astype("uint8")
    finally:
        _close_all(memfiles, masks)


def _pick_ref_crs(datasets):
    """Deterministic target CRS for a (possibly multi-CRS) group: the smallest EPSG
    code present. This makes every band's merge of the SAME cell agree on one CRS, so a
    later frombands can np.stack them. Same-CRS groups return that single CRS unchanged
    (fast path, no reprojection). Falls back to the first dataset's CRS."""
    from databricks.labs.gbx.core.crs import authority_srid_of

    best, best_epsg = None, None
    for ds in datasets:
        c = ds.crs
        if c is None:
            continue
        e = authority_srid_of(c)
        if e is None:
            best = best or c
            continue
        if best_epsg is None or e < best_epsg:
            best, best_epsg = c, e
    return best if best is not None else datasets[0].crs


def _reference_tile(datasets, ref_crs):
    """The tile whose grid the merge keeps: the first one in the reference CRS (its pixel size is in the
    reference CRS's units), else the first. ``_pick_ref_crs`` names a CRS one of the tiles has, so the
    tile that stands first in byte order is not the reference when it is in another CRS.
    """
    return next((d for d in datasets if d.crs == ref_crs), datasets[0])


def _pixel_size(ds):
    """``(xres, yres)`` of *ds* (positive), or ``None`` when it has no pixel size."""
    rx, ry = abs(ds.transform.a), abs(ds.transform.e)
    return (rx, ry) if rx > 0 and ry > 0 else None


def _grid_corner(ds):
    """``(left, top)`` of *ds*: where the pixel grid a reprojected tile is put on starts."""
    return ds.transform.c, ds.transform.f


def _bounds_in(ds, ref_crs):
    """``(left, bottom, right, top)`` of *ds* in the reference CRS (its own bounds when it is in it)."""
    if ref_crs is None or ds.crs is None or ds.crs == ref_crs:
        return tuple(ds.bounds)
    from rasterio.warp import transform_bounds

    return transform_bounds(ds.crs, ref_crs, *ds.bounds)


def _union_decoded_bytes(datasets, ref_crs) -> int:
    """Decoded size of the mosaic of *datasets* in the reference CRS: the union of their bounds in that
    CRS over the pixel size of the reference tile. 0 when the reference tile has no pixel size.
    """
    ref = _reference_tile(datasets, ref_crs)
    res = _pixel_size(ref)
    if res is None:
        return 0
    px_x, px_y = res
    boxes = [_bounds_in(d, ref_crs) for d in datasets]
    width = int(round((max(b[2] for b in boxes) - min(b[0] for b in boxes)) / px_x))
    height = int(round((max(b[3] for b in boxes) - min(b[1] for b in boxes)) / px_y))
    return width * height * ref.count * np.dtype(ref.dtypes[0]).itemsize


def _merge_budget() -> int:
    """Bytes of memory one merge or combine may work in, on this worker: the read cap (``pyrx.core.budget.tile_read_cap``), the
    one number the read gate of every member also judges by (the cap the driver carried into the call, which a worker
    cannot work out for itself, else this process's own budget). A group whose working set is over it is folded window by
    window or streamed through a VRT, so the peak does not grow with the number of members.
    """
    return tile_read_cap()


# A member too big to decode in RAM: merge_by_key(method="vrt") reads paths only, so the members need no RAM (its result
# is still BINARY, bounded by the same cap as every row).
_VRT_ROUTE = (
    "keep the tiles as files and mosaic them with "
    'databricks.labs.gbx.rasterx.helpers.merge_by_key(..., method="vrt")'
)
# A mosaic whose result is over the cap: no route that returns it in a row can help, so keep it as files (the
# documented VRT mosaic, docs/api/vrt-mosaic).
_MOSAIC_ROUTE = (
    "keep the tiles as files on a Volume and build the mosaic over them with "
    'databricks.labs.gbx.rasterx.helpers.mint_vrt(paths, out="/Volumes/...") '
    "(or the cog_gbx writer with vrtMosaic=true), then read the VRT"
)
_COMBINE_ROUTE = (
    "combine the tiles per key, outside the aggregate, with "
    "databricks.labs.gbx.rasterx.helpers.combine_by_key(...)"
)
# A combine whose result is over the cap: smaller results first, or combine_by_key where the result fits the cap.
# Retiling changes the shape of the result, so the Fix says so: grouped by key and sub-tile it is one raster per sub-tile.
_COMBINE_OUT_ROUTE = (
    "retile the tiles first (gbx_rst_retile) so each result is smaller (the result is then one raster per sub-tile, "
    "not one per key), or combine them per key, outside the aggregate, with "
    "databricks.labs.gbx.rasterx.helpers.combine_by_key(...) where the result fits"
)
# A VRT (mint_vrt, merge_by_key(method="vrt")) needs every member on one CRS and pixel size: a mixed group is brought to
# one grid first, as merge_tiles itself does onto its reference grid.
_ONE_GRID = (
    "bring the tiles to one CRS and pixel size first (gbx_rst_transform to one SRID, gbx_rst_resample_to_res to one "
    "pixel size), then "
)


def _mixed_grid(hdrs, ref_crs) -> bool:
    """True when the members are not on one CRS and pixel size (what ``merge_tiles`` reprojects onto its reference grid),
    so a VRT over them cannot be built until they are."""
    if any(
        h.crs is not None and ref_crs is not None and h.crs != ref_crs for h in hdrs
    ):
        return True
    sizes = [s for s in (_pixel_size(h) for h in hdrs) if s is not None]
    return any(
        not (
            math.isclose(s[0], sizes[0][0], rel_tol=1e-9)
            and math.isclose(s[1], sizes[0][1], rel_tol=1e-9)
        )
        for s in sizes[1:]
    )


def _vrt_fix(route: str, mixed: bool) -> str:
    """*route*, led by the step that gives a mixed group the one grid its VRT needs."""
    return _ONE_GRID + route if mixed else route


class _Hdr:
    """What a merge reads of a member's header: it stands in for an open dataset in the size gate and the choice of
    the reference tile, so no member stays open (or decoded) while the group is sized.
    """

    __slots__ = (
        "crs",
        "bounds",
        "transform",
        "count",
        "dtypes",
        "width",
        "height",
        "decoded",
        "own_mask",
        "alpha",
        "fill_as_data",
        "encoded",
    )


def _header_of(ds, encoded: int) -> _Hdr:
    """What a merge reads of the header of an open dataset *ds* (or of a view of a window of one): *encoded* is
    the size of the member's bytes (the in-RAM estimate counts them)."""
    h = _Hdr()
    h.crs, h.bounds, h.transform = ds.crs, tuple(ds.bounds), ds.transform
    h.count, h.dtypes = ds.count, tuple(ds.dtypes)
    h.width, h.height, h.decoded = ds.width, ds.height, _decoded_bytes(ds)
    h.own_mask, h.alpha = _nodata.has_own_mask(ds), _nodata.has_alpha(ds)
    h.fill_as_data = _fill_reads_as_data(ds)
    h.encoded = encoded
    return h


def _bytes_header(b: bytes) -> _Hdr:
    """The header of one member's bytes (no pixel is read; one copy of the member is open)."""
    with MemoryFile(bytes(b)) as mf, open_bytes(mf) as ds:
        return _header_of(ds, len(b))


def _scan_headers(rasters: List[bytes]) -> List[_Hdr]:
    """The header of each member, opened one at a time (no pixel is read, and one copy of one member is in memory)."""
    return [_bytes_header(b) for b in rasters]


def _reprojected_decoded(h, ref_crs, res) -> int:
    """Decoded size of member *h* once it is reprojected onto the reference grid: its bounds in the reference CRS over
    the reference pixel size, and the pixel the snap to that grid can add on each side. *h*'s own decoded size when the
    size cannot be worked out (a CRS that does not transform its bounds, no pixel size).
    """
    if res is None:
        return h.decoded
    try:
        left, bottom, right, top = _bounds_in(h, ref_crs)
        cols = math.ceil((right - left) / res[0]) + 2
        rows = math.ceil((top - bottom) / res[1]) + 2
    except Exception:  # noqa: BLE001
        return h.decoded
    return int(cols * rows * h.count * np.dtype(h.dtypes[0]).itemsize)


# Headroom over the summed terms of _in_ram_bytes: allocator slack and the temporaries of GDAL that no term names.
_IN_RAM_MARGIN = 1.15


def _in_ram_bytes(hdrs: List[_Hdr], ref_crs, union: int) -> int:
    """What the in-RAM merge holds at its peak, an upper estimate (a group over the budget is streamed instead).

    Terms, in the order the merge allocates them:

    * the mosaic array ``rasterio.merge`` fills (*union*);
    * the temporaries of ``rasterio.merge`` for the largest source it reads: the source window (the member at its
      target size) as a masked array, so its pixels plus a one-byte mask per sample, and the copy it composites;
    * a decoded copy of each member in another CRS (at its TARGET size, the reprojected tile) or with an alpha band
      (its own size), each in a ``MemoryFile`` beside the open source, and a one-byte mask of each member when the
      group has masks (own masks, or tiles reprojected: the footprint of the warp is written as a mask);
    * the union-sized mask of the mosaic (``_union_own_mask``): the merged mask, the ``np.where`` of it (int64) and its
      uint8 cast, about ten bytes a pixel, when the group has masks;
    * the members' bytes twice (the list the caller holds and the ``MemoryFile`` each is opened from);
    * the encoded mosaic, at most the mosaic decoded (compression can only do better), and its copy ``read()`` returns.

    The sum is scaled by ``_IN_RAM_MARGIN``. Measured peak RSS of the in-RAM merge (adjacent 16 MiB float32 tiles, zstd,
    one CRS, a fresh process, over the heap before the call): 2, 4 and 8 tiles peaked at 3.7, 4.2 and 4.1 times the
    mosaic, and 3.8, 4.5 and 4.3 times with a NoData value. The sum alone was 1.00 to 1.33 times the peak (no margin at
    all for 4 tiles with NoData); with the margin it is 1.15 to 1.53 times.
    """
    ref = _reference_tile(hdrs, ref_crs)
    res = _pixel_size(ref)
    px_bytes = max(1, ref.count * np.dtype(ref.dtypes[0]).itemsize)
    union_px = union // px_bytes
    other = [
        ref_crs is not None and h.crs is not None and h.crs != ref_crs for h in hdrs
    ]
    masked = any(other) or any(h.own_mask for h in hdrs)
    held, widest, encoded = 0, 0, 0
    for h, o in zip(hdrs, other):
        size = _reprojected_decoded(h, ref_crs, res) if o else h.decoded
        samples = size // max(1, np.dtype(h.dtypes[0]).itemsize)
        widest = max(widest, size + 2 * samples)
        if o or h.alpha:
            held += size
        if masked:
            held += samples // max(1, h.count)
        encoded += h.encoded
    total = union + widest + held + 2 * encoded + 2 * union
    if masked:
        total += 10 * union_px
    return int(total * _IN_RAM_MARGIN)


def _merge_refusal(decoded: int, cap: int, mixed: bool = False) -> str:
    """A member that must be decoded whole (reprojected, or rewritten with its mask) is over the budget (*decoded* is
    the size it is decoded at: for a reprojected one, the reprojected tile). *mixed*: the group is not on one CRS and
    pixel size, which its VRT route needs first."""
    return f"{read_cap_refusal(decoded, cap)}, or {_vrt_fix(_VRT_ROUTE, mixed)}"


def _output_refusal(size: int, cap: int, route: str) -> str:
    """A result whose encoded bytes are over what one row may hold: the bytes of a BINARY aggregate come back whole, so
    they are bounded by the same cap the members are."""

    def fmt(n: int) -> str:
        return f"{n >> 20} MiB" if n >= (1 << 20) else f"{n >> 10} KiB"

    return (
        f"the result is {fmt(size)} encoded, over this compute's cap for the bytes of one row ({fmt(cap)}). "
        f"Fix: {route}"
    )


def _byte_drivers() -> list:
    """The drivers a member's bytes may use (``open_bytes``': the allowed ones less the class that can name other files)."""
    return [n for n in allowed_names() if n not in set(bytes_refused_light_names())]


class _Entry:
    """One member of a merge, as the spool keeps it (the header, and where the member is: never its pixels).

    ``kind``: ``"b"`` a bytes member (``data`` in memory, or ``path`` a file of the spool); ``"r"`` a reference the VRT
    names as it is (``path`` the source file, ``window`` the part of it, ``vt`` the virtual tile); ``"m"`` a reference
    with instructions (a clip, a CRS, a pending band, nodata or SRID, a Zarr store) that was written out once,
    one at a time, as bytes (``data`` / ``path``). ``order`` places a reference among the references (see
    :func:`_ref_order`); a bytes member has none, it is placed by its bytes.
    """

    __slots__ = ("kind", "hdr", "data", "path", "window", "vt", "order", "hold")

    def __init__(
        self,
        kind,
        hdr,
        *,
        data=None,
        path=None,
        window=None,
        vt=None,
        order=None,
        hold=None,
    ):
        self.kind, self.hdr, self.data, self.path = kind, hdr, data, path
        self.window, self.vt, self.order, self.hold = window, vt, order, hold

    def read_bytes(self) -> bytes:
        """The member as GTiff bytes in memory (the in-RAM merge's input; a reference is read now, one at a time)."""
        if self.kind == "r":
            from databricks.labs.gbx.pyrx.core import open_tile as ot

            return ot.materialize_to_bytes(self.vt).raster
        if self.data is not None:
            return self.data
        with open(self.path, "rb") as fh:
            return fh.read()

    def release(self) -> None:
        """Let go of the member's bytes (and its spool file): the merge needs them no more."""
        self.data = None
        if self.kind != "r" and self.path is not None:
            try:
                os.remove(self.path)
            except OSError:
                pass
            self.path = None


@contextmanager
def _open_member(e: _Entry):
    """The member as an open dataset: a reference's window is read (and its instructions applied) by the one front
    door, a bytes member is opened under the byte-open driver policy, from memory or from its spool file.
    """
    if e.kind == "r":
        from databricks.labs.gbx.pyrx.core import open_tile as ot

        with ot._open(e.vt) as ds:
            yield ds
    elif e.data is not None:
        with MemoryFile(e.data) as mf, open_bytes(mf) as ds:
            yield ds
    else:
        ds = open_data(e.path, driver=_byte_drivers())
        try:
            yield ds
        finally:
            ds.close()


def _sources_readdir_mode(entries: List[_Entry]) -> Optional[str]:
    """``EMPTY_DIR`` when every member is a reference to a file with nothing beside it (the reader's stamp, or the
    worker's listing of its folder, says so: :func:`pyrx._env.readdir_mode`), else None (the open keeps its setting).

    GDAL opens a VRT's sources itself, when the COG pass reads them, with the process setting (a by-name check of every
    sidecar, a dozen look-ups an open): on a Volume's FUSE that is most of the time of a merge by reference. The option
    is read at each open and is per thread, so the pass runs under it. A group with a bytes member keeps the setting:
    its scratch files are the merge's own, and a rewritten one may have a sidecar.
    """
    if not entries or any(e.kind != "r" for e in entries):
        return None
    for e in entries:
        # a stamp lapses (it lapses after a time): a long merge renews it here
        note_tile_hints(e.path, e.vt.metadata)
    modes = {readdir_mode(e.path) for e in entries}
    return "EMPTY_DIR" if modes == {"EMPTY_DIR"} else None


def _stream_entries(
    entries: List[_Entry],
    scratch: str,
    max_out=None,
    to_dir=None,
    prefix=None,
    spool_dir=None,
):
    """Out-of-core merge path: stream the members through a VRT to a COG file.

    *entries* are in fold order, a later member winning where members overlap (:func:`_fold_order`). Used when the
    mosaic, or what the in-RAM merge would hold beside it (:func:`_in_ram_bytes`), exceeds the merge budget
    (:func:`_merge_budget`). ``max_out`` bounds the COG's encoded size (a BINARY aggregate returns it whole). With
    ``to_dir`` the COG is copied from *scratch* straight to that directory and its path is returned: it is never read
    back. *scratch* is a local directory the caller owns. *spool_dir*, if given, maps the size of a bytes member to the
    directory it is written to when it needs nothing done to it (the spool's overflow, see :class:`_MemberSpool`).

    A reference with no instruction to apply (a whole file, or a window of one) goes into the VRT as it is, its window
    as the source rectangle: nothing of it is read before the COG pass. A bytes member that needs nothing done to it
    is a file of the scratch (written when it arrived, or here) the VRT names. A member that is reprojected, or
    rewritten with its mask, is decoded whole, one at a time, into a new file of the scratch, and let go of.

    DETERMINISM: ``_build_mosaic_vrt`` emits ComplexSource entries (with ``<NODATA>`` masking) in the entries' order;
    GDAL's last-valid-source-wins resolution matches ``method="last"`` nodata handling in the in-RAM path.

    MASKS: when some tile has an own mask band the mosaic carries the union of the tiles' masks
    (``mask,1`` of each scratch tile), which is band 1's mask alone for a tile without one. Each such
    tile is rewritten with its validity by the rule of both tiers (:func:`_valid_u8`: any band) as its
    mask, so a pixel with data in one band only stays valid, as in the in-RAM merge. So is every tile with
    an alpha band, in any group: GDAL's mask of a tile with its own mask band, or of a 3-band gray+x+alpha
    one, does not follow the alpha band, and that of RGBA leaves the alpha band itself out, so a
    transparent pixel of a later tile would hide an opaque one under it. A group with no mask band of its
    own gets no mask band in the mosaic (the alpha band carries it).

    MEMORY: peak RSS is one member (decoded whole only for a reprojected one or one rewritten with its mask) + GDAL's
    COG cache (<=200 MiB); the full decoded mosaic array is NEVER allocated, no member is held open while the next is
    written, and a reference is never read into memory. A member decoded whole is refused with a Fix
    (:class:`ReadCapRefused`) when it is over the merge budget at the size it is decoded at (the reprojected tile, for a
    reprojected one).
    """
    # Pass 1: the headers (read when each member arrived), to determine ref_crs and per-tile CRS.
    # Also capture the reference tile's pixel size so CRS-mismatched tiles can
    # be reprojected onto the same pixel grid (fixing sub-pixel drift).
    hdrs = [e.hdr for e in entries]
    ref_crs = _pick_ref_crs(hdrs)
    mixed = _mixed_grid(hdrs, ref_crs)
    tile_crses = [h.crs for h in hdrs]
    ref_tile = _reference_tile(hdrs, ref_crs)
    ref_pixel_res = _pixel_size(ref_tile)
    ref_corner = _grid_corner(ref_tile)
    has_mask = [h.own_mask for h in hdrs]
    has_alpha = [h.alpha for h in hdrs]
    # a tile reprojected below is written with the footprint of its warp as a mask
    # band when its fill would read as data (see :func:`_reproject_dataset`)
    has_mask = [
        m
        or (
            ref_crs is not None
            and h.crs is not None
            and h.crs != ref_crs
            and h.fill_as_data
        )
        for m, h in zip(has_mask, hdrs)
    ]

    # Pass 2: one member at a time. In a group with an own-masked tile every tile without one is written with its
    # validity as its mask band (see MASKS above).
    group_masked = any(has_mask)
    tile_paths, windows = [], []
    budget = _merge_budget()
    for i, (e, tile_crs) in enumerate(zip(entries, tile_crses)):
        tile_path, window = os.path.join(scratch, f"tile_{i}.tif"), None
        needs_reproject = (
            ref_crs is not None and tile_crs is not None and tile_crs != ref_crs
        )
        rewrite = (group_masked and not has_mask[i]) or has_alpha[i]
        if needs_reproject or rewrite:
            # this member is decoded whole to be reprojected or rewritten with its mask, at the size of the tile
            # that results (the reprojected one is on the reference grid); a member copied to the scratch dir as
            # it is is only ever read a window at a time, whatever its size
            whole = (
                _reprojected_decoded(hdrs[i], ref_crs, ref_pixel_res)
                if needs_reproject
                else hdrs[i].decoded
            )
            if whole > budget:
                raise ReadCapRefused(_merge_refusal(whole, budget, mixed))
            with _open_member(e) as src:
                if needs_reproject:
                    # Reproject to ref_crs, snapping to the reference pixel grid via
                    # target_res so all tiles share the same pixel boundaries and the
                    # VRT mosaic has no sub-pixel placement drift.  Nearest resampling
                    # preserves source values; nodata is carried through.
                    rep_mf, rep_ds = _reproject_dataset(
                        src, ref_crs, target_res=ref_pixel_res, anchor=ref_corner
                    )
                    try:
                        _write_masked_tile(
                            rep_ds,
                            tile_path,
                            (
                                _valid_u8(rep_ds)
                                if group_masked or has_alpha[i]
                                else None
                            ),
                        )
                    finally:
                        rep_ds.close()
                        rep_mf.close()
                else:
                    _write_masked_tile(src, tile_path, _valid_u8(src))
            e.release()
        elif e.kind == "r":
            # a reference with nothing to apply: the VRT names the file, and the window is its source rectangle
            tile_path, window = e.path, e.window
        elif e.path is not None:
            tile_path = e.path  # a bytes member the spool wrote when it arrived
        else:
            # Same CRS (or no CRS): write raw GTiff bytes straight to disk, and let go of them.
            if spool_dir is not None:
                tile_path = os.path.join(spool_dir(len(e.data)), f"tile_{i}.tif")
            with open(tile_path, "wb") as fh:
                fh.write(e.data)
            e.data = None
        tile_paths.append(tile_path)
        windows.append(window)

    # Build a GDAL VRT mosaic index (pure rasterio + ElementTree, no osgeo).
    # VRT emits ComplexSource entries (with <NODATA> masking) in tile_paths order;
    # GDAL last-valid-source-wins on overlapping pixels (ComplexSource skips nodata
    # values), matching method="last" nodata handling in the in-RAM path.
    # The mosaic carries a mask band only where the group had one; a tile with an alpha band was
    # rewritten with its validity as its mask so that it is composited by it, not by that band.
    extra = {"windows": windows} if any(w is not None for w in windows) else {}
    vrt_path = _build_mosaic_vrt(
        tile_paths, scratch, vrt_paths="absolute", carry_mask=group_masked, **extra
    )

    # Stream-convert the VRT to a COG (bounded GDAL cache; never decodes
    # the full mosaic into a numpy array).
    cog_path = os.path.join(scratch, "mosaic.tif")
    with readdir_option(_sources_readdir_mode(entries)):
        cog_convert_file(vrt_path, cog_path)

    if to_dir is not None:
        return _publish_cog(cog_path, to_dir, prefix)
    # Read the COG back as bytes and return (a bound on a BINARY aggregate's row is judged before the read).
    if max_out is not None and os.path.getsize(cog_path) > max_out:
        raise OutputOverBudget(
            _output_refusal(
                os.path.getsize(cog_path), max_out, _vrt_fix(_MOSAIC_ROUTE, mixed)
            )
        )
    with open(cog_path, "rb") as fh:
        return fh.read()


def _publish_cog(cog_path: str, to_dir: str, prefix) -> str:
    """Copy the file at *cog_path* into *to_dir* (a Volume directory) as ``[<prefix>_]0_0_0_<w>_<h>_<id>.tif`` and return
    the path: the bytes of the mosaic are never read into Python. The name is the one ``open_tile.shape_output`` gives
    a produced tile of cell 0, plus a random id: two groups with a mosaic of one size would otherwise be written to
    the same file, the second over the first. The copy is content-only (``copyfile``: a Volume FUSE mount refuses the
    stat calls of ``copy2``).
    """
    with open_data(cog_path) as ds:
        w, h = ds.width, ds.height
    name = f"0_0_0_{w}_{h}_{uuid.uuid4().hex[:12]}.tif"
    out = os.path.join(to_dir, f"{prefix}_{name}" if prefix else name)
    os.makedirs(to_dir, exist_ok=True)
    shutil.copyfile(cog_path, out)
    return out


def _merge_tiles_streaming(sorted_rasters: List[bytes], max_out=None) -> bytes:
    """The out-of-core merge of members that are bytes already in fold order (:func:`_stream_entries`): each is
    written to a local scratch directory one at a time, a VRT is built over them and streamed to a COG, and the COG
    is read back as bytes."""
    scratch = new_local_temp_dir("gbx_merge")
    try:
        entries = [
            _Entry("b", h, data=b)
            for h, b in zip(_scan_headers(sorted_rasters), sorted_rasters)
        ]
        return _stream_entries(entries, scratch, max_out)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


class ReadCapRefused(ValueError):
    """A member of a group is decoded over the read cap (the reason and Fix every level gives). Where the aggregate's
    result is a tile it is that group's error tile; where it is bytes the aggregate raises with it (a NULL would read as
    an empty group).
    """


class OutputOverBudget(ReadCapRefused):
    """The encoded result of a BINARY aggregate is over the cap for the bytes of one row (see :func:`_output_refusal`)."""


class ArrayOverBudget(ReadCapRefused):
    """A stack of members is past what a per-pixel combine can fold in the memory budget of one call: one row of it,
    all members at that row, is over the budget (see :func:`_fold_windows`). The scalar array functions answer it with
    that row's error tile and its Fix."""


def refuse_group_over_task_cap(n: int, decoded: int, cap: Optional[int] = None) -> None:
    """Raise :class:`ReadCapRefused` when the *n* members of a group, *decoded* bytes together, are over *cap* (the read
    cap the task carries, else this worker's own). A Serverless task holds the whole group in one process before any
    merge begins, so the merge cannot bound it and the task would end in an out-of-memory kill: the group is refused
    before that, with the route that needs no RAM for the members.
    """
    cap = tile_read_cap() if cap is None else cap
    if decoded > cap:
        raise ReadCapRefused(
            f"the {n} tiles of this group are {size_text(decoded)} decoded together, over what one task of this "
            f"compute holds ({size_text(cap)}). Fix: {_VRT_ROUTE}"
        )


class MosaicTooLarge(ValueError):
    """The mosaic of a group is past what a merge can stream (see :func:`merge_tiles`). It is a ValueError for a
    caller that lets it stop the query; one whose result is a tile turns it into that group's error tile, and an
    aggregate whose result is bytes raises it."""


def _merge_size_refusal(n: int, mixed: bool) -> str:
    """The refusal of a merge that may not stream when the size of its mosaic cannot be worked out."""
    return (
        f"the size of the mosaic of these {n} tiles could not be worked out, so it cannot be held to this "
        f"compute's memory budget for one call. "
        f"Fix: {_vrt_fix(_MOSAIC_ROUTE, mixed)}"
    )


def _merge_route(hdrs, ref_crs, mixed: bool, n: int, stream: bool) -> str:
    """How a merge of the members whose headers are *hdrs* is built: ``"stream"`` is the in-RAM merge (the name is the
    COG decision's), ``"driver"`` the streamed one through a VRT and a COG. Raises :class:`MosaicTooLarge` for a mosaic
    past what a merge can stream; with ``stream=False`` a mosaic that would need streaming raises
    :class:`ReadCapRefused` instead. A failure to work out the size falls back to the in-RAM path, except with
    ``stream=False``: a mosaic whose size is unknown is not built in memory under a budget, it is refused.
    """
    union = None
    try:
        union = _union_decoded_bytes(hdrs, ref_crs)
    except Exception:
        _log.debug(
            "merge_tiles: size-gate computation failed; falling back to in-RAM path",
            exc_info=True,
        )
    if union is None:
        if not stream:
            raise ReadCapRefused(_merge_size_refusal(n, mixed))
        return "stream"
    budget = _merge_budget()
    how = "stream"
    if stream:
        from databricks.labs.gbx.ds.file_gbx import (
            _COG_DRIVER_MAX_BYTES,
            materialize_decision,
        )

        how = materialize_decision(union, "cog_write", cap_bytes=budget)
        if how == "error":
            raise MosaicTooLarge(
                f"the mosaic of these {n} tiles would be {union / 2**30:.0f} GiB decoded, over the "
                f"{_COG_DRIVER_MAX_BYTES / 2**30:.0f} GiB a merge can stream: tiles far apart (or far apart "
                f"once in one CRS) make an extent that is almost all NoData. Fix: merge the tiles of one "
                f"place and one CRS together (key the group by them), or retile first; to mosaic the files of "
                f"one place, {_vrt_fix(_MOSAIC_ROUTE, mixed)}"
            )
    elif union > budget:
        how = "driver"
    if how == "stream" and _in_ram_bytes(hdrs, ref_crs, union) > budget:
        how = "driver"
    if how == "driver" and not stream:
        raise ReadCapRefused(
            f"the mosaic of these {n} tiles is over this compute's memory budget "
            f"({size_text(budget)}) for one call. Fix: {_vrt_fix(_MOSAIC_ROUTE, mixed)}"
        )
    return how


class Ref:
    """A group member kept by reference: a virtual tile (a path, a window and any instruction), no pixels.

    *tile* is a ``VirtualTile`` or a tile struct (a dict or Row) of the V2 schema. A whole-file or windowed
    reference goes into the merge's VRT as its own file (the window as the source rectangle) and is never read into
    memory; one with instructions is written out once, one at a time (see :class:`_Entry`).
    """

    __slots__ = ("tile", "hold")

    def __init__(self, tile, hold=None):
        self.tile = tile
        self.hold = hold  # an object the file depends on (a FileRef): kept alive until the merge is done with the member


def _ref_order(vt) -> tuple:
    """Where a reference folds among the references: by ``(path, window)`` (a whole file before any of its windows),
    then by what else tells two references of one window apart (clip, CRS, instructions), so the order is total.
    """
    window = tuple(vt.window) if vt.window is not None else (-1, -1, -1, -1)
    return (
        vt.path or "",
        window,
        bytes(vt.clip_polygon or b""),
        vt.clip_crs or "",
        vt.crs or "",
        tuple(sorted((vt.metadata or {}).items())),
    )


def _plain_ref(vt):
    """``(local path, window or None, header)`` of a virtual tile the VRT can name as it is, else None.

    Plain: a window of a file with no bytes, no clip, no target CRS, no pending instruction and no Zarr store (the
    tile ``open_tile.with_plain_window`` takes), whose file the VRT can read: it is a local or FUSE absolute path (not a cloud URI or a /vsi name), it opens directly (a Volume file is
    probed with the transient-miss retry; a file that needs staging to local disk is not plain), is not a VRT itself,
    is north-up and not GCP-georeferenced, has bands, and the window lies inside it. The header is the window's.
    """
    from rasterio.windows import Window

    from databricks.labs.gbx.core._retry import _retry_transient
    from databricks.labs.gbx.ds._listing import to_local_path
    from databricks.labs.gbx.ds.file_gbx import _is_fuse_path, _probe_direct_open
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    # the reader's word about this file's folder (no sidecar beside it; how many files it holds), as every other open
    # of a tile takes it: without it the first opens of a worker list a folder of tens of thousands of names
    _env.note_tile_hints(vt.path, vt.metadata)
    if (
        vt.raster is not None
        or not vt.path
        or vt.clip_polygon is not None
        or vt.crs
        or ot._has_pending(vt.metadata)
        or ot._zarr_tiles.is_zarr_tile(vt)
    ):
        return None
    local = to_local_path(vt.path)
    # a VRT names a file GDAL opens by that name: a local or FUSE path. A cloud URI (s3://, abfss://, gs://) or a /vsi
    # name is read through the front door, which hands it to rasterio, the one that turns a URI into a GDAL file system
    if "://" in local or local.startswith("/vsi") or not os.path.isabs(local):
        return None
    if _is_fuse_path(local):
        try:
            _probe_direct_open(local)
        except Exception:  # noqa: BLE001 - the file needs staging: not plain
            return None
    src = _retry_transient(lambda: ot.open_raster(local))
    try:
        t = src.transform
        if (
            src.driver == "VRT"
            or src.count < 1
            or t.b != 0
            or t.d != 0
            or not t.a > 0
            or not t.e < 0
            or _warp.gcp_georef(src) is not None
        ):
            return None
        window, view = None, src
        if vt.window is not None:
            c, r, w, h = (int(v) for v in vt.window)
            if (
                min(c, r) < 0
                or min(w, h) <= 0
                or c + w > src.width
                or r + h > src.height
            ):
                return None
            if (c, r, w, h) != (0, 0, src.width, src.height):
                window = (c, r, w, h)
                view = ot._WindowHeaderView(src, Window(c, r, w, h))
        hdr = _header_of(view, 0)
        hdr.encoded = (
            hdr.decoded
        )  # no bytes exist: the estimate counts the decoded size, an upper bound
        return local, window, hdr
    finally:
        src.close()


# Free local disk a spool write must leave: the COG the merge writes, and the files it rewrites, need room too.
_LOCAL_DISK_RESERVE = 1 << 29


class _MemberSpool:
    """The members of one merge, kept without their pixels.

    ``add`` takes a member at a time: bytes, or a :class:`Ref`. Its header is read at once and nothing of it is kept
    but that, and, for a bytes member, its bytes: in memory, or (``spill=True``) in files of a local scratch
    directory once they pass a quarter of the merge budget. A caller whose members are in memory already, in the
    Arrow batch of an aggregate, passes ``spill=False``: files would gain nothing. A reference with nothing to
    apply is kept as its path and window; one with instructions is written out once, as bytes, and kept like a
    bytes member.

    ``result`` merges them in the fold order of :meth:`ordered`; ``close`` removes the scratch. With
    ``overflow_parent`` (a Volume directory) a spool file that would leave the local disk under
    :data:`_LOCAL_DISK_RESERVE` is written to a scratch directory under it instead (``ds._scratch.new_scratch_dir``):
    the COG and the members that are rewritten stay on the local disk.
    """

    def __init__(self, spill: bool = False, overflow_parent: Optional[str] = None):
        self.entries: List[_Entry] = []
        self._spill_on = spill
        self._limit = _merge_budget() // 4
        self._held = 0
        self._spilled = False
        self._local: Optional[str] = None
        self._overflow: Optional[str] = None
        self._overflow_parent = overflow_parent
        self._written = 0

    def __len__(self):
        return len(self.entries)

    # -- adding --
    def add(self, member, *, drop_bytes: bool = False, drop_refs: bool = False) -> bool:
        """Take one member. False for a member that is dropped (``drop_bytes`` / ``drop_refs``: its bytes, or its
        file, do not open); otherwise a member that cannot be opened raises."""
        if isinstance(member, Ref):
            try:
                self._add_ref(member)
            except ReadCapRefused:
                raise  # a member over the cap is a refusal with its Fix, never a member dropped
            except Exception:  # noqa: BLE001
                if drop_refs:
                    return False
                raise
            return True
        try:
            hdr = _bytes_header(member)
        except Exception:  # noqa: BLE001
            if drop_bytes:
                return False
            raise
        e = _Entry("b", hdr)
        self._keep(e, bytes(member))
        self.entries.append(e)
        return True

    def _add_ref(self, ref: Ref) -> None:
        from databricks.labs.gbx.pyrx.core import open_tile as ot

        vt = ot._to_virtual_tile(ref.tile)
        if vt.raster is not None:  # bytes after all
            e = _Entry("b", _bytes_header(vt.raster))
            self._keep(e, bytes(vt.raster))
            self.entries.append(e)
            return
        plain = _plain_ref(vt)
        if plain is not None:
            local, window, hdr = plain
            self.entries.append(
                _Entry(
                    "r",
                    hdr,
                    path=local,
                    window=window,
                    vt=vt,
                    order=_ref_order(vt),
                    hold=ref.hold,
                )
            )
            return
        # the one front door: the window, the instructions and the clip are applied as for any tile
        try:
            data = ot.materialize_to_bytes(vt).raster
        except (
            WindowOverReadCap
        ) as exc:  # decoded whole to apply its instructions: the merge's Fix names its route
            raise ReadCapRefused(f"{exc}, or {_vrt_fix(_VRT_ROUTE, False)}") from None
        e = _Entry("m", _bytes_header(data), order=_ref_order(vt))
        self._keep(e, data)
        self.entries.append(e)

    # -- where the bytes of a member are kept --
    def scratch(self) -> str:
        """The local scratch directory (the rewritten members, the VRT and the COG go here)."""
        if self._local is None:
            self._local = new_local_temp_dir("gbx_merge")
        return self._local

    def _spool_dir(self, need: int) -> str:
        local = self.scratch()
        if self._overflow is None and self._overflow_parent:
            if shutil.disk_usage(local).free < need + _LOCAL_DISK_RESERVE:
                from databricks.labs.gbx.ds._scratch import new_scratch_dir

                self._overflow = new_scratch_dir(self._overflow_parent)
                os.makedirs(self._overflow, exist_ok=True)
        return self._overflow or local

    def _write(self, data: bytes) -> str:
        self._written += 1
        path = os.path.join(self._spool_dir(len(data)), f"member_{self._written}.tif")
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    def _keep(self, e: _Entry, data: bytes) -> None:
        if not self._spill_on or (
            not self._spilled and self._held + len(data) <= self._limit
        ):
            e.data = data
            self._held += len(data)
            return
        if not self._spilled:
            self._spilled = True
            for held in self.entries:
                if held.data is not None:
                    held.path, held.data = self._write(held.data), None
            self._held = 0
        e.path = self._write(data)

    # -- the order --
    def ordered(self) -> List[_Entry]:
        """The members in fold order: the bytes members first, sorted by their bytes (the order of the former JVM
        aggregate), then the references, sorted by ``(path, window)``. The last folds last and wins an overlap.
        """
        by_bytes = [e for e in self.entries if e.kind == "b"]
        refs = sorted((e for e in self.entries if e.kind != "b"), key=lambda e: e.order)
        if all(e.data is not None for e in by_bytes):
            by_bytes.sort(key=lambda e: e.data)
        else:
            by_bytes.sort(key=cmp_to_key(lambda x, y: _compare_files(x, y)))
        return by_bytes + refs

    # -- the merge --
    def result(self, max_out=None, stream: bool = True, to_dir=None, prefix=None):
        """The merge of the members: GTiff bytes, ``None`` for no member, or, with ``to_dir``, the path of the file
        written there (see :func:`merge_members`)."""
        order = self.ordered()
        if not order:
            return None
        n = len(order)
        if n == 1 and (order[0].kind != "r" or order[0].hdr.decoded <= _merge_budget()):
            single = order[0].read_bytes()
            if to_dir is not None:
                return self._publish(single, to_dir, prefix)
            return _bounded(single, max_out, _vrt_fix(_MOSAIC_ROUTE, False))
        hdrs = [e.hdr for e in order]
        ref_crs = _pick_ref_crs(hdrs)
        mixed = _mixed_grid(hdrs, ref_crs)
        how = _merge_route(hdrs, ref_crs, mixed, n, stream)

        def streamed():
            return _stream_entries(
                order, self.scratch(), max_out, to_dir, prefix, self._spool_dir
            )

        if how == "driver":
            return streamed()
        rasters = [e.read_bytes() for e in order]

        def fall_back():
            rasters.clear()
            return streamed()

        merged = _merge_in_ram(rasters, ref_crs, mixed, max_out, stream, fall_back)
        return (
            merged
            if to_dir is None or isinstance(merged, str)
            else self._publish(merged, to_dir, prefix)
        )

    def _publish(self, data: bytes, to_dir: str, prefix) -> str:
        """Write mosaic *data* (built in RAM) to *to_dir*, by local file and copy as a streamed one is."""
        local = os.path.join(self.scratch(), "published.tif")
        with open(local, "wb") as fh:
            fh.write(data)
        return _publish_cog(local, to_dir, prefix)

    def close(self) -> None:
        if self._local is not None:
            shutil.rmtree(self._local, ignore_errors=True)
            self._local = None
        if self._overflow is not None:
            from databricks.labs.gbx.ds._scratch import remove_scratch_dir

            remove_scratch_dir(self._overflow)
            self._overflow = None
        for e in self.entries:
            e.data = None
            e.hold = None

    def __del__(
        self,
    ):  # a merge that failed before it closed the spool: its scratch goes with it
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass


def _compare_files(x: _Entry, y: _Entry) -> int:
    """-1, 0 or 1 as the bytes of the spool file of *x* sort before, with or after those of *y* (a chunk at a time)."""
    with open(x.path, "rb") as fa, open(y.path, "rb") as fb:
        while True:
            ca, cb = fa.read(1 << 20), fb.read(1 << 20)
            if ca != cb:
                n = min(len(ca), len(cb))
                if ca[:n] != cb[:n]:
                    return -1 if ca[:n] < cb[:n] else 1
                return -1 if len(ca) < len(cb) else 1
            if not ca:
                return 0


def merge_members(
    members,
    max_out=None,
    stream: bool = True,
    to_dir=None,
    prefix=None,
    spill: bool = False,
    overflow_parent=None,
):
    """Merge the group's members into one spatial mosaic: GTiff bytes, ``None`` for no member.

    *members* is an iterable of GTiff ``bytes`` and :class:`Ref` (a virtual tile kept by reference), taken one at a
    time. This is the one core under :func:`merge_tiles` and every grouped merge (the aggregate and its force-output
    form, the array merge, the table function, ``merge_by_key``): what a member costs is judged where it arrives,
    so the memory of the merge does not grow with the number of members that are references. See
    :func:`merge_tiles` for the rules of the merge itself.

    OVERLAP ORDER: the last member in the fold order wins where members overlap, and the fold order is defined: the
    bytes members first, sorted by their raw GTiff bytes, then the references, sorted by ``(path, window)``. A
    reference folds after a bytes member, so it wins an overlap with one; a group of bytes members is ordered as it
    always was.

    With ``to_dir`` (a Volume directory) the mosaic is copied from the local scratch to
    ``[<prefix>_]0_0_0_<w>_<h>_<id>.tif`` there and the path is returned, a ``str``: a streamed mosaic's bytes are never
    read back into Python. ``spill``: keep the bytes members in
    local scratch files once they pass a quarter of the merge budget (for a caller that is handed them one at a
    time and holds none). ``overflow_parent``: see :class:`_MemberSpool`.
    """
    spool = _MemberSpool(spill=spill, overflow_parent=overflow_parent)
    try:
        for m in members:
            spool.add(m)
        return spool.result(max_out, stream, to_dir, prefix)
    finally:
        spool.close()


def _merge_in_ram(rasters, ref_crs, mixed, max_out, stream, fall_back):
    """The in-RAM merge of *rasters* (GTiff bytes in fold order): ``rasterio.merge`` over the open members. A member
    decoded over the read cap that this path would hold whole goes to *fall_back* (the streamed merge) instead.
    """
    memfiles, datasets = [], []
    extra = []  # (memfile, dataset) pairs for reprojected sources, closed in finally
    try:
        try:
            memfiles, datasets = _open_all(rasters)
        except ReadCapRefused as exc:
            if not stream:
                raise ReadCapRefused(
                    f"{exc}, or {_vrt_fix(_VRT_ROUTE, mixed)}"
                ) from exc
            # a member decoded over the read cap that the in-RAM path would hold whole: the streaming path copies it as
            # it is (or refuses it with its own Fix if it must be decoded whole there)
            return fall_back()
        # Reconcile CRS before merging: real AOIs that straddle a UTM zone boundary
        # (e.g. Sentinel-2 over SE Alaska -> EPSG:32608 + 32609) yield groups whose
        # tiles span multiple CRSs, but rasterio.merge requires one. Reproject any
        # mismatched source to the reference CRS, onto the pixel size of the reference
        # tile (a tile in that CRS, whichever stands first in the fold order): the merge takes
        # the pixel size of its first tile, which is then that size whichever tile that
        # is, as in the streamed merge. Same-CRS groups are untouched.
        ref_tile = _reference_tile(datasets, ref_crs)
        ref_res, ref_corner = _pixel_size(ref_tile), _grid_corner(ref_tile)
        merge_ds = []
        for ds in datasets:
            if ref_crs is None or ds.crs == ref_crs:
                merge_ds.append(ds)
            else:
                mf, rds = _reproject_dataset(
                    ds, ref_crs, target_res=ref_res, anchor=ref_corner
                )
                extra.append((mf, rds))
                merge_ds.append(rds)
        ref = merge_ds[0]
        # A tile with its own mask band (not an alpha band): the mosaic carries the union of the
        # tiles' masks, built from the masks themselves (:func:`_union_own_mask`), never from the
        # pixel values: a valid pixel of 0 stays valid. Tiles without one are merged as before,
        # with no mask band.
        # The pixels are composited by the tiles' validity, as the mask union is: a tile with an alpha band
        # is merged as a copy whose mask band is its validity, since GDAL's mask of one with its own mask
        # band, or of a 3-band gray+x+alpha one, ignores the alpha band, and that of RGBA leaves the alpha
        # band itself out (a transparent pixel of a later tile would hide an opaque one under it, or keep
        # the colours and zero the alpha). The mosaic gets a mask band only if a tile had one.
        group_masked = any(_nodata.has_own_mask(d) for d in merge_ds)
        composite = []
        for d in merge_ds:
            if _nodata.has_alpha(d):
                copy_mf = MemoryFile()
                # Listed before the write, so the ``finally`` closes it even if the write
                # fails (rasterio's MemoryFile frees its in-memory file only on close()).
                extra.append((copy_mf, None))
                _write_masked_tile(d, copy_mf, _valid_u8(d))
                copy = open_bytes(copy_mf)
                extra[-1] = (copy_mf, copy)
                d = copy
            composite.append(d)
        mosaic, out_transform = _merge_pixels(composite)
        own_mask = _union_own_mask(merge_ds) if group_masked else None
        out_dtype = str(mosaic.dtype)
        decoded_bytes = mosaic.nbytes
        profile = _comp.source_profile(ref.profile)
        profile.update(
            driver="GTiff",
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            count=mosaic.shape[0],
            transform=out_transform,
        )
        profile.update(
            _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
        )
        with _comp.internal_mask_env(own_mask), MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                _comp.match_alpha(dst, ref.colorinterp)
                dst.write(mosaic)
                if own_mask is not None:
                    dst.write_mask(own_mask)
            out = out_mf.read()
        return _bounded(out, max_out, _vrt_fix(_MOSAIC_ROUTE, mixed))
    finally:
        for mf, rds in extra:
            if rds is not None:
                rds.close()
            mf.close()
        _close_all(memfiles, datasets)


def merge_tiles(rasters: List[bytes], max_out=None, stream: bool = True) -> bytes:
    """Merge the group's tile rasters into one spatial mosaic (GTiff bytes). ``max_out`` bounds the encoded result: a
    BINARY aggregate returns it whole in a row, so a larger one raises :class:`OutputOverBudget` with a Fix.
    ``stream=False`` is for a caller that cannot stream (a Unity Catalog function: no scratch COG, no pyspark): a group
    the merge would stream, or a member it must decode over the budget, raises :class:`ReadCapRefused` with the same
    Fix instead, and nothing outside this module's pyspark-free imports is read.

    The members are bytes, held by the caller; a caller whose members are tiles that may be references, or that
    receives them one at a time, calls :func:`merge_members`, which this is a call of.

    Each GTiff carries its own georef/CRS, so ``rasterio.merge.merge`` places
    them by extent and the output spans the union extent (the same mosaic as
    ``gdalbuildvrt -resolution highest``). On overlap we use ``method="last"`` so
    the LAST source in the fold order wins, as in ``gdalbuildvrt`` (overlapping
    pixels take the last-listed source).

    DETERMINISM: a Spark ``groupBy().agg()`` does not guarantee the order rows
    reach the reducer, so a last-wins mosaic would otherwise pick a different
    overlap winner from run to run. To make the fold
    order-invariant we sort the inputs by their raw GTiff byte content -- a total
    order intrinsic to each tile that has NO ties for distinct content -- before
    merging; the highest-bytes tile folds last and wins the overlap. The former
    JVM aggregate sorted on the identical key (the same serialized tile bytes
    each row carries), so the winner is the same for ALL inputs
    -- including same-origin overlapping tiles, which a geotransform-origin key
    could not separate (it tied on origin and fell back to a per-open
    ``/vsimem/<uuid>`` description, i.e. random). Raw bytes are bitwise-identical
    across tiers, so no cross-tier hash agreement is required. A tile that is a reference
    (:class:`Ref`, see :func:`merge_members`) has no bytes to sort by: it folds after every bytes member, by
    ``(path, window)``.

    PIXEL PLACEMENT (tiles in another CRS): such a tile is reprojected onto the pixel
    size AND the pixel grid of the reference tile (:func:`cellraster.snap_bounds`; the grid
    line at or beyond its top-left corner, so it can grow by up to a pixel on each
    side). The in-RAM merge and the streamed one (a VRT) place a tile by different
    rounding, ``floor(offset + 0.1)`` pixels in ``rasterio.merge`` and ``round(offset)``
    in the VRT. A reprojected tile whose corner is off the reference grid can set the
    mosaic's origin, which makes the offset of every other tile from it fractional: at
    0.5 to 0.9 of a pixel the two merges put that tile one pixel apart (measured: a
    12-pixel 4326 tile, reprojected, beside a 12-pixel 3857 tile at 50 km pixels; the
    3857 tile covered columns 15-26 in RAM and 16-27 streamed, the reprojected one 0-12
    in both). On the reference grid every offset is whole and both paths agree.

    MASKS (tiles in another CRS): the pixels of the reprojected tile's box that the warp does not
    reach are zeros, which read as data unless the tile has a NoData, an alpha band or a mask band
    to mark them. A tile with none of those, and every tile with a mask band of its own, is
    written with the footprint of its warp as its mask band (:func:`_reproject_dataset`), so it
    does not overwrite the pixels of the tile beside it, and the mosaic then carries a mask band
    (a pixel no tile reaches is invalid). A group whose tiles are all in the reference CRS is
    merged as before: a gap between tiles with no NoData is zeros, valid.

    IN RAM AND STREAMED: the two merges give the same grid, NoData and pixels, tile by tile: each
    source is skipped where it is NoData by ITS OWN NoData (``rasterio.merge`` reads every source
    that way, and the streamed mosaic index takes the NoData of each tile, not of the first). They
    differ in layout, so never in a byte comparison: the in-RAM result is a striped zstd GeoTIFF, the
    streamed one a Cloud Optimized GeoTIFF (512-pixel tiles, overviews, DEFLATE).

    TILES IN ANOTHER CRS: both are mosaicked. The former JVM aggregate did not merge them.
    It built a ``gdalbuildvrt`` mosaic, which skips a tile whose projection differs, and
    returned ONE tile, the one that sorts first in byte order (a 4326 and a 3857 tile
    gave a single 12 x 12 tile).

    SIZE GATE: the headers are read one member at a time (no pixel is read and no member stays open), and the
    canonical COG decision (`materialize_decision`, kind "cog_write") says what to do with the decoded size of the
    union-extent mosaic: within the memory budget of this call (`_merge_budget`: the read cap the driver carried) the
    merge is built in RAM ("stream"); beyond it the merge streams through a VRT and a COG ("driver": bounded GDAL
    cache, the mosaic is never decoded whole); beyond the COG ceiling it stops with a Fix ("error"). Tiles a continent
    apart (or in places far apart once put in one CRS) make an extent of terabytes that is nearly all NoData:
    streaming that takes hours and a COG of gigabytes, so it is refused with a Fix: before anything is opened for
    reading or written. In RAM the merge also holds a copy of each member it must reproject or rewrite, a mask per
    member, and the members open beside the mosaic, so a group that adds up to more than the budget streams too: the
    peak does not grow with the number of members. A failure to compute the size falls back to the in-RAM path rather
    than crashing.
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return _bounded(bytes(rasters[0]), max_out, _vrt_fix(_MOSAIC_ROUTE, False))
    return merge_members(rasters, max_out=max_out, stream=stream)


# The working set of a per-pixel fold, in bytes per pixel of one band in the window (a multiplier on the window, not a
# cap): the members read as float64 or float32 beside the running values, the validity masks and the temporaries of
# the reduction. A stack of N members adds the member arrays held together.
_FOLD_PX = {"avg": 40, "running": 32, "stack": 40}


@contextmanager
def _members(rasters: List[bytes], budget: int):
    """The group's members open for windowed reads, yielded as a list of datasets.

    A group whose encoded bytes fit a quarter of the budget is opened from memory; a larger one is spooled to local
    scratch files first and opened by path, so a second copy of the whole group is never held in RAM (GDAL reads a
    window of a file a block at a time). No member is decoded whole either way: that is the caller's windows.
    """
    cache = block_cache_for(
        budget
    )  # the members stay open: their blocks must not pile up in GDAL's cache
    if sum(len(r) for r in rasters) <= budget // 4:
        memfiles, datasets = _open_all(rasters, gate=False)
        try:
            with capped_block_cache(cache):
                yield datasets
        finally:
            _close_all(memfiles, datasets)
        return
    scratch = new_local_temp_dir("gbx_combine")
    datasets = []
    # the drivers a tile's bytes may use (open_bytes': the allowed ones less the class that can name other files)
    drivers = [n for n in allowed_names() if n not in set(bytes_refused_light_names())]
    try:
        for i, b in enumerate(rasters):
            # the byte-open policy holds for the spool: a member of a driver that can name other files is refused
            # (DriverPolicyError) before it is written, and the file is opened with the same driver list
            open_bytes(b).close()
            path = os.path.join(scratch, f"member_{i}.tif")
            with open(path, "wb") as fh:
                fh.write(b)
            datasets.append(open_data(path, driver=drivers))
        with capped_block_cache(cache):
            yield datasets
    finally:
        for ds in datasets:
            ds.close()
        shutil.rmtree(scratch, ignore_errors=True)


def _fold_windows(shape, kind: str, n: int, budget: int, block_rows: int = 1):
    """The row windows a per-pixel fold of ``n`` members of ``shape`` (bands, rows, cols) runs in, sized so one window's
    working set is within half the budget (the other half is the output, GDAL's cache and the group's own bytes). One
    window when the whole canvas fits. A single row of the stack over the budget cannot be folded: refused with the Fix.

    A window is a whole number of the output's blocks (``block_rows`` tall) when the budget allows one, so each
    compressed block of the output is written by one window, never rewritten by the next.
    """
    from rasterio.windows import Window

    bands, rows, cols = shape
    per_px = _FOLD_PX[kind] * (n + 1 if kind == "stack" else 1)
    per_row = bands * cols * per_px
    step = (budget // 2) // per_row
    if step < 1:
        size = (
            f"{per_row >> 20} MiB" if per_row >= (1 << 20) else f"{per_row >> 10} KiB"
        )
        have = f"{budget >> 20} MiB" if budget >= (1 << 20) else f"{budget >> 10} KiB"
        raise ArrayOverBudget(
            f"the {n} tiles are too many to combine at {cols} pixels wide: one row of them needs {size}, over this "
            f"compute's memory budget ({have}) for one call. Fix: {_COMBINE_ROUTE}, or retile them first "
            "(gbx_rst_retile) into smaller tiles, which gives one result per sub-tile, not one per key"
        )
    if step >= block_rows > 1:
        step -= step % block_rows
    step = min(step, rows)
    return [Window(0, r0, cols, min(step, rows - r0)) for r0 in range(0, rows, step)]


def _bounded(out: bytes, max_out, route: str) -> bytes:
    """*out*, or :class:`OutputOverBudget` when it is over ``max_out`` (None: no bound)."""
    if max_out is not None and len(out) > max_out:
        raise OutputOverBudget(_output_refusal(len(out), max_out, route))
    return out


def combineavg_tiles(rasters: List[bytes], max_out=None) -> bytes:
    """Per-pixel mean across the group's aligned tiles, ignoring NoData (GTiff bytes).

    Each tile's declared NoData is excluded from BOTH the sum and the divisor; a
    valid ``0`` counts toward the mean. Where every input at a pixel is NoData, the
    output cell carries the first declared input NoData (or 0 if none declared one), and
    that NoData value is stamped on the output band.

    NOTE: the former JVM form built a VRT with ``-resolution highest``
    which tolerated differing grids; this reducer assumes the tiles are
    ALREADY aligned (same shape/extent/CRS) and raises ``ValueError`` if their
    raster shapes differ, rather than silently resampling.

    SCALE: the group is folded window by window (rows of the canvas), each window one member at a time into a
    running sum + count, so the peak is ~O(one window of the canvas + 2 float64/int64 accumulators), independent of
    the group size N and of how large a member is. A canvas that fits the budget (:func:`_merge_budget`) is one window,
    which is the plain accumulate-then-write; stacking all N tiles as float64 (the obvious form) peaks at ~4x the raw
    group bytes -- a 16-tile group of 1024x1024x4 float32 hit ~1.2 GB and would OOM a Spark Python worker.
    A row of the canvas too wide for the budget is refused with a Fix (:class:`ArrayOverBudget`). ``max_out`` bounds
    the encoded result for a BINARY aggregate (:class:`OutputOverBudget`).
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return bytes(rasters[0])
    budget = _merge_budget()
    with _members(rasters, budget) as datasets:
        ref = datasets[0]
        shape = (ref.count, ref.height, ref.width)
        for ds in datasets[1:]:
            if (ds.count, ds.height, ds.width) != shape:
                raise ValueError(
                    "rst_combineavg_agg requires aligned tiles (same shape); got "
                    f"{(ds.count, ds.height, ds.width)} vs {shape}"
                )
        ref_profile = _comp.source_profile(ref.profile)
        ref_labels = list(ref.colorinterp)
        out_dtype = ref.dtypes[0]
        nodatas = [ds.nodata for ds in datasets]
        any_nodata = any(nd is not None for nd in nodatas)
        fallback = next((nd for nd in nodatas if nd is not None), 0.0)

        ref_profile.update(driver="GTiff")
        if any_nodata:
            ref_profile.update(nodata=fallback)
        decoded_bytes = int(np.prod(shape)) * np.dtype(out_dtype).itemsize
        ref_profile.update(
            _comp.creation_opts(
                str(np.dtype(out_dtype)), decoded_bytes=decoded_bytes, compress="auto"
            )
        )
        with MemoryFile() as out_mf:
            with out_mf.open(**ref_profile) as dst:
                _comp.match_alpha(dst, ref_labels)
                windows = _fold_windows(
                    shape, "avg", len(datasets), budget, dst.block_shapes[0][0]
                )
                for window in windows:
                    h, w = window.height, window.width
                    sums = np.zeros((shape[0], h, w), dtype="float64")
                    counts = np.zeros((shape[0], h, w), dtype="int64")
                    for ds, nd in zip(datasets, nodatas):
                        arr = ds.read(window=window).astype("float64")
                        if nd is None:
                            sums += arr
                            counts += 1
                        else:
                            valid = arr != nd
                            sums += np.where(valid, arr, 0.0)
                            counts += valid
                    means = np.where(counts > 0, sums / np.maximum(counts, 1), fallback)
                    if np.issubdtype(np.dtype(out_dtype), np.integer):
                        means = np.rint(means)
                    dst.write(means.astype(out_dtype), window=window)
            return _bounded(out_mf.read(), max_out, _COMBINE_OUT_ROUTE)


def frombands_tiles(indexed: List[Tuple[int, bytes]]) -> bytes:
    """Stack single-band (or multi-band) tiles into one multi-band tile (GTiff bytes).

    *indexed* is a list of ``(band_index, raster_bytes)``. The list is sorted by
    ``band_index`` ASCENDING (the critical ordering guarantee of
    RST_FromBandsAgg), then each tile's band(s) are concatenated in that order.
    Georef/CRS/dtype/nodata are taken from the first (lowest-index) tile. The stack has an
    alpha band only when its last band is the alpha band of the last tile; where every
    tile has its own mask band the stack has the union of them.
    """
    if not indexed:
        return None
    ordered = sorted(indexed, key=lambda t: int(t[0]))
    rasters = [b for _, b in ordered]
    memfiles, datasets = _open_all(rasters)
    try:
        import rasterio
        from rasterio.warp import Resampling, reproject

        ref = datasets[0]
        # Align every band onto the reference (lowest band-index) grid before stacking.
        # In an agg/grid context the per-band source tiles can have slightly different
        # extents/shapes (e.g. uneven scene coverage across bands, or a UTM-zone-boundary
        # cell), so a bare np.stack would fail. This mirrors the heavyweight RST_FromBands,
        # which builds a VRT and gdal_translate-resamples (bilinear) the bands to one grid.
        bands = []
        labels = []  # the colour interpretation of every band of the stack, in order
        masks = []  # per tile: its own mask on the reference grid (bool), or None
        for ds in datasets:
            aligned = (
                ds.width == ref.width
                and ds.height == ref.height
                and ds.transform == ref.transform
                and ds.crs == ref.crs
            )
            labels.extend(ds.colorinterp)
            if _nodata.has_own_mask(ds):
                own = (
                    ds.read_masks(1)
                    if aligned
                    else _warp.warped_own_mask(
                        ds,
                        dst_crs=ref.crs,
                        transform=ref.transform,
                        width=ref.width,
                        height=ref.height,
                    )
                )
                masks.append(own != 0)
            else:
                masks.append(None)
            for i in range(1, ds.count + 1):
                if aligned:
                    bands.append(ds.read(i))
                else:
                    dest = np.empty((ref.height, ref.width), dtype=ds.dtypes[i - 1])
                    reproject(
                        source=rasterio.band(ds, i),
                        destination=dest,
                        src_transform=ds.transform,
                        src_crs=ds.crs,
                        dst_transform=ref.transform,
                        dst_crs=ref.crs,
                        src_nodata=ds.nodata,
                        dst_nodata=ds.nodata,
                        resampling=Resampling.bilinear,
                    )
                    bands.append(dest)
        data = np.stack(bands)
        profile = _comp.source_profile(ref.profile)
        profile.update(driver="GTiff", count=data.shape[0])
        # A pixel of the stack is valid when a band of it is (a data band valid, the rule of
        # both tiers), so the stack keeps an own mask only where EVERY tile has one: the union
        # of their masks. A tile without one is valid everywhere, and so is the stack.
        mask = None
        if all(m is not None for m in masks):
            mask = np.where(np.logical_or.reduce(masks), 255, 0).astype("uint8")
        # The alpha band of the stack is the last band, when it is the alpha band of the
        # last tile (``write_gtiff`` -> ``match_alpha``); GDAL would otherwise label band 4
        # of every 4-band uint8 stack alpha.
        return _comp.write_gtiff(profile, data, alpha_like=labels, mask=mask)
    finally:
        _close_all(memfiles, datasets)


def rasterize_features(
    features: List[Tuple[bytes, float]],
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
) -> bytes:
    """Burn all ``(geom_wkb, value)`` features into ONE raster (GTiff bytes).

    Mirrors RST_RasterizeAgg: features are burned over the extent
    ``[xmin,ymin,xmax,ymax]`` at ``width_px x height_px`` in EPSG:``srid``, the
    value carried as the burn attribute. Overlap is LAST-WINS in feature order
    (rasterio burns the shape list in order, last write per cell wins). Pixels
    touched by no feature get NoData (-9999.0).

    DETERMINISM: a Spark ``groupBy().agg()`` does not guarantee the order
    features reach the reducer, so a last-wins burn would otherwise pick a
    different overlap winner from run to run. To make the fold order-invariant we
    burn features in a canonical order, sorted by ``(geom_wkb, value)`` -- a
    stable key intrinsic to each feature. The former JVM aggregate burned
    in the same canonical order, so overlaps resolve the same way.
    """
    if not features:
        return None
    width_px = int(width_px)
    height_px = int(height_px)
    transform = from_bounds(
        float(xmin), float(ymin), float(xmax), float(ymax), width_px, height_px
    )
    ordered = sorted(
        (
            (bytes(wkb), float(v))
            for wkb, v in features
            if wkb is not None and len(bytes(wkb)) > 0
        ),
        key=lambda t: (t[0], t[1]),
    )
    shapes = [(shapely.wkb.loads(wkb), v) for wkb, v in ordered]
    if not shapes:
        return None
    arr = _rasterize(
        shapes,
        out_shape=(height_px, width_px),
        transform=transform,
        fill=_NODATA,
        dtype="float64",
    )
    decoded_bytes = arr.nbytes
    profile = dict(
        driver="GTiff",
        width=width_px,
        height=height_px,
        count=1,
        dtype="float64",
        crs=f"EPSG:{int(srid)}",
        transform=transform,
        nodata=_NODATA,
    )
    profile.update(
        _comp.creation_opts("float64", decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr, 1)
        return mf.read()


def derivedband_tiles(rasters: List[bytes], python_func: str, func_name: str) -> bytes:
    """Apply a user GDAL VRT pixel function across the group's tiles (GTiff bytes).

    Each tile in the group contributes one input band (its band 1); the N tiles
    are stacked into one N-band raster, then the pyfunc (``func_name`` entry
    point) is run across the bands -- mirroring RST_DerivedBandAgg, which feeds
    the N group rasters as N inputs to the same pixel function. Georef/CRS come
    from the first tile. Returns a single-band Float64 raster.

    SECURITY: ``python_func`` is exec'd in-process without sandboxing -- treat as
    trusted developer code (same stance as the existing pyrx derivedband).
    """
    if not rasters:
        return None
    memfiles, datasets = _open_all(rasters)
    try:
        ref = datasets[0]
        bands = [ds.read(1) for ds in datasets]
        data = np.stack(bands)
        # The tiles are stacked as plain bands, none of them alpha (GDAL would label band 4
        # of a 4-band uint8 stack alpha, and the pixel function would then see an alpha band).
        stacked_bytes = _comp.write_gtiff(
            {**ref.profile, "count": data.shape[0]},
            data,
            alpha_like=[ColorInterp.undefined] * data.shape[0],
        )
    finally:
        _close_all(memfiles, datasets)

    with MemoryFile(stacked_bytes) as mf:
        with open_bytes(mf) as ds:
            return _derivedband.derivedband(ds, str(python_func), str(func_name))


# ---------------------------------------------------------------------------
# Cross-raster combine family (6 stats)
# ---------------------------------------------------------------------------
# Per-pixel/per-band reduction over VALID (non-NoData)
# values across a stack of ALIGNED tiles. Alignment is a hard precondition: inputs
# must share identical CRS, extent, pixel size, and dimensions. A pixel that is
# NoData in ALL inputs produces NoData in the output.
#
# Conventions (the rules of the former JVM combine functions):
#   median : np.ma.median (mean of two middle values on even count)
#   stddev : population (ddof=0), masked.std() default
#   count  : number of valid (non-NoData) inputs; all-NoData pixel -> NoData, NOT 0
#   dtype  : output dtype preserved from the first input; integer output rounded
#            via np.rint then cast with numpy's default (unsafe) cast — matches
#            heavy: `if np.issubdtype(dtype, np.integer): result = np.rint(result);
#            result.astype(dtype)`.
# ---------------------------------------------------------------------------

_REL_TOL = 1e-6


def _approx_eq(a: float, b: float) -> bool:
    """Relative tolerance comparison."""
    return abs(a - b) <= _REL_TOL * (1.0 + abs(b))


def _crs_equal(a_crs, b_crs) -> bool:
    """True when two rasterio CRS values represent the same CRS.

    Both-None counts as same (grid-native convention, mirrors sameCrs in Scala).
    Uses pyproj for semantic equivalence where possible.
    """
    a_empty = a_crs is None
    b_empty = b_crs is None
    if a_empty and b_empty:
        return True
    if a_empty != b_empty:
        return False
    # Fast path: identical string representation → equal, skip pyproj overhead.
    if str(a_crs) == str(b_crs):
        return True
    try:
        from databricks.labs.gbx.core.crs import crs_equal

        return crs_equal(a_crs, b_crs)
    except Exception:
        return str(a_crs) == str(b_crs)


def _gt_coeffs(ds):
    """Return the 6 GDAL geotransform coefficients from a rasterio dataset."""
    t = ds.transform
    # GDAL order: [xoff, xscale, xrot, yoff, yrot, yscale]
    return (t.c, t.a, t.b, t.f, t.d, t.e)


def _require_aligned(datasets, func_name: str) -> None:
    """Raise ValueError if any dataset does not match datasets[0]'s grid.

    Checks: identical (width, height), geotransform within _REL_TOL, same CRS.
    A stack of 0 or 1 tiles is trivially aligned (mirrors requireAligned in Scala).
    The error message always contains 'align' and points at gbx_rst_align_to.
    """
    if len(datasets) <= 1:
        return
    ref = datasets[0]
    ref_gt = _gt_coeffs(ref)
    for i, ds in enumerate(datasets[1:], start=1):
        if ds.width != ref.width or ds.height != ref.height:
            raise ValueError(
                f"{func_name} requires all input tiles to share an identical grid "
                f"(CRS, extent, pixel size, and dimensions), but tile {i} does not "
                f"match tile 0. "
                f"tile 0: {ref.width}x{ref.height}px; tile {i}: {ds.width}x{ds.height}px. "
                f"Cross-raster combine does NOT resample or reproject its inputs — "
                f"align them first with gbx_rst_align_to(tile, reference_tile)."
            )
        ds_gt = _gt_coeffs(ds)
        if not all(_approx_eq(ds_gt[j], ref_gt[j]) for j in range(6)):
            raise ValueError(
                f"{func_name} requires all input tiles to share an identical grid, "
                f"but tile {i} has a different geotransform. "
                f"Align them first with gbx_rst_align_to(tile, reference_tile)."
            )
        if not _crs_equal(ref.crs, ds.crs):
            raise ValueError(
                f"{func_name} requires all input tiles to share an identical grid, "
                f"but tile {i} has a different CRS from tile 0. "
                f"Align them first with gbx_rst_align_to(tile, reference_tile)."
            )


# min and max fold one tile at a time into a running value; the others need the whole stack.
_RUNNING_STATS = {"min": (np.inf, np.minimum), "max": (-np.inf, np.maximum)}


def _running_stat(datasets, nodatas, stat: str, fallback, window=None) -> np.ndarray:
    """min or max over the tiles (of ``window``, else whole), one tile in memory at a time (plus the running value and count).

    float32 like the stacked reduction, so the values are the same; NaN propagates as it does there.
    """
    init, op = _RUNNING_STATS[stat]
    acc = counts = None
    for ds, nd in zip(datasets, nodatas):
        arr = ds.read(window=window).astype("float32")  # (bands, h, w)
        valid = np.ones(arr.shape, dtype=bool) if nd is None else arr != nd
        if acc is None:
            acc = np.full(arr.shape, init, dtype="float32")
            counts = np.zeros(arr.shape, dtype="int64")
        acc = op(acc, np.where(valid, arr, np.float32(init)))
        counts += valid
    return np.where(counts > 0, acc.astype("float64"), fallback)


def _stacked_stat(datasets, nodatas, stat: str, fallback, window=None) -> np.ndarray:
    """sum, count, median or stddev over the stack of tiles (N, bands, h, w), of ``window`` when given."""
    # sum/stddev/median need float64 precision to avoid accumulation error; count is exact in float32.
    _ACCUM_STATS = {"sum", "stddev", "median"}
    stack_dtype = "float64" if stat in _ACCUM_STATS else "float32"
    # Stack: (N, bands, h, w) — mirrors in_ar layout in the VRT pixel function.
    stacked = np.stack(
        [ds.read(window=window).astype(stack_dtype) for ds in datasets], axis=0
    )

    # Build per-tile validity mask: (N, bands, h, w).
    valid = np.ones(stacked.shape, dtype=bool)
    for i, nd in enumerate(nodatas):
        if nd is not None:
            valid[i] = stacked[i] != nd

    counts = valid.sum(axis=0)  # (bands, h, w)
    masked = np.ma.masked_array(stacked, mask=~valid)

    if stat == "sum":
        raw_stat = masked.sum(axis=0)
    elif stat == "count":
        # count = number of valid inputs; all-NoData pixel -> NoData (not 0).
        raw_stat = counts.astype("float64")
    elif stat == "median":
        raw_stat = np.ma.median(masked, axis=0)  # mean-of-two on even count
    elif stat == "stddev":
        raw_stat = masked.std(axis=0)  # population std, ddof=0
    else:
        raise ValueError(
            f"_combine_stat_tiles: unsupported stat '{stat}'; "
            "expected one of: min, max, sum, count, median, stddev"
        )

    # Fill masked/NoData cells with the fallback sentinel.
    filled = np.ma.filled(np.ma.asarray(raw_stat, dtype="float64"), fallback)
    # All-NoData pixels (counts==0) → fallback.
    return np.where(counts > 0, filled, fallback)


def _combine_stat_tiles(rasters: List[bytes], stat: str, max_out=None) -> bytes:
    """Per-pixel ``stat`` across aligned tiles, excluding NoData (GTiff bytes).

    The masked reduction per-pixel per-band. min and max fold
    one tile at a time into a running value (memory is about four tiles, however many there are);
    sum, count, median and stddev stack all N tile arrays. Either way the canvas is folded in row windows sized to
    the budget (:func:`_merge_budget`), one window when it fits: the peak is a window of the stack, not the canvas.
    Output dtype is preserved from the
    first input tile; integer output is rounded via np.rint before the cast (same
    as the former JVM VRT pixel function's `np.copyto(out_ar, np.rint(result), casting='unsafe')`).

    All-NoData pixels → the chosen fallback NoData sentinel (first declared NoData
    or 0.0 if none declared). The fallback is stamped as NoData on every output band
    when at least one input declared NoData.
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return bytes(rasters[0])

    budget = _merge_budget()
    with _members(rasters, budget) as datasets:
        _require_aligned(datasets, f"gbx_rst_combine{stat}")

        ref = datasets[0]
        out_dtype = ref.dtypes[0]
        ref_profile = _comp.source_profile(ref.profile)
        shape = (ref.count, ref.height, ref.width)
        running = stat in _RUNNING_STATS

        nodatas = [ds.nodata for ds in datasets]  # header values, no pixel read
        any_nodata = any(nd is not None for nd in nodatas)
        fallback = next((nd for nd in nodatas if nd is not None), 0.0)

        reduce = _running_stat if running else _stacked_stat
        ref_profile.update(driver="GTiff")
        if any_nodata:
            ref_profile.update(nodata=fallback)
        decoded_bytes = int(np.prod(shape)) * np.dtype(out_dtype).itemsize
        ref_profile.update(
            _comp.creation_opts(
                str(np.dtype(out_dtype)), decoded_bytes=decoded_bytes, compress="auto"
            )
        )
        with MemoryFile() as out_mf:
            with out_mf.open(**ref_profile) as dst:
                _comp.match_alpha(dst, ref.colorinterp)
                windows = _fold_windows(
                    shape,
                    "running" if running else "stack",
                    len(datasets),
                    budget,
                    dst.block_shapes[0][0],
                )
                for window in windows:
                    result = reduce(datasets, nodatas, stat, fallback, window)
                    # Dtype preservation + integer rounding (the rule of the former JVM form).
                    if np.issubdtype(np.dtype(out_dtype), np.integer):
                        result = np.rint(result)
                    dst.write(result.astype(out_dtype), window=window)
            return _bounded(out_mf.read(), max_out, _COMBINE_OUT_ROUTE)


def combine_min_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel minimum of valid (non-NoData) values across aligned tiles (GTiff bytes).

    Mirrors ``gbx_rst_combinemin``: alignment is a hard precondition (same grid).
    """
    return _combine_stat_tiles(rasters, "min")


def combine_max_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel maximum of valid (non-NoData) values across aligned tiles (GTiff bytes).

    Mirrors ``gbx_rst_combinemax``.
    """
    return _combine_stat_tiles(rasters, "max")


def combine_sum_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel sum of valid (non-NoData) values across aligned tiles (GTiff bytes).

    Mirrors ``gbx_rst_combinesum``. Integer stacks may wrap/overflow (matches heavy).
    """
    return _combine_stat_tiles(rasters, "sum")


def combine_count_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel count of valid (non-NoData) inputs across aligned tiles (GTiff bytes).

    Mirrors ``gbx_rst_combinecount``. All-NoData pixels → NoData, NOT 0.
    """
    return _combine_stat_tiles(rasters, "count")


def combine_median_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel median of valid (non-NoData) values across aligned tiles (GTiff bytes).

    Mirrors ``gbx_rst_combinemedian``. Even-count median = mean of the two middle
    values (``np.ma.median`` default).
    """
    return _combine_stat_tiles(rasters, "median")


def combine_stddev_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel population stddev of valid (non-NoData) values (GTiff bytes).

    Mirrors ``gbx_rst_combinestddev``. Population standard deviation (ddof=0),
    ``masked.std()`` default.
    """
    return _combine_stat_tiles(rasters, "stddev")


# The alignment warp lives with the other warps (warp.py, which does not reach pyspark); the combine family and
# the callers of this module keep their names.
align_datasets = _warp.align_datasets
align_to_tiles = _warp.align_to_tiles
