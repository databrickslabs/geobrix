"""Spark-free aggregation reducers — pure-Python counterparts to the heavyweight
rasterx ``*_agg`` UDAFs. Each reducer takes plain Python inputs (lists of raster
GTiff ``bytes``, ``(band_index, bytes)`` pairs, or ``(wkb, value)`` feature lists
plus extent params) and returns the result raster's GTiff ``bytes``.

These mirror the heavyweight operations:
  * ``merge_tiles``           -> RST_MergeAgg / MergeRasters (spatial mosaic)
  * ``combineavg_tiles``      -> RST_CombineAvgAgg / CombineAVG (per-pixel mean, NoData-aware)
  * ``combine_{stat}_tiles``  -> gbx_rst_combine{min,max,sum,count,median,stddev}
  * ``frombands_tiles``       -> RST_FromBandsAgg (stack bands, ascending band_index)
  * ``rasterize_features``    -> RST_RasterizeAgg (burn all features into one raster)
  * ``derivedband_tiles``     -> RST_DerivedBandAgg (user pyfunc across N tiles-as-bands)
  * ``align_to_tiles``        -> gbx_rst_align_to (warp tile to reference grid)
"""

from typing import List, Tuple

import numpy as np
import rasterio
import shapely.wkb
from rasterio.features import rasterize as _rasterize
from rasterio.io import MemoryFile
from rasterio.merge import merge as _rio_merge
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core import derivedband as _derivedband

_NODATA = -9999.0


def _open_all(rasters: List[bytes]):
    """Open a list of GTiff byte buffers as rasterio datasets.

    Returns ``(memfiles, datasets)``; callers MUST close both (datasets first).

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
            datasets.append(mf.open())
    except Exception:
        _close_all(memfiles, datasets)
        raise
    return memfiles, datasets


def _close_all(memfiles, datasets):
    for ds in datasets:
        ds.close()
    for mf in memfiles:
        mf.close()


def _reproject_dataset(src, dst_crs):
    """Reproject an open rasterio dataset to ``dst_crs``; return (memfile, dataset).

    Used by merge_tiles to reconcile a group whose tiles span multiple CRSs (e.g. a
    UTM zone boundary) before rasterio.merge, which requires a single CRS. Nearest
    resampling preserves the source values; the source NoData is carried through.
    Caller MUST close the returned dataset then memfile.
    """
    import rasterio
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    transform, width, height = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds
    )
    out_dtype = src.dtypes[0]
    decoded_bytes = src.count * width * height * np.dtype(out_dtype).itemsize
    profile = src.profile.copy()
    profile.update(
        driver="GTiff", crs=dst_crs, transform=transform, width=width, height=height
    )
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    mf = MemoryFile()
    dst = mf.open(**profile)
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
    return mf, dst


def _pick_ref_crs(datasets):
    """Deterministic target CRS for a (possibly multi-CRS) group: the smallest EPSG
    code present. This makes every band's merge of the SAME cell agree on one CRS, so a
    later frombands can np.stack them. Same-CRS groups return that single CRS unchanged
    (fast path, no reprojection). Falls back to the first dataset's CRS."""
    best, best_epsg = None, None
    for ds in datasets:
        c = ds.crs
        if c is None:
            continue
        e = c.to_epsg()
        if e is None:
            best = best or c
            continue
        if best_epsg is None or e < best_epsg:
            best, best_epsg = c, e
    return best if best is not None else datasets[0].crs


def merge_tiles(rasters: List[bytes]) -> bytes:
    """Merge the group's tile rasters into one spatial mosaic (GTiff bytes).

    Each GTiff carries its own georef/CRS, so ``rasterio.merge.merge`` places
    them by extent and the output spans the union extent (mirrors the
    heavyweight RST_MergeAgg / MergeRasters ``gdalbuildvrt -resolution highest``
    mosaic). On overlap we use ``method="last"`` so the LAST source in the fold
    order wins, matching the heavyweight ``gdalbuildvrt`` (overlapping pixels
    take the last-listed source).

    DETERMINISM: a Spark ``groupBy().agg()`` does not guarantee the order rows
    reach the reducer, so a last-wins mosaic would otherwise pick a different
    overlap winner from run to run (and from the heavyweight). To make the fold
    order-invariant we sort the inputs by their raw GTiff byte content -- a total
    order intrinsic to each tile that has NO ties for distinct content -- before
    merging; the highest-bytes tile folds last and wins the overlap. The
    heavyweight RST_MergeAgg sorts on the identical key (the same serialized tile
    bytes each row carries), so the two tiers pick the same winner for ALL inputs
    -- including same-origin overlapping tiles, which a geotransform-origin key
    could not separate (it tied on origin and fell back to a per-open
    ``/vsimem/<uuid>`` description, i.e. random). Raw bytes are bitwise-identical
    across tiers, so no cross-tier hash agreement is required.
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return bytes(rasters[0])
    # Sort by raw GTiff bytes so the last-wins overlap winner is deterministic
    # (and tier-agreeing) regardless of caller/row-arrival order.
    rasters = sorted((bytes(r) for r in rasters))
    memfiles, datasets = _open_all(rasters)
    extra = []  # (memfile, dataset) pairs for reprojected sources, closed in finally
    try:
        ref_crs = _pick_ref_crs(datasets)
        # Reconcile CRS before merging: real AOIs that straddle a UTM zone boundary
        # (e.g. Sentinel-2 over SE Alaska -> EPSG:32608 + 32609) yield groups whose
        # tiles span multiple CRSs, but rasterio.merge requires one. Reproject any
        # mismatched source to the reference (first) CRS; same-CRS groups are untouched.
        merge_ds = []
        for ds in datasets:
            if ref_crs is None or ds.crs == ref_crs:
                merge_ds.append(ds)
            else:
                mf, rds = _reproject_dataset(ds, ref_crs)
                extra.append((mf, rds))
                merge_ds.append(rds)
        ref = merge_ds[0]
        mosaic, out_transform = _rio_merge(merge_ds, method="last")
        out_dtype = str(mosaic.dtype)
        decoded_bytes = mosaic.nbytes
        profile = ref.profile.copy()
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
        with MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                dst.write(mosaic)
            return out_mf.read()
    finally:
        for mf, rds in extra:
            rds.close()
            mf.close()
        _close_all(memfiles, datasets)


def combineavg_tiles(rasters: List[bytes]) -> bytes:
    """Per-pixel mean across the group's aligned tiles, ignoring NoData (GTiff bytes).

    Mirrors the heavyweight RST_CombineAvgAgg / CombineAVG: each tile's declared
    NoData is excluded from BOTH the sum and the divisor; a valid ``0`` counts
    toward the mean. Where every input at a pixel is NoData, the output cell
    carries the first declared input NoData (or 0 if none declared one), and
    that NoData value is stamped on the output band.

    PARITY DIVERGENCE: the heavyweight builds a VRT with ``-resolution highest``
    which can tolerate differing grids; this reducer assumes the tiles are
    ALREADY aligned (same shape/extent/CRS) and raises ``ValueError`` if their
    raster shapes differ, rather than silently resampling.

    SCALE: the group's running sum + count are accumulated ONE tile at a time
    (open, add, close) so peak memory is ~O(one tile + 2 float64/int64
    accumulators), independent of the group size N. Stacking all N tiles as
    float64 (the obvious form) peaks at ~4x the raw group bytes -- a 16-tile group
    of 1024x1024x4 float32 hit ~1.2 GB and would OOM a Spark Python worker; the
    streaming form holds ~170 MB for the same input and is byte-identical.
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return bytes(rasters[0])
    sums = counts = None
    shape = ref_profile = out_dtype = None
    fallback = None
    any_nodata = False
    for b in rasters:
        with MemoryFile(bytes(b)) as mf, mf.open() as ds:
            if sums is None:
                shape = (ds.count, ds.height, ds.width)
                sums = np.zeros(shape, dtype="float64")
                counts = np.zeros(shape, dtype="int64")
                ref_profile = ds.profile.copy()
                out_dtype = ds.dtypes[0]
            elif (ds.count, ds.height, ds.width) != shape:
                raise ValueError(
                    "rst_combineavg_agg requires aligned tiles (same shape); got "
                    f"{(ds.count, ds.height, ds.width)} vs {shape}"
                )
            nd = ds.nodata
            arr = ds.read().astype("float64")
            if nd is not None:
                any_nodata = True
                if fallback is None:
                    fallback = nd
                valid = arr != nd
            else:
                valid = None
        # tile closed; only the running accumulators + this one tile stay resident
        if valid is None:
            sums += arr
            counts += 1
        else:
            sums += np.where(valid, arr, 0.0)
            counts += valid

    if fallback is None:
        fallback = 0.0
    means = np.where(counts > 0, sums / np.maximum(counts, 1), fallback)
    if np.issubdtype(np.dtype(out_dtype), np.integer):
        out = np.rint(means)
    else:
        out = means
    out = out.astype(out_dtype)

    ref_profile.update(driver="GTiff")
    if any_nodata:
        ref_profile.update(nodata=fallback)
    decoded_bytes = out.nbytes
    ref_profile.update(
        _comp.creation_opts(
            str(out.dtype), decoded_bytes=decoded_bytes, compress="auto"
        )
    )
    with MemoryFile() as out_mf:
        with out_mf.open(**ref_profile) as dst:
            dst.write(out)
        return out_mf.read()


def frombands_tiles(indexed: List[Tuple[int, bytes]]) -> bytes:
    """Stack single-band (or multi-band) tiles into one multi-band tile (GTiff bytes).

    *indexed* is a list of ``(band_index, raster_bytes)``. The list is sorted by
    ``band_index`` ASCENDING (the critical ordering guarantee of
    RST_FromBandsAgg), then each tile's band(s) are concatenated in that order.
    Georef/CRS/dtype/nodata are taken from the first (lowest-index) tile.
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
        for ds in datasets:
            aligned = (
                ds.width == ref.width
                and ds.height == ref.height
                and ds.transform == ref.transform
                and ds.crs == ref.crs
            )
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
        out_dtype = str(data.dtype)
        profile = ref.profile.copy()
        profile.update(driver="GTiff", count=data.shape[0])
        profile.update(
            _comp.creation_opts(out_dtype, decoded_bytes=data.nbytes, compress="auto")
        )
        with MemoryFile() as out_mf:
            with out_mf.open(**profile) as dst:
                dst.write(data)
            return out_mf.read()
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
    stable key intrinsic to each feature. The heavyweight RST_RasterizeAgg burns
    in the same canonical order, so both tiers resolve overlaps identically.
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
        profile = ref.profile.copy()
        profile.update(driver="GTiff", count=data.shape[0])
        with MemoryFile() as stack_mf:
            with stack_mf.open(**profile) as dst:
                dst.write(data)
            stacked_bytes = stack_mf.read()
    finally:
        _close_all(memfiles, datasets)

    with MemoryFile(stacked_bytes) as mf:
        with mf.open() as ds:
            return _derivedband.derivedband(ds, str(python_func), str(func_name))


# ---------------------------------------------------------------------------
# Cross-raster combine family (6 stats)
# ---------------------------------------------------------------------------
# Mirrors CombineStats.scala: per-pixel/per-band reduction over VALID (non-NoData)
# values across a stack of ALIGNED tiles. Alignment is a hard precondition: inputs
# must share identical CRS, extent, pixel size, and dimensions. A pixel that is
# NoData in ALL inputs produces NoData in the output.
#
# Conventions (mirrors the heavy tier exactly for parity):
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
    """Relative tolerance comparison (matches RasterAlignment.approxEq in Scala)."""
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
        from pyproj import CRS as _ProjCRS

        return _ProjCRS.from_user_input(a_crs).equals(_ProjCRS.from_user_input(b_crs))
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


def _combine_stat_tiles(rasters: List[bytes], stat: str) -> bytes:
    """Per-pixel ``stat`` across aligned tiles, excluding NoData (GTiff bytes).

    Mirrors CombineStats.compute (Scala): stacks all N tile arrays then applies
    the masked reduction per-pixel per-band. Output dtype is preserved from the
    first input tile; integer output is rounded via np.rint before the cast (same
    as the heavy VRT pixel function's `np.copyto(out_ar, np.rint(result), casting='unsafe')`).

    All-NoData pixels → the chosen fallback NoData sentinel (first declared NoData
    or 0.0 if none declared). The fallback is stamped as NoData on every output band
    when at least one input declared NoData.
    """
    if not rasters:
        return None
    if len(rasters) == 1:
        return bytes(rasters[0])

    memfiles, datasets = _open_all(rasters)
    try:
        _require_aligned(datasets, f"gbx_rst_combine{stat}")

        ref = datasets[0]
        out_dtype = ref.dtypes[0]
        ref_profile = ref.profile.copy()

        # Collect per-tile NoData values and per-band arrays.
        # min/max/count don't accumulate, so float32 decode is safe and faster.
        # sum/stddev/median need float64 precision to avoid accumulation error.
        _ACCUM_STATS = {"sum", "stddev", "median"}
        stack_dtype = "float64" if stat in _ACCUM_STATS else "float32"

        nodatas = []
        tile_arrays = []
        any_nodata = False
        fallback = None

        for ds in datasets:
            nd = ds.nodata
            arr = ds.read().astype(stack_dtype)  # (bands, h, w)
            tile_arrays.append(arr)
            nodatas.append(nd)
            if nd is not None:
                any_nodata = True
                if fallback is None:
                    fallback = nd

        if fallback is None:
            fallback = 0.0

        # Stack: (N, bands, h, w) — mirrors in_ar layout in the VRT pixel function.
        stacked = np.stack(tile_arrays, axis=0)

        # Build per-tile validity mask: (N, bands, h, w).
        valid = np.ones(stacked.shape, dtype=bool)
        for i, nd in enumerate(nodatas):
            if nd is not None:
                valid[i] = stacked[i] != nd

        counts = valid.sum(axis=0)  # (bands, h, w)
        masked = np.ma.masked_array(stacked, mask=~valid)

        if stat == "min":
            raw_stat = masked.min(axis=0)
        elif stat == "max":
            raw_stat = masked.max(axis=0)
        elif stat == "sum":
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
        result = np.where(counts > 0, filled, fallback)

        # Dtype preservation + integer rounding (matches heavy tier exactly).
        if np.issubdtype(np.dtype(out_dtype), np.integer):
            result = np.rint(result)
        out = result.astype(out_dtype)  # numpy default casting='unsafe'

        ref_profile.update(driver="GTiff")
        if any_nodata:
            ref_profile.update(nodata=fallback)
        decoded_bytes = out.nbytes
        ref_profile.update(
            _comp.creation_opts(
                str(out.dtype), decoded_bytes=decoded_bytes, compress="auto"
            )
        )
        with MemoryFile() as out_mf:
            with out_mf.open(**ref_profile) as dst:
                dst.write(out)
            return out_mf.read()
    finally:
        _close_all(memfiles, datasets)


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
    matching the heavy tier's ``masked.std()`` default.
    """
    return _combine_stat_tiles(rasters, "stddev")


# ---------------------------------------------------------------------------
# Warp helper: align a tile to a reference grid
# ---------------------------------------------------------------------------


def align_to_tiles(tile_bytes: bytes, ref_bytes: bytes, dst_nodata=None) -> bytes:
    """Warp ``tile_bytes`` onto ``ref_bytes``'s grid (GTiff bytes).

    Mirrors ``gbx_rst_align_to``: the output has exactly the same CRS, width,
    height, and geotransform as the reference. Nearest-neighbour resampling is
    used so pixel values are not interpolated (``-r near`` in gdalwarp).

    This is the explicit fix for the alignment precondition of the combine
    family: align each tile to a common reference, then combine.

    When the reference grid extends beyond the tile's footprint, the uncovered
    destination pixels are filled with the destination NoData value. By default
    that is the tile's own NoData (so a NoData-less tile leaves them at GDAL's
    fill of 0). Pass ``dst_nodata`` to force a sentinel for those uncovered
    pixels — this is what a NoData-less tile needs so padding is distinguishable
    from real values; the sentinel is also written as the output's NoData tag.

    Returns ``None`` when either input is missing/empty.
    """
    if not tile_bytes or not ref_bytes:
        return None

    with MemoryFile(bytes(tile_bytes)) as src_mf:
        with src_mf.open() as src:
            with MemoryFile(bytes(ref_bytes)) as ref_mf:
                with ref_mf.open() as ref:
                    dst_crs = ref.crs
                    dst_transform = ref.transform
                    dst_width = ref.width
                    dst_height = ref.height

            src_nodata = src.nodata
            effective_dst_nodata = src_nodata if dst_nodata is None else dst_nodata
            out_dtype = src.dtypes[0]
            decoded_bytes = (
                src.count * dst_width * dst_height * np.dtype(out_dtype).itemsize
            )

            profile = src.profile.copy()
            profile.update(
                driver="GTiff",
                crs=dst_crs,
                transform=dst_transform,
                width=dst_width,
                height=dst_height,
            )
            profile.update(
                _comp.creation_opts(
                    out_dtype, decoded_bytes=decoded_bytes, compress="auto"
                )
            )
            if dst_nodata is not None:
                profile.update(nodata=dst_nodata)

            with MemoryFile() as out_mf:
                with out_mf.open(**profile) as dst:
                    for b in range(1, src.count + 1):
                        reproject(
                            source=rasterio.band(src, b),
                            destination=rasterio.band(dst, b),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=dst_transform,
                            dst_crs=dst_crs,
                            src_nodata=src_nodata,
                            dst_nodata=effective_dst_nodata,
                            resampling=Resampling.nearest,
                        )
                return out_mf.read()
