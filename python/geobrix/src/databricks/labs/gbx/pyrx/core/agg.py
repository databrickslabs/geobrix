"""Spark-free aggregation reducers — pure-Python counterparts to the heavyweight
rasterx ``*_agg`` UDAFs. Each reducer takes plain Python inputs (lists of raster
GTiff ``bytes``, ``(band_index, bytes)`` pairs, or ``(wkb, value)`` feature lists
plus extent params) and returns the result raster's GTiff ``bytes``.

These mirror the heavyweight operations:
  * ``merge_tiles``       -> RST_MergeAgg / MergeRasters (spatial mosaic)
  * ``combineavg_tiles``  -> RST_CombineAvgAgg / CombineAVG (per-pixel mean, NoData-aware)
  * ``frombands_tiles``   -> RST_FromBandsAgg (stack bands, ascending band_index)
  * ``rasterize_features``-> RST_RasterizeAgg (burn all features into one raster)
  * ``derivedband_tiles`` -> RST_DerivedBandAgg (user pyfunc across N tiles-as-bands)
"""

from typing import List, Tuple

import numpy as np
import shapely.wkb
from rasterio.features import rasterize as _rasterize
from rasterio.io import MemoryFile
from rasterio.merge import merge as _rio_merge
from rasterio.transform import from_bounds

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
