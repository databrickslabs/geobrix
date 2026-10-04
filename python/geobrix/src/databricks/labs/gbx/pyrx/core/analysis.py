"""Spark-free RasterX *Analysis* ports: proximity (distance-to-source),
COG conversion, contour extraction, and viewshed.

Faithful ports of the heavyweight ``gbx_rst_proximity`` (``gdal.ComputeProximity``),
``gbx_rst_cog_convert`` (``gdal.Translate -of COG``), ``gbx_rst_contour``
(``gdal.ContourGenerateEx``), and ``gbx_rst_viewshed`` (``gdal.ViewshedGenerate``)
expressions, implemented without the JAR:

  * ``proximity`` uses ``scipy.ndimage.distance_transform_edt`` (scipy is already
    a pyrx dependency) instead of GDAL's ComputeProximity.
  * ``cog_convert`` uses GDAL's native ``driver="COG"`` via rasterio instead of
    ``gdal_translate -of COG``. This approach writes directly from the decoded
    array without re-decoding (unlike rio-cogeo's ``cog_translate``), keeping
    peak RSS at ~2.8× the decoded tile size — safe under Databricks Serverless's
    1 GB Python UDF cap.
  * ``contour`` uses ``skimage.measure.find_contours`` instead of GDAL's
    ContourGenerateEx.
  * ``viewshed`` uses ``GDALViewshedGenerate`` (via ctypes against the bundled
    libgdal) for large DEMs, and ``xrspatial.viewshed`` for small ones.  The
    ctypes engine streams block-by-block (peak RSS ≈ GDAL cache, independent of
    DEM size); xrspatial is kept for small tiles to avoid ctypes/file overhead.
"""

import math
import os
import threading

import numpy as np
from rasterio.io import MemoryFile

# Module-level lock used to serialize the ``psutil.virtual_memory`` patch that
# bypasses xrspatial's system-RAM guard (see ``viewshed()`` below).  The patch is
# not thread-safe on its own — concurrent Spark Connect UDF workers sharing the
# same Python process race on the module attribute.  The RLock serialises calls
# within one process; separate OS processes are unaffected.
_VIEWSHED_PSUTIL_LOCK = threading.RLock()

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core.budget import _cgroup_task_limit_bytes, decoded_budget_bytes
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir, new_local_temp_file

# NoData sentinel for the proximity output — mirrors the heavyweight, which sets
# NODATA=-1.0 so beyond-max / unreachable pixels are distinguishable from
# zero-distance (source) pixels.
_PROXIMITY_NODATA = -1.0


def proximity(ds, target_values, distunits, max_distance):
    """Compute a Float32 proximity raster: each pixel holds the distance to the
    nearest source pixel.

    Mirrors the heavyweight ``gbx_rst_proximity`` semantics:

      * ``target_values``: optional comma-separated string of source pixel
        values, matched in GDAL's integer domain (each pixel is rounded to the
        nearest integer, half away from zero, before the comparison). When given,
        source pixels are those whose rounded value is in that set. When
        None/empty, the GDAL default applies: source = pixels whose rounded value
        is ``!= 0``.
      * ``distunits``: ``"GEO"`` (default) measures distance in CRS ground units
        (scaled by the pixel size from the GeoTransform); ``"PIXEL"`` measures in
        pixel counts. Any other value raises ``ValueError``.
      * ``max_distance``: optional cap; must be ``> 0`` and finite when given.
        Pixels whose distance exceeds it become NoData.

    The output is a single-band Float32 GTiff at the same extent/CRS as ``ds``,
    with ``nodata = -1.0``. Source pixels get distance 0.

    Args:
        ds:            Open rasterio DatasetReader.
        target_values: Optional comma-separated source-value string, or None.
        distunits:     ``"GEO"`` or ``"PIXEL"``.
        max_distance:  Optional positive distance cap, or None.

    Returns:
        Single-band Float32 GTiff bytes (nodata = -1.0).
    """
    distunits = "GEO" if distunits is None else str(distunits)
    if distunits not in ("GEO", "PIXEL"):
        raise ValueError(
            f"rst_proximity: distunits must be 'GEO' or 'PIXEL'; got '{distunits}'"
        )
    if max_distance is not None:
        max_distance = float(max_distance)
        if (
            not (max_distance > 0.0)
            or math.isinf(max_distance)
            or math.isnan(max_distance)
        ):
            raise ValueError(
                f"rst_proximity: max_distance must be > 0 and finite; got {max_distance}"
            )

    # scipy is a pyrx dependency; import lazily so module import stays cheap.
    from scipy import ndimage

    band1 = ds.read(1)

    # GDAL ComputeProximity compares targets in the INTEGER domain: it copies the
    # source scanline into a GInt32 buffer (GDALCopyWords rounds float->int half
    # away from zero), so VALUES are matched against the *rounded* pixel value and
    # the default "non-zero" rule is "rounded value != 0". A continuous float band
    # therefore has many targets (e.g. every pixel in [0.5, 1.5) matches VALUES=1),
    # not only pixels exactly equal to an integer. Round the same way for parity.
    band1_int = np.floor(np.abs(band1) + 0.5).astype(np.int64) * np.where(
        band1 < 0, -1, 1
    )

    # Build the source mask in the integer domain.
    if target_values is not None and str(target_values).strip() != "":
        vals = [
            int(round(float(v)))
            for v in str(target_values).split(",")
            if v.strip() != ""
        ]
        source_mask = np.isin(band1_int, np.array(vals, dtype=np.int64))
    else:
        # GDAL default: any pixel whose rounded value is non-zero is a source.
        source_mask = band1_int != 0

    # distance_transform_edt computes, for every True (non-zero) cell of its
    # input, the distance to the nearest False (zero) cell. We want the distance
    # to the nearest SOURCE pixel, so invert the source mask: source pixels are
    # the "background" (0 distance) and everything else measures distance to it.
    if distunits == "GEO":
        # Pixel ground size from the GeoTransform. sampling = (row spacing,
        # col spacing) = (pixel height, pixel width).
        px_w = abs(ds.transform.a)
        px_h = abs(ds.transform.e)
        sampling = (px_h, px_w)
    else:
        sampling = 1.0

    if not source_mask.any():
        # No source pixels at all: every pixel is unreachable -> all NoData.
        dist = np.full(band1.shape, _PROXIMITY_NODATA, dtype="float32")
    else:
        dist = ndimage.distance_transform_edt(~source_mask, sampling=sampling)
        dist = dist.astype("float32")
        if max_distance is not None:
            dist = np.where(dist > max_distance, _PROXIMITY_NODATA, dist).astype(
                "float32"
            )

    decoded_bytes = dist.nbytes
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        count=1,
        dtype="float32",
        nodata=_PROXIMITY_NODATA,
    )
    profile.update(
        _comp.creation_opts("float32", decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(dist, 1)
        return mf.read()


def cog_convert(ds, compression, blocksize, overview_resampling):
    """Convert a raster to a Cloud Optimized GeoTIFF (COG) layout.

    Mirrors the heavyweight ``gbx_rst_cog_convert`` (``gdal.Translate -of COG``)
    using GDAL's native ``driver="COG"`` via rasterio.

      * ``compression`` (default ``"DEFLATE"``): GDAL COG compression name.
        Accepted values are the same profile names accepted by rio-cogeo:
        ``deflate``, ``lzw``, ``zstd``, ``jpeg``, ``webp``, ``packbits``,
        ``lzma``, ``lerc``, ``lerc_deflate``, ``lerc_zstd``, ``raw``.
        Unknown values raise ``ValueError``.
      * ``blocksize`` (default 512): internal tile size; must be ``> 0``.
      * ``overview_resampling`` (default ``"AVERAGE"``): downsampling algorithm
        for the auto-generated overview pyramid (e.g. ``"AVERAGE"``,
        ``"NEAREST"``, ``"BILINEAR"``).

    Uses GDAL's ``driver="COG"`` directly (not rio-cogeo's ``cog_translate``).
    This writes from the already-decoded array without a re-decode pass, keeping
    peak RSS at ~2.8× the decoded tile size.  Output is spec-valid COG
    (cog_validate=True) — GDAL's COG driver orders IFDs correctly by construction.

    The result is COG-layout GTiff bytes that reopen with rasterio.

    Args:
        ds:                  Open rasterio DatasetReader.
        compression:         COG compression name (case-insensitive).
        blocksize:           Internal tile size in pixels (square).
        overview_resampling: Overview resampling algorithm name.

    Returns:
        COG-layout GTiff bytes.
    """
    blocksize = int(blocksize)
    if blocksize <= 0:
        raise ValueError(f"rst_cog_convert: blocksize must be > 0; got {blocksize}")
    if compression is None or str(compression).strip() == "":
        raise ValueError("rst_cog_convert: compression must be non-empty")
    if overview_resampling is None or str(overview_resampling).strip() == "":
        raise ValueError("rst_cog_convert: overview_resampling must be non-empty")
    compression = str(compression).upper()
    overview_resampling = str(overview_resampling).upper()

    # "AUTO" is a special sentinel meaning "size-adaptive ZSTD" — it bypasses
    # the cog_profiles validation (which has no "auto" entry) and routes through
    # creation_opts(compress="auto", decoded_bytes=...) so size-adaptive level +
    # predictor are preserved, matching the GTiff path's ZSTD baseline.
    _is_auto = compression == "AUTO"

    if not _is_auto:
        # Validate compression against the same set that rio-cogeo accepts, so
        # callers passing profile names (e.g. "deflate", "lzw", "raw") still work.
        # "raw" maps to no compression (omit the compress kwarg for driver="COG").
        from rio_cogeo.profiles import cog_profiles

        try:
            _profile_check = cog_profiles.get(compression.lower())
        except KeyError as exc:
            raise ValueError(
                f"rst_cog_convert: unknown compression '{compression}'; "
                f"valid profiles: {', '.join(sorted(cog_profiles.keys()))}"
            ) from exc
        if _profile_check is None:
            raise ValueError(
                f"rst_cog_convert: unknown compression '{compression}'; "
                f"valid profiles: {', '.join(sorted(cog_profiles.keys()))}"
            )

    import rasterio

    # Read decoded data from the source dataset FIRST, then free it after
    # writing to minimise concurrent-buffer overlap.
    data = ds.read()

    # Route compression through the authority for codec/level/predictor.
    # Map "RAW" -> "none" so the authority emits no compression keys.
    # "AUTO" is passed through directly so creation_opts picks size-adaptive
    # ZSTD level from decoded_bytes (the ZSTD baseline, same as the GTiff path).
    _comp_arg = "none" if compression == "RAW" else compression.lower()
    _comp_opts = _comp.creation_opts(
        str(ds.dtypes[0]), decoded_bytes=data.nbytes, compress=_comp_arg, driver="COG"
    )

    profile = ds.profile.copy()
    profile.update(
        driver="COG",
        blocksize=blocksize,
        overview_resampling=overview_resampling,
    )
    # Remove stale compression keys from the source profile before merging authority opts.
    for _k in ("compress", "predictor", "zstd_level", "zlevel"):
        profile.pop(_k, None)
    # Merge authority options (codec + level + predictor); RAW path emits empty dict.
    profile.update(_comp_opts)

    tmp = new_local_temp_file(suffix=".tif")
    try:
        with rasterio.open(tmp, "w", **profile) as dst:
            dst.write(data)
        # Free the decoded array before reading the output bytes back so
        # the peak is capped at max(data, cog_file_bytes), not their sum.
        del data
        with open(tmp, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp)


# Allowed BIGTIFF creation-option values (GDAL). Default YES: every GeoBrix
# COG is BigTIFF (one predictable format, no ~4 GiB boundary risk, universally
# readable by GDAL/rasterio). IF_SAFER = Classic for small outputs, BigTIFF when
# size needs it. NO = force Classic (fails past ~4 GiB — advanced/compat only).
_BIGTIFF_VALUES = frozenset({"YES", "NO", "IF_NEEDED", "IF_SAFER"})


def cog_convert_file(
    src_path: str,
    dst_path: str,
    compression: str = "DEFLATE",
    blocksize: int = 512,
    overview_resampling: str = "AVERAGE",
    bigtiff: str = "YES",
    compress_level: int = None,
    predictor: int = None,
) -> None:
    """Convert a raster file to COG layout using streaming block-by-block copy.

    Unlike ``cog_convert`` (which decodes the whole raster into a Python array),
    this function uses ``rasterio.shutil.copy`` with ``driver="COG"`` inside a
    bounded GDAL cache.  GDAL streams block-by-block so peak RSS stays at ~GDAL
    cache size regardless of file size — safe for large files under Databricks
    Serverless's 1 GB Python UDF cap.

    Uses rasterio only — does NOT import osgeo.gdal.

    Args:
        src_path:            Source raster path (local or FUSE-mounted Volume).
        dst_path:            Destination path for the output COG (must be local;
                             caller is responsible for FUSE-safe staging if needed).
        compression:         COG compression name (case-insensitive, e.g. "DEFLATE").
        blocksize:           Internal tile size in pixels (square, must be > 0).
        overview_resampling: Overview resampling algorithm (e.g. "AVERAGE").
        bigtiff:             GDAL BIGTIFF creation option (case-insensitive):
                             ``YES`` (default — always BigTIFF), ``IF_SAFER`` /
                             ``IF_NEEDED`` (size-adaptive), or ``NO`` (force
                             Classic TIFF, which FAILS for outputs past ~4 GiB).
                             A COG exceeding ~4 GiB overflows the Classic TIFF
                             32-bit directory-offset limit and MUST be BigTIFF.
        compress_level:      Optional compression level (for ZSTD/DEFLATE). Ignored
                             when compression is "none" or unknown.
        predictor:           Optional TIFF predictor tag (1-3). Ignored when
                             compression is "none" or unknown codecs.
    """
    blocksize = int(blocksize)
    if blocksize <= 0:
        raise ValueError(f"cog_convert_file: blocksize must be > 0; got {blocksize}")
    if compression is None or str(compression).strip() == "":
        raise ValueError("cog_convert_file: compression must be non-empty")
    if overview_resampling is None or str(overview_resampling).strip() == "":
        raise ValueError("cog_convert_file: overview_resampling must be non-empty")
    bigtiff_val = str(bigtiff).upper()
    if bigtiff_val not in _BIGTIFF_VALUES:
        raise ValueError(
            f"cog_convert_file: bigtiff must be one of {sorted(_BIGTIFF_VALUES)}; "
            f"got {bigtiff!r}"
        )

    import rasterio
    import rasterio.shutil as rio_shutil

    # Determine the source dtype so the authority can pick the right predictor.
    with rasterio.open(src_path) as _src:
        _src_dtype = _src.dtypes[0]

    # Route compression through the authority for codec/level/predictor.
    # Map "RAW" -> "none" so the authority emits no compression keys.
    _comp_arg = (
        "none" if str(compression).upper() == "RAW" else str(compression).lower()
    )
    _comp_opts = _comp.creation_opts(
        _src_dtype,
        decoded_bytes=None,
        compress=_comp_arg,
        level=compress_level,
        predictor=predictor,
        driver="COG",
    )

    creation = dict(
        blocksize=int(blocksize),
        overview_resampling=str(overview_resampling).upper(),
        bigtiff=bigtiff_val,
    )
    # Merge authority options (codec + level + predictor); RAW path emits empty dict.
    creation.update(_comp_opts)

    with rasterio.Env(GDAL_CACHEMAX=200):
        rio_shutil.copy(src_path, dst_path, driver="COG", **creation)


def contour(ds, levels, interval, base, attr_field):
    """Generate contour lines from band 1 as ``(geom_wkb, value)`` features.

    Mirrors the heavyweight ``gbx_rst_contour`` (``gdal.ContourGenerateEx``),
    implemented with ``skimage.measure.find_contours``:

      * ``levels`` (list[float]): explicit fixed contour values. When non-empty,
        one contour is generated at each level.
      * ``interval`` (float): when ``levels`` is empty, equal-step contours are
        generated at ``base + k*interval`` for every such value inside the
        band's finite data range (min..max). ``interval`` must then be ``> 0``
        and finite, else ``ValueError``.
      * ``base`` (float): contour base value; only meaningful with ``interval``.
      * ``attr_field`` (str): name of the contour value field; in pyrx it does
        not change the output shape (the struct field is always ``value``), but
        it must be non-empty (parity with the heavyweight ``require``).

    Output geometries are LineStrings in the source CRS — pixel (row, col)
    coordinates from ``find_contours`` are mapped to world (x, y) via the
    raster transform (``rasterio.transform.xy(..., offset="center")``). NoData
    pixels are masked to NaN before contouring so contours do not trace the
    sentinel. Paths with fewer than 2 points are skipped.

    Args:
        ds:         Open rasterio DatasetReader.
        levels:     List of fixed contour values (possibly empty).
        interval:   Equal-interval step (used only when ``levels`` is empty).
        base:       Contour base value for the interval mode.
        attr_field: Non-empty value-field name (parity-only).

    Returns:
        list[dict]: one ``{"geom_wkb": bytes, "value": float}`` per contour
        LineString.
    """
    if attr_field is None or str(attr_field).strip() == "":
        raise ValueError("rst_contour: attr_field must be non-empty")

    levels = [] if levels is None else [float(v) for v in levels]

    band = ds.read(1).astype("float64")
    # Mask NoData so contours do not trace the sentinel value.
    msk = ds.read_masks(1)  # 0 where NoData, 255 where valid
    band = np.where(msk == 0, np.nan, band)

    if not levels:
        if not (interval > 0.0) or math.isinf(interval) or math.isnan(interval):
            raise ValueError(
                "rst_contour: levels is empty so interval must be > 0 and finite; "
                f"got {interval}"
            )
        finite = band[np.isfinite(band)]
        if finite.size == 0:
            return []
        lo = float(finite.min())
        hi = float(finite.max())
        # Equal-step levels at base + k*interval within [lo, hi].
        k_start = math.ceil((lo - base) / interval)
        k_end = math.floor((hi - base) / interval)
        levels = [base + k * interval for k in range(k_start, k_end + 1)]

    # skimage is a pyrx (contour) dependency; import lazily.
    import shapely.wkb
    from rasterio.transform import xy as _xy
    from shapely.geometry import LineString
    from skimage.measure import find_contours

    out = []
    for level in levels:
        for path in find_contours(band, level):
            if path.shape[0] < 2:
                continue
            rows = path[:, 0]
            cols = path[:, 1]
            xs, ys = _xy(ds.transform, rows, cols, offset="center")
            coords = list(zip(np.asarray(xs).tolist(), np.asarray(ys).tolist()))
            line = LineString(coords)
            out.append({"geom_wkb": shapely.wkb.dumps(line), "value": float(level)})
    return out


# ---------------------------------------------------------------------------
# Viewshed helpers
# ---------------------------------------------------------------------------


def _load_libgdal():
    """Locate and return the bundled libgdal CDLL (no osgeo, light-tier safe)."""
    import ctypes
    import glob as _glob
    import os as _os

    import rasterio

    rp = _os.path.dirname(rasterio.__file__)
    sp = _os.path.dirname(rp)
    for d in (_os.path.join(rp, ".dylibs"), _os.path.join(sp, "rasterio.libs")):
        if _os.path.isdir(d):
            hits = _glob.glob(_os.path.join(d, "*gdal*"))
            if hits:
                return ctypes.CDLL(hits[0])
    # last resort: symbol may be global after rasterio dlopened libgdal
    lib = ctypes.CDLL(None)
    if hasattr(lib, "GDALViewshedGenerate"):
        return lib
    raise RuntimeError("rst_viewshed: GDALViewshedGenerate not found in bundled libgdal")


def _viewshed_xrspatial(ds, observer_x, observer_y, observer_height, target_height, max_distance):
    """xrspatial engine for small DEMs.  Returns a uint8 numpy array (255 visible / 0 invisible).

    Moves the xrspatial imports, NUMBA_CACHE_DIR guard, and _available_memory_bytes
    patch inside this helper so it can be independently tested and so the ctypes path
    never pays the numba warm-up cost.
    """
    import importlib as _importlib
    import os as _os
    import tempfile as _tempfile

    if "NUMBA_DISABLE_JIT" not in _os.environ and "NUMBA_CACHE_DIR" not in _os.environ:
        _os.environ["NUMBA_CACHE_DIR"] = _tempfile.mkdtemp(prefix="gbx_numba_")
    import xarray as xr
    from rasterio.transform import xy as _xy
    from xrspatial import viewshed as _viewshed

    band = ds.read(1).astype("float64")
    height, width = band.shape
    xs, _ = _xy(ds.transform, np.zeros(width), np.arange(width), offset="center")
    _, ys = _xy(ds.transform, np.arange(height), np.zeros(height), offset="center")
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    da = xr.DataArray(band, dims=["y", "x"], coords={"y": ys, "x": xs})
    # xrspatial.viewshed guards against large rasters by checking available RAM.
    # On a shared Serverless cluster the system-wide RAM probe fires even for tiny
    # tiles.  Replace the guard with the honest per-task cgroup limit (or 1 GiB
    # fallback) so it reflects the actual task quota rather than node-total RAM.
    _xrs_vs_mod = _importlib.import_module("xrspatial.viewshed")
    with _VIEWSHED_PSUTIL_LOCK:
        _real_avail_mem_fn = _xrs_vs_mod._available_memory_bytes
        _xrs_vs_mod._available_memory_bytes = (
            lambda: _cgroup_task_limit_bytes() or (1024**3)
        )
        try:
            res = _viewshed(
                da,
                x=observer_x,
                y=observer_y,
                observer_elev=observer_height,
                target_elev=target_height,
                max_distance=max_distance,
            )
        finally:
            _xrs_vs_mod._available_memory_bytes = _real_avail_mem_fn
    vals = np.asarray(res.values)
    # Visible cells carry an angle in [0, 180]; invisible/out-of-range carry -1.
    return np.where(vals >= 0.0, 255, 0).astype("uint8")


def _viewshed_gdal_ctypes(ds, observer_x, observer_y, observer_height, target_height, max_distance):
    """GDALViewshedGenerate ctypes engine for large DEMs.

    Writes the DEM to a local scratch file, calls GDALViewshedGenerate via ctypes
    against the bundled libgdal, reads the binary 0/255 output, and returns a
    uint8 numpy array.  Peak RSS ≈ GDAL block cache (independent of DEM size).

    The scratch dir is cleaned up unconditionally in a finally block.
    """
    import ctypes
    import os as _os

    import rasterio as _rasterio

    scratch = new_local_temp_dir(prefix="gbx_viewshed_")
    try:
        dem_path = _os.path.join(scratch, "dem.tif")
        out_path = _os.path.join(scratch, "viewshed.tif")

        # Stage the DEM to a local scratch file without loading the whole band into
        # Python memory.  Preferred path: rasterio.shutil.copy streams GDAL-to-GDAL
        # (block-by-block internally, no numpy array).  ds.name is the GDAL-native
        # path: "/vsimem/..." for MemoryFile-backed tiles, or a real path for
        # file-backed datasets — both are valid GDAL source identifiers.
        # Fallback: windowed block copy (O(block_size) peak RAM) for the rare case
        # where ds.name is empty or the copy fails (e.g., unusual source format).
        import rasterio.shutil as _rio_shutil

        dem_profile = {
            "driver": "GTiff",
            "width": ds.width,
            "height": ds.height,
            "count": 1,
            "dtype": ds.dtypes[0],
            "crs": ds.crs,
            "transform": ds.transform,
        }
        if ds.nodata is not None:
            dem_profile["nodata"] = ds.nodata

        src_name = getattr(ds, "name", None) or ""
        if src_name:
            try:
                _rio_shutil.copy(src_name, dem_path, driver="GTiff")
                src_name = dem_path  # success — dem_path is ready
            except Exception:
                src_name = ""
        if not src_name:
            # Windowed block copy: each read is bounded by ds.block_shapes[0].
            with _rasterio.open(dem_path, "w", **dem_profile) as wdst:
                for _, window in ds.block_windows(1):
                    wdst.write(ds.read(1, window=window), 1, window=window)

        gdal = _load_libgdal()

        c_vp = ctypes.c_void_p
        c_cp = ctypes.c_char_p
        c_d = ctypes.c_double
        c_i = ctypes.c_int

        gdal.GDALAllRegister.restype = None
        gdal.GDALOpen.argtypes = [c_cp, c_i]
        gdal.GDALOpen.restype = c_vp
        gdal.GDALGetRasterBand.argtypes = [c_vp, c_i]
        gdal.GDALGetRasterBand.restype = c_vp
        gdal.GDALClose.argtypes = [c_vp]
        gdal.GDALViewshedGenerate.restype = c_vp
        gdal.GDALViewshedGenerate.argtypes = [
            c_vp, c_cp, c_cp, c_vp,  # hBand, driver, targetRasterName, creationOpts(NULL)
            c_d, c_d, c_d, c_d,       # obsX, obsY, obsHeight, targetHeight
            c_d, c_d, c_d, c_d, c_d,  # visibleVal, invisibleVal, outOfRangeVal, nodataVal, curvCoeff
            c_i,                       # eMode  (GVM_Max = 3)
            c_d,                       # maxDistance (0 = unlimited)
            c_vp, c_vp,                # pfnProgress(NULL), pProgressArg(NULL)
            c_i,                       # heightMode (GVOT_NORMAL = 0)
            c_vp,                      # extraOptions(NULL)
        ]

        gdal.GDALAllRegister()
        ds_h = gdal.GDALOpen(dem_path.encode(), 0)  # GA_ReadOnly = 0
        band_h = gdal.GDALGetRasterBand(ds_h, 1)
        out_h = gdal.GDALViewshedGenerate(
            band_h, b"GTiff", out_path.encode(), None,
            float(observer_x), float(observer_y),
            float(observer_height), float(target_height),
            255.0, 0.0, 0.0, 0.0, 0.85714,
            3,                                                  # GVM_Max
            float(max_distance) if max_distance is not None else 0.0,
            None, None,
            0,                                                  # GVOT_NORMAL -> binary 0/255 mask
            None,
        )
        if not out_h:
            raise RuntimeError("rst_viewshed: GDALViewshedGenerate returned NULL")
        gdal.GDALClose(out_h)
        gdal.GDALClose(ds_h)

        # The output is already uint8 0/255 (GVOT_NORMAL); no remap needed.
        with _rasterio.open(out_path) as result_ds:
            return result_ds.read(1).astype("uint8")
    finally:
        import shutil as _shutil

        _shutil.rmtree(scratch, ignore_errors=True)


def viewshed(ds, observer_x, observer_y, observer_height, target_height, max_distance):
    """Compute a binary viewshed (255 visible / 0 invisible) from band 1.

    Mirrors the heavyweight ``gbx_rst_viewshed`` (``gdal.ViewshedGenerate``,
    GVOT_NORMAL binary 0/255 mask).

    For small DEMs (decoded size ≤ the Serverless per-task budget) the
    ``xrspatial.viewshed`` engine is used; for large DEMs ``GDALViewshedGenerate``
    is called via ctypes against the bundled libgdal, which streams the DEM
    block-by-block (peak RSS ≈ GDAL cache, independent of DEM size).

    The result is a single-band Byte (uint8) GTiff at the same extent / CRS as
    ``ds`` (no NoData; 0 = invisible).

    Args:
        ds:              Open rasterio DatasetReader (the DEM).
        observer_x:      Observer X in the raster's CRS (world units).
        observer_y:      Observer Y in the raster's CRS (world units).
        observer_height: Observer height above the DEM (>= 0).
        target_height:   Target height above the DEM at each tested cell (>= 0).
        max_distance:    Optional analysis-distance cap in CRS ground units;
                         must be ``> 0`` and finite when given. ``None`` =
                         unlimited (bounded only by the raster extent).

    Returns:
        Single-band uint8 GTiff bytes (255 visible / 0 invisible).

    Note:
        When the xrspatial engine is selected, the first call in a Python process
        compiles the underlying numba kernels — a one-time cost of several seconds,
        amortised across subsequent calls.  The ctypes (GDAL) engine has no such
        warm-up cost.
    """
    observer_height = float(observer_height)
    target_height = float(target_height)
    if (
        not (observer_height >= 0.0)
        or math.isinf(observer_height)
        or math.isnan(observer_height)
    ):
        raise ValueError(
            f"rst_viewshed: observer_height must be >= 0 and finite; got {observer_height}"
        )
    if (
        not (target_height >= 0.0)
        or math.isinf(target_height)
        or math.isnan(target_height)
    ):
        raise ValueError(
            f"rst_viewshed: target_height must be >= 0 and finite; got {target_height}"
        )
    if max_distance is not None:
        max_distance = float(max_distance)
        if (
            not (max_distance > 0.0)
            or math.isinf(max_distance)
            or math.isnan(max_distance)
        ):
            raise ValueError(
                f"rst_viewshed: max_distance must be > 0 and finite; got {max_distance}"
            )

    from rasterio.transform import xy as _xy

    height, width = ds.height, ds.width

    # Pixel-center world coordinates along each axis (north-up: y descends).
    xs, _ = _xy(ds.transform, np.zeros(width), np.arange(width), offset="center")
    _, ys = _xy(ds.transform, np.arange(height), np.zeros(height), offset="center")
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    # Graceful out-of-bounds: an observer outside the raster extent has no valid
    # line-of-sight origin. Both engines produce no visibility in that case. Mirror
    # with an all-invisible (0) raster rather than crashing the job. Bounds are the
    # min/max pixel-center coords (north-up: ys descends, so use min/max).
    x_lo, x_hi = float(min(xs[0], xs[-1])), float(max(xs[0], xs[-1]))
    y_lo, y_hi = float(min(ys[0], ys[-1])), float(max(ys[0], ys[-1]))
    if not (x_lo <= observer_x <= x_hi and y_lo <= observer_y <= y_hi):
        out = np.zeros((height, width), dtype="uint8")
    else:
        # Gate on decoded DEM size: use xrspatial for small DEMs (avoids ctypes /
        # file-write overhead), ctypes GDALViewshedGenerate for large DEMs (bounded
        # peak RSS regardless of DEM size).
        decoded = ds.count * ds.width * ds.height * np.dtype(ds.dtypes[0]).itemsize
        if decoded <= decoded_budget_bytes("serverless"):
            out = _viewshed_xrspatial(
                ds, observer_x, observer_y, observer_height, target_height, max_distance
            )
        else:
            out = _viewshed_gdal_ctypes(
                ds, observer_x, observer_y, observer_height, target_height, max_distance
            )

    decoded_bytes = out.nbytes
    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype="uint8", nodata=None)
    profile.update(
        _comp.creation_opts("uint8", decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()
