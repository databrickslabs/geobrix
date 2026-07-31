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
  * ``viewshed`` uses ``xrspatial.viewshed`` instead of GDAL's ViewshedGenerate.
"""

import math
import os
import tempfile

import numpy as np
from rasterio.io import MemoryFile

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

    profile = ds.profile.copy()
    profile.update(
        driver="GTiff",
        count=1,
        dtype="float32",
        nodata=_PROXIMITY_NODATA,
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

    profile = ds.profile.copy()
    profile.update(
        driver="COG",
        blocksize=blocksize,
        overview_resampling=overview_resampling,
    )
    # "raw" means no compression in rio-cogeo; driver="COG" omits the key.
    if compression != "RAW":
        profile["compress"] = compression
    else:
        profile.pop("compress", None)

    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.close()
    try:
        with rasterio.open(tmp.name, "w", **profile) as dst:
            dst.write(data)
        # Free the decoded array before reading the output bytes back so
        # the peak is capped at max(data, cog_file_bytes), not their sum.
        del data
        with open(tmp.name, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp.name)


def cog_convert_file(
    src_path: str,
    dst_path: str,
    compression: str = "DEFLATE",
    blocksize: int = 512,
    overview_resampling: str = "AVERAGE",
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
    """
    blocksize = int(blocksize)
    if blocksize <= 0:
        raise ValueError(f"cog_convert_file: blocksize must be > 0; got {blocksize}")
    if compression is None or str(compression).strip() == "":
        raise ValueError("cog_convert_file: compression must be non-empty")
    if overview_resampling is None or str(overview_resampling).strip() == "":
        raise ValueError("cog_convert_file: overview_resampling must be non-empty")

    import rasterio
    import rasterio.shutil as rio_shutil

    creation = dict(
        blocksize=int(blocksize),
        overview_resampling=str(overview_resampling).upper(),
        # A COG whose output exceeds ~4 GiB overflows the Classic TIFF 32-bit
        # directory-offset limit ("TIFFRewriteDirectory: ... exceeds 32 bit range
        # allowed for Classic TIFF") and must be written as BigTIFF. IF_SAFER lets
        # GDAL pick BigTIFF automatically when the estimated size needs it, while
        # keeping Classic TIFF for small outputs (max compatibility).
        bigtiff="IF_SAFER",
    )
    comp = str(compression).upper()
    if comp != "RAW":
        creation["compress"] = comp

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


def viewshed(ds, observer_x, observer_y, observer_height, target_height, max_distance):
    """Compute a binary viewshed (255 visible / 0 invisible) from band 1.

    Mirrors the heavyweight ``gbx_rst_viewshed`` (``gdal.ViewshedGenerate``,
    GVOT_NORMAL binary 0/255 mask), implemented with ``xrspatial.viewshed``.

    ``xrspatial.viewshed`` returns, for each cell, the vertical viewing angle in
    ``[0, 180]`` when the cell is visible from the observer, and ``-1`` when it
    is invisible (blocked by terrain or beyond ``max_distance``). We map
    ``value >= 0 -> 255`` (visible) and ``value < 0 -> 0`` (invisible), matching
    the heavyweight's GVOT_NORMAL binary mask.

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
        The first viewshed call in a Python process compiles the underlying
        numba kernels -- a one-time cost of several seconds, amortized across all
        subsequent calls in that process. Steady-state cost scales with tile size.
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

    # xarray / xrspatial are pyrx (viewshed) dependencies; import lazily so the
    # numba JIT warm-up is only paid when viewshed is actually called.
    import xarray as xr
    from rasterio.transform import xy as _xy
    from xrspatial import viewshed as _viewshed

    band = ds.read(1).astype("float64")
    height, width = band.shape

    # Pixel-center world coordinates along each axis (north-up: y descends).
    xs, _ = _xy(ds.transform, np.zeros(width), np.arange(width), offset="center")
    _, ys = _xy(ds.transform, np.arange(height), np.zeros(height), offset="center")
    xs = np.asarray(xs, dtype="float64")
    ys = np.asarray(ys, dtype="float64")

    # Graceful out-of-bounds: an observer outside the raster extent has no valid
    # line-of-sight origin. xrspatial would error (or clamp) on such input; heavy
    # GDAL ViewshedGenerate likewise produces no visibility. Mirror that with an
    # all-invisible (0) raster rather than crashing the job. Bounds are the
    # min/max pixel-center coords (north-up: ys descends, so use min/max).
    x_lo, x_hi = float(min(xs[0], xs[-1])), float(max(xs[0], xs[-1]))
    y_lo, y_hi = float(min(ys[0], ys[-1])), float(max(ys[0], ys[-1]))
    if not (x_lo <= observer_x <= x_hi and y_lo <= observer_y <= y_hi):
        out = np.zeros((height, width), dtype="uint8")
    else:
        da = xr.DataArray(band, dims=["y", "x"], coords={"y": ys, "x": xs})
        res = _viewshed(
            da,
            x=observer_x,
            y=observer_y,
            observer_elev=observer_height,
            target_elev=target_height,
            max_distance=max_distance,
        )
        vals = np.asarray(res.values)
        # Visible cells carry an angle in [0, 180]; invisible/out-of-range carry -1.
        out = np.where(vals >= 0.0, 255, 0).astype("uint8")

    profile = ds.profile.copy()
    profile.update(driver="GTiff", count=1, dtype="uint8", nodata=None)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(out, 1)
        return mf.read()
