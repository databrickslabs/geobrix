"""Spark-free accessor functions over an open rasterio DatasetReader.

These contain ALL the raster logic; the Spark layer (functions.py) only wraps
them in Arrow UDFs. Keeping them Spark-free makes them fast to unit test.
"""

import json
import math
from typing import Dict, List, Optional

import numpy as np
from shapely import wkb as _wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical

# Rasterio/numpy dtype string -> GDAL data-type name (mirrors heavyweight rst_type).
_NP_TO_GDAL = {
    "uint8": "Byte",
    "int8": "Int8",
    "uint16": "UInt16",
    "int16": "Int16",
    "uint32": "UInt32",
    "int32": "Int32",
    "float32": "Float32",
    "float64": "Float64",
}


def width(ds) -> int:
    return int(ds.width)


def height(ds) -> int:
    return int(ds.height)


def numbands(ds) -> int:
    return int(ds.count)


def srid(ds) -> Optional[int]:
    return ds.crs.to_epsg() if ds.crs is not None else None


def crs(ds) -> Optional[str]:
    """Return the canonical CRS string: authority string ('EPSG:4326'/'ESRI:54008') or WKT.

    None-safe: returns None when ds.crs is None. Distinct from srid() which
    returns int/null; crs() preserves non-EPSG authority codes (ESRI, etc.)
    and falls back to WKT for authority-less projections.
    """
    return crs_to_canonical(ds.crs)


def pixelwidth(ds) -> float:
    # Ground pixel size in X = magnitude including skew: sqrt(scaleX^2 + skewY^2).
    # Mirrors heavyweight RST_PixelWidth (always non-negative); distinct from the
    # raw signed scalex(). rasterio Affine: a=scaleX(gt1), d=skewY(gt4).
    return float(math.hypot(ds.transform.a, ds.transform.d))


def pixelheight(ds) -> float:
    # Ground pixel size in Y = magnitude including skew: sqrt(scaleY^2 + skewX^2).
    # Mirrors heavyweight RST_PixelHeight (always non-negative); distinct from the
    # raw signed scaley(). rasterio Affine: e=scaleY(gt5), b=skewX(gt2).
    return float(math.hypot(ds.transform.e, ds.transform.b))


def upperleftx(ds) -> float:
    return float(ds.transform.c)


def upperlefty(ds) -> float:
    return float(ds.transform.f)


def boundingbox(ds) -> bytes:
    b = ds.bounds  # (left, bottom, right, top)
    return _wkb.dumps(box(b.left, b.bottom, b.right, b.top))


def metadata(ds) -> Dict[str, str]:
    meta = {
        "driver": ds.driver,
        "width": str(ds.width),
        "height": str(ds.height),
        "count": str(ds.count),
        "dtype": str(ds.dtypes[0]) if ds.count else "",
        "crs": ds.crs.to_string() if ds.crs is not None else "",
        "nodata": "" if ds.nodata is None else str(ds.nodata),
    }
    meta.update({f"tag.{k}": str(v) for k, v in ds.tags().items()})
    return meta


def scalex(ds) -> float:
    return float(ds.transform.a)


def scaley(ds) -> float:
    return float(ds.transform.e)


def isempty(ds) -> bool:
    """True if the raster has no size, or every band has zero valid pixels.

    Mirrors heavyweight RasterAccessors.isEmpty (null / no size / all bands
    fully NoData). A dimensionally-valid raster whose every band is NoData is
    still empty (issue #59).
    """
    if int(ds.width) == 0 or int(ds.height) == 0 or int(ds.count) == 0:
        return True
    return all(_valid_values(ds, bi).size == 0 for bi in range(1, ds.count + 1))


def type(ds) -> List[str]:
    """Return the GDAL data-type name per band (e.g. ['Float32', 'Float32'])."""
    return [_NP_TO_GDAL.get(str(dt), str(dt)) for dt in ds.dtypes]


def getnodata(ds) -> Optional[List[float]]:
    """Return the NoData value per band as a list of doubles, or None if not set."""
    nd = ds.nodata
    if nd is None:
        return None
    return [float(nd)] * ds.count


# --- per-band statistics over VALID pixels ----------------------------------
# Heavyweight uses GDAL GetStatistics, which excludes NoData. We mirror that by
# masking with rasterio read_masks (0 = invalid; also covers nodata).
def _valid_values(ds, band_index: int) -> np.ndarray:
    """Return a 1-D array of valid (non-masked, non-NoData) pixel values for a band."""
    arr = ds.read(band_index)
    mask = ds.read_masks(band_index)  # uint8: 0 = invalid, 255 = valid
    return arr[mask != 0].ravel()


def avg(ds) -> List[Optional[float]]:
    """Per-band mean of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.mean(vals)) if vals.size else None)
    return out


def minimum(ds) -> List[Optional[float]]:
    """Per-band min of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.min(vals)) if vals.size else None)
    return out


def maximum(ds) -> List[Optional[float]]:
    """Per-band max of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.max(vals)) if vals.size else None)
    return out


def median(ds) -> List[Optional[float]]:
    """Per-band median of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.median(vals)) if vals.size else None)
    return out


def pixelcount(ds) -> List[int]:
    """Per-band count of valid pixels (0 for empty/all-invalid bands)."""
    return [int(_valid_values(ds, bi).size) for bi in range(1, ds.count + 1)]


# --- geotransform-derived accessors -----------------------------------------
def _gdal_gt(ds):
    """Return the GDAL 6-tuple geotransform (gt0..gt5)."""
    return ds.transform.to_gdal()


def rotation(ds) -> float:
    """Rotation angle = atan(skewY / scaleX) = atan(gt4 / gt1)."""
    gt = _gdal_gt(ds)
    return math.atan(gt[4] / gt[1])


def skewx(ds) -> float:
    """X skew = gt2."""
    return float(_gdal_gt(ds)[2])


def skewy(ds) -> float:
    """Y skew = gt4."""
    return float(_gdal_gt(ds)[4])


def format(ds) -> str:  # noqa: A001 - mirrors heavyweight rst_format name
    """GDAL driver short name (e.g. 'GTiff')."""
    return ds.driver


def georeference(ds) -> Dict[str, float]:
    """Geotransform as a map with the heavyweight key names."""
    gt = _gdal_gt(ds)
    return {
        "upperLeftX": float(gt[0]),
        "upperLeftY": float(gt[3]),
        "scaleX": float(gt[1]),
        "scaleY": float(gt[5]),
        "skewX": float(gt[2]),
        "skewY": float(gt[4]),
    }


def bandmetadata(ds, band: int) -> Dict[str, str]:
    """Metadata tags of the given 1-based band (values coerced to str)."""
    return {str(k): str(v) for k, v in ds.tags(int(band)).items()}


def subdatasets(ds) -> Dict[str, str]:
    """Subdataset map mirroring GDAL's GetMetadata_Dict('SUBDATASETS').

    Keys follow GDAL's SUBDATASET_<n>_NAME / SUBDATASET_<n>_DESC convention; for
    a plain single-dataset raster this is an empty map.
    """
    try:
        return {str(k): str(v) for k, v in ds.tags(ns="SUBDATASETS").items()}
    except Exception:
        return {}


def getsubdataset(ds, name: str):
    """Open the named subdataset and return its GTiff-encoded bytes.

    Mirrors heavyweight RST_GetSubdataset: builds ``<driver>:<path>:<name>`` and
    opens it. Raises ValueError if no subdataset matches ``name``.
    """
    import rasterio
    from rasterio.io import MemoryFile

    subs = ds.subdatasets or []
    # Match the subdataset URI whose trailing token equals the requested name.
    match = None
    for uri in subs:
        # URIs look like 'NETCDF:"/path/file.nc":varname'
        if uri.rsplit(":", 1)[-1].strip('"') == str(name) or uri.endswith(str(name)):
            match = uri
            break
    if match is None:
        raise ValueError(
            f"gbx_rst_getsubdataset: no subdataset named '{name}' "
            f"(available: {subs})"
        )
    with rasterio.open(match) as sub:
        profile = sub.profile
        profile.update(driver="GTiff")
        data = sub.read()
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            return mf.read()


def summary(ds) -> str:
    """gdalinfo-style JSON summary string with per-band statistics.

    Heavyweight emits GDAL's ``gdalinfo -json``; rasterio has no exact
    equivalent, so we emit a stable JSON dict (driver, size, crs, bands with
    min/max/mean/stdDev over valid pixels). Shape is GeoBrix-specific, not a
    byte-for-byte gdalinfo match.
    """
    bands = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        if vals.size:
            stats = {
                "min": float(np.min(vals)),
                "max": float(np.max(vals)),
                "mean": float(np.mean(vals)),
                "stdDev": float(np.std(vals)),
            }
        else:
            stats = {"min": None, "max": None, "mean": None, "stdDev": None}
        nd = ds.nodatavals[bi - 1] if ds.nodatavals else None
        bands.append(
            {
                "band": bi,
                "type": _NP_TO_GDAL.get(str(ds.dtypes[bi - 1]), str(ds.dtypes[bi - 1])),
                "noDataValue": None if nd is None else float(nd),
                **stats,
            }
        )
    info = {
        "driverShortName": ds.driver,
        "size": [int(ds.width), int(ds.height)],
        "coordinateSystem": {
            "epsg": ds.crs.to_epsg() if ds.crs is not None else None,
            "crs": crs_to_canonical(ds.crs),
        },
        "geoTransform": list(_gdal_gt(ds)),
        "bands": bands,
    }
    return json.dumps(info)


def histogram(
    ds,
    n_buckets: int = 256,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    include_nodata: bool = False,
) -> Dict[str, List[int]]:
    """Per-band histogram as MAP<STRING, ARRAY<LONG>> keyed by ``band_<i>``.

    Mirrors heavyweight RST_Histogram: ``n_buckets`` equal-width buckets across
    ``[min, max]``; values outside the range are dropped (no out-of-range
    bucket). When min/max are omitted they default to the band's valid-pixel
    range. ``include_nodata`` includes masked pixels in the binning.
    """
    n_buckets = int(n_buckets)
    if n_buckets < 1:
        raise ValueError(f"gbx_rst_histogram: n_buckets must be >= 1; got {n_buckets}")
    result: Dict[str, List[int]] = {}
    for bi in range(1, ds.count + 1):
        if include_nodata:
            vals = ds.read(bi).ravel()
        else:
            vals = _valid_values(ds, bi)
        if min_val is not None and max_val is not None:
            lo, hi = float(min_val), float(max_val)
        else:
            if vals.size:
                lo = float(min_val) if min_val is not None else float(np.min(vals))
                hi = float(max_val) if max_val is not None else float(np.max(vals))
            else:
                lo = float(min_val) if min_val is not None else 0.0
                hi = float(max_val) if max_val is not None else 0.0
        # GDAL requires hi > lo; pad a constant range so all pixels land in bucket 0.
        if not (hi > lo):
            eps = 1.0 if lo == 0.0 else abs(lo) * 1e-9 + 1e-12
            hi = lo + eps
        # numpy.histogram is [lo, hi) per bin except the last bin is [.., hi];
        # values strictly outside [lo, hi] are dropped, matching the contract.
        in_range = vals[(vals >= lo) & (vals <= hi)]
        counts, _ = np.histogram(in_range, bins=n_buckets, range=(lo, hi))
        result[f"band_{bi}"] = [int(c) for c in counts]
    return result
