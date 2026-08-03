"""Spark-free RasterX *Operations* ports: try-open validity check, format
conversion, internal-overview building, and point sampling.

Faithful ports of the heavyweight ``gbx_rst_tryopen`` / ``gbx_rst_asformat`` /
``gbx_rst_buildoverviews`` / ``gbx_rst_sample`` expressions, implemented with
rasterio's bundled GDAL (no JAR)."""

import os
import tempfile

import rasterio
import shapely.wkb
from rasterio.enums import Resampling
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import compression as _comp

# Heavyweight gdaladdo resampling name -> rasterio.enums.Resampling name.
# Mirrors the AllowedResampling set in RST_BuildOverviews.scala; "near" is the
# heavyweight alias for "nearest" and "cubicspline" maps to GDAL's cubic_spline.
_OVERVIEW_RESAMPLING_MAP = {
    "nearest": "nearest",
    "near": "nearest",
    "average": "average",
    "rms": "rms",
    "gauss": "gauss",
    "cubic": "cubic",
    "cubicspline": "cubic_spline",
    "lanczos": "lanczos",
    "bilinear": "bilinear",
    "mode": "mode",
}


def try_open(raster_bytes: bytes) -> bool:
    """Return True if ``raster_bytes`` open as a valid raster, False otherwise.

    Mirrors the heavyweight ``gbx_rst_tryopen``: any failure to open (corrupt
    bytes, unknown format, etc.) yields False rather than raising.
    """
    if raster_bytes is None:
        return False
    try:
        with _serde.open_tile(bytes(raster_bytes)) as ds:
            # Touch a property so a lazily-opened-but-invalid dataset still trips.
            _ = ds.count
        return True
    except Exception:
        return False


def as_format(ds, new_format: str) -> bytes:
    """Re-encode the raster to another GDAL driver (e.g. PNG, GTiff).

    Mirrors the heavyweight ``gbx_rst_asformat``. Validates the requested
    driver is available in rasterio's bundled GDAL build; raises ValueError
    otherwise. Returns the raster bytes encoded in ``new_format``.
    """
    new_format = str(new_format)
    # raster_driver_extensions maps extension -> driver short name; the value
    # set is the writable raster drivers available in this GDAL build.
    available = set(rasterio.drivers.raster_driver_extensions().values())
    if new_format not in available:
        raise ValueError(
            f"rst_asformat: driver '{new_format}' is not available in this "
            f"GDAL build; available: {', '.join(sorted(available))}"
        )
    data = ds.read()
    profile = ds.profile.copy()
    profile.update(driver=new_format)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def build_overviews(ds, levels, resampling: str = "average") -> bytes:
    """Build internal pyramid overviews at ``levels`` and return GTiff bytes.

    Mirrors the heavyweight ``gbx_rst_buildoverviews``:
      * ``levels`` is a non-empty list of integer decimation factors, each >= 2.
      * ``resampling`` defaults to "average"; mapped to rasterio's Resampling
        enum (near->nearest, cubicspline->cubic_spline, ...).

    Overviews are embedded internally in the GTiff (no .ovr sidecar).
    """
    if levels is None or len(levels) == 0:
        raise ValueError(
            "rst_buildoverviews: levels must be a non-empty integer array "
            "(e.g. [2, 4, 8])"
        )
    levels = [int(level) for level in levels]
    for level in levels:
        if level < 2:
            raise ValueError(
                f"rst_buildoverviews: each level must be >= 2; got {level}"
            )
    resampling = (
        "average" if resampling is None or resampling == "" else str(resampling)
    )
    key = resampling.lower()
    if key not in _OVERVIEW_RESAMPLING_MAP:
        allowed = ", ".join(sorted(_OVERVIEW_RESAMPLING_MAP))
        raise ValueError(
            f"rst_buildoverviews: unsupported resampling '{resampling}'; "
            f"allowed: {allowed}"
        )
    resampling_enum = Resampling[_OVERVIEW_RESAMPLING_MAP[key]]

    data = ds.read()
    out_dtype = ds.dtypes[0]
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=data.nbytes, compress="auto")
    )
    # BuildOverviews needs an r+ dataset; reopening a MemoryFile in update mode
    # mints a fresh vsimem path that does not see the just-written bytes. A
    # round-trip through a real temp file keeps the path stable, so overviews
    # embed internally; then read the bytes (with their .ovr) back.
    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.close()
    try:
        with rasterio.open(tmp.name, "w", **profile) as dst:
            dst.write(data)
        with rasterio.open(tmp.name, "r+") as dst:
            dst.build_overviews(levels, resampling_enum)
            dst.update_tags(ns="rio_overview", resampling=key)
        with open(tmp.name, "rb") as fh:
            return fh.read()
    finally:
        os.unlink(tmp.name)


def sample(ds, geom) -> list:
    """Sample per-band raster values at a POINT geometry.

    Mirrors the heavyweight ``gbx_rst_sample`` (requires a POINT; one Double per
    band in band order) with two robustness guarantees beyond the heavy compute
    path:

      * **CRS alignment** — ``geom`` may be a shapely geometry (carrying an SRID
        via ``shapely.set_srid``) or raw WKB/EWKB ``bytes`` (back-compat). When
        the point carries a positive SRID and the raster has a CRS, the point is
        reprojected from EPSG:srid to the raster CRS before indexing (so a 4326
        point against a UTM raster lands correctly). Otherwise the point is
        assumed already aligned to the raster CRS. (Heavy's pure ``execute`` path
        assumes the point is pre-aligned; the light tier reprojects so a
        differently-projected point does not silently miss.)
      * **Graceful out-of-bounds** — a point outside the raster pixel extent
        returns ``None`` (not the NoData fill ``ds.sample`` would emit, and not a
        crash), matching heavy ``RST_Sample`` which returns ``null``.
    """
    if isinstance(geom, (bytes, bytearray)):
        geom = shapely.wkb.loads(bytes(geom))  # handles WKB and EWKB
    if geom.geom_type != "Point":
        raise ValueError(f"rst_sample requires a POINT geometry; got {geom.geom_type}")

    x, y = geom.x, geom.y
    srid = shapely.get_srid(geom)  # 0 when no SRID is set
    dst_crs = ds.crs  # rasterio CRS or None
    if srid > 0 and dst_crs is not None:
        try:
            from rasterio.warp import transform as _transform

            src_crs = rasterio.crs.CRS.from_epsg(srid)
            if src_crs != dst_crs:
                xs, ys = _transform(src_crs, dst_crs, [x], [y])
                x, y = xs[0], ys[0]
        except Exception:
            # Unknown EPSG / transform failure -> assume already aligned.
            x, y = geom.x, geom.y

    # Guard the pixel extent ourselves: ds.sample yields the band's NoData fill
    # for an out-of-window point rather than signalling miss, so a row far from
    # the raster would look like a valid (NoData) reading. ds.index gives the
    # (row, col); outside [0,h) x [0,w) -> None (heavy returns null).
    row, col = ds.index(x, y)
    if not (0 <= row < ds.height and 0 <= col < ds.width):
        return None
    values = list(ds.sample([(x, y)]))[0]
    return [float(v) for v in values]
