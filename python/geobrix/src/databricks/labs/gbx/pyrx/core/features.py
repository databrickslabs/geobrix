"""Spark-free vector<->raster bridge ops. rasterize: build a raster from a
geometry. fill_nodata: interpolate across NoData. Both return GTiff bytes."""

import numpy as np
import shapely.wkb
from rasterio.features import rasterize as _rasterize
from rasterio.features import shapes as _shapes
from rasterio.fill import fillnodata as _fillnodata
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from shapely.geometry import shape as _shape

_NODATA = -9999.0


def rasterize_geom(
    geom_wkb: bytes, value, xmin, ymin, xmax, ymax, width_px, height_px,
    out_srid=None, out_crs=None
) -> bytes:
    """Burn a geometry (WKB/EWKB) into a new raster at the given extent/size.

    Pixels inside the geometry get *value*; outside pixels get NoData (-9999.0).
    Returns GTiff bytes.

    Output CRS (Rule 2): ``out_crs`` (string) wins over ``out_srid`` (int); both
    set -> error; neither -> the geometry's own source CRS carried through (or
    CRS-less). The geometry is reprojected from its source CRS (embedded EWKB
    SRID) into the output CRS before burning, so a geometry in one CRS and an
    output declared in another is correct — not garbage (Rule-2 reprojection).
    """
    from databricks.labs.gbx.pyrx.core.crs import (
        crs_to_canonical,
        get_transformer,
        resolve_crs,
        resolve_source_crs,
    )

    geom = shapely.wkb.loads(bytes(geom_wkb))
    width_px = int(width_px)
    height_px = int(height_px)

    # Rule 1 source CRS (embedded SRID only — rasterize has no source param).
    src_crs = resolve_source_crs(shapely.get_srid(geom))
    # Rule 2 target CRS: out_crs wins over out_srid (both -> error); neither ->
    # the source CRS carried through (NOT a forced default).
    if out_srid is not None and out_crs is not None:
        raise ValueError("rst_rasterize: provide out_srid OR out_crs, not both")
    if out_crs is not None:
        tgt_crs = resolve_crs(out_crs)
    elif out_srid is not None:
        tgt_crs = resolve_crs(out_srid)
    else:
        tgt_crs = src_crs  # carry source through; may be None (CRS-less)

    # Reproject the geometry source -> target before burning (never on CRS-less).
    if src_crs is not None and tgt_crs is not None and src_crs != tgt_crs:
        from shapely.ops import transform as _shapely_transform

        tr = get_transformer(src_crs, tgt_crs)
        geom = _shapely_transform(lambda xs, ys: tr.transform(xs, ys), geom)

    transform = from_bounds(
        float(xmin), float(ymin), float(xmax), float(ymax), width_px, height_px
    )
    arr = _rasterize(
        [(geom, float(value))],
        out_shape=(height_px, width_px),
        transform=transform,
        fill=_NODATA,
        dtype="float64",
    )
    profile = dict(
        driver="GTiff",
        width=width_px,
        height=height_px,
        count=1,
        dtype="float64",
        crs=crs_to_canonical(tgt_crs),  # None -> CRS-less output
        transform=transform,
        nodata=_NODATA,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr, 1)
        return mf.read()


def polygonize(ds, band: int = 1, connectedness: int = 4):
    """Extract vector polygons from contiguous equal-value regions of a band.

    Returns a list of (geom_wkb: bytes, value: float). NoData pixels are
    excluded via the band mask. ``connectedness`` is 4 or 8.

    Mirrors heavyweight ``gbx_rst_polygonize`` (GDAL ``GDALPolygonize``), which
    is the *integer* polygonize: it reads the band as ``Int32``, so a float
    band is ROUNDED to the nearest integer (GDAL's float->int RasterIO uses
    round-to-nearest, not truncation) before contiguous equal-value regions are
    grouped. Grouping on raw float equality (as ``rasterio.features.shapes``
    does natively) would emit one polygon per pixel on any continuous band --
    both wrong vs the GDAL contract and ~100x slower. We round to int32 first to
    match GDAL exactly (a bare truncating cast collapses a [0,1] field to a
    single 0-region and diverges from heavy's 1664/6835 regions).
    """
    b = int(band)
    arr = ds.read(b)
    msk = ds.read_masks(b)  # 0 where NoData -> excluded from shapes
    # GDALPolygonize reads the band as Int32: GDAL's float->int RasterIO rounds
    # to nearest, so np.round (round-half-to-even) reproduces it bit-for-bit.
    arr = np.round(arr).astype("int32")
    out = []
    for geom_dict, value in _shapes(
        arr, mask=msk, connectivity=int(connectedness), transform=ds.transform
    ):
        out.append((shapely.wkb.dumps(_shape(geom_dict)), float(value)))
    return out


def fill_nodata(ds, max_search_dist=None, smoothing_iter=None) -> bytes:
    """Interpolate across NoData gaps in a raster dataset.

    *ds* is an open rasterio DatasetReader. Returns GTiff bytes with NoData
    pixels filled by interpolation from their neighbours.
    """
    msd = 100.0 if max_search_dist is None else float(max_search_dist)
    smi = 0 if smoothing_iter is None else int(smoothing_iter)
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    bands = []
    for i in range(1, ds.count + 1):
        band = ds.read(i)
        msk = ds.read_masks(i)  # 0 where NoData, 255 where valid
        bands.append(
            _fillnodata(
                band, mask=msk, max_search_distance=msd, smoothing_iterations=smi
            )
        )
    data = np.stack(bands)
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()
