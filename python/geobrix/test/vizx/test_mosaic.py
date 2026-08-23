import matplotlib
matplotlib.use("Agg")  # headless: no display needed

import os
import numpy as np
import matplotlib.pyplot as plt
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.vizx import plot_mosaic


def _write_cog(path, *, width, height, origin=(10.0, 50.0), pixel=0.5, epsg=4326,
               nodata=-9999.0, count=1, overviews=True, tags=None):
    """Write a small COG with a known georeference. origin=(ulx, uly), north-up.

    overviews=False writes a plain GTiff (no internal overviews) to exercise the
    single-zoom on-the-fly decimation fallback.
    """
    transform = from_origin(origin[0], origin[1], pixel, pixel)
    driver = "COG" if overviews else "GTiff"
    profile = dict(driver=driver, width=width, height=height, count=count,
                   dtype="float32", crs=f"EPSG:{epsg}", transform=transform,
                   nodata=nodata)
    if driver == "COG":
        profile["blocksize"] = 512
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(path, "w", **profile) as ds:
        for b in range(1, count + 1):
            ds.write(data + (b - 1) * 100.0, b)
        if tags:
            ds.update_tags(**tags)
    return transform


def _write_vrt(vrt_path, members, *, transform, width, height, crs_wkt, nodata=-9999.0):
    """Hand-write a minimal 1-band VRT indexing `members` at their pixel offsets.

    members: list of (relpath, xoff, yoff, w, h). relativeToVRT="1".
    """
    gt = transform.to_gdal()  # (c, a, b, f, d, e) GDAL order
    srcs = "".join(
        f'''<SimpleSource>
      <SourceFilename relativeToVRT="1">{rel}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SrcRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/>
      <DstRect xOff="{xo}" yOff="{yo}" xSize="{w}" ySize="{h}"/>
    </SimpleSource>'''
        for (rel, xo, yo, w, h) in members
    )
    xml = f'''<VRTDataset rasterXSize="{width}" rasterYSize="{height}">
  <SRS>{crs_wkt}</SRS>
  <GeoTransform>{gt[0]}, {gt[1]}, {gt[2]}, {gt[3]}, {gt[4]}, {gt[5]}</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>{nodata}</NoDataValue>
    {srcs}
  </VRTRasterBand>
</VRTDataset>'''
    with open(vrt_path, "w") as fh:
        fh.write(xml)
    return vrt_path


def _build_native_mosaic(tmp_path, *, overviews=True):
    """Two 64x64 tiles side by side -> 128x64 mosaic in EPSG:4326."""
    epsg = 4326
    _write_cog(str(tmp_path / "t0.tif"), width=64, height=64,
               origin=(10.0, 50.0), pixel=0.5, epsg=epsg, overviews=overviews)
    _write_cog(str(tmp_path / "t1.tif"), width=64, height=64,
               origin=(10.0 + 0.5 * 64, 50.0), pixel=0.5, epsg=epsg, overviews=overviews)
    crs_wkt = rasterio.crs.CRS.from_epsg(epsg).to_wkt()
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    vrt = str(tmp_path / "mosaic.vrt")
    _write_vrt(vrt, [("t0.tif", 0, 0, 64, 64), ("t1.tif", 64, 0, 64, 64)],
               transform=transform, width=128, height=64, crs_wkt=crs_wkt)
    return vrt


def test_plot_mosaic_full_envelope_native(tmp_path):
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)
    plot_mosaic(vrt, debug_mode=0)
    assert len(plt.get_fignums()) == 1


def test_plot_mosaic_out_shape_bounded_by_max_pixels(tmp_path):
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)
    plot_mosaic(vrt, max_pixels=32, debug_mode=0)
    ax = plt.gcf().axes[0]
    arr = ax.get_images()[0].get_array()
    # rendered array long edge must not exceed max_pixels
    assert max(arr.shape[0], arr.shape[1]) <= 32


def test_plot_mosaic_single_zoom_fallback_renders(tmp_path):
    """A mosaic of overview-free GTiffs still renders (on-the-fly decimation)."""
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path, overviews=False)
    plot_mosaic(vrt, max_pixels=32, debug_mode=0)
    assert len(plt.get_fignums()) == 1


def test_plot_mosaic_accepts_directory(tmp_path):
    plt.close("all")
    _build_native_mosaic(tmp_path)
    plot_mosaic(str(tmp_path), debug_mode=0)  # dir containing exactly one .vrt
    assert len(plt.get_fignums()) == 1
