import matplotlib

matplotlib.use("Agg")  # headless: no display needed

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pytest  # noqa: E402
import rasterio  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

from databricks.labs.gbx.vizx import plot_mosaic  # noqa: E402


def _write_cog(
    path,
    *,
    width,
    height,
    origin=(10.0, 50.0),
    pixel=0.5,
    epsg=4326,
    nodata=-9999.0,
    count=1,
    overviews=True,
    tags=None,
):
    """Write a small COG with a known georeference. origin=(ulx, uly), north-up.

    overviews=False writes a plain GTiff (no internal overviews) to exercise the
    single-zoom on-the-fly decimation fallback.
    """
    transform = from_origin(origin[0], origin[1], pixel, pixel)
    driver = "COG" if overviews else "GTiff"
    profile = dict(
        driver=driver,
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=nodata,
    )
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
    srcs = "".join(f"""<SimpleSource>
      <SourceFilename relativeToVRT="1">{rel}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SrcRect xOff="0" yOff="0" xSize="{w}" ySize="{h}"/>
      <DstRect xOff="{xo}" yOff="{yo}" xSize="{w}" ySize="{h}"/>
    </SimpleSource>""" for (rel, xo, yo, w, h) in members)
    xml = f"""<VRTDataset rasterXSize="{width}" rasterYSize="{height}">
  <SRS>{crs_wkt}</SRS>
  <GeoTransform>{gt[0]}, {gt[1]}, {gt[2]}, {gt[3]}, {gt[4]}, {gt[5]}</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>{nodata}</NoDataValue>
    {srcs}
  </VRTRasterBand>
</VRTDataset>"""
    with open(vrt_path, "w") as fh:
        fh.write(xml)
    return vrt_path


def _build_native_mosaic(tmp_path, *, overviews=True):
    """Two 64x64 tiles side by side -> 128x64 mosaic in EPSG:4326."""
    epsg = 4326
    _write_cog(
        str(tmp_path / "t0.tif"),
        width=64,
        height=64,
        origin=(10.0, 50.0),
        pixel=0.5,
        epsg=epsg,
        overviews=overviews,
    )
    _write_cog(
        str(tmp_path / "t1.tif"),
        width=64,
        height=64,
        origin=(10.0 + 0.5 * 64, 50.0),
        pixel=0.5,
        epsg=epsg,
        overviews=overviews,
    )
    crs_wkt = rasterio.crs.CRS.from_epsg(epsg).to_wkt()
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    vrt = str(tmp_path / "mosaic.vrt")
    _write_vrt(
        vrt,
        [("t0.tif", 0, 0, 64, 64), ("t1.tif", 64, 0, 64, 64)],
        transform=transform,
        width=128,
        height=64,
        crs_wkt=crs_wkt,
    )
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


def test_plot_mosaic_bbox_narrows_extent(tmp_path):
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)  # full extent x: 10..74, y: 18..50
    plot_mosaic(vrt, bbox=(10.0, 34.0, 42.0, 50.0), max_pixels=256, debug_mode=0)
    ax = plt.gcf().axes[0]
    x0, x1 = ax.get_xlim()
    assert x1 - x0 < 40.0  # narrower than the full 64-degree width


def test_plot_mosaic_bbox_non_intersecting_raises(tmp_path):
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)
    with pytest.raises(ValueError):
        plot_mosaic(vrt, bbox=(200.0, 80.0, 210.0, 85.0), debug_mode=0)


def test_plot_mosaic_bbox_crs_reprojects(tmp_path):
    """A bbox given in EPSG:3857 must reproject to the 4326 mosaic and render."""
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)
    from databricks.labs.gbx.core.crs import get_transformer

    tr = get_transformer("EPSG:4326", "EPSG:3857")
    x0, y0 = tr.transform(10.0, 34.0)
    x1, y1 = tr.transform(42.0, 50.0)
    plot_mosaic(
        vrt, bbox=(x0, y0, x1, y1), bbox_crs="EPSG:3857", max_pixels=256, debug_mode=0
    )
    assert len(plt.get_fignums()) == 1


def _build_h3_mosaic(tmp_path):
    """Two h3 res-1 cells as tagged mini-COGs + VRT, with nodata margins.

    Uses real h3 cells so cell_to_boundary reconstructs valid outlines.
    """
    import h3

    epsg = 4326
    cells = list(h3.cell_to_children(h3.latlng_to_cell(50.0, 10.0, 0), 1))[:2]
    members = []
    # place the two tiles side by side in a synthetic 128x64 grid for simplicity
    for i, cellid in enumerate(cells):
        path = str(tmp_path / f"cell_{i}.tif")
        _write_cog(
            path,
            width=64,
            height=64,
            origin=(10.0 + i * 0.5 * 64, 50.0),
            pixel=0.5,
            epsg=epsg,
            nodata=-9999.0,
            overviews=False,
            tags={"GBX_CELLID": cellid, "GBX_GRIDSYSTEM": "h3"},
        )
        # punch a nodata border so a "masked" region exists
        with rasterio.open(path, "r+") as ds:
            arr = ds.read(1)
            arr[:8, :] = -9999.0
            ds.write(arr, 1)
        members.append((f"cell_{i}.tif", i * 64, 0, 64, 64))
    crs_wkt = rasterio.crs.CRS.from_epsg(epsg).to_wkt()
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    vrt = str(tmp_path / "mosaic.vrt")
    _write_vrt(vrt, members, transform=transform, width=128, height=64, crs_wkt=crs_wkt)
    return vrt, cells


def test_plot_mosaic_h3_masked_nodata_transparent(tmp_path):
    plt.close("all")
    vrt, _ = _build_h3_mosaic(tmp_path)
    plot_mosaic(vrt, max_pixels=256, debug_mode=0)
    ax = plt.gcf().axes[0]
    arr = ax.get_images()[0].get_array()
    # rendered array is a masked array; the nodata border must be masked out
    assert np.ma.is_masked(arr)


def test_plot_mosaic_show_cells_draws_outlines(tmp_path):
    plt.close("all")
    vrt, cells = _build_h3_mosaic(tmp_path)
    plot_mosaic(vrt, show_cells=True, max_pixels=256, debug_mode=0)
    ax = plt.gcf().axes[0]
    assert len(ax.patches) == len(cells)  # one polygon per cell


def test_plot_mosaic_show_cells_non_h3_raises(tmp_path):
    plt.close("all")
    vrt = _build_native_mosaic(tmp_path)  # no grid tags
    with pytest.raises(ValueError):
        plot_mosaic(vrt, show_cells=True, debug_mode=0)
