"""Band layouts for the alpha tests: which band, if any, a raster labels as alpha.

GDAL labels band 4 of every 4-band ``uint8`` GTiff as alpha when it creates one, and
masks by alpha only for 2- and 4-band rasters. The layouts below cover the shapes that
told the two apart wrongly: an alpha band that is last in a raster of any band count
(gray+x+alpha, RGB+extra+alpha), an alpha label that is NOT last (RGBA+extra), and a
fourth band that is data (RGB+NIR).
"""

import numpy as np
import rasterio
from rasterio.enums import ColorInterp as CI
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

# name -> (colour interpretation per band, alpha is the last band)
LAYOUTS = {
    "rgba": ((CI.red, CI.green, CI.blue, CI.alpha), True),
    "gray_alpha": ((CI.gray, CI.alpha), True),
    "gray_x_alpha": ((CI.gray, CI.undefined, CI.alpha), True),
    "rgb_x_alpha": (
        (CI.red, CI.green, CI.blue, CI.undefined, CI.undefined, CI.alpha),
        True,
    ),
    "rgba_x": ((CI.red, CI.green, CI.blue, CI.alpha, CI.undefined), False),
    "rgb_nir": ((CI.red, CI.green, CI.blue, CI.undefined), False),
}
LAST_ALPHA = [k for k, (_, last) in LAYOUTS.items() if last]
NOT_ALPHA = [k for k, (_, last) in LAYOUTS.items() if not last]


def pattern(count, dtype="uint8"):
    """``(count, 4, 4)`` pixels. Row 0: every band 7, the last band 0. Row 1: every band 0,
    the last band 255. Row 2: every band 0. Row 3: every band 7, the last band 255."""
    data = np.full((count, 4, 4), 7, dtype=dtype)
    data[-1, 0] = 0
    data[:, 1] = 0
    data[-1, 1] = 255
    data[:, 2] = 0
    data[-1, 3] = 255
    return data


def layout_bytes(
    name,
    *,
    nodata=None,
    dtype="uint8",
    crs="EPSG:4326",
    transform=None,
    data=None,
    mask=None,
):
    """GTiff bytes of layout ``name`` holding :func:`pattern` (or ``data``). ``mask``, a
    ``(rows, cols)`` uint8 array (255 valid, 0 invalid), is written as the raster's own
    internal mask band, beside any alpha band."""
    colorinterp, _ = LAYOUTS[name]
    count = len(colorinterp)
    if data is None:
        data = pattern(count, dtype)
    prof = dict(
        driver="GTiff",
        width=data.shape[2],
        height=data.shape[1],
        count=count,
        dtype=dtype,
        crs=crs,
        transform=transform or from_origin(10, 50, 0.5, 0.5),
        nodata=nodata,
        photometric=(
            "RGB"
            if tuple(colorinterp[:3]) == (CI.red, CI.green, CI.blue)
            else "MINISBLACK"
        ),
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), MemoryFile() as mf:
        with mf.open(**prof) as dst:
            # before the write: GDAL ignores a label set after the pixels are written
            dst.colorinterp = colorinterp
            dst.write(data)
            if mask is not None:
                dst.write_mask(mask)
        raw = mf.read()
    with MemoryFile(raw) as mf, mf.open() as chk:
        assert tuple(chk.colorinterp) == tuple(colorinterp), (name, chk.colorinterp)
    return raw


def labelled_alpha(raster_bytes):
    """The 0-based indices of the bands that ``raster_bytes`` labels alpha."""
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return [i for i, c in enumerate(ds.colorinterp) if c == CI.alpha]


def row_coded(
    name,
    *,
    width=12,
    height=12,
    masked_rows=(),
    transparent_rows=(),
    nodata=None,
    crs="EPSG:4326",
    transform=None,
):
    """Layout ``name`` whose every data band holds ``100 + source row``, so any output pixel
    names the source row it came from. The alpha band (when the layout has one) is 255 except
    in ``transparent_rows`` (0); ``masked_rows`` are hidden by an internal mask band. The valid
    rows are :func:`valid_rows`."""
    colorinterp, alpha_last = LAYOUTS[name]
    count = len(colorinterp)
    data = np.empty((count, height, width), dtype="uint8")
    data[:] = (100 + np.arange(height, dtype="uint8"))[None, :, None]
    if alpha_last:
        data[-1] = 255
        data[-1, list(transparent_rows), :] = 0
    mask = None
    if masked_rows:
        mask = np.full((height, width), 255, dtype="uint8")
        mask[list(masked_rows), :] = 0
    return layout_bytes(
        name,
        data=data,
        mask=mask,
        nodata=nodata,
        crs=crs,
        transform=transform or from_origin(10, 50, 0.5, 0.5),
    )


def valid_rows(name, height=12, masked_rows=(), transparent_rows=()):
    """The rows of :func:`row_coded` that are valid: not masked and, for a layout with an
    alpha band, not transparent."""
    hidden = set(masked_rows) | (set(transparent_rows) if LAYOUTS[name][1] else set())
    return [r for r in range(height) if r not in hidden]
