"""Spark-free resampling (decimated read + scaled transform). Tile-returning:
returns new GTiff bytes. CRS and geographic extent are preserved; only the
pixel grid (dimensions / resolution) changes."""

from affine import Affine
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core._util import resampling_enum


def _write_resampled(ds, dst_width: int, dst_height: int, algorithm: str) -> bytes:
    dst_width = max(1, int(dst_width))
    dst_height = max(1, int(dst_height))
    # Scale the affine so the same ground extent maps onto the new pixel grid.
    new_transform = ds.transform * Affine.scale(
        ds.width / dst_width, ds.height / dst_height
    )
    profile = ds.profile.copy()
    profile.update(
        driver="GTiff", width=dst_width, height=dst_height, transform=new_transform
    )
    # Passing out_shape lets GDAL serve the read from internal overviews
    # when the dataset has them (COGs being the common case we produce).
    # GDAL selects the best overview level automatically; no explicit
    # overview-selection code is needed.  For upsampling or plain GTiffs
    # without overviews, out_shape triggers the same decimated-read path
    # at full resolution — behaviour is identical to the pre-COG code.
    data = ds.read(
        out_shape=(ds.count, dst_height, dst_width),
        resampling=resampling_enum(algorithm),
    )
    out_dtype = profile.get("dtype", ds.dtypes[0])
    decoded_bytes = data.nbytes
    profile.update(
        _comp.creation_opts(out_dtype, decoded_bytes=decoded_bytes, compress="auto")
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def resample_by_factor(ds, factor: float, algorithm: str = "bilinear") -> bytes:
    """Resample by a multiplicative factor (>1 upsamples, 0<factor<1 downsamples)."""
    f = float(factor)
    return _write_resampled(ds, round(ds.width * f), round(ds.height * f), algorithm)


def resample_to_size(ds, width_px, height_px, algorithm: str = "bilinear") -> bytes:
    """Resample to exact pixel dimensions."""
    return _write_resampled(ds, width_px, height_px, algorithm)


def resample_to_res(ds, x_res, y_res, algorithm: str = "bilinear") -> bytes:
    """Resample to a target ground resolution in CRS units."""
    left, bottom, right, top = ds.bounds
    return _write_resampled(
        ds,
        round((right - left) / float(x_res)),
        round((top - bottom) / float(y_res)),
        algorithm,
    )
