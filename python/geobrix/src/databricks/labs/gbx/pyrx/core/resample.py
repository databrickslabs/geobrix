"""Spark-free resampling (decimated read + scaled transform). Tile-returning:
returns new GTiff bytes. CRS and geographic extent are preserved; only the
pixel grid (dimensions / resolution) changes."""

from affine import Affine
from rasterio.io import MemoryFile

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
    # rasterio's windowed out_shape read automatically selects the best
    # internal overview level when the source is a COG.  For downsampling
    # a COG this means GDAL reads from the overview band (fewer bytes
    # decoded) rather than decoding full resolution and decimating in
    # memory.  No explicit overview-selection code is needed: out_shape
    # is the lever that triggers this optimisation.
    data = ds.read(
        out_shape=(ds.count, dst_height, dst_width),
        resampling=resampling_enum(algorithm),
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
