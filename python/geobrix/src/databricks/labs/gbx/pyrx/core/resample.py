"""Spark-free resampling (decimated read + scaled transform). Tile-returning:
returns new GTiff bytes. CRS and geographic extent are preserved; only the
pixel grid (dimensions / resolution) changes."""

from affine import Affine
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import compression as _comp
from databricks.labs.gbx.pyrx.core._util import resampling_enum


def _positive(who: str, **values) -> None:
    """Each value is a positive, finite number, else a ``ValueError`` naming the parameter and its value."""
    for name, v in values.items():
        if not (0.0 < float(v) < float("inf")):
            raise ValueError(f"{who}: {name} must be positive and finite; got {v}")


def check_by_factor(*, factor, algorithm="bilinear", **_) -> None:
    """The arguments of ``rst_resample``: a wrong one raises a ``ValueError`` that names it."""
    _positive("rst_resample", factor=factor)
    resampling_enum(algorithm)


def check_to_size(*, width_px, height_px, algorithm="bilinear", **_) -> None:
    """The arguments of ``rst_resample_to_size``: a wrong one raises a ``ValueError`` that names it."""
    _positive("rst_resample_to_size", width_px=width_px, height_px=height_px)
    resampling_enum(algorithm)


def check_to_res(*, x_res, y_res, algorithm="bilinear", **_) -> None:
    """The arguments of ``rst_resample_to_res``: a wrong one raises a ``ValueError`` that names it."""
    _positive("rst_resample_to_res", x_res=x_res, y_res=y_res)
    resampling_enum(algorithm)


def _write_resampled(ds, dst_width: int, dst_height: int, algorithm: str) -> bytes:
    resampling_enum(algorithm)  # a word that names no resampling raises before any work
    dst_width = max(1, int(dst_width))
    dst_height = max(1, int(dst_height))
    # Scale the affine so the same ground extent maps onto the new pixel grid.
    new_transform = ds.transform * Affine.scale(
        ds.width / dst_width, ds.height / dst_height
    )
    profile = _comp.source_profile(ds.profile)
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
            _comp.match_alpha(dst, ds.colorinterp)
            dst.write(data)
        return mf.read()


def resample_by_factor(ds, factor: float, algorithm: str = "bilinear") -> bytes:
    """Resample by a multiplicative factor (>1 upsamples, 0<factor<1 downsamples)."""
    check_by_factor(factor=factor, algorithm=algorithm)
    f = float(factor)
    return _write_resampled(ds, round(ds.width * f), round(ds.height * f), algorithm)


def resample_to_size(ds, width_px, height_px, algorithm: str = "bilinear") -> bytes:
    """Resample to exact pixel dimensions."""
    check_to_size(width_px=width_px, height_px=height_px, algorithm=algorithm)
    return _write_resampled(ds, width_px, height_px, algorithm)


def resample_to_res(ds, x_res, y_res, algorithm: str = "bilinear") -> bytes:
    """Resample to a target ground resolution in CRS units."""
    check_to_res(x_res=x_res, y_res=y_res, algorithm=algorithm)
    left, bottom, right, top = ds.bounds
    return _write_resampled(
        ds,
        round((right - left) / float(x_res)),
        round((top - bottom) / float(y_res)),
        algorithm,
    )
