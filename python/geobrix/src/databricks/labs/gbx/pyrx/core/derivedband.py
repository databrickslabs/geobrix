"""Spark-free derived-band: apply a USER-PROVIDED Python function to the raster's
bands. The function follows GDAL's VRT Python pixel-function signature, so a
pyfunc written for the heavyweight rst_derivedband works here unchanged.

SECURITY: the pyfunc source is exec'd IN-PROCESS WITHOUT SANDBOXING — treat it as
trusted developer code (the same trust model as any user-authored Spark UDF). Do
NOT pass pyfunc sourced from untrusted input."""

import numpy as np

from databricks.labs.gbx.pyrx.core import _nodata
from databricks.labs.gbx.pyrx.core import compression as _comp


def derivedband(ds, pyfunc: str, func_name: str) -> bytes:
    """Apply a user-provided Python function to the raster's bands.

    Args:
        ds:        Open rasterio DatasetReader.
        pyfunc:    Python source code (string) defining the function.  The
                   function must follow GDAL's VRT pixel-function signature::

                       func(in_ar, out_ar, xoff, yoff, xsize, ysize,
                            raster_xsize, raster_ysize, buf_radius, gt, **kwargs)

                   where ``in_ar`` is a list of 2-D NumPy arrays (one per input
                   band) and ``out_ar`` is a preallocated 2-D output array the
                   function fills in-place (``out_ar[:] = ...``).
        func_name: Name of the callable within ``pyfunc``.

    Returns:
        GTiff bytes of the resulting single-band Float64 raster; its own mask band marks the
        pixels the input marks transparent (alpha band) or masked (own mask band).
    """
    namespace = {"np": np, "numpy": np}
    exec(pyfunc, namespace)  # noqa: S102 — trusted developer code by design
    func = namespace[func_name]
    in_ar = [ds.read(i) for i in range(1, ds.count + 1)]
    out_ar = np.zeros((ds.height, ds.width), dtype="float64")
    gt = ds.transform.to_gdal()
    func(in_ar, out_ar, 0, 0, ds.width, ds.height, ds.width, ds.height, 0, gt)
    profile = _comp.drop_ycbcr(ds.profile)
    profile.update(driver="GTiff", count=1, dtype="float64")
    # The output has one band and no alpha band of its own: where the input is transparent
    # or masked (the rule of both tiers, ``_nodata.valid_mask``) the output is invalid,
    # carried in its own mask band. An input with neither is written as before.
    mask = None
    if _nodata.has_alpha(ds) or _nodata.has_own_mask(ds):
        mask = np.where(_nodata.valid_mask(ds), 255, 0).astype("uint8")
    return _comp.write_gtiff(profile, out_ar[np.newaxis, ...], mask=mask)
