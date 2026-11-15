"""Read a window of an open raster, clipped to a polygon, as GTiff bytes.

One implementation for the Python and Unity Catalog levels of ``gbx_rst_fromfile``: both read the window of the
geometry's envelope and clip it with the kernel ``gbx_rst_clip`` runs (``edit.clip_to_geom``), so the two levels give
the same pixels.
"""

from __future__ import annotations

from typing import Optional

import numpy as np


def _decoded_bytes(ds, w, h) -> int:
    return int(ds.count) * int(w) * int(h) * int(np.dtype(ds.dtypes[0]).itemsize)


def read_clip(
    ds,
    geom=None,
    geom_crs: Optional[str] = None,
    all_touched: bool = False,
    cap_bytes: Optional[int] = None,
) -> Optional[bytes]:
    """GTiff bytes of the window of the open dataset *ds* that covers *geom* (all of *ds* when ``geom`` is None),
    clipped to *geom* when it is given.

    *geom* is a shapely geometry in *geom_crs* (the raster's CRS when None). Returns None when *geom* misses the
    raster. Raises ``StageTooLargeError`` with a Fix when the window decodes to more than *cap_bytes*; the check runs
    before any pixel is read.
    """
    from rasterio.io import MemoryFile
    from rasterio.windows import Window

    from databricks.labs.gbx.ds._window import window_for_geom
    from databricks.labs.gbx.ds.file_gbx import StageTooLargeError
    from databricks.labs.gbx.pyrx.core.compression import write_gtiff
    from databricks.labs.gbx.pyrx.core.edit import clip_to_geom

    win = (
        Window(0, 0, ds.width, ds.height)
        if geom is None
        else window_for_geom(ds, geom, geom_crs)
    )
    if win is None:
        return None
    decoded = _decoded_bytes(ds, win.width, win.height)
    if cap_bytes is not None and decoded > cap_bytes:
        raise StageTooLargeError(
            f"gbx_rst_fromfile: the window is {decoded} bytes decoded, over the {cap_bytes}-byte cap. "
            "Fix: pass a coarser resolution or a smaller geom."
        )
    data = ds.read(window=win)
    profile = {
        **ds.profile,
        "width": int(win.width),
        "height": int(win.height),
        "transform": ds.window_transform(win),
    }
    window_bytes = write_gtiff(
        profile, data, alpha_like=ds.colorinterp, all_labels=True
    )
    if geom is None:
        return window_bytes
    with MemoryFile(window_bytes) as mf, mf.open() as wds:
        return clip_to_geom(wds, geom, bool(all_touched), geom_crs=geom_crs)
