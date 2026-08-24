"""Antimeridian-crossing geometry ops (distributed shapely, light tier).

Pure-shapely coordinate operations that reproduce the PostGIS antimeridian-split
pattern. WKB in/out, CRS preserved (never reprojected). See
docs/docs/api/geometry-validity-antimeridian.mdx.
"""
from typing import Optional

import numpy as np
from shapely import get_srid, set_srid, to_wkb
from shapely.ops import transform

from ._geom import parse_geom


def _finish(g, srid: int) -> bytes:
    """Re-stamp the original SRID and emit EWKB (SRID preserved, no reprojection)."""
    if srid:
        g = set_srid(g, srid)
    return to_wkb(g, include_srid=True)


def st_shiftlongitude(geom) -> Optional[bytes]:
    """Shift X from [-180, 180] into [0, 360] (PostGIS ST_ShiftLongitude)."""
    g = parse_geom(geom)
    if g is None:
        return None
    srid = get_srid(g)

    def _shift(x, y, z=None):
        x = np.asarray(x, dtype=float)
        x = np.where(x < 0.0, x + 360.0, x)
        return (x, y) if z is None else (x, y, z)

    return _finish(transform(_shift, g), srid)


def _udf_st_shiftlongitude(geom) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_shiftlongitude (BINARY return)."""
    return st_shiftlongitude(geom)
