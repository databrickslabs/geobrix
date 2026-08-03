"""Clip a windowed dataset to a polygon (virtual-tile clip stage).

Thin adapter over pyrx.core.edit.clip_to_geom, which already: reprojects a
cutline carrying a positive SRID to the raster CRS, masks with crop=True (the
intersection envelope), and returns None on non-overlap. This adds clip_crs
precedence: embedded SRID (EWKB/EWKT with SRID > 0) wins; clip_crs only applies
when the geometry carries no embedded SRID (SRID == 0), so a plain WKB/WKT can
declare its CRS. Reprojects the polygon, never the raster.
"""

from typing import Optional

import shapely

from databricks.labs.gbx._geom import parse_geom
from databricks.labs.gbx.pyrx.core import edit


def _epsg_int(clip_crs: str) -> Optional[int]:
    """Parse 'EPSG:4326' / '4326' -> 4326; None if not an EPSG code."""
    s = clip_crs.strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None  # WKT2/PROJ string not yet supported for stamping


def clip_dataset(ds, clip_polygon: bytes, clip_crs: Optional[str]) -> Optional[bytes]:
    geom = parse_geom(clip_polygon)
    if geom is None:
        return None
    # clip_crs applies ONLY when the geometry carries no embedded SRID.
    # embedded SRID (>0) -> clip_crs -> raster CRS (edit.clip_to_geom's own fallback).
    if clip_crs and shapely.get_srid(geom) <= 0:
        code = _epsg_int(clip_crs)
        if code is not None:
            geom = shapely.set_srid(geom, code)
    return edit.clip_to_geom(ds, geom)
