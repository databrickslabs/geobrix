"""Shared, shapely-only geometry-input parsing for all GeoBrix light tiers.

This module is the single source of truth for decoding geometry inputs across
the pyrx / pyvx / pygx light tiers. It depends ONLY on ``shapely`` (already a
light-tier dependency), so any light tier can import it without pulling in
another tier's dependencies (e.g. importing this from pyrx must NOT drag in
pyvx's MVT/heavy deps).

Accepted encodings mirror the heavyweight tier, which accepts BINARY|STRING for
geometry inputs: WKB / EWKB ``bytes`` and WKT / EWKT ``str``.
"""

from typing import Any, Optional

import shapely
from shapely import from_wkb, from_wkt, set_srid
from shapely.geometry.base import BaseGeometry


def parse_geom(x: Any) -> Optional[BaseGeometry]:
    """Parse a geometry from WKB/EWKB bytes or WKT/EWKT text. None -> None.

    - ``bytes``/``bytearray`` -> ``from_wkb`` (handles WKB and EWKB).
    - ``str`` -> strip an optional ``SRID=...;`` / ``srid=...;`` EWKT prefix and
      apply ``set_srid``, then ``from_wkt``.
    - shapely geometry -> passthrough.
    - ``None`` / empty string -> ``None``.
    """
    if x is None:
        return None
    if isinstance(x, BaseGeometry):
        return x
    if isinstance(x, (bytes, bytearray)):
        return from_wkb(bytes(x))  # handles WKB and EWKB
    s = str(x).strip()
    if not s:
        return None
    if s[:5].upper() == "SRID=":
        srid_part, _, wkt_part = s.partition(";")
        geom = from_wkt(wkt_part)
        try:
            return set_srid(geom, int(srid_part[5:]))
        except ValueError:
            return geom
    return from_wkt(s)


def geom_to_wkb(x: Any) -> Optional[bytes]:
    """Decode any accepted geometry encoding to WKB ``bytes``. None -> None.

    - WKB/EWKB ``bytes``/``bytearray`` -> returned as-is (already WKB).
    - WKT/EWKT ``str`` -> parsed via :func:`parse_geom` then ``to_wkb``.
    - shapely geometry -> ``to_wkb``.

    The returned WKB bytes are what the pyrx core ops (``edit.clip_to_geom``,
    ``ops_core.sample``, ``features.rasterize_geom``) consume.
    """
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        return bytes(x)  # already WKB / EWKB
    geom = parse_geom(x)
    if geom is None:
        return None
    return shapely.to_wkb(geom)


def jvm_rect(x0: float, y0: float, x1: float, y1: float) -> BaseGeometry:
    """The rectangle (x0,y0)-(x1,y1) with the JVM grid cell's ring: south-west corner first, counter-clockwise,
    closed. shapely's ``box`` starts at the south-east corner, so its WKB and WKT differ from the JVM's.
    """
    return shapely.Polygon([(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)])
