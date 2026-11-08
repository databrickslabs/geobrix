"""Shared, shapely-only geometry-input parsing for all GeoBrix light tiers.

This module is the single source of truth for decoding geometry inputs across
the pyrx / pyvx / pygx light tiers. It depends ONLY on ``shapely`` (already a
light-tier dependency), so any light tier can import it without pulling in
another tier's dependencies (e.g. importing this from pyrx must NOT drag in
pyvx's MVT/heavy deps).

Accepted encodings mirror the heavyweight tier, which accepts BINARY|STRING for
geometry inputs: WKB / EWKB ``bytes`` and WKT / EWKT ``str``.
"""

import re
from typing import Any, Optional

import shapely
from shapely import from_wkb, from_wkt, set_srid
from shapely.geometry.base import BaseGeometry

# A WKT numeric ordinate: decimal, exponent, or the non-finite tokens a 3D writer emits
# for a missing Z (NaN / inf, optionally signed).
_WKT_NUM_RE = re.compile(
    r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?" r"|[-+]?nan" r"|[-+]?inf(?:inity)?",
    re.IGNORECASE,
)

# A ``Z`` / ``M`` / ``ZM`` dimensionality tag standing as its own token, e.g. the ``Z`` in
# ``POINT Z (...)`` or in ``POINT Z EMPTY``. Matched with word boundaries so the ``M`` in
# ``MULTIPOINT`` or the ``Z`` inside a name is never touched.
_WKT_DIMTAG_RE = re.compile(r"\s+\b(?:ZM|Z|M)\b", re.IGNORECASE)

# The ``EMPTY`` keyword. An empty component carries no ordinates to pad, so its
# dimensionality can only be restored by re-tagging it explicitly.
_WKT_EMPTY_RE = re.compile(r"\bEMPTY\b", re.IGNORECASE)


def wkt_pad_z(wkt: str) -> str:
    """Rewrite a WKT/EWKT body to uniform 3D: every coordinate gets exactly 3 ordinates.

    ONLY meaningful for a body GEOS has already refused to parse. A WKT whose parts
    disagree about dimensionality (``GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT
    (12 43))``) is rejected outright — ``Cannot mix dimensionality in a geometry`` — so
    there is no geometry object to normalize and the text must be made uniform first.

    PRECONDITION: call this only on WKT that failed to parse (see :func:`parse_geom`, its
    sole caller). Applied to clean 2D WKT it would pad ``POINT (11 42)`` to
    ``POINT (11 42 NaN)``, turning a 2D geometry 3D — so it must never become an
    unconditional pre-parse step. :func:`parse_geom` enforces this by trying a plain parse
    first and only falling back here.

    Padding UP to 3D (missing Z -> the ``NaN`` token) rather than stripping down to 2D
    reproduces the heavyweight tier exactly: JTS reads that same WKT into a uniformly-3D
    geometry carrying ``NaN`` for the absent Z, which is why heavy's ``st_setcrs`` returns
    ``POINT Z (12 43 NaN)``. ``NaN`` is the ABSENCE of a Z — the marker JTS and shapely
    both already use internally — not a fabricated elevation; nothing invents a 0.

    Two structural rules make this work for every WKT shape rather than the flat
    ``GEOMETRYCOLLECTION Z (POINT Z, POINT)`` case only:

    1. **Strip every dimensionality tag first, then let the writer re-derive it.** Editing
       ordinates while leaving the original tags in place produces contradictions that
       GEOS rejects just as hard as the input did — a truncated ``POINT ZM (1 2 3)``, or a
       nested ``GEOMETRYCOLLECTION`` whose coordinates gained a Z but whose tag did not.
       With all tags removed, a uniform 3-ordinate body parses and shapely re-emits the
       correct ``Z`` tags at every nesting level.
    2. **A chunk with fewer than 2 numbers is a keyword, not a coordinate.** An ``EMPTY``
       component has no ordinates to pad, so it is re-tagged ``Z EMPTY`` explicitly —
       otherwise it would come back 2D inside an otherwise-3D collection and the encoded
       bytes would differ from heavy's. A bare top-level ``GEOMETRYCOLLECTION EMPTY``
       stays 2D, matching heavy (there is nothing to be 3D about).

    **M is dropped**, matching heavy: JTS is XYZ-only, so heavy reads ``POINT ZM (1 2 3 4)``
    as ``POINT Z (1 2 3)`` and the measure is gone. Keeping M here would make the tiers
    disagree, and M is out of scope for this family (as it is for ``st_legacyaswkb``).
    """
    out = []
    for chunk in re.split(r"([(),])", wkt):
        if chunk in "(),":
            out.append(chunk)
            continue
        nums = _WKT_NUM_RE.findall(chunk)
        if len(nums) < 2:
            # Keyword chunk (POINT, GEOMETRYCOLLECTION, EMPTY, ...): drop any dim tag so
            # the writer re-derives it from the now-uniform ordinates, then re-assert Z on
            # an EMPTY component, which has no ordinates to derive it from.
            out.append(_WKT_EMPTY_RE.sub("Z EMPTY", _WKT_DIMTAG_RE.sub("", chunk)))
            continue
        # Coordinate chunk: exactly 3 ordinates (M truncated, missing Z -> NaN).
        ordinates = nums[:3] + ["NaN"] * max(0, 3 - len(nums))
        out.append(" " + " ".join(ordinates) + " ")
    return "".join(out)


def is_mixed_dimension(wkt: str) -> bool:
    """True when WKT text mixes dimensionalities, the only text :func:`wkt_pad_z` may repair.

    Mixed means coordinates with different ordinate counts (``LINESTRING Z (15 51 5, 16 52)``)
    or an ``EMPTY`` component beside tagged ones, which has no ordinates to count. Text whose
    coordinates all agree but contradict the tag (``POINT Z (1 2)``) is malformed, not mixed.
    The heavyweight reader applies the same test, so both levels refuse the same text.
    """
    counts = set()
    for chunk in re.split(r"[(),]", wkt):
        n = len(_WKT_NUM_RE.findall(chunk))
        if n >= 2:
            counts.add(n)
    return len(counts) > 1 or (bool(counts) and bool(_WKT_EMPTY_RE.search(wkt)))


def _from_wkt_mixed_dim(text: str) -> BaseGeometry:
    """``from_wkt``, reading mixed-dimensionality text (a Z on one vertex only) as uniform 3D.

    The Z values that exist are kept and the missing ones are ``NaN``; nothing is filled with 0
    and no real Z is dropped. Malformed text that is not mixed (``POINT Z (1 2)``) and text
    that is still unreadable raise the original error.
    """
    try:
        return from_wkt(text)
    except Exception as exc:  # noqa: BLE001 — GEOSException and friends
        if not is_mixed_dimension(text):
            raise
        padded = wkt_pad_z(text)
        if padded == text:
            raise
        try:
            return from_wkt(padded)
        except Exception:  # noqa: BLE001
            raise exc from None


def parse_geom(x: Any) -> Optional[BaseGeometry]:
    """Parse a geometry from WKB/EWKB bytes or WKT/EWKT text. None -> None.

    - ``bytes``/``bytearray`` -> ``from_wkb`` (handles WKB and EWKB).
    - ``str`` -> strip an optional ``SRID=...;`` / ``srid=...;`` EWKT prefix and
      apply ``set_srid``, then ``from_wkt``. Mixed-dimensionality text (``LINESTRING Z
      (15 51 5, 16 52)``) is read as 3D with the missing Z as ``NaN``, the same as the
      heavyweight tier's reader.
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
        geom = _from_wkt_mixed_dim(wkt_part)
        try:
            return set_srid(geom, int(srid_part[5:]))
        except ValueError:
            return geom
    return _from_wkt_mixed_dim(s)


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
