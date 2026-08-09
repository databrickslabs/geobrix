"""VectorX CRS functions for the pyvx light tier (Serverless-safe).

Implements ``st_crs``, ``st_setcrs``, and ``st_transformcrs`` for vector
geometries — the analogue of the raster ``rst_crs`` / ``rst_setcrs`` /
``rst_transformcrs`` family in pyrx.

## Design — two layers

### Python core layer (medium-preserving)
``st_crs``, ``st_setcrs``, and ``st_transformcrs`` preserve the encoding medium
of their input:

- bytes / bytearray input (WKB, EWKB) -> bytes output (WKB, EWKB)
- str input (WKT, EWKT) -> str output (WKT, EWKT)

This lets Python callers and encoding-matrix tests operate with minimal
round-trips and without implicit WKB coercion.

### SQL/UDF layer (WKB-normalised)
The registered SQL UDFs ``gbx_st_crs`` (STRING) and ``gbx_st_setcrs`` /
``gbx_st_transformcrs`` (BINARY) follow a single fixed Spark return type,
which is the SQL-surface contract:

- ``gbx_st_crs`` returns STRING (canonical authority string, or NULL).
- ``gbx_st_setcrs`` / ``gbx_st_transformcrs`` return BINARY (WKB/EWKB bytes
  regardless of whether the input was text). SQL callers always work in WKB.

One registered function declares ONE return type: an input-dependent type cannot be
used in a view or any fixed schema, and WKB is how the rest of the ``gbx_st_*`` surface
and the built-in ``st_*`` functions exchange geometries. The heavyweight tier declares
exactly the same contract, so a query is a one-line tier swap.

The private helpers ``_udf_st_setcrs``, ``_udf_st_transformcrs`` are the
scalar-UDF callables registered with Spark; they call the medium-preserving
core and coerce the result to the fixed SQL return type.

## CRS handling rules (mirrors raster tier, geometry-specific semantics)

- A geometry stores ONLY an integer SRID (in EWKB / EWKT).
- ``st_crs`` reads that integer and classifies it via ``resolve_crs`` /
  ``crs_to_canonical`` (authoritative PROJ rule: ESRI-range codes come back as
  ``ESRI:<n>``, not ``EPSG:<n>``).
- ``st_setcrs`` stamps an integer SRID; a CRS with no integer authority code
  cannot be stored in an int, so it raises ``ValueError``.  That covers both
  authority-less definitions (raw WKT / PROJ4) and resolvable CRSes whose code is
  non-numeric (``OGC:CRS84``, ``IGNF:LAMB93``).
- ``st_transformcrs`` reprojects coordinates.  Source-CRS errors split by type:
  a geometry with an unresolvable embedded SRID, no embedded SRID and no explicit
  ``source_crs``, or an unparseable geometry all degrade to NULL (DATA error); an
  explicitly-provided but unresolvable ``source_crs`` raises ``ValueError``
  (PARAMETER error).  Geometries whose coordinates lie outside the target CRS's
  ``area_of_use`` bounding box also return NULL; when the target has no
  ``area_of_use`` the check is skipped.  A target with no integer authority code
  is allowed; it yields a plain geometry with the (now stale) SRID cleared.

Both rules go through :func:`databricks.labs.gbx.core.crs.authority_srid_of`, which
probes the authority at ``confidence_threshold=100`` so a PROJ4 or raw-WKT target is
treated as authority-less rather than fuzzy-matched onto a nearby EPSG code — the same
answer GDAL gives on the heavyweight tier.

## M handling — dropped at the parse boundary

The heavyweight tier is XYZ-only (JTS), so its readers discard any M ordinate before CRS
code sees the geometry: ``POINT ZM (11 42 5 99)`` is read as ``POINT Z (11 42 5)`` and both
heavy functions return the 3D form.  This tier matches that by dropping M in
:func:`_drop_m`, called from the single parse entry point :func:`_parse_geom_safe` — so no
function in this module can be reached with an M-carrying geometry, and both tiers agree in
every input encoding.

The measure is TRUNCATED, never traded for something else: an ``M``-only geometry
(``POINT M (11 42 99)``) becomes plain 2D rather than gaining an invented ``Z=0``.

## Z handling (current behavior)

A geometry where **every** vertex has a finite Z keeps its Z through both ``st_setcrs``
and ``st_transformcrs``; a genuinely 2D geometry stays 2D.  For a **partial-Z** geometry
(some vertices carry a Z, some do not):

- ``st_transformcrs`` reprojects it as **2D**.  Reprojecting a non-finite Z propagates it
  into X and Y, so the vertex would come back with no position at all — dropping the Z
  keeps every X/Y correct.
- ``st_setcrs`` writes the Z back verbatim, because it only stamps an SRID and never
  touches coordinates.  A *mixed-dimensionality WKT body* (which no WKT parser accepts)
  is normalized to uniform 3D first, carrying ``NaN`` for the absent Z — the same
  representation the heavyweight tier's JTS reader produces, so both tiers return
  identical results here.

Nothing raises (mixed-dimensionality input must not break a column) and nothing is
fabricated — a missing Z is never filled with ``0``, which would be indistinguishable
from measured elevation downstream.  This is the CURRENT rule, matching the heavyweight
tier.

Serverless-safe: pure Python plus shapely/pyproj, driven entirely through UDFs.
Session configuration, the JVM bridge, and the resilient-distributed-dataset API
are all untouched.
"""

import re
from typing import Optional, Union

from shapely import (
    force_2d,
    from_wkb,
    get_coordinates,
    get_srid,
    has_m,
    set_srid,
    to_wkb,
    to_wkt,
)
from shapely.ops import transform

from databricks.labs.gbx.core.crs import (
    authority_srid_of,
    crs_to_canonical,
    get_transformer,
    resolve_crs,
)

from ._geom import parse_geom

# ---------------------------------------------------------------------------
# Encoding classifier
# ---------------------------------------------------------------------------


def _is_text(x) -> bool:
    """Return True if ``x`` is a str (WKT / EWKT), False for bytes/bytearray."""
    return isinstance(x, str)


# ---------------------------------------------------------------------------
# Z normalization
# ---------------------------------------------------------------------------

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


def _wkt_pad_z(wkt: str) -> str:
    """Rewrite a WKT/EWKT body to uniform 3D: every coordinate gets exactly 3 ordinates.

    ONLY meaningful for a body GEOS has already refused to parse. A WKT whose parts
    disagree about dimensionality (``GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT
    (12 43))``) is rejected outright — ``Cannot mix dimensionality in a geometry`` — so
    there is no geometry object to normalize and the text must be made uniform first.

    PRECONDITION: call this only on WKT that failed to parse (see
    :func:`_parse_geom_safe`, its sole caller). Applied to clean 2D WKT it would pad
    ``POINT (11 42)`` to ``POINT (11 42 NaN)``, turning a 2D geometry 3D — so it must
    never become an unconditional pre-parse step. :func:`_parse_geom_safe` enforces this
    by trying a plain parse first and only falling back here.

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


def _drop_m(g):
    """Return ``g`` with any M (measure) ordinate removed, keeping X, Y, Z and the SRID.

    THE M gate for the whole light CRS family, applied at the single parse entry point
    (:func:`_parse_geom_safe`) so no function can be reached with an M-carrying geometry.
    Placing it at the parse boundary is the exact analogue of the heavyweight tier: JTS is
    XYZ-only, so heavy's WKB/WKT *readers* discard M before any CRS code sees the geometry —
    ``POINT ZM (11 42 5 99)`` is read as ``POINT Z (11 42 5)``, and both ``st_setcrs`` and
    ``st_transformcrs`` return the 33-byte 3D EWKB of that.

    Without this gate the light tier diverged from heavy in two ways at once, and both were
    on the registered SQL surface:

    1. ``st_setcrs`` KEPT the measure (``SRID=4326;POINT ZM (11 42 5 99)``), so a column
       round-tripped through the two tiers came back with different geometry types.
    2. ``st_transformcrs`` RAISED ``ValueError: The ordinate (last) dimension should be 2 or
       3, got 4`` on every uniform-ZM shape — ``shapely.ops.transform`` hands the full
       ordinate array to the pyproj transformer, which takes 2 or 3 ordinates only. That
       broke the never-error invariant: one ZM row failed the whole stage.

    Truncation, never fabrication: the measure is DISCARDED, and a geometry that carries M
    without Z (``POINT M (11 42 99)``) becomes plain 2D rather than gaining an invented
    ``Z=0``. ``shapely.force_3d`` would do exactly that, which is why the M drop goes
    through a WKB round-trip at the geometry's own dimensionality instead.

    A geometry with no M is returned untouched (identity, no round-trip), so the clean-3D,
    genuine-2D, and partial-Z paths are bit-for-bit unaffected.
    """
    if not has_m(g):
        return g
    srid = int(get_srid(g))
    # output_dimension=3 keeps Z when there is one; 2 for an M-only geometry, which must
    # NOT gain a fabricated Z. include_srid is not usable together with the round-trip
    # (extended-flavor SRID + M is not a shape shapely re-reads), so the SRID is re-stamped.
    g_xyz = from_wkb(to_wkb(g, output_dimension=3 if g.has_z else 2))
    return set_srid(g_xyz, srid) if srid > 0 else g_xyz


def _has_partial_z(g) -> bool:
    """True when the geometry claims Z but at least one vertex has a non-finite Z.

    ``get_coordinates(include_z=True)`` returns NaN in the Z column for any vertex that
    carries no Z, so an all-or-nothing check is one array scan.  An empty geometry has no
    coordinates and is never "partial".
    """
    if not g.has_z:
        return False
    coords = get_coordinates(g, include_z=True)
    if coords.shape[0] == 0:
        return False
    z = coords[:, 2]
    return bool((z != z).any() or (abs(z) == float("inf")).any())


def _as_2d(g):
    """Return the 2D form of ``g``, preserving its SRID (``force_2d`` clears it)."""
    srid = int(get_srid(g))
    g2d = force_2d(g)
    return set_srid(g2d, srid) if srid > 0 else g2d


def _drop_partial_z(g):
    """Return ``g`` unchanged when its Z is uniformly finite (or absent), else its 2D form.

    Applied before reprojection.  A partial-Z geometry cannot be reprojected in 3D: PROJ
    propagates the non-finite Z into X and Y, so the vertex comes back ``NaN NaN NaN`` —
    silent loss of the horizontal position, which is far worse than losing the Z.  Dropping
    to 2D keeps every X/Y correct.  The heavyweight tier reaches the same 2D answer from the
    other direction (OGR refuses non-finite ordinates outright).

    Z is never fabricated: a vertex with no elevation stays without one rather than getting
    an invented value (e.g. 0) that would be indistinguishable from measured elevation
    downstream.  Never raises.

    NOTE: this is the CURRENT rule for partial-Z input on both tiers.
    """
    return _as_2d(g) if _has_partial_z(g) else g


def _has_nonfinite_xy(g) -> bool:
    """True when any X or Y coordinate in ``g`` is non-finite (Infinity or NaN).

    Called AFTER a reprojection to detect an out-of-domain result (e.g. PROJ returning
    ``Infinity`` for a mismatched CRS or an invalid latitude).  Only X/Y is inspected —
    Z is excluded deliberately:

    - ``_drop_partial_z`` strips any NaN Z *before* the transform, so a non-finite value
      left in X or Y after the transform is unambiguously from the projection, not from a
      legitimately absent Z ordinate.
    - Clean-3D geometries (every Z finite) carry those Z values through unchanged; their
      Z cannot become non-finite via a 2D CRS change.

    Scope boundary: this guard catches only non-finite (Infinity / NaN) X/Y.  A reprojection
    that yields finite-but-meaningless coordinates — e.g. ``SRID=4326;POINT (200 42)``
    reprojects to a real UTM easting — is NOT caught here and must not be.  Broader domain
    validation is a separate queued workstream.

    Empty geometries carry no coordinates and are never "non-finite".
    """
    coords = get_coordinates(g)  # include_z=False (default) -> shape (n, 2), X/Y only
    if coords.shape[0] == 0:
        return False
    xy = coords.ravel()
    return bool((xy != xy).any() or (abs(xy) == float("inf")).any())


def _parse_geom_safe(geom):
    """Parse a geometry input, retrying mixed-dimensionality WKT as uniform 3D.

    THE single parse entry point for every CRS function AND for the registered SQL UDFs.
    Routing every parse through here is what keeps the never-error invariant true on the
    surface users actually call: a bare ``parse_geom`` raises ``GEOSException`` on
    mixed-dimensionality WKT, and one such row is enough to kill a whole stage.

    Binary input always parses (EWKB declares one dimensionality for the whole geometry),
    so the retry only ever fires for text.  Returns ``None`` when the input cannot be
    parsed at all — callers treat that as a pass-through, never as an error.

    M is dropped here (:func:`_drop_m`) rather than in each function, so the invariant
    "no geometry inside this module ever carries an M" holds by construction — the same
    place heavy drops it (its JTS readers). ``_wkt_pad_z`` already truncates M on the
    mixed-dimensionality retry path; this covers the far more common case of WKT/WKB that
    parses on the first attempt, which is every UNIFORM ZM geometry.
    """
    try:
        g = parse_geom(geom)
    except Exception:  # noqa: BLE001 — mixed-dimensionality WKT and plain garbage alike
        if not isinstance(geom, str):
            return None
        try:
            g = parse_geom(_wkt_pad_z(geom))
        except Exception:  # noqa: BLE001
            return None
    return _drop_m(g) if g is not None else None


def _to_wkb_preserving_z(g) -> bytes:
    """Encode to (E)WKB, keeping 3D when the geometry carries Z and SRID when set.

    ``shapely.to_wkb`` defaults to ``output_dimension=3``, but spelling it out here makes
    the Z-preservation contract explicit at the SQL boundary, which is where a silent
    downcast would be hardest to notice.
    """
    srid = int(get_srid(g))
    return to_wkb(g, include_srid=srid > 0, output_dimension=3 if g.has_z else 2)


# ---------------------------------------------------------------------------
# Core medium-preserving functions
# ---------------------------------------------------------------------------


def st_crs(geom) -> Optional[str]:
    """Return the canonical CRS string for a geometry's embedded SRID, or None.

    Reads the integer SRID from EWKB / EWKT; classifies it via the authoritative
    PROJ code sets (EPSG or ESRI). Returns ``None`` for plain WKB / plain WKT
    (no embedded SRID), None inputs, and any unresolvable SRID.

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string.

    Returns:
        Canonical CRS string (``'EPSG:4326'``, ``'ESRI:54008'``, …) or ``None``.
    """
    if geom is None:
        return None
    g = _parse_geom_safe(geom)
    if g is None:
        return None
    s = int(get_srid(g))
    if s <= 0:
        return None
    try:
        return crs_to_canonical(resolve_crs(s))
    except Exception:
        return None


def st_setcrs(geom, crs: Union[int, str]) -> Union[bytes, str]:
    """Stamp a new CRS on a geometry without reprojecting (medium-preserving).

    Assigns the EPSG/ESRI SRID to the geometry.  A CRS with no integer authority
    code raises ``ValueError`` because a geometry can only store an integer SRID.
    That covers authority-less definitions (raw WKT / PROJ4) and resolvable CRSes
    whose authority code is non-numeric (``OGC:CRS84``, ``IGNF:LAMB93``).

    Encoding is preserved:
    - bytes in (WKB / EWKB) -> EWKB bytes out.
    - str in (WKT / EWKT) -> EWKT str out (``SRID=<n>;<WKT>``).

    Coordinates are preserved exactly (``rounding_precision=-1``).  Z is preserved
    when every vertex carries a finite one; a partial-Z geometry is stamped as 2D.

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string. None returns None.
        crs: EPSG/ESRI authority string (``'EPSG:4326'``), integer SRID, or
            any string resolvable by ``resolve_crs``. WKT / PROJ4 are rejected.

    Returns:
        Geometry with stamped SRID in the same encoding as the input.

    Raises:
        ValueError: if ``crs`` resolves to a CRS with no integer authority code
            (authority-less, or a non-numeric code — neither can be stored as an
            integer SRID).
    """
    if geom is None:
        return None
    text = _is_text(geom)
    g = _parse_geom_safe(geom)
    if g is None:
        return None
    # NOTE: no _drop_partial_z here, deliberately. st_setcrs only stamps an integer
    # SRID — it never touches coordinates, so a non-finite Z cannot corrupt X/Y the
    # way it does through a reprojection. Whatever Z the input carried is written back
    # verbatim, which is also what the heavyweight tier's ST_SetCrs does.
    c = resolve_crs(crs)
    code = authority_srid_of(c)
    if code is None:
        raise ValueError(
            "st_setcrs: cannot stamp an authority-less CRS (WKT / PROJ4) onto a "
            "geometry — a geometry SRID must be an EPSG or ESRI integer code. "
            f"Resolved CRS: {c.to_wkt()[:120]!r}"
        )
    g = set_srid(g, code)
    if text:
        return f"SRID={code};{to_wkt(g, rounding_precision=-1)}"
    return to_wkb(g, include_srid=True)


def st_transformcrs(
    geom,
    target_crs: Union[int, str],
    source_crs: Optional[Union[int, str]] = None,
) -> Union[bytes, str, None]:
    """Reproject a geometry to ``target_crs`` (medium-preserving).

    Source CRS resolution — data errors degrade to NULL; parameter errors raise:

    1. Unparseable geometry input (DATA) -> NULL.
    2. Embedded SRID present but unresolvable (SRID rides in the geometry = DATA)
       -> NULL.
    3. Embedded SRID absent, explicit ``source_crs`` provided but unresolvable
       (PARAMETER) -> raises ``ValueError`` (a bad argument, not bad data).
    4. No embedded SRID and no ``source_crs`` (plain geometry with no CRS DATA)
       -> NULL (cannot reproject without a source CRS).
    5. Bad / unresolvable ``target_crs`` (PARAMETER) -> raises ``ValueError``
       (via ``resolve_crs``).

    After reprojection two additional NULL guards apply:
    - Non-finite X/Y result (PROJ returns Infinity for mislabelled CRS or invalid
      lat) -> NULL.
    - Input coordinates outside the target CRS's ``area_of_use`` bounding box
      (the finite-nonsense survivor: a point on the wrong side of the globe
      reprojects to a finite but meaningless UTM easting) -> NULL.  When the
      target CRS has no ``area_of_use`` metadata the check is skipped (never NULL
      what cannot be disproved).

    Output encoding follows the input medium (bytes -> bytes, str -> str).
    Output SRID:
    - Target with an integer authority code (``EPSG:n`` / ``ESRI:n``) -> SRID ``n``
      stamped (E-form in the input medium).
    - Target with no integer authority code (raw WKT / PROJ4, or a non-numeric code
      such as ``OGC:CRS84``) -> the now-stale SRID is cleared (plain form).

    Coordinates are preserved exactly (``rounding_precision=-1``).  Z is carried
    through the reprojection when every vertex has a finite one; a partial-Z
    geometry is reprojected as 2D (see the module docstring).

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string.
        target_crs: target CRS as EPSG/ESRI string, int SRID, WKT, or PROJ4.
        source_crs: explicit source CRS override for plain (SRID-less) inputs.
            Providing an invalid string raises ``ValueError`` (parameter error).

    Returns:
        Reprojected geometry in the same encoding as the input, or ``None`` when
        the source CRS is unresolvable from the geometry data, or when the input
        coordinates lie outside the target CRS's valid area.

    Raises:
        ValueError: if ``target_crs`` is unresolvable (always), or if an explicit
            ``source_crs`` is provided but unresolvable (parameter error).
    """
    if geom is None:
        return None
    text = _is_text(geom)
    g = _parse_geom_safe(geom)
    if g is None:
        # Unparseable geometry DATA -> NULL (not a parameter error).
        return None
    # Partial-Z input is reprojected in 2D: PROJ would otherwise propagate the
    # non-finite Z into X and Y and lose the horizontal position entirely.
    g = _drop_partial_z(g)

    # Resolve source CRS — data errors degrade to NULL; parameter errors raise.
    embedded_srid = int(get_srid(g))
    src = None
    if embedded_srid > 0:
        try:
            src = resolve_crs(embedded_srid)
        except Exception:
            # Unresolvable embedded SRID — the SRID rides in the geometry = DATA.
            return None
    elif source_crs is not None:
        # An explicit source_crs is a PARAMETER — let resolve_crs raise ValueError.
        src = resolve_crs(source_crs)

    if src is None:
        # No source CRS at all: plain geometry with no embedded SRID and no
        # explicit override.  Cannot reproject — DATA has no CRS -> NULL.
        return None

    # target_crs is a PARAMETER — resolve_crs raises ValueError if invalid.
    tgt = resolve_crs(target_crs)
    tr = get_transformer(src, tgt)
    g_proj = transform(tr.transform, g)

    # Non-finite X/Y guard — return NULL rather than propagating Infinity/NaN coordinates.
    #
    # PROJ returns Infinity (not an error) for out-of-domain inputs: e.g. mislabelled CRS
    # (UTM coordinates claimed as EPSG:4326 then reprojected into UTM again), or an invalid
    # latitude (lat=100 is outside any geographic CRS domain).  Infinity asserts a *location*
    # — it poisons extents, spatial indexes, and aggregations downstream and is
    # indistinguishable from real data.  NULL is what the never-error invariant already
    # implies here: the target CRS resolved fine; it is the input row that cannot be
    # projected.  The heavy tier reaches the same NULL by a different route: OGR/PROJ throws
    # ("PROJ: utm: Invalid latitude") mid-transform and TransformCrsCore catches that NonFatal
    # error on an already-valid CRS pair, so both tiers agree (see test_crs_parity.py).
    #
    # This guard is scoped to NON-FINITE X/Y only.  A reprojection that yields finite-but-
    # meaningless coordinates (e.g. lon=200 reprojects to a real UTM easting) is not caught
    # and must not be — broader domain validation is a separate queued workstream.
    #
    # NaN Z is excluded from the check: ``_drop_partial_z`` above removes any non-finite Z
    # before the transform, so any non-finite value now in X or Y is unambiguously from the
    # projection math, not from a legitimately absent Z ordinate.
    if _has_nonfinite_xy(g_proj):
        return None

    # Area-of-use domain check — catch the finite-nonsense survivor.
    #
    # A geometry can reproject to finite-but-wrong coordinates when its coordinates are
    # plausible for the source CRS's projection math but outside the target CRS's intended
    # geographic extent (e.g. SRID=4326;POINT(150 -80) -> EPSG:27700 yields a real-looking
    # BNG easting/northing ~16,500 km south of GB).  The non-finite guard above misses this
    # because PROJ returns a finite number; only the target CRS's area_of_use bounds catch it.
    #
    # The domain check uses the INPUT coordinates in lon/lat (EPSG:4326).  When the source
    # CRS is geographic (e.g. EPSG:4326, OGC:CRS84) the geometry's own coordinates are
    # already lon/lat.  When the source is projected, a secondary transform to EPSG:4326 is
    # required to put the input in the same frame as the bounds.
    #
    # If the target CRS has no area_of_use metadata (e.g. raw PROJ4, custom WKT without
    # USAGE blocks) the check returns None and is skipped — never NULL what cannot be disproved.
    #
    # ``tgt`` is a rasterio CRS (from ``resolve_crs``), which does not carry area_of_use.
    # Convert via the authority tuple when available (preserves the registry bounds); fall
    # back to from_wkt otherwise (area_of_use will be None -> skip).
    try:
        import pyproj as _pyproj

        _auth = tgt.to_authority(confidence_threshold=100)
        _tgt_pyproj = (
            _pyproj.CRS.from_authority(*_auth)
            if _auth
            else _pyproj.CRS.from_wkt(tgt.to_wkt())
        )
    except Exception:
        _tgt_pyproj = tgt  # fallback: area_of_use absent -> skip

    from databricks.labs.gbx.core.crs import in_target_domain

    lonlat = (
        get_coordinates(g)
        if src.is_geographic
        else get_coordinates(transform(get_transformer(src, resolve_crs(4326)).transform, g))
    )
    dom = in_target_domain(lonlat, _tgt_pyproj)
    if dom is False:
        return None
    # dom is True (in-domain) or None (no area_of_use -> skip) -> proceed

    # Determine output SRID from the target's integer authority code (None for a
    # raw WKT / PROJ4 target, or for a non-numeric code such as OGC:CRS84).
    tgt_srid = authority_srid_of(tgt)
    if tgt_srid is not None:
        g_proj = set_srid(g_proj, tgt_srid)
        if text:
            return f"SRID={tgt_srid};{to_wkt(g_proj, rounding_precision=-1)}"
        return to_wkb(g_proj, include_srid=True)
    else:
        # No integer authority code to carry: clear the now-stale SRID.
        g_proj = set_srid(g_proj, 0)
        if text:
            return to_wkt(g_proj, rounding_precision=-1)
        return to_wkb(g_proj)


# ---------------------------------------------------------------------------
# SQL/UDF layer — WKB-normalised callables for Spark registration
# ---------------------------------------------------------------------------


def _text_result_to_wkb(result: str) -> Optional[bytes]:
    """Re-encode a text-medium core result as (E)WKB for the BINARY SQL surface.

    Uses :func:`_parse_geom_safe`, NOT a bare ``parse_geom``.  Mixed-dimensionality WKT
    (e.g. ``GEOMETRYCOLLECTION Z (POINT Z (1 2 3), POINT (4 5))``) fails GEOS's plain
    parser — a bare re-parse here would raise ``GEOSException`` and fail the stage.
    Using :func:`_parse_geom_safe` handles the normalisation transparently.
    Returns ``None`` only when the text is unparseable even after normalisation.
    """
    g = _parse_geom_safe(result)
    if g is None:
        return None
    return _to_wkb_preserving_z(g)


def _udf_st_setcrs(geom, crs) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_setcrs (BINARY return type).

    Coerces the result to EWKB bytes regardless of input medium — SQL callers
    always receive WKB.  A CRS with no integer authority code raises (per core
    contract).  None geom or crs returns None.
    """
    if geom is None or crs is None:
        return None
    result = st_setcrs(geom, crs)
    if result is None:
        return None
    if isinstance(result, str):
        # Text input was processed medium-preserving -> convert to EWKB for SQL.
        return _text_result_to_wkb(result)
    return bytes(result)


def _udf_st_transformcrs(geom, target_crs, source_crs=None) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_transformcrs (BINARY return type).

    Coerces the result to WKB/EWKB bytes regardless of input medium — SQL callers
    always receive WKB.  Returns None if geom or target_crs is None, and whenever
    the core returns None (data error, unresolvable source, or domain-out-of-area).
    Raises ValueError when an explicitly-provided source_crs or target_crs is invalid
    (parameter error).
    """
    if geom is None or target_crs is None:
        return None
    result = st_transformcrs(geom, target_crs, source_crs)
    if result is None:
        return None
    if isinstance(result, str):
        # Text-input core result -> convert to (E)WKB for the SQL surface.
        return _text_result_to_wkb(result)
    return bytes(result)
