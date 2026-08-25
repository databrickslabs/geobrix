"""
Python code examples for the light (pyvx) tier of VectorX functions.

Single source of truth for the Python (light) tab in
docs/docs/api/vectorx-functions.mdx. Consumed by the FunctionExamples
component (``pythonLight`` prop) and by the generate-function-info.py
``_TIER_SCANS`` binding detector.

All examples use the shared canonical fixtures from ``_fixtures.py``
so every function's four tabs show the SAME example — the same fixture,
operation, and argument values expressed in each tier's language.

Fixture view assignments (created by create_setup_views_vectorx_light)
-----------------------------------------------------------------------
``tin_survey``   — 1 row: pts ARRAY<BINARY> (4 WKB POINT Z), bl ARRAY<BINARY> (empty)
                   Backs: st_triangulate, st_interpolateelevationbbox,
                          st_interpolateelevationgeom
``mvt_features`` — 2 rows: z INT, x INT, y INT, geom_wkb BINARY, attrs STRUCT
                   Backs: st_asmvt, st_asmvt_pyramid
``vector_geoms`` — 1 row: geom STRING (EWKT 'SRID=4326;POINT (13 42)')
                   Backs: st_crs, st_setcrs, st_transformcrs
``legacy_geoms`` — 1 row: geom_legacy STRUCT (Mosaic InternalGeometry)
                   Backs: st_legacyaswkb

Per-function examples (T2–T5 batches)
--------------------------------------
Family 1 (vector-tile):   st_asmvt, st_asmvt_pyramid        — T2
Family 2 (TIN):           st_triangulate, st_interpolateelevationbbox,
                          st_interpolateelevationgeom         — T3
Family 3 (CRS):           st_crs, st_setcrs, st_transformcrs — T4
Family 4 (legacy):        st_legacyaswkb                     — T5

Light-only UDTF invocation form
---------------------------------
``st_asmvt_pyramid``, ``st_triangulate``, ``st_interpolateelevationbbox``,
and ``st_interpolateelevationgeom`` are Python UDTFs in the lightweight tier.
They have **no Python DataFrame Column form** — the Python (light) tab for
these functions uses::

    spark.sql("SELECT t.* FROM <view>, LATERAL gbx_<fn>(...) t")

This is identical in behavior to the SQL tab; the Column API form is
heavyweight-only for these generators.
"""

try:
    from databricks.labs.gbx.pyvx import functions as vx
except ImportError:
    vx = None


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py
# ---------------------------------------------------------------------------


def _get_tin_df(spark):
    from ._fixtures import tin_df  # noqa: PLC0415

    return tin_df(spark)


def _get_mvt_features_df(spark):
    from ._fixtures import mvt_features_df  # noqa: PLC0415

    return mvt_features_df(spark)


def _get_geom_ewkt_df(spark):
    from ._fixtures import geom_ewkt_df  # noqa: PLC0415

    return geom_ewkt_df(spark)


def _get_legacy_geom_df(spark):
    from ._fixtures import legacy_geom_df  # noqa: PLC0415

    return legacy_geom_df(spark)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def vectorx_light_setup_example(spark):
    """Register VectorX (lightweight pyvx tier). Run once before examples.

    Assumes GeoBrix is already installed (see the Installation guide).
    After this call, all ``gbx_st_*`` SQL functions and the ``pyvx``
    Python Column API are available in the current Spark session.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    vx.register(spark)
    return None


vectorx_light_setup_example_output = """
VectorX (light) registered. You can now use gbx_st_* SQL functions via the pyvx tier.
"""

# ---------------------------------------------------------------------------
# Per-function examples are added by T2–T5 (family-by-family batches).
# Each function follows this naming convention:
#
#   def <base>_python_light_example(spark):
#       ...
#   <base>_python_light_example_output = """..."""
#
# where <base> is the function's base name (e.g. st_asmvt, st_triangulate).
# The generate-function-info.py _TIER_SCANS scanner detects these by their
# def signature; the FunctionExamples MDX component renders the source text.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Vector-tile family — st_asmvt, st_asmvt_pyramid
# ---------------------------------------------------------------------------
#
# st_asmvt
# Fixture: ``mvt_features`` view — 2 rows: tile-local WKB POINTs in (z=0,x=0,y=0)
# POINT(100,100) and POINT(200,200) in pixel space; attrs STRUCT<name STRING, id LONG>.
# The aggregator consumes tile-local coordinates and encodes them as MVT protobufs.
#
# st_asmvt_pyramid
# Input must be WGS-84 lon/lat — the UDTF clips and transforms per tile internally.
# POINT(0, 0) (equator × prime meridian) at zoom 0–2 intersects 3 tiles:
# (z=0,x=0,y=0), (z=1,x=1,y=1), (z=2,x=2,y=2).
# NOTE: The ``mvt_features`` fixture carries tile-local pixel coordinates which
# are outside valid WGS-84 latitude range; the pyramid UDTF is therefore shown
# with inline WGS-84 data rather than the ``mvt_features`` view.
# ---------------------------------------------------------------------------

_WKB_POINT_0_0 = "010100000000000000000000000000000000000000"


def st_asmvt_python_light_example(spark):
    """Aggregate tile-local features into an MVT protobuf blob per tile (light pyvx tier).

    Reads the ``mvt_features`` setup view (2 tile-local WKB POINTs in z=0/x=0/y=0)
    and groups by ``(z, x, y)`` before aggregating.  The aggregator encodes all
    features in each group into one MVT blob.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("mvt_features")
    result = df.groupBy("z", "x", "y").agg(
        vx.st_asmvt("geom_wkb", f.col("attrs"), "layer").alias("mvt")
    )
    row = result.first()
    return row["mvt"]


st_asmvt_python_light_example_output = """
+---+---+---+---------+
|  z|  x|  y|      mvt|
+---+---+---+---------+
|  0|  0|  0|[binary] |
+---+---+---+---------+
... (MVT binary)
"""


def st_asmvt_pyramid_python_light_example(spark):
    """Explode a WGS-84 feature into per-tile MVT rows via SQL LATERAL (light pyvx tier).

    The lightweight pyramid is a Python UDTF with no Python DataFrame Column form —
    invoke it via SQL ``LATERAL`` only.  POINT(0, 0) WGS-84 at zoom 0–2 intersects
    3 tiles and emits one ``(z, x, y, mvt_bytes)`` row per tile.  Returns the first
    tile's ``mvt_bytes`` so the test can verify the MVT encoding path is exercised.
    """
    result = spark.sql(f"""
        WITH feats AS (
            SELECT unhex('{_WKB_POINT_0_0}') AS geom_wkb,
                   named_struct('name', 'origin', 'id', 1L) AS attrs
        )
        SELECT t.*
        FROM feats, LATERAL gbx_st_asmvt_pyramid(geom_wkb, attrs, 0, 2, 'layer', 4096) t
        """)
    rows = result.collect()
    return rows[0]["mvt_bytes"] if rows else None


st_asmvt_pyramid_python_light_example_output = """
+---+---+---+-----------+
|  z|  x|  y|  mvt_bytes|
+---+---+---+-----------+
|  0|  0|  0|  [binary] |
|  1|  1|  1|  [binary] |
|  2|  2|  2|  [binary] |
+---+---+---+-----------+
... (MVT binary — one row per intersecting tile across zoom levels 0–2)
"""


# ---------------------------------------------------------------------------
# TIN family — st_triangulate, st_interpolateelevationbbox,
#              st_interpolateelevationgeom
# Fixture: ``tin_survey`` view — 1 row: pts ARRAY<BINARY> (4 WKB POINT Z),
#          bl ARRAY<BINARY> (empty).
#
# These three functions are Python UDTFs in the lightweight tier: they have
# NO Python DataFrame Column form.  The Python (light) tab for each invokes
# the registered UDTF via SQL LATERAL (identical in behavior to the SQL tab,
# just wrapped in spark.sql()).
#
# mode='constrained' is the default and is available in BOTH tiers.
# mode='conforming' is HEAVYWEIGHT-ONLY (NotImplementedError in pyvx).
# ---------------------------------------------------------------------------

# WKB POINT(0, 10) — plain 2D, used as grid_origin for st_interpolateelevationgeom.
# Little-endian: 01 01000000 0000000000000000 0000000000002440
_WKB_POINT_0_10 = "010100000000000000000000000000000000002440"


def st_triangulate_python_light_example(spark):
    """Build a Delaunay TIN and emit one triangle polygon per row (light pyvx tier).

    The lightweight tier exposes ``gbx_st_triangulate`` as a registered SQL UDTF
    (no Python DataFrame Column form).  Reads the ``tin_survey`` setup view — 4
    WKB POINT Z corners of a 10×10 m square — and materialises the 2-triangle
    Delaunay mesh via SQL ``LATERAL``.  Returns the first triangle's WKB bytes.
    ``mode='constrained'`` is both-tier; ``mode='conforming'`` is heavyweight-only.
    """
    result = spark.sql("""
        SELECT t.triangle
        FROM tin_survey, LATERAL gbx_st_triangulate(pts, bl, 0, 0, 'NONENCROACHING', 'constrained') t
    """)
    rows = result.collect()
    return rows[0]["triangle"] if rows else None


st_triangulate_python_light_example_output = """
+--------+
|triangle|
+--------+
|[binary]|
|[binary]|
+--------+
... (WKB binary — 2 Delaunay triangle polygons)
"""


def st_interpolateelevationbbox_python_light_example(spark):
    """Sample TIN elevation on a regular bounding-box grid (light pyvx tier).

    The lightweight tier exposes ``gbx_st_interpolateelevationbbox`` as a
    registered SQL UDTF (no Python DataFrame Column form).  Reads the
    ``tin_survey`` setup view and samples the TIN on a 3×3 grid spanning
    the full 10×10 m extent (SRID=0).  All 9 cell centres fall inside the
    TIN convex hull, so the generator emits 9 elevation rows.
    Returns the first row's WKB POINT Z bytes.
    """
    result = spark.sql("""
        SELECT t.elevation_point
        FROM tin_survey, LATERAL gbx_st_interpolateelevationbbox(pts, bl, 0, 0, 'NONENCROACHING', 0, 0, 10, 10, 3, 3, 0, 'constrained') t
    """)
    rows = result.collect()
    return rows[0]["elevation_point"] if rows else None


st_interpolateelevationbbox_python_light_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 elevation grid, 9 rows)
"""


def st_interpolateelevationgeom_python_light_example(spark):
    """Sample TIN elevation on a grid anchored to a geometry origin (light pyvx tier).

    The lightweight tier exposes ``gbx_st_interpolateelevationgeom`` as a
    registered SQL UDTF (no Python DataFrame Column form).  Reads the
    ``tin_survey`` setup view and samples on a 3×3 grid anchored to
    POINT(0, 10) — top-left corner of the TIN extent.  Cell sizes are
    3.0 m × 3.0 m (negative Y steps downward per raster convention).
    All 9 cell centres fall inside the TIN hull; output SRID is 0
    (plain WKB origin carries no SRID).  Returns the first row's WKB bytes.
    """
    result = spark.sql(f"""
        SELECT t.elevation_point
        FROM tin_survey, LATERAL gbx_st_interpolateelevationgeom(pts, bl, 0, 0, 'NONENCROACHING', unhex('{_WKB_POINT_0_10}'), 3, 3, 3, -3, 'constrained') t
    """)
    rows = result.collect()
    return rows[0]["elevation_point"] if rows else None


st_interpolateelevationgeom_python_light_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 origin-anchored grid, 9 rows)
"""


# ---------------------------------------------------------------------------
# CRS family — st_crs, st_setcrs, st_transformcrs
# Fixture: ``vector_geoms`` view — 1 row: geom STRING = 'SRID=4326;POINT (13 42)'
# POINT(13, 42) is in central Italy, inside UTM zone 33N's area of use
# (12°E–18°E), so st_transformcrs to EPSG:32633 is in-domain (non-null result).
# ---------------------------------------------------------------------------


def st_crs_python_light_example(spark):
    """Return the canonical CRS string for a geometry's embedded SRID (light pyvx tier)."""
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(vx.st_crs("geom").alias("crs")).first()
    return result["crs"]


st_crs_python_light_example_output = """
+---------+
|crs      |
+---------+
|EPSG:4326|
+---------+
"""


def st_setcrs_python_light_example(spark):
    """Stamp a CRS on a geometry without reprojecting (light pyvx tier)."""
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(vx.st_setcrs("geom", "EPSG:4326").alias("stamped")).first()
    return result["stamped"]


st_setcrs_python_light_example_output = """
+---------+
|stamped  |
+---------+
|[binary] |
+---------+
... (EWKB binary — coordinates preserved, SRID=4326 embedded)
"""


def st_transformcrs_python_light_example(spark):
    """Reproject a geometry to a target CRS (light pyvx tier)."""
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(vx.st_transformcrs("geom", "EPSG:32633").alias("utm33n")).first()
    return result["utm33n"]


st_transformcrs_python_light_example_output = """
+--------+
|utm33n  |
+--------+
|[binary]|
+--------+
... (EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)
"""


# ---------------------------------------------------------------------------
# Legacy Mosaic conversion family — st_legacyaswkb
# Fixture: ``legacy_geoms`` view — 1 row: geom_legacy STRUCT (Mosaic InternalGeometry)
# Encodes POINT(13, 42): typeId=1, srid=0, boundaries=[[[13.0, 42.0]]], holes=[].
# Both tiers: same scalar Column form; import path differs between pyvx and
# vectorx.jts.legacy. Output: plain WKB (no embedded SRID).
# ---------------------------------------------------------------------------


def st_legacyaswkb_python_light_example(spark):
    """Convert a legacy Mosaic geometry struct to standard WKB (light pyvx tier).

    Uses the ``legacy_geoms`` setup view — one row with a Mosaic InternalGeometry
    struct encoding POINT(13, 42) — and returns the plain WKB bytes for that point.
    ``st_legacyaswkb`` is a scalar function in both tiers (same name, same output
    bytes); the only difference between tiers is the import path.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415

    df = spark.table("legacy_geoms")
    result = df.select(vx.st_legacyaswkb("geom_legacy").alias("wkb")).first()
    return result["wkb"]


st_legacyaswkb_python_light_example_output = """
+--------+
|wkb     |
+--------+
|[binary]|
+--------+
... (WKB binary)
"""


# ---------------------------------------------------------------------------
# Antimeridian family — st_shiftlongitude, st_wrapx, st_split
# Light-only (pyvx tier).  These functions are pure-Python UDFs — they have
# no heavyweight Scala equivalent and no JAR dependency.
# Input: WKB BINARY (created here via shapely.to_wkb for self-contained examples).
# Output: WKB BINARY.
# ---------------------------------------------------------------------------


def st_shiftlongitude_python_light_example(spark):
    """Shift longitude from [-180,180] to [0,360] (light pyvx tier).

    Creates a WKB POINT(-170, 10) inline via shapely and passes it through
    ``st_shiftlongitude``.  Negative x coordinates are moved by +360, so
    x=-170 becomes x=190.  Returns the shifted WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import to_wkb  # noqa: PLC0415
    from shapely.geometry import Point  # noqa: PLC0415

    df = spark.createDataFrame([(to_wkb(Point(-170.0, 10.0)),)], ["geom"])
    return df.select(vx.st_shiftlongitude("geom").alias("shifted")).first()["shifted"]


st_shiftlongitude_python_light_example_output = """
+--------+
|shifted |
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(190.0, 10.0): x shifted from -170 to 190)
"""


def st_wrapx_python_light_example(spark):
    """Wrap X coordinates back into [-180,180] (light pyvx tier).

    Creates a WKB POINT(190, 10) inline — a coordinate in [0,360] longitude
    space — and wraps it back into [-180,180] using ``wrap_x_origin=180`` and
    ``wrap_direction=-360``.  Any x ≥ 180 is shifted by -360, so x=190 becomes
    x=-170.  Returns the wrapped WKB bytes.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import to_wkb  # noqa: PLC0415
    from shapely.geometry import Point  # noqa: PLC0415

    df = spark.createDataFrame([(to_wkb(Point(190.0, 10.0)),)], ["geom"])
    return df.select(
        vx.st_wrapx("geom", f.lit(180.0), f.lit(-360.0)).alias("wrapped")
    ).first()["wrapped"]


st_wrapx_python_light_example_output = """
+--------+
|wrapped |
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(-170.0, 10.0): x=190 wrapped back by -360)
"""


def st_split_python_light_example(spark):
    """Split a polygon by the 180° meridian; returns a GEOMETRYCOLLECTION (light pyvx tier).

    Creates a WKB polygon that straddles the antimeridian (x range 170 to 190)
    and a WKB blade linestring at x=180.  The split returns a two-piece
    GeometryCollection — one polygon on each side of the cut.  Returns the
    GeometryCollection WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import to_wkb  # noqa: PLC0415
    from shapely.geometry import LineString, Polygon  # noqa: PLC0415

    input_poly = Polygon([(170, -10), (190, -10), (190, 10), (170, 10), (170, -10)])
    blade_line = LineString([(180, -90), (180, 90)])
    df = spark.createDataFrame(
        [(to_wkb(input_poly), to_wkb(blade_line))],
        ["input_geom", "blade_geom"],
    )
    return df.select(
        vx.st_split("input_geom", "blade_geom").alias("pieces")
    ).first()["pieces"]


st_split_python_light_example_output = """
+--------+
|pieces  |
+--------+
|[binary]|
+--------+
... (WKB binary — GeometryCollection with 2 polygon pieces split at x=180)
"""


# ---------------------------------------------------------------------------
# Geometry validity family — st_makevalid, st_explainvalidity
# Light-only (pyvx tier). These functions are scalar Python UDFs.
# Input: WKB bytes (created inline via shapely for self-contained examples).
# Output: BINARY (st_makevalid) / STRING/JSON (st_explainvalidity).
# ---------------------------------------------------------------------------


def st_makevalid_python_light_example(spark):
    """Repair an invalid geometry to OGC-SFS validity (light pyvx tier).

    Creates a WKB bowtie self-intersecting polygon inline via shapely and
    repairs it with ``st_makevalid``.  The default ``linework`` level nodes the
    self-intersection into a valid multi-polygon.  Returns the repaired WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415

    bowtie = from_wkt("POLYGON((0 0,1 1,1 0,0 1,0 0))")
    df = spark.createDataFrame([(to_wkb(bowtie),)], ["geom"])
    return df.select(vx.st_makevalid("geom").alias("clean")).first()["clean"]


st_makevalid_python_light_example_output = """
+--------+
|clean   |
+--------+
|[binary]|
+--------+
... (WKB binary — repaired geometry; bowtie becomes a valid multi-polygon)
"""


def st_explainvalidity_python_light_example(spark):
    """Diagnose SFS validity as JSON {valid, reason, code, location} (light pyvx tier).

    Creates a WKB bowtie self-intersecting polygon inline and calls
    ``st_explainvalidity``.  Returns the JSON string with ``valid=false``,
    the GEOS reason string, ``code=10`` (self-intersection), and
    ``POINT(0.5 0.5)`` as the location where the violation occurs.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415

    bowtie = from_wkt("POLYGON((0 0,1 1,1 0,0 1,0 0))")
    df = spark.createDataFrame([(to_wkb(bowtie),)], ["geom"])
    return df.select(vx.st_explainvalidity("geom").alias("detail")).first()["detail"]


st_explainvalidity_python_light_example_output = """
+----------------------------------------------------------------------+
|detail                                                                |
+----------------------------------------------------------------------+
|{"valid": false, "reason": "Self-intersection[0.5 0.5]", "code": 10,|
+----------------------------------------------------------------------+
... (JSON string — {valid, reason, code, location} for SFS validity diagnosis)
"""


# ---------------------------------------------------------------------------
# Geometry cleaning family — st_simplifypreservetopology, st_removerepeatedpoints,
#                            st_reduceprecision, st_node, st_snap
# Light-only (pyvx tier). These functions are pure-Python scalar UDFs.
# Input: WKT strings or WKB bytes. Output: BINARY (WKB).
# ---------------------------------------------------------------------------


def st_simplifypreservetopology_python_light_example(spark):
    """Simplify a polygon while preserving topology (light pyvx tier).

    Creates an inline near-collinear polygon and simplifies it with tolerance=1.0.
    The topology-preserving Douglas-Peucker algorithm drops the near-collinear
    vertex without collapsing or splitting the polygon.  Returns the simplified
    WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    geom = from_wkt("POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))")
    df = spark.createDataFrame([(to_wkb(geom),)], ["geom"])
    return df.select(
        vx.st_simplifypreservetopology("geom", f.lit(1.0)).alias("simplified")
    ).first()["simplified"]


st_simplifypreservetopology_python_light_example_output = """
+--------+
|simplified|
+--------+
|[binary]|
+--------+
... (WKB binary — simplified polygon with near-collinear vertex removed, topology preserved)
"""


def st_removerepeatedpoints_python_light_example(spark):
    """Remove consecutive duplicate vertices from a linestring (light pyvx tier).

    Creates a WKB linestring with repeated consecutive vertices inline and removes
    the exact duplicates with the default tolerance=0.0.  Returns the deduplicated
    WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415

    geom = from_wkt("LINESTRING(0 0,0 0,1 1,1 1,2 2)")
    df = spark.createDataFrame([(to_wkb(geom),)], ["geom"])
    return df.select(
        vx.st_removerepeatedpoints("geom").alias("deduped")
    ).first()["deduped"]


st_removerepeatedpoints_python_light_example_output = """
+--------+
|deduped |
+--------+
|[binary]|
+--------+
... (WKB binary — linestring with duplicate consecutive vertices removed: LINESTRING(0 0,1 1,2 2))
"""


def st_reduceprecision_python_light_example(spark):
    """Snap coordinates to a precision grid (light pyvx tier).

    Creates a WKB POINT(1.234, 5.678) inline and snaps it to a grid of size 1.0.
    The coordinates are rounded to the nearest integer grid lines: x=1.234 -> 1.0,
    y=5.678 -> 6.0.  Returns the snapped WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    geom = from_wkt("POINT(1.234 5.678)")
    df = spark.createDataFrame([(to_wkb(geom),)], ["geom"])
    return df.select(
        vx.st_reduceprecision("geom", f.lit(1.0)).alias("snapped")
    ).first()["snapped"]


st_reduceprecision_python_light_example_output = """
+--------+
|snapped |
+--------+
|[binary]|
+--------+
... (WKB binary — POINT(1.0, 6.0): coordinates snapped to nearest 1.0 grid lines)
"""


def st_node_python_light_example(spark):
    """Node linework: split at all self-intersections (light pyvx tier).

    Creates a self-intersecting figure-eight linestring inline and nodes it.
    The self-intersection at (5, 5) is resolved by splitting the linework, returning
    a MultiLineString with clean non-overlapping segments.  Returns the noded WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415

    geom = from_wkt("LINESTRING(0 0,10 10,0 10,10 0)")
    df = spark.createDataFrame([(to_wkb(geom),)], ["geom"])
    return df.select(vx.st_node("geom").alias("noded")).first()["noded"]


st_node_python_light_example_output = """
+--------+
|noded   |
+--------+
|[binary]|
+--------+
... (WKB binary — MultiLineString: figure-eight split into clean segments at the self-intersection)
"""


def st_snap_python_light_example(spark):
    """Snap geometry vertices onto a reference within a tolerance (light pyvx tier).

    Creates a WKB linestring that misses a reference by 0.4 units and snaps it
    onto the reference with tolerance=0.5.  The near-miss vertices (y=0.4) snap
    onto the reference (y=0), aligning the two geometries.  Returns the snapped
    WKB bytes.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    geom = from_wkt("LINESTRING(0 0.4,10 0.4)")
    ref = from_wkt("LINESTRING(0 0,10 0)")
    df = spark.createDataFrame([(to_wkb(geom), to_wkb(ref))], ["geom", "ref"])
    return df.select(
        vx.st_snap("geom", "ref", f.lit(0.5)).alias("snapped")
    ).first()["snapped"]


st_snap_python_light_example_output = """
+--------+
|snapped |
+--------+
|[binary]|
+--------+
... (WKB binary — linestring with near-miss vertices snapped onto the reference at y=0)
"""


# ---------------------------------------------------------------------------
# Coverage validity family — coverage_simplify
# Light-only (pyvx tier). Python-API helper (no SQL form).
# Topology-preserving simplification of a whole coverage. N rows in → N rows out;
# all input columns preserved; simplified geometry written to out_col (BINARY).
# The shared edge between adjacent polygons is preserved exactly in both outputs.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Coverage validity family (SQL-registered aggs) — st_coverageisvalid,
#                                                   st_coverageinvalidedges
# Light-only (pyvx tier). Both are grouped-aggregate Column wrappers invoked
# via groupBy().agg(), identical API shape to st_asmvt.
# ---------------------------------------------------------------------------


def st_coverageisvalid_python_light_example(spark):
    """Check polygon-coverage validity via grouped-agg Column wrapper (light pyvx tier).

    Builds two adjacent unit squares sharing the edge x=5 in coverage group
    ``cov_id=1``.  Uses shapely ``box`` (alias for ``from_shapely_bounds``) to
    produce WKB bytes for each polygon, creates a two-row DataFrame, and calls
    ``vx.st_coverageisvalid("geom", 0.0)`` inside ``groupBy().agg()``.
    The two squares do not overlap and share a clean edge → ``is_valid = True``.
    ``gap_width=0.0`` detects only overlaps (no gap tolerance).
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import to_wkb  # noqa: PLC0415
    from shapely.geometry import box  # noqa: PLC0415

    left = box(0.0, 0.0, 5.0, 5.0)
    right = box(5.0, 0.0, 10.0, 5.0)
    df = spark.createDataFrame(
        [
            (1, to_wkb(left)),
            (1, to_wkb(right)),
        ],
        ["cov_id", "geom"],
    )
    result = df.groupBy("cov_id").agg(
        vx.st_coverageisvalid("geom", 0.0).alias("is_valid")
    )
    return result.first()["is_valid"]


st_coverageisvalid_python_light_example_output = """
+------+--------+
|cov_id|is_valid|
+------+--------+
|     1|    true|
+------+--------+
... (BOOLEAN — true: the two adjacent squares share a clean edge with no overlap)
"""


def st_coverageinvalidedges_python_light_example(spark):
    """Return invalid edge segments of an overlapping coverage (light pyvx tier).

    Builds two overlapping squares in coverage group ``cov_id=1``: polygon 1
    covers [0,6]×[0,6] and polygon 2 covers [4,10]×[4,10], overlapping in
    [4,6]×[4,6].  ``vx.st_coverageinvalidedges("geom", 0.0)`` aggregates the
    group and returns the BINARY union of the boundary segments that violate
    the coverage.  Returns non-empty BINARY (WKB/EWKB) because of the overlap.
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import to_wkb  # noqa: PLC0415
    from shapely.geometry import box  # noqa: PLC0415

    poly1 = box(0.0, 0.0, 6.0, 6.0)
    poly2 = box(4.0, 4.0, 10.0, 10.0)
    df = spark.createDataFrame(
        [
            (1, to_wkb(poly1)),
            (1, to_wkb(poly2)),
        ],
        ["cov_id", "geom"],
    )
    result = df.groupBy("cov_id").agg(
        vx.st_coverageinvalidedges("geom", 0.0).alias("bad_edges")
    )
    return result.first()["bad_edges"]


st_coverageinvalidedges_python_light_example_output = """
+------+---------+
|cov_id|bad_edges|
+------+---------+
|     1|[binary] |
+------+---------+
... (BINARY — union of the invalid edge segments; non-empty because the two squares overlap)
"""


def coverage_simplify_python_light_example(spark):
    """Topology-preserving coverage simplification (light pyvx tier, Python-API only).

    Builds a two-polygon coverage where each polygon has one near-collinear vertex
    on its outer boundary (not on the shared edge):

    - left:  ``POLYGON((0 0, 2.5 0.001, 5 0, 5 5, 0 5, 0 0))`` — vertex (2.5, 0.001)
             deviates 0.001 units from the bottom edge, within the 0.1 tolerance.
    - right: ``POLYGON((5 0, 7.5 0.001, 10 0, 10 5, 5 5, 5 0))`` — same pattern.

    Both polygons share the edge from (5, 0) to (5, 5).  After
    ``coverage_simplify(tolerance=0.1)`` the near-collinear vertices are dropped
    from each outer boundary while the shared edge is preserved exactly — the two
    simplified polygons still meet at x=5.  Returns the list of simplified WKB bytes
    (2 rows in, 2 rows out).
    """
    from databricks.labs.gbx.pyvx import functions as vx  # noqa: PLC0415
    from shapely import from_wkt, to_wkb  # noqa: PLC0415

    left = from_wkt("POLYGON((0 0, 2.5 0.001, 5 0, 5 5, 0 5, 0 0))")
    right = from_wkt("POLYGON((5 0, 7.5 0.001, 10 0, 10 5, 5 5, 5 0))")
    df = spark.createDataFrame(
        [
            (1, to_wkb(left)),
            (1, to_wkb(right)),
        ],
        ["cov_id", "geom"],
    )
    result = vx.coverage_simplify(df, "cov_id", "geom", 0.1)
    rows = result.collect()
    return [row["geom_simplified"] for row in rows]


coverage_simplify_python_light_example_output = """
+------+--------+----------------+
|cov_id|    geom|geom_simplified |
+------+--------+----------------+
|     1|[binary]|       [binary] |
|     1|[binary]|       [binary] |
+------+--------+----------------+
... (BINARY — 2 rows in, 2 rows out; near-collinear vertices dropped, shared edge preserved)
"""
