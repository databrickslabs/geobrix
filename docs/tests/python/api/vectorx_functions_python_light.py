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
(EWKB binary — coordinates preserved, SRID=4326 embedded)
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
(EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)
"""
