"""Light (pyvx) vs heavy (vectorx) CRS-family parity: st_crs / st_setcrs / st_transformcrs.

## Comparison rule: DECODED GEOMETRIES, never raw bytes

The two tiers emit the same EWKB *content* in different byte orders — the heavy tier's
JTS ``WKBWriter`` defaults to big-endian (``0020000001…``) while shapely writes
little-endian (``0101000020…``). Both are valid EWKB, both decode identically, and the
difference is a pre-existing writer default with nothing to do with CRS behavior. A
byte-equality parity test would fail spuriously, so every assertion here compares:

- decoded geometry type and coordinates (within ``COORD_TOL``),
- decoded SRID,
- ``st_crs`` strings (plain string equality — it is an authority string, not a geometry).

## How each tier is invoked

Both tiers register the SAME three SQL names, so they cannot both own
``gbx_st_transformcrs`` in one Spark session. Heavy is therefore exercised through real
SQL against the JAR (the registered surface), and light through the exact Python
callables its registrar binds to those names (``_crs.st_crs``, ``_crs._udf_st_setcrs``,
``_crs._udf_st_transformcrs``) — same code path, no name collision. One dedicated test
registers the light callables under temporary ``gbxlight_*`` names to prove the light SQL
surface also *declares* BINARY in the Spark schema.

## What is covered

- all three functions;
- all four geometry input encodings (WKB / EWKB / WKT / EWKT);
- authority-coded targets (EPSG and ESRI);
- authority-less targets — raw ``PROJCS[...]`` WKT and a PROJ4 string;
- a non-numeric authority code (``OGC:CRS84``);
- the never-error degrade paths (unresolvable embedded SRID, unresolvable ``source_crs``,
  plain geometry with no source at all);
- ``st_setcrs`` raising for a CRS with no integer authority code;
- the always-BINARY SQL contract on both tiers;
- clean-3D Z preservation and partial-Z quiet-2D handling.

## Running this suite — it is a GATE, not an optional extra

Heavy requires the geobrix JAR *and* the GDAL/OGR native libraries (JNI), both present in
the geobrix-dev Docker container. Because the suite is ``integration``-marked AND needs a
staged JAR, a plain ``gbx:test:python`` run SKIPS it — and a skipped suite reads as green.
Use the dedicated gate, which rebuilds the JAR, stages it, enables integration, and fails
if nothing actually ran::

    bash scripts/commands/gbx-test-parity.sh                  # full gate
    bash scripts/commands/gbx-test-parity.sh --skip-build -k crs   # fast iteration

A stale staged JAR shows up as mass ``UNRESOLVED_ROUTINE`` errors (the heavy functions
simply are not in it); the gate's default rebuild removes that failure mode.
"""

import logging
from pathlib import Path

import pytest

pytest.importorskip("shapely")
pytest.importorskip("pyproj")

import shapely  # noqa: E402
from shapely import from_wkb, get_coordinates, get_srid, to_wkb  # noqa: E402
from shapely.geometry import LineString, Point  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyvx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Reprojected metre coordinates are compared to 1e-6 m (a micrometre): far tighter than
# any real georeferencing need, loose enough for double-rounding between PROJ (light) and
# the GDAL/OGR CT (heavy), which are the same PROJ underneath.
COORD_TOL = 1e-6

_CUSTOM_TM_WKT = (
    'PROJCS["Custom_TM",'
    'GEOGCS["WGS 84",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],'
    'UNIT["degree",0.0174532925199433]],'
    'PROJECTION["Transverse_Mercator"],'
    'PARAMETER["central_meridian",13.7],'
    'PARAMETER["scale_factor",0.9996],'
    'UNIT["metre",1]]'
)
_PROJ4_UTM33 = "+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs"

# Dutch RD written as PROJ4 with a NULL datum shift. PROJ's fuzzy matcher pairs this with
# EPSG:28992 at its default confidence, but the +towgs84=0,0,0,0,0,0,0 forces a ballpark
# datum transformation, so its true coordinates sit ~177 m from real EPSG:28992.
#
# This is the cell _PROJ4_UTM33 cannot cover: for UTM33/WGS84 the fuzzy match and the exact
# definition coincide numerically, so a fuzzy authority leaking into the reprojection math
# is invisible there. Here it is a 177 m cross-tier divergence.
_PROJ4_RD_NULL_SHIFT = (
    "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 "
    "+x_0=155000 +y_0=463000 +ellps=bessel +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
)
_RD_LON, _RD_LAT = 5.39, 52.16

# A WKT body whose parts disagree about dimensionality — no WKT parser accepts it as-is.
_MIXED_DIM_WKT = "GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT (12 43))"


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


# A skipped parity suite reads as green, which is worse than an absent one. Both skip
# reasons below therefore name the ONE command that turns this into a real gate, and say
# outright that the skip is not a pass. `gbx:test:parity` additionally fails the run when
# 0 tests actually executed, so a fully-skipped suite cannot be mistaken for success.
_GATE_CMD = "bash scripts/commands/gbx-test-parity.sh"


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "CROSS-TIER PARITY NOT VERIFIED (this skip is NOT a pass): no geobrix "
            "assembly JAR staged under python/geobrix/lib/, so the heavy tier cannot be "
            f"called. Run the gate: {_GATE_CMD}"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "CROSS-TIER PARITY NOT VERIFIED (this skip is NOT a pass): a JAR-free "
                "Spark session is already live in this process, so spark.jars cannot "
                f"take effect. Run the gate, which runs this suite properly: {_GATE_CMD}"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pyvx-crs-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


@pytest.fixture(scope="module")
def heavy(spark_with_jar):
    """Spark session with the HEAVY (JAR) gbx_st_* CRS functions registered."""
    from databricks.labs.gbx.vectorx import functions as hx

    hx.register(spark_with_jar)
    return spark_with_jar


@pytest.fixture(scope="module")
def light():
    """The light-tier callables the pyvx registrar binds to the gbx_st_* names."""
    from databricks.labs.gbx.pyvx import _crs

    return _crs


# ---------------------------------------------------------------------------
# Geometry fixtures / helpers
# ---------------------------------------------------------------------------


def _ewkb(srid: int, geom=None) -> bytes:
    g = Point(11.0, 42.0) if geom is None else geom
    return to_wkb(shapely.set_srid(g, srid), include_srid=True)


def _plain_wkb() -> bytes:
    return to_wkb(Point(11.0, 42.0))


def _ewkt(srid: int) -> str:
    return f"SRID={srid};POINT (11 42)"


def _plain_wkt() -> str:
    return "POINT (11 42)"


def _sql_lit(geom) -> str:
    """SQL literal for a geometry: unhex(...) for bytes, a quoted string for text."""
    if isinstance(geom, (bytes, bytearray)):
        return f"unhex('{bytes(geom).hex()}')"
    escaped = str(geom).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _heavy_binary(spark, sql: str) -> bytes:
    value = spark.sql(sql).first()[0]
    return None if value is None else bytes(value)


def _decoded(value):
    """Decode a tier result (bytes or EWKT/WKT text) into (geom, srid)."""
    if value is None:
        return None, None
    if isinstance(value, (bytes, bytearray)):
        g = from_wkb(bytes(value))
    else:
        from databricks.labs.gbx._geom import parse_geom

        g = parse_geom(value)
    return g, int(get_srid(g))


def assert_geom_parity(light_value, heavy_value, *, expect_srid=None, expect_z=None):
    """Assert two tier results describe the same geometry: type, SRID, coordinates.

    Compares DECODED geometries, never raw bytes — see the module docstring on the
    big-endian (heavy/JTS) vs little-endian (light/shapely) EWKB writer defaults.
    """
    lg, lsrid = _decoded(light_value)
    hg, hsrid = _decoded(heavy_value)
    assert lg is not None, "light returned no geometry"
    assert hg is not None, "heavy returned no geometry"

    assert (
        lg.geom_type == hg.geom_type
    ), f"geometry type mismatch: light={lg.geom_type} heavy={hg.geom_type}"
    assert lsrid == hsrid, f"SRID mismatch: light={lsrid} heavy={hsrid}"
    if expect_srid is not None:
        assert (
            lsrid == expect_srid
        ), f"expected SRID {expect_srid}, both tiers gave {lsrid}"

    assert (
        lg.has_z == hg.has_z
    ), f"dimensionality mismatch: light has_z={lg.has_z} heavy has_z={hg.has_z}"
    if expect_z is not None:
        assert (
            lg.has_z is expect_z
        ), f"expected has_z={expect_z}, both tiers gave {lg.has_z}"

    lc = get_coordinates(lg, include_z=lg.has_z)
    hc = get_coordinates(hg, include_z=hg.has_z)
    assert lc.shape == hc.shape, f"vertex count mismatch: {lc.shape} vs {hc.shape}"
    for i, (lrow, hrow) in enumerate(zip(lc, hc)):
        for j, (lv, hv) in enumerate(zip(lrow, hrow)):
            if lv != lv and hv != hv:  # both NaN — agreed "no value"
                continue
            assert abs(lv - hv) <= COORD_TOL, (
                f"coordinate mismatch at vertex {i} ordinate {j}: "
                f"light={lv!r} heavy={hv!r} (tol {COORD_TOL})"
            )


# ---------------------------------------------------------------------------
# st_crs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "geom,expected",
    [
        (_ewkb(4326), "EPSG:4326"),
        (_ewkb(54008), "ESRI:54008"),
        (_ewkb(32633), "EPSG:32633"),
        (_ewkt(4326), "EPSG:4326"),
        (_ewkt(54008), "ESRI:54008"),
    ],
    ids=["ewkb-4326", "ewkb-esri54008", "ewkb-32633", "ewkt-4326", "ewkt-esri54008"],
)
def test_st_crs_parity_authority_strings(heavy, light, geom, expected):
    """Both tiers classify an embedded SRID to the same canonical authority string."""
    heavy_value = heavy.sql(f"SELECT gbx_st_crs({_sql_lit(geom)})").first()[0]
    light_value = light.st_crs(geom)
    assert light_value == heavy_value == expected


@pytest.mark.parametrize(
    "geom",
    [_plain_wkb(), _plain_wkt(), _ewkb(999999)],
    ids=["plain-wkb", "plain-wkt", "unresolvable-srid"],
)
def test_st_crs_parity_null_cases(heavy, light, geom):
    """Plain geometries and unresolvable SRIDs return NULL/None on both tiers."""
    heavy_value = heavy.sql(f"SELECT gbx_st_crs({_sql_lit(geom)})").first()[0]
    assert heavy_value is None
    assert light.st_crs(geom) is None


def test_st_crs_parity_null_geom(heavy, light):
    heavy_value = heavy.sql("SELECT gbx_st_crs(CAST(NULL AS BINARY))").first()[0]
    assert heavy_value is None
    assert light.st_crs(None) is None


# ---------------------------------------------------------------------------
# st_setcrs — all four input encodings x authority-coded CRS
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "geom",
    [_plain_wkb(), _ewkb(4326), _plain_wkt(), _ewkt(4326)],
    ids=["wkb", "ewkb", "wkt", "ewkt"],
)
@pytest.mark.parametrize("crs,srid", [("EPSG:32633", 32633), ("ESRI:54008", 54008)])
def test_st_setcrs_parity_encodings(heavy, light, geom, crs, srid):
    """st_setcrs stamps the same SRID on the same coordinates from every encoding.

    Also the always-BINARY contract: the heavy SQL result is BINARY even for the WKT and
    EWKT inputs, matching the light UDF.
    """
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_setcrs({_sql_lit(geom)}, '{crs}')"
    )
    light_value = light._udf_st_setcrs(geom, crs)
    assert isinstance(light_value, (bytes, bytearray))
    assert isinstance(heavy_value, bytes)
    assert_geom_parity(light_value, heavy_value, expect_srid=srid)


def test_st_setcrs_parity_integer_srid_argument(heavy, light):
    """An int-castable CRS argument behaves the same on both tiers."""
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_setcrs({_sql_lit(_plain_wkb())}, 32633)"
    )
    light_value = light._udf_st_setcrs(_plain_wkb(), "32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=32633)


@pytest.mark.parametrize(
    "crs",
    [_PROJ4_UTM33, _PROJ4_RD_NULL_SHIFT, _CUSTOM_TM_WKT, "OGC:CRS84", "IGNF:LAMB93"],
    ids=["proj4", "proj4-null-shift", "raw-wkt", "ogc-crs84", "ignf-lamb93"],
)
def test_st_setcrs_parity_no_integer_authority_raises_both_tiers(heavy, light, crs):
    """A CRS with no integer authority code is rejected by BOTH tiers.

    PROJ4 and raw WKT are authority-less; 'OGC:CRS84' and 'IGNF:LAMB93' resolve but their
    codes are not integers. None can be stored in a geometry's SRID slot, so both tiers
    raise rather than stamp a guess.
    """
    with pytest.raises(Exception):
        heavy.sql(
            f"SELECT gbx_st_setcrs({_sql_lit(_plain_wkb())}, {_sql_lit(crs)})"
        ).first()
    with pytest.raises(ValueError):
        light._udf_st_setcrs(_plain_wkb(), crs)


# ---------------------------------------------------------------------------
# st_transformcrs — authority-coded targets across all four input encodings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("geom", [_ewkb(4326), _ewkt(4326)], ids=["ewkb", "ewkt"])
@pytest.mark.parametrize("crs,srid", [("EPSG:32633", 32633), ("ESRI:54008", 54008)])
def test_st_transformcrs_parity_authority_targets(heavy, light, geom, crs, srid):
    """Reprojection to an authority-coded target agrees in coordinates and SRID."""
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, '{crs}')"
    )
    light_value = light._udf_st_transformcrs(geom, crs)
    assert isinstance(light_value, (bytes, bytearray))
    assert isinstance(heavy_value, bytes)
    assert_geom_parity(light_value, heavy_value, expect_srid=srid)


@pytest.mark.parametrize("geom", [_plain_wkb(), _plain_wkt()], ids=["wkb", "wkt"])
@pytest.mark.parametrize(
    "crs,srid", [("EPSG:32633", 32633), ("ESRI:54008", 54008)], ids=["epsg", "esri"]
)
def test_st_transformcrs_parity_explicit_source_crs(heavy, light, geom, crs, srid):
    """The 3-arg form (explicit source_crs for plain inputs) agrees across tiers.

    Parametrized over an ESRI target as well as EPSG: ESRI codes travel a different
    authority path than EPSG on both tiers, so the 3-arg x ESRI cell is worth pinning
    rather than assuming it follows from the 2-arg ESRI case.
    """
    heavy_value = _heavy_binary(
        heavy,
        f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, '{crs}', 'EPSG:4326')",
    )
    light_value = light._udf_st_transformcrs(geom, crs, "EPSG:4326")
    assert_geom_parity(light_value, heavy_value, expect_srid=srid)


def test_st_transformcrs_parity_identity_target(heavy, light):
    """An identity transform (4326 -> 4326) stamps the target SRID on both tiers."""
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(_ewkb(4326))}, 'EPSG:4326')"
    )
    light_value = light._udf_st_transformcrs(_ewkb(4326), "EPSG:4326")
    assert_geom_parity(light_value, heavy_value, expect_srid=4326)


def test_st_transformcrs_parity_round_trip(heavy, light):
    """Reproject out and back: both tiers land on the original lon/lat."""
    for tier_value in (
        _heavy_binary(
            heavy,
            "SELECT gbx_st_transformcrs(gbx_st_transformcrs("
            f"{_sql_lit(_ewkb(4326))}, 'EPSG:32633'), 'EPSG:4326')",
        ),
        light._udf_st_transformcrs(
            light._udf_st_transformcrs(_ewkb(4326), "EPSG:32633"), "EPSG:4326"
        ),
    ):
        g, srid = _decoded(tier_value)
        assert srid == 4326
        assert abs(g.x - 11.0) < 1e-8 and abs(g.y - 42.0) < 1e-8


# ---------------------------------------------------------------------------
# st_transformcrs — targets with no integer authority code (SRID cleared)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "crs",
    [_CUSTOM_TM_WKT, _PROJ4_UTM33, "OGC:CRS84", "IGNF:LAMB93"],
    ids=["raw-wkt", "proj4", "ogc-crs84", "ignf-lamb93"],
)
@pytest.mark.parametrize("geom", [_ewkb(4326), _ewkt(4326)], ids=["ewkb", "ewkt"])
def test_st_transformcrs_parity_authorityless_target_clears_srid(
    heavy, light, crs, geom
):
    """A target with no integer authority code reprojects and CLEARS the stale SRID.

    A PROJ4 string would fuzzy-match EPSG:32633 at PROJ's default 70% confidence; both
    tiers refuse that guess (heavy because GDAL reports no authority at all, light
    because it probes at full confidence), so PROJ4 lands on this authority-less path.
    """
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, {_sql_lit(crs)})"
    )
    light_value = light._udf_st_transformcrs(geom, crs)
    assert_geom_parity(light_value, heavy_value, expect_srid=0)


def test_st_transformcrs_parity_proj4_reaches_utm_coordinates(heavy, light):
    """The PROJ4 target still reprojects (only the SRID is dropped, not the math)."""
    heavy_value = _heavy_binary(
        heavy,
        f"SELECT gbx_st_transformcrs({_sql_lit(_ewkb(4326))}, {_sql_lit(_PROJ4_UTM33)})",
    )
    light_value = light._udf_st_transformcrs(_ewkb(4326), _PROJ4_UTM33)
    assert_geom_parity(light_value, heavy_value, expect_srid=0)
    g, _ = _decoded(light_value)
    assert abs(g.x - 168701.015089) < 1e-3, f"PROJ4 target did not reproject: {g}"


@pytest.mark.parametrize(
    "order",
    [[_PROJ4_RD_NULL_SHIFT, "EPSG:28992"], ["EPSG:28992", _PROJ4_RD_NULL_SHIFT]],
    ids=["proj4-first", "epsg-first"],
)
def test_st_transformcrs_parity_fuzzy_matched_proj4_does_not_leak_into_math(
    heavy, light, order
):
    """A PROJ4 CRS whose fuzzy match DIFFERS numerically must still agree cross-tier.

    THE cell the rest of the suite cannot reach. `_PROJ4_UTM33` is the case where the
    fuzzy match (EPSG:32633) and the exact definition coincide numerically, so a fuzzy
    authority leaking into the reprojection math is invisible there. This PROJ4 string
    fuzzy-matches EPSG:28992 but its null datum shift puts coordinates ~177 m away, so a
    leak shows up as a large cross-tier divergence.

    Both request orders run because the leak was a transformer CACHE COLLISION: keyed on
    the canonical name, whichever CRS was requested first answered for both. Each target
    is therefore checked against heavy in both orders — a collision in either direction
    fails.
    """
    geom = _ewkb(4326, Point(_RD_LON, _RD_LAT))
    for crs in order:
        heavy_value = _heavy_binary(
            heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, {_sql_lit(crs)})"
        )
        light_value = light._udf_st_transformcrs(geom, crs)
        expect_srid = 28992 if crs == "EPSG:28992" else 0
        assert_geom_parity(light_value, heavy_value, expect_srid=expect_srid)

    # And the two CRSes must remain genuinely distinct on the light tier — if they had
    # collapsed onto one transformer this separation would be 0.
    a, _ = _decoded(light._udf_st_transformcrs(geom, "EPSG:28992"))
    b, _ = _decoded(light._udf_st_transformcrs(geom, _PROJ4_RD_NULL_SHIFT))
    separation = ((a.x - b.x) ** 2 + (a.y - b.y) ** 2) ** 0.5
    assert separation > 100.0, (
        f"EPSG:28992 and its fuzzy-matched PROJ4 twin collapsed to the same "
        f"transformer (separation {separation} m)"
    )


# ---------------------------------------------------------------------------
# Never-error invariant: degrade paths
# ---------------------------------------------------------------------------


def test_st_transformcrs_parity_unresolvable_embedded_srid_degrades(heavy, light):
    """An SRID in no registry returns the input UNCHANGED on both tiers, no exception."""
    bad = _ewkb(999999)
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(bad)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(bad, "EPSG:32633")
    # A degrade returns the caller's own bytes, so here the two tiers even agree byte-wise.
    assert heavy_value == bad
    assert bytes(light_value) == bad
    assert_geom_parity(light_value, heavy_value, expect_srid=999999)


def test_st_transformcrs_parity_unresolvable_source_crs_degrades(heavy, light):
    """An unparseable explicit source_crs degrades to unchanged on both tiers."""
    plain = _plain_wkb()
    heavy_value = _heavy_binary(
        heavy,
        f"SELECT gbx_st_transformcrs({_sql_lit(plain)}, 'EPSG:32633', 'NOT_A_CRS_XYZ')",
    )
    light_value = light._udf_st_transformcrs(plain, "EPSG:32633", "NOT_A_CRS_XYZ")
    assert heavy_value == plain
    assert bytes(light_value) == plain


@pytest.mark.parametrize("geom", [_plain_wkb(), _plain_wkt()], ids=["wkb", "wkt"])
def test_st_transformcrs_parity_no_source_at_all_degrades(heavy, light, geom):
    """A plain geometry with no source CRS is returned unchanged (coords intact)."""
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(geom, "EPSG:32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=0)
    g, _ = _decoded(light_value)
    assert abs(g.x - 11.0) < 1e-12 and abs(g.y - 42.0) < 1e-12


def test_st_transformcrs_parity_unresolvable_target_raises_both_tiers(heavy, light):
    """An explicitly bad TARGET is the one case that may raise — on both tiers."""
    with pytest.raises(Exception):
        heavy.sql(
            f"SELECT gbx_st_transformcrs({_sql_lit(_ewkb(4326))}, 'NOT_A_CRS_XYZ')"
        ).first()
    with pytest.raises(Exception):
        light._udf_st_transformcrs(_ewkb(4326), "NOT_A_CRS_XYZ")


def test_st_transformcrs_parity_null_target_returns_null(heavy, light):
    """NULL target_crs returns NULL/None on both tiers, never an error."""
    heavy_value = heavy.sql(
        f"SELECT gbx_st_transformcrs({_sql_lit(_ewkb(4326))}, CAST(NULL AS STRING))"
    ).first()[0]
    assert heavy_value is None
    assert light._udf_st_transformcrs(_ewkb(4326), None) is None


def test_st_setcrs_parity_null_geom_returns_null(heavy, light):
    heavy_value = heavy.sql(
        "SELECT gbx_st_setcrs(CAST(NULL AS BINARY), 'EPSG:4326')"
    ).first()[0]
    assert heavy_value is None
    assert light._udf_st_setcrs(None, "EPSG:4326") is None


# ---------------------------------------------------------------------------
# Z handling parity
# ---------------------------------------------------------------------------


def _ewkb3d(srid: int, geom) -> bytes:
    return to_wkb(shapely.set_srid(geom, srid), include_srid=True, output_dimension=3)


def test_st_transformcrs_parity_clean_3d_preserves_z(heavy, light):
    """Every vertex has a finite Z -> both tiers carry the Z through the reprojection."""
    geom = _ewkb3d(4326, Point(11.0, 42.0, 500.0))
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(geom, "EPSG:32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=32633, expect_z=True)
    g, _ = _decoded(light_value)
    assert abs(g.z - 500.0) < 1e-6


def test_st_setcrs_parity_clean_3d_preserves_z(heavy, light):
    geom = _ewkb3d(0, Point(11.0, 42.0, 500.0))
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_setcrs({_sql_lit(geom)}, 'EPSG:4326')"
    )
    light_value = light._udf_st_setcrs(geom, "EPSG:4326")
    assert_geom_parity(light_value, heavy_value, expect_srid=4326, expect_z=True)


def test_transformcrs_parity_2d_stays_2d(heavy, light):
    """A genuinely 2D geometry must not gain a Z slot on either tier."""
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(_ewkb(4326))}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(_ewkb(4326), "EPSG:32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=32633, expect_z=False)
    # 2D EWKB is 25 bytes for a POINT; 3D would be 33.
    assert len(heavy_value) == 25
    assert len(bytes(light_value)) == 25


def test_st_transformcrs_parity_partial_z_is_quiet_2d(heavy, light):
    """A partial-Z geometry reprojects as 2D on both tiers — no throw, no NaN X/Y.

    This is the CURRENT rule for mixed-dimensionality input: the missing Z is not
    fabricated and the geometry is handled as 2D so that no horizontal coordinate is
    destroyed by propagating a non-finite ordinate through the transform.
    """
    ls = LineString([(11.0, 42.0, 5.0), (12.0, 43.0, float("nan"))])
    geom = _ewkb3d(4326, ls)
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(geom, "EPSG:32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=32633, expect_z=False)
    for value in (light_value, heavy_value):
        g, _ = _decoded(value)
        for x, y in g.coords:
            assert x == x and y == y, f"coordinate corrupted to NaN: {list(g.coords)}"


def test_st_transformcrs_parity_mixed_dimensionality_wkt_does_not_raise(heavy, light):
    """Mixed-dimensionality WKT reprojects to the same 2D result on both tiers."""
    wkt = f"SRID=4326;{_MIXED_DIM_WKT}"
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_transformcrs({_sql_lit(wkt)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_transformcrs(wkt, "EPSG:32633")
    assert heavy_value is not None and light_value is not None
    # Partial Z -> reprojected as 2D on both tiers, X/Y intact for every vertex.
    assert_geom_parity(light_value, heavy_value, expect_srid=32633, expect_z=False)


@pytest.mark.parametrize(
    "geom,source,expect_srid",
    [
        (f"SRID=999999;{_MIXED_DIM_WKT}", None, 999999),
        (_MIXED_DIM_WKT, None, 0),
        (_MIXED_DIM_WKT, "NOT_A_CRS_XYZ", 0),
    ],
    ids=["unresolvable-srid", "no-source", "bad-source-crs"],
)
def test_st_transformcrs_parity_mixed_dim_wkt_degrade(
    heavy, light, geom, source, expect_srid
):
    """Mixed-dimensionality WKT x DEGRADE path: neither tier raises, and they agree.

    The intersection of two hazards, and the one that broke on the light SQL surface: the
    core degrades by returning the caller's ORIGINAL unparseable string, and the UDF then
    had to re-encode it as BINARY. Both tiers must return the unchanged geometry — heavy
    keeps it as uniform 3D (JTS carries NaN for the absent Z), so light must too, which is
    what `expect_z` pins via the cross-tier has_z comparison.
    """
    src_sql = "" if source is None else f", {_sql_lit(source)}"
    heavy_value = _heavy_binary(
        heavy,
        f"SELECT gbx_st_transformcrs({_sql_lit(geom)}, 'EPSG:32633'{src_sql})",
    )
    light_value = light._udf_st_transformcrs(geom, "EPSG:32633", source)
    assert heavy_value is not None, "heavy must degrade, not return NULL"
    assert light_value is not None, "light must degrade, not return NULL"
    assert_geom_parity(light_value, heavy_value, expect_srid=expect_srid)

    # Degrade means unchanged coordinates on both tiers.
    for value in (light_value, heavy_value):
        g, _ = _decoded(value)
        xy = get_coordinates(g).tolist()
        assert xy == [
            [11.0, 42.0],
            [12.0, 43.0],
        ], f"coordinates moved on a degrade: {xy}"


def test_st_setcrs_parity_mixed_dim_wkt(heavy, light):
    """st_setcrs on mixed-dimensionality WKT must agree across tiers, including Z.

    st_setcrs never touches coordinates, so heavy keeps the geometry uniformly 3D with
    NaN for the vertex that had no Z. Light must produce the same thing rather than
    downcasting to 2D — otherwise the tiers disagree in the text medium.
    """
    geom = f"SRID=4326;{_MIXED_DIM_WKT}"
    heavy_value = _heavy_binary(
        heavy, f"SELECT gbx_st_setcrs({_sql_lit(geom)}, 'EPSG:32633')"
    )
    light_value = light._udf_st_setcrs(geom, "EPSG:32633")
    assert_geom_parity(light_value, heavy_value, expect_srid=32633, expect_z=True)

    # The vertex that HAD a Z keeps its exact value; the one that did not stays absent.
    for value in (light_value, heavy_value):
        g, _ = _decoded(value)
        zs = get_coordinates(g, include_z=True)[:, 2].tolist()
        assert zs[0] == pytest.approx(5.0)
        assert zs[1] != zs[1], "absent Z must stay absent, never fabricated"


# ---------------------------------------------------------------------------
# SQL-surface BINARY contract on the LIGHT tier
# ---------------------------------------------------------------------------


def test_light_sql_surface_declares_binary(spark_with_jar, light):
    """The light UDFs declare BINARY in the Spark schema, matching the heavy expression.

    Registered under temporary ``gbxlight_*`` names: the two tiers share the real
    ``gbx_st_*`` names and cannot both own them in one session, and this test only needs
    the declared return type of the same callables the light registrar binds.
    """
    from pyspark.sql.types import BinaryType, StringType

    spark = spark_with_jar
    spark.udf.register("gbxlight_st_crs", light.st_crs, StringType())
    spark.udf.register("gbxlight_st_setcrs", light._udf_st_setcrs, BinaryType())
    spark.udf.register(
        "gbxlight_st_transformcrs", light._udf_st_transformcrs, BinaryType()
    )

    ewkt = _ewkt(4326)
    df = spark.sql(
        f"SELECT gbxlight_st_crs('{ewkt}') AS crs, "
        f"gbxlight_st_setcrs('{ewkt}', 'EPSG:32633') AS stamped, "
        f"gbxlight_st_transformcrs('{ewkt}', 'EPSG:32633') AS projected"
    )
    fields = {f.name: f.dataType for f in df.schema.fields}
    assert isinstance(fields["crs"], StringType)
    assert isinstance(fields["stamped"], BinaryType)
    assert isinstance(fields["projected"], BinaryType)

    row = df.first()
    assert row["crs"] == "EPSG:4326"
    # TEXT geometry input, BINARY output — the same contract the heavy expression declares.
    assert get_srid(from_wkb(bytes(row["stamped"]))) == 32633
    assert get_srid(from_wkb(bytes(row["projected"]))) == 32633
