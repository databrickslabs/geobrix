"""
Unity Catalog functions page examples.

The install, the CRS forms and the SQL calls shown on docs/docs/api/uc-functions.mdx. Nothing here needs a
workspace: the install runs as a dry run, which returns the statements it would send.
"""

VOLUME_DIR = "/Volumes/main/geobrix/artifacts/env6"

# The install, as a user runs it from an env 6 notebook or job.
INSTALL_PYTHON = '''import databricks.labs.gbx as gbx

gbx.uc.install("main", "geobrix", "/Volumes/main/geobrix/artifacts/env6")
'''

# Check that the schema holds this wheel's functions, without registering any session function.
VERIFY_PYTHON = '''from databricks.labs.gbx import register

register(spark, target="uc", catalog="main", schema="geobrix")
'''

# Opt in to unqualified names. GeoBrix never runs SET PATH for you.
SET_PATH_SQL = (
    "SET PATH = `main`.`geobrix`, system.session, system.builtin, system.ai, CURRENT_SCHEMA"
)

# A geometry goes in as GEOMETRY with an integer SRID. BNG needs 27700; quadbin and H3 need 4326.
UC_BNG_POINTASCELL_SQL = """SELECT main.geobrix.gbx_bng_pointascell(
  st_geomfromwkt('POINT(530000 180000)', 27700), '3') AS cell"""

UC_BNG_KRING_SQL = "SELECT main.geobrix.gbx_bng_kring('TQ3080', 1) AS cells"

UC_QUADBIN_POLYFILL_SQL = """SELECT main.geobrix.gbx_quadbin_polyfill(
  st_geomfromwkt('POLYGON((-0.2 51.4, -0.2 51.6, 0.0 51.6, 0.0 51.4, -0.2 51.4))', 4326), '10') AS cells"""

# A table function is called in FROM.
UC_KRINGEXPLODE_SQL = "SELECT cellid FROM main.geobrix.gbx_bng_kringexplode('TQ3080', 1)"

# A WKB BINARY is not a GEOMETRY. Build the GEOMETRY first.
UC_WKB_TO_GEOMETRY_SQL = """SELECT main.geobrix.gbx_bng_pointascell(
  st_geomfromwkb(wkb, 27700), '3') AS cell
FROM points"""

# The CRS argument of a UC function takes three forms.
UC_TRANSFORMCRS_SQL = "SELECT main.geobrix.gbx_rst_transformcrs(tile, 'EPSG:4326') AS tile FROM rasters"

# Check the version and the language of the functions in a schema.
UC_ROUTINES_SQL = """SELECT routine_name, external_language, comment
FROM main.information_schema.routines
WHERE routine_schema = 'geobrix' AND routine_name LIKE 'gbx_%'"""


def preview_install():
    """Return the statements install would send, without sending them."""
    import databricks.labs.gbx as gbx

    statements = gbx.uc.install(
        "main",
        "geobrix",
        "/Volumes/main/geobrix/artifacts/env6",
        dry_run=True,
    )
    return statements


def preview_install_with_jar(version):
    """Name the JAR to make the JVM-only BNG functions Scala functions."""
    import databricks.labs.gbx as gbx

    jar = f"/Volumes/main/geobrix/artifacts/env6/geobrix-{version}-jar-with-dependencies.jar"
    statements = gbx.uc.install(
        "main", "geobrix", "/Volumes/main/geobrix/artifacts/env6", dry_run=True, jar=jar
    )
    return statements


def install_only_the_bng_cells():
    """Install a few functions and the table functions that call them."""
    import databricks.labs.gbx as gbx

    statements = gbx.uc.install(
        "main",
        "geobrix",
        "/Volumes/main/geobrix/artifacts/env6",
        only=["bng_kring", "bng_kringexplode"],
        dry_run=True,
    )
    return statements


def crs_forms():
    """The three CRS forms a UC function accepts."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    forms = [parse_uc_crs(value, fn="gbx_rst_transformcrs") for value in (4326, "EPSG:4326", "ESRI:102100")]
    return forms


def crs_refused():
    """Any other CRS text is refused with a Fix."""
    from databricks.labs.gbx.core.crs import parse_uc_crs

    try:
        parse_uc_crs("+proj=longlat +datum=WGS84", fn="gbx_rst_transformcrs")
    except ValueError as err:
        return str(err)


def bad_data_is_null():
    """A UC function returns NULL for data it cannot serve: a text that is no cell id."""
    from databricks.labs.gbx.uc import handlers

    cells = handlers.bng_kring("not a cell", 1)
    return cells


def bad_parameter_is_refused():
    """A resolution BNG does not define raises, naming the value."""
    from databricks.labs.gbx.uc import handlers

    try:
        handlers.bng_eastnorthasbng(530000.0, 180000.0, "99")
    except ValueError as err:
        return str(err)
