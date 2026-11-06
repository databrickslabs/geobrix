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

# Raster functions take a tile column that carries its pixels. A header function reads no pixel.
UC_RASTER_HEADER_SQL = """SELECT main.geobrix.gbx_rst_width(tile) AS width,
       main.geobrix.gbx_rst_srid(tile) AS srid,
       main.geobrix.gbx_rst_avg(tile) AS band_means
FROM rasters"""

# A tile function returns a tile. A CRS argument is an integer SRID, EPSG:<n> or ESRI:<n>.
UC_RASTER_TILE_SQL = """SELECT main.geobrix.gbx_rst_ndvi(tile, 3, 4) AS ndvi,
       main.geobrix.gbx_rst_slope(tile, 'percent') AS slope
FROM rasters"""

# An array function takes an ARRAY of tiles, so it needs no aggregate.
UC_RASTER_MERGE_SQL = """SELECT main.geobrix.gbx_rst_merge(collect_list(tile)) AS mosaic
FROM rasters
WHERE scene = 'T30UXC'"""

# The statistics of a cell across the tiles of a table: the partial state of each tile, merged by a native GROUP BY.
# The partial has default arguments, so call it in a subquery and explode the column (UC refuses a SQL function in a generator).
UC_RASTER_TO_GRID_SQL = """WITH parts AS (
  SELECT main.geobrix.gbx_rst_h3_rastertogrid_partial(tile, 7) AS cells FROM rasters
),
p AS (SELECT c.* FROM parts LATERAL VIEW inline(cells) c),
w AS (
  SELECT *, try_divide(sum(sum) OVER (PARTITION BY band, cellid),
                       sum(count) OVER (PARTITION BY band, cellid)) AS mu
  FROM p
)
SELECT band, cellid,
       sum(count) AS count,
       try_divide(sum(sum), sum(count)) AS avg,
       try_divide(sum(m2) + sum(count * pow(try_divide(sum, count) - mu, 2)), sum(count)) AS variance,
       min(min) AS min, max(max) AS max
FROM w
GROUP BY band, cellid"""

# A tile is cut into tiles by a table function: LATERAL gives one row for each sub-tile, with the session's columns.
UC_RASTER_RETILE_SQL = """SELECT r.scene, t.window, t.raster
FROM rasters AS r,
     LATERAL main.geobrix.gbx_rst_retile(r.tile, 512, 512) AS t"""

# A tile is cut into the tiles of the grid cells it covers; the cell id is in cellid.
UC_RASTER_TESSELLATE_SQL = """SELECT r.scene, t.cellid, t.raster
FROM rasters AS r,
     LATERAL main.geobrix.gbx_rst_h3_tessellate(r.tile, 5) AS t"""

# A tile is rendered into the web tiles it touches across a range of zoom levels.
UC_RASTER_PYRAMID_SQL = """SELECT r.scene, t.z, t.x, t.y, t.bytes
FROM rasters AS r,
     LATERAL main.geobrix.gbx_rst_xyzpyramid(r.tile, 4, 6) AS t"""

# The points of a surface are collected as GEOMETRY values; each triangle comes out as 2D WKB.
UC_VECTOR_TRIANGULATE_SQL = """SELECT t.triangle
FROM (SELECT collect_list(CAST(geom AS GEOMETRY(ANY))) AS pts FROM spot_heights) AS s,
     LATERAL main.geobrix.gbx_st_triangulate(s.pts, CAST(NULL AS ARRAY<GEOMETRY(ANY)>), 0.0, 0.0, 'NONENCROACHING') AS t"""

# The points of a terrain are collected as GEOMETRY values; the Z of each point is kept.
UC_RASTER_DTM_SQL = """SELECT main.geobrix.gbx_rst_dtmfromgeoms(
           collect_list(CAST(geom AS GEOMETRY(ANY))),
           CAST(NULL AS ARRAY<GEOMETRY(ANY)>),
           0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 256, 256, 3857) AS dtm
FROM spot_heights"""

# A band tile records its band number in its metadata.
UC_RASTER_BANDS_SQL = """SELECT r.scene, t.metadata['band'] AS band, t.raster
FROM rasters AS r,
     LATERAL main.geobrix.gbx_rst_separatebands(r.tile) AS t"""

# A geometry cleaner takes GEOMETRY(ANY) and returns a GEOMETRY in the SRID of the first argument.
UC_VECTOR_SNAP_SQL = """SELECT id,
       main.geobrix.gbx_st_snap(CAST(geom AS GEOMETRY(ANY)), CAST(ref AS GEOMETRY(ANY)), 0.5) AS snapped
FROM parcels"""

# A coverage check takes the polygons of one group as an array. EWKB keeps their SRID on the edges.
UC_VECTOR_COVERAGE_SQL = """SELECT district,
       main.geobrix.gbx_st_coverageisvalid_array(collect_list(st_asewkb(geom)), 0.0) AS is_valid
FROM parcels
GROUP BY district"""

# A vector tile layer is made from the features of one tile; the attributes go in as JSON text.
UC_VECTOR_MVT_SQL = """SELECT z, x, y,
       main.geobrix.gbx_st_asmvt_array(
         collect_list(struct(st_asbinary(geom) AS geom, to_json(attrs) AS attrs)), 'roads') AS mvt
FROM features
GROUP BY z, x, y"""

# A tile is laid out as a Cloud Optimized GeoTIFF, and given overviews, by a scalar function.
UC_RASTER_FILES_SQL = """SELECT scene,
       main.geobrix.gbx_rst_buildoverviews(
         main.geobrix.gbx_rst_cog_convert(tile, 'AUTO', 512, 'AVERAGE'), array(2, 4, 8)) AS tile
FROM rasters"""

# Install the functions and the procedures that call them.
INSTALL_PROCEDURES_PYTHON = '''import databricks.labs.gbx as gbx

gbx.uc.install("main", "geobrix", "/Volumes/main/geobrix/artifacts/env6", procedures=True)
'''

# A procedure takes table names; the columns it reads are fixed.
CALL_POLYGONS_TO_BNG_SQL = """CALL main.geobrix.gbx_polygons_to_bng(
  source_table => 'main.geobrix.parcels',
  target_table => 'main.geobrix.parcel_cells',
  bng_resolution => '100m')"""

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


def install_with_each_function_its_own_packages():
    """Give each Python function only the packages it imports."""
    import databricks.labs.gbx as gbx

    statements = gbx.uc.install(
        "main",
        "geobrix",
        "/Volumes/main/geobrix/artifacts/env6",
        deps="per-function",
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


def _float_tile(width, height, sparse=False):
    """A tile that carries the pixels of a width by height float32 GeoTIFF, as a Unity Catalog function receives it.
    A sparse tile writes no block, so a tile of hundreds of MiB decoded is a few KiB of file."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(0.0, 1.0, 1.0 / width, 1.0 / height),
            compress="deflate",
            tiled=True,
            sparse_ok=sparse,
        ) as dst:
            if not sparse:
                dst.write(np.zeros((height, width), dtype="float32"), 1)
        return {"cellid": 0, "raster": mf.read(), "path": None, "metadata": {}}


def virtual_tile_is_refused():
    """A virtual tile holds a path and no pixels. A UC function refuses it and names the session function."""
    from databricks.labs.gbx.uc import tier_b

    virtual = {"cellid": 0, "raster": None, "path": "/Volumes/main/geobrix/rasters/dem.tif", "metadata": {}}
    try:
        tier_b.HANDLERS["gbx_rst_avg"](virtual)
    except ValueError as err:
        return str(err)


def tile_over_the_limit():
    """A pixel function refuses a tile decoded over 64 MiB. A header function answers at any size."""
    from databricks.labs.gbx.uc import tier_b

    tile = _float_tile(4200, 4200)  # 67 MiB decoded
    width = tier_b.HANDLERS["gbx_rst_width"](tile)
    try:
        tier_b.HANDLERS["gbx_rst_avg"](tile)
    except ValueError as err:
        return width, str(err)


def retile_over_the_limit():
    """A tile decoded over 256 MiB is refused by retile, with a Fix. The pieces are made in a session."""
    from databricks.labs.gbx.uc import forms

    tile = _float_tile(8300, 8300, sparse=True)  # 262 MiB decoded
    try:
        forms.rst_retile_tiles(tile, 512, 512)
    except ValueError as err:
        return str(err)


def retile_a_tile_over_the_pixel_limit():
    """Retile reads a tile in windows, so it cuts a tile of 67 MiB decoded that a pixel function such as gbx_rst_avg refuses."""
    from databricks.labs.gbx.uc import forms, tier_b

    tile = _float_tile(4200, 4200)  # 67 MiB decoded
    pieces = forms.rst_retile_tiles(tile, 1400, 1400)
    try:
        tier_b.HANDLERS["gbx_rst_avg"](tile)
    except ValueError as err:
        return len(pieces), str(err)


def dtm_from_geometry_points():
    """The Z of each Geometry point reaches the terrain: the height at the centre of a plane z = x + 2 y is 15."""
    import numpy as np
    import shapely
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.uc import tier_b

    class Geometry:  # what a UC function receives for a GEOMETRY value
        def __init__(self, geom, srid):
            self._wkb, self._srid = shapely.to_wkb(geom, flavor="iso"), srid

        def getBytes(self):  # noqa: N802 - the pyspark name
            return self._wkb

        def getSrid(self):  # noqa: N802
            return self._srid

    corners = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
    points = [Geometry(shapely.Point(x, y, x + 2 * y), 3857) for x, y in corners]
    tile = tier_b.HANDLERS["gbx_rst_dtmfromgeoms"](
        points, None, 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 5, 5, 3857, -9999.0, None
    )
    with MemoryFile(tile["raster"]) as mf, mf.open() as ds:
        return float(np.asarray(ds.read(1))[2, 2])


def snap_across_srids_is_refused():
    """Two geometries in different SRIDs are refused, with the Fix to transform one of them."""
    import shapely

    from databricks.labs.gbx.uc import handlers

    class Geometry:
        def __init__(self, wkt, srid):
            self._wkb, self._srid = shapely.from_wkt(wkt).wkb, srid

        def getBytes(self):  # noqa: N802 - the pyspark name
            return self._wkb

        def getSrid(self):  # noqa: N802
            return self._srid

    try:
        handlers.st_snap(Geometry("POINT(1 1)", 4326), Geometry("POINT(1 1)", 3857), 0.5)
    except ValueError as err:
        return str(err)


def feature_attributes_must_be_json():
    """The attributes of a feature are a JSON object text; a struct cast to text is named as such."""
    import shapely

    from databricks.labs.gbx.uc import forms

    try:
        forms.st_asmvt_array([{"geom": shapely.Point(5, 7).wkb, "attrs": "{1, a}"}], "roads")
    except ValueError as err:
        return str(err)


def overview_levels_must_be_2_or_more():
    """A level below 2 raises with a Fix, whatever the tile holds."""
    from databricks.labs.gbx.uc import tier_b

    try:
        tier_b.rst_buildoverviews(_float_tile(64, 48), [1])
    except ValueError as err:
        return str(err)


def preview_procedures():
    """Return the statements install would send for the functions and the procedures, without sending them."""
    import databricks.labs.gbx as gbx

    return gbx.uc.install(
        "main",
        "geobrix",
        "/Volumes/main/geobrix/artifacts/env6",
        dry_run=True,
        procedures=True,
    )
