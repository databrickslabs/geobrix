"""Oracle table for the raster constructors, `gbx_rst_fromcontent` and `gbx_rst_fromfile`: inputs -> expected, run at
every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions, never from the function under test: the 64 x 48 float32 GeoTIFF
of `tile_4326` (EPSG:4326, NoData -9999) and a 16 x 12 UInt8 PNG are the files and the bytes, so the tile's
raster must open as exactly those (format, size, type, CRS) with the pixels unchanged. `fromfile` has one level (the
light file reader) and reads the file `tile_virtual` is a window of."""

from databricks.labs.gbx.bench.oracle_tables._uc_tile import recipe, share
from databricks.labs.gbx.bench.oracles import Build, Case, builder

# The session rows also run on the plain Python function behind the registered UDF (test_family_oracle_l3_callables).
L3_CALLABLE_FNS = ("gbx_rst_fromcontent",)

FC, FF = "gbx_rst_fromcontent", "gbx_rst_fromfile"
GTIFF, PNG = Build("rc_gtiff_bytes"), Build("rc_png_bytes")
GARBAGE, VRT = Build("rc_garbage_bytes"), Build("rc_vrt_bytes")
FILE, PNG_FILE, GARBAGE_FILE, OVERVIEW_FILE = (
    Build("rc_file"),
    Build("rc_png_file"),
    Build("rc_garbage_file"),
    Build("rc_overview_file"),
)
# the rc_file raster is 64 x 48 pixels of 0.01 degrees from (-1, 52): x -1..-0.36, y 51.52..52
# a quarter pixel inside the pixel edges, so no edge falls on a pixel boundary, where rounding would decide: columns
# 10..49 and rows 10..39, 40 x 30 pixels
BOX_IN = "POLYGON((-0.8975 51.6025, -0.5025 51.6025, -0.5025 51.8975, -0.8975 51.8975, -0.8975 51.6025))"
BOX_OUT = "POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))"
ONLY = ("session",)
MATERIALIZE = (
    ("materialize", True),
)  # the Python entry returns a virtual tile unless it is asked for the pixels
FILE_META = {
    "width": 64,
    "height": 48,
    "count": 1,
    "dtype": "float32",
    "nodata": -9999.0,
    "crs": "EPSG:4326",
    "driver": "GTiff",
    "pixels_of_arg0": True,
}


@builder("rc_gtiff_bytes")
def _rc_gtiff_bytes(tmpdir):
    from databricks.labs.gbx.bench.oracles import _tile_bytes

    return _tile_bytes("EPSG:4326")


def _png():
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _encode

    y, x = np.mgrid[0:12, 0:16]
    return _encode(((x * 5 + y * 3) % 200).astype("uint8")[np.newaxis], "PNG")


@builder("rc_png_bytes")
def _rc_png_bytes(tmpdir):
    return _png()


@builder("rc_garbage_bytes")
def _rc_garbage_bytes(tmpdir):
    return b"II*\x00not-a-real-tiff"


@builder("rc_vrt_bytes")
def _rc_vrt_bytes(tmpdir):
    from databricks.labs.gbx.bench.oracles import _build

    return _build("tile_vrt_bytes", tmpdir)["raster"]


@builder("rc_file")
def _rc_file(tmpdir):
    from databricks.labs.gbx.bench.oracles import _build

    return _build("tile_virtual", tmpdir)[
        "path"
    ]  # the GeoTIFF the virtual tile is a window of


@builder("rc_overview_file")
def _rc_overview_file(tmpdir):
    """The `rc_file` raster as a GeoTIFF with overviews (factors 2 and 4): a read at a coarser resolution takes one."""
    from databricks.labs.gbx._gdal_policy import open_data
    from databricks.labs.gbx.bench.oracles import _tile_bytes

    path = _write(tmpdir, "rc_overview.tif", _tile_bytes("EPSG:4326"))
    with open_data(path, "r+") as ds:
        ds.build_overviews([2, 4])
    return path


def _write(tmpdir, name, content):
    import os

    path = os.path.join(tmpdir, name)
    with open(path, "wb") as fh:
        fh.write(content)
    return path


@builder("rc_png_file")
def _rc_png_file(tmpdir):
    return _write(tmpdir, "rc_oracle.png", _png())


@builder("rc_garbage_file")
def _rc_garbage_file(tmpdir):
    return _write(tmpdir, "rc_oracle_garbage.tif", b"II*\x00not-a-real-tiff")


_SESSION_ROWS = (
    # --- fromcontent: the bytes of a file become a tile that opens as that file
    Case(
        "fromcontent_gtiff",
        FC,
        (GTIFF, "GTiff"),
        "tile_meta",
        {**FILE_META, "meta_driver": "GTiff", "materialized": True},
        ONLY,
    ),
    Case(
        "fromcontent_png",
        FC,
        (PNG, "PNG"),
        "tile_meta",
        {
            "width": 16,
            "height": 12,
            "count": 1,
            "dtype": "uint8",
            "driver": "PNG",
            "meta_driver": "PNG",
            "pixels_of_arg0": True,
        },
        ONLY,
    ),
    # the driver is read in any case and stored as GDAL spells it
    Case(
        "fromcontent_lower_case_driver",
        FC,
        (GTIFF, "gtiff"),
        "tile_meta",
        {"meta_driver": "GTiff"},
        ONLY,
    ),
    Case(
        "fromcontent_bad_driver",
        FC,
        (GTIFF, "NoSuchDriver"),
        "raises",
        "NoSuchDriver",
        ONLY,
    ),
    # a vector-only driver is no raster driver: refused, naming it
    Case(
        "fromcontent_vector_driver",
        FC,
        (GTIFF, "GeoJSON"),
        "raises",
        "GeoJSON",
        ONLY,
    ),
    Case("fromcontent_null_content", FC, (None, "GTiff"), "null", None, ONLY),
    Case("fromcontent_null_driver", FC, (GTIFF, None), "null", None, ONLY),
    Case("fromcontent_corrupt", FC, (GARBAGE, "GTiff"), "error_fix", None, ONLY),
    # a format that can name other files is refused, with the reason
    Case("fromcontent_vrt_refused", FC, (VRT, "VRT"), "error_fix", None, ONLY),
    # --- fromfile: the file at a path becomes a tile of its pixels (the default driver is GTiff)
    Case(
        "fromfile_gtiff",
        FF,
        (FILE, "GTiff"),
        "tile_meta",
        {**FILE_META, "meta_driver": "GTiff", "materialized": True},
        ONLY,
        py_kw=MATERIALIZE,
    ),
    Case(
        "fromfile_default_driver",
        FF,
        (FILE,),
        "tile_meta",
        {"meta_driver": "GTiff", "materialized": True, "pixels_of_arg0": True},
        ONLY,
        py_kw=MATERIALIZE,
    ),
    # the Python entry alone: without `materialize` the tile is virtual (a path, no pixels read), as the light API always
    # returned; SQL reads the pixels
    Case(
        "fromfile_python_default_virtual",
        FF,
        (FILE,),
        "tile_meta",
        {**FILE_META, "materialized": False},
        ("python",),
    ),
    # another format comes out as a GeoTIFF of the same pixels, the driver argument only a hint
    Case(
        "fromfile_png",
        FF,
        (PNG_FILE, "PNG"),
        "tile_meta",
        {
            "width": 16,
            "height": 12,
            "dtype": "uint8",
            "meta_driver": "PNG",
            "materialized": True,
            "pixels_of_arg0": True,
        },
        ONLY,
        py_kw=MATERIALIZE,
    ),
    Case(
        "fromfile_missing",
        FF,
        ("/no/such/dir/oracle_missing.tif", "GTiff"),
        "null",
        None,
        ONLY,
    ),
    # a polygon clips the read to its window: only the blocks it covers are read, and the pixels are those of
    # gbx_rst_clip on the whole raster (the box is 40 x 30 pixels)
    Case(
        "fromfile_clip_geom",
        FF,
        (FILE, "GTiff", BOX_IN, None, None),
        "tile_meta",
        {
            "width": 40,
            "height": 30,
            "count": 1,
            "dtype": "float32",
            "nodata": -9999.0,
            "crs": "EPSG:4326",
            "driver": "GTiff",
            "meta_driver": "GTiff",
            "materialized": True,
        },
        ONLY,
    ),
    # an SRID in the geometry wins over clip_crs: this box is in 4326, clip_crs says 3857
    Case(
        "fromfile_clip_geom_srid_wins",
        FF,
        (FILE, "GTiff", "SRID=4326;" + BOX_IN, "EPSG:3857", None),
        "tile_meta",
        {"width": 40, "height": 30, "crs": "EPSG:4326", "materialized": True},
        ONLY,
    ),
    # a coarser resolution reads an overview: 0.02 degrees is factor 2 of the 0.01 degree file
    Case(
        "fromfile_resolution",
        FF,
        (OVERVIEW_FILE, "GTiff", None, None, 0.02),
        "tile_meta",
        {
            "width": 32,
            "height": 24,
            "count": 1,
            "dtype": "float32",
            "crs": "EPSG:4326",
            "materialized": True,
        },
        ONLY,
    ),
    Case(
        "fromfile_geom_outside_null",
        FF,
        (FILE, "GTiff", BOX_OUT, None, None),
        "null",
        None,
        ONLY,
    ),
    Case("fromfile_corrupt_file", FF, (GARBAGE_FILE, "GTiff"), "null", None, ONLY),
    Case("fromfile_null_path", FF, (None, "GTiff"), "null", None, ONLY),
    Case(
        "fromfile_bad_driver",
        FF,
        (FILE, "NoSuchDriver"),
        "raises",
        "NoSuchDriver",
        ONLY,
    ),
    # a path that is data and names a remote location is refused at once, with the way out, unless the driver's
    # remote-read switch is on
    Case(
        "fromfile_remote_refused",
        FF,
        ("https://example.invalid/oracle.tif", "GTiff"),
        "raises",
        "Fix: copy the data to a Volume",
        ONLY,
    ),
)


# --- fromfile on Unity Catalog: a FILE on a Volume, read by the SQL body. The gate stages the fixture files under
# `{vol}/fixtures/` (examples/tests/uc_fixtures.py builds them with the same constants as here).
#   cog.tif           2000 x 1500, 3 bands uint16, 512 px blocks, overviews 2/4/8, EPSG:32614, about 21 MB: past the 4 MiB
#                     head, so every window's blocks are fetched by ranges. Origin (400000, 4000000), 10 m pixels.
#   cog_prefixed.tif  FIXTURE_PREFIX_BYTES of other bytes, then cog.tif: a FILE with that offset reads as cog.tif.
#   striped.tif       a 600 x 600 GeoTIFF in strips, which a windowed read refuses.
#   notiff.bin        bytes that are no TIFF.
FIXTURE_PREFIX_BYTES = 12345
COG_SHAPE = (3, 1500, 2000)
_CLIP_WKT = "POLYGON((405000 3990000,412000 3990000,412000 3996000,405000 3996000,405000 3990000))"
_FAR_WKT = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
# the clip box in EPSG:4326 (its corners moved from EPSG:32614 and rounded to 6 places): the window is its bounding box
_CLIP_4326_WKT = (
    "POLYGON((-99.977003 36.050579,-99.977672 36.104666,-100.05543 36.104006,"
    "-100.054708 36.049921,-99.977003 36.050579))"
)


def cog_pixels():
    """The pixels of cog.tif, from the generator in python/geobrix/test/_cog_fixture.py (a test pins that they agree)."""
    import numpy as np

    return np.random.default_rng(7).integers(1, 5000, size=COG_SHAPE).astype("uint16")


def _clip_avg():
    """The per-band mean of the clip box's window: columns 500..1199, rows 400..999 of cog.tif."""
    d = cog_pixels()
    return [float(d[b, 400:1000, 500:1200].mean()) for b in range(COG_SHAPE[0])]


_CLIP_4326_RING = (
    (-99.977003, 36.050579),
    (-99.977672, 36.104666),
    (-100.05543, 36.104006),
    (-100.054708, 36.049921),
)
_COG_ORIGIN, _COG_PIXEL = (400000.0, 4000000.0), 10.0


def _clip_4326_width():
    """The width of the 4326 clip's window, from pyproj and the fixture's grid, not from the function: the ring's vertices
    in EPSG:32614, their x bounds snapped outward to whole pixels (floor of the near edge, ceil of the far edge).
    """
    import math

    from databricks.labs.gbx.core.crs import get_transformer

    tr = get_transformer(4326, 32614)
    xs = [tr.transform(x, y)[0] for x, y in _CLIP_4326_RING]
    x0, x1 = (min(xs) - _COG_ORIGIN[0]) / _COG_PIXEL, (
        max(xs) - _COG_ORIGIN[0]
    ) / _COG_PIXEL
    return math.ceil(x1) - math.floor(x0)


def _full_avg():
    d = cog_pixels()
    return [float(d[b].mean()) for b in range(COG_SHAPE[0])]


def _file(name):
    return f"to_file('{{vol}}/fixtures/{name}')"


def _geom(wkt, srid):
    return f"st_geomfromwkt('{wkt}', {srid})"


def _read(file_expr, geom="NULL", resolution="NULL"):
    return f"{{q}}gbx_rst_fromfile({file_expr}, 'GTiff', {geom}, NULL, {resolution})"


# a FILE that is a slice of its object: the object is cog_prefixed.tif and the slice starts after the prefix
_OFFSET_FILE = (
    "(SELECT CAST(named_struct('uri', f.uri, 'offset', CAST("
    + str(FIXTURE_PREFIX_BYTES)
    + " AS BIGINT), 'size', CAST(f.size - "
    + str(FIXTURE_PREFIX_BYTES)
    + " AS BIGINT), 'content_type', f.content_type, 'checksum', f.checksum) AS FILE) AS file "
    + "FROM (SELECT "
    + _file("cog_prefixed.tif")
    + " AS f))"
)

# the head the body reads (the first 4 MiB of a FILE), for the row that asks the planner for its ranges
_HEAD = (
    "CAST(CAST(named_struct('uri', f.uri, 'offset', CAST(0 AS BIGINT), 'size', "
    "least(f.size, CAST(4194304 AS BIGINT)), 'content_type', f.content_type, 'checksum', f.checksum) AS FILE) AS BINARY)"
)

UC_FROMFILE = (
    # a polygon clips the read to its window (a 700 x 600 pixel box); the window's blocks are past the head
    recipe(
        "uc_fromfile_clip",
        FF,
        "SELECT {q}gbx_rst_width("
        + _read(_file("cog.tif"), _geom(_CLIP_WKT, 32614))
        + ") AS out",
        "exact",
        700,
    ),
    # the pixels, not only the size: the mean of each band of the window, from the generator's own numbers. The window
    # spans two block rows and three block columns, so the body fetches more than one range and passes them as an
    # ARRAY<BINARY> (a CAST-only transform) to the Python reader
    recipe(
        "uc_fromfile_clip_pixels",
        FF,
        "SELECT {q}gbx_rst_avg("
        + _read(_file("cog.tif"), _geom(_CLIP_WKT, 32614))
        + ") AS out",
        "approx",
        _clip_avg,
    ),
    # the planner is a function of its own: for this window it names more than one range, the first past the head
    recipe(
        "uc_fromfile_clip_ranges",
        FF,
        "SELECT size(p.offs) > 1 AND p.offs[0] >= 4194304 AS out FROM (SELECT {q}gbx__fromfile_plan("
        + _HEAD
        + ", f.size, "
        + _geom(_CLIP_WKT, 32614)
        + ", NULL, NULL) AS p FROM (SELECT "
        + _file("cog.tif")
        + " AS f))",
        "exact",
        True,
    ),
    # no geometry: the whole raster, every block fetched
    recipe(
        "uc_fromfile_full_pixels",
        FF,
        "SELECT {q}gbx_rst_avg(" + _read(_file("cog.tif")) + ") AS out",
        "approx",
        _full_avg,
    ),
    # a coarser resolution reads an overview: 40 m is factor 4 of the 10 m file, 2000 / 4 pixels wide
    recipe(
        "uc_fromfile_resolution",
        FF,
        "SELECT {q}gbx_rst_width("
        + _read(_file("cog.tif"), resolution="40.0")
        + ") AS out",
        "exact",
        500,
    ),
    # a polygon that misses the raster gives NULL
    recipe(
        "uc_fromfile_outside_null",
        FF,
        "SELECT " + _read(_file("cog.tif"), _geom(_FAR_WKT, 32614)) + " AS out",
        "null",
    ),
    # a GEOMETRY(4326) passes straight into the GEOMETRY(ANY) parameter, its SRID read: the clip box in 4326 is a window
    # of its bounding box, 702 pixels wide
    recipe(
        "uc_fromfile_geom4326",
        FF,
        "SELECT {q}gbx_rst_width("
        + _read(_file("cog.tif"), _geom(_CLIP_4326_WKT, 4326))
        + ") AS out",
        "exact",
        _clip_4326_width,
    ),
    # a FILE with a non-NULL offset reads as the file it is a slice of: the clip's pixels equal cog.tif's own
    recipe(
        "uc_fromfile_offset_file",
        FF,
        "SELECT {q}gbx_rst_avg("
        + _read("s.file", _geom(_CLIP_WKT, 32614))
        + ") AS out FROM "
        + _OFFSET_FILE
        + " AS s",
        "approx",
        _clip_avg,
    ),
    # a GeoTIFF in strips cannot be read by block: refused with the way out, never a slow whole-file read
    recipe(
        "uc_fromfile_striped_refused",
        FF,
        "SELECT " + _read(_file("striped.tif")) + " AS out",
        "raises",
        "the GeoTIFF is striped. Fix: write it as a COG",
    ),
    # bytes that are no TIFF are refused, not NULL
    recipe(
        "uc_fromfile_not_tiff_refused",
        FF,
        "SELECT " + _read(_file("notiff.bin")) + " AS out",
        "raises",
        "reads tiled GeoTIFF/COG files only. Fix:",
    ),
)


# Unity Catalog serves fromcontent (the bytes are the argument) and fromfile (a FILE on a Volume: the UC_FROMFILE rows
# above). Bytes GDAL cannot open are refused with the reason and a Fix, where the session answers an error tile.
RASTER_CONSTRUCTOR = share(
    _SESSION_ROWS,
    "fromcontent_gtiff", "fromcontent_png", "fromcontent_lower_case_driver", "fromcontent_bad_driver",
    "fromcontent_vector_driver", "fromcontent_null_content", "fromcontent_null_driver",
) + (
    Case("uc_fromcontent_corrupt_refused", FC, (GARBAGE, "GTiff"), "uc_refused", None, ("uc",)),
    Case("uc_fromcontent_vrt_refused", FC, (VRT, "VRT"), "uc_refused", None, ("uc",)),
) + UC_FROMFILE  # fmt: skip
