"""Oracle table for the raster constructors, `gbx_rst_fromcontent` and `gbx_rst_fromfile`: inputs -> expected, run at
every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions, never from the function under test: the 64 x 48 float32 GeoTIFF
of `tile_4326` (EPSG:4326, NoData -9999) and a 16 x 12 UInt8 PNG are the files and the bytes, so the tile's
raster must open as exactly those (format, size, type, CRS) with the pixels unchanged. `fromfile` has one level (the
light file reader) and reads the file `tile_virtual` is a window of."""

from databricks.labs.gbx.bench.oracles import Build, Case, builder

FC, FF = "gbx_rst_fromcontent", "gbx_rst_fromfile"
GTIFF, PNG = Build("rc_gtiff_bytes"), Build("rc_png_bytes")
GARBAGE, VRT = Build("rc_garbage_bytes"), Build("rc_vrt_bytes")
FILE, PNG_FILE, GARBAGE_FILE = (
    Build("rc_file"),
    Build("rc_png_file"),
    Build("rc_garbage_file"),
)
ONLY = ("session",)
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


RASTER_CONSTRUCTOR = (
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
    ),
    Case(
        "fromfile_default_driver",
        FF,
        (FILE,),
        "tile_meta",
        {"meta_driver": "GTiff", "materialized": True, "pixels_of_arg0": True},
        ONLY,
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
    ),
    Case(
        "fromfile_missing",
        FF,
        ("/no/such/dir/oracle_missing.tif", "GTiff"),
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
