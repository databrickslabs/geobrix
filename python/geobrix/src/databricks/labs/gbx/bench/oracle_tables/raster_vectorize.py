"""Oracle table for the raster functions that turn pixels into vector features, here the iso-bands (the contour
lines are in SPEC_DEM and the polygonize rows in SPEC_ANALYSIS, which hold the GDAL spec engine's functions):
inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixture's definition, never from the function under test. `vz_columns` is a 6 x 6
raster of 10 m pixels in EPSG:3857 whose value is its column number, 0 to 5, so a band of breaks is a block of whole
columns and its area is the number of pixels times 100 square metres.
"""

import os

from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, share
from databricks.labs.gbx.bench.oracles import Build, Case, _gtiff, _tile, builder

ISO = "gbx_rst_isoband"
COLUMNS, ERR, CORRUPT = Build("vz_columns"), Build("tile_error"), Build("tile_corrupt")
MISSING = Build("vz_missing")
ONLY = ("session",)


@builder("vz_columns")
def _vz_columns(tmpdir):
    import numpy as np

    z = np.tile(np.arange(6, dtype="float64"), (6, 1))
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857", nodata=None))


@builder("vz_missing")
def _vz_missing(tmpdir):
    """A virtual tile whose file is not there (a UC Volume path that has not caught up): not corrupt bytes."""
    return _tile(None, os.path.join(tmpdir, "gbx_missing_tile.tif"))


@builder("vz_columns_virtual")
def _vz_columns_virtual(tmpdir):
    """`vz_columns` as a virtual tile: the same pixels in a file, the tile a path and no bytes."""
    import numpy as np

    z = np.tile(np.arange(6, dtype="float64"), (6, 1))
    path = os.path.join(tmpdir, "gbx_vz_columns.tif")
    with open(path, "wb") as fh:
        fh.write(_gtiff(z[np.newaxis], "EPSG:3857", nodata=None))
    return _tile(None, path)


def _iso_tile(z, nodata=None):
    import numpy as np

    return _tile(
        _gtiff(np.asarray(z, dtype="float32")[np.newaxis], "EPSG:3857", nodata=nodata)
    )


@builder("vz_quadrants")
def _vz_quadrants(tmpdir):
    """6 x 6 pixels, one value per 3 x 3 quadrant: 25, 75 / 125, 175."""
    import numpy as np

    z = np.zeros((6, 6))
    z[:3, :3], z[:3, 3:], z[3:, :3], z[3:, 3:] = 25.0, 75.0, 125.0, 175.0
    return _iso_tile(z)


@builder("vz_nodata_hole")
def _vz_nodata_hole(tmpdir):
    """4 x 4 pixels of 25 with the 2 x 2 in the middle NoData (-9999)."""
    import numpy as np

    z = np.full((4, 4), 25.0)
    z[1:3, 1:3] = -9999.0
    return _iso_tile(z, nodata=-9999.0)


@builder("vz_all_high")
def _vz_all_high(tmpdir):
    import numpy as np

    return _iso_tile(np.full((4, 4), 300.0))


@builder("vz_two_islands")
def _vz_two_islands(tmpdir):
    """8 x 8 pixels of 75 with two 3 x 3 islands of 25 at opposite corners."""
    import numpy as np

    z = np.full((8, 8), 75.0)
    z[:3, :3] = 25.0
    z[5:, 5:] = 25.0
    return _iso_tile(z)


_FIVE = [0.0, 50.0, 100.0, 150.0, 200.0]
_SESSION_ROWS = (
    # [0, 2) holds columns 0 and 1, [2, 4) columns 2 and 3, [4, 6) columns 4 and 5: 12 pixels each, 1200 m2.
    Case(
        "isoband_three_bands",
        ISO,
        (COLUMNS, [0.0, 2.0, 4.0, 6.0]),
        "isobands",
        [(0, 0.0, 2.0, 1200.0), (1, 2.0, 4.0, 1200.0), (2, 4.0, 6.0, 1200.0)],
        ONLY,
    ),
    # Pixels below the first break and at or above the last are in no band.
    Case(
        "isoband_outside_dropped",
        ISO,
        (COLUMNS, [1.0, 3.0]),
        "isobands",
        [(0, 1.0, 3.0, 1200.0)],
        ONLY,
    ),
    Case(
        "isoband_virtual",
        ISO,
        (Build("vz_columns_virtual"), [1.0, 3.0]),
        "isobands",
        [(0, 1.0, 3.0, 1200.0)],
        ONLY,
    ),
    # one 3 x 3 block (900 m2) in each of four bands; breaks given as inline decimal-looking literals
    Case(
        "isoband_quadrants",
        ISO,
        (Build("vz_quadrants"), _FIVE),
        "isobands",
        [
            (0, 0.0, 50.0, 900.0),
            (1, 50.0, 100.0, 900.0),
            (2, 100.0, 150.0, 900.0),
            (3, 150.0, 200.0, 900.0),
        ],
        ONLY,
    ),
    # NoData pixels are in no band: the ring of 12 pixels is one polygon, the hole not counted
    Case(
        "isoband_nodata_excluded",
        ISO,
        (Build("vz_nodata_hole"), _FIVE),
        "isobands",
        [(0, 0.0, 50.0, 1200.0)],
        ONLY,
    ),
    # every pixel at or above the last break: no band, no polygon
    Case("isoband_all_above", ISO, (Build("vz_all_high"), _FIVE), "isobands", [], ONLY),
    # two islands of one band stay two polygons; the other band is the 46 pixels around them
    Case(
        "isoband_disjoint_islands",
        ISO,
        (Build("vz_two_islands"), _FIVE),
        "isobands",
        [(0, 0.0, 50.0, 900.0), (0, 0.0, 50.0, 900.0), (1, 50.0, 100.0, 4600.0)],
        ONLY,
    ),
    Case("isoband_null_tile", ISO, (None, [0.0, 1.0]), "null", None, ONLY),
    Case("isoband_null_breaks", ISO, (COLUMNS, None), "null", None, ONLY),
    Case("isoband_empty_breaks", ISO, (COLUMNS, []), "null", None, ONLY),
    Case(
        "isoband_not_ascending",
        ISO,
        (COLUMNS, [0.0, 4.0, 2.0]),
        "raises",
        "ascending",
        ONLY,
    ),
    Case(
        "isoband_null_element_break",
        ISO,
        (COLUMNS, [0.0, None, 2.0]),
        "raises",
        "NULL",
        ONLY,
    ),
    Case("isoband_error_tile", ISO, (ERR, [0.0, 1.0]), "null", None, ONLY),
    Case("isoband_corrupt", ISO, (CORRUPT, [0.0, 1.0]), "null", None, ONLY),
    # A file that is not reachable fails the task so that Spark retries; only bytes GDAL cannot read give NULL.
    Case(
        "isoband_missing_path",
        ISO,
        (MISSING, [0.0, 1.0]),
        "raises",
        "gbx_missing_tile",
        ONLY,
    ),
)

# Unity Catalog: the surface of a materialized tile is the session's; a virtual tile is refused with a Fix, a corrupt
# one is NULL, and breaks that are not ascending (or hold a NULL) are a wrong argument whatever the tile holds.
RASTER_VECTORIZE = share(
    _SESSION_ROWS,
    "isoband_three_bands", "isoband_outside_dropped", "isoband_null_tile", "isoband_null_breaks",
    "isoband_empty_breaks", "isoband_not_ascending", "isoband_null_element_break", "isoband_error_tile",
    "isoband_corrupt",
) + edges(isoband=([0.0, 1.0],))  # fmt: skip
