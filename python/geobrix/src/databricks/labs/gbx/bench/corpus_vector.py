"""Scaled vector benchmark corpus generator. Mints a 1M-polygon seed in the light
vector-writer schema, transcodes it to each format via the *_gbx writers, and
replicates each seed into a per-format directory on the bench Volume. Runs locally
(small scale) and on the bench cluster (full scale). FileGDB writing needs the
heavyweight GDAL natives (native osgeo) -- cluster only.

Shapefile and FileGDB seeds+copies are stored as self-contained zip archives
(.shp.zip / .gdb.zip) so both the light (*_gbx) and heavy (*_ogr) readers can read
a directory of copies -- the heavy OGR dir-read requires each entry to be a single
self-contained file."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from typing import List


def generate_polygon_seed(spark, n_rows: int, srid: str = "4326"):
    """A DataFrame of ``n_rows`` synthetic polygons in the light vector-writer schema
    (geom_0 WKB, geom_0_srid, geom_0_srid_proj, id, name). Polygons are small axis-
    aligned boxes at deterministic pseudo-random lon/lat from the row id."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import BinaryType

    @F.udf(BinaryType())
    def _poly(i):
        from shapely import box, to_wkb

        lon = (int(i) * 73 % 35900) / 100.0 - 179.0
        lat = (int(i) * 37 % 17800) / 100.0 - 89.0
        d = 0.01
        return bytes(to_wkb(box(lon, lat, lon + d, lat + d)))

    return spark.range(n_rows).select(
        _poly(F.col("id")).alias("geom_0"),
        F.lit(srid).alias("geom_0_srid"),
        F.lit("").alias("geom_0_srid_proj"),
        F.col("id").cast("int").alias("id"),
        F.concat(F.lit("feat_"), F.col("id").cast("string")).alias("name"),
    )


_EXT = {
    "geojson_gbx": "geojson",
    # shapefile_gbx and file_gdb_gbx produce .shp.zip / .gdb.zip after transcode_vector_seed
    # zips the raw writer output; the _EXT values below are the intermediate extensions
    # produced by the *_gbx writers before zipping.
    "shapefile_gbx": "shp",
    "gpkg_gbx": "gpkg",
    "file_gdb_gbx": "gdb",
    "vector_gbx": "geojson",
}


def _zip_shapefile(seed_dir: str, stem: str) -> str:
    """Zip the shapefile component files (seed.*) from seed_dir into seed_dir/stem.shp.zip.
    The archive is flat: each component sits at the zip root (no subdirectory), matching
    what /vsizip/…/seed.shp.zip expects for ESRI Shapefile.

    The zip is built on driver-local disk then sequential-copied to the target: UC Volumes
    are object storage, and ``zipfile.close()`` seeks back to write the central directory,
    which fails on a FUSE mount (``OSError: [Errno 5]``).  Removes the loose component files
    after.  Returns the zip path."""
    zip_path = os.path.join(seed_dir, f"{stem}.shp.zip")
    components = [
        n
        for n in os.listdir(seed_dir)
        if n.startswith(stem + ".") and not n.endswith(".zip")
    ]
    local_dir = tempfile.mkdtemp(prefix="gbx_zipshp_")
    try:
        local_zip = os.path.join(local_dir, f"{stem}.shp.zip")
        with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for name in components:
                zf.write(os.path.join(seed_dir, name), arcname=name)
        shutil.copy(local_zip, zip_path)  # sequential -> FUSE-safe
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    for name in components:
        os.remove(os.path.join(seed_dir, name))
    return zip_path


def _zip_gdb(gdb_path: str) -> str:
    """Zip seed.gdb/ into seed.gdb.zip such that the archive contains the seed.gdb/
    directory at its root (arcname = seed.gdb/<relpath>).  /vsizip/…/seed.gdb.zip then
    exposes the .gdb for OpenFileGDB.

    Built on driver-local disk then sequential-copied to the target (FUSE-safe -- see
    _zip_shapefile).  Removes the original .gdb directory after.  Returns the zip path.
    """
    gdb_name = os.path.basename(gdb_path.rstrip("/"))  # e.g. "seed.gdb"
    zip_path = gdb_path + ".zip"
    local_dir = tempfile.mkdtemp(prefix="gbx_zipgdb_")
    try:
        local_zip = os.path.join(local_dir, gdb_name + ".zip")
        with zipfile.ZipFile(local_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for dirpath, _dirnames, filenames in os.walk(gdb_path):
                for fname in filenames:
                    full = os.path.join(dirpath, fname)
                    zf.write(
                        full,
                        arcname=os.path.join(gdb_name, os.path.relpath(full, gdb_path)),
                    )
        shutil.copy(local_zip, zip_path)  # sequential -> FUSE-safe
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    shutil.rmtree(gdb_path)
    return zip_path


def transcode_vector_seed(spark, seed_df, formats: List[str], out_base: str) -> dict:
    """Write the seed DataFrame to each format's seed file via the *_gbx writers.
    Returns {fmt: seed_path}. The seed is cached so each write reuses it. FileGDB
    requires the native osgeo (heavyweight GDAL natives).

    Shapefile and FileGDB outputs are zipped into self-contained archives (.shp.zip
    and .gdb.zip respectively) so both the light and heavy readers can dir-read a
    directory of copies -- the heavy OGR dir-read needs each entry to be one file."""
    seed_df = seed_df.cache()
    seed_df.count()  # materialize the cache
    out: dict = {}
    for fmt in formats:
        ext = _EXT.get(fmt, "out")
        path = f"{out_base}/{fmt}/seed.{ext}"
        writer = seed_df.coalesce(1).write.format(fmt).mode("overwrite")
        if fmt in ("vector_gbx", "ogr_gbx"):
            writer = writer.option("driverName", "GeoJSON")
        writer.save(path)
        if fmt == "shapefile_gbx":
            seed_dir = os.path.dirname(path)
            path = _zip_shapefile(seed_dir, "seed")
        elif fmt == "file_gdb_gbx" and os.path.isdir(path):
            path = _zip_gdb(path)
        out[fmt] = path
    return out


def replicate_vector_seed(seed_path: str, n_copies: int, copies_dir: str) -> List[str]:
    """Copy the per-format seed n_copies times into copies_dir as copy_<i>.<ext>.
    Sequential copies (FUSE-safe). Returns the copy paths.

    Seeds for shapefile_gbx / file_gdb_gbx are single .shp.zip / .gdb.zip archives
    produced by transcode_vector_seed, so every format is a single file copy.  A bare
    .gdb directory (non-zipped) is accepted as a fallback and tree-copied."""
    os.makedirs(copies_dir, exist_ok=True)
    base = os.path.basename(seed_path.rstrip("/"))
    # Preserve the full extension after the first dot (e.g. "shp.zip", "gdb.zip",
    # "geojson", "gpkg") so copy_0.shp.zip / copy_0.gdb.zip are named correctly.
    dot = base.find(".")
    ext = base[dot + 1 :] if dot != -1 else ""
    paths: List[str] = []
    for i in range(n_copies):
        dst = os.path.join(copies_dir, f"copy_{i}.{ext}" if ext else f"copy_{i}")
        if os.path.isdir(seed_path):  # fallback: bare .gdb directory (non-zipped)
            shutil.copytree(seed_path, dst, dirs_exist_ok=True)
        else:
            shutil.copy(seed_path, dst)
        paths.append(dst)
    return paths


# ---------------------------------------------------------------------------------------------
# TIN + legacy corpus builders (light pyvx vs heavy vectorx bench).  Plain-Python, deterministic.
# Each returns (rows, schema) where ``rows`` is a list of tuples and ``schema`` is a DDL string
# suitable for ``spark.createDataFrame(rows, schema)``.  The TIN builders share a base point/
# breakline layout and add the per-function grid columns so all three TIN functions can be timed
# from one corpus shape.
# ---------------------------------------------------------------------------------------------

# A 7-point set in general position (no four cocircular) so the Delaunay triangulation is ~unique.
# Mirrors test_parity_tin._GENERAL_PTS.  Tiled + offset per row to scale the corpus.
_GENERAL_PTS = [
    (0.0, 0.0, 0.0),
    (10.0, 0.0, 5.0),
    (10.0, 10.0, 12.0),
    (0.0, 10.0, 7.0),
    (3.0, 4.0, 3.0),
    (7.0, 2.0, 6.0),
    (4.0, 8.0, 9.0),
]


def _tin_row_points(row_idx: int, n_points: int):
    """Deterministic Z-valued points for one row, in general position.

    Starts from the 7-point general-position set and, if ``n_points`` > 7, adds
    further pseudo-random-but-deterministic interior points (kept inside the
    [0,10] x [0,10] square so they all fall in the hull).  A per-row Z offset
    keeps each row's surface distinct without changing the planar layout (so the
    Delaunay partition -- and thus light-vs-heavy parity -- is stable)."""
    z_off = float(row_idx)
    pts = [(x, y, z + z_off) for (x, y, z) in _GENERAL_PTS]
    extra = max(0, n_points - len(_GENERAL_PTS))
    for k in range(extra):
        # Deterministic interior coordinates avoiding cocircular degeneracy: irrational
        # multipliers spread points without lining up on a circle.
        t = row_idx * 31 + k * 17 + 1
        x = 0.5 + (t * 0.6180339887 % 1.0) * 9.0
        y = 0.5 + (t * 0.7548776662 % 1.0) * 9.0
        z = ((t * 0.4142135623) % 1.0) * 10.0 + z_off
        pts.append((x, y, z))
    return pts


def generate_tin_points(n_rows: int, n_points: int = 25, with_breaklines: bool = False):
    """Build the shared TIN corpus covering all three TIN functions.

    Returns ``(rows, schema)`` where each row carries, in order:

      * ``pts``    ARRAY<BINARY>  -- WKB Z-valued points (general position)
      * ``bl``     ARRAY<BINARY>  -- WKB LineString breaklines ([] unless requested)
      * ``mt``     DOUBLE         -- merge tolerance (0.0)
      * ``st``     DOUBLE         -- snap tolerance (0.0)
      * ``spf``    STRING         -- split-point finder ("NONENCROACHING")
      * ``xmin,ymin,xmax,ymax`` DOUBLE -- bbox grid extent (for interp bbox)
      * ``w,h``    INT            -- bbox grid columns/rows (for interp bbox)
      * ``srid``   INT            -- output SRID (for interp bbox)
      * ``origin`` BINARY         -- WKB POINT grid origin (for interp geom)
      * ``cols,rows_n`` INT       -- origin-grid columns/rows (for interp geom)
      * ``cell_x,cell_y`` DOUBLE  -- origin-grid per-cell size (for interp geom)

    Deterministic: row ``i`` always produces the same bytes for a given
    ``(n_points, with_breaklines)``.  Points sit in [0,10] x [0,10]; the grids
    cover that square so cells fall inside the TIN hull.
    """
    from shapely import to_wkb
    from shapely.geometry import LineString, Point

    rows = []
    for i in range(n_rows):
        coords = _tin_row_points(i, n_points)
        pts = [bytearray(to_wkb(Point(*c))) for c in coords]
        if with_breaklines:
            bl = [bytearray(to_wkb(LineString([(0.0, 0.0), (10.0, 10.0)])))]
        else:
            bl = []
        origin = bytearray(to_wkb(Point(0.0, 10.0)))
        rows.append(
            (
                pts,
                bl,
                0.0,  # mt
                0.0,  # st
                "NONENCROACHING",  # spf
                0.0,  # xmin
                0.0,  # ymin
                10.0,  # xmax
                10.0,  # ymax
                7,  # w
                7,  # h
                0,  # srid
                origin,  # grid origin (interp geom)
                5,  # cols
                5,  # rows_n
                2.0,  # cell_x
                -2.0,  # cell_y  (origin is top-left; step down)
            )
        )
    schema = (
        "pts array<binary>, bl array<binary>, mt double, st double, spf string, "
        "xmin double, ymin double, xmax double, ymax double, w int, h int, srid int, "
        "origin binary, cols int, rows_n int, cell_x double, cell_y double"
    )
    return rows, schema


def generate_legacy_structs(n_rows: int):
    """Build ``n_rows`` legacy-geometry structs (polygon-with-hole-and-Z, deterministic).

    Returns ``(rows, schema)``.  Each row is a single-field tuple holding the
    legacy Mosaic geometry struct (typeId 5 = polygon).  Mirrors the polygon-
    with-hole-and-Z shape from test_parity_legacy, offset per row so each is
    distinct while staying a valid polygon with one interior ring."""
    schema = (
        "g struct<typeId:int,srid:int,"
        "boundaries:array<array<array<double>>>,"
        "holes:array<array<array<array<double>>>>>"
    )
    rows = []
    for i in range(n_rows):
        o = float(i) * 0.5  # per-row planar offset
        z = 1.0 + float(i)
        outer = [
            [o + 0.0, o + 0.0, z],
            [o + 10.0, o + 0.0, z],
            [o + 10.0, o + 10.0, z],
            [o + 0.0, o + 10.0, z],
            [o + 0.0, o + 0.0, z],
        ]
        hole = [
            [o + 2.0, o + 2.0, z],
            [o + 4.0, o + 2.0, z],
            [o + 4.0, o + 4.0, z],
            [o + 2.0, o + 4.0, z],
            [o + 2.0, o + 2.0, z],
        ]
        rows.append(
            (
                {
                    "typeId": 5,
                    "srid": 0,
                    "boundaries": [outer],
                    "holes": [[hole]],
                },
            )
        )
    return rows, schema


# ---------------------------------------------------------------------------------------------
# Quadbin (light pygx vs heavy gridx.quadbin) corpus builders.  Plain-Python, deterministic.
# Each returns (rows, schema) for ``spark.createDataFrame(rows, schema)``.  Shapes mirror the
# benched quadbin functions:
#   * points        -> quadbin_pointascell (scalar lon/lat -> cell)
#   * polygons WKT  -> quadbin_polyfill (geom -> ARRAY<cell>) and quadbin_tessellate (struct-array)
#   * cell-id arrays + group keys -> quadbin_cellunion_agg (grouped aggregate)
#   * single cell ids -> quadbin_resolution / kring / aswkb / centroid (scalar cell-in)
#   * cell-id pairs   -> quadbin_distance (scalar two-cell-in)
#   * cell-id arrays  -> quadbin_cellunion (scalar ARRAY<cell> -> EWKB; reuses cellid_arrays)
# Coordinates stay well inside the WebMercator-valid band (|lat| < 85) so both tiers agree.
# ---------------------------------------------------------------------------------------------


def generate_quadbin_points(n_rows: int):
    """``n_rows`` deterministic WGS84 (lon, lat) points for quadbin_pointascell.

    Returns ``(rows, schema)`` where each row is ``(lon, lat)``.  Points are
    spread pseudo-randomly-but-deterministically over the WebMercator-valid band
    (|lat| < 85) so row ``i`` always yields the same cell in both tiers."""
    rows = []
    for i in range(n_rows):
        lon = (i * 73 % 35900) / 100.0 - 179.0  # [-179, 179]
        lat = (i * 37 % 16800) / 100.0 - 84.0  # [-84, 84]  (inside |lat| < 85)
        rows.append((float(lon), float(lat)))
    return rows, "lon double, lat double"


def generate_quadbin_polygons(n_rows: int):
    """``n_rows`` deterministic WKT polygons for quadbin_polyfill / quadbin_tessellate.

    Returns ``(rows, schema)`` where each row is a single-field tuple holding a
    WKT polygon string.  Each polygon is a small axis-aligned box at a
    deterministic pseudo-random lon/lat (inside |lat| < 85), sized so polyfill
    yields a handful of cells -- enough to exercise the bbox enumeration + the
    per-cell intersect (tessellate) without ballooning the cell count."""
    rows = []
    for i in range(n_rows):
        lon = (i * 73 % 35000) / 100.0 - 175.0  # leave room for +d
        lat = (i * 37 % 16000) / 100.0 - 80.0
        d = 0.5  # ~0.5 deg box -> several cells at the benched resolution
        wkt = (
            f"POLYGON(({lon} {lat}, {lon + d} {lat}, "
            f"{lon + d} {lat + d}, {lon} {lat + d}, {lon} {lat}))"
        )
        rows.append((wkt,))
    return rows, "geom string"


def generate_quadbin_cellid_arrays(n_rows: int, res: int = 8):
    """``n_rows`` (group_key, cell BIGINT) rows for the quadbin_cellunion_agg grouped agg.

    Returns ``(rows, schema)``.  Each row carries a ``group`` key plus a single
    quadbin ``cell`` id (one cell per row, streamed into the aggregator).  Cells
    are computed by pure-Python quadbin math (matching both tiers) from a
    deterministic lon/lat at ``res``; rows are distributed across a small number
    of groups so the grouped aggregate produces several unions.  Cells within a
    group are spatially adjacent (a k=1 footprint walked deterministically) so
    each group's union is a contiguous coverage, not scattered specks."""
    from databricks.labs.gbx.pygx import _quadbin as _qb

    n_groups = max(1, min(8, n_rows))
    rows = []
    for i in range(n_rows):
        g = i % n_groups
        # Deterministic center per (group); offset cells within the group by a
        # small lon/lat step so a group spans a contiguous patch of cells.
        base_lon = (g * 41 % 340) - 170.0
        base_lat = (g * 23 % 160) - 80.0
        step = (i // n_groups) + 1
        lon = base_lon + (step % 5) * 0.5
        lat = base_lat + ((step // 5) % 5) * 0.5
        cell = _qb.point_as_cell(float(lon), float(lat), int(res))
        rows.append((int(g), int(cell)))
    return rows, "group int, cell bigint"


def generate_quadbin_cells(n_rows: int, res: int = 12):
    """``n_rows`` deterministic single quadbin ``cell`` ids for the scalar legs.

    Returns ``(rows, schema)`` where each row is a one-field tuple ``(cell,)``
    holding a BIGINT quadbin cell id.  Cells are computed by the pure-Python
    ``_quadbin.point_as_cell`` over a deterministic lon/lat sweep at a fixed
    ``res`` (default 12), so light and heavy see the SAME input cells (cell-id
    math is identical to both tiers' ``point_as_cell``).  Feeds the scalar
    cell-in legs: ``resolution``, ``kring``, ``aswkb``, ``centroid``.  Points
    stay inside the WebMercator-valid band (|lat| < 85) so every row yields a
    well-defined cell."""
    from databricks.labs.gbx.pygx import _quadbin as _qb

    rows = []
    for i in range(n_rows):
        lon = (i * 73 % 35900) / 100.0 - 179.0  # [-179, 179]
        lat = (i * 37 % 16800) / 100.0 - 84.0  # [-84, 84]  (inside |lat| < 85)
        cell = _qb.point_as_cell(float(lon), float(lat), int(res))
        rows.append((int(cell),))
    return rows, "cell bigint"


def generate_quadbin_cell_pairs(n_rows: int, res: int = 12):
    """``n_rows`` deterministic (cell_a, cell_b) BIGINT pairs for quadbin_distance.

    Returns ``(rows, schema)`` where each row is ``(cell_a, cell_b)`` -- two
    quadbin cell ids at the SAME ``res`` (a precondition of quadbin_distance).
    ``cell_a`` is the deterministic sweep cell (same as ``generate_quadbin_cells``);
    ``cell_b`` is a second cell a small, deterministic lon/lat step away (so the
    two are at the same resolution but generally a non-zero chessboard distance
    apart).  Cells are computed by pure-Python ``_quadbin.point_as_cell`` so both
    tiers see identical inputs."""
    from databricks.labs.gbx.pygx import _quadbin as _qb

    rows = []
    for i in range(n_rows):
        lon = (i * 73 % 35900) / 100.0 - 179.0  # [-179, 179]
        lat = (i * 37 % 16800) / 100.0 - 84.0  # [-84, 84]
        # Second point a deterministic small step away, kept inside the band.
        lon_b = lon + ((i % 5) + 1) * 0.05
        lat_b = lat + ((i % 3) + 1) * 0.05
        if lon_b > 179.0:
            lon_b = lon - 0.25
        if lat_b > 84.0:
            lat_b = lat - 0.25
        cell_a = _qb.point_as_cell(float(lon), float(lat), int(res))
        cell_b = _qb.point_as_cell(float(lon_b), float(lat_b), int(res))
        rows.append((int(cell_a), int(cell_b)))
    return rows, "cell_a bigint, cell_b bigint"


# ---------------------------------------------------------------------------------------------
# BNG (light pygx vs heavy gridx.bng) corpus builders.  Plain-Python, deterministic.  Mirrors
# the quadbin builders above but in EPSG:27700 (British National Grid eastings/northings, NOT
# WGS84 lon/lat) and STRING cell ids (not BIGINT).  Each returns (rows, schema).  Shapes mirror
# the benched BNG functions:
#   * WKT points   -> bng_pointascell (geom centroid -> STRING cell)
#   * WKT polygons -> bng_polyfill (geom -> ARRAY<STRING>) and bng_tessellate (chip struct-array)
#   * single cells -> bng_kring (STRING cell-in -> ARRAY<STRING>)
#   * (group, chip STRUCT) -> bng_cellunion_agg (grouped aggregate over chip structs)
# Coordinates stay well inside the BNG-valid land extent (the GB land mass, roughly
# easting [0, 700000], northing [0, 1300000]); a London anchor (e=530000, n=180000) keeps every
# generated point on valid BNG land so both tiers agree.  Cells are computed by pure-Python
# ``_bng`` so light and heavy consume identical inputs.  Resolutions are passed as the canonical
# string keys ("1km", "100m", ...) -- NEVER metres-as-Int -- per the BNG resolution convention.
# ---------------------------------------------------------------------------------------------

# London anchor in EPSG:27700 (easting, northing) -- on valid BNG land.
_BNG_ANCHOR_E = 530000.0
_BNG_ANCHOR_N = 180000.0


def generate_bng_points(n_rows: int):
    """``n_rows`` deterministic EPSG:27700 WKT points for bng_pointascell.

    Returns ``(rows, schema)`` where each row is a one-field tuple ``(geom,)``
    holding a WKT ``POINT(e n)`` in British National Grid eastings/northings
    (NOT WGS84).  Points are spread deterministically over a ~50km x 50km patch
    around the London anchor so they stay on valid BNG land and row ``i`` always
    yields the same cell in both tiers."""
    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 500) * 100.0  # +[0, 50000) m
        n = _BNG_ANCHOR_N + (i * 37 % 500) * 100.0
        rows.append((f"POINT({e} {n})",))
    return rows, "geom string"


def generate_bng_polygons(n_rows: int):
    """``n_rows`` deterministic EPSG:27700 WKT polygons for bng_polyfill / bng_tessellate.

    Returns ``(rows, schema)`` where each row is a one-field tuple holding a WKT
    polygon string in BNG eastings/northings.  Each polygon is a small
    axis-aligned box (~2.5km) at a deterministic offset from the London anchor,
    sized so polyfill at "1km" yields a handful of cells -- enough to exercise
    the bbox enumeration + per-cell intersect (tessellate) without ballooning."""
    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 400) * 100.0  # +[0, 40000) m, leaves room for +d
        n = _BNG_ANCHOR_N + (i * 37 % 400) * 100.0
        d = 2500.0  # ~2.5km box -> several 1km cells
        wkt = (
            f"POLYGON(({e} {n}, {e + d} {n}, "
            f"{e + d} {n + d}, {e} {n + d}, {e} {n}))"
        )
        rows.append((wkt,))
    return rows, "geom string"


def generate_bng_cells(n_rows: int, res="1km"):
    """``n_rows`` deterministic single BNG ``cell`` id STRINGs for the scalar legs.

    Returns ``(rows, schema)`` where each row is a one-field tuple ``(cell,)``
    holding a STRING BNG cell id.  Cells are computed by pure-Python
    ``_bng.point_as_cell`` over a deterministic easting/northing sweep around the
    London anchor at ``res`` (a string resolution key, default "1km"), so light
    and heavy see the SAME input cells.  Feeds the scalar cell-in legs (kring)."""
    from databricks.labs.gbx.pygx import _bng

    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 500) * 100.0
        n = _BNG_ANCHOR_N + (i * 37 % 500) * 100.0
        cell = _bng.point_as_cell(float(e), float(n), res)
        rows.append((str(cell),))
    return rows, "cell string"


def generate_bng_chip_groups(n_rows: int, res="1km"):
    """``n_rows`` (group, chip STRUCT) rows for the bng_cellunion_agg grouped agg.

    Returns ``(rows, schema)``.  Each row carries a ``group`` key plus a single
    BNG chip ``STRUCT<cellid STRING, core BOOLEAN, chip BINARY>`` (one chip per
    row, streamed into the aggregator).  Both tiers' cellunion_agg consume the
    same chip struct shape; the chip geometry is the full cell polygon (a core
    chip materialized to WKB) for the cell at a deterministic easting/northing.
    Rows are distributed across a small number of groups. `cellunion_agg` is a
    SAME-CELL union (heavy enforces one cell id per aggregate), so every chip in
    a group is the SAME cell — the group key maps 1:1 to a distinct cell, and the
    group's union is that cell's polygon. (A multi-cell group is outside the
    function's contract and the two tiers legitimately diverge there, so we don't
    benchmark it.)"""
    from shapely import to_wkb as _to_wkb

    from databricks.labs.gbx.pygx import _bng

    n_groups = max(1, min(8, n_rows))
    rows = []
    for i in range(n_rows):
        g = i % n_groups
        # One distinct cell per group; ALL chips in a group share that cell, so
        # the grouped aggregate performs a same-cell union (its contract).
        e = _BNG_ANCHOR_E + (g * 7000.0)
        n = _BNG_ANCHOR_N + (g * 5000.0)
        cellid = _bng.point_as_cell(float(e), float(n), res)
        chip = _to_wkb(_bng.cell_id_to_geometry(_bng.parse(cellid)))
        rows.append((int(g), (str(cellid), False, chip)))
    schema = "group int, chip struct<cellid:string,core:boolean,chip:binary>"
    return rows, schema


def generate_bng_cell_pairs(n_rows: int, res="1km"):
    """``n_rows`` deterministic (cell_a, cell_b) STRING pairs for the two-arg legs.

    Returns ``(rows, schema)`` where each row is ``(cell_a, cell_b)`` -- two BNG
    cell ids at the SAME ``res`` (a precondition of bng_distance /
    bng_euclideandistance).  ``cell_a`` is the deterministic sweep cell (same as
    ``generate_bng_cells``); ``cell_b`` is a second cell a small, deterministic
    easting/northing step away (so the two are at the same resolution but
    generally a non-zero grid distance apart).  Cells are computed by pure-Python
    ``_bng.point_as_cell`` over EPSG:27700 eastings/northings around the London
    anchor so both tiers see identical inputs and stay on valid BNG land."""
    from databricks.labs.gbx.pygx import _bng

    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 500) * 100.0
        n = _BNG_ANCHOR_N + (i * 37 % 500) * 100.0
        # Second cell a deterministic small step away (+[100, 500]m east, +[100,
        # 300]m north), kept on valid BNG land near the London anchor.
        e_b = e + ((i % 5) + 1) * 100.0
        n_b = n + ((i % 3) + 1) * 100.0
        cell_a = _bng.point_as_cell(float(e), float(n), res)
        cell_b = _bng.point_as_cell(float(e_b), float(n_b), res)
        rows.append((str(cell_a), str(cell_b)))
    return rows, "cell_a string, cell_b string"


def generate_bng_eastnorth(n_rows: int):
    """``n_rows`` deterministic (e, n) DOUBLE pairs for bng_eastnorthasbng.

    Returns ``(rows, schema)`` where each row is ``(e, n)`` -- two EPSG:27700
    eastings/northings (DOUBLE, BNG, NOT WGS84).  Mirrors ``generate_bng_points``
    but emits two doubles instead of a WKT string, sweeping deterministically over
    a ~50km x 50km patch around the London anchor so every row stays on valid BNG
    land and yields the same cell in both tiers."""
    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 500) * 100.0
        n = _BNG_ANCHOR_N + (i * 37 % 500) * 100.0
        rows.append((float(e), float(n)))
    return rows, "e double, n double"


def generate_bng_chip_pairs(n_rows: int, res="1km"):
    """``n_rows`` (lid, lcore, lchip, rid, rcore, rchip) rows for the chip-struct legs.

    Returns ``(rows, schema)`` for bng_cellintersection / bng_cellunion, which
    take TWO chip ``STRUCT<cellid, core, chip>`` args.  Each row holds two chip
    structs over the SAME cell id (a core chip and a full-cell chip), so union /
    intersection both produce the whole cell.  The chip geometry is the full cell
    polygon materialized to WKB (``_to_wkb(_bng.cell_id_to_geometry(parse(cellid)))``)
    for a deterministic cell swept around the London anchor at ``res`` (a string
    resolution key), so light and heavy consume identical inputs.  Mirrors the
    ``chip_row`` fixture in the BNG parity test: ``(seed, True, chip, seed, False,
    chip)``."""
    from shapely import to_wkb as _to_wkb

    from databricks.labs.gbx.pygx import _bng

    rows = []
    for i in range(n_rows):
        e = _BNG_ANCHOR_E + (i * 73 % 500) * 100.0
        n = _BNG_ANCHOR_N + (i * 37 % 500) * 100.0
        cellid = _bng.point_as_cell(float(e), float(n), res)
        chip = _to_wkb(_bng.cell_id_to_geometry(_bng.parse(cellid)))
        # Same cellid both sides: a core chip + a full-cell chip -> union /
        # intersection over the same cell yield the whole cell polygon.
        rows.append((str(cellid), True, chip, str(cellid), False, chip))
    schema = (
        "lid string, lcore boolean, lchip binary, "
        "rid string, rcore boolean, rchip binary"
    )
    return rows, schema


# ---------------------------------------------------------------------------------------------
# Custom-grid corpus generators (gbx_custom_* light pygx vs heavy gridx.custom).
#
# All legs share ONE fixed grid spec -- the doc SQL example grid:
#   gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700)
# i.e. a 0..1,000,000 extent, cell_splits=2, 1000m root cells (1000x1000 of them at
# res 0). Coordinates are arbitrary planar units in the grid CRS (here labelled
# EPSG:27700 via srid, but the srid is METADATA only -- output geometry is plain WKB,
# no SRID). pointascell uses the geometry's FIRST coordinate (heavy geom.getCoordinate),
# NOT the centroid (unlike BNG), so the points/polygons are placed so their first
# vertex falls strictly inside [0, 1000000) at the chosen resolution.
# ---------------------------------------------------------------------------------------------

# The fixed custom grid both tiers build via gbx_custom_grid(...). Shared by every
# run_custom_* leg and the parity gate so light and heavy consume the SAME struct.
CUSTOM_GRID_ARGS = (0, 1_000_000, 0, 1_000_000, 2, 1000, 1000, 27700)
CUSTOM_GRID_SQL = "gbx_custom_grid(%d, %d, %d, %d, %d, %d, %d, %d)" % CUSTOM_GRID_ARGS

# Anchor well inside the extent (mirrors the parity test's _PX/_PY) so the swept
# corpus stays in-bounds at res 0 (1000m cells).
_CUSTOM_ANCHOR_X = 530000.0
_CUSTOM_ANCHOR_Y = 180000.0


def _custom_conf():
    """The shared CustomGridConf (pure-Python) for cell-id corpus generation."""
    from databricks.labs.gbx.pygx import _custom

    x_min, x_max, y_min, y_max, splits, rx, ry, srid = CUSTOM_GRID_ARGS
    return _custom.CustomGridConf(
        bound_x_min=x_min,
        bound_x_max=x_max,
        bound_y_min=y_min,
        bound_y_max=y_max,
        cell_splits=splits,
        root_cell_size_x=rx,
        root_cell_size_y=ry,
        srid=srid,
    )


def generate_custom_points(n_rows: int):
    """``n_rows`` deterministic WKT points (in the custom grid CRS) for custom_pointascell.

    Returns ``(rows, schema)`` where each row is a one-field tuple ``(geom,)`` holding a
    WKT ``POINT(x y)`` in the grid's planar units.  Points sweep a ~50km x 50km patch
    around the anchor, all strictly inside ``[0, 1000000)`` so the FIRST coordinate maps
    to a valid cell at res 0.  Row ``i`` always yields the same cell in both tiers."""
    rows = []
    for i in range(n_rows):
        x = _CUSTOM_ANCHOR_X + (i * 73 % 500) * 100.0  # +[0, 50000)
        y = _CUSTOM_ANCHOR_Y + (i * 37 % 500) * 100.0
        rows.append((f"POINT({x} {y})",))
    return rows, "geom string"


def generate_custom_polygons(n_rows: int):
    """``n_rows`` deterministic WKT polygons (custom grid CRS) for custom_polyfill.

    Returns ``(rows, schema)`` where each row is a one-field tuple holding a WKT polygon.
    Each polygon is a small axis-aligned ~3km box at a deterministic offset from the
    anchor, sized so polyfill at res 0 (1000m cells) yields a handful of cells via the
    centroid-containment filter -- enough to exercise the bbox over-scan + per-cell test
    without ballooning.  All vertices stay inside ``[0, 1000000)``."""
    rows = []
    for i in range(n_rows):
        x = _CUSTOM_ANCHOR_X + (i * 73 % 400) * 100.0  # +[0, 40000), leaves room for +d
        y = _CUSTOM_ANCHOR_Y + (i * 37 % 400) * 100.0
        d = 3000.0  # ~3km box -> several 1000m cells by centroid containment
        wkt = (
            f"POLYGON(({x} {y}, {x + d} {y}, "
            f"{x + d} {y + d}, {x} {y + d}, {x} {y}))"
        )
        rows.append((wkt,))
    return rows, "geom string"


def generate_custom_cells(n_rows: int, res: int = 0):
    """``n_rows`` deterministic single custom-grid ``cell`` id BIGINTs for the scalar legs.

    Returns ``(rows, schema)`` where each row is a one-field tuple ``(cell,)`` holding a
    BIGINT custom-grid cell id.  Cells are computed by the pure-Python
    ``_custom.point_to_cell_id`` over a deterministic x/y sweep around the anchor at
    ``res`` (an INT resolution, default 0), so light and heavy see the SAME input cells.
    Feeds cellaswkb / cellaswkt / centroid / kring."""
    from databricks.labs.gbx.pygx import _custom

    conf = _custom_conf()
    rows = []
    for i in range(n_rows):
        x = _CUSTOM_ANCHOR_X + (i * 73 % 500) * 100.0
        y = _CUSTOM_ANCHOR_Y + (i * 37 % 500) * 100.0
        cell = _custom.point_to_cell_id(conf, float(x), float(y), res)
        rows.append((int(cell),))
    return rows, "cell long"


def build_vector_corpus(
    spark, rows: int, copies: int, formats: List[str], out_base: str, srid: str = "4326"
) -> dict:
    """Full pipeline: generate the polygon seed -> transcode to each format ->
    replicate ×copies. Returns {fmt: {"seed": path, "copies": [paths]}}."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    seed_df = generate_polygon_seed(spark, rows, srid=srid)
    seeds = transcode_vector_seed(spark, seed_df, formats, out_base)
    result: dict = {}
    for fmt, seed_path in seeds.items():
        copies_dir = f"{out_base}/{fmt}/copies"
        result[fmt] = {
            "seed": seed_path,
            "copies": replicate_vector_seed(seed_path, copies, copies_dir),
        }
    return result


def stage_gpkg_bench_corpus(
    spark,
    out_base: str,
    *,
    rows: int = 100000,
    copies_ladder=(10, 80, 160),
    srid: str = "4326",
) -> dict:
    """Stage GeoPackage bench corpora sized on the ROW-per-file axis: one copies-dir
    per ``copies`` value, each holding ``copies`` .gpkg files of ``rows`` features.

    ``rows`` must exceed the largest chunkSize in the sweep (>=100k) so a 100k chunk
    still yields multiple chunks/file (chunkSize x LRU visible). ``copies_ladder``
    brackets the cluster slot count (below/at/above ~80) so the fanout axis
    (files-vs-slots) separates from the amortization axis (rows/file). Reuses
    ``build_vector_corpus`` (formats=['gpkg_gbx']) once per copies value, into a
    distinct sub-base so the copies-dirs don't collide.

    Returns ``{copies: copies_dir}`` where each ``copies_dir`` holds ``copies``
    ``.gpkg`` files of ``rows`` features."""
    out: dict = {}
    for c in copies_ladder:
        sub = f"{out_base}/copies_{c}"
        built = build_vector_corpus(
            spark, rows=rows, copies=c, formats=["gpkg_gbx"], out_base=sub, srid=srid
        )
        assert built["gpkg_gbx"]["copies"]  # non-empty
        out[c] = f"{sub}/gpkg_gbx/copies"
    return out
