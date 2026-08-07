"""CARTO quadbin v0 Python API.

Thin wrappers around GeoBrix Scala functions (gbx_quadbin_*). Register with
``register(spark)`` then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_quadbin_<name>;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)`` — so you can write ``quadbin_pointascell(lon,
lat, 10)`` or ``quadbin_kring(cell, 1)`` instead of wrapping in ``f.lit``.
Strings and ``Column`` values pass through unchanged.
"""

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through."""
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def register(_spark: SparkSession) -> None:
    """Register Quadbin functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_quadbin_* SQL
    functions are available. Uses the active Spark session if not provided.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option(
        "functions", "gridx.quadbin"
    ).load().collect()


def quadbin_pointascell(longitude: ColLike, latitude: ColLike, resolution: ColLike) -> Column:
    """Encode (longitude, latitude) at a given zoom as a CARTO quadbin v0 cell (BIGINT).

    Args:
        longitude: Longitude in EPSG:4326 (degrees).
        latitude: Latitude in EPSG:4326 (degrees).
        resolution: Quadbin zoom level, integer in ``[0, 26]``.

    Returns:
        Column of BIGINT quadbin cell ids.
    """
    return f.call_function(
        "gbx_quadbin_pointascell", _col(longitude), _col(latitude), _col(resolution)
    )


def quadbin_aswkb(cell: ColLike) -> Column:
    """Return the quadbin cell footprint as an EWKB polygon (SRID=4326).

    Args:
        cell: Column of BIGINT quadbin cell ids.

    Returns:
        Column of EWKB bytes (polygon).
    """
    return f.call_function("gbx_quadbin_aswkb", _col(cell))


def quadbin_centroid(cell: ColLike) -> Column:
    """Return the quadbin cell centroid as an EWKB point (SRID=4326).

    Args:
        cell: Column of BIGINT quadbin cell ids.

    Returns:
        Column of EWKB bytes (point).
    """
    return f.call_function("gbx_quadbin_centroid", _col(cell))


def quadbin_resolution(cell: ColLike) -> Column:
    """Return the resolution (zoom level, 0..26) of a quadbin cell.

    Args:
        cell: Column of BIGINT quadbin cell ids.

    Returns:
        Column of INT resolutions.
    """
    return f.call_function("gbx_quadbin_resolution", _col(cell))


def quadbin_polyfill(geom: ColLike, resolution: ColLike) -> Column:
    """Return the quadbin cells covering the geometry's envelope at the given resolution.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Quadbin zoom level, integer in ``[0, 20]`` (cell-count guard).

    Returns:
        Column of ``ARRAY<BIGINT>`` quadbin cell ids.
    """
    return f.call_function("gbx_quadbin_polyfill", _col(geom), _col(resolution))


def quadbin_kring(cell: ColLike, k: ColLike) -> Column:
    """Return all quadbin cells within Chebyshev distance ``k`` of ``cell`` (inclusive).

    Args:
        cell: Column of BIGINT quadbin cell ids.
        k: Ring distance (0 = cell itself only).

    Returns:
        Column of ``ARRAY<BIGINT>`` quadbin cell ids.
    """
    return f.call_function("gbx_quadbin_kring", _col(cell), _col(k))


def quadbin_tessellate(geom: ColLike, resolution: ColLike) -> Column:
    """Tessellate a geometry into quadbin cells; returns ``ARRAY<struct(cell, geom)>``.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Quadbin zoom level, integer in ``[0, 20]``.

    Returns:
        Column of ``ARRAY<STRUCT<cell:BIGINT, geom:BINARY>>``.
    """
    return f.call_function("gbx_quadbin_tessellate", _col(geom), _col(resolution))


def quadbin_cellunion(cells: ColLike) -> Column:
    """Union an ARRAY of quadbin cells into a single MultiPolygon (EWKB SRID=4326).

    Args:
        cells: Column of ``ARRAY<BIGINT>`` quadbin cell ids.

    Returns:
        Column of EWKB bytes (Polygon or MultiPolygon).
    """
    return f.call_function("gbx_quadbin_cellunion", _col(cells))


def quadbin_distance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Chebyshev distance (in tile-grid steps) between two cells at the same resolution.

    Args:
        cellid1: First quadbin cell column.
        cellid2: Second quadbin cell column.

    Returns:
        Column of INT (cells must share resolution; otherwise the underlying eval throws).
    """
    return f.call_function("gbx_quadbin_distance", _col(cellid1), _col(cellid2))


def quadbin_cellunion_agg(cellid: ColLike) -> Column:
    """Aggregate quadbin cell BIGINTs into their union geometry (use with groupBy).

    Streams one cell id per row and returns the unioned MultiPolygon as EWKB
    (SRID=4326).  Parity with ``gbx_bng_cellunion_agg`` and Mosaic
    ``grid_cell_union_agg``.

    Args:
        cellid: BIGINT column of quadbin cell ids.

    Returns:
        Column of BINARY (EWKB Polygon or MultiPolygon, SRID 4326).
    """
    return f.call_function("gbx_quadbin_cellunion_agg", _col(cellid))
