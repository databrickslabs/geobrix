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

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import Optional, Union  # noqa: F401

from pyspark.sql import Column, DataFrame, SparkSession  # noqa: F401
from pyspark.sql import functions as f

from databricks.labs.gbx import _api  # noqa: F401

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


def quadbin_cellfill(
    cellid: ColLike,
    value: ColLike,
    k: ColLike = 1,
    method: ColLike = "mean",
    power: ColLike = 2.0,
) -> Column:
    """Grouped aggregator: fill NULL quadbin cells from valid neighbours.

    Use with ``groupBy(...).agg(qx.quadbin_cellfill(...))`` to interpolate missing
    values. Returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.

    Args:
        cellid: Column of BIGINT quadbin cell ids.
        value: Column of DOUBLE values (NULL marks cells to be filled).
        k: Neighbour ring radius (default ``1``).
        method: Interpolation method — ``'mean'`` (default) or ``'idw'``.
        power: IDW power parameter (default ``2.0``; ignored for ``'mean'``).

    Returns:
        Column of ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    """
    _method = f.lit(method) if isinstance(method, str) else _col(method)
    return f.call_function(
        "gbx_quadbin_cellfill",
        _col(cellid),
        _col(value),
        _col(k),
        _method,
        _col(power),
    )


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


def quadbin_geomkringexplode(
    geom: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = "boundary-out",
    coverage: Optional[ColLike] = None,
) -> Column:
    """Generator: explodes geometry-aware k-ring for quadbin into one row per cell.

    Returns all cells reachable from the geometry's covering set in at most ``k``
    Chebyshev steps, yielding one row per cell with schema ``STRUCT<cellid BIGINT>``.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Quadbin zoom level, integer in ``[0, 20]``.
        k: Ring distance (0 = covering set only).
        mode: Dilation mode (default ``"boundary-out"``).
        coverage: Coverage mode (default ``None``; uses Scala default).

    Returns:
        Generator Column yielding one row per cell.
    """
    mode_arg = mode if isinstance(mode, Column) else f.lit(mode)
    if coverage is None:
        return f.call_function(
            "gbx_quadbin_geomkringexplode",
            _col(geom),
            _col(resolution),
            _col(k),
            mode_arg,
        )
    coverage_arg = coverage if isinstance(coverage, Column) else f.lit(coverage)
    return f.call_function(
        "gbx_quadbin_geomkringexplode",
        _col(geom),
        _col(resolution),
        _col(k),
        mode_arg,
        coverage_arg,
    )


def quadbin_geomkloopexplode(
    geom: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = "boundary-out",
    coverage: Optional[ColLike] = None,
) -> Column:
    """Generator: explodes geometry-aware k-loop (hollow ring) for quadbin into one row per cell.

    Returns cells at EXACTLY ``k`` Chebyshev steps from the geometry's covering set,
    yielding one row per cell with schema ``STRUCT<cellid BIGINT>``.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Quadbin zoom level, integer in ``[0, 20]``.
        k: Ring distance.
        mode: Dilation mode (default ``"boundary-out"``). See :func:`quadbin_geomkring`.
        coverage: Coverage mode (default ``None``; uses Scala default).

    Returns:
        Generator Column yielding one row per cell.
    """
    mode_arg = mode if isinstance(mode, Column) else f.lit(mode)
    if coverage is None:
        return f.call_function(
            "gbx_quadbin_geomkloopexplode",
            _col(geom),
            _col(resolution),
            _col(k),
            mode_arg,
        )
    coverage_arg = coverage if isinstance(coverage, Column) else f.lit(coverage)
    return f.call_function(
        "gbx_quadbin_geomkloopexplode",
        _col(geom),
        _col(resolution),
        _col(k),
        mode_arg,
        coverage_arg,
    )


# BEGIN GENERATED by gbx:manifest:gen: edit manifest/functions.json, then run the generator


def quadbin_aswkb(
    cell: ColLike,
) -> Column:
    """Return the footprint of a quadbin cell as an EWKB polygon in EPSG:4326.

    Args:
        cell: The quadbin cell id.

    Returns:
        The cell polygon, or NULL for a NULL cell.

    SQL: ``gbx_quadbin_aswkb(cell)``
    """
    return _api.call(
        "gbx_quadbin_aswkb",
        cell,
    )


def quadbin_cellunion(
    cells: ColLike,
) -> Column:
    """Return the union of an array of quadbin cells as an EWKB polygon in EPSG:4326.

    Args:
        cells: The quadbin cell ids to merge.

    Returns:
        The merged polygon or multipolygon, or NULL for a NULL or empty array.

    SQL: ``gbx_quadbin_cellunion(cells)``
    """
    return _api.call(
        "gbx_quadbin_cellunion",
        cells,
    )


def quadbin_centroid(
    cell: ColLike,
) -> Column:
    """Return the centre of a quadbin cell as an EWKB point in EPSG:4326.

    Args:
        cell: The quadbin cell id.

    Returns:
        The cell centre, the middle of its longitude and latitude range, or NULL for a
        NULL cell.

    SQL: ``gbx_quadbin_centroid(cell)``
    """
    return _api.call(
        "gbx_quadbin_centroid",
        cell,
    )


def quadbin_distance(
    cellid1: ColLike,
    cellid2: ColLike,
) -> Column:
    """Return the distance in cells between two quadbin cells of the same zoom level.

    Args:
        cellid1: The first quadbin cell id.
        cellid2: The second quadbin cell id, at the same zoom level.

    Returns:
        The number of cell steps (a diagonal step counts as one), or NULL when either
        cell is NULL. Cells of different zoom levels raise an error.

    SQL: ``gbx_quadbin_distance(cellid1, cellid2)``
    """
    return _api.call(
        "gbx_quadbin_distance",
        cellid1,
        cellid2,
    )


def quadbin_geomkloop(
    geom: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = None,
    coverage: ColLike = None,
) -> Column:
    """Return the quadbin cells at exactly k rings from a geometry.

    Args:
        geom: The geometry: WKB, EWKB, WKT or EWKT, in EPSG:4326 degrees.
        resolution: Quadbin zoom level, a whole number from 0 to 20.
        k: The loop distance: only the cells at exactly this ring distance from the
            covering cells are returned.
        mode: The dilation mode: `'boundary-out'`, `'boundary-in'`,
            `'boundary-in-ignore-holes'`, `'hole-in'`, `'hole-out'` or
            `'hole-out-ignore-geom'`. SQL default: ``'boundary-out'``.
        coverage: How a cell belongs to the geometry: `'coveras'` (it overlaps),
            `'polyfill'` (its centre is inside) or `'core'` (it is inside). SQL default:
            ``'coveras'``.

    Returns:
        The cell ids of the hollow band at distance k, or NULL for a NULL or unreadable
        geometry.

    SQL: ``gbx_quadbin_geomkloop(geom, resolution, k, [mode], [coverage])``
    """
    return _api.call(
        "gbx_quadbin_geomkloop",
        geom,
        resolution,
        k,
        mode,
        coverage,
    )


def quadbin_geomkring(
    geom: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = None,
    coverage: ColLike = None,
) -> Column:
    """Return the quadbin cells of a geometry dilated by k rings.

    Args:
        geom: The geometry: WKB, EWKB, WKT or EWKT, in EPSG:4326 degrees.
        resolution: Quadbin zoom level, a whole number from 0 to 20.
        k: The ring distance: 0 is the covering cells alone, 1 adds the cells around
            them.
        mode: The dilation mode: `'boundary-out'`, `'boundary-in'`,
            `'boundary-in-ignore-holes'`, `'hole-in'`, `'hole-out'` or
            `'hole-out-ignore-geom'`. SQL default: ``'boundary-out'``.
        coverage: How a cell belongs to the geometry: `'coveras'` (it overlaps),
            `'polyfill'` (its centre is inside) or `'core'` (it is inside). SQL default:
            ``'coveras'``.

    Returns:
        The cell ids of the dilated band, or NULL for a NULL or unreadable geometry.

    SQL: ``gbx_quadbin_geomkring(geom, resolution, k, [mode], [coverage])``
    """
    return _api.call(
        "gbx_quadbin_geomkring",
        geom,
        resolution,
        k,
        mode,
        coverage,
    )


def quadbin_kloop(
    cell: ColLike,
    k: ColLike,
) -> Column:
    """Return the quadbin cells at exactly ring distance k from a cell.

    Args:
        cell: The quadbin cell id.
        k: The loop distance: only the cells at exactly this distance are returned.

    Returns:
        The cell ids of the hollow square at distance k (the cell itself for k = 0), or
        NULL for a NULL argument.

    SQL: ``gbx_quadbin_kloop(cell, k)``
    """
    return _api.call(
        "gbx_quadbin_kloop",
        cell,
        k,
    )


def quadbin_kring(
    cell: ColLike,
    k: ColLike,
) -> Column:
    """Return the quadbin cells within ring distance k of a cell, the cell included.

    Args:
        cell: The quadbin cell id.
        k: The ring distance: 0 is the cell alone, 1 adds its eight neighbours.

    Returns:
        The cell ids of the filled square of side 2k+1 around the cell, or NULL for a
        NULL argument.

    SQL: ``gbx_quadbin_kring(cell, k)``
    """
    return _api.call(
        "gbx_quadbin_kring",
        cell,
        k,
    )


def quadbin_pointascell(
    longitude: ColLike,
    latitude: ColLike,
    resolution: ColLike,
) -> Column:
    """Return the quadbin cell that holds a longitude and latitude.

    Args:
        longitude: Longitude in EPSG:4326 degrees.
        latitude: Latitude in EPSG:4326 degrees.
        resolution: Quadbin zoom level, a whole number from 0 to 26.

    Returns:
        The cell id, or NULL when the longitude, latitude or resolution is NULL or not
        finite.

    SQL: ``gbx_quadbin_pointascell(longitude, latitude, resolution)``
    """
    return _api.call(
        "gbx_quadbin_pointascell",
        longitude,
        latitude,
        resolution,
    )


def quadbin_polyfill(
    geom: ColLike,
    resolution: ColLike,
) -> Column:
    """Return the quadbin cells that cover the bounding box of a geometry.

    Args:
        geom: The geometry: WKB, EWKB, WKT or EWKT, in EPSG:4326 degrees.
        resolution: Quadbin zoom level, a whole number from 0 to 20.

    Returns:
        The cell ids of every cell the bounding box touches, or NULL for a NULL or
        unreadable geometry.

    SQL: ``gbx_quadbin_polyfill(geom, resolution)``
    """
    return _api.call(
        "gbx_quadbin_polyfill",
        geom,
        resolution,
    )


def quadbin_resolution(
    cell: ColLike,
) -> Column:
    """Return the zoom level of a quadbin cell.

    Args:
        cell: The quadbin cell id.

    Returns:
        The zoom level, from 0 to 26, or NULL for a NULL cell.

    SQL: ``gbx_quadbin_resolution(cell)``
    """
    return _api.call(
        "gbx_quadbin_resolution",
        cell,
    )


def quadbin_tessellate(
    geom: ColLike,
    resolution: ColLike,
) -> Column:
    """Cut a geometry into quadbin cells, one chip per cell it touches.

    Args:
        geom: The geometry: WKB, EWKB, WKT or EWKT, in EPSG:4326 degrees.
        resolution: Quadbin zoom level, a whole number from 0 to 20.

    Returns:
        One struct per cell: `cell` and `geom`, the part of the geometry in that cell as
        EWKB in EPSG:4326, or NULL for a NULL or unreadable geometry.

    SQL: ``gbx_quadbin_tessellate(geom, resolution)``
    """
    return _api.call(
        "gbx_quadbin_tessellate",
        geom,
        resolution,
    )


# END GENERATED
