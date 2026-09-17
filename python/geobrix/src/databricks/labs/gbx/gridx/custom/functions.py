"""GeoBrix custom grid Python API.

Thin wrappers around GeoBrix Scala functions (gbx_custom_*). Register with
``register(spark)`` then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_custom_<name>;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)`` — so you can write
``custom_grid(0, 100, 0, 100, 2, 10, 10)`` instead of wrapping in ``f.lit``.
Strings and ``Column`` values pass through unchanged.

Grid parameter types:
  All bounds and root cell sizes must be integers (INT or LONG) — the
  underlying Scala expression does not accept floating-point values.
"""

from typing import Optional, Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through."""
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def register(_spark: SparkSession) -> None:
    """Register custom grid functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_custom_* SQL
    functions are available. Uses the active Spark session if not provided.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option(
        "functions", "gridx.custom"
    ).load().collect()


def custom_grid(
    bound_x_min: ColLike,
    bound_x_max: ColLike,
    bound_y_min: ColLike,
    bound_y_max: ColLike,
    cell_splits: ColLike,
    root_cell_size_x: ColLike,
    root_cell_size_y: ColLike,
    srid: Optional[ColLike] = None,
) -> Column:
    """Build a custom grid specification struct for use with other gbx_custom_* functions.

    All numeric parameters must be integers (INT or LONG). Bounds define the
    extent of the grid in native CRS units; root cell sizes define the top-level
    tile size in the same units; cell_splits controls how many times each root
    cell is subdivided per resolution level.

    Args:
        bound_x_min: Minimum x coordinate of the grid extent.
        bound_x_max: Maximum x coordinate of the grid extent.
        bound_y_min: Minimum y coordinate of the grid extent.
        bound_y_max: Maximum y coordinate of the grid extent.
        cell_splits: Number of subdivisions per axis at each resolution level (>= 2).
        root_cell_size_x: Root cell width in native CRS units (> 0).
        root_cell_size_y: Root cell height in native CRS units (> 0).
        srid: Optional EPSG SRID for the grid CRS (``None`` means no CRS, stored as -1).

    Returns:
        Column of grid-spec STRUCT consumed by all other gbx_custom_* functions.
    """
    if srid is None:
        return f.call_function(
            "gbx_custom_grid",
            _col(bound_x_min),
            _col(bound_x_max),
            _col(bound_y_min),
            _col(bound_y_max),
            _col(cell_splits),
            _col(root_cell_size_x),
            _col(root_cell_size_y),
        )
    return f.call_function(
        "gbx_custom_grid",
        _col(bound_x_min),
        _col(bound_x_max),
        _col(bound_y_min),
        _col(bound_y_max),
        _col(cell_splits),
        _col(root_cell_size_x),
        _col(root_cell_size_y),
        _col(srid),
    )


def custom_pointascell(geom: ColLike, grid: ColLike, resolution: ColLike) -> Column:
    """Encode a point geometry as a custom grid cell id at the given resolution.

    Args:
        geom: Point geometry column (WKB bytes or WKT string, native CRS).
        grid: Grid-spec struct column produced by ``custom_grid``.
        resolution: Resolution level (0 = root; each level subdivides by cell_splits).

    Returns:
        Column of BIGINT custom grid cell ids.
    """
    return f.call_function(
        "gbx_custom_pointascell", _col(geom), _col(grid), _col(resolution)
    )


def custom_cellaswkb(cell: ColLike, grid: ColLike) -> Column:
    """Return the custom grid cell footprint as a WKB polygon.

    Args:
        cell: Column of BIGINT custom grid cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.

    Returns:
        Column of BINARY (WKB polygon).
    """
    return f.call_function("gbx_custom_cellaswkb", _col(cell), _col(grid))


def custom_cellaswkt(cell: ColLike, grid: ColLike) -> Column:
    """Return the custom grid cell footprint as a WKT string.

    Args:
        cell: Column of BIGINT custom grid cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.

    Returns:
        Column of STRING (WKT polygon).
    """
    return f.call_function("gbx_custom_cellaswkt", _col(cell), _col(grid))


def custom_centroid(cell: ColLike, grid: ColLike) -> Column:
    """Return the centroid of a custom grid cell as a WKB point.

    Args:
        cell: Column of BIGINT custom grid cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.

    Returns:
        Column of BINARY (WKB point).
    """
    return f.call_function("gbx_custom_centroid", _col(cell), _col(grid))


def custom_polyfill(geom: ColLike, grid: ColLike, resolution: ColLike) -> Column:
    """Return the custom grid cells covering a geometry at the given resolution.

    Args:
        geom: Geometry column (WKB bytes or WKT string, native CRS).
        grid: Grid-spec struct column produced by ``custom_grid``.
        resolution: Resolution level (0 = root; each level subdivides by cell_splits).

    Returns:
        Column of ``ARRAY<BIGINT>`` custom grid cell ids.
    """
    return f.call_function(
        "gbx_custom_polyfill", _col(geom), _col(grid), _col(resolution)
    )


def custom_kring(cell: ColLike, grid: ColLike, k: ColLike) -> Column:
    """Return all custom grid cells within Chebyshev distance ``k`` of ``cell`` (inclusive).

    Args:
        cell: Column of BIGINT custom grid cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.
        k: Ring distance (0 = cell itself only).

    Returns:
        Column of ``ARRAY<BIGINT>`` custom grid cell ids.
    """
    return f.call_function("gbx_custom_kring", _col(cell), _col(grid), _col(k))


def custom_kloop(cell: ColLike, grid: ColLike, k: ColLike) -> Column:
    """Return custom grid cells at EXACTLY Chebyshev distance ``k`` (hollow ring).

    At ``k=0`` returns the center cell; at ``k=1`` returns the 8 surrounding
    cells (center excluded).

    Args:
        cell: Column of BIGINT custom grid cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.
        k: Loop distance — only the shell at exactly distance ``k`` is returned.

    Returns:
        Column of ``ARRAY<BIGINT>`` custom grid cell ids.
    """
    return f.call_function("gbx_custom_kloop", _col(cell), _col(grid), _col(k))


def custom_distance(cell1: ColLike, grid: ColLike, cell2: ColLike) -> Column:
    """Chebyshev grid-ring distance between two custom-grid cells.

    Returns ``max(|dx|, |dy|)`` — the minimum number of ring steps needed to
    travel from ``cell1`` to ``cell2``.

    Args:
        cell1: Column of BIGINT source cell ids.
        grid: Grid-spec struct column produced by ``custom_grid``.
        cell2: Column of BIGINT target cell ids.

    Returns:
        Column of BIGINT Chebyshev distance.
    """
    return f.call_function("gbx_custom_distance", _col(cell1), _col(grid), _col(cell2))


def custom_cellfill(
    cellid: ColLike,
    value: ColLike,
    grid: ColLike,
    k: ColLike = 1,
    method: ColLike = "mean",
    power: ColLike = 2.0,
) -> Column:
    """Grouped aggregator: fill NULL custom-grid cells from valid neighbours.

    Use with ``groupBy(...).agg(cx.custom_cellfill(...))`` to interpolate missing
    values. Returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.

    Args:
        cellid: Column of BIGINT custom grid cell ids.
        value: Column of DOUBLE values (NULL marks cells to be filled).
        grid: Grid-spec struct column produced by ``custom_grid``.
        k: Neighbour ring radius (default ``1``).
        method: Interpolation method — ``'mean'`` (default) or ``'idw'``.
        power: IDW power parameter (default ``2.0``; ignored for ``'mean'``).

    Returns:
        Column of ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    """
    _method = f.lit(method) if isinstance(method, str) else _col(method)
    return f.call_function(
        "gbx_custom_cellfill",
        _col(cellid),
        _col(value),
        _col(grid),
        _col(k),
        _method,
        _col(power),
    )


def custom_geomkring(
    geom: ColLike,
    grid: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = "boundary-out",
) -> Column:
    """Geometry-aware k-ring for a custom grid.

    Returns all cells reachable from the geometry's covering set in at most ``k``
    Chebyshev steps.

    Args:
        geom: Geometry column (WKB bytes or WKT string, native grid CRS).
        grid: Grid-spec struct column produced by ``custom_grid``.
        resolution: Resolution level.
        k: Dilation distance (0 = covering set only).
        mode: Dilation mode (default ``"boundary-out"``).

    Returns:
        Column of ``ARRAY<BIGINT>`` custom grid cell ids.
    """
    # mode is always a string VALUE (never a column name); use f.lit so Spark
    # does not misinterpret it as an unresolved column reference.
    mode_arg = mode if isinstance(mode, Column) else f.lit(mode)
    return f.call_function(
        "gbx_custom_geomkring",
        _col(geom),
        _col(grid),
        _col(resolution),
        _col(k),
        mode_arg,
    )


def custom_geomkloop(
    geom: ColLike,
    grid: ColLike,
    resolution: ColLike,
    k: ColLike,
    mode: ColLike = "boundary-out",
) -> Column:
    """Geometry-aware k-loop (hollow ring) for a custom grid.

    Returns cells at EXACTLY ``k`` Chebyshev steps from the geometry's covering set.

    Args:
        geom: Geometry column (WKB bytes or WKT string, native grid CRS).
        grid: Grid-spec struct column produced by ``custom_grid``.
        resolution: Resolution level.
        k: Dilation distance.
        mode: Dilation mode (default ``"boundary-out"``).

    Returns:
        Column of ``ARRAY<BIGINT>`` custom grid cell ids.
    """
    mode_arg = mode if isinstance(mode, Column) else f.lit(mode)
    return f.call_function(
        "gbx_custom_geomkloop",
        _col(geom),
        _col(grid),
        _col(resolution),
        _col(k),
        mode_arg,
    )


def custom_geomkringexplode(*args, **kwargs) -> Column:
    """Streaming UDTF (SQL-LATERAL): SELECT cellid FROM gbx_custom_geomkringexplode(geom, grid, res, k). No Column form."""
    raise NotImplementedError(
        "Light custom_geomkringexplode is a streaming table function (registered UDTF "
        "gbx_custom_geomkringexplode): invoke via SQL LATERAL, e.g. "
        "SELECT t.* FROM <df>, LATERAL gbx_custom_geomkringexplode(...) t."
    )


def custom_geomkloopexplode(*args, **kwargs) -> Column:
    """Streaming UDTF (SQL-LATERAL): SELECT cellid FROM gbx_custom_geomkloopexplode(geom, grid, res, k). No Column form."""
    raise NotImplementedError(
        "Light custom_geomkloopexplode is a streaming table function (registered UDTF "
        "gbx_custom_geomkloopexplode): invoke via SQL LATERAL, e.g. "
        "SELECT t.* FROM <df>, LATERAL gbx_custom_geomkloopexplode(...) t."
    )
