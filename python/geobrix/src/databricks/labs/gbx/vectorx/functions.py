"""VectorX Python API.

Thin wrappers around GeoBrix Scala functions (``gbx_st_*``). Register with
``vx.register(spark)`` then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_st_<name>;

As of v0.4.0 this package exposes the ``gbx_st_asmvt`` MVT aggregator,
``gbx_st_asmvt_pyramid`` MVT pyramid generator, and the TIN/elevation
generators ``gbx_st_triangulate``, ``gbx_st_interpolateelevationbbox``, and
``gbx_st_interpolateelevationgeom``.

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)``. Strings and ``Column`` values pass through
unchanged — pyspark treats a bare string as a dataframe column reference
(``f.col("name")``); wrap in ``f.lit(...)`` to pass a string literal
(e.g. ``vx.st_asmvt(geom, attrs, f.lit("roads"))``).
"""

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through.

    Strings stay as strings so pyspark's call_function treats them as column
    references. Use f.lit("...") for string literals.
    """
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def _mode_col(mode: ColLike) -> Column:
    """Resolve the optional ``mode`` arg to a literal Column.

    A bare ``str`` (the common case — ``"constrained"`` / ``"conforming"``) is a
    constant value, not a column reference, so it is wrapped with ``f.lit``. A
    ``Column`` passes through unchanged for callers driving mode from data.
    """
    return f.lit(mode) if isinstance(mode, str) else _col(mode)


def register(spark: SparkSession) -> None:
    """Register VectorX expression-level SQL functions with the Spark session.

    Call once (e.g. after creating the session) so that ``gbx_st_*``
    expression-level functions are available. Delegates to the JVM
    ``com.databricks.labs.gbx.vectorx.functions.register`` entry point —
    this is independent from the data-source registry used by other GeoBrix
    packages because VectorX expression-level functions are new in v0.4.0.

    Args:
        spark: Spark session (uses active session if not provided).
    """
    spark = spark or SparkSession.builder.getOrCreate()
    spark._jvm.com.databricks.labs.gbx.vectorx.functions.register(spark._jsparkSession)


def st_asmvt(geom: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into a Mapbox Vector Tile (MVT) protobuf blob.

    Args:
        geom: Per-row geometry (WKB, EWKB, WKT, or EWKT) column, in tile-local coordinates.
        attrs:    Per-row attribute struct column (encoded with native MVT value types).
        layer_name: Constant MVT layer name. Pass a plain ``str`` for a literal layer
                    name (auto-wrapped with ``f.lit``), or a ``Column`` to reference
                    a column. To reference a column by name, use ``f.col("...")``.

    Returns:
        Aggregate Column producing the MVT protobuf bytes (``BINARY``) for one tile layer.
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return f.call_function("gbx_st_asmvt", _col(geom), _col(attrs), _col(layer_name))


def st_asmvt_pyramid(
    geom: ColLike,
    attrs: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    layer_name: Union[ColLike, None] = None,
    extent: Union[ColLike, None] = None,
) -> Column:
    """Generator: emit one row per intersecting ``(z, x, y)`` tile across ``[min_z, max_z]``.

    Per-row output column is a struct
    ``tile: STRUCT<z INT, x INT, y INT, mvt_bytes BINARY>``. Invoke directly in
    ``select(...)`` (top-level generator, do not wrap in ``F.explode``).

    Inputs are assumed in EPSG:4326 lon/lat. Per-tile clip + MVT encode happen
    in the helper; the row output is ready to feed into ``gbx_pmtiles_agg`` for
    end-to-end vector publishing. ``max_z`` capped at 20; total tile-count
    across the requested zoom range capped at 10^6.

    Args:
        geom:       Per-feature geometry (WKB, EWKB, WKT, or EWKT) column.
        attrs:      Per-feature attribute struct column (encoded with native MVT value types).
        min_z:      Inclusive minimum zoom level.
        max_z:      Inclusive maximum zoom level (<= 20).
        layer_name: Constant MVT layer name. Pass a plain ``str`` for a literal
                    layer name (auto-wrapped with ``f.lit``).
        extent:     MVT tile extent in pixels (default 4096).

    Returns:
        Generator Column producing one row per intersecting tile.
    """
    layer_name_col = (
        f.lit("layer")
        if layer_name is None
        else (f.lit(layer_name) if isinstance(layer_name, str) else _col(layer_name))
    )
    extent_col = f.lit(4096) if extent is None else _col(extent)
    return f.call_function(
        "gbx_st_asmvt_pyramid",
        _col(geom),
        _col(attrs),
        _col(min_z),
        _col(max_z),
        layer_name_col,
        extent_col,
    )


def st_triangulate(
    points_array: ColLike,
    breaklines_array: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    split_point_finder: ColLike,
    mode: ColLike = "constrained",
) -> Column:
    """Generator: emit one row per TIN triangle polygon from a Delaunay triangulation.

    Each output row is a struct ``STRUCT<triangle BINARY>`` containing a WKB-encoded triangle
    polygon. Invoke directly in ``select(...)`` as a top-level generator — do not wrap in
    ``F.explode``.

    Points that are co-linear or degenerate produce zero rows. Valid non-collinear input of
    N points produces at least ``N - 2`` triangle rows (Delaunay property).

    Args:
        points_array:       Array column of Z-valued point geometries (``ARRAY<BINARY|STRING>``).
                            Each element is a WKB byte array or a WKT/EWKT string.
        breaklines_array:   Array column of LineString geometries (``ARRAY<BINARY|STRING>``).
                            Pass an empty array (``array().cast(ArrayType(StringType()))``) when
                            no breaklines are needed.
        merge_tolerance:    Distance tolerance for merging nearby vertices (``DOUBLE``).
        snap_tolerance:     Snap tolerance for the triangulator (``DOUBLE``).
        split_point_finder: Strategy name for constrained edge splitting. Valid values:
                            ``"NONENCROACHING"`` (default) and ``"MIDPOINT"``.
        mode:               Triangulation mode (``STRING``). ``"constrained"`` (default) recovers
                            breakline edges without inserting Steiner points; ``"conforming"``
                            inserts Steiner points for a conforming constrained Delaunay TIN.
                            Pass a plain ``str`` (auto-wrapped with ``f.lit``).

    Returns:
        Generator Column producing one ``STRUCT<triangle BINARY>`` row per TIN triangle.
    """
    return f.call_function(
        "gbx_st_triangulate",
        _col(points_array),
        _col(breaklines_array),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(split_point_finder),
        _mode_col(mode),
    )


def st_interpolateelevationbbox(
    points_array: ColLike,
    breaklines_array: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    split_point_finder: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    mode: ColLike = "constrained",
) -> Column:
    """Generator: emit one Z-interpolated grid point per cell over a bounding-box-defined grid.

    Builds a TIN from the input Z-valued points via constrained Delaunay triangulation, then
    interpolates elevation at each center of a regular ``width_px × height_px`` grid spanning
    the given bounding box. Grid cells whose centers fall outside the TIN convex hull are
    silently dropped. Each output row is a struct ``STRUCT<elevation_point BINARY>`` containing
    a WKB-encoded 3D Point. Invoke directly in ``select(...)`` as a top-level generator.

    Args:
        points_array:       Array column of Z-valued point geometries (``ARRAY<BINARY|STRING>``).
        breaklines_array:   Array column of LineString geometries (``ARRAY<BINARY|STRING>``).
        merge_tolerance:    Vertex merge tolerance (``DOUBLE``).
        snap_tolerance:     Triangulator snap tolerance (``DOUBLE``).
        split_point_finder: Edge-split strategy — ``"NONENCROACHING"`` or ``"MIDPOINT"``.
        xmin:               West extent of the grid (``DOUBLE``).
        ymin:               South extent of the grid (``DOUBLE``).
        xmax:               East extent of the grid (``DOUBLE``).
        ymax:               North extent of the grid (``DOUBLE``).
        width_px:           Number of grid columns (``INT``).
        height_px:          Number of grid rows (``INT``).
        srid:               Spatial reference ID to assign to output points (``INT``).
        mode:               Triangulation mode (``STRING``). ``"constrained"`` (default) recovers
                            breakline edges without Steiner points; ``"conforming"`` inserts
                            Steiner points for a conforming constrained Delaunay TIN. Pass a plain
                            ``str`` (auto-wrapped with ``f.lit``).

    Returns:
        Generator Column producing one ``STRUCT<elevation_point BINARY>`` row per interpolated
        grid point inside the TIN hull.
    """
    return f.call_function(
        "gbx_st_interpolateelevationbbox",
        _col(points_array),
        _col(breaklines_array),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(split_point_finder),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
        _mode_col(mode),
    )


def st_interpolateelevationgeom(
    points_array: ColLike,
    breaklines_array: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    split_point_finder: ColLike,
    grid_origin: ColLike,
    grid_cols: ColLike,
    grid_rows: ColLike,
    cell_size_x: ColLike,
    cell_size_y: ColLike,
    mode: ColLike = "constrained",
) -> Column:
    """Generator: emit one Z-interpolated grid point per cell over an origin-defined grid.

    Builds a TIN from the input Z-valued points via constrained Delaunay triangulation, then
    interpolates elevation at each center of a regular grid defined by an origin corner point,
    column/row counts, and per-cell dimensions. Grid cells whose centers fall outside the TIN
    convex hull are silently dropped. Each output row is a struct ``STRUCT<elevation_point BINARY>``
    containing a WKB-encoded 3D Point. Invoke directly in ``select(...)`` as a top-level generator.

    The ``grid_origin`` geometry carries the SRID of the output. Encode it as EWKB (e.g. via
    ``ST_SetSRID``) or as an EWKT string (``SRID=32633;POINT(...)``) to propagate a non-zero SRID
    to the output points. Plain WKB and plain WKT carry no SRID; in that case output SRID is 0.

    Args:
        points_array:       Array column of Z-valued point geometries (``ARRAY<BINARY|STRING>``).
        breaklines_array:   Array column of LineString geometries (``ARRAY<BINARY|STRING>``).
        merge_tolerance:    Vertex merge tolerance (``DOUBLE``).
        snap_tolerance:     Triangulator snap tolerance (``DOUBLE``).
        split_point_finder: Edge-split strategy — ``"NONENCROACHING"`` or ``"MIDPOINT"``.
        grid_origin:        Single POINT geometry (``BINARY|STRING``) for the grid's origin corner.
        grid_cols:          Number of grid columns (``INT``).
        grid_rows:          Number of grid rows (``INT``).
        cell_size_x:        Width of each grid cell in the CRS units (``DOUBLE``).
        cell_size_y:        Height of each grid cell in the CRS units (``DOUBLE``).
        mode:               Triangulation mode (``STRING``). ``"constrained"`` (default) recovers
                            breakline edges without Steiner points; ``"conforming"`` inserts
                            Steiner points for a conforming constrained Delaunay TIN. Pass a plain
                            ``str`` (auto-wrapped with ``f.lit``).

    Returns:
        Generator Column producing one ``STRUCT<elevation_point BINARY>`` row per interpolated
        grid point inside the TIN hull.
    """
    return f.call_function(
        "gbx_st_interpolateelevationgeom",
        _col(points_array),
        _col(breaklines_array),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(split_point_finder),
        _col(grid_origin),
        _col(grid_cols),
        _col(grid_rows),
        _col(cell_size_x),
        _col(cell_size_y),
        _mode_col(mode),
    )
