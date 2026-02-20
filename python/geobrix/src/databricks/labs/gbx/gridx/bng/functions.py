"""British National Grid (BNG) Python API.

Thin wrappers around GeoBrix Scala functions (gbx_bng_*). Register with
bx.register(spark) then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_bng_<name>;
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f


def register(_spark: SparkSession) -> None:
    """Register BNG functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_bng_* SQL
    functions are available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "gridx.bng").load().collect()


def bng_aswkb(cell_id: Column) -> Column:
    """Return the BNG cell as Well-Known Binary.

    Args:
        cell_id: BNG cell identifier column.

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_bng_aswkb", cell_id)


def bng_aswkt(cell_id: Column) -> Column:
    """Return the BNG cell as Well-Known Text.

    Args:
        cell_id: BNG cell identifier column.

    Returns:
        Column of WKT (string).
    """
    return f.call_function("gbx_bng_aswkt", cell_id)


def bng_cellarea(cell_id: Column) -> Column:
    """Return the area of the BNG cell in square metres.

    Args:
        cell_id: BNG cell identifier column.

    Returns:
        Column of area (double).
    """
    return f.call_function("gbx_bng_cellarea", cell_id)


def bng_cellintersection(cell_id1: Column, cell_id2: Column) -> Column:
    """Return the intersection of two BNG cells as geometry.

    Args:
        cell_id1: First BNG cell identifier column.
        cell_id2: Second BNG cell identifier column.

    Returns:
        Column of geometry (WKB).
    """
    return f.call_function("gbx_bng_cellintersection", cell_id1, cell_id2)


def bng_cellunion(cell_id1: Column, cell_id2: Column) -> Column:
    """Return the union of two BNG cells as geometry.

    Args:
        cell_id1: First BNG cell identifier column.
        cell_id2: Second BNG cell identifier column.

    Returns:
        Column of geometry (WKB).
    """
    return f.call_function("gbx_bng_cellunion", cell_id1, cell_id2)


def bng_centroid(cell_id: Column) -> Column:
    """Return the centroid of the BNG cell as geometry.

    Args:
        cell_id: BNG cell identifier column.

    Returns:
        Column of point geometry (WKB).
    """
    return f.call_function("gbx_bng_centroid", cell_id)


def bng_distance(cell_id1: Column, cell_id2: Column) -> Column:
    """Return the grid distance between two BNG cells (in cell units).

    Args:
        cell_id1: First BNG cell identifier column.
        cell_id2: Second BNG cell identifier column.

    Returns:
        Column of long (grid distance).
    """
    return f.call_function("gbx_bng_distance", cell_id1, cell_id2)


def bng_eastnorthasbng(east: Column, north: Column, resolution: Column) -> Column:
    """Convert easting and northing to a BNG cell identifier.

    Args:
        east: Easting column (metres).
        north: Northing column (metres).
        resolution: Grid resolution (e.g. 1000 for 1 km).

    Returns:
        Column of BNG cell identifier.
    """
    return f.call_function("gbx_bng_eastnorthasbng", east, north, resolution)


def bng_euclideandistance(cell_id1: Column, cell_id2: Column) -> Column:
    """Return the Euclidean distance between two BNG cell centres (metres).

    Args:
        cell_id1: First BNG cell identifier column.
        cell_id2: Second BNG cell identifier column.

    Returns:
        Column of long (distance in metres).
    """
    return f.call_function("gbx_bng_euclideandistance", cell_id1, cell_id2)


def bng_geomkloop(geom: Column, resolution: Column, k: Column) -> Column:
    """Return the k-ring of cells around the geometry (as array of cell IDs).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        k: Ring distance (0 = cell(s) covering geometry only).

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_geomkloop", geom, resolution, k)


def bng_geomkring(geom: Column, resolution: Column, k: Column) -> Column:
    """Return the k-loop (hollow ring) of cells around the geometry.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        k: Ring distance.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_geomkring", geom, resolution, k)


def bng_kloop(cell_id: Column, k: Column) -> Column:
    """Return the k-ring of cell IDs around the given cell (including centre).

    Args:
        cell_id: BNG cell identifier column.
        k: Ring distance (0 = cell itself only).

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_kloop", cell_id, k)


def bng_kring(cell_id: Column, k: Column) -> Column:
    """Return the k-loop (hollow ring) of cell IDs around the given cell.

    Args:
        cell_id: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_kring", cell_id, k)


def bng_pointascell(point: Column, resolution: Column) -> Column:
    """Convert a point geometry to a BNG grid cell identifier.

    The point must be a Column of WKT (string) or WKB (binary). GeoBrix does
    not accept native Databricks geometry types (e.g. do not pass the result
    of st_point() or other DBR geometry functions).

    Args:
        point: Point geometry column (WKT or WKB).
        resolution: Grid resolution column (e.g. f.lit(1000)).

    Returns:
        Column of BNG cell identifier.

    Example:
        bx.bng_pointascell(f.lit('POINT(-0.1278 51.5074)'), f.lit(1000))
    """
    return f.call_function("gbx_bng_pointascell", point, resolution)


def bng_polyfill(geom: Column, resolution: Column) -> Column:
    """Return the set of BNG cells that cover the geometry (as array).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_polyfill", geom, resolution)


def bng_tessellate(
    geom: Column, resolution: Column, keep_core_geom: bool = True
) -> Column:
    """Tessellate the geometry into BNG cells (as array of cell IDs).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        keep_core_geom: If True, include the original geometry in the result.

    Returns:
        Column of array of BNG cell identifiers (and optionally geometry).
    """
    return f.call_function(
        "gbx_bng_tessellate", geom, resolution, f.lit(keep_core_geom)
    )


# Aggregators


def bng_cellintersection_agg(cells: Column) -> Column:
    """Aggregate multiple BNG cell IDs into their intersection geometry.

    Use with grouped aggregation (groupBy).

    Args:
        cells: Column of array of BNG cell identifiers.

    Returns:
        Column of geometry (WKB).
    """
    return f.call_function("gbx_bng_cellintersection_agg", cells)


def bng_cellunion_agg(cells: Column) -> Column:
    """Aggregate multiple BNG cell IDs into their union geometry.

    Use with grouped aggregation (groupBy).

    Args:
        cells: Column of array of BNG cell identifiers.

    Returns:
        Column of geometry (WKB).
    """
    return f.call_function("gbx_bng_cellunion_agg", cells)


# Generators


def bng_geomkloopexplode(geom: Column, resolution: Column, k: Column) -> Column:
    """Explode the k-ring of cells around the geometry into one row per cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_geomkloopexplode", geom, resolution, k))


def bng_geomkringexplode(geom: Column, resolution: Column, k: Column) -> Column:
    """Explode the k-loop (hollow ring) around the geometry into one row per cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_geomkringexplode", geom, resolution, k))


def bng_kloopexplode(cell_id: Column, k: Column) -> Column:
    """Explode the k-ring around the cell into one row per cell.

    Args:
        cell_id: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_kloopexplode", cell_id, k))


def bng_kringexplode(cell_id: Column, k: Column) -> Column:
    """Explode the k-loop (hollow ring) around the cell into one row per cell.

    Args:
        cell_id: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_kringexplode", cell_id, k))


def bng_tessellateexplode(
    geom: Column, resolution: Column, keep_core_geom: bool = True
) -> Column:
    """Explode the tessellation of the geometry into one row per BNG cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: Grid resolution column.
        keep_core_geom: If True, include the original geometry in the result.

    Returns:
        Column of exploded BNG cell identifiers (and optionally geometry).
    """
    return f.call_function(
        "gbx_bng_tessellateexplode", geom, resolution, f.lit(keep_core_geom)
    )
