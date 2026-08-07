"""British National Grid (BNG) Python API.

Thin wrappers around GeoBrix Scala functions (gbx_bng_*). Register with
bx.register(spark) then use the functions on Spark columns. For full
descriptions and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_bng_<name>;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)`` — so you can write ``bng_pointascell(pt, 1)``
or ``bng_kring(cell, 2)`` instead of wrapping in ``f.lit``. Strings and
``Column`` values pass through unchanged — pyspark treats a bare string as a
dataframe column reference (``f.col("name")``); wrap in ``f.lit(...)`` to pass
a string literal (e.g. ``bng_pointascell(pt, f.lit("1km"))``).
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


def register(_spark: SparkSession) -> None:
    """Register BNG functions with the Spark session.

    Call once (e.g. after creating the session) so that gbx_bng_* SQL
    functions are available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "gridx.bng").load().collect()


def bng_aswkb(cellid: ColLike) -> Column:
    """Return the BNG cell as Well-Known Binary.

    Args:
        cellid: BNG cell identifier column.

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_bng_aswkb", _col(cellid))


def bng_aswkt(cellid: ColLike) -> Column:
    """Return the BNG cell as Well-Known Text.

    Args:
        cellid: BNG cell identifier column.

    Returns:
        Column of WKT (string).
    """
    return f.call_function("gbx_bng_aswkt", _col(cellid))


def bng_cellarea(cellid: ColLike) -> Column:
    """Return the area of the BNG cell in square kilometres.

    Args:
        cellid: BNG cell identifier column.

    Returns:
        Column of area (double, km²).
    """
    return f.call_function("gbx_bng_cellarea", _col(cellid))


def bng_cellintersection(left_chip: ColLike, right_chip: ColLike) -> Column:
    """Return the intersection of two BNG cell chips as a chip struct.

    Args:
        left_chip: Left BNG chip struct column.
        right_chip: Right BNG chip struct column.

    Returns:
        Column of chip struct (cellid, core, chip).
    """
    return f.call_function(
        "gbx_bng_cellintersection", _col(left_chip), _col(right_chip)
    )


def bng_cellunion(left_chip: ColLike, right_chip: ColLike) -> Column:
    """Return the union of two BNG cell chips as a chip struct.

    Args:
        left_chip: Left BNG chip struct column.
        right_chip: Right BNG chip struct column.

    Returns:
        Column of chip struct (cellid, core, chip).
    """
    return f.call_function("gbx_bng_cellunion", _col(left_chip), _col(right_chip))


def bng_centroid(cellid: ColLike) -> Column:
    """Return the centroid of the BNG cell as geometry.

    Args:
        cellid: BNG cell identifier column.

    Returns:
        Column of point geometry (WKB).
    """
    return f.call_function("gbx_bng_centroid", _col(cellid))


def bng_distance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Return the grid distance between two BNG cells (in cell units).

    Args:
        cellid1: First BNG cell identifier column.
        cellid2: Second BNG cell identifier column.

    Returns:
        Column of long (grid distance).
    """
    return f.call_function("gbx_bng_distance", _col(cellid1), _col(cellid2))


def bng_eastnorthasbng(easting: ColLike, northing: ColLike, resolution: ColLike) -> Column:
    """Convert easting and northing to a BNG cell identifier.

    Args:
        easting: Easting column (metres).
        northing: Northing column (metres).
        resolution: BNG resolution — integer index ±1..±6 or string key from
            ``BNG.resolutionMap`` (e.g. ``"1km"``). NOT metres.

    Returns:
        Column of BNG cell identifier.
    """
    return f.call_function(
        "gbx_bng_eastnorthasbng", _col(easting), _col(northing), _col(resolution)
    )


def bng_euclideandistance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Return the Euclidean distance between two BNG cell centres (metres).

    Args:
        cellid1: First BNG cell identifier column.
        cellid2: Second BNG cell identifier column.

    Returns:
        Column of long (distance in metres).
    """
    return f.call_function("gbx_bng_euclideandistance", _col(cellid1), _col(cellid2))


def bng_geomkloop(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """Return the k-ring of cells around the geometry (as array of cell IDs).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        k: Ring distance (0 = cell(s) covering geometry only).

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_geomkloop", _col(geom), _col(resolution), _col(k))


def bng_geomkring(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """Return the k-loop (hollow ring) of cells around the geometry.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        k: Ring distance.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_geomkring", _col(geom), _col(resolution), _col(k))


def bng_kloop(cellid: ColLike, k: ColLike) -> Column:
    """Return the k-ring of cell IDs around the given cell (including centre).

    Args:
        cellid: BNG cell identifier column.
        k: Ring distance (0 = cell itself only).

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_kloop", _col(cellid), _col(k))


def bng_kring(cellid: ColLike, k: ColLike) -> Column:
    """Return the k-loop (hollow ring) of cell IDs around the given cell.

    Args:
        cellid: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_kring", _col(cellid), _col(k))


def bng_pointascell(geom: ColLike, resolution: ColLike) -> Column:
    """Convert a point geometry to a BNG grid cell identifier.

    The point must be a Column of WKT (string) or WKB (binary) with BNG
    eastings/northings (EPSG:27700). GeoBrix does not accept native Databricks
    geometry types (e.g. do not pass the result of st_point() or other DBR
    geometry functions).

    Args:
        geom: Point geometry (WKT/WKB) in BNG coordinates.
        resolution: BNG resolution — integer index ±1..±6 or string key.

    Returns:
        Column of BNG cell identifier.

    Example:
        bx.bng_pointascell("POINT(400000 400000)", "1km")
    """
    return f.call_function("gbx_bng_pointascell", _col(geom), _col(resolution))


def bng_polyfill(geom: ColLike, resolution: ColLike) -> Column:
    """Return the set of BNG cells that cover the geometry (as array).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.

    Returns:
        Column of array of BNG cell identifiers.
    """
    return f.call_function("gbx_bng_polyfill", _col(geom), _col(resolution))


def bng_tessellate(
    geom: ColLike, resolution: ColLike, keep_core_geom: ColLike = True
) -> Column:
    """Tessellate the geometry into BNG cells (as array of cell IDs).

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        keep_core_geom: If True, include the original geometry in the result.

    Returns:
        Column of array of BNG cell identifiers (and optionally geometry).
    """
    return f.call_function(
        "gbx_bng_tessellate", _col(geom), _col(resolution), _col(keep_core_geom)
    )


# Aggregators


def bng_cellintersection_agg(input_chip: ColLike) -> Column:
    """Aggregate multiple BNG cell chips into their intersection chip.

    Use with grouped aggregation (groupBy).

    Args:
        input_chip: Column of BNG chip structs.

    Returns:
        Column of chip struct (cellid, core, chip).
    """
    return f.call_function("gbx_bng_cellintersection_agg", _col(input_chip))


def bng_cellunion_agg(input_chip: ColLike) -> Column:
    """Aggregate multiple BNG cell chips into their union chip.

    Use with grouped aggregation (groupBy).

    Args:
        input_chip: Column of BNG chip structs.

    Returns:
        Column of chip struct (cellid, core, chip).
    """
    return f.call_function("gbx_bng_cellunion_agg", _col(input_chip))


# Generators


def bng_geomkloopexplode(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """Explode the k-ring of cells around the geometry into one row per cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(
        f.call_function(
            "gbx_bng_geomkloopexplode", _col(geom), _col(resolution), _col(k)
        )
    )


def bng_geomkringexplode(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """Explode the k-loop (hollow ring) around the geometry into one row per cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(
        f.call_function(
            "gbx_bng_geomkringexplode", _col(geom), _col(resolution), _col(k)
        )
    )


def bng_kloopexplode(cellid: ColLike, k: ColLike) -> Column:
    """Explode the k-ring around the cell into one row per cell.

    Args:
        cellid: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_kloopexplode", _col(cellid), _col(k)))


def bng_kringexplode(cellid: ColLike, k: ColLike) -> Column:
    """Explode the k-loop (hollow ring) around the cell into one row per cell.

    Args:
        cellid: BNG cell identifier column.
        k: Ring distance.

    Returns:
        Column of exploded BNG cell identifiers (use with explode).
    """
    return f.explode(f.call_function("gbx_bng_kringexplode", _col(cellid), _col(k)))


def bng_tessellateexplode(
    geom: ColLike, resolution: ColLike, keep_core_geom: ColLike = True
) -> Column:
    """Explode the tessellation of the geometry into one row per BNG cell.

    Args:
        geom: Geometry column (WKT or WKB).
        resolution: BNG resolution — integer index ±1..±6 or string key.
        keep_core_geom: If True, include the original geometry in the result.

    Returns:
        Column of exploded BNG cell identifiers (and optionally geometry).
    """
    return f.call_function(
        "gbx_bng_tessellateexplode",
        _col(geom),
        _col(resolution),
        _col(keep_core_geom),
    )
