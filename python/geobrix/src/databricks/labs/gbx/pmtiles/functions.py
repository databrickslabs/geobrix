"""PMTiles Python API.

Thin wrapper around GeoBrix's PMTiles v3 encoder. Two paths:

  1. ``pmtiles_agg(bytes, z, x, y, [metadata_json])`` — Spark UDAF that
     aggregates a column of tile bytes into a single PMTile BINARY blob.
     Use when the full pyramid fits in a Spark cell (rough ceiling: ~100 MiB
     of tile payload / 2 GiB Spark cell limit).

  2. ``df.write.format("pmtiles").mode("overwrite").save(path)`` — Spark V2
     DataSource that streams arbitrarily large pyramids to a single
     ``.pmtiles`` file via a partitioned commit protocol. No Python wrapper
     is needed for the DataSource path — it is registered automatically when
     the GeoBrix JAR is on the Spark classpath.

Register the UDAF once per session before use::

    from databricks.labs.gbx.pmtiles import functions as px
    px.register(spark)

Spec: https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

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
    """Register PMTiles functions with the Spark session.

    Call once (e.g. after creating the session) so that ``gbx_pmtiles_agg``
    is available as a SQL function. The DataSource format string ``pmtiles``
    is wired via ``META-INF/services`` and does not need an explicit register
    call.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "pmtiles").load().collect()


def pmtiles_agg(
    bytes_col: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    metadata_json: Union[Column, str] = None,
) -> Column:
    """Aggregate tile rows into a single PMTile v3 BINARY blob.

    Use with ``df.agg(...)`` or ``df.groupBy(...).agg(...)``. Returns a column
    of BINARY containing the canonical single-file PMTile container.

    Args:
        bytes_col: Tile-payload column (BINARY) — passed through verbatim
            (callers compress before aggregating).
        z: Tile zoom column (INT).
        x: Tile x column (INT).
        y: Tile y column (INT).
        metadata_json: Optional JSON metadata. Pass either a ``Column`` (e.g.
            ``f.lit('{"name":"x"}')``) or a Python ``str``; bare ``str`` is
            wrapped in ``f.lit`` for you. Defaults to ``"{}"``. Stored
            verbatim in the PMTile spec section 5 metadata section.

    Returns:
        Column of BINARY (PMTile v3 archive bytes).

    Example::

        from databricks.labs.gbx.pmtiles import functions as px
        px.register(spark)
        from pyspark.sql import functions as f
        pmt = tiles_df.agg(
            px.pmtiles_agg(f.col("bytes"), f.col("z"), f.col("x"), f.col("y"),
                            '{"name":"my_tiles"}').alias("pmt")
        ).collect()[0]["pmt"]
        # pmt is now a bytes/bytearray; write to disk or post to a tile server.
    """
    if metadata_json is None:
        meta = f.lit("{}")
    elif isinstance(metadata_json, Column):
        meta = metadata_json
    else:
        # Treat bare Python strings as JSON literals (NOT column references) — the
        # default-of-`"{}"` UX was confusing otherwise. For users who genuinely want a
        # metadata *column*, pass `f.col("metadata_col")` explicitly.
        meta = f.lit(metadata_json)
    return f.call_function(
        "gbx_pmtiles_agg",
        _col(bytes_col),
        _col(z),
        _col(x),
        _col(y),
        meta,
    )
