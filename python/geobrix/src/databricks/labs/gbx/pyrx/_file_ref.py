"""Feature-detect and windowed-read helpers for Databricks FILE type support.

file_supported()
    Checks once per SparkSession whether FILE is available, using a plan-level
    try_to_file mint followed by a UDF consume roundtrip. Result is memoized so
    subsequent calls are free.

open_windowed_via_fileref(file_ref, window, pending)
    Context manager: opens a FileRef's seekable stream via rasterio, reads
    exactly the requested window (applying pending instructions), and yields
    a DatasetReader backed by an in-memory GeoTIFF.  Raises FileRefReadError
    on any failure so callers can degrade to a local-file fallback.

Serverless-safe: no .rdd / _jvm / _jsc / sparkContext / conf.set.
"""

import os
from contextlib import contextmanager

import rasterio
from rasterio.io import MemoryFile
from rasterio.windows import Window

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

_FILE_SUPPORT_CACHE: dict = {}


def file_supported() -> bool:
    """Memoized per-SparkSession capability check for FILE support.

    Obtains the active SparkSession internally via SparkSession.getActiveSession()
    (Serverless-safe, no .rdd / _jvm / conf.set). Returns True if:
    - GBX_DISABLE_FILE env var is not set to "1", AND
    - FILE type is recognized and usable (end-to-end roundtrip succeeds).

    Returns False if:
    - GBX_DISABLE_FILE="1" (no spark touched), OR
    - No active SparkSession, OR
    - Any exception during the roundtrip (UNSUPPORTED_DATATYPE, sentinel
      unreadable, consume failure, etc.).

    Result is cached per SparkSession; the roundtrip runs at most once per session.

    Sentinel detail: the feature-detect mints try_to_file on a sentinel Volume
    path and consumes it in a UDF. Returns False if the file is not found
    (acceptable — detect-failure causing fallback is always safe).
    """
    if os.environ.get("GBX_DISABLE_FILE") == "1":
        return False

    spark = SparkSession.getActiveSession()
    if spark is None:
        return False

    session_id = id(spark)
    if session_id in _FILE_SUPPORT_CACHE:
        return _FILE_SUPPORT_CACHE[session_id]

    result = _check_file_support(spark)
    _FILE_SUPPORT_CACHE[session_id] = result
    return result


def _check_file_support(spark: SparkSession) -> bool:
    """Run end-to-end roundtrip to verify FILE is usable.

    Mints try_to_file on a sentinel Volume path in the Spark PLAN (not in a
    UDF), then consumes the FileRef in a UDF that calls fref.open().read(1).
    Returns True only if the roundtrip succeeds; False on any exception.

    CRITICAL: FileRef is MINTED IN THE PLAN via try_to_file (a SQL function),
    NOT constructed inside a UDF (pyspark.sql.types.FileRef(path) does not
    exist, and pyspark.sql.functions.try_to_file does not exist either).
    """
    try:
        from pyspark.sql import functions as F

        sentinel_path = (
            "/Volumes/main/geobrix_samples/geobrix-examples/london/"
            "LC08_L2SP_202_024_20200625_20200705_02_T1_SR_B1.TIF"
        )

        # Mint FileRef in the PLAN via try_to_file (a Spark SQL function).
        # Returns a DataFrame with a FILE-type column.
        df_with_fref = spark.sql(f"SELECT try_to_file('{sentinel_path}') AS fref")

        # Consume the FileRef column in a UDF.
        @F.udf("string")
        def _consume_fref(fref):
            try:
                with fref.open() as f:
                    byte_read = f.read(1)
                return "success" if byte_read else "empty"
            except Exception:
                return "failed"

        result_df = df_with_fref.select(_consume_fref(F.col("fref")))
        result = result_df.collect()[0][0]

        return result == "success"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Column-level FILE injection helper
# ---------------------------------------------------------------------------


def file_ref_arg(tile_col: Column) -> Column:
    """Return a Column expression for the file_ref argument to tile-reading UDFs.

    If file_supported() is True, returns a plan-level FILE mint expression
    (F.call_function("try_to_file", tile_col["path"])) that arrives at the UDF
    as a real FileRef value.  Otherwise returns F.lit(None) so the UDF falls
    back to the plain-path read.

    Uses SparkSession.getActiveSession() internally (Serverless-safe: no
    .rdd / _jvm / _jsc / sparkContext / conf.set).  Takes no spark param.
    """
    if file_supported():
        # F.call_function calls a named Spark SQL function as a plan expression.
        # try_to_file is a Spark SQL built-in (not a PySpark function) that mints
        # a FILE reference from a Volume path string.
        return F.call_function("try_to_file", tile_col["path"])
    # FILE not supported or no session — pass None (UDF uses fallback path).
    return F.lit(None)


# ---------------------------------------------------------------------------
# FileRef windowed-read helper
# ---------------------------------------------------------------------------


class FileRefReadError(Exception):
    """Raised when a FILE/FILEREF windowed read fails (allows degradation)."""

    pass


@contextmanager
def open_windowed_via_fileref(file_ref, window, pending):
    """Open a FileRef as a rasterio source and read exactly the window.

    The FileRef's .open() method must return a seekable stream (e.g. a
    ``_io.BufferedReader`` or ``io.BytesIO``).  The window and pending
    instructions are applied exactly as in open_tile, then a rasterio
    DatasetReader backed by an in-memory GeoTIFF is yielded.

    Args:
        file_ref: Object with ``.open()`` (returns seekable stream) and
            ``.as_local_file()`` (returns local path) methods — matching the
            real ``pyspark.sql.types.FileRef`` contract.
        window: ``(col_off, row_off, width, height)`` tuple from
            ``VirtualTile.window``.
        pending: 4-tuple ``(bands|None, nodata|None, srid|None, crs_str|None)``
            from ``_parse_pending(tile.metadata)``.

    Yields:
        An open ``rasterio.io.DatasetReader`` covering exactly the window.

    Raises:
        FileRefReadError: if the stream is not seekable, rasterio open fails,
            or any other windowed-read failure occurs.  Callers may degrade to
            ``file_ref.as_local_file()`` as a fallback.
    """
    try:
        stream = file_ref.open()
        if not stream.seekable():
            raise FileRefReadError("FileRef stream is not seekable")

        with rasterio.open(stream) as src:
            col_off, row_off, width, height = window
            rio_window = Window(col_off, row_off, width, height)

            from databricks.labs.gbx.pyrx.core.open_tile import _window_dataset_bytes

            tile_bytes = _window_dataset_bytes(src, rio_window, pending=pending)

            with MemoryFile(tile_bytes) as mf:
                with mf.open() as ds:
                    yield ds
    except FileRefReadError:
        raise
    except Exception as exc:
        raise FileRefReadError(f"FileRef windowed read failed: {exc}") from exc
