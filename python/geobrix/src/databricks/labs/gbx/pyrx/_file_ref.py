"""Feature-detect and windowed-read helpers for Databricks FILE type support.

file_supported()
    Checks once per SparkSession whether FILE is available, using a fixture-free
    plan-only probe (try_to_file on a nonexistent path). Result is memoized so
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


def _epsg_of_str(crs_str: str):
    """Parse 'EPSG:3857' / '3857' -> 3857; None if not a plain EPSG code."""
    s = str(crs_str).strip().upper()
    if s.startswith("EPSG:"):
        s = s[5:]
    try:
        return int(s)
    except ValueError:
        return None


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

    Result is cached per SparkSession; the probe runs at most once per session.
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
    """Run a fixture-free, plan-only probe to verify FILE is usable.

    Executes: SELECT try_to_file('<probe_path>') IS NULL
    where probe_path is a clearly-nonexistent Volumes-shaped path.

    Rationale: try_to_file (the try_ variant) returns NULL for a missing file
    WHEN FILE is enabled and fileReferenceCreationMode is set; it RAISES
    (UNSUPPORTED_DATATYPE "FILE", or a creation-mode error) when FILE is not
    available/usable.  So: if the statement executes without raising → FILE is
    usable → return True; on ANY exception → return False.  Using IS NULL returns
    a boolean (never collects a FILE-typed value), which avoids the
    "display of a FILE column fails" issue on older Serverless clients.
    """
    try:
        probe_path = "/Volumes/__gbx_file_probe__/__none__/__probe__.bin"
        spark.sql(f"SELECT try_to_file('{probe_path}') IS NULL").collect()
        return True
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
    # NOTE: on a FILE-enabled session the first call to file_supported() triggers
    # a synchronous feature-detect query (memoized once per session) — so building
    # the Column has a one-time side effect.
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
def open_windowed_via_fileref(file_ref, window, pending, tile_crs=None):
    """Open a FileRef as a rasterio source and read exactly the window.

    The FileRef's .open() method must return a seekable stream (e.g. a
    ``_io.BufferedReader`` or ``io.BytesIO``).  The window and pending
    instructions are applied exactly as in open_tile, then a rasterio
    DatasetReader backed by an in-memory GeoTIFF is yielded.

    A FileRef is a handle to the same source file — it does NOT reproject or
    clip.  When ``tile_crs`` is set and a reprojection is required (the
    requested CRS differs from the source CRS after any pending_srid relabel),
    ``FileRefReadError`` is raised so the caller can degrade to the local-file
    path (which warps correctly).

    Args:
        file_ref: Object with ``.open()`` (returns seekable stream) and
            ``.as_local_file()`` (returns local path) methods — matching the
            real ``pyspark.sql.types.FileRef`` contract.
        window: ``(col_off, row_off, width, height)`` tuple from
            ``VirtualTile.window``.
        pending: 4-tuple ``(bands|None, nodata|None, srid|None, crs_str|None)``
            from ``_parse_pending(tile.metadata)``.
        tile_crs: Optional target CRS string from the tile (``tile.crs``).
            When set and a reprojection would be required, ``FileRefReadError``
            is raised to trigger fallback to the local-file path.

    Yields:
        An open ``rasterio.io.DatasetReader`` covering exactly the window.

    Raises:
        FileRefReadError: if the stream is not seekable, rasterio open fails,
            a reprojection is required, or any other windowed-read setup
            failure.  Callers may degrade to ``file_ref.as_local_file()``.
    """
    # Setup phase: open stream, check seekable, check warp, build tile bytes.
    # The broad except converts any failure here to FileRefReadError so the
    # caller can degrade to the local-file path.  The yield is OUTSIDE this
    # handler so that caller-body exceptions propagate unchanged (not wrapped).
    try:
        stream = file_ref.open()
        if not stream.seekable():
            raise FileRefReadError("FileRef stream is not seekable")

        with rasterio.open(stream) as src:
            # Reprojection check: if tile_crs requests a different CRS than
            # the source (after any pending_srid relabel), the FILE path cannot
            # warp — raise FileRefReadError to degrade to the local-file path.
            if tile_crs is not None:
                _, _, pending_srid, _ = pending
                src_epsg = src.crs.to_epsg() if src.crs else None
                effective_src_epsg = (
                    pending_srid if pending_srid is not None else src_epsg
                )
                want_epsg = _epsg_of_str(tile_crs)
                if want_epsg is not None:
                    if want_epsg != effective_src_epsg:
                        raise FileRefReadError(
                            "Reprojection required; degrading to local-file path"
                        )
                else:
                    # Non-EPSG target (ESRI:*, WKT, PROJ4): cannot warp via FILE path.
                    raise FileRefReadError(
                        "Non-EPSG reprojection required; degrading to local-file path"
                    )

            col_off, row_off, width, height = window
            rio_window = Window(col_off, row_off, width, height)

            from databricks.labs.gbx.pyrx.core.open_tile import _window_dataset_bytes

            tile_bytes = _window_dataset_bytes(src, rio_window, pending=pending)
    except FileRefReadError:
        raise
    except Exception as exc:
        raise FileRefReadError(f"FileRef windowed read failed: {exc}") from exc

    # Yield phase: outside the broad handler so that caller-body exceptions
    # propagate unchanged (not converted to FileRefReadError → RuntimeError).
    with MemoryFile(tile_bytes) as mf:
        with mf.open() as ds:
            yield ds
