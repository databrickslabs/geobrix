"""Feature-detect for Databricks FILE type support.

Checks once per SparkSession whether FILE is available, using a plan-level
try_to_file mint followed by a UDF consume roundtrip. Result is memoized so
subsequent calls are free.

Serverless-safe: uses only spark.sql + UDF registration; no .rdd / _jvm /
_jsc / sparkContext / conf.set.
"""

import os

from pyspark.sql import SparkSession

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
