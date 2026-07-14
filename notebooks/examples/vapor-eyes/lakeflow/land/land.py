"""Task 1 of the vapor-eyes-lf job: date-parameterized downloader driver.

Runs the GeoBrix sample downloaders idempotently (their own skip-guards avoid
re-downloading valid staged files) and lands raw files into the pipeline's own
Volume subtree. Emits NO Delta tables — the pipeline's Auto Loader bronze layer
inventories the staged files. This file is NOT part of the pipeline and must not
import pyspark.pipelines."""
import argparse
import os

# Dual-context import. Under pytest (tests/conftest.py puts `lakeflow/` on
# sys.path) `land` is a namespace package, so `from land._dates` resolves. As a
# job `spark_python_task` the file is exec()'d with its own dir `land/` on
# sys.path[0] and no `__file__`, so `import land` finds land.py itself (not a
# package) — fall back to the flat sibling import `from _dates`.
try:
    from land._dates import asof_window
except ImportError:  # pragma: no cover - exercised only in the job runtime
    from _dates import asof_window


def _subtree(catalog, schema, volume):
    root = f"/Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf"
    return {
        "root": root, "s5p": f"{root}/s5p", "s2": f"{root}/sentinel2",
        "emit": f"{root}/emit", "wells": f"{root}/wells",
    }


def run_land(spark, sources, *, catalog, schema, volume, date_window,
             s5p_temporal, bbox=(-103.60, 31.05, -102.60, 31.85),
             earthdata_secret="geospatial_docs.vapor_eyes.earthdata_token"):
    from databricks.labs.gbx.sample import (
        EmitDownloader, TropomiDownloader, WellsDownloader)
    dirs = _subtree(catalog, schema, volume)
    for d in dirs.values():
        _mkdir(spark, d)
    staged = {}
    if "s5p" in sources:
        df = TropomiDownloader().download(bbox, dirs["s5p"], temporal=s5p_temporal, spark=spark)
        rows = df.select("out_file_path", "out_file_sz", "is_out_file_valid").collect()
        for r in rows:
            print(f"... s5p granule: valid={r['is_out_file_valid']} "
                  f"sz={r['out_file_sz']} path={r['out_file_path']}")
        staged["s5p"] = len(rows)
        _list_dir(dirs["s5p"], "s5p")
    if "s2" in sources:
        from databricks.labs.gbx.stac import StacClient
        staged["s2"] = _land_s2(spark, StacClient(), bbox, dirs["s2"], date_window)
    if "emit" in sources:
        # EMIT (NASA LP DAAC) needs an Earthdata bearer token. Read it from the
        # UC secret and export EARTHDATA_TOKEN so the downloader's HTTP client
        # picks it up. Guarded: if the secret is unreadable we log and continue —
        # S5P/wells do not need it, and EMIT then fails loudly on its own.
        token = _read_earthdata_token(spark, earthdata_secret)
        if token:
            os.environ["EARTHDATA_TOKEN"] = token
            print(f"... EARTHDATA_TOKEN set from secret '{earthdata_secret}' "
                  f"({len(token)} chars)")
        else:
            print(f"... WARNING: no Earthdata token from '{earthdata_secret}'; "
                  f"EMIT download may fail (S5P/wells unaffected)")
        df = EmitDownloader().download(bbox, dirs["emit"], temporal=date_window, spark=spark)
        staged["emit"] = df.count()
        _list_dir(dirs["emit"], "emit")
    if "wells" in sources:
        df = WellsDownloader().download(bbox, dirs["wells"], spark=spark)
        staged["wells"] = int(df.first()["feature_count"])
        _list_dir(dirs["wells"], "wells")
    print(f"... landed: {staged}")
    return staged


def _get_dbutils(spark):
    """Return a dbutils handle usable from a serverless spark_python_task.
    `pyspark.dbutils.DBUtils(spark)` is the in-cluster path; fall back to the
    SDK's RemoteDbUtils if that import is unavailable."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:  # pragma: no cover - depends on runtime
        from databricks.sdk.dbutils import RemoteDbUtils
        return RemoteDbUtils()


def _read_earthdata_token(spark, secret_ref):
    """Read the Earthdata token from a UC secret.

    `secret_ref` is a dotted path. A 3-part ref (`catalog.schema.key`) uses the
    3-arg UC-secret overload `dbutils.secrets.get(catalog, schema, key)` (matches
    the notebook series); a 2-part ref falls back to the classic
    `dbutils.secrets.get(scope, key)`. Returns None (never raises) on any failure
    so the caller can degrade gracefully."""
    parts = secret_ref.split(".")
    try:
        dbutils = _get_dbutils(spark)
    except Exception as e:
        print(f"... dbutils unavailable for secret read: {type(e).__name__}: {e}")
        return None
    try:
        if len(parts) == 3:
            return dbutils.secrets.get(parts[0], parts[1], parts[2])
        if len(parts) == 2:
            return dbutils.secrets.get(parts[0], parts[1])
        print(f"... unexpected secret ref '{secret_ref}' (want catalog.schema.key)")
        return None
    except Exception as e:
        print(f"... secret read failed for '{secret_ref}': {type(e).__name__}: {e}")
        return None


def _list_dir(path, label):
    """Driver-side listing of a staged Volume dir — confirms files persisted
    (the download UDF writes on executors; a swallowed executor-side write leaves
    a nonzero row count but an empty dir)."""
    try:
        import os
        entries = os.listdir(path)
        print(f"... {label} dir {path}: {len(entries)} file(s): {entries[:10]}")
    except Exception as e:
        print(f"... {label} dir listing failed {path}: {type(e).__name__}: {e}")


def _mkdir(spark, path):
    try:
        import os
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        print(f"... mkdir skipped {path}: {type(e).__name__}")


def _land_s2(spark, stac, bbox, out_dir, date_window):
    # Filled in Phase 2 (S2 needs a computed top-hotspot window); Phase 1 no-op.
    return 0


def main():
    from pyspark.sql import SparkSession
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="s5p")
    ap.add_argument("--window")
    ap.add_argument("--asof")
    ap.add_argument("--catalog", default="geospatial_docs")
    ap.add_argument("--schema", default="vapor_eyes_lf")
    ap.add_argument("--volume", default="data")
    ap.add_argument("--s5p-temporal", default="2024-08-23/2024-08-24")
    ap.add_argument("--earthdata-secret",
                    default="geospatial_docs.vapor_eyes.earthdata_token")
    a = ap.parse_args()
    window = a.window or (asof_window(a.asof) if a.asof else "2023-07-15/2023-08-20")
    spark = SparkSession.builder.getOrCreate()
    run_land(spark, a.sources.split(","), catalog=a.catalog, schema=a.schema,
             volume=a.volume, date_window=window, s5p_temporal=a.s5p_temporal,
             earthdata_secret=a.earthdata_secret)


if __name__ == "__main__":
    main()
