"""File-level change handling for incremental ingestion (``on_change``).

Auto Loader re-delivers a file whose content was overwritten (``allowOverwrites``,
all modes). ``scd2`` (default) keeps every distinct content version of a file: rows
are append-only and a small versions table (AUTO CDC SCD 2, history on the content
hash) records when each version was current. ``replace`` keeps the latest content
only (one atomic ``replaceWhere`` per batch). ``append`` writes every delivery, so a
re-delivered file is duplicated by design. Under ``scd2`` and ``replace`` a
re-delivery with unchanged content writes nothing.

Writes (``write_batch``) run in an SDP ``foreach_batch_sink``; ``declare_ingest``
wires the pipeline. Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional, Set

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.paths import SOURCE_MTIME_COL, SOURCE_PATH_COL, read_paths

_LOG = logging.getLogger(__name__)

ON_CHANGE_MODES = ("scd2", "replace", "append")
SOURCE_HASH_COL = "_source_hash"
SOURCE_SIZE_COL = "_source_size"
_EVENTS_SCHEMA = (
    f"path string, modificationTime timestamp, length long, {SOURCE_HASH_COL} string"
)
_MTIME_US = "_mtime_us"  # driver-side only: exact epoch microseconds of _source_mtime


def _check_mode(mode: str) -> str:
    if mode not in ON_CHANGE_MODES:
        raise ValueError(f"on_change must be one of {ON_CHANGE_MODES}; got {mode!r}.")
    return mode


def _sha256(local: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(local, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_batches(batches):
    """Append the SHA-256 of each named file; a file that no longer exists is
    skipped with a warning (like ``read_paths``)."""
    import pyarrow as pa

    for b in batches:
        digests = []
        for p in b.column("path").to_pylist():
            local = _listing.to_local_path(p)
            try:
                digests.append(
                    _listing._retry_transient(lambda local=local: _sha256(local))
                )
            except FileNotFoundError:
                _LOG.warning("hash_files: %s no longer exists; skipped", local)
                digests.append(None)
        b = b.append_column(SOURCE_HASH_COL, pa.array(digests, pa.string()))
        if None in digests:
            b = b.filter(pa.array([d is not None for d in digests], pa.bool_()))
        yield b


def hash_files(df, path_col="path", mtime_col="modificationTime", size_col="length"):
    """One row per file delivery: bare path, mtime, SHA-256, size (batch or stream)."""
    from pyspark.sql import functions as F

    src = df.select(
        F.col(path_col).alias("path"),
        F.col(mtime_col).cast("timestamp").alias("modificationTime"),
        F.col(size_col).cast("long").alias("length"),
    )
    return src.mapInArrow(_hash_batches, _EVENTS_SCHEMA).select(
        F.regexp_replace("path", "^(dbfs|file):", "").alias(SOURCE_PATH_COL),
        F.col("modificationTime").alias(SOURCE_MTIME_COL),
        F.col(SOURCE_HASH_COL),
        F.col("length").alias(SOURCE_SIZE_COL),
    )


def plan_writes(
    events: List[dict], stored: Dict[str, Set[str]], mode: str
) -> List[dict]:
    """Events whose rows must be written for ``mode``.

    ``append``: every event. ``scd2`` / ``replace``: only the newest event per path
    in the batch (rows are read from the file as it is now, so older deliveries in
    the same batch would carry a hash that does not match their content), and only
    if its hash is not already stored for that path (``stored`` holds every stored
    hash per path for ``scd2``, the single current hash for ``replace``).
    """
    _check_mode(mode)
    if mode == "append":
        return list(events)
    ordered = sorted(events, key=lambda e: (e[SOURCE_PATH_COL], e[SOURCE_MTIME_COL]))
    newest = {}
    for e in ordered:
        newest[e[SOURCE_PATH_COL]] = e
    return [
        e
        for p, e in sorted(newest.items())
        if e[SOURCE_HASH_COL] not in stored.get(p, set())
    ]


def replace_predicate(paths: List[str]) -> str:
    """``_source_path IN (...)`` as a Spark SQL predicate. Spark string literals
    treat backslash as the escape character and ``''`` as literal concatenation,
    so backslash and quote are both escaped with a backslash."""
    quoted = ", ".join(
        "'" + p.replace("\\", "\\\\").replace("'", "\\'") + "'" for p in paths
    )
    return f"{SOURCE_PATH_COL} IN ({quoted})"


def _quote(name: str) -> str:
    """Backtick-quote each dot-separated identifier part (embedded backticks doubled)."""
    return ".".join("`" + part.replace("`", "``") + "`" for part in name.split("."))


def _table_exists(spark, name: str) -> bool:
    try:  # catalog.tableExists is not allowed inside pipeline sinks
        spark.table(name).columns
        return True
    except Exception as e:  # noqa: BLE001 - narrowed by the message check
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
            raise
        return False


def _stored_hashes(
    spark, target: str, paths: List[str], mode: str
) -> Dict[str, Set[str]]:
    from pyspark.sql import functions as F

    if not paths or not _table_exists(spark, target):
        return {}
    out: Dict[str, Set[str]] = {}
    for r in (
        spark.table(target)
        .where(F.col(SOURCE_PATH_COL).isin(paths))
        .select(SOURCE_PATH_COL, SOURCE_HASH_COL)
        .distinct()
        .collect()
    ):
        out.setdefault(r[0], set()).add(r[1])
    return out


def _current_view_sql(target: str, versions: str) -> str:
    return (
        f"CREATE VIEW IF NOT EXISTS {_quote(target + '_current')} AS "
        f"SELECT r.* FROM {_quote(target)} r JOIN {_quote(versions)} v "
        f"ON r.{SOURCE_PATH_COL} = v.{SOURCE_PATH_COL} "
        f"AND r.{SOURCE_HASH_COL} = v.{SOURCE_HASH_COL} WHERE v.__END_AT IS NULL"
    )


def _ensure_current_view(spark, target: str, versions: str) -> None:
    """Create ``<target>_current`` if missing. Best effort: the versions table may not
    exist yet (the flows run in parallel); the next batch retries."""
    if _table_exists(spark, _quote(target + "_current")):
        return
    try:
        spark.sql(_current_view_sql(target, versions))
    except Exception as e:  # noqa: BLE001 - narrowed by the message check
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
            raise
        _LOG.warning(
            "%s_current not created yet (%s not available); will retry",
            target,
            versions,
        )


def _write_rows(
    rows_df, target: str, mode: str, paths: List[str], versions: Optional[str]
) -> None:
    spark = rows_df.sparkSession
    if not _table_exists(spark, target):
        rows_df.write.mode("append").clusterBy(SOURCE_PATH_COL).saveAsTable(target)
    elif mode == "replace":
        (
            rows_df.write.mode("overwrite")
            .option("replaceWhere", replace_predicate(paths))
            .saveAsTable(target)
        )
    else:
        rows_df.write.mode("append").saveAsTable(target)
    if mode == "scd2" and versions:
        _ensure_current_view(spark, target, versions)


def _delivery_is_current(event: dict) -> bool:
    """True if the file on disk still matches the delivery (size and mtime).

    ``binaryFile`` mtimes have millisecond resolution while ``stat`` has finer, so
    they are compared to within 1 ms. A file overwritten since it was hashed (a newer
    delivery is pending) or deleted is not current.
    """
    local = _listing.to_local_path(event[SOURCE_PATH_COL])
    try:
        st = _listing._retry_transient(lambda: os.stat(local))
    except FileNotFoundError:
        _LOG.warning("on_change: %s no longer exists; delivery skipped", local)
        return False
    if st.st_size != event[SOURCE_SIZE_COL]:
        _LOG.warning(
            "on_change: %s changed since it was hashed; delivery skipped", local
        )
        return False
    if abs(st.st_mtime_ns // 1_000_000 - event[_MTIME_US] // 1000) > 1:
        _LOG.warning(
            "on_change: %s was modified since it was hashed; delivery skipped", local
        )
        return False
    return True


def write_batch(
    batch_df,
    target,
    format,
    options,
    mode,
    *,
    versions=None,
    schema_path,
    num_partitions=None,
):
    """foreach_batch_sink body: ``batch_df`` = ``hash_files`` rows. Returns files written.

    Before reading, each planned file is stat-ed on the driver and skipped (with a
    warning) if its size or mtime no longer matches the delivery, so a row's
    ``_source_hash`` describes the content it was read from; the skipped file's newer
    delivery arrives in a later batch. A file overwritten between that stat and the
    parse can still be read with the older hash (residual race; the next delivery of
    the file is written and, under ``scd2``, recorded as the current version).
    """
    from pyspark.sql import functions as F

    _check_mode(mode)
    spark = batch_df.sparkSession
    events = [
        r.asDict()
        for r in batch_df.withColumn(
            _MTIME_US, F.unix_micros(F.col(SOURCE_MTIME_COL))
        ).collect()
    ]
    if not events:
        return 0
    paths = sorted({e[SOURCE_PATH_COL] for e in events})
    stored = {} if mode == "append" else _stored_hashes(spark, target, paths, mode)
    todo = [e for e in plan_writes(events, stored, mode) if _delivery_is_current(e)]
    if not todo:
        return 0
    files = spark.createDataFrame(
        [(e[SOURCE_PATH_COL], e[SOURCE_MTIME_COL], e[SOURCE_HASH_COL]) for e in todo],
        f"path string, modificationTime timestamp, {SOURCE_HASH_COL} string",
    )
    rows = read_paths(
        files, format, options, schema_path=schema_path, num_partitions=num_partitions
    )
    keyed = files.select(
        F.col("path").alias(SOURCE_PATH_COL),
        F.col("modificationTime").alias(SOURCE_MTIME_COL),
        SOURCE_HASH_COL,
    )
    rows = rows.join(F.broadcast(keyed), [SOURCE_PATH_COL, SOURCE_MTIME_COL])
    _write_rows(
        rows, target, mode, sorted({e[SOURCE_PATH_COL] for e in todo}), versions
    )
    return len(todo)


def declare_ingest(
    target,
    source_dir,
    format,
    options=None,
    *,
    on_change="scd2",
    glob=None,
    num_partitions=None,
    schema_path=None,
):
    """Declare an incremental file-ingestion flow in an SDP pipeline.

    Creates (names from ``target``'s last part ``t``): ``t_file_events`` and, for
    ``scd2``, ``t_file_versions`` (AUTO CDC SCD 2 on ``_source_path``, history on
    ``_source_hash``) plus the view ``<target>_current``; rows land in ``target``.
    ``append`` declares a plain streaming table ``target`` instead. ``target`` must be
    a fully qualified ``catalog.schema.table`` in the pipeline's catalog/schema.
    """
    _check_mode(on_change)
    parts = target.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"target must be a fully qualified catalog.schema.table name; got {target!r}."
        )

    from pyspark import pipelines as dp
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession()
    short = parts[-1]
    qualifier = target.rsplit(".", 1)[0]
    schema_src = schema_path or source_dir

    def _files():
        r = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.allowOverwrites", "true")
        )
        if glob:
            r = r.option("pathGlobFilter", glob)
        return r.load(source_dir).select("path", "modificationTime", "length")

    if on_change == "append":

        @dp.table(name=short)
        def _rows():
            return read_paths(
                _files(),
                format,
                options,
                schema_path=schema_src,
                num_partitions=num_partitions,
            )

        return

    events, versions = f"{short}_file_events", f"{short}_file_versions"

    @dp.table(name=events)
    def _events():
        return hash_files(_files())

    if on_change == "scd2":
        dp.create_streaming_table(versions)
        dp.create_auto_cdc_flow(
            target=versions,
            source=events,
            keys=[SOURCE_PATH_COL],
            sequence_by=F.col(SOURCE_MTIME_COL),
            stored_as_scd_type=2,
            track_history_column_list=[SOURCE_HASH_COL],
            name=f"{short}_versions_flow",
        )

    sink = f"{short}_rows_sink"
    full_versions = f"{qualifier}.{versions}" if on_change == "scd2" else None

    @dp.foreach_batch_sink(name=sink)
    def _sink(batch_df, batch_id):
        write_batch(
            batch_df,
            target,
            format,
            options,
            on_change,
            versions=full_versions,
            schema_path=schema_src,
            num_partitions=num_partitions,
        )

    @dp.append_flow(name=f"{short}_rows_flow", target=sink)
    def _flow():
        return spark.readStream.table(events)
