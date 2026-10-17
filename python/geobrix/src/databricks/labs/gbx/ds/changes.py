"""File-level change handling for incremental ingestion (``on_change``).

``scd2`` (default) keeps every distinct content version of a file: rows are
append-only and a small versions table (AUTO CDC SCD 2, history on the content
hash) records when each version was current. ``replace`` keeps the latest
content only (one atomic ``replaceWhere`` per batch). ``append`` writes every
delivery. Unchanged re-deliveries write nothing except under ``append``.

Writes (``write_batch``) run in an SDP ``foreach_batch_sink``; ``declare_ingest``
wires the pipeline. Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

from databricks.labs.gbx.ds.paths import SOURCE_MTIME_COL, SOURCE_PATH_COL, read_paths

ON_CHANGE_MODES = ("scd2", "replace", "append")
SOURCE_HASH_COL = "_source_hash"
SOURCE_SIZE_COL = "_source_size"
_EVENTS_SCHEMA = (
    f"path string, modificationTime timestamp, length long, {SOURCE_HASH_COL} string"
)


def _check_mode(mode: str) -> str:
    if mode not in ON_CHANGE_MODES:
        raise ValueError(f"on_change must be one of {ON_CHANGE_MODES}; got {mode!r}.")
    return mode


def _hash_batches(batches):
    import hashlib

    import pyarrow as pa

    from databricks.labs.gbx.ds import _listing

    for b in batches:
        out = []
        for p in b.column("path").to_pylist():
            local = _listing.to_local_path(p)

            def _digest(local=local):
                h = hashlib.sha256()
                with open(local, "rb") as f:
                    for chunk in iter(lambda: f.read(8 << 20), b""):
                        h.update(chunk)
                return h.hexdigest()

            out.append(_listing._retry_transient(_digest))
        yield b.append_column(SOURCE_HASH_COL, pa.array(out, pa.string()))


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
    """Events whose rows must be written for ``mode`` (see module docstring)."""
    _check_mode(mode)
    if mode == "append":
        return list(events)
    ordered = sorted(events, key=lambda e: (e[SOURCE_PATH_COL], e[SOURCE_MTIME_COL]))
    if mode == "replace":
        newest = {}
        for e in ordered:
            newest[e[SOURCE_PATH_COL]] = e
        return [
            e
            for p, e in sorted(newest.items())
            if e[SOURCE_HASH_COL] not in stored.get(p, set())
        ]
    seen, out = set(), []
    for e in ordered:
        key = (e[SOURCE_PATH_COL], e[SOURCE_HASH_COL])
        if key in seen or e[SOURCE_HASH_COL] in stored.get(key[0], set()):
            continue
        seen.add(key)
        out.append(e)
    return out


def replace_predicate(paths: List[str]) -> str:
    quoted = ", ".join("'" + p.replace("'", "''") + "'" for p in paths)
    return f"{SOURCE_PATH_COL} IN ({quoted})"


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


def _write_rows(
    rows_df, target: str, mode: str, paths: List[str], versions: Optional[str]
) -> None:
    spark = rows_df.sparkSession
    if not _table_exists(spark, target):
        rows_df.write.mode("overwrite").clusterBy(SOURCE_PATH_COL).saveAsTable(target)
        if mode == "scd2" and versions:
            spark.sql(
                f"CREATE VIEW IF NOT EXISTS {target}_current AS SELECT r.* FROM {target} r "
                f"JOIN {versions} v ON r.{SOURCE_PATH_COL} = v.{SOURCE_PATH_COL} "
                f"AND r.{SOURCE_HASH_COL} = v.{SOURCE_HASH_COL} WHERE v.__END_AT IS NULL"
            )
        return
    if mode == "replace":
        (
            rows_df.write.mode("overwrite")
            .option("replaceWhere", replace_predicate(paths))
            .saveAsTable(target)
        )
    else:
        rows_df.write.mode("append").saveAsTable(target)


def write_batch(batch_df, target, format, options, mode, *, versions=None, schema_path):
    """foreach_batch_sink body: ``batch_df`` = ``hash_files`` rows. Returns files written."""
    from pyspark.sql import functions as F

    _check_mode(mode)
    spark = batch_df.sparkSession
    events = [r.asDict() for r in batch_df.collect()]
    if not events:
        return 0
    paths = sorted({e[SOURCE_PATH_COL] for e in events})
    stored = {} if mode == "append" else _stored_hashes(spark, target, paths, mode)
    todo = plan_writes(events, stored, mode)
    if not todo:
        return 0
    files = spark.createDataFrame(
        [(e[SOURCE_PATH_COL], e[SOURCE_MTIME_COL], e[SOURCE_HASH_COL]) for e in todo],
        f"path string, modificationTime timestamp, {SOURCE_HASH_COL} string",
    )
    rows = read_paths(files, format, options, schema_path=schema_path)
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
    fully qualified and in the pipeline's catalog/schema.
    """
    from pyspark import pipelines as dp
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    _check_mode(on_change)
    spark = SparkSession.getActiveSession()
    short = target.split(".")[-1]
    qualifier = target.rsplit(".", 1)[0]
    schema_src = schema_path or source_dir

    def _files():
        r = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option(
                "cloudFiles.allowOverwrites",
                "true" if on_change != "append" else "false",
            )
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
        )

    @dp.append_flow(name=f"{short}_rows_flow", target=sink)
    def _flow():
        return spark.readStream.table(events)
