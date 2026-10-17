"""File-level change handling for incremental ingestion (``on_change``).

Auto Loader re-delivers a file whose content was overwritten (``allowOverwrites``,
all modes). ``scd2`` (default) keeps every distinct content version of a file: rows
are append-only and a small versions table (AUTO CDC SCD 2, history on the content
hash) records when each version was current. ``replace`` keeps the latest content
only (one atomic ``replaceWhere`` per batch). ``append`` writes every delivery, so a
re-delivered file is duplicated by design. Under ``scd2`` and ``replace`` a
re-delivery with unchanged content writes nothing.

Writes (``write_batch``) run in an SDP ``foreach_batch_sink``; ``declare_ingest``
wires the pipeline. Paths are Unity Catalog Volume (FUSE) paths; object-store URIs
are rejected. Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.paths import (
    SOURCE_MTIME_COL,
    SOURCE_PATH_COL,
    _fuse_path,
    default_glob,
    path_schema,
    read_paths,
)

_LOG = logging.getLogger(__name__)

ON_CHANGE_MODES = ("scd2", "replace", "append")
SOURCE_HASH_COL = "_source_hash"
SOURCE_SIZE_COL = "_source_size"
_EVENTS_SCHEMA = (
    f"path string, modificationTime timestamp, length long, {SOURCE_HASH_COL} string"
)
_MTIME_US = "_mtime_us"  # driver-side only: exact epoch microseconds of _source_mtime
# A delivery is current while the file keeps its size and its mtime stays within
# this many microseconds of the delivered one.
_MTIME_TOLERANCE_US = 1_000_000
# Driver threads for the per-file stat checks in write_batch.
_STAT_WORKERS = 16


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
            local = _fuse_path(p, "hash_files")
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


def _split_ident(name: str) -> List[str]:
    """Parts of a dotted table name. A part may be backtick-quoted (it can then hold
    dots; a doubled backtick is a literal one); an unquoted part is taken as is."""
    parts: List[str] = []
    i, n = 0, len(name)
    while True:
        if i < n and name[i] == "`":
            buf, j = [], i + 1
            while True:
                k = name.find("`", j)
                if k < 0:
                    raise ValueError(f"Unclosed backtick in table name {name!r}.")
                if name.startswith("``", k):
                    buf.append(name[j : k + 1])
                    j = k + 2
                    continue
                buf.append(name[j:k])
                i = k + 1
                break
            if i < n and name[i] != ".":
                raise ValueError(f"Invalid table name {name!r}.")
            parts.append("".join(buf))
        else:
            k = name.find(".", i)
            k = n if k < 0 else k
            parts.append(name[i:k])
            i = k
        if i >= n:
            return parts
        i += 1


def _quote_parts(parts: List[str]) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def _quote(name: str) -> str:
    """Backtick-quote every part of a table name for SQL / ``spark.table`` /
    ``saveAsTable``. Accepts plain and already quoted names (idempotent)."""
    return _quote_parts(_split_ident(name))


_PLAIN_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _ident(parts: List[str]) -> str:
    """Dotted name that quotes only the parts that need it (pipeline dataset names)."""
    return ".".join(
        p if _PLAIN_IDENT.match(p) else "`" + p.replace("`", "``") + "`" for p in parts
    )


def _suffixed(name: str, suffix: str) -> str:
    """Quoted ``name`` with ``suffix`` appended to its last part."""
    parts = _split_ident(name)
    return _quote_parts(parts[:-1] + [parts[-1] + suffix])


def _table_exists(spark, name: str) -> bool:
    try:  # catalog.tableExists is not allowed inside pipeline sinks
        spark.table(_quote(name)).columns
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
    # A semi-join against the batch's paths, not a literal IN list: a batch can
    # name thousands of files.
    keys = spark.createDataFrame([(p,) for p in paths], f"{SOURCE_PATH_COL} string")
    out: Dict[str, Set[str]] = {}
    for r in (
        spark.table(_quote(target))
        .select(SOURCE_PATH_COL, SOURCE_HASH_COL)
        .join(F.broadcast(keys), SOURCE_PATH_COL, "left_semi")
        .distinct()
        .collect()
    ):
        out.setdefault(r[0], set()).add(r[1])
    return out


def _current_view_sql(target: str, versions: str) -> str:
    """``<target>_current``: each file's rows of its latest READABLE version, the
    newest version (by ``__START_AT``) whose content has rows in ``target``. A
    delivery that could not be read (set aside in ``<t>_bad_records``, or never
    written) is still the file's latest version in ``<t>_file_versions``, but it
    does not hide the last content that was read."""
    p, h, t = SOURCE_PATH_COL, SOURCE_HASH_COL, _quote(target)
    return (
        f"CREATE VIEW IF NOT EXISTS {_suffixed(target, '_current')} AS "
        f"WITH readable AS (SELECT v.{p}, v.{h}, v.__START_AT FROM {_quote(versions)} v "
        f"LEFT SEMI JOIN (SELECT DISTINCT {p}, {h} FROM {t}) s "
        f"ON v.{p} = s.{p} AND v.{h} = s.{h}), "
        f"latest AS (SELECT {p}, max_by({h}, __START_AT) AS {h} FROM readable "
        f"GROUP BY {p}) "
        f"SELECT r.* FROM {t} r JOIN latest l ON r.{p} = l.{p} AND r.{h} = l.{h}"
    )


def _ensure_current_view(spark, target: str, versions: str) -> None:
    """Create ``<target>_current`` if missing. Best effort: the versions table may not
    exist yet (the flows run in parallel); the next batch retries."""
    if _table_exists(spark, _suffixed(target, "_current")):
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
    name = _quote(target)
    if not _table_exists(spark, name):
        rows_df.write.mode("append").clusterBy(SOURCE_PATH_COL).saveAsTable(name)
    elif mode == "replace":
        (
            rows_df.write.mode("overwrite")
            .option("replaceWhere", replace_predicate(paths))
            .saveAsTable(name)
        )
    else:
        rows_df.write.mode("append").saveAsTable(name)
    if mode == "scd2" and versions:
        _ensure_current_view(spark, target, versions)


def _delivery_is_current(event: dict) -> bool:
    """True if the file on disk still matches the delivery: the same size, and an
    mtime within 1 second of the delivered one.

    Auto Loader truncates ``modificationTime`` to milliseconds and some file systems
    keep coarser mtimes than ``stat`` reports, hence the 1 second tolerance. A file
    overwritten since it was hashed (a newer delivery is pending) or deleted is not
    current. An object-store URI raises ``ValueError`` naming ``declare_ingest``,
    the function whose flow runs this check.
    """
    local = _fuse_path(event[SOURCE_PATH_COL], "declare_ingest")
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
    if abs(st.st_mtime_ns // 1000 - event[_MTIME_US]) > _MTIME_TOLERANCE_US:
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
    schema_path=None,
    schema=None,
    num_partitions=None,
):
    """foreach_batch_sink body: ``batch_df`` = ``hash_files`` rows. Returns files written.

    The rows' schema is ``schema`` (internal: resolved once by ``declare_ingest``) or
    else read from ``schema_path``.

    Before reading, each planned file is stat-ed on the driver (in a small thread
    pool) and skipped (with a warning) if its size or mtime no longer matches the
    delivery, so a row's ``_source_hash`` describes the content it was read from; the
    skipped file's newer delivery arrives in a later batch. Residual races: a file
    overwritten between that stat and the parse (or with the same size within a
    second) can still be read with the older hash (the next delivery of the file is
    written and, under ``scd2``, recorded as the current version); under ``replace``,
    a file deleted between that stat and the parse yields no rows but is still in
    the ``replaceWhere``, so its earlier rows are removed.
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
    planned = plan_writes(events, stored, mode)
    if not planned:
        return 0
    # Stat checks run concurrently: a vanished file costs its FUSE retries, and
    # those must not add up across the batch.
    with ThreadPoolExecutor(max_workers=min(_STAT_WORKERS, len(planned))) as pool:
        current = list(pool.map(_delivery_is_current, planned))
    todo = [e for e, ok in zip(planned, current) if ok]
    if not todo:
        return 0
    # mtimes travel as epoch microseconds (no Python datetime round trip), so the
    # join below matches the rows' _source_mtime exactly.
    files = spark.createDataFrame(
        [(e[SOURCE_PATH_COL], e[_MTIME_US], e[SOURCE_HASH_COL]) for e in todo],
        f"path string, {_MTIME_US} long, {SOURCE_HASH_COL} string",
    ).select(
        "path",
        F.timestamp_micros(F.col(_MTIME_US)).alias("modificationTime"),
        SOURCE_HASH_COL,
    )
    rows = read_paths(
        files,
        format,
        options,
        schema_path=schema_path,
        schema=schema,
        num_partitions=num_partitions,
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


def _ingest_setup(
    target, source_dir, format, on_change, glob, schema_path, options=None
) -> dict:
    """Validated names and inputs for :func:`declare_ingest` (no session needed)."""
    _check_mode(on_change)
    try:
        parts = _split_ident(target)
    except ValueError:
        parts = []
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"target must be a fully qualified catalog.schema.table name; got {target!r}."
        )
    _fuse_path(source_dir, "declare_ingest")
    if schema_path:
        _fuse_path(schema_path, "declare_ingest")
    short = parts[-1]

    def qualified(name: str) -> str:
        return _ident(parts[:-1] + [name])

    return {
        "rows": qualified(short),
        "events": qualified(f"{short}_file_events"),
        "versions": qualified(f"{short}_file_versions"),
        "versions_flow": f"{short}_versions_flow",
        "sink": f"{short}_rows_sink",
        "rows_flow": f"{short}_rows_flow",
        "glob": glob or default_glob(format, options),
        "schema_src": schema_path or source_dir,
    }


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

    Creates (names from ``target`` = ``catalog.schema.t``, all in ``catalog.schema``):
    ``t_file_events`` and, for ``scd2``, ``t_file_versions`` (AUTO CDC SCD 2 on
    ``_source_path``, history on ``_source_hash``) plus the view ``<target>_current``;
    rows land in ``target``. ``append`` declares a plain streaming table ``target``
    instead.

    Auto Loader delivers the files under ``source_dir`` whose name matches ``glob``
    (default: the format's :func:`~databricks.labs.gbx.ds.paths.default_glob`
    pattern; ``pmtiles_gbx`` needs its ``source`` option). The table schema is
    resolved once, here, from ``schema_path`` (default ``source_dir``; a folder's
    first file matching ``glob``) and reused by every micro-batch. ``source_dir``
    and ``schema_path`` are Volume paths.
    """
    setup = _ingest_setup(
        target, source_dir, format, on_change, glob, schema_path, options
    )
    schema = path_schema(format, options, setup["schema_src"], glob=setup["glob"])

    from pyspark import pipelines as dp
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.getActiveSession()

    def _files():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.allowOverwrites", "true")
            .option("pathGlobFilter", setup["glob"])
            .load(source_dir)
            .select("path", "modificationTime", "length")
        )

    if on_change == "append":

        @dp.table(name=setup["rows"])
        def _rows():
            return read_paths(
                _files(),
                format,
                options,
                schema=schema,
                num_partitions=num_partitions,
            )

        return

    events, versions = setup["events"], setup["versions"]

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
            name=setup["versions_flow"],
        )

    @dp.foreach_batch_sink(name=setup["sink"])
    def _sink(batch_df, batch_id):
        write_batch(
            batch_df,
            setup["rows"],
            format,
            options,
            on_change,
            versions=versions if on_change == "scd2" else None,
            schema=schema,
            num_partitions=num_partitions,
        )

    @dp.append_flow(name=setup["rows_flow"], target=setup["sink"])
    def _flow():
        return spark.readStream.table(_quote(events))
