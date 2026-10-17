"""Reader-over-paths: run a light ``*_gbx`` reader over a DataFrame of file paths.

Each row names one file; the reader's own DataSource contract (``schema()`` ->
``reader().partitions()`` -> ``read()``) parses it inside ``mapInArrow``, so the
same code serves batch reads, Auto Loader streams, and the Lakeflow connector.

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Iterator, Optional

import pyarrow as pa
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from databricks.labs.gbx.ds import _listing

_LOG = logging.getLogger(__name__)

SOURCE_PATH_COL = "_source_path"
SOURCE_MTIME_COL = "_source_mtime"
_TS = pa.timestamp("us", tz="UTC")

# Readers whose read() yields Arrow RecordBatches, one partition per file.
SUPPORTED_FORMATS = (
    "vector_gbx",
    "shapefile_gbx",
    "geojson_gbx",
    "geojsonl_gbx",
    "gpkg_gbx",
    "filegdb_gbx",
    "lidar_gbx",
    "exif_gbx",
)


def _sources() -> Dict[str, type]:
    from databricks.labs.gbx.ds.register import _SOURCES

    return {src.name(): src for src in _SOURCES}


def resolve_format(fmt: str) -> type:
    """DataSource class for a light reader name; ``_gbx`` suffix optional, any case."""
    name = (fmt or "").strip().lower()
    if not name.endswith("_gbx"):
        name += "_gbx"
    if name not in SUPPORTED_FORMATS:
        raise ValueError(
            f"read_paths does not support {fmt!r}. Supported formats: "
            f"{', '.join(SUPPORTED_FORMATS)}."
        )
    return _sources()[name]


def _lineage_fields() -> list:
    return [
        StructField(SOURCE_PATH_COL, StringType(), True),
        StructField(SOURCE_MTIME_COL, TimestampType(), True),
    ]


def _reader_schema(fmt: str, options: Optional[dict], path: str) -> StructType:
    ds = resolve_format(fmt)({**(options or {}), "path": _listing.to_local_path(path)})
    schema = ds.schema()
    if isinstance(schema, str):
        schema = StructType.fromDDL(schema)
    return schema


def path_schema(
    fmt: str, options: Optional[dict], schema_path: str, lineage: bool = True
) -> StructType:
    """Output schema: the reader schema of ``schema_path`` (+ lineage columns)."""
    base = _reader_schema(fmt, options, schema_path)
    return StructType(list(base.fields) + (_lineage_fields() if lineage else []))


def _base(expected: StructType) -> StructType:
    return StructType(
        [
            f
            for f in expected.fields
            if f.name not in (SOURCE_PATH_COL, SOURCE_MTIME_COL)
        ]
    )


def _norm_type(t: pa.DataType) -> pa.DataType:
    """Collapse Arrow large/plain variants that map to the same Spark type."""
    if pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_large_binary(t):
        return pa.binary()
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        return pa.list_(_norm_type(t.value_type))
    return t


def _check_schema(path: str, got: pa.Schema, want: pa.Schema) -> None:
    g = {f.name: _norm_type(f.type) for f in got}
    w = {f.name: _norm_type(f.type) for f in want}
    if g == w:
        return
    missing = sorted(set(w) - set(g))
    extra = sorted(set(g) - set(w))
    changed = sorted(n for n in set(g) & set(w) if g[n] != w[n])
    raise ValueError(
        f"{path}: schema differs from the table schema "
        f"(missing {missing}, extra {extra}, changed types {changed})."
    )


def iter_path_batches(
    path: str,
    mtime_us: Optional[int],
    fmt: str,
    options: Optional[dict],
    expected: Optional[StructType] = None,
    lineage: bool = True,
) -> Iterator[pa.RecordBatch]:
    """Arrow batches for one file (session-free). A file that no longer exists is
    skipped with a warning; ``expected`` (lineage included or not) is enforced."""
    from pyspark.sql.pandas.types import to_arrow_schema

    local = _listing.to_local_path(path)
    try:
        _listing._retry_transient(lambda: os.stat(local))
    except FileNotFoundError:
        _LOG.warning("read_paths: %s no longer exists; skipped", local)
        return
    ds = resolve_format(fmt)({**(options or {}), "path": local})
    schema = (
        _base(expected) if expected is not None else _reader_schema(fmt, options, local)
    )
    want = to_arrow_schema(schema)
    reader = ds.reader(schema)
    for part in reader.partitions():
        for batch in reader.read(part):
            if batch.num_rows == 0:
                # A zero-row batch (e.g. a zero-feature GeoJSON) carries no usable
                # schema; there is nothing to emit or validate.
                continue
            _check_schema(local, batch.schema, want)
            if batch.schema.names != want.names:
                # Same columns, different file order: realign to the positional
                # output schema so values never land in the wrong column.
                batch = batch.select(want.names)
            if lineage:
                n = batch.num_rows
                batch = batch.append_column(
                    SOURCE_PATH_COL, pa.array([local] * n, pa.string())
                )
                batch = batch.append_column(
                    SOURCE_MTIME_COL, pa.array([mtime_us] * n, _TS)
                )
            yield batch


def read_paths(
    df,
    format: str,
    options: Optional[dict] = None,
    *,
    path_col: str = "path",
    mtime_col: Optional[str] = "modificationTime",
    schema_path: Optional[str] = None,
    num_partitions: Optional[int] = None,
    lineage: bool = True,
):
    """Read every file named in ``df[path_col]`` with a light reader.

    ``df`` may be batch or streaming (e.g. Auto Loader ``binaryFile`` projected to
    ``path, modificationTime``). The output schema comes from ``schema_path``
    (default: the first path of a batch ``df``; required for a stream). A file
    deleted before it is read is skipped with a warning.
    """
    from pyspark.sql import functions as F

    resolve_format(format)
    if schema_path is None:
        if df.isStreaming:
            raise ValueError(
                "read_paths on a streaming DataFrame needs schema_path= (a file or "
                "folder whose first file defines the table schema)."
            )
        first = df.select(path_col).limit(1).collect()
        if not first:
            raise ValueError("read_paths: empty DataFrame and no schema_path given.")
        schema_path = first[0][0]
    schema = path_schema(format, options, schema_path, lineage)
    cols = [F.col(path_col).alias("path")]
    cols.append(
        F.col(mtime_col).cast("timestamp").alias("mtime")
        if mtime_col
        else F.lit(None).cast("timestamp").alias("mtime")
    )
    src = df.select(*cols)
    if num_partitions:
        src = src.repartition(int(num_partitions), "path")
    fmt, opts, keep = format, dict(options or {}), lineage

    def _read(batches):
        for b in batches:
            paths = b.column("path").to_pylist()
            mtimes = b.column("mtime").cast(pa.int64()).to_pylist()
            for p, us in zip(paths, mtimes):
                yield from iter_path_batches(p, us, fmt, opts, schema, keep)

    return src.mapInArrow(_read, schema)
