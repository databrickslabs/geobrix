"""Reader-over-paths: run a light ``*_gbx`` reader over a DataFrame of file paths.

Each row names one file; the reader's own DataSource contract (``schema()`` ->
``reader().partitions()`` -> ``read()``) parses it inside ``mapInArrow``, so the
same code serves batch reads, Auto Loader streams, and the Lakeflow connector.

Raster readers plan in two stages: each file is first planned into its tile
windows (one row per reader partition), the windows are repartitioned, and each
window is read on its own, so one large raster spreads across tasks.

Paths are Unity Catalog Volume (FUSE) paths; ``dbfs:`` / ``file:`` prefixes are
accepted; object-store URIs (``s3://``, ``abfss://``, ...) and GDAL ``/vsi...``
paths (other than ``/vsimem/``) are rejected.

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import functools
import logging
import os
import pickle
import re
from typing import Dict, Iterable, Iterator, Optional

import pyarrow as pa
from pyspark.sql.types import (
    BinaryType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.gbx.ds import _listing

_LOG = logging.getLogger(__name__)

SOURCE_PATH_COL = "_source_path"
SOURCE_MTIME_COL = "_source_mtime"
_TS = pa.timestamp("us", tz="UTC")

# Light readers read_paths can drive. Vector / LiDAR / EXIF read() yields Arrow
# RecordBatches; the raster readers yield row tuples (converted in _as_batches).
SUPPORTED_FORMATS = (
    "vector_gbx",
    "shapefile_gbx",
    "geojson_gbx",
    "geojsonl_gbx",
    "gpkg_gbx",
    "file_gdb_gbx",
    "lidar_gbx",
    "exif_gbx",
    "raster_gbx",
    "gtiff_gbx",
)

# Readers with many partitions (tile windows) per file: read_paths plans them
# first, then spreads the windows across tasks.
TWO_STAGE_FORMATS = ("raster_gbx", "gtiff_gbx")

# Default file pattern per reader, in Spark ``pathGlobFilter`` (Hadoop glob)
# syntax, matched against the file name. Hadoop globs are case-sensitive, so each
# pattern lists the lower- and upper-case extensions (camera and drone imagery is
# often ``.JPG`` / ``.TIF``). ``declare_ingest`` hands the pattern to Auto Loader,
# which delivers leaf files: without it, shapefile sidecars or the files inside a
# ``.gdb`` folder would be read as data. A folder ``schema_path`` takes the schema
# from its first file matching the same pattern.
DEFAULT_GLOBS = {
    "vector_gbx": (
        "*.{gpkg,geojson,json,geojsonl,geojsons,shp,shz,shp.zip,gdb.zip,"
        "GPKG,GEOJSON,JSON,GEOJSONL,GEOJSONS,SHP,SHZ,SHP.ZIP,GDB.ZIP}"
    ),
    "shapefile_gbx": "*.{shp,shz,shp.zip,SHP,SHZ,SHP.ZIP}",
    "geojson_gbx": "*.{geojson,json,GEOJSON,JSON}",
    "geojsonl_gbx": "*.{geojsonl,geojsons,GEOJSONL,GEOJSONS}",
    "gpkg_gbx": "*.{gpkg,GPKG}",
    "file_gdb_gbx": "*.{gdb.zip,GDB.ZIP}",
    "lidar_gbx": "*.{las,laz,LAS,LAZ}",
    "exif_gbx": "*.{jpg,jpeg,JPG,JPEG}",
    "raster_gbx": "*.{tif,tiff,TIF,TIFF}",
    "gtiff_gbx": "*.{tif,tiff,TIF,TIFF}",
}

# A "<scheme>://" prefix left after to_local_path strips dbfs: / file:, or a GDAL
# virtual file system path (/vsis3/, /vsizip/, ...; /vsimem/ is allowed).
_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
_VSI = re.compile(r"^/vsi(?!mem/)[A-Za-z0-9_]+/")

# Plan-stage rows: one per reader partition (a pickled InputPartition). Built
# explicitly (not from DDL) so workers never need a parser.
_PLAN_SCHEMA = StructType(
    [
        StructField("path", StringType(), True),
        StructField("mtime", TimestampType(), True),
        StructField("part", BinaryType(), True),
    ]
)

# Tuple rows per Arrow batch when a reader yields rows instead of batches.
_ROWS_PER_BATCH = 256

# Plan rows per Arrow batch: one input batch of files can plan to millions of
# windows, so the plan stage flushes at this cap instead of buffering them all.
_PLAN_ROWS_PER_BATCH = 8192

# Raster options that plan from a manifest / table instead of the row's path: in
# read_paths every input path would replan the whole manifest (duplicate rows).
_PATHLESS_RASTER_OPTIONS = ("manifest", "tilesTable")


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


def default_glob(fmt: str) -> str:
    """The :data:`DEFAULT_GLOBS` pattern for a reader name (alias or any case)."""
    return DEFAULT_GLOBS[resolve_format(fmt).name()]


def _glob_regex(pattern: str) -> str:
    """Hadoop ``GlobPattern`` (Spark ``pathGlobFilter``) -> Python regex, translated
    the same way: ``*``, ``?``, ``[...]`` / ``[!...]``, nestable ``{a,b}`` groups and
    ``\\`` escapes; other regex metacharacters are literal."""
    out = []
    in_set = False
    curly = 0
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "\\":
            i += 1
            if i >= len(pattern):
                raise ValueError(f"glob {pattern!r}: missing escaped character")
            out.append(re.escape(pattern[i]))
        elif c in ".$()|+":
            out.append("\\" + c)
        elif c == "*":
            out.append(".*")
        elif c == "?":
            out.append(".")
        elif c == "{":
            out.append("(?:")
            curly += 1
        elif c == "," and curly:
            out.append("|")
        elif c == "}" and curly:
            out.append(")")
            curly -= 1
        elif c == "}":
            out.append("\\}")
        elif c == "[":
            if in_set:
                raise ValueError(f"glob {pattern!r}: unclosed character class")
            in_set = True
            out.append(c)
        elif c == "^":
            out.append(c if in_set else "\\^")
        elif c == "!":
            out.append("^" if in_set and pattern[i - 1] == "[" else "!")
        elif c == "]":
            in_set = False
            out.append(c)
        else:
            out.append(c)
        i += 1
    if in_set:
        raise ValueError(f"glob {pattern!r}: unclosed character class")
    if curly:
        raise ValueError(f"glob {pattern!r}: unclosed group")
    return "".join(out)


@functools.lru_cache(maxsize=128)
def _glob_compiled(pattern: str) -> "re.Pattern":
    return re.compile(_glob_regex(pattern))


def glob_match(name: str, pattern: str) -> bool:
    """True if the file name ``name`` matches the glob ``pattern`` exactly as Spark's
    ``pathGlobFilter`` would (Hadoop glob syntax, case-sensitive)."""
    return _glob_compiled(pattern).fullmatch(name) is not None


def _fuse_path(path: str) -> str:
    """Bare local path for ``path`` (``dbfs:`` / ``file:`` stripped). Object-store
    URIs and GDAL ``/vsi...`` paths (other than ``/vsimem/``) are rejected: the
    readers open files through the Volume FUSE mount, so a stat on ``s3://...`` or
    ``/vsis3/...`` could only fail (after the transient-miss retries) and the file
    would be skipped as deleted."""
    local = _listing.to_local_path(path)
    if _URI.match(local) or _VSI.match(local):
        raise ValueError(
            f"read_paths reads Unity Catalog Volume / FUSE paths; got {path}. Read "
            "object storage through a Volume (for example an external volume over "
            "the location) instead."
        )
    return local


def _schema_file(fmt: str, schema_path: str, glob: Optional[str] = None) -> str:
    """The one file whose reader schema defines the output: ``schema_path`` itself,
    or for a folder its first file (sorted path order, hidden entries skipped) whose
    name matches ``glob`` (default: the format's :data:`DEFAULT_GLOBS` pattern). A
    ``.gdb`` folder is a single dataset and is used as is."""
    local = _fuse_path(schema_path)
    if not os.path.isdir(local) or local.lower().rstrip("/").endswith(".gdb"):
        return local
    pattern = glob or default_glob(fmt)
    found = _listing.first_file(local, lambda name: glob_match(name, pattern))
    if found is None:
        raise ValueError(
            f"No file under {schema_path} matches {pattern!r}: stage at least one "
            f"matching file there, or pass schema_path= a file."
        )
    return found


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
    fmt: str,
    options: Optional[dict],
    schema_path: str,
    lineage: bool = True,
    *,
    glob: Optional[str] = None,
) -> StructType:
    """Output schema: the reader schema of ``schema_path`` (+ lineage columns).

    A folder ``schema_path`` opens only its first file whose name matches ``glob``
    (default: the format's :data:`DEFAULT_GLOBS` pattern), so other files in the
    folder (drifted, unrelated or not yet complete) never affect the schema.
    """
    base = _reader_schema(fmt, options, _schema_file(fmt, schema_path, glob))
    return _output_schema(base, lineage)


def _base(expected: StructType) -> StructType:
    return StructType(
        [
            f
            for f in expected.fields
            if f.name not in (SOURCE_PATH_COL, SOURCE_MTIME_COL)
        ]
    )


def _output_schema(schema: StructType, lineage: bool) -> StructType:
    """``schema`` without, then (when ``lineage``) with, the lineage columns."""
    return StructType(
        list(_base(schema).fields) + (_lineage_fields() if lineage else [])
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


def _arrow_schema(schema: StructType) -> pa.Schema:
    """Spark schema -> Arrow; TIMESTAMP is UTC on every supported PySpark (>= 4.1
    takes ``timezone=``, 4.0 always emits UTC)."""
    import inspect

    from pyspark.sql.pandas.types import to_arrow_schema

    if "timezone" in inspect.signature(to_arrow_schema).parameters:
        return to_arrow_schema(schema, timezone="UTC")
    return to_arrow_schema(schema)


def _as_batches(items: Iterable, schema: StructType) -> Iterator[pa.RecordBatch]:
    """Readers yield RecordBatches or row tuples; normalize to RecordBatches."""
    from pyspark.sql.conversion import LocalDataToArrowConversion

    rows: list = []
    for item in items:
        if isinstance(item, pa.RecordBatch):
            yield item
            continue
        rows.append(item)
        if len(rows) >= _ROWS_PER_BATCH:
            yield from LocalDataToArrowConversion.convert(
                rows, schema, False
            ).to_batches()
            rows = []
    if rows:
        yield from LocalDataToArrowConversion.convert(rows, schema, False).to_batches()


def _exists(local: str) -> bool:
    """True when the file is still there (transient FUSE misses retried); a file
    deleted since it was listed is logged and skipped by every caller. An
    object-store URI raises ``ValueError`` before any stat."""
    _fuse_path(local)
    try:
        _listing._retry_transient(lambda: os.stat(local))
        return True
    except FileNotFoundError:
        _LOG.warning("read_paths: %s no longer exists; skipped", local)
        return False


def _read_partition(
    reader,
    part,
    local: str,
    mtime_us: Optional[int],
    schema: StructType,
    want: pa.Schema,
    lineage: bool,
) -> Iterator[pa.RecordBatch]:
    """Arrow batches for one reader partition, checked against ``want`` and
    realigned to its column order (+ lineage columns)."""
    for batch in _as_batches(reader.read(part), schema):
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
            batch = batch.append_column(SOURCE_MTIME_COL, pa.array([mtime_us] * n, _TS))
        yield batch


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
    local = _fuse_path(path)
    if not _exists(local):
        return
    ds = resolve_format(fmt)({**(options or {}), "path": local})
    schema = (
        _base(expected) if expected is not None else _reader_schema(fmt, options, local)
    )
    want = _arrow_schema(schema)
    reader = ds.reader(schema)
    for part in reader.partitions():
        yield from _read_partition(reader, part, local, mtime_us, schema, want, lineage)


def _plan_batches(fmt: str, opts: dict, schema: StructType):
    """``mapInArrow`` function: ``(path, mtime)`` rows -> one ``(path, mtime,
    part)`` row per reader partition of each file (``part`` = pickled partition).
    Files that no longer exist are skipped with a warning. Output batches hold at
    most ``_PLAN_ROWS_PER_BATCH`` rows (read here, on the driver, so the value
    travels with the function)."""
    base = _base(schema)
    cap = int(_PLAN_ROWS_PER_BATCH)

    def _plan(batches):
        out = _arrow_schema(_PLAN_SCHEMA)
        paths: list = []
        mtimes: list = []
        parts: list = []

        def _flush():
            batch = pa.RecordBatch.from_arrays(
                [
                    pa.array(paths, pa.string()),
                    pa.array(mtimes, _TS),
                    pa.array(parts, pa.binary()),
                ],
                schema=out,
            )
            paths.clear()
            mtimes.clear()
            parts.clear()
            return batch

        for b in batches:
            for p, us in zip(
                b.column("path").to_pylist(),
                b.column("mtime").cast(pa.int64()).to_pylist(),
            ):
                local = _fuse_path(p)
                if not _exists(local):
                    continue
                reader = resolve_format(fmt)({**opts, "path": local}).reader(base)
                for part in reader.partitions():
                    paths.append(local)
                    mtimes.append(us)
                    parts.append(pickle.dumps(part))
                    if len(parts) >= cap:
                        yield _flush()
        if parts:
            yield _flush()

    return _plan


def _read_planned(fmt: str, opts: dict, schema: StructType, lineage: bool):
    """``mapInArrow`` function: planned ``(path, mtime, part)`` rows -> the
    reader's output for each partition. One reader (and one existence check) per
    file per task."""
    base = _base(schema)

    def _read(batches):
        want = _arrow_schema(base)
        readers: dict = {}
        for b in batches:
            for local, us, blob in zip(
                b.column("path").to_pylist(),
                b.column("mtime").cast(pa.int64()).to_pylist(),
                b.column("part").to_pylist(),
            ):
                if local not in readers:
                    readers[local] = (
                        resolve_format(fmt)({**opts, "path": local}).reader(base)
                        if _exists(local)
                        else None
                    )
                reader = readers[local]
                if reader is None:
                    continue
                part = pickle.loads(blob)
                yield from _read_partition(reader, part, local, us, base, want, lineage)

    return _read


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
    schema: Optional[StructType] = None,
):
    """Read every file named in ``df[path_col]`` with a light reader.

    ``df`` may be batch or streaming (e.g. Auto Loader ``binaryFile`` projected to
    ``path, modificationTime``). Paths are Volume (FUSE) paths; an object-store URI
    fails the read. The output schema comes from ``schema_path``: a file, or a
    folder whose first file matching the format's :data:`DEFAULT_GLOBS` pattern is
    opened (default: the first path of a batch ``df``; required for a stream). A
    file deleted before it is read is skipped with a warning.

    ``schema`` (internal) is an already resolved schema, as from
    :func:`path_schema` (lineage columns optional); given, no file is opened to
    resolve it.

    ``num_partitions`` repartitions the work: by file for vector / LiDAR / EXIF,
    and by tile window for raster (``raster_gbx`` / ``gtiff_gbx``), whose files
    are planned into their windows first so one large raster spreads across
    tasks.
    """
    from pyspark.sql import functions as F

    two_stage = resolve_format(format).name() in TWO_STAGE_FORMATS
    if two_stage:
        given = {k.lower() for k in (options or {})}
        bad = [o for o in _PATHLESS_RASTER_OPTIONS if o.lower() in given]
        if bad:
            raise ValueError(
                f"read_paths does not support the {bad} option(s): each row names "
                f"one file to read, so a manifest or tiles table would be replanned "
                f"for every row. Read it with spark.read.format({format!r}) instead."
            )
    if schema is not None:
        schema = _output_schema(schema, lineage)
    else:
        if schema_path is None:
            if df.isStreaming:
                raise ValueError(
                    "read_paths on a streaming DataFrame needs schema_path= (a file "
                    "or folder whose first file defines the table schema)."
                )
            first = df.select(path_col).limit(1).collect()
            if not first:
                raise ValueError(
                    "read_paths: empty DataFrame and no schema_path given."
                )
            schema_path = first[0][0]
        schema = path_schema(format, options, schema_path, lineage)
    cols = [F.col(path_col).alias("path")]
    cols.append(
        F.col(mtime_col).cast("timestamp").alias("mtime")
        if mtime_col
        else F.lit(None).cast("timestamp").alias("mtime")
    )
    src = df.select(*cols)
    fmt, opts, keep = format, dict(options or {}), lineage
    if two_stage:
        planned = src.mapInArrow(_plan_batches(fmt, opts, schema), _PLAN_SCHEMA)
        if num_partitions:
            # Hash on the pickled partition: each window of one file is distinct.
            planned = planned.repartition(int(num_partitions), "part")
        return planned.mapInArrow(_read_planned(fmt, opts, schema, keep), schema)
    if num_partitions:
        src = src.repartition(int(num_partitions), "path")

    def _read(batches):
        for b in batches:
            paths = b.column("path").to_pylist()
            mtimes = b.column("mtime").cast(pa.int64()).to_pylist()
            for p, us in zip(paths, mtimes):
                yield from iter_path_batches(p, us, fmt, opts, schema, keep)

    return src.mapInArrow(_read, schema)
