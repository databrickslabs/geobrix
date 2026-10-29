"""Reader-over-paths: run a light ``*_gbx`` reader over a DataFrame of file paths.

Each row names one file; the reader's own DataSource contract (``schema()`` ->
``reader().partitions()`` -> ``read()``) parses it inside ``mapInArrow``, so the
same code serves batch reads, Auto Loader streams, and the Lakeflow connector.

Raster and PMTiles readers plan in two stages: each file is first planned into
its reader partitions (tile windows, or groups of PMTiles tiles; one row each),
those are repartitioned, and each is read on its own, so one large file spreads
across tasks.

Paths are Unity Catalog Volume (FUSE) paths; ``dbfs:`` / ``file:`` prefixes are
accepted; object-store URIs (``s3://``, ``abfss://``, ...) and GDAL ``/vsi...``
paths (``/vsimem/`` included) are rejected.

``on_error="skip"`` reads each file, and each reader partition of it, on its
own and sets bad records aside (see ``ds/_quarantine.py``); ``declare_ingest``
quarantines them to a table the same way.

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

from databricks.labs.gbx import _gdal_policy
from databricks.labs.gbx.ds import _arrow_utc, _listing, _quarantine

_LOG = logging.getLogger(__name__)

SOURCE_PATH_COL = "_source_path"
SOURCE_MTIME_COL = "_source_mtime"
_TS = pa.timestamp("us", tz="UTC")

# Light readers read_paths can drive. Vector / LiDAR / EXIF read() yields Arrow
# RecordBatches; the raster, NetCDF and PMTiles readers yield row tuples
# (converted in _as_batches). file_gbx is left out on purpose (it lists files,
# which the binaryFile / Auto Loader input already does), as are the heavyweight
# JVM readers.
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
    "cog_gbx",
    "netcdf_gbx",
    "pmtiles_gbx",
)

# Readers with many partitions per file: read_paths plans them first, then spreads
# them across tasks. raster / gtiff / cog: tile windows; pmtiles: groups of
# ``tilesPerPartition`` tiles, both for an archive and for a raster it renders.
# netcdf_gbx is not one: both of its modes read a file as one partition (every
# variable and slice in one pass), so it is spread by file like the vector readers.
TWO_STAGE_FORMATS = ("raster_gbx", "gtiff_gbx", "cog_gbx", "pmtiles_gbx")

# read_paths(on_error=...): "fail" an error fails the read (naming the file);
# "skip" logs and skips bad records (see _quarantine) and reads everything else.
ON_ERROR_MODES = ("fail", "skip")

# Default file pattern per reader, in Spark ``pathGlobFilter`` (Hadoop glob)
# syntax, matched against the file name. Hadoop globs are case-sensitive, so each
# pattern lists the lower- and upper-case extensions (camera and drone imagery is
# often ``.JPG`` / ``.TIF``). ``declare_ingest`` hands the pattern to Auto Loader,
# which delivers leaf files: without it, shapefile sidecars or the files inside a
# ``.gdb`` folder would be read as data. A folder ``schema_path`` takes the schema
# from its first file matching the same pattern (or, for the readers in
# _GDB_DIR_FORMATS, its first unzipped ``.gdb`` folder, whichever sorts first).
# GeoJSON is strict: a ``.json`` file is read as GeoJSON only when a glob names it.
# pmtiles_gbx: the ``source="archive"`` pattern; see _PMTILES_GLOBS.
DEFAULT_GLOBS = {
    "vector_gbx": (
        "*.{gpkg,geojson,geojsonl,geojsons,shp,shz,shp.zip,gdb.zip,"
        "GPKG,GEOJSON,GEOJSONL,GEOJSONS,SHP,SHZ,SHP.ZIP,GDB.ZIP}"
    ),
    "shapefile_gbx": "*.{shp,shz,shp.zip,SHP,SHZ,SHP.ZIP}",
    "geojson_gbx": "*.{geojson,GEOJSON}",
    "geojsonl_gbx": "*.{geojsonl,geojsons,GEOJSONL,GEOJSONS}",
    "gpkg_gbx": "*.{gpkg,GPKG}",
    "file_gdb_gbx": "*.{gdb.zip,GDB.ZIP}",
    "lidar_gbx": "*.{las,laz,LAS,LAZ}",
    "exif_gbx": "*.{jpg,jpeg,JPG,JPEG}",
    "raster_gbx": "*.{tif,tiff,TIF,TIFF}",
    "gtiff_gbx": "*.{tif,tiff,TIF,TIFF}",
    "cog_gbx": "*.{tif,tiff,TIF,TIFF}",
    "netcdf_gbx": "*.{nc,NC}",
    "pmtiles_gbx": "*.{pmtiles,PMTILES}",
}

# pmtiles_gbx reads one of two inputs, chosen by its ``source`` option, which
# read_paths and declare_ingest require (the reader's own default, ``raster``,
# would fail on a .pmtiles file): ``archive`` reads the tiles stored in .pmtiles
# files; ``raster`` renders XYZ tiles from GeoTIFFs, and its pattern keeps to that
# reader's ``filterRegex`` default (``.tif`` only).
_PMTILES_GLOBS = {"archive": DEFAULT_GLOBS["pmtiles_gbx"], "raster": "*.tif"}

# Readers that read an unzipped ``.gdb`` folder as one dataset.
_GDB_DIR_FORMATS = ("file_gdb_gbx", "vector_gbx")

# A "<scheme>://" prefix left after to_local_path strips dbfs: / file:, or a GDAL
# virtual file system path (/vsis3/, /vsizip/, /vsimem/, ...): os.stat sees none.
_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
_VSI = re.compile(r"^/vsi[A-Za-z0-9_]+/")

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
# They belong to the readers that plan with RasterGbxReader.partitions() (netcdf_gbx
# plans its own per-file partitions and ignores them).
_PATHLESS_RASTER_OPTIONS = ("manifest", "tilesTable")
_MANIFEST_FORMATS = ("raster_gbx", "gtiff_gbx", "cog_gbx")


def _check_on_error(on_error: str, bad_records_dir: Optional[str] = None) -> str:
    if on_error not in ON_ERROR_MODES:
        raise ValueError(
            f"read_paths on_error must be one of {ON_ERROR_MODES}; got {on_error!r}."
        )
    if bad_records_dir is not None and on_error != "skip":
        raise ValueError("bad_records_dir needs on_error='skip'.")
    return on_error


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


def _pmtiles_source(options: Optional[dict]) -> str:
    """The ``source`` option of a pmtiles_gbx read (``archive`` or ``raster``)."""
    given = (options or {}).get("source")
    value = str(given).lower() if given is not None else ""
    if value not in _PMTILES_GLOBS:
        raise ValueError(
            "pmtiles_gbx needs the `source` option in read_paths and declare_ingest: "
            "'archive' reads the tiles stored in .pmtiles files, 'raster' renders "
            f"XYZ tiles from GeoTIFF files; got {given!r}."
        )
    return value


def default_glob(fmt: str, options: Optional[dict] = None) -> str:
    """The default file pattern for a reader name (``_gbx`` optional, any case):
    its :data:`DEFAULT_GLOBS` entry, or for ``pmtiles_gbx`` the pattern of its
    ``source`` option (required)."""
    name = resolve_format(fmt).name()
    if name == "pmtiles_gbx":
        return _PMTILES_GLOBS[_pmtiles_source(options)]
    return DEFAULT_GLOBS[name]


def _check_options(fmt: str, options: Optional[dict]) -> str:
    """Reader name for ``fmt``, after rejecting the reader options read_paths
    cannot honor: a raster manifest / tiles table (replanned for every row), or a
    pmtiles_gbx read without its ``source``."""
    name = resolve_format(fmt).name()
    if name in _MANIFEST_FORMATS:
        given = {k.lower() for k in (options or {})}
        bad = [o for o in _PATHLESS_RASTER_OPTIONS if o.lower() in given]
        if bad:
            fixes = {
                "manifest": f"read a manifest with spark.read.format({fmt!r})",
                "tilesTable": (
                    "read a tile index with read_tiles_table(spark, table, options) "
                    "from databricks.labs.gbx.ds.raster"
                ),
            }
            raise ValueError(
                f"read_paths does not support the {bad} option(s): each row names "
                f"one file to read, so a manifest or tile index would be replanned "
                f"for every row. Fix: {'; '.join(fixes[o] for o in bad)}."
            )
    if name == "pmtiles_gbx":
        _pmtiles_source(options)
    return name


# Hadoop's GlobPattern copies an escape "\X" into its regex (RE2, via re2j)
# unchanged. An escaped ASCII punctuation character is that character; these
# escaped letters mean the same in Python's re with re.ASCII (RE2's \s is
# [\t\n\f\r ], without Python's \v). Every other escape (octal, \p, \Q, \b,
# non-ASCII, ...) is rejected rather than matched differently.
_GLOB_ESCAPES = {
    "a": r"\a",
    "f": r"\f",
    "n": r"\n",
    "r": r"\r",
    "t": r"\t",
    "v": r"\v",
    "x": r"\x",  # \xhh: the two hex digits follow as plain characters
    "d": r"\d",
    "D": r"\D",
    "w": r"\w",
    "W": r"\W",
    "s": r"[\t\n\f\r ]",
    "S": r"[^\t\n\f\r ]",
}
# Inside [...]: \s is spelled out; \S has no Python form there.
_GLOB_SET_ESCAPES = {
    **{k: v for k, v in _GLOB_ESCAPES.items() if k != "S"},
    "s": r"\t\n\f\r ",
}


def _glob_escape(pattern: str, ch: str, in_set: bool) -> str:
    if ch.isascii() and not ch.isalnum():
        return re.escape(ch)
    table = _GLOB_SET_ESCAPES if in_set else _GLOB_ESCAPES
    if ch not in table:
        raise ValueError(
            f"glob {pattern!r}: unsupported escape \\{ch} (escape glob characters "
            f"such as \\*, \\? or \\{{ to match them literally)"
        )
    return table[ch]


def _glob_regex(pattern: str) -> str:
    """Hadoop ``GlobPattern`` (Spark ``pathGlobFilter``) -> Python regex, translated
    the same way: ``*``, ``?``, ``[...]`` / ``[!...]``, nestable ``{a,b}`` groups and
    ``\\`` escapes (see :data:`_GLOB_ESCAPES`); other regex metacharacters are
    literal."""
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
            out.append(_glob_escape(pattern, pattern[i], in_set))
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
    # Hadoop compiles with DOTALL; re.ASCII gives \d / \w RE2's ASCII classes.
    try:
        return re.compile(_glob_regex(pattern), re.ASCII | re.DOTALL)
    except re.error as e:
        raise ValueError(f"glob {pattern!r}: invalid pattern ({e})") from None


def glob_match(name: str, pattern: str) -> bool:
    """True if the file name ``name`` matches the glob ``pattern`` exactly as Spark's
    ``pathGlobFilter`` would (Hadoop glob syntax, case-sensitive). A pattern Spark
    rejects, or one whose escapes have no exact Python equivalent, raises
    ``ValueError``."""
    return _glob_compiled(pattern).fullmatch(name) is not None


def _fuse_path(path: str, caller: str = "read_paths") -> str:
    """Bare local path for ``path`` (``dbfs:`` / ``file:`` stripped). Object-store
    URIs and GDAL ``/vsi...`` paths (``/vsimem/`` included) are rejected: the
    readers open files through the Volume FUSE mount, so a stat on ``s3://...`` or
    ``/vsis3/...`` could only fail (after the transient-miss retries) and the file
    would be skipped as deleted. ``caller`` names the function in the error."""
    local = _listing.to_local_path(path)
    if _URI.match(local) or _VSI.match(local):
        raise ValueError(
            f"{caller} reads Unity Catalog Volume / FUSE paths; got {path}. Read "
            "object storage through a Volume (for example an external volume over "
            "the location) instead."
        )
    return local


def _is_gdb(path: str) -> bool:
    return path.lower().rstrip("/").endswith(".gdb")


def _schema_file(
    fmt: str,
    schema_path: str,
    glob: Optional[str] = None,
    options: Optional[dict] = None,
) -> str:
    """The one file whose reader schema defines the output: ``schema_path`` itself,
    or for a folder its first file (sorted path order; files and folders whose
    names start with ``_`` or ``.`` skipped) whose name matches ``glob`` (default:
    :func:`default_glob` of the format and ``options``). A ``.gdb`` folder is a
    single dataset and is used as is; inside a folder, for the readers in
    :data:`_GDB_DIR_FORMATS`, a ``.gdb`` folder counts as one file."""
    local = _fuse_path(schema_path)
    if not os.path.isdir(local) or _is_gdb(local):
        return local
    pattern = glob or default_glob(fmt, options)
    gdb_dirs = resolve_format(fmt).name() in _GDB_DIR_FORMATS
    found = _listing.first_file(
        local,
        lambda name: glob_match(name, pattern),
        accept_dir=_is_gdb if gdb_dirs else None,
    )
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
    (default: the format's :func:`default_glob` pattern), so other files in the
    folder (drifted, unrelated or not yet complete) never affect the schema.
    Options read_paths cannot honor are rejected here (see :func:`_check_options`).
    """
    _check_options(fmt, options)
    base = _reader_schema(fmt, options, _schema_file(fmt, schema_path, glob, options))
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


def _check_file_schema(ds, reader, local: str, want: pa.Schema) -> None:
    """Check the schema a file declares against ``want`` once, before any of its
    rows is read -- also when it has no rows: an empty layer whose header is
    garbled declares no features and no fields, yields no batch, and so would
    pass the per-batch check unseen. A reader's ``declared_schema()`` (the vector
    readers) returns None for a file that declares no fields (GeoJSON fields come
    from its features: its first batch is checked, and a file with no features has
    nothing to check); other readers declare ``ds.schema()``."""
    declared = getattr(reader, "declared_schema", None)
    got = declared() if declared is not None else ds.schema()
    if got is None:
        return
    if isinstance(got, str):
        got = StructType.fromDDL(got)
    _check_schema(local, _arrow_schema(_base(got)), want)


def _arrow_schema(schema: StructType) -> pa.Schema:
    """Spark schema -> Arrow; TIMESTAMP is UTC on every supported PySpark."""
    return _arrow_utc.arrow_schema_utc(schema)


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


class _ReadState:
    """Rows of one reader partition so far: ``seen`` = returned by the reader (a
    row's position in the file), ``kept`` = emitted."""

    __slots__ = ("seen", "kept")

    def __init__(self):
        self.seen = 0
        self.kept = 0


def _is_vector(reader) -> bool:
    from databricks.labs.gbx.ds.vector import VectorGbxReader

    return isinstance(reader, VectorGbxReader)


def _set_aside(batch, geometry, bad, local, mtime_us, state) -> pa.RecordBatch:
    """``batch`` without its bad rows (see :func:`_quarantine.bad_rows`), which go
    to ``bad`` with their position in the file and their other columns."""
    offset = state.seen
    state.seen += batch.num_rows
    found = _quarantine.bad_rows(batch, geometry)
    if not found:
        return batch
    unit = "feature" if geometry else "row"
    skip = (geometry, f"{geometry}_srid", f"{geometry}_srid_proj") if geometry else ()
    for i in sorted(found):
        cls, msg = found[i]
        raw = _quarantine.row_json(batch, i, skip)
        bad.add(local, mtime_us, f"{unit}={offset + i}", cls, msg, raw)
    keep = pa.array([i not in found for i in range(batch.num_rows)], pa.bool_())
    return batch.filter(keep)


def _read_partition(
    reader,
    part,
    local: str,
    mtime_us: Optional[int],
    schema: StructType,
    want: pa.Schema,
    lineage: bool,
    bad=None,
    state: Optional[_ReadState] = None,
) -> Iterator[pa.RecordBatch]:
    """Arrow batches for one reader partition, checked against ``want`` and
    realigned to its column order (+ lineage columns). With ``bad`` (a
    :class:`_quarantine.BadRecords`), rows that are not data are set aside."""
    geometry = None
    if bad is not None:
        state = state if state is not None else _ReadState()
        geometry = _quarantine.geometry_column(want) if _is_vector(reader) else None
    for batch in _as_batches(reader.read(part), schema):
        if batch.num_rows == 0:
            # Nothing to emit; a zero-row batch (e.g. a zero-feature GeoJSON)
            # carries no usable schema. The file's declared schema was checked
            # before reading (_check_file_schema).
            continue
        _check_schema(local, batch.schema, want)
        if batch.schema.names != want.names:
            # Same columns, different file order: realign to the positional
            # output schema so values never land in the wrong column.
            batch = batch.select(want.names)
        if bad is not None:
            batch = _set_aside(batch, geometry, bad, local, mtime_us, state)
            if batch.num_rows == 0:
                continue
            state.kept += batch.num_rows
        if lineage:
            n = batch.num_rows
            batch = batch.append_column(
                SOURCE_PATH_COL, pa.array([local] * n, pa.string())
            )
            batch = batch.append_column(SOURCE_MTIME_COL, pa.array([mtime_us] * n, _TS))
        yield batch


def _bounded(it, bad, local, mtime_us, state, label=None):
    """Batches of ``it`` with ``it`` as the unit of failure: an error (any
    exception, a Rust panic included) is recorded with the warnings and log
    lines seen while reading, rows already emitted stay, and the read goes on
    with what follows. Returns True when ``it`` completed."""
    diag = _quarantine.Diagnostics()
    while True:
        try:
            with _quarantine.capture(diag):
                batch = next(it)
        except StopIteration:
            return True
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException as e:  # noqa: BLE001 - recorded as a bad record
            # Not the data's fault (memory, a missing module, I/O): fail the
            # task so Spark retries it, rather than set the rest of it aside.
            _quarantine.reraise_environment(e)
            bad.add_error(local, mtime_us, e, diag, state.kept, label)
            return False
        yield batch


def _guarded(reader, part, local, mtime_us, schema, want, lineage, bad, state):
    """:func:`_read_partition` with the partition as the unit of failure (see
    :func:`_bounded`; ``record_index`` = the partition's place in the file)."""
    it = _read_partition(
        reader, part, local, mtime_us, schema, want, lineage, bad, state
    )
    label = _quarantine.part_label(part)
    return (yield from _bounded(it, bad, local, mtime_us, state, label))


def _open_file(fmt, options, local, expected):
    """``(ds, reader, schema, want, partitions)`` for one file, with its declared
    schema checked against ``expected``."""
    ds = resolve_format(fmt)({**(options or {}), "path": local})
    schema = (
        _base(expected) if expected is not None else _reader_schema(fmt, options, local)
    )
    want = _arrow_schema(schema)
    reader = ds.reader(schema)
    if expected is not None:
        _check_file_schema(ds, reader, local, want)
    return ds, reader, schema, want, list(reader.partitions())


def _open_guarded(bad, local, mtime_us, opener):
    """``opener()``, or None after recording its error as a whole-file bad record.
    An environment error fails the read instead; a transient I/O error is retried
    first (``_listing._retry_transient``)."""
    diag = _quarantine.Diagnostics()
    try:
        with _quarantine.capture(diag):
            return opener()
    except _quarantine.PASS_THROUGH:
        raise
    except BaseException as e:  # noqa: BLE001 - recorded as a bad record
        if _quarantine.is_transient(e):
            try:
                return _listing._retry_transient(opener)
            except _quarantine.PASS_THROUGH:
                raise
            except BaseException as again:  # noqa: BLE001 - classified below
                e = again  # still transient: raised below; else a data error
        _quarantine.reraise_environment(e)
        bad.add_error(local, mtime_us, e, diag)
        return None


def iter_path_batches(
    path: str,
    mtime_us: Optional[int],
    fmt: str,
    options: Optional[dict],
    expected: Optional[StructType] = None,
    lineage: bool = True,
    bad_records=None,
) -> Iterator[pa.RecordBatch]:
    """Arrow batches for one file (session-free). A file that no longer exists is
    skipped with a warning; ``expected`` (lineage included or not) is enforced,
    first on the schema the file declares (even with no rows), then per batch.

    ``bad_records`` None (``on_error="fail"``): an error fails the read and names
    the file. A :class:`_quarantine.BadRecords`: errors and rows that are not
    data are recorded there instead, and the rest of the file is read."""
    local = _fuse_path(path)
    if not _exists(local):
        return
    if bad_records is None:
        try:
            _, reader, schema, want, parts = _open_file(fmt, options, local, expected)
            for part in parts:
                yield from _read_partition(
                    reader, part, local, mtime_us, schema, want, lineage
                )
        except Exception as e:
            _quarantine.name_the_file(e, local)
            raise
        return
    bad = bad_records
    opened = _open_guarded(
        bad, local, mtime_us, lambda: _open_file(fmt, options, local, expected)
    )
    if opened is None:
        return
    _, reader, schema, want, parts = opened
    setattr(reader, _quarantine.HOOK, bad.hook(local, mtime_us))
    vector = _is_vector(reader)
    for part in parts:
        state = _ReadState()
        done = yield from _guarded(
            reader, part, local, mtime_us, schema, want, lineage, bad, state
        )
        if done and vector:
            checked = _reconcile_vector(
                reader, part, local, mtime_us, schema, want, lineage, bad, state
            )
            yield from _bounded(checked, bad, local, mtime_us, state)


def _collector(on_error: str, bad_records_dir: Optional[str]):
    return _quarantine.BadRecords(bad_records_dir) if on_error == "skip" else None


def _plan_batches(
    fmt: str,
    opts: dict,
    schema: StructType,
    on_error: str = "fail",
    bad_records_dir: Optional[str] = None,
):
    """``mapInArrow`` function: ``(path, mtime)`` rows -> one ``(path, mtime,
    part)`` row per reader partition of each file (``part`` = pickled partition).
    Each file's declared schema is checked against ``schema`` here, once. Files
    that no longer exist are skipped with a warning; with ``on_error="skip"`` a
    file that cannot be planned is a bad record. Output batches hold at most
    ``_PLAN_ROWS_PER_BATCH`` rows (read here, on the driver, so the value travels
    with the function)."""
    base = _base(schema)
    cap = int(_PLAN_ROWS_PER_BATCH)

    def _plan(batches):
        out = _arrow_schema(_PLAN_SCHEMA)
        want = _arrow_schema(base)
        bad = _collector(on_error, bad_records_dir)
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

        def _partitions(local):
            ds = resolve_format(fmt)({**opts, "path": local})
            reader = ds.reader(base)
            _check_file_schema(ds, reader, local, want)
            return list(reader.partitions())

        try:
            for b in batches:
                for p, us in zip(
                    b.column("path").to_pylist(),
                    b.column("mtime").cast(pa.int64()).to_pylist(),
                ):
                    local = _fuse_path(p)
                    if not _exists(local):
                        continue
                    if bad is None:
                        try:
                            planned = _partitions(local)
                        except Exception as e:
                            _quarantine.name_the_file(e, local)
                            raise
                    else:
                        planned = _open_guarded(
                            bad, local, us, lambda local=local: _partitions(local)
                        )
                        if planned is None:
                            continue
                    for part in planned:
                        paths.append(local)
                        mtimes.append(us)
                        parts.append(pickle.dumps(part))
                        if len(parts) >= cap:
                            yield _flush()
            if parts:
                yield _flush()
        finally:
            if bad is not None:
                bad.close()

    return _plan


def _read_planned(
    fmt: str,
    opts: dict,
    schema: StructType,
    lineage: bool,
    on_error: str = "fail",
    bad_records_dir: Optional[str] = None,
):
    """``mapInArrow`` function: planned ``(path, mtime, part)`` rows -> the
    reader's output for each partition. One reader (and one existence check) per
    file per task. With ``on_error="skip"`` each partition (a tile window, a
    group of tiles) is the unit of failure."""
    base = _base(schema)

    def _read(batches):
        want = _arrow_schema(base)
        bad = _collector(on_error, bad_records_dir)
        readers: dict = {}

        def _reader(local, us):
            if not _exists(local):
                return None
            if bad is None:
                try:
                    return resolve_format(fmt)({**opts, "path": local}).reader(base)
                except Exception as e:
                    _quarantine.name_the_file(e, local)
                    raise
            reader = _open_guarded(
                bad,
                local,
                us,
                lambda: resolve_format(fmt)({**opts, "path": local}).reader(base),
            )
            if reader is not None:
                setattr(reader, _quarantine.HOOK, bad.hook(local, us))
            return reader

        try:
            for b in batches:
                for local, us, blob in zip(
                    b.column("path").to_pylist(),
                    b.column("mtime").cast(pa.int64()).to_pylist(),
                    b.column("part").to_pylist(),
                ):
                    if local not in readers:
                        readers[local] = _reader(local, us)
                    reader = readers[local]
                    if reader is None:
                        continue
                    part = pickle.loads(blob)
                    if bad is not None:
                        yield from _guarded(
                            reader,
                            part,
                            local,
                            us,
                            base,
                            want,
                            lineage,
                            bad,
                            _ReadState(),
                        )
                        continue
                    try:
                        yield from _read_partition(
                            reader, part, local, us, base, want, lineage
                        )
                    except Exception as e:
                        _quarantine.name_the_file(e, local)
                        raise
        finally:
            if bad is not None:
                bad.close()

    return _read


# ---------------------------------------------------------------------------
# Vector count checks: features a file declares but the reader did not return
# ---------------------------------------------------------------------------

# Bound on the per-feature probes over one unreadable stretch of a GeoPackage.
_GPKG_PROBE_LIMIT = 100_000


def _reconcile_vector(reader, part, local, mtime_us, schema, want, lineage, bad, state):
    """After a vector file read without an error, compare the features read with
    the count the file declares (GeoJSONSeq: its records). Not with a ``bbox``
    or ``where`` filter, nor for GeoJSON (parsed whole: it fails rather than
    dropping a feature). A shortfall is a bad record; a GeoPackage read that
    ended early is resumed past the unreadable page first, and the features on
    it are recorded one by one. Yields the resumed rows."""
    if getattr(part, "bbox", None) or getattr(part, "where", None):
        return
    kind = _quarantine.count_kind(part.path)
    if kind is None:
        return
    try:
        with _quarantine.capture():
            if kind == "lines":
                declared = _quarantine.sequence_record_count(part.path)
            else:
                info = reader._info_for(part.path)
                declared = int(info.get("features", -1))
    except _quarantine.PASS_THROUGH:
        raise
    except Exception as e:  # noqa: BLE001 - nothing to compare with
        _quarantine.reraise_environment(e)
        return
    if declared < 0 or state.seen >= declared:
        return
    if kind == "lines":
        _bad_lines(part.path, local, mtime_us, declared, state, bad)
        return
    driver = info.get("driver") or ""
    if driver in ("GPKG", "SQLite"):
        yield from _resume_gpkg(
            reader,
            part,
            info,
            declared,
            local,
            mtime_us,
            schema,
            want,
            lineage,
            bad,
            state,
        )
        return
    deleted = 0
    if driver == "ESRI Shapefile":
        try:
            deleted = _quarantine.dbf_deleted_count(part.path)
        except OSError as e:
            _quarantine.reraise_environment(e)  # I/O: fail the read
            deleted = 0
    if state.seen + deleted < declared:
        _not_read(bad, local, mtime_us, state.seen + deleted, declared)


def _not_read(bad, local, mtime_us, read, declared, why="") -> None:
    bad.add(
        local,
        mtime_us,
        None,
        _quarantine.RECORDS_NOT_READ,
        f"{declared - read} of the {declared} features the file declares were not "
        f"returned by the reader{why}",
    )


def _bad_lines(path, local, mtime_us, declared, state, bad) -> None:
    """GeoJSONSeq: the records the reader dropped, found by parsing each record."""
    import json

    found = _quarantine.unparseable_lines(path, limit=declared - state.seen)
    for line, err, text in found:
        bad.add(
            local,
            mtime_us,
            f"line={line}",
            _quarantine.RECORDS_NOT_READ,
            f"line {line} is not a GeoJSON feature: {err}",
            json.dumps({"line": text}, ensure_ascii=False),
        )
    if state.seen + len(found) < declared:
        _not_read(bad, local, mtime_us, state.seen + len(found), declared)


def _gpkg_fids(name, layer, where=None, max_features=None) -> list:
    pyogrio = _gdal_policy.pyogrio()  # the driver policy is active first

    kw = {"layer": layer, "columns": [], "read_geometry": False, "return_fids": True}
    if where:
        kw["where"] = where
    if max_features is not None:
        kw["max_features"] = max_features
    meta, tbl = pyogrio.read_arrow(name, **kw)
    return tbl.column(meta.get("fid_column") or "OGC_FID").to_pylist()


class _GpkgGaps:
    """Probes over one staged GeoPackage layer for :func:`_resume_gpkg`."""

    def __init__(self, name, layer, fid_column):
        self.name, self.layer = name, layer
        self.fid = _quarantine.quote_ident(fid_column or "fid")
        self.lost: list = []  # fids whose read fails: on an unreadable page
        self.unclear: list = []  # gap fids no read finds (absent, or garbled)
        self.first_error = ""

    def max_fid(self):
        pyogrio = _gdal_policy.pyogrio()  # the driver policy is active first

        _, tbl = pyogrio.read_arrow(
            self.name,
            sql=f"SELECT MAX({self.fid}) AS m FROM {_quarantine.quote_ident(self.layer)}",
        )
        return tbl.column(0)[0].as_py()

    def last_fid(self, where, n):
        """The fid of the ``n``-th feature ``where`` reads (in fid order)."""
        return _gpkg_fids(self.name, self.layer, where, n)[-1]

    def starts(self, k) -> bool:
        """True if a read of ``fid >= k`` starts, with geometry and attributes
        as the resumed read will (a garbled cell can read without them)."""
        pyogrio = _gdal_policy.pyogrio()  # the driver policy is active first

        try:
            pyogrio.read_arrow(
                self.name, layer=self.layer, where=f"{self.fid} >= {k}", max_features=1
            )
            return True
        except Exception as e:  # noqa: BLE001 - the read cannot start there
            _quarantine.reraise_environment(e)
            return False

    def first_start(self, last, max_fid) -> int:
        """Smallest k in (last, max_fid] a read can start from (max_fid + 1 when
        none can): exponential search, then bisection; ``lo`` cannot start (or
        is ``last``), ``hi`` can."""
        lo, step, hi = last, 1, None
        while last + step <= max_fid:
            if self.starts(last + step):
                hi = last + step
                break
            lo = last + step
            step *= 2
        if hi is None:
            hi = max_fid if self.starts(max_fid) else max_fid + 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if self.starts(mid):
                hi = mid
            else:
                lo = mid
        return hi

    def probe(self, gap) -> list:
        """Classify each fid of ``gap`` with ``fid = x``: lost (the read fails),
        unclear (no row) or readable (returned)."""
        readable = []
        for x in gap:
            try:
                if _gpkg_fids(self.name, self.layer, f"{self.fid} = {x}"):
                    readable.append(x)
                else:
                    self.unclear.append(x)
            except Exception as e:  # noqa: BLE001 - on the unreadable page
                _quarantine.reraise_environment(e)
                self.lost.append(x)
                self.first_error = self.first_error or _quarantine._one_line(e)
        return readable

    def record(self, bad, local, mtime_us, seen, declared) -> None:
        # A garbled cell can hide its fid: the gap fids no read finds are the
        # missing features when exactly that many are missing.
        if self.unclear and declared - seen - len(self.lost) == len(self.unclear):
            self.lost, self.unclear = sorted(self.lost + self.unclear), []
            self.first_error = self.first_error or "no read finds it"
        for x in self.lost:
            bad.add(
                local,
                mtime_us,
                f"fid={x}",
                _quarantine.RECORDS_NOT_READ,
                f"feature fid {x} is on an unreadable page of the GeoPackage: "
                f"{self.first_error}",
            )
        if seen + len(self.lost) < declared:
            why = (
                f" (fids {self.unclear[0]} to {self.unclear[-1]} are on an "
                "unreadable page)"
                if self.unclear
                else ""
            )
            _not_read(bad, local, mtime_us, seen + len(self.lost), declared, why)


def _resume_gpkg(
    reader, part, info, declared, local, mtime_us, schema, want, lineage, bad, state
):
    """A GeoPackage whose read ended early (a corrupt SQLite page ends OGR's scan
    without an error): resume with ``fid >= k`` (an indexed seek past the page;
    ``skip_features`` would scan into it), where ``k`` is the first fid a read can
    start from, and probe each fid in between (``fid = x``): one that fails is a
    feature on the unreadable page, recorded exactly. Repeats until every
    declared feature is read or accounted for."""
    from databricks.labs.gbx.ds.vector import _ChunkPartition

    def segment(where):
        """Read ``where`` through the reader (rows set aside as usual); an error
        ends it: the next probe finds what it could not read."""
        seg = _ChunkPartition(
            part.path, part.driver, part.layer, part.as_wkb, 0, 0, where=where
        )
        it = _read_partition(
            reader, seg, local, mtime_us, schema, want, lineage, bad, state
        )
        while True:
            try:
                with _quarantine.capture():
                    batch = next(it)
            except StopIteration:
                return
            except _quarantine.PASS_THROUGH:
                raise
            except Exception as e:  # noqa: BLE001 - found by the next probe
                _quarantine.reraise_environment(e)
                return
            yield batch

    with reader._staged(part.path) as staged:
        gaps = _GpkgGaps(
            reader._source_name(part.path, staged),
            info.get("layer_name") or part.layer,
            info.get("fid_column"),
        )
        fid = gaps.fid
        try:
            max_fid = gaps.max_fid()
            last = gaps.last_fid(None, state.seen) if state.seen else 0
        except Exception as e:  # noqa: BLE001 - cannot locate the gap
            _quarantine.reraise_environment(e)
            max_fid, last = None, 0
        while max_fid is not None and last < max_fid:
            if state.seen + len(gaps.lost) >= declared:
                break
            before = (state.seen, len(gaps.lost))
            k = gaps.first_start(last, max_fid)
            if k - last - 1 > _GPKG_PROBE_LIMIT:
                break
            readable = gaps.probe(range(last + 1, k))
            if readable:
                yield from segment(f"{fid} IN ({', '.join(map(str, readable))})")
            if k > max_fid:
                break
            n0 = state.seen
            yield from segment(f"{fid} >= {k}")
            read = state.seen - n0
            last = gaps.last_fid(f"{fid} >= {k}", read) if read else k
            if (state.seen, len(gaps.lost)) == before:
                break
    gaps.record(bad, local, mtime_us, state.seen, declared)


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
    on_error: str = "fail",
    schema: Optional[StructType] = None,
    bad_records_dir: Optional[str] = None,
):
    """Read every file named in ``df[path_col]`` with a light reader.

    ``df`` may be batch or streaming (e.g. Auto Loader ``binaryFile`` projected to
    ``path, modificationTime``). Paths are Volume (FUSE) paths; an object-store URI
    fails the read. The output schema comes from ``schema_path``: a file, or a
    folder whose first file matching the :func:`default_glob` pattern of
    ``format`` and ``options`` is opened (default: the first path of a batch
    ``df``; required for a stream). A file deleted before it is read is skipped
    with a warning.

    ``on_error`` (see :data:`ON_ERROR_MODES`): ``"fail"`` (default) fails the
    read on an error, naming the file. ``"skip"`` reads each file (and each
    raster window or group of PMTiles tiles) on its own: an error, a row that is
    not data (invalid text; for vector formats geometry bytes that cannot be
    decoded -- a NULL or empty geometry is data) and records a file declares but
    its reader does not return are logged as warnings and skipped, and everything
    else is read.

    ``schema`` (internal) is an already resolved schema, as from
    :func:`path_schema` (lineage columns optional); given, no file is opened to
    resolve it. ``bad_records_dir`` (internal, with ``on_error="skip"``): write
    the skipped records to Parquet files in that folder instead of logging them
    (``declare_ingest`` quarantine).

    ``num_partitions`` repartitions the work: by file for vector / LiDAR / EXIF /
    NetCDF, by tile window for raster (``raster_gbx`` / ``gtiff_gbx`` /
    ``cog_gbx``) and by group of tiles for ``pmtiles_gbx``; the files of those
    :data:`TWO_STAGE_FORMATS` are planned into their partitions first, so one
    large file spreads across tasks. ``pmtiles_gbx`` needs its ``source`` option.
    """
    from pyspark.sql import functions as F

    _check_on_error(on_error, bad_records_dir)
    two_stage = _check_options(format, options) in TWO_STAGE_FORMATS
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
    mode, bad_dir = on_error, bad_records_dir
    if two_stage:
        planned = src.mapInArrow(
            _plan_batches(fmt, opts, schema, mode, bad_dir), _PLAN_SCHEMA
        )
        if num_partitions:
            # Hash on the pickled partition: each window of one file is distinct.
            planned = planned.repartition(int(num_partitions), "part")
        return planned.mapInArrow(
            _read_planned(fmt, opts, schema, keep, mode, bad_dir), schema
        )
    if num_partitions:
        src = src.repartition(int(num_partitions), "path")

    def _read(batches):
        bad = _collector(mode, bad_dir)
        try:
            for b in batches:
                paths = b.column("path").to_pylist()
                mtimes = b.column("mtime").cast(pa.int64()).to_pylist()
                for p, us in zip(paths, mtimes):
                    yield from iter_path_batches(p, us, fmt, opts, schema, keep, bad)
        finally:
            if bad is not None:
                bad.close()

    return src.mapInArrow(_read, schema)
