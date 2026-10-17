"""Bad-record handling for the readers driven over paths (``on_error``).

``read_paths(on_error="skip")`` and ``declare_ingest(on_error="quarantine")``
read each file in isolation: an error in one file, or in one reader partition of
it (a raster tile window, a PMTiles tile group), is caught there, rows already
read are kept, and the rest of the input is read on. Per-row checks and per-file
count checks set aside the records a reader returns without an error but which
are not data (geometry bytes that cannot be decoded, invalid UTF-8, features a
file declares but the reader never returned; a NULL or empty geometry is data).
Every such record is a *bad record*.

A :class:`BadRecords` collector receives them on the executors. With a scratch
folder it writes them to Parquet fragments there (``declare_ingest`` appends
those to ``<table>_bad_records`` after the rows are written); without one it
logs them (``read_paths(on_error="skip")``).

Readers that check their own records (LiDAR points, PMTiles tiles, NetCDF
slices, EXIF files) report through the hook :data:`HOOK` that ``read_paths``
sets on the reader: ``hook(record_index, error_class, message, raw=None)``. A
reader with no hook keeps its own behavior.

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import uuid
import warnings
from typing import Callable, Iterator, List, Optional, Tuple

import pyarrow as pa

_LOG = logging.getLogger(__name__)

#: Attribute ``read_paths`` sets on a reader: the bad-record callable for the
#: file it reads, ``hook(record_index, error_class, message, raw=None)``.
HOOK = "_gbx_bad_record"

# Errors that are never a bad record: they stop the read.
PASS_THROUGH = (KeyboardInterrupt, SystemExit, GeneratorExit)

# error_class of the records a check (not an exception) sets aside. A NULL or
# empty geometry is data (an unlocated feature, POINT EMPTY), not a bad record.
UNREADABLE_GEOMETRY = "UnreadableGeometry"
INVALID_TEXT = "InvalidText"
RECORDS_NOT_READ = "RecordsNotRead"
POINT_OUT_OF_BOUNDS = "PointOutOfBounds"
SHORT_TILE = "ShortTile"
NOT_JPEG = "NotJpeg"
UNREADABLE_FILE = "UnreadableFile"
FILE_TOO_LARGE = "FileTooLarge"
# declare_ingest: a delivery none of whose rows could be read (one summary record).
NOTHING_READABLE = "NothingReadable"

#: Every error_class a check can give (exceptions give their class name).
CHECK_CLASSES = (
    UNREADABLE_GEOMETRY,
    INVALID_TEXT,
    RECORDS_NOT_READ,
    POINT_OUT_OF_BOUNDS,
    SHORT_TILE,
    NOT_JPEG,
    UNREADABLE_FILE,
    FILE_TOO_LARGE,
    NOTHING_READABLE,
)

_TS = pa.timestamp("us", tz="UTC")

#: What the executors write; the driver adds hash, size and quarantined_at.
FRAGMENT_SCHEMA = pa.schema(
    [
        ("_source_path", pa.string()),
        ("_source_mtime", _TS),
        ("record_index", pa.string()),
        ("error_class", pa.string()),
        ("error_message", pa.string()),
        ("raw", pa.string()),
    ]
)
FRAGMENT_DDL = (
    "_source_path string, _source_mtime timestamp, record_index string, "
    "error_class string, error_message string, raw string"
)

#: Columns of ``<table>_bad_records``, in order.
BAD_RECORDS_COLUMNS = (
    "_source_path",
    "_source_mtime",
    "_source_hash",
    "_source_size",
    "record_index",
    "error_class",
    "error_message",
    "raw",
    "quarantined_at",
)

_MESSAGE_LIMIT = 4096  # characters of one error_message
_PART_LIMIT = 600  # characters of one exception / warning / log line in it
_DIAG_LIMIT = 8  # distinct warnings and log lines kept per partition
_FLUSH_ROWS = 5000  # records per fragment
_LOG_PER_FILE = 20  # bad records logged per file (on_error="skip")

# Loggers whose records explain a failed read.
_LOGGERS = ("laspy", "exifread", "rasterio", "pyogrio", "databricks.labs.gbx")
# Noise on every open in older code paths, not a symptom of a bad file.
_NOISE = ("does not support open option DRIVER",)


# ---------------------------------------------------------------------------
# Diagnostics: warnings and log records seen while one partition is read
# ---------------------------------------------------------------------------


class Diagnostics:
    """Distinct warnings and log lines captured while a partition was read."""

    def __init__(self):
        self.warnings: List[str] = []
        self.logs: List[str] = []

    @staticmethod
    def _add(seen: List[str], text: str) -> None:
        if any(n in text for n in _NOISE):
            return
        text = text[:_PART_LIMIT]
        if text not in seen and len(seen) < _DIAG_LIMIT:
            seen.append(text)

    def warning(self, message, category, *_a, **_k) -> None:
        self._add(self.warnings, f"{category.__name__}: {message}")


class _LogCapture(logging.Handler):
    def __init__(self, diag: Diagnostics):
        super().__init__(level=logging.WARNING)
        self.diag = diag

    def emit(self, record):
        if record.name == __name__:  # this module's own skip-mode log lines
            return
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001 - a broken log call is not our error
            msg = str(record.msg)
        self.diag._add(self.diag.logs, f"{record.levelname} {record.name}: {msg}")


@contextlib.contextmanager
def capture(diag: Optional[Diagnostics] = None) -> Iterator[Diagnostics]:
    """Collect the warnings and the warning/error log records of the readers'
    libraries (laspy, exifread, rasterio, pyogrio, GeoBrix) while the block runs,
    into ``diag`` (a new one by default). Warnings are recorded every time (not
    once per location) and not printed. Keep the block short -- never across a
    ``yield`` -- since the warning filters are process-wide."""
    diag = diag if diag is not None else Diagnostics()
    handler = _LogCapture(diag)
    loggers = [logging.getLogger(n) for n in _LOGGERS]
    with warnings.catch_warnings():
        warnings.simplefilter("always")
        warnings.showwarning = diag.warning
        for lg in loggers:
            lg.addHandler(handler)
        try:
            yield diag
        finally:
            for lg in loggers:
                lg.removeHandler(handler)


# ---------------------------------------------------------------------------
# Describing an error
# ---------------------------------------------------------------------------


def error_class(exc: BaseException) -> str:
    """``module.Class`` of an exception (just ``Class`` for builtins)."""
    t = type(exc)
    mod = getattr(t, "__module__", "") or ""
    return t.__qualname__ if mod == "builtins" else f"{mod}.{t.__qualname__}"


def _one_line(exc: BaseException) -> str:
    text = " ".join(str(exc).split())
    return f"{error_class(exc)}: {text}"[:_PART_LIMIT]


def _causes(exc: BaseException) -> List[BaseException]:
    """The exceptions behind ``exc`` (``__cause__``, else ``__context__``)."""
    out, seen = [], {id(exc)}
    cur = exc
    while len(out) < 5:
        nxt = cur.__cause__ or (None if cur.__suppress_context__ else cur.__context__)
        if nxt is None or id(nxt) in seen:
            break
        out.append(nxt)
        seen.add(id(nxt))
        cur = nxt
    return out


def describe(
    exc: BaseException, diag: Optional[Diagnostics] = None, rows_kept: int = 0
) -> str:
    """One bounded message for a failed read: the error, what caused it (rasterio
    keeps the real reason there), the warnings and log lines seen while reading,
    and how many rows of the file were read and kept before it."""
    parts = [_one_line(exc)] + [f"caused by {_one_line(c)}" for c in _causes(exc)]
    if diag is not None and diag.warnings:
        parts.append("warnings: " + "; ".join(diag.warnings))
    if diag is not None and diag.logs:
        parts.append("log: " + "; ".join(diag.logs))
    if rows_kept:
        parts.append(f"{rows_kept} rows of this file were read before the error")
    return " | ".join(parts)[:_MESSAGE_LIMIT]


def name_the_file(exc: BaseException, path: str) -> None:
    """Make sure a read error names its file (``on_error="fail"``): GDAL's errors
    do not always do. A note keeps the exception's type and message."""
    if path in str(exc) or any(path in n for n in getattr(exc, "__notes__", ())):
        return
    add = getattr(exc, "add_note", None)
    if add is not None:
        add(f"while reading {path}")


# ---------------------------------------------------------------------------
# The collector
# ---------------------------------------------------------------------------


def _fragment_name(seq: int) -> str:
    """``s<stage>-p<partition>-a<attempt>-<seq>-<uuid>.parquet``: a task retry or
    a speculative copy writes its own files (the driver drops duplicates)."""
    try:
        from pyspark import TaskContext

        tc = TaskContext.get()
    except Exception:  # noqa: BLE001 - outside a Spark task
        tc = None
    if tc is not None:
        tag = f"s{tc.stageId()}-p{tc.partitionId():05d}-a{tc.attemptNumber()}"
    else:
        tag = "local"
    return f"{tag}-{seq:04d}-{uuid.uuid4().hex[:8]}.parquet"


class BadRecords:
    """Receives the bad records of one task. ``scratch_dir`` set: written to
    Parquet fragments there (``declare_ingest``); else logged as warnings
    (``read_paths(on_error="skip")``), at most :data:`_LOG_PER_FILE` per file."""

    def __init__(self, scratch_dir: Optional[str] = None):
        self.scratch_dir = scratch_dir
        self._rows: List[tuple] = []
        self._seq = 0
        self._logged: dict = {}
        self.count = 0

    def add(
        self,
        path: str,
        mtime_us: Optional[int],
        record_index: Optional[str],
        error_class: str,
        message: str,
        raw: Optional[str] = None,
    ) -> None:
        self.count += 1
        message = (message or "")[:_MESSAGE_LIMIT]
        if self.scratch_dir is None:
            self._log(path, record_index, error_class, message)
            return
        self._rows.append((path, mtime_us, record_index, error_class, message, raw))
        if len(self._rows) >= _FLUSH_ROWS:
            self.flush()

    def add_error(
        self,
        path: str,
        mtime_us: Optional[int],
        exc: BaseException,
        diag: Optional[Diagnostics] = None,
        rows_kept: int = 0,
        record_index: Optional[str] = None,
    ) -> None:
        """A read that raised: one record (``record_index`` None = the file)."""
        self.add(
            path,
            mtime_us,
            record_index,
            error_class(exc),
            describe(exc, diag, rows_kept),
        )

    def hook(self, path: str, mtime_us: Optional[int]) -> Callable:
        """The :data:`HOOK` callable for one file."""

        def report(record_index, error_class, message, raw=None):
            self.add(path, mtime_us, record_index, error_class, message, raw)

        return report

    def _log(self, path, record_index, error_class, message) -> None:
        n = self._logged.get(path, 0) + 1
        self._logged[path] = n
        if n <= _LOG_PER_FILE:
            _LOG.warning(
                "read_paths: skipped a bad record in %s (%s): %s: %s",
                path,
                record_index if record_index is not None else "whole file",
                error_class,
                message,
            )

    def flush(self) -> None:
        if not self._rows or self.scratch_dir is None:
            return
        import pyarrow.parquet as pq

        cols = list(zip(*self._rows))
        table = pa.Table.from_arrays(
            [pa.array(c, type=f.type) for c, f in zip(cols, FRAGMENT_SCHEMA)],
            schema=FRAGMENT_SCHEMA,
        )
        name = _fragment_name(self._seq)
        tmp = os.path.join(self.scratch_dir, "." + name)  # hidden until complete
        try:
            os.makedirs(self.scratch_dir, exist_ok=True)
            pq.write_table(table, tmp)
            os.replace(tmp, os.path.join(self.scratch_dir, name))
        except OSError as e:
            raise OSError(
                f"cannot write bad records to {self.scratch_dir}: {e}. "
                "on_error='quarantine' keeps them in a hidden folder under the "
                "landing folder while a micro-batch is read: give the pipeline "
                "write access to that folder, or use on_error='fail'."
            ) from e
        self._seq += 1
        self._rows = []

    def close(self) -> None:
        self.flush()
        for path, n in self._logged.items():
            if n > _LOG_PER_FILE:
                _LOG.warning(
                    "read_paths: %d more bad records skipped in %s",
                    n - _LOG_PER_FILE,
                    path,
                )


def hook_of(reader) -> Optional[Callable]:
    """The bad-record hook ``read_paths`` set on ``reader``, or None."""
    return getattr(reader, HOOK, None)


# ---------------------------------------------------------------------------
# Labels and per-row checks
# ---------------------------------------------------------------------------


def part_label(part) -> Optional[str]:
    """``record_index`` of a reader partition that is part of a file (a raster
    tile window, a group of PMTiles tiles); None when it is the whole file."""
    window = getattr(part, "window", None)
    if window is not None:
        return "window=" + ",".join(str(int(v)) for v in window)
    tiles = getattr(part, "tiles", None)
    if tiles:
        first, last = tiles[0], tiles[-1]
        span = "/".join(map(str, first))
        if len(tiles) > 1:
            span += ".." + "/".join(map(str, last))
        return f"tiles={span}"
    return None


def geometry_column(schema: pa.Schema) -> Optional[str]:
    """The geometry column of a vector reader's output: ``g`` with ``g_srid``
    and ``g_srid_proj`` beside it."""
    names = set(schema.names)
    for n in schema.names:
        if f"{n}_srid" in names and f"{n}_srid_proj" in names:
            return n
    return None


def _value(col: pa.Array, i: int):
    v = col[i]
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        raw = v.cast(pa.binary()).as_py()
        return None if raw is None else raw.decode("utf-8", "backslashreplace")
    py = v.as_py()
    if isinstance(py, (bytes, bytearray)):
        return py.hex()
    return py


def row_json(batch: pa.RecordBatch, i: int, skip: Tuple[str, ...] = ()) -> str:
    """Row ``i``'s columns (but ``skip``) as JSON; invalid UTF-8 is escaped, bytes
    become hex."""
    row = {
        name: _value(batch.column(j), i)
        for j, name in enumerate(batch.schema.names)
        if name not in skip
    }
    return json.dumps(row, default=str, ensure_ascii=False)


def _invalid_text_rows(batch: pa.RecordBatch) -> dict:
    try:
        batch.validate(full=True)
        return {}
    except pa.ArrowInvalid:
        pass
    out = {}
    for i in range(batch.num_rows):
        try:
            batch.slice(i, 1).validate(full=True)
        except pa.ArrowInvalid as e:
            out[i] = (INVALID_TEXT, " ".join(str(e).split())[:_PART_LIMIT])
    return out


def bad_rows(batch: pa.RecordBatch, geometry: Optional[str]) -> dict:
    """``{row: (error_class, message)}`` for the rows of a reader batch that are
    not data: invalid UTF-8 text (any reader), and for a vector reader
    (``geometry`` names its column) a geometry value that is not readable WKB /
    WKT. A NULL or empty geometry is data (an unlocated feature, ``POINT
    EMPTY``) and stays."""
    out = _invalid_text_rows(batch)
    if geometry is None or batch.num_rows == 0:
        return out
    import numpy as np
    import shapely

    col = batch.column(geometry)
    values = col.to_numpy(zero_copy_only=False)
    is_wkb = pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type)
    parse = shapely.from_wkb if is_wkb else shapely.from_wkt
    present = ~np.asarray(col.is_null().to_numpy(zero_copy_only=False), dtype=bool)
    if not present.any():
        return out
    geoms = np.full(len(values), None, dtype=object)
    geoms[present] = parse(values[present], on_invalid="ignore")
    unreadable = present & np.fromiter(
        (g is None for g in geoms), dtype=bool, count=len(geoms)
    )
    kind = "WKB" if is_wkb else "WKT (its WKB could not be decoded)"
    for i in np.nonzero(unreadable)[0]:
        out.setdefault(
            int(i), (UNREADABLE_GEOMETRY, f"{geometry} is not readable {kind}")
        )
    return out


# ---------------------------------------------------------------------------
# Per-file count checks (vector)
# ---------------------------------------------------------------------------

_SEQ_EXTS = (".geojsonl", ".geojsons")
_GEOJSON_EXTS = (".geojson", ".json")


def count_kind(path: str) -> Optional[str]:
    """How a vector file's records are counted: ``"lines"`` (GeoJSONSeq), None
    (a GeoJSON document: parsed whole, it fails rather than dropping features)
    or ``"info"`` (the feature count the dataset declares)."""
    low = path.lower().rstrip("/")
    if low.endswith(".zip"):
        return "info"
    if low.endswith(_SEQ_EXTS):
        return "lines"
    if low.endswith(_GEOJSON_EXTS):
        return None
    return "info"


def _records(path: str):
    """``(line_number, text)`` of each record of a GeoJSON text sequence: one per
    non-blank line, or one per RS (0x1E) separator when the file uses them."""
    from databricks.labs.gbx.ds import _listing

    with _listing._retry_transient(lambda: open(path, "rb")) as fh:
        rs = None  # decided by the first non-blank byte
        start, buf = 0, []
        for n, raw in enumerate(fh, start=1):  # streamed, one line at a time
            if rs is None:
                head = raw.lstrip()
                if not head:
                    continue
                rs = head[:1] == b"\x1e"
            if not rs:
                text = raw.strip()
                if text:
                    yield n, text
                continue
            pieces = raw.split(b"\x1e")
            for k, piece in enumerate(pieces):
                if k > 0:  # an RS: the record so far is complete
                    text = b"".join(buf).strip()
                    if text:
                        yield start, text
                    start, buf = n, []
                buf.append(piece)
        if rs:
            text = b"".join(buf).strip()
            if text:
                yield start, text


def sequence_record_count(path: str) -> int:
    """Records of a GeoJSON text sequence (non-blank lines, or RS records)."""
    return sum(1 for _ in _records(path))


def unparseable_lines(path: str, limit: int = 10000) -> List[Tuple[int, str, str]]:
    """``(line, error, text)`` of the records of a GeoJSON text sequence that are
    not a JSON object (at most ``limit``)."""
    out = []
    for n, raw in _records(path):
        text = raw.decode("utf-8", "backslashreplace")
        try:
            ok = isinstance(json.loads(text), dict)
            err = "not a JSON object"
        except ValueError as e:
            ok, err = False, f"{type(e).__name__}: {e}"
        if not ok:
            out.append((n, err[:_PART_LIMIT], text))
            if len(out) >= limit:
                break
    return out


def dbf_deleted_count(shp_path: str) -> int:
    """Records a shapefile's ``.dbf`` marks deleted (OGR skips them; they are
    not bad records). 0 when there is no plain ``.dbf`` beside the ``.shp``."""
    base, ext = os.path.splitext(shp_path)
    if ext.lower() != ".shp":
        return 0
    for cand in (base + ".dbf", base + ".DBF"):
        if os.path.exists(cand):
            break
    else:
        return 0
    import struct

    with open(cand, "rb") as fh:
        head = fh.read(32)
        if len(head) < 12:
            return 0
        n, header_len, rec_len = struct.unpack("<IHH", head[4:12])
        if rec_len <= 0:
            return 0
        fh.read(max(0, header_len - 32))
        deleted = 0
        for _ in range(n):
            rec = fh.read(rec_len)
            if len(rec) < 1:
                break
            if rec[:1] == b"*":
                deleted += 1
        return deleted


_IDENT = re.compile(r'"')


def quote_ident(name: str) -> str:
    """A double-quoted SQL identifier."""
    return '"' + _IDENT.sub('""', name) + '"'
