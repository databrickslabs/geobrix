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
those to ``<table>_bad_records`` first, then writes the rows staged beside them);
without one it logs them (``read_paths(on_error="skip")``).

Errors of the environment, not of the data (out of memory, a missing module, a
transient I/O error, a file too large for the task's memory) are never bad
records: they fail the read so that Spark retries the task or the update fails
loudly (see :func:`is_environment_error`).

Readers that check their own records (LiDAR points, PMTiles tiles, NetCDF
slices, EXIF files) report through the hook :data:`HOOK` that ``read_paths``
sets on the reader: ``hook(record_index, error_class, message, raw=None)``. A
reader with no hook keeps its own behavior.

Connect-safe: no sparkContext / .rdd / _jvm / conf.set.
"""

from __future__ import annotations

import contextlib
import errno
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
    NOTHING_READABLE,
)


# ---------------------------------------------------------------------------
# Errors of the environment, not of the data
# ---------------------------------------------------------------------------


class EnvironmentFailure(RuntimeError):
    """A read that failed because of where it ran, not what it read."""


class FileTooLarge(EnvironmentFailure):
    """A file too large to read whole in the task's memory."""


# errnos of an I/O or system failure (FUSE, network, permissions, disk), as
# opposed to a library's parse error (rasterio, PIL, netCDF raise OSError
# without an errno, or with a negative library code).
_ENVIRONMENT_ERRNOS = frozenset(
    getattr(errno, n)
    for n in (
        "EIO",
        "EAGAIN",
        "ETIMEDOUT",
        "ESTALE",
        "EBUSY",
        "EINTR",
        "ENOENT",
        "EACCES",
        "EPERM",
        "ENOSPC",
        "EMFILE",
        "ENFILE",
        "EROFS",
        "ENOTCONN",
        "ECONNRESET",
        "ECONNABORTED",
        "ECONNREFUSED",
        "EHOSTUNREACH",
        "ENETUNREACH",
        "ENETDOWN",
    )
    if hasattr(errno, n)
)


def is_environment_error(exc: BaseException) -> bool:
    """True for an error that is not about the data: out of memory, a missing
    module (``ImportError``), :class:`EnvironmentFailure` (a file too large for
    the task, a missing LAZ backend), or an ``OSError`` carrying a system errno
    (I/O, network, permissions, disk). Such errors fail the read instead of
    becoming bad records."""
    if isinstance(exc, (MemoryError, ImportError, EnvironmentFailure)):
        return True
    return isinstance(exc, OSError) and exc.errno in _ENVIRONMENT_ERRNOS


def is_transient(exc: BaseException) -> bool:
    """An environment error worth retrying in place (an ``OSError`` with a
    system errno)."""
    return isinstance(exc, OSError) and exc.errno in _ENVIRONMENT_ERRNOS


def reraise_environment(exc: BaseException) -> None:
    """Raise ``exc`` again when it is an environment error (see
    :func:`is_environment_error`); return otherwise."""
    if is_environment_error(exc):
        raise exc


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
        raw = cap_raw(raw)
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
            # describe() messages start with the error class already.
            text = (
                message
                if message.startswith(error_class)
                else (f"{error_class}: {message}")
            )
            _LOG.warning(
                "read_paths: skipped a bad record in %s (%s): %s",
                path,
                record_index if record_index is not None else "whole file",
                text,
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
                e.errno,
                f"cannot write bad records to {self.scratch_dir}: {e}. "
                "on_error='quarantine' stages each micro-batch under "
                "declare_ingest's scratch_dir (default: source_dir): pass a "
                "scratch_dir the pipeline can write, or use on_error='fail'.",
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


_RAW_LIMIT = 4096  # characters of one record's raw JSON


def _describe_bytes(value) -> str:
    return f"<binary, {len(value)} bytes>"


def _plain(value):
    """``value`` with every bytes value (nested too) replaced by a descriptor:
    a tile or blob payload is not something to dump into a record."""
    if isinstance(value, (bytes, bytearray)):
        return _describe_bytes(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _value(col: pa.Array, i: int):
    v = col[i]
    if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
        raw = v.cast(pa.binary()).as_py()
        return None if raw is None else raw.decode("utf-8", "backslashreplace")
    if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
        raw = v.as_py()
        return None if raw is None else _describe_bytes(raw)
    return _plain(v.as_py())


def row_json(batch: pa.RecordBatch, i: int, skip: Tuple[str, ...] = ()) -> str:
    """Row ``i``'s columns (but ``skip``) as JSON, at most :data:`_RAW_LIMIT`
    characters: invalid UTF-8 is escaped, binary values become a descriptor
    (``<binary, N bytes>``), and a longer document is kept as
    ``{"truncated": true, "json": <its first characters>}``."""
    row = {
        name: _value(batch.column(j), i)
        for j, name in enumerate(batch.schema.names)
        if name not in skip
    }
    return cap_raw(json.dumps(row, default=str, ensure_ascii=False))


def cap_raw(text: Optional[str]) -> Optional[str]:
    """``text`` (a record's raw JSON) if it has at most :data:`_RAW_LIMIT`
    characters, else ``{"truncated": true, "json": <its first characters>}``."""
    if text is None or len(text) <= _RAW_LIMIT:
        return text
    head = text[: _RAW_LIMIT - 64]
    out = json.dumps({"truncated": True, "json": head}, ensure_ascii=False)
    while len(out) > _RAW_LIMIT:  # escaping can grow it
        head = head[: len(head) - (len(out) - _RAW_LIMIT) - 8]
        out = json.dumps({"truncated": True, "json": head}, ensure_ascii=False)
    return out


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


# WKB geometry types shapely cannot build (curves, surfaces: ISO 8-17) though
# they are valid data; their Z / M / ZM variants add 1000 / 2000 / 3000 (ISO),
# or set the EWKB high-bit flags.
_UNPARSED_WKB_TYPES = frozenset(range(8, 18))
# Text the light vector reader writes, as WKT output under a read_paths hook,
# for a WKB it could not decode, and for a curve or surface (valid, but shapely
# cannot write it as WKT).
UNREADABLE_WKB = "UNREADABLE WKB"
UNWRITABLE_WKB = "CURVE OR SURFACE WKB"
_MARKER_MESSAGES = {
    UNREADABLE_WKB: "the reader could not decode its WKB to WKT",
    UNWRITABLE_WKB: "a curve or surface cannot be written as WKT; read it with "
    "asWKB=true",
}


def _walk_wkb(buf, off: int = 0, depth: int = 0) -> Tuple[int, bool]:
    """``(offset after the geometry at off, whether it is or holds a curve or
    surface)`` for a WKB / EWKB value; raises on a malformed one. Walks into
    collections (a GeometryCollection holding a TIN, say)."""
    import struct

    if depth > 32:
        raise ValueError("WKB nested too deep")
    order = buf[off]
    if order not in (0, 1):
        raise ValueError("not a WKB byte order")
    e = "<" if order == 1 else ">"
    (code,) = struct.unpack_from(e + "I", buf, off + 1)
    off += 5
    iso = code & 0x0FFFFFFF
    base, dims = iso % 1000, iso // 1000
    z = bool(code & 0x80000000) or dims in (1, 3)
    m = bool(code & 0x40000000) or dims in (2, 3)
    if code & 0x20000000:  # EWKB SRID
        off += 4
    point = 8 * (2 + z + m)

    def count():
        (n,) = struct.unpack_from(e + "I", buf, off)
        return n

    if base == 1:
        end = off + point
    elif base in (2, 8):  # LineString, CircularString
        end = off + 4 + count() * point
    elif base in (3, 17):  # Polygon, Triangle
        rings, end = count(), off + 4
        for _ in range(rings):
            (n,) = struct.unpack_from(e + "I", buf, end)
            end += 4 + n * point
    elif base in (4, 5, 6, 7, 9, 10, 11, 12, 15, 16):  # multi / collections
        n, end, inner = count(), off + 4, False
        for _ in range(n):
            end, u = _walk_wkb(buf, end, depth + 1)
            inner = inner or u
        if end > len(buf):
            raise ValueError("WKB ends early")
        return end, inner or base in _UNPARSED_WKB_TYPES
    else:
        raise ValueError(f"WKB geometry type {base}")
    if end > len(buf):
        raise ValueError("WKB ends early")
    return end, base in _UNPARSED_WKB_TYPES


def _holds_unparsed(value) -> bool:
    """True if a WKB value is, or holds, a curve or surface (types 8-17)."""
    if wkb_base_type(value) in _UNPARSED_WKB_TYPES:
        return True
    if wkb_base_type(value) != 7:
        return False
    try:
        return _walk_wkb(value)[1]
    except Exception:  # noqa: BLE001 - malformed: shapely decides
        return False


def wkb_base_type(value) -> Optional[int]:
    """The base geometry type (1-17) a WKB / EWKB value declares, or None when
    its header is not one."""
    if value is None or len(value) < 5 or value[0] not in (0, 1):
        return None
    code = int.from_bytes(value[1:5], "little" if value[0] == 1 else "big")
    return (code & 0x0FFFFFFF) % 1000


def _readable_wkb(values) -> "object":
    """Per value: True if readable as a geometry. Curves and surfaces are taken
    as declared (shapely cannot build them); every other value is parsed,
    vectorized, then one by one if the batch parse raises (a curve inside a
    collection), so only the offending value is flagged."""
    import numpy as np
    import shapely

    n = len(values)
    ok = np.zeros(n, dtype=bool)
    todo = []
    for i, v in enumerate(values):
        if v is None:
            continue
        if _holds_unparsed(v):
            ok[i] = True
        else:
            todo.append(i)
    if not todo:
        return ok
    idx = np.asarray(todo)
    sub = np.empty(len(todo), dtype=object)
    sub[:] = [values[i] for i in todo]
    try:
        parsed = shapely.from_wkb(sub, on_invalid="ignore")
        ok[idx] = np.fromiter(
            (g is not None for g in parsed), dtype=bool, count=len(sub)
        )
        return ok
    except Exception:  # noqa: BLE001 - parse value by value below
        pass
    for i in todo:
        try:
            ok[i] = shapely.from_wkb(values[i], on_invalid="ignore") is not None
        except NotImplementedError:  # a geometry shapely cannot build: valid
            ok[i] = True
        except Exception:  # noqa: BLE001 - not a geometry
            ok[i] = False
    return ok


def wkt_or_marker(raw) -> "object":
    """WKT for each WKB value (the light vector reader's ``asWKB=false`` output
    under a read_paths hook): None for None, :data:`UNWRITABLE_WKB` for a curve
    or surface, :data:`UNREADABLE_WKB` for a value that is not WKB."""
    import numpy as np
    import shapely

    out = np.full(len(raw), None, dtype=object)

    def one(v):
        if _holds_unparsed(v):
            return UNWRITABLE_WKB
        try:
            g = shapely.from_wkb(v, on_invalid="ignore")
        except NotImplementedError:  # a curve inside a collection
            return UNWRITABLE_WKB
        except Exception:  # noqa: BLE001 - not WKB
            return UNREADABLE_WKB
        return UNREADABLE_WKB if g is None else shapely.to_wkt(g)

    todo = [i for i, v in enumerate(raw) if v is not None]
    if not todo:
        return out
    sub = np.empty(len(todo), dtype=object)
    sub[:] = [raw[i] for i in todo]
    try:
        if any(_holds_unparsed(v) for v in sub):
            raise NotImplementedError
        geoms = shapely.from_wkb(sub, on_invalid="ignore")
        wkt = shapely.to_wkt(geoms)
        for i, g, w in zip(todo, geoms, wkt):
            out[i] = UNREADABLE_WKB if g is None else w
    except Exception:  # noqa: BLE001 - value by value
        for i in todo:
            out[i] = one(raw[i])
    return out


def bad_rows(batch: pa.RecordBatch, geometry: Optional[str]) -> dict:
    """``{row: (error_class, message)}`` for the rows of a reader batch that are
    not data: invalid UTF-8 text (any reader), and for a vector reader
    (``geometry`` names its column) a geometry value that is not readable WKB --
    or, as WKT output, the reader's :data:`UNREADABLE_WKB` marker. A NULL or
    empty geometry is data (an unlocated feature, ``POINT EMPTY``), and so are
    curves and surfaces shapely cannot build."""
    out = _invalid_text_rows(batch)
    if geometry is None or batch.num_rows == 0:
        return out
    import numpy as np

    col = batch.column(geometry)
    if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
        values = col.to_pylist()
        present = np.fromiter(
            (v is not None for v in values), dtype=bool, count=len(values)
        )
        bad = present & ~_readable_wkb(values)
        message = f"{geometry} is not readable WKB"
        for i in np.nonzero(bad)[0]:
            out.setdefault(int(i), (UNREADABLE_GEOMETRY, message))
        return out
    for i, v in enumerate(col.to_pylist()):
        if v in _MARKER_MESSAGES:
            out.setdefault(
                i, (UNREADABLE_GEOMETRY, f"{geometry}: {_MARKER_MESSAGES[v]}")
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


def _deleted_in(fh) -> int:
    """Records a ``.dbf`` stream marks deleted (read sequentially)."""
    import struct

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


def dbf_deleted_count(shp_path: str) -> int:
    """Records a shapefile's ``.dbf`` marks deleted (OGR skips them; they are
    not bad records): the ``.dbf`` beside a ``.shp``, or the one ``.dbf`` inside
    a zipped shapefile (``.shp.zip`` / ``.shz`` / ``.zip``). 0 when there is
    none (or a zip holds several)."""
    low = shp_path.lower()
    if low.endswith((".zip", ".shz")):
        import zipfile

        try:
            with zipfile.ZipFile(shp_path) as zf:
                dbfs = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if len(dbfs) != 1:
                    return 0
                with zf.open(dbfs[0]) as fh:
                    return _deleted_in(fh)
        except zipfile.BadZipFile:
            return 0
        except OSError as e:
            reraise_environment(e)  # I/O: fail the read; else a damaged zip
            return 0
    base, ext = os.path.splitext(shp_path)
    if ext.lower() != ".shp":
        return 0
    for cand in (base + ".dbf", base + ".DBF"):
        if os.path.exists(cand):
            with open(cand, "rb") as fh:
                return _deleted_in(fh)
    return 0


_IDENT = re.compile(r'"')


def quote_ident(name: str) -> str:
    """A double-quoted SQL identifier."""
    return '"' + _IDENT.sub('""', name) + '"'
