"""Light Spark Python DataSource for LAS/LAZ point clouds (lidar_gbx).

mode="metadata": one row per file, header-only (cheap; never loads points).
mode="points":   one row per point, chunk-iterated with class/return/decimate filters.

Serverless-safe: session-free partitions()/read(); one InputPartition per file.
Uses no forbidden Spark internal APIs — safe on Spark Connect / Serverless.
"""

import io
import os
import struct
import warnings
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Union,
)

import numpy as np
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from databricks.labs.gbx.core.crs import crs_to_canonical, resolve_crs
from databricks.labs.gbx.ds import _listing, _quarantine
from databricks.labs.gbx.ds.file_gbx import list_local_files

if TYPE_CHECKING:
    import pyarrow as pa

LIDAR_META_SCHEMA = StructType(
    [
        StructField("path", StringType(), False),
        StructField("name", StringType(), False),
        StructField("point_count", LongType(), True),
        StructField("x_min", DoubleType(), True),
        StructField("x_max", DoubleType(), True),
        StructField("y_min", DoubleType(), True),
        StructField("y_max", DoubleType(), True),
        StructField("z_min", DoubleType(), True),
        StructField("z_max", DoubleType(), True),
        StructField("crs", StringType(), True),
        StructField("point_format", IntegerType(), True),
        StructField("dimensions", ArrayType(StringType()), True),
        StructField("scale", ArrayType(DoubleType()), True),
        StructField("offset", ArrayType(DoubleType()), True),
        StructField("return_histogram", ArrayType(LongType()), True),
        StructField("density", DoubleType(), True),
        StructField("version", StringType(), True),
        StructField("size", LongType(), True),
        StructField("modificationTime", TimestampNTZType(), True),
    ]
)

LIDAR_POINTS_SCHEMA = StructType(
    [
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("z", DoubleType(), True),
        StructField("intensity", IntegerType(), True),
        StructField("return_number", IntegerType(), True),
        StructField("number_of_returns", IntegerType(), True),
        StructField("classification", IntegerType(), True),
        StructField("gps_time", DoubleType(), True),
    ]
)

_LIDAR_EXTS = (".las", ".laz")


def _parse_int_list(value: Optional[str]) -> Optional[List[int]]:
    """Parse a comma-separated string of ints, or return None if value is absent."""
    if value is None:
        return None
    return [int(v.strip()) for v in value.split(",") if v.strip()]


_POINT_FIELD_NAMES = tuple(f.name for f in LIDAR_POINTS_SCHEMA.fields)


def _select_point_dimensions(value: Optional[str]) -> Optional[List[str]]:
    """Parse the ``dimensions`` points-mode option: a comma-separated subset of the
    base point columns to return. Returns the subset in canonical schema order
    (de-duplicated); ``None`` / absent / empty -> all columns. An unknown name raises
    ``ValueError`` listing the allowed names."""
    if value is None:
        return None
    requested = {v.strip() for v in value.split(",") if v.strip()}
    if not requested:
        return None
    unknown = sorted(d for d in requested if d not in _POINT_FIELD_NAMES)
    if unknown:
        raise ValueError(
            f"lidar_gbx: unknown dimension(s) {unknown}; "
            f"allowed: {list(_POINT_FIELD_NAMES)}"
        )
    return [n for n in _POINT_FIELD_NAMES if n in requested]


def _points_schema(dims: Optional[List[str]]) -> StructType:
    """Points schema restricted to *dims* (schema order); all columns if ``None``."""
    if dims is None:
        return LIDAR_POINTS_SCHEMA
    keep = set(dims)
    return StructType([f for f in LIDAR_POINTS_SCHEMA.fields if f.name in keep])


def _read_dim(chunk, name: str, n: int) -> np.ndarray:
    """One point dimension of a chunk as a numpy array (gps_time NaN when the
    point format has none)."""
    if name in ("x", "y", "z"):
        return np.asarray(getattr(chunk, name), dtype=np.float64)
    if name == "gps_time":
        if "gps_time" in chunk.point_format.dimension_names:
            return np.asarray(chunk.gps_time, dtype=np.float64)
        return np.full(n, np.nan, dtype=np.float64)
    return np.asarray(getattr(chunk, name))


class _LidarFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class LidarGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        raw_path = options.get("path")
        if not raw_path:
            raise ValueError("lidar_gbx requires a 'path' (e.g. .load(path)).")
        # Normalise once (strip any file:/dbfs: scheme) so list_local_files and the
        # per-file reads all see a bare local path — matches the vector reader.
        self.path = _listing.to_local_path(raw_path)
        self.mode = (options.get("mode") or "points").lower()
        # Points-mode options
        self.class_filter: Optional[List[int]] = _parse_int_list(
            options.get("classFilter")
        )
        return_filter_raw = options.get("returnFilter")
        self.return_filter: Optional[int] = (
            int(return_filter_raw) if return_filter_raw is not None else None
        )
        self.decimate: int = int(options.get("decimate", "1"))
        self.chunk_size: int = int(options.get("chunkSize", "1000000"))
        # Under a read_paths / declare_ingest bad-record hook only: set aside points
        # far outside the bounds the file header declares (see _header_bounds).
        # On by default: it is what catches garbage from damaged LAZ data that
        # decodes without an error. "false" turns it off.
        self.check_header_bounds: bool = (
            str(options.get("checkHeaderBounds", "true")).lower() == "true"
        )
        # Optional column projection: comma-separated subset of the base point
        # columns to return (schema order). None = all columns.
        self.dimensions: Optional[List[str]] = _select_point_dimensions(
            options.get("dimensions")
        )

    def partitions(self) -> Sequence[InputPartition]:
        files = list_local_files(self.path, extensions=_LIDAR_EXTS)
        return [_LidarFilePartition(f) for f in files]

    def read(self, partition: "_LidarFilePartition") -> Iterator["pa.RecordBatch"]:
        if self.mode == "metadata":
            yield from self._read_metadata(partition.file_path)
        else:
            yield from self._read_points(partition.file_path)

    def _read_points(self, file_path: str) -> Iterator["pa.RecordBatch"]:
        import laspy
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_type

        local = _listing.to_local_path(file_path)
        # Tolerate bad nodes without dropping healthy ones: retry transient UC Volume
        # FUSE misses (eventual-consistency lag) so a transient miss is never mistaken
        # for a bad node, then skip only a genuinely empty (0-byte) or unreadable /
        # corrupt file — a single bad .laz must not fail a distributed read (mirrors the
        # downloader's "skip nodes that never came back"). Under read_paths'
        # on_error="skip" the skipped file is a bad record instead of a warning.
        opened = self._open(laspy, local)
        if opened is None:
            return
        st, opener = opened
        # A LAZ chunk table is checked before any decoder exists: lazrs sizes buffers
        # from numbers in it, and a refused allocation aborts the whole process.
        table = _checked_table(opener, local, st.st_size)
        if isinstance(table, str):
            self._skip(local, table)
            return
        # Columnar (Arrow) output: one RecordBatch per chunk, gathered with vectorised
        # numpy — never one Python tuple per point. A full-resolution read (decimate=1)
        # over hundreds of millions of returns would otherwise stall or kill the Spark
        # session on per-row conversion. The `dimensions` option restricts the emitted
        # columns; only the requested dimensions (plus any a filter needs) are read.
        # Arrow types match LIDAR_POINTS_SCHEMA (IntegerType -> int32, DoubleType ->
        # float64).
        out_fields = self.dimensions or list(_POINT_FIELD_NAMES)
        out_set = set(out_fields)
        arrow_schema = pa.schema(
            [
                pa.field(f.name, to_arrow_type(f.dataType))
                for f in LIDAR_POINTS_SCHEMA.fields
                if f.name in out_set
            ]
        )
        # Read the emitted dimensions plus any a filter needs: classification for
        # classFilter, return_number for returnFilter — these feed the keep mask but
        # are not emitted unless also requested.
        need = set(out_fields)
        if self.class_filter is not None:
            need.add("classification")
        if self.return_filter is not None:
            need.add("return_number")
        hook = _quarantine.hook_of(self)
        check_bounds = hook is not None and self.check_header_bounds
        if check_bounds:
            need |= {"x", "y", "z"}  # checked against the header bounds

        seen = 0  # filter-passing points seen in this file (decimate parity)
        bounds = None

        def _emit(chunk, start: int):
            nonlocal seen
            n = len(chunk)
            cols = {name: _read_dim(chunk, name, n) for name in need}

            keep = np.ones(n, dtype=bool)
            if self.class_filter is not None:
                keep &= np.isin(cols["classification"], self.class_filter)
            if self.return_filter is not None:
                keep &= cols["return_number"] == self.return_filter
            if bounds is not None:
                keep &= ~_out_of_bounds(cols, keep, bounds, start, hook)

            if self.decimate > 1:
                # Keep every Nth filter-passing point via a 1-based counter that
                # runs continuously across chunks (identical selection to the
                # row-wise reader this replaces).
                masked_idx = np.nonzero(keep)[0]
                emit_no = seen + np.arange(1, masked_idx.size + 1)
                keep[masked_idx[emit_no % self.decimate != 0]] = False
                seen += masked_idx.size

            if not keep.any():
                return None
            arrays = []
            for f in LIDAR_POINTS_SCHEMA.fields:
                if f.name not in out_set:
                    continue
                at = to_arrow_type(f.dataType)
                a = cols[f.name][keep]
                if pa.types.is_integer(at):
                    a = a.astype(np.int32)
                arrays.append(pa.array(a, type=at))
            return pa.record_batch(arrays, schema=arrow_schema)

        with opener as reader:
            if hook is None:
                for chunk in reader.chunk_iterator(self.chunk_size):
                    batch = _emit(chunk, 0)
                    if batch is not None:
                        yield batch
                return
            if check_bounds:
                bounds = _header_bounds(reader.header)
            limit = _readable_points(reader.header, local, st.st_size, hook, table)
            yield from _checked_chunks(
                laspy, local, reader, limit, self.chunk_size, _emit, hook
            )

    def _skip(self, local: str, message: str) -> None:
        """Set a file aside as unreadable: a bad record under a read_paths hook,
        a warning otherwise (as :meth:`_open` does)."""
        hook = _quarantine.hook_of(self)
        if hook is not None:
            hook(None, _quarantine.UNREADABLE_FILE, message)
        else:
            warnings.warn(
                f"lidar_gbx: skipping unreadable file {local!r}: {message}",
                stacklevel=3,
            )

    def _open(self, laspy, local: str):
        """``(stat, laspy opener)`` for a file, or None when it is empty or
        cannot be opened: skipped with a warning, or (with a read_paths hook) a
        bad record."""
        hook = _quarantine.hook_of(self)
        try:
            st = _listing._retry_transient(lambda: os.stat(local))
            if st.st_size == 0:
                if hook is not None:
                    hook(None, _quarantine.UNREADABLE_FILE, "empty file (0 bytes)")
                else:
                    warnings.warn(
                        f"lidar_gbx: skipping empty file {local!r} (0 bytes)",
                        stacklevel=3,
                    )
                return None
            return st, _listing._retry_transient(lambda: laspy.open(local))
        except (
            Exception
        ) as exc:  # noqa: BLE001 — corrupt/truncated or persistently missing
            if hook is not None:
                _raise_if_environment(exc)  # memory, LAZ backend, I/O: not the file's
                hook(None, _quarantine.error_class(exc), _quarantine.describe(exc))
            else:
                warnings.warn(
                    f"lidar_gbx: skipping unreadable file {local!r}: {exc}",
                    stacklevel=3,
                )
            return None

    def _read_metadata(self, file_path: str) -> Iterator["pa.RecordBatch"]:
        import datetime as _dt

        import laspy
        import pyarrow as pa
        from pyspark.sql.pandas.types import to_arrow_schema

        local = _listing.to_local_path(file_path)
        source = _listing.to_spark_uri(file_path)
        name = os.path.basename(local)
        # Same bad-node tolerance as _read_points: retry transient UC Volume FUSE
        # misses (eventual-consistency lag), then skip only a genuinely empty or
        # unreadable / corrupt file so one bad node can't fail a directory scan.
        opened = self._open(laspy, local)
        if opened is None:
            return
        st, opener = opened
        with opener as reader:
            h = reader.header
            mins = h.mins
            maxs = h.maxs
            try:
                crs_obj = h.parse_crs()
                crs = (
                    crs_to_canonical(resolve_crs(crs_obj.to_wkt()))
                    if crs_obj is not None
                    else None
                )
            except Exception:  # noqa: BLE001
                crs = None
            dims = [d.name for d in h.point_format.dimensions]
            # getattr + len check avoids `or []` on a numpy array (ambiguous truth value).
            rh_raw = getattr(h, "number_of_points_by_return", None)
            rh = (
                [int(x) for x in rh_raw]
                if rh_raw is not None and len(rh_raw) > 0
                else []
            )
            area = float((maxs[0] - mins[0]) * (maxs[1] - mins[1]))
            density = (float(h.point_count) / area) if area > 0 else None
            # One row per file, emitted columnar (Arrow) for output consistency with
            # points mode. Build a single-row batch from Python values against the
            # declared schema so nested-list and timestamp types resolve exactly.
            row = {
                "path": source,
                "name": name,
                "point_count": int(h.point_count),
                "x_min": float(mins[0]),
                "x_max": float(maxs[0]),
                "y_min": float(mins[1]),
                "y_max": float(maxs[1]),
                "z_min": float(mins[2]),
                "z_max": float(maxs[2]),
                "crs": crs,
                "point_format": int(h.point_format.id),
                "dimensions": dims,
                "scale": [float(s) for s in h.scales],
                "offset": [float(o) for o in h.offsets],
                "return_histogram": rh,
                "density": density,
                "version": str(h.version),
                "size": int(st.st_size),
                "modificationTime": _dt.datetime.fromtimestamp(st.st_mtime),
            }
            yield pa.RecordBatch.from_pylist(
                [row], schema=to_arrow_schema(LIDAR_META_SCHEMA)
            )


# Points re-read at a time to narrow down a chunk that failed (a LAZ chunk is
# then lost from where it stops decoding to its end, and nothing else).
_SALVAGE_POINTS = 1000
# Unread points in a row after which the rest of a file is given up (a decoder
# that fails on everything, a garbled huge point count): at least three chunks,
# and well past one LAZ chunk (~50,000 points) so that one bad LAZ chunk never
# ends the read, whatever chunkSize is.
_GIVE_UP_CHUNKS = 3
_GIVE_UP_POINTS = 250_000

# Limits for the sanity check of a LAZ chunk table (_read_chunk_table): plausibility
# bounds on numbers read from the file, far above anything a writer produces and far
# below what would exhaust a worker; not a memory budget.
_MAX_LAZ_CHUNKS = 1 << 24  # 16.7 million chunks (800 billion points at 50,000)
_MAX_LAZ_CHUNK_BYTES = 128 << 20  # a chunk that decodes to more than 128 MiB
# The chunk size a LASzip record holds when chunks vary in size (COPC files).
_VARIABLE_CHUNK_SIZE = 0xFFFFFFFF

# Dense runs of 0xFF in compressed point data (see _dense_ff_in): the blocks scanned,
# the 0xFF bytes in one that make it damaged (a random block holds about one, a sound
# LAZ stream's longest run is a few bytes), and the bytes read at a time (a multiple
# of the block).
_FF_BLOCK = 256
_FF_BLOCK_LIMIT = 128
_FF_SCAN_BYTES = 1 << 20
_DENSE_FF_PROBLEM = (
    "compressed point data looks damaged (a run of 0xFF bytes); "
    "the decoder would crash on it"
)


class _ChunkTable(NamedTuple):
    """A LAZ chunk table that passed the check, as the decoder will see it."""

    entries: list  # [(points, bytes), ...] as lazrs.read_chunk_table returns them
    item_size: int  # bytes per point record (LazVlr.item_size())
    chunk_size: Optional[int]  # points per chunk (LASzip record); None = variable


def _checked_table(opener, local: str, size: int) -> Union[str, _ChunkTable, None]:
    """:func:`_read_chunk_table` of a file opened by laspy (``opener``), which is
    closed when the table is refused or the check raises."""
    try:
        table = _read_chunk_table(opener.header, local, size)
    except BaseException:  # the second open failed (I/O, memory): close the first
        opener.close()
        raise
    if isinstance(table, str):
        opener.close()
    return table


def _find_chunk_table(f, start: int, size: int):
    """``(offset, chunk count)`` of the chunk table of the LAZ points at ``start``,
    or a str saying why it cannot be found. ``f`` is the open file."""
    f.seek(start)
    raw = f.read(8)
    if len(raw) < 8:
        return "the file ends before its point data does"
    at = struct.unpack("<q", raw)[0]
    if at <= start:  # the writer could not update it: the file's last 8 bytes
        f.seek(max(size - 8, 0))
        tail = f.read(8)
        at = struct.unpack("<q", tail)[0] if len(tail) == 8 else -1
    if not start + 8 <= at <= size - 8:
        return f"the chunk table offset ({at}) is outside the file"
    f.seek(at)
    head = f.read(8)
    if len(head) < 8:
        return "the file ends inside the chunk table"
    _version, count = struct.unpack("<II", head)
    return at, count


def _needed_chunks(entries, total: int) -> list:
    """The chunks a read needs: the first ones holding the header's ``total``
    points. Entries after them (an empty last chunk, say) are never read, so
    their sizes are not held against the file and their bytes are not scanned."""
    needed, points_left = [], total
    for points, nbytes in entries:
        if points_left <= 0:
            break
        needed.append((points, nbytes))
        points_left -= int(points)
    return needed


def _chunk_sizes_problem(entries, total: int, item: int, room: int) -> Optional[str]:
    """Why the ``(points, bytes)`` of the chunks cannot be read, or None. ``total``
    is the header's point count, ``item`` the bytes of a point, ``room`` the bytes
    from the start of the compressed points to the end of the file."""
    needed = _needed_chunks(entries, total)
    if sum(int(nbytes) for _, nbytes in needed) > room:
        return "the chunk table sizes run past the end of the file"
    for points, _ in entries:  # lazrs sizes a buffer from the biggest chunk
        if int(points) * item > _MAX_LAZ_CHUNK_BYTES:
            return f"the chunk table declares a chunk of {points} points"
    for points, nbytes in needed:
        if nbytes > 2 * int(points) * item + 4096:  # no bigger than its raw points
            return (
                f"the chunk table declares a chunk of {points} points as {nbytes} bytes"
            )
    return None


class _LaszipRecord(NamedTuple):
    """The LASzip record of a LAZ file, decoded."""

    vlr: object  # lazrs.LazVlr
    item_size: int  # bytes per point record
    chunk_size: Optional[int]  # points per chunk; None = variable


def _most_chunks(points: int, chunk_size: Optional[int]) -> int:
    """The most chunks a LAZ file of ``points`` points can have: ``chunk_size``
    points each (fixed chunks) or at least one each (variable chunks), plus the
    empty chunk lazrs's writer ends a file with."""
    return -(-points // (1 if chunk_size is None else chunk_size)) + 1


def _laszip_record(h) -> Union[str, _LaszipRecord, None]:
    """The LASzip record of a LAZ file, checked; a str saying why it cannot go to
    the decoder; None for a file that is not LAZ, or when lazrs (so no abort to
    guard against) is not the decoder."""
    if not h.are_points_compressed:
        return None
    try:
        import lazrs
    except ImportError:  # another LAZ backend (or none): nothing of lazrs's to guard
        return None
    laszip = h.vlrs.get("LasZipVlr")
    if not laszip:
        return "the file has no LASzip record"
    record = bytes(laszip[0].record_data)
    try:
        vlr = lazrs.LazVlr(record)
    except _quarantine.PASS_THROUGH:
        raise
    except Exception as e:  # noqa: BLE001 - LazrsError: a record that cannot decode
        _raise_if_environment(e)
        return f"the LASzip record cannot be decoded ({type(e).__name__}: {e})"
    item = max(int(vlr.item_size()), 1)
    # The record holds the points per chunk at byte 12 (all ones: chunks vary).
    chunk_size = struct.unpack_from("<I", record, 12)[0]
    if chunk_size == _VARIABLE_CHUNK_SIZE:
        chunk_size = None
    elif chunk_size == 0 or chunk_size * item > _MAX_LAZ_CHUNK_BYTES:
        return f"the LASzip record declares chunks of {chunk_size} points"
    return _LaszipRecord(vlr, item, chunk_size)


class _IOWatch:
    """A file handle that remembers the OSError its ``read`` / ``readinto`` /
    ``seek`` / ``tell`` raised. lazrs reads through the handle it is given and
    turns an error of it into a LazrsError ("Failed to use readinto to read
    bytes"), which says nothing of the Volume error behind it."""

    def __init__(self, f):
        self._f = f
        self.error: Optional[OSError] = None

    def _call(self, name: str, *args):
        try:
            return getattr(self._f, name)(*args)
        except OSError as e:
            self.error = e
            raise

    def read(self, *args):
        return self._call("read", *args)

    def readinto(self, buffer):
        return self._call("readinto", buffer)

    def seek(self, *args):
        return self._call("seek", *args)

    def tell(self):
        return self._call("tell")

    def __getattr__(self, name):  # whatever else lazrs asks of a file
        return getattr(self._f, name)


def _read_at(f, at: int, n: int) -> bytes:
    f.seek(at)
    return f.read(n)


def _dense_ff_in(f, start: int, needed: list, item: int) -> bool:
    """True when the compressed points of ``needed`` (the ``(points, bytes)`` of
    the chunks a read decodes) hold a 256-byte block that is at least half 0xFF.

    The GPS-time decompressor of the native LAZ decoder (the ``laz`` crate, in
    lazrs) calls itself once for every "switch to another sequence" symbol with no
    depth limit, and 0xFF bytes make the arithmetic decoder return that symbol
    for ever: the stack overflows, which no ``except`` catches. Erased flash or a
    failed copy leaves such runs. The chunks lie one after another from
    ``start + 8``; the first record of each (``item`` bytes) is stored
    uncompressed and never reaches the decoder, so it is not counted. ``f`` is
    read in pieces of ``_FF_SCAN_BYTES``, in order. Measured on a 250 MB file
    (20 million points), the pass costs about 4-6% of the decode time warm and up
    to about 10% cold, on local storage: it reads the compressed points once more."""
    sizes = np.array([int(nbytes) for _, nbytes in needed], dtype=np.int64)
    ends = np.cumsum(sizes)
    begins = ends - sizes
    raw_ends = begins + np.minimum(item, sizes)  # the uncompressed first records
    base, total, done = start + 8, int(ends[-1]) if sizes.size else 0, 0
    while done < total:
        want = min(_FF_SCAN_BYTES, total - done)
        data = _listing._retry_transient(lambda: _read_at(f, base + done, want))
        if not data:  # the file ends early: the decoder says so
            return False
        ff = np.frombuffer(data, dtype=np.uint8) == 0xFF
        top = done + ff.size
        for i in range(
            int(np.searchsorted(raw_ends, done, side="right")),
            int(np.searchsorted(begins, top, side="left")),
        ):
            ff[max(int(begins[i]) - done, 0) : int(raw_ends[i]) - done] = False
        blocks = ff.size // _FF_BLOCK
        if blocks:
            full = ff[: blocks * _FF_BLOCK].view(np.uint8).reshape(blocks, _FF_BLOCK)
            if (full.sum(axis=1, dtype=np.uint16) >= _FF_BLOCK_LIMIT).any():
                return True
        if ff.size < want:  # a short read: the file ends here
            return False
        done = top
    return False


def _chunk_table_in(h, record: _LaszipRecord, f, size: int) -> Union[str, _ChunkTable]:
    """The chunk table of the LAZ points of the open file ``f`` (``size`` bytes)
    when it can be handed to the decoder; a str saying why not when it cannot."""
    import lazrs

    item = record.item_size
    points = int(h.point_count)
    start = int(h.offset_to_point_data)
    found = _find_chunk_table(f, start, size)
    if isinstance(found, str):
        return found
    at, count = found
    if count == 0:
        if points:
            return "the chunk table is empty"
        return _ChunkTable([], item, record.chunk_size)
    # One chunk holds at least its first point, uncompressed; the writer's last
    # chunk may hold none.
    if count > min((at - start - 8) // item + 1, _MAX_LAZ_CHUNKS):
        return f"the chunk table declares {count} chunks, more than the file can hold"
    # Nor more chunks than the header's points fill: a garbled offset deep in a big
    # file can read as a count up to _MAX_LAZ_CHUNKS, and lazrs builds a list of
    # that many entries (about 2.5 GB).
    if count > _most_chunks(points, record.chunk_size):
        return (
            f"the chunk table declares {count} chunks, more than the {points} "
            "points of the header need (the header's point count may be wrong)"
        )
    handle = _IOWatch(f)
    handle.seek(start)
    try:  # now bounded by the file and the header: lazrs reserves 16 bytes per chunk
        entries = lazrs.read_chunk_table(handle, record.vlr)
    except _quarantine.PASS_THROUGH:
        raise
    except BaseException as e:  # noqa: BLE001 - LazrsError, a Rust panic
        _raise_if_environment(e)  # memory is the machine's, not the file's
        if handle.error is not None:  # an I/O error of the Volume, not the file's
            _raise_if_environment(handle.error)
        return f"the chunk table cannot be decoded ({type(e).__name__}: {e})"
    if not entries:
        return "the chunk table cannot be found"
    problem = _chunk_sizes_problem(entries, points, item, size - (start + 8))
    if problem is None and "gps_time" in h.point_format.dimension_names:
        # Only a format with GPS time can overflow the decoder's stack on a run of
        # 0xFF (formats 0 and 2 decode it into garbage or an error).
        if _dense_ff_in(handle, start, _needed_chunks(entries, points), item):
            problem = _DENSE_FF_PROBLEM
    return (
        problem
        if problem is not None
        else _ChunkTable(entries, item, record.chunk_size)
    )


def _read_chunk_table(h, local: str, size: int) -> Union[str, _ChunkTable, None]:
    """The chunk table of a LAZ file when it can be handed to the decoder; a str
    saying why not when it cannot; None for a file that is not LAZ, or when lazrs
    (so no abort to guard against) is not the decoder.

    lazrs reserves memory for numbers it reads out of the file (the chunk count,
    each chunk's byte count and point count, the chunk size of the LASzip record)
    before it decodes a point, and the process aborts on a buffer the machine
    cannot give it: no exception reaches Python, so one garbled byte would take
    the worker down. The numbers are checked here first, against the size of the
    file, against the point count of the header and against what a chunk can be.
    Whatever else is wrong with the table (sizes that do not add up, a chunk that
    does not decode) is left to the decoder and the salvage that follows: it
    raises. A point format with GPS time also has the compressed points of the
    chunks the read needs scanned for dense runs of 0xFF, which overflow the
    decoder's stack (see :func:`_dense_ff_in`); other damage inside the points
    reaches the decoder. Cost: a second open of the file (retried like the
    first), three small reads, lazrs's own reads of the table through that
    handle (a few seeks and 8 KiB reads; more reads for a table of thousands of
    chunks), and for a format with GPS time one more sequential read of the
    compressed points."""
    record = _laszip_record(h)
    if record is None or isinstance(record, str):
        return record
    f = _listing._retry_transient(lambda: open(local, "rb"))
    with f:
        return _chunk_table_in(h, record, f, size)


def check_laz(source, name: str) -> None:
    """Raise a ValueError naming ``name`` when ``source`` (the path or the bytes
    of a LAS/LAZ file) is a LAZ file the decoder must not be given: a LASzip
    record that cannot be decoded, a chunk table that is implausible, or (a
    point format with GPS time) compressed points with a dense run of 0xFF bytes
    (see :func:`_read_chunk_table`). Callers that read a LAZ file with
    ``laspy.read`` call this first, since such a file can end the process
    instead of raising. Anything else passes: an uncompressed file, a file
    without lazrs as its decoder, and a LAZ file whose table and points are
    sound as far as these checks go (other damage can still reach the
    decoder)."""
    import laspy

    if isinstance(source, (bytes, bytearray, memoryview)):
        f, size = io.BytesIO(source), len(source)
    else:
        f = open(source, "rb")
        size = os.fstat(f.fileno()).st_size
    with f:
        h = laspy.LasHeader.read_from(f, read_evlrs=False)  # points only
        record = _laszip_record(h)
        if record is None:
            return
        found = (
            record if isinstance(record, str) else _chunk_table_in(h, record, f, size)
        )
    if isinstance(found, str):
        raise ValueError(f"{name}: not read, {found}")


def _header_bounds(h):
    """``(mins, maxs, tolerance)`` the points are checked against, or None when
    the header declares no usable bounds (all zero, or a max below a min).

    The margin is wide: on every axis, the largest of the x / y / z extents
    (plus one scale step). A header whose bounds were not updated after points
    were added or moved keeps those points, while garbage decoded from damaged
    LAZ data, which lands orders of magnitude away, is caught. One margin for
    every axis, so a flat axis (all z equal: zero extent) does not reject
    every point that is off it by a rounding step or a stale value."""
    mins = np.asarray(h.mins, dtype=np.float64)
    maxs = np.asarray(h.maxs, dtype=np.float64)
    if np.any(maxs < mins) or (not mins.any() and not maxs.any()):
        return None
    tol = np.abs(np.asarray(h.scales, dtype=np.float64)) + float(np.max(maxs - mins))
    return mins, maxs, tol


def _out_of_bounds(cols, keep, bounds, start, hook) -> np.ndarray:
    """Points (among ``keep``) outside the header's bounds widened by their
    margin (see :func:`_header_bounds`; garbled point records and garbage from
    damaged LAZ data decode to such values). Each run of consecutive ones is a bad record
    (``points=<first>-<last>``, by index in the file; a single point is
    ``point=<i>`` with its x/y/z). Returns their mask."""
    import json

    mins, maxs, tol = bounds
    xyz = np.stack([cols["x"], cols["y"], cols["z"]], axis=1)
    out = keep & np.any((xyz < mins - tol) | (xyz > maxs + tol), axis=1)
    idx = np.nonzero(out)[0]
    if idx.size == 0:
        return out
    for run in np.split(idx, np.nonzero(np.diff(idx) != 1)[0] + 1):
        a, b = start + int(run[0]), start + int(run[-1])
        if a == b:
            x, y, z = (float(v) for v in xyz[run[0]])
            hook(
                f"point={a}",
                _quarantine.POINT_OUT_OF_BOUNDS,
                "the point lies outside the bounds the file header declares",
                json.dumps({"x": x, "y": y, "z": z}),
            )
        else:
            hook(
                f"points={a}-{b}",
                _quarantine.POINT_OUT_OF_BOUNDS,
                f"{b - a + 1} points lie outside the bounds the file header declares",
            )
    return out


def _readable_points(
    h, local: str, size: int, hook, table: Optional[_ChunkTable]
) -> int:
    """Points of a LAS/LAZ file that can be read: its header count, checked
    against what the file holds first. An uncompressed file that ends early is
    read up to its last complete point record; a record length that does not
    fit the data reads none; points past the declared count are not read. A LAZ
    file is read up to the points its chunk table (``table``, from
    :func:`_read_chunk_table`) holds. Each is a bad record of the file."""
    total = int(h.point_count)
    offset = int(h.offset_to_point_data)
    if h.are_points_compressed:
        if table is None:  # lazrs is not the decoder: its own errors, then salvage
            return total
        # lazrs gives ``chunk_size`` points for every entry of a fixed-size table,
        # the partial last chunk's too (verified on lazrs 0.8.2: a 5,000-point file
        # has the entry (50000, ...), 120,000 points have (50000, ...) x 3), so
        # entries x chunk_size is an upper bound, never fewer than the points held.
        # A variable-size table counts the points of each chunk.
        coverage = (
            sum(int(points) for points, _ in table.entries)
            if table.chunk_size is None
            else len(table.entries) * table.chunk_size
        )
        if total > coverage:
            hook(
                None,
                _quarantine.RECORDS_NOT_READ,
                f"the header declares {total} points, more than the {coverage} its "
                f"chunk table holds; at most {coverage} were read",
            )
            return coverage
        return total
    reclen = int(h.point_format.size)
    # The point records end where the next section starts: the EVLRs (LAS 1.4)
    # or the waveform data packets stored in the file (LAS 1.3+).
    ends = [size]
    if h.number_of_evlrs:
        ends.append(int(h.start_of_first_evlr))
    wave = int(getattr(h, "start_of_waveform_data_packet_record", 0) or 0)
    if offset < wave < size:
        ends.append(wave)
    data = min(ends) - offset
    if reclen <= 0 or data == total * reclen:
        return total
    if data < total * reclen:
        extra = int(h.point_format.num_extra_bytes)
        standard = reclen - extra
        if extra and standard > 0 and data == total * standard:
            hook(
                None,
                _quarantine.UNREADABLE_FILE,
                f"the header's point record length ({reclen} bytes) does not match "
                f"the point data ({total} records of {standard} bytes); no point "
                "was read",
            )
            return 0
        complete = max(0, data // reclen)
        hook(
            None,
            _quarantine.RECORDS_NOT_READ,
            f"the file ends after {complete} of the {total} points its header "
            f"declares; {total - complete} points were not read",
        )
        return complete
    more = (data - total * reclen) // reclen
    if more > 0:
        hook(
            None,
            _quarantine.RECORDS_NOT_READ,
            f"the file holds {more} point records after the {total} its header "
            "declares; they were not read",
        )
    return total


def _serial_laz_backends(laspy):
    """The installed LAZ backends without laspy's parallel one, or None when no
    other is installed. Used only to tell a zero-filled tail from real zeros
    (see :meth:`_Cursor.read`): reading is left on laspy's default (parallel)
    decoder, which fails on garbled compressed data, where the single-threaded
    one can return wrong points without failing after a seek."""
    backends = tuple(
        b
        for b in laspy.LazBackend.detect_available()
        if b is not laspy.LazBackend.LazrsParallel
    )
    return backends or None


def _zero_tail(points) -> int:
    """How many records at the end of ``points`` have all their bytes zero."""
    n = len(points)
    if n == 0:
        return 0
    raw = np.ascontiguousarray(points.array).view(np.uint8).reshape(n, -1)
    if raw[-1].any():
        return 0
    nonzero = np.flatnonzero(raw.any(axis=1))
    return n - (int(nonzero[-1]) + 1 if nonzero.size else 0)


class _Cursor:
    """Where points are read from: the file's reader, or after a failure a
    fresh open of the file positioned where the next read starts (the previous
    one is closed). Environment errors (memory, I/O, no LAZ backend) raise."""

    def __init__(self, laspy, local, reader):
        self.laspy, self.local = laspy, local
        self.reader, self._owned, self.pos = reader, None, 0
        self.error = None  # why the last reopen failed

    def reopen(self, at: int) -> bool:
        self.close()
        try:
            r = self.laspy.open(self.local)
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException as e:  # noqa: BLE001 - the caller records the loss
            _raise_if_environment(e)
            self.error = e
            return False
        self._owned = r
        try:
            r.seek(at)
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException as e:  # noqa: BLE001 - the caller records the loss
            self.close()
            _raise_if_environment(e)
            self.error = e
            return False
        self.reader, self.pos = r, at
        return True

    def read(self, at: int, n: int):
        """``n`` points from point ``at``; raises when they cannot be read.

        After a seek, laspy's parallel LAZ decoder fills a read that runs past
        the compressed data with zero bytes instead of failing (a file whose
        header declares more points than it holds). So on a reopened reader of
        compressed points, a read whose last records are all zero is checked:
        that run is re-read with a single-threaded decoder, which decodes real
        zero records and fails past the data. Only then is it data; otherwise
        the read raises and the run is a loss like any undecodable stretch. A
        zero record followed by any other record is not checked, nor is a read
        of the file's own reader (no seek: it fails past the data)."""
        if (self.reader is None or self.pos != at) and not self.reopen(at):
            raise self.error  # the decoder's own error: the record's class
        points = self.reader.read_points(n)
        if len(points) != n:
            raise ValueError(f"read {len(points)} of {n} points")
        if self._owned is not None and self.reader.header.are_points_compressed:
            zeros = _zero_tail(points)
            if zeros:
                self._check_zero_run(at + n - zeros, zeros)
        self.pos = at + n
        return points

    def _check_zero_run(self, first: int, count: int) -> None:
        """Raise unless points ``[first, first + count)``, all-zero records from
        the parallel decoder, decode as all-zero records single-threaded."""
        backends = _serial_laz_backends(self.laspy)
        cause = None
        if backends is not None:
            try:
                with self.laspy.open(self.local, laz_backend=backends) as r:
                    r.seek(first)
                    again = r.read_points(count)
                if len(again) == count and _zero_tail(again) == count:
                    return
            except _quarantine.PASS_THROUGH:
                raise
            except BaseException as e:  # noqa: BLE001 - past the data: a loss
                _raise_if_environment(e)
                cause = e
        raise ValueError(
            f"points {first} to {first + count - 1} decode as zero-filled "
            "records past the end of the compressed point data"
        ) from cause

    def close(self) -> None:
        self.reader = None
        if self._owned is not None:
            try:
                self._owned.close()
            except Exception:  # noqa: BLE001 - closing a broken reader
                pass
            self._owned = None


def _lost(hook, first, end, exc) -> None:
    hook(
        f"points={first}-{end - 1}",
        _quarantine.error_class(exc),
        f"points {first} to {end - 1} could not be decoded: "
        + _quarantine.describe(exc),
    )


class _Losses:
    """Point ranges that could not be read; contiguous ones are one record."""

    def __init__(self, hook):
        self.hook, self.first, self.end, self.exc = hook, None, None, None

    def add(self, first: int, end: int, exc) -> None:
        if self.end == first:
            self.end = end
            return
        self.flush()
        self.first, self.end, self.exc = first, end, exc

    def unread_before(self, at: int) -> int:
        """Points not read in a row up to ``at`` (0 when ``at`` was read)."""
        return self.end - self.first if self.first is not None and self.end == at else 0

    def flush(self) -> None:
        if self.first is not None:
            _lost(self.hook, self.first, self.end, self.exc)
        self.first = self.end = None


def _raise_if_environment(exc: BaseException) -> None:
    """Fail the read on an error of the environment, not the file (see
    ``_quarantine.is_environment_error``), including laspy's missing LAZ
    backend (no lazrs / laszip installed)."""
    if "LazBackend" in str(exc):
        raise _quarantine.EnvironmentFailure(
            f"cannot decompress LAZ: no LAZ backend (lazrs or laszip) is "
            f"installed ({exc})"
        ) from exc
    _quarantine.reraise_environment(exc)


def _try(cur, at: int, n: int):
    """``(points, None)``, or ``(None, error)`` when they cannot be read."""
    try:
        return cur.read(at, n), None
    except _quarantine.PASS_THROUGH:
        raise
    except BaseException as e:  # noqa: BLE001 - the caller narrows it down
        _raise_if_environment(e)
        cur.close()
        return None, e


def _next_readable(cur, at: int, end: int) -> int:
    """The first point after ``at`` (on the :data:`_SALVAGE_POINTS` grid from
    ``at``) where reading works again, or ``end``: exponential search, then
    bisection, so a stretch of undecodable points costs O(log) reads (a LAZ
    chunk decodes again from its start)."""
    step = _SALVAGE_POINTS

    def works(k: int) -> bool:
        p = at + k * step
        return _try(cur, p, min(step, end - p))[0] is not None

    lo, hi = 0, None
    k = 1
    while at + k * step < end:
        if works(k):
            hi = k
            break
        lo, k = k, k * 2
    if hi is None:
        return end
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if works(mid):
            hi = mid
        else:
            lo = mid
    return at + hi * step


def _narrow(cur, start: int, end: int, emit, lost):
    """Re-read the failed chunk ``[start, end)`` in pieces: the points that
    decode are kept; a stretch that does not is skipped to where reading works
    again and is a loss; the rest of the chunk is then tried in one read.
    Returns True when any point was read."""
    s, kept = start, False
    while s < end:
        m = min(_SALVAGE_POINTS, end - s)
        points, err = _try(cur, s, m)
        if points is None:
            nxt = _next_readable(cur, s, end)
            lost.add(s, nxt, err)
            s = nxt
            if s >= end:
                break
            points, _ = _try(cur, s, end - s)  # the rest of the chunk at once
            if points is None:
                continue
            m = end - s
        kept = True
        batch = emit(points, s)
        s += m
        if batch is not None:
            yield batch
    return kept


def _checked_chunks(laspy, local, reader, limit, chunk_size, emit, hook):
    """Read points ``[0, limit)`` chunk by chunk. A chunk that fails (a LAZ
    decode error, a Rust panic included) is re-read in pieces of
    :data:`_SALVAGE_POINTS` to keep every point that decodes (see
    :func:`_narrow`); points that do not are bad records
    (``points=<first>-<last>``, contiguous ones merged). After
    ``max(_GIVE_UP_CHUNKS * chunk_size, _GIVE_UP_POINTS)`` points in a row that
    could not be read, the rest of the file is one record. Environment errors
    (memory, a missing LAZ backend) fail the read."""
    cur = _Cursor(laspy, local, reader)
    lost = _Losses(hook)
    give_up = max(_GIVE_UP_CHUNKS * chunk_size, _GIVE_UP_POINTS)
    start = 0
    try:
        while start < limit:
            end = min(start + chunk_size, limit)
            points, err = _try(cur, start, end - start)
            if points is not None:
                batch = emit(points, start)
                start = end
                if batch is not None:
                    yield batch
                continue
            yield from _narrow(cur, start, end, emit, lost)
            start = end
            if lost.unread_before(start) >= give_up and start < limit:
                lost.add(start, limit, err)
                break
        lost.flush()
    finally:
        cur.close()


class LidarGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "lidar_gbx"

    def schema(self) -> StructType:
        mode = (self.options.get("mode") or "points").lower()
        if mode == "metadata":
            return LIDAR_META_SCHEMA
        return _points_schema(_select_point_dimensions(self.options.get("dimensions")))

    def reader(self, schema: StructType) -> DataSourceReader:
        return LidarGbxReader(self.options)

    def writer(self, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._write_lidar import LidarGbxWriter

        if not self.options.get("path"):
            raise ValueError("lidar_gbx writer requires an output path (.save(path)).")
        return LidarGbxWriter(self.options, schema, overwrite)
