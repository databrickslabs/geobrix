"""Light Spark Python DataSource for LAS/LAZ point clouds (lidar_gbx).

mode="metadata": one row per file, header-only (cheap; never loads points).
mode="points":   one row per point, chunk-iterated with class/return/decimate filters.

Serverless-safe: session-free partitions()/read(); one InputPartition per file.
Uses no forbidden Spark internal APIs — safe on Spark Connect / Serverless.
"""

import os
import warnings
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Sequence

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
        if hook is not None:
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
            bounds = _header_bounds(reader.header)
            limit = _readable_points(reader.header, local, st.st_size, hook)
            yield from _checked_chunks(
                laspy, local, reader, limit, self.chunk_size, _emit, hook
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


def _header_bounds(h):
    """``(mins, maxs, tolerance)`` the points are checked against, or None when
    the header declares no usable bounds (all zero, or a max below a min)."""
    mins = np.asarray(h.mins, dtype=np.float64)
    maxs = np.asarray(h.maxs, dtype=np.float64)
    if np.any(maxs < mins) or (not mins.any() and not maxs.any()):
        return None
    tol = np.abs(np.asarray(h.scales, dtype=np.float64)) + 1e-9 * np.maximum(
        1.0, np.abs(maxs - mins)
    )
    return mins, maxs, tol


def _out_of_bounds(cols, keep, bounds, start, hook) -> np.ndarray:
    """Points (among ``keep``) outside the header's bounds (garbled point
    records decode to such values). Each run of consecutive ones is a bad record
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


def _readable_points(h, local: str, size: int, hook) -> int:
    """Points of an uncompressed LAS file that can be read: its header count,
    checked against the file size first. A file that ends early is read up to
    its last complete point record; a record length that does not fit the data
    reads none; points past the declared count are not read. Each is a bad
    record of the file. LAZ (compressed) files are not checked here."""
    total = int(h.point_count)
    if h.are_points_compressed:
        return total
    reclen = int(h.point_format.size)
    end = int(h.start_of_first_evlr) if h.number_of_evlrs else size
    data = end - int(h.offset_to_point_data)
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


class _Cursor:
    """Where points are read from: the file's reader, or after a failure a
    fresh open of the file positioned past it (the previous one is closed)."""

    def __init__(self, laspy, local, reader):
        self.laspy, self.local = laspy, local
        self.reader, self._owned = reader, None

    def reopen(self, at: int) -> bool:
        self.close()
        try:
            r = self.laspy.open(self.local)
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException:  # noqa: BLE001 - the caller records the loss
            return False
        self._owned = r
        try:
            r.seek(at)
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException:  # noqa: BLE001 - the caller records the loss
            self.close()
            return False
        self.reader = r
        return True

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


def _checked_chunks(laspy, local, reader, limit, chunk_size, emit, hook):
    """Read points ``[0, limit)`` chunk by chunk. A chunk that fails (a LAZ
    decode error, a Rust panic included) is read again in small pieces from a
    fresh open, to keep every point that decodes; the pieces that fail are bad
    records (``points=<first>-<last>``) and the read goes on after them."""
    cur = _Cursor(laspy, local, reader)
    try:
        start = 0
        while start < limit:
            n = min(chunk_size, limit - start)
            try:
                if cur.reader is None:
                    raise RuntimeError(f"cannot reopen {local} at point {start}")
                points = cur.reader.read_points(n)
            except _quarantine.PASS_THROUGH:
                raise
            except BaseException as e:  # noqa: BLE001 - salvaged below
                yield from _salvage(cur, start, n, emit, hook, e)
                start += n
                if start < limit and not cur.reopen(start):
                    _lost(hook, start, limit, e)
                    return
                continue
            if len(points) == 0:
                return
            batch = emit(points, start)
            start += len(points)
            if batch is not None:
                yield batch
    finally:
        cur.close()


def _salvage(cur, start, n, emit, hook, exc):
    """Re-read ``[start, start + n)`` in pieces of :data:`_SALVAGE_POINTS`, each
    failing piece from a fresh open; consecutive failed pieces are one record."""
    end, s = start + n, start
    bad_from, first_exc = None, exc
    fresh = False
    while s < end:
        m = min(_SALVAGE_POINTS, end - s)
        try:
            if not fresh and not cur.reopen(s):
                raise first_exc
            fresh = True
            points = cur.reader.read_points(m)
            if len(points) != m:
                raise ValueError(f"read {len(points)} of {m} points")
        except _quarantine.PASS_THROUGH:
            raise
        except BaseException as e:  # noqa: BLE001 - this piece is lost
            if bad_from is None:
                bad_from, first_exc = s, e
            fresh = False
            s += m
            continue
        if bad_from is not None:
            _lost(hook, bad_from, s, first_exc)
            bad_from = None
        batch = emit(points, s)
        if batch is not None:
            yield batch
        s += m
    if bad_from is not None:
        _lost(hook, bad_from, end, first_exc)


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
