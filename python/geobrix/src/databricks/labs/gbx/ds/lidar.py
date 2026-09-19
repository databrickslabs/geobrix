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
    TimestampType,
)

from databricks.labs.gbx.ds import _listing
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
        StructField("modificationTime", TimestampType(), True),
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
        # downloader's "skip nodes that never came back").
        try:
            st = _listing._retry_transient(lambda: os.stat(local))
            if st.st_size == 0:
                warnings.warn(
                    f"lidar_gbx: skipping empty file {local!r} (0 bytes)",
                    stacklevel=2,
                )
                return
            opener = _listing._retry_transient(lambda: laspy.open(local))
        except (
            Exception
        ) as exc:  # noqa: BLE001 — corrupt/truncated or persistently missing
            warnings.warn(
                f"lidar_gbx: skipping unreadable file {local!r}: {exc}",
                stacklevel=2,
            )
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

        def _read_dim(chunk, name: str, n: int) -> np.ndarray:
            if name in ("x", "y", "z"):
                return np.asarray(getattr(chunk, name), dtype=np.float64)
            if name == "gps_time":
                if "gps_time" in chunk.point_format.dimension_names:
                    return np.asarray(chunk.gps_time, dtype=np.float64)
                return np.full(n, np.nan, dtype=np.float64)
            return np.asarray(getattr(chunk, name))

        with opener as reader:
            seen = 0  # filter-passing points seen in this file (decimate parity)
            for chunk in reader.chunk_iterator(self.chunk_size):
                n = len(chunk)
                cols = {name: _read_dim(chunk, name, n) for name in need}

                keep = np.ones(n, dtype=bool)
                if self.class_filter is not None:
                    keep &= np.isin(cols["classification"], self.class_filter)
                if self.return_filter is not None:
                    keep &= cols["return_number"] == self.return_filter

                if self.decimate > 1:
                    # Keep every Nth filter-passing point via a 1-based counter that
                    # runs continuously across chunks (identical selection to the
                    # row-wise reader this replaces).
                    masked_idx = np.nonzero(keep)[0]
                    emit_no = seen + np.arange(1, masked_idx.size + 1)
                    keep[masked_idx[emit_no % self.decimate != 0]] = False
                    seen += masked_idx.size

                if not keep.any():
                    continue
                arrays = []
                for f in LIDAR_POINTS_SCHEMA.fields:
                    if f.name not in out_set:
                        continue
                    at = to_arrow_type(f.dataType)
                    a = cols[f.name][keep]
                    if pa.types.is_integer(at):
                        a = a.astype(np.int32)
                    arrays.append(pa.array(a, type=at))
                yield pa.record_batch(arrays, schema=arrow_schema)

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
        try:
            st = _listing._retry_transient(lambda: os.stat(local))
            if st.st_size == 0:
                warnings.warn(
                    f"lidar_gbx: skipping empty file {local!r} (0 bytes)", stacklevel=2
                )
                return
            opener = _listing._retry_transient(lambda: laspy.open(local))
        except (
            Exception
        ) as exc:  # noqa: BLE001 — corrupt/truncated or persistently missing
            warnings.warn(
                f"lidar_gbx: skipping unreadable file {local!r}: {exc}", stacklevel=2
            )
            return
        with opener as reader:
            h = reader.header
            mins = h.mins
            maxs = h.maxs
            try:
                crs_obj = h.parse_crs()
                crs = crs_obj.to_wkt() if crs_obj is not None else None
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
