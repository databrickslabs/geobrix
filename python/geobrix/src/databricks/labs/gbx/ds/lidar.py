"""Light Spark Python DataSource for LAS/LAZ point clouds (lidar_gbx).

mode="metadata": one row per file, header-only (cheap; never loads points).
mode="points":   one row per point, chunk-iterated with class/return/decimate filters.

Serverless-safe: session-free partitions()/read(); one InputPartition per file.
Uses no forbidden Spark internal APIs — safe on Spark Connect / Serverless.
"""

import os
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

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


class _LidarFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class LidarGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("lidar_gbx requires a 'path' (e.g. .load(path)).")
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

    def partitions(self) -> Sequence[InputPartition]:
        files = list_local_files(self.path, extensions=_LIDAR_EXTS)
        return [_LidarFilePartition(f) for f in files]

    def read(self, partition: "_LidarFilePartition") -> Iterator[Tuple]:
        if self.mode == "metadata":
            yield from self._read_metadata(partition.file_path)
        else:
            yield from self._read_points(partition.file_path)

    def _read_points(self, file_path: str) -> Iterator[Tuple]:
        import laspy

        local = _listing.to_local_path(file_path)
        with laspy.open(local) as reader:
            emitted = 0
            for chunk in reader.chunk_iterator(self.chunk_size):
                xs = np.asarray(chunk.x)
                ys = np.asarray(chunk.y)
                zs = np.asarray(chunk.z)
                cls = np.asarray(chunk.classification)
                rn = np.asarray(chunk.return_number)
                nr = np.asarray(chunk.number_of_returns)
                inten = np.asarray(chunk.intensity)
                if "gps_time" in chunk.point_format.dimension_names:
                    gt = np.asarray(chunk.gps_time)
                else:
                    gt = np.full(len(xs), np.nan)
                mask = np.ones(len(xs), dtype=bool)
                if self.class_filter is not None:
                    mask &= np.isin(cls, self.class_filter)
                if self.return_filter is not None:
                    mask &= rn == self.return_filter
                idx = np.nonzero(mask)[0]
                for i in idx:
                    emitted += 1
                    if self.decimate > 1 and (emitted % self.decimate) != 0:
                        continue
                    yield (
                        float(xs[i]),
                        float(ys[i]),
                        float(zs[i]),
                        int(inten[i]),
                        int(rn[i]),
                        int(nr[i]),
                        int(cls[i]),
                        float(gt[i]),
                    )

    def _read_metadata(self, file_path: str) -> Iterator[Tuple]:
        import datetime as _dt

        import laspy

        local = _listing.to_local_path(file_path)
        # UC Volume FUSE can transiently raise FileNotFoundError (eventual-
        # consistency lag); retry up to 10x before re-raising.
        st = _listing._retry_transient(lambda: os.stat(local))
        source = _listing.to_spark_uri(file_path)
        name = os.path.basename(local)
        with laspy.open(local) as reader:
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
            yield (
                source,
                name,
                int(h.point_count),
                float(mins[0]),
                float(maxs[0]),
                float(mins[1]),
                float(maxs[1]),
                float(mins[2]),
                float(maxs[2]),
                crs,
                int(h.point_format.id),
                dims,
                [float(s) for s in h.scales],
                [float(o) for o in h.offsets],
                rh,
                density,
                str(h.version),
                int(st.st_size),
                _dt.datetime.fromtimestamp(st.st_mtime),
            )


class LidarGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "lidar_gbx"

    def schema(self) -> StructType:
        mode = (self.options.get("mode") or "points").lower()
        if mode == "metadata":
            return LIDAR_META_SCHEMA
        return LIDAR_POINTS_SCHEMA

    def reader(self, schema: StructType) -> DataSourceReader:
        return LidarGbxReader(self.options)
