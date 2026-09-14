"""Light Spark Python DataSource for LAS/LAZ point clouds (lidar_gbx).

mode="metadata" (this task): one row per file, header-only (cheap; never loads points).
mode="points"   (Task 3):    one row per point, chunk-iterated.

Serverless-safe: session-free partitions()/read(); one InputPartition per file.
Uses no forbidden Spark internal APIs — safe on Spark Connect / Serverless.
"""

import os
from typing import Dict, Iterator, Sequence, Tuple

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

_LIDAR_EXTS = (".las", ".laz")


class _LidarFilePartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class LidarGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("lidar_gbx requires a 'path' (e.g. .load(path)).")
        self.mode = (options.get("mode") or "points").lower()

    def partitions(self) -> Sequence[InputPartition]:
        files = list_local_files(self.path, extensions=_LIDAR_EXTS)
        return [_LidarFilePartition(f) for f in files]

    def read(self, partition: "_LidarFilePartition") -> Iterator[Tuple]:
        if self.mode == "metadata":
            yield from self._read_metadata(partition.file_path)
        else:
            raise ValueError(
                "lidar_gbx points mode not yet available; use mode='metadata'."
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
            rh = [int(x) for x in rh_raw] if rh_raw is not None and len(rh_raw) > 0 else []
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
        # points schema wired in Task 3
        raise ValueError(
            "lidar_gbx points mode not yet available; use mode='metadata'."
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return LidarGbxReader(self.options)
