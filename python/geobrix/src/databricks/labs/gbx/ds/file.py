"""file_gbx — path-listing DataSource. Emits file REFERENCES, never content
(the deliberate contrast to binaryFile, which drags bytes into memory).
Raster-agnostic: a pure lister; consumers decide what to do with paths."""
from __future__ import annotations

import os
from typing import Dict, Iterator, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

from databricks.labs.gbx.ds import _listing

FILE_SCHEMA = StructType([
    StructField("path", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("extension", StringType(), nullable=True),
    StructField("size", LongType(), nullable=False),
    StructField("modificationTime", TimestampType(), nullable=True),
])


class _PathPartition(InputPartition):
    def __init__(self, file_path: str):
        self.file_path = file_path


class FileGbxReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("file_gbx requires a 'path' (e.g. .load(path)).")
        self.filter_regex = options.get("filterRegex", ".*")

    def partitions(self) -> Sequence[InputPartition]:
        files = _listing.list_files(self.path, self.filter_regex)
        return [_PathPartition(f) for f in files]

    def read(self, partition: "_PathPartition") -> Iterator[Tuple]:
        import datetime as _dt

        local = _listing.to_local_path(partition.file_path)
        st = os.stat(local)
        name = os.path.basename(local)
        stem, ext = os.path.splitext(name)
        extension = ext[1:].lower() if ext else None
        source = _listing.to_spark_uri(partition.file_path)
        yield (
            source,
            name,
            extension,
            int(st.st_size),
            _dt.datetime.fromtimestamp(st.st_mtime),
        )


class FileGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "file_gbx"

    def schema(self) -> StructType:
        return FILE_SCHEMA

    def reader(self, schema: StructType) -> DataSourceReader:
        return FileGbxReader(self.options)
