"""GeoBrix connector: the PySpark DataSource side.

Hybrid: the framework's ``LakeflowSource`` serves metadata and its virtual
tables; real tables are read by GeoBrix Arrow-native readers (whole Arrow
batches per file) instead of the framework's row-at-a-time dict conversion.
"""

import json

from pyspark.sql.datasource import DataSourceReader, DataSourceStreamReader, InputPartition
from pyspark.sql.streaming.datasource import ReadAllAvailable, SupportsTriggerAvailableNow
from pyspark.sql.types import StructType

from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    IS_DELETE_FLOW,
    TABLE_NAME,
    VIRTUAL_TABLES,
    LakeflowSource,
)

from .lakeflow_connect import GeoBrixLakeflowConnect


class _ArrowFileReads:
    """Shared by the batch and stream readers: one partition = one file."""

    def _init(self, options: dict, schema: StructType, connect: GeoBrixLakeflowConnect):
        self.table_name = options[TABLE_NAME]
        self.table_options = {k: v for k, v in options.items() if k != IS_DELETE_FLOW}
        self.schema = schema
        self.connect = connect

    @staticmethod
    def _to_partitions(descs):
        return [InputPartition(json.dumps(d)) for d in descs]

    def read(self, partition: InputPartition):
        desc = json.loads(partition.value)
        yield from self.connect.iter_member_batches(
            desc["path"], int(desc["mtime_ns"]), self.table_options, self.schema
        )


class GeoBrixBatchReader(_ArrowFileReads, DataSourceReader):
    def __init__(self, options: dict, schema: StructType, connect: GeoBrixLakeflowConnect):
        self._init(options, schema, connect)

    def partitions(self):
        return self._to_partitions(
            self.connect.get_partitions(self.table_name, self.table_options)
        )


class GeoBrixStreamReader(
    _ArrowFileReads, DataSourceStreamReader, SupportsTriggerAvailableNow
):
    """File-level incremental reads: each micro-batch covers files in (start, end]."""

    def __init__(self, options: dict, schema: StructType, connect: GeoBrixLakeflowConnect):
        self._init(options, schema, connect)

    def initialOffset(self):
        return {}

    def getDefaultReadLimit(self):
        return ReadAllAvailable()

    def latestOffset(self, start: dict, limit) -> dict:
        if not isinstance(limit, ReadAllAvailable):
            raise ValueError(
                f"geobrix only supports ReadAllAvailable; got {type(limit).__name__}."
            )
        return self.connect.latest_offset(self.table_name, self.table_options, start)

    def partitions(self, start: dict, end: dict):
        return self._to_partitions(
            self.connect.get_partitions(self.table_name, self.table_options, start, end)
        )

    def prepareForTriggerAvailableNow(self) -> None:
        pass


class GeoBrixDataSource(LakeflowSource):
    """``lakeflow_connect`` data source for geospatial files on Unity Catalog Volumes."""

    _lakeflow_connect_cls = GeoBrixLakeflowConnect

    def reader(self, schema: StructType):
        if self.options.get(TABLE_NAME) in VIRTUAL_TABLES:
            return super().reader(schema)
        return GeoBrixBatchReader(self.options, schema, self.lakeflow_connect)

    def streamReader(self, schema: StructType):
        if self.options.get(TABLE_NAME) in VIRTUAL_TABLES:
            return super().streamReader(schema)
        return GeoBrixStreamReader(self.options, schema, self.lakeflow_connect)
