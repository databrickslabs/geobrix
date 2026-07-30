"""gtiff_gbx — named GeoTIFF reader. Light analogue of Scala GTiff_DataSource:
extends the catch-all and presets driver="GTiff" (the dsExtraMap mirror).
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql.datasource import DataSourceReader, DataSourceWriter
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds.raster import RasterGbxDataSource, RasterGbxReader
from databricks.labs.gbx.ds.writer import RasterGbxWriter


class GTiffGbxReader(RasterGbxReader):
    def __init__(self, options: Dict[str, str]):
        super().__init__(options)
        self.driver = "GTiff"


class GTiffGbxDataSource(RasterGbxDataSource):
    @classmethod
    def name(cls) -> str:
        return "gtiff_gbx"

    def reader(self, schema: StructType) -> DataSourceReader:
        return GTiffGbxReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("gtiff_gbx writer requires an output path (.save(path)).")
        return RasterGbxWriter(
            path,
            schema,
            overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver="GTiff",
        )
