"""cog_gbx — the optimized COG lane. Reader is COG-aware (efficient windowed /
overview reads; bbox AOI is the blessed clip path). Writer prepares master COGs.
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql.datasource import DataSourceReader, DataSourceWriter
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds.raster import RasterGbxDataSource, RasterGbxReader


class CogGbxReader(RasterGbxReader):
    def __init__(self, options: Dict[str, str]):
        super().__init__(options)
        self.driver = "GTiff"  # COG opens as GTiff


class CogGbxDataSource(RasterGbxDataSource):
    @classmethod
    def name(cls) -> str:
        return "cog_gbx"

    def reader(self, schema: StructType) -> DataSourceReader:
        return CogGbxReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        from databricks.labs.gbx.ds.cog_writer import CogGbxWriter

        path = self.options.get("path")
        if not path:
            raise ValueError("cog_gbx writer requires an output path (.save(path)).")
        return CogGbxWriter(
            path,
            schema,
            overwrite,
            cog_blocksize=int(self.options.get("cogBlockSize", "512")),
            cog_overview_resampling=self.options.get(
                "cogOverviewResampling", "AVERAGE"
            ),
            cog_compression=self.options.get("cogCompression", "DEFLATE"),
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            cog_subdataset=self.options.get("cogSubdataset"),
            cog_skip_if_exists=self.options.get("cogSkipIfExists", "true").lower()
            == "true",
            driver_mode=self.options.get("driverMode", "false").lower()
            == "true",
            driver_mode_verbose=self.options.get("driverModeVerbose", "true")
            .lower()
            == "true",
        )
