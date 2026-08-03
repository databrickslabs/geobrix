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
        # Resolve the compression surface.
        # ``compress`` is the canonical option; ``cogCompression`` is a
        # deprecated alias retained for back-compat. When both are supplied,
        # ``compress`` wins.
        _compress = self.options.get("compress")
        _cog_compression_alias = self.options.get("cogCompression")
        if _compress is None and _cog_compression_alias is not None:
            _compress = _cog_compression_alias.lower()
        if _compress is None:
            _compress = "auto"
        _compress_level_raw = self.options.get("compressLevel")
        _compress_level = (
            int(_compress_level_raw) if _compress_level_raw is not None else None
        )
        _predictor_raw = self.options.get("predictor")
        _predictor = int(_predictor_raw) if _predictor_raw is not None else None
        return RasterGbxWriter(
            path,
            schema,
            overwrite,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            force_driver="GTiff",
            compress=_compress,
            compress_level=_compress_level,
            predictor=_predictor,
        )
