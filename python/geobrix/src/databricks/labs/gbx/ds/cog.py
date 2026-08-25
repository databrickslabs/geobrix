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
        from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, parse_mosaic_options

        path = self.options.get("path")
        if not path:
            raise ValueError("cog_gbx writer requires an output path (.save(path)).")
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
        # Parse and validate mosaic-mode options up front (raises ValueError
        # before any I/O if the combination is unsupported).
        _mosaic_opts = parse_mosaic_options(self.options)
        return CogGbxWriter(
            path,
            schema,
            overwrite,
            cog_blocksize=int(self.options.get("cogBlockSize", "512")),
            cog_overview_resampling=self.options.get(
                "cogOverviewResampling", "AVERAGE"
            ),
            compress=_compress,
            compress_level=_compress_level,
            predictor=_predictor,
            name_col=self.options.get("nameCol"),
            ext=self.options.get("ext", "tif"),
            cog_subdataset=self.options.get("cogSubdataset"),
            cog_skip_if_exists=self.options.get("cogSkipIfExists", "true").lower()
            == "true",
            driver_mode=self.options.get("driverMode", "false").lower() == "true",
            driver_mode_verbose=self.options.get("driverModeVerbose", "true").lower()
            == "true",
            cog_bigtiff=self.options.get("cogBigTiff", "YES"),
            mosaic_opts=_mosaic_opts,
        )
