"""netcdf_gbx — lightweight NetCDF reader.

One DataSource, two modes (the `mode` option, default "raster"):
  * raster — CF regular/projected grids -> the shared (source, tile) GeoTIFF struct.
  * vector — DSG points, or any 2-D field (incl. curvilinear swath) coerced to
    per-cell points -> the light vector schema (attrs + geom_0 WKB + srid cols).

Class 4 (raw sensor geometry + GLT) is rejected in both modes.
Serverless-safe: registers a DataSource and builds Column output only (no runtime
Spark-config mutation or JVM-bridge access).
"""

from __future__ import annotations

from typing import Dict, Iterator, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _encode, _listing, _netcdf
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition, reader_schema


class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode each CF grid variable to a GeoTIFF tile (one row per variable)."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex/bbox/bboxCrs
        self.options = dict(options)
        self.group = options.get("group")

    def partitions(self):
        # NetCDF raster reader emits one row PER VARIABLE per file, not per tile
        # window. The tile-window planning in RasterGbxReader.partitions() does not
        # apply here — return one legacy _FilePartition per file so read() receives
        # a file-scoped partition and can iterate over variables itself.
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        from rasterio.io import MemoryFile

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = _netcdf.select_variables(ds, self.options, "raster")
            for var in variables:
                transform, crs = _netcdf.grid_transform_crs(ds, var)
                arr = _netcdf.array_2d(ds, var)
                nodata = _netcdf.nodata_of(ds, var)
                source = f'NETCDF:"{partition.file_path}":{var}'
                h, w = arr.shape[-2], arr.shape[-1]
                profile = dict(
                    driver="GTiff",
                    width=w,
                    height=h,
                    count=1,
                    dtype=str(arr.dtype),
                    crs=crs,
                    transform=transform,
                )
                if nodata is not None:
                    profile["nodata"] = nodata
                with MemoryFile() as mf:
                    with mf.open(**profile) as out:
                        out.write(arr.astype(profile["dtype"]), 1)
                    with mf.open() as rds:
                        cellid, raster_bytes, meta = _encode.encode_tile(
                            rds,
                            window=(0, 0, w, h),
                            source_path=partition.file_path,
                            all_parents="",
                            tile_format="gtiff",
                        )
                yield (source, (cellid, raster_bytes, meta))


class NetcdfGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "netcdf_gbx"

    def _mode(self) -> str:
        return self.options.get("mode", "raster").lower()

    def schema(self) -> StructType:
        mode = self._mode()
        if mode == "raster":
            return reader_schema()
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

            return NetcdfVectorReader(self.options).schema()
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        mode = self._mode()
        if mode == "raster":
            return NetcdfRasterReader(self.options)
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

            return NetcdfVectorReader(self.options)
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )

    def writer(self, schema: StructType, overwrite: bool):
        mode = self._mode()
        if mode == "raster":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfRasterGbxWriter

            if not self.options.get("path"):
                raise ValueError(
                    "netcdf_gbx writer requires an output path (.save(path))."
                )
            return NetcdfRasterGbxWriter(self.options, schema, overwrite)
        if mode == "vector":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfVectorGbxWriter

            if not self.options.get("path"):
                raise ValueError(
                    "netcdf_gbx writer requires an output path (.save(path))."
                )
            return NetcdfVectorGbxWriter(self.options, schema, overwrite)
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )
