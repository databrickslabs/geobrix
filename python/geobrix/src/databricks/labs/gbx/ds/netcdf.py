"""netcdf_gbx — lightweight NetCDF reader.

One DataSource, two modes (the `mode` option, default "raster"):
  * raster — CF regular/projected grids -> the shared (source, tile) GeoTIFF struct.
  * vector — DSG points, or any 2-D field (incl. curvilinear swath) coerced to
    per-cell points -> the light vector schema (attrs + geom_0 WKB + srid cols).

Class 4 (raw sensor geometry + GLT) is rejected in both modes.
Serverless-safe: no spark.conf/_jvm/.rdd/cache/persist.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _encode, _netcdf
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition, reader_schema


def _requested_variables(options: Dict[str, str]) -> List[str]:
    raw = options.get("variables") or options.get("variable")
    if not raw:
        raise ValueError(
            "netcdf_gbx requires a 'variable' (or 'variables') option naming the "
            "NetCDF variable(s) to read."
        )
    return [v.strip() for v in str(raw).split(",") if v.strip()]


class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode a CF grid variable to a GeoTIFF tile."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex/bbox/bboxCrs
        self.variables = _requested_variables(options)
        self.group = options.get("group")

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.ds import _listing

        source = _listing.to_spark_uri(partition.file_path)
        var = self.variables[0]  # raster mode reads a single variable per tile
        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            kind = _netcdf.classify(ds, var)
            if kind == _netcdf.CURVILINEAR:
                raise ValueError(
                    f"netcdf_gbx: variable '{var}' in {partition.file_path} is "
                    f"curvilinear/swath (2-D lat/lon); read it with "
                    f"option('mode','vector') to get per-cell points."
                )
            if kind != _netcdf.GRID:
                raise ValueError(
                    f"netcdf_gbx: variable '{var}' is not a regular grid "
                    f"({kind}); raster mode supports CF regular/projected grids only."
                )
            transform, crs = _netcdf.grid_transform_crs(ds, var)
            arr = _netcdf.array_2d(ds, var)
            nodata = _netcdf.nodata_of(ds, var)

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
        # Build an in-memory rasterio dataset, then reuse the shared encode_tile so
        # the 11-key metadata + GTiff re-encode stay DRY with the other readers.
        with MemoryFile() as mf:
            with mf.open(**profile) as out:
                out.write(arr.astype(profile["dtype"]), 1)
            with mf.open() as rds:
                cellid, raster_bytes, meta = _encode.encode_tile(
                    rds,
                    window=(0, 0, w, h),
                    source_path=partition.file_path,
                    all_parents="",
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
