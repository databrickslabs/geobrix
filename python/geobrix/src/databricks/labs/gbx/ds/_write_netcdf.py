"""netcdf_gbx writer (DataSource V2). Inverse of the netcdf_gbx reader.

Raster mode: (source, tile) grid tiles -> CF grid NetCDF, one .nc per row.
Serverless-safe: write(iterator) + netCDF4 encode to worker-local temp then
shutil.copyfile to the FUSE path. No spark.conf/_jvm/.rdd.
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


@dataclass
class NetcdfCommitMessage(WriterCommitMessage):
    paths: List[str]


def _var_from_source(source: Optional[str]) -> Optional[str]:
    """Parse variable name from a NETCDF:"<path>":<var> source selector."""
    if source and source.startswith("NETCDF:") and ":" in source:
        return source.rsplit(":", 1)[-1] or None
    return None


class NetcdfRasterGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path
        from databricks.labs.gbx.ds.writer import assert_write_schema

        assert_write_schema(schema)  # exact (source, tile)
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.var_name_col = options.get("varNameCol")
        # Use self.path (scheme stripped), NOT the raw path: a dbfs:/file:-qualified
        # path makes os.path.isdir(path) False, silently skipping overwrite cleanup.
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.nc")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import numpy as np
        from netCDF4 import Dataset
        from rasterio.io import MemoryFile

        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            source = row["source"]
            raster_bytes = bytes(row["tile"]["raster"])
            with MemoryFile(raster_bytes) as mf, mf.open() as ds:
                arr = ds.read(1)
                transform = ds.transform
                epsg = ds.crs.to_epsg() if ds.crs else None
                nodata = ds.nodata
            h, w = arr.shape[-2], arr.shape[-1]
            # pixel-centre 1-D coords; array_2d is north-up so transform.e < 0
            # -> lat values are descending (south) as index increases.
            lon = np.array([transform.c + transform.a * (i + 0.5) for i in range(w)])
            lat = np.array([transform.f + transform.e * (j + 0.5) for j in range(h)])
            # variable name: varNameCol override -> source selector -> "data"
            var: Optional[str] = None
            if self.var_name_col and row[self.var_name_col]:
                var = os.path.basename(str(row[self.var_name_col]))
            var = var or _var_from_source(source) or "data"
            # filename: nameCol (basename) -> var name (if from source) -> uuid
            if self.name_col and row[self.name_col]:
                stem = os.path.basename(str(row[self.name_col]))
            else:
                stem = _var_from_source(source) or uuid.uuid4().hex[:12]

            tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
            tmp.close()
            try:
                nc = Dataset(tmp.name, "w")
                try:
                    nc.createDimension("lat", h)
                    nc.createDimension("lon", w)
                    vlat = nc.createVariable("lat", "f8", ("lat",))
                    vlat.standard_name = "latitude"
                    vlat.units = "degrees_north"
                    vlat[:] = lat
                    vlon = nc.createVariable("lon", "f8", ("lon",))
                    vlon.standard_name = "longitude"
                    vlon.units = "degrees_east"
                    vlon[:] = lon
                    kw = {} if nodata is None else {"fill_value": nodata}
                    dv = nc.createVariable(var, arr.dtype.str[1:], ("lat", "lon"), **kw)
                    if epsg and epsg != 4326:
                        crs_var = nc.createVariable("crs", "i4")
                        crs_var.grid_mapping_name = "latitude_longitude"
                        crs_var.spatial_epsg = int(epsg)
                        dv.grid_mapping = "crs"
                    dv[:] = arr
                finally:
                    nc.close()
                out = os.path.join(self.path, f"{stem}.nc")
                # shutil.copyfile is FUSE-safe: no chmod (copy2 sets perms, FUSE
                # rejects); no cross-device rename (os.rename fails on FUSE).
                shutil.copyfile(tmp.name, out)
                written.append(out)
            finally:
                os.unlink(tmp.name)
        return NetcdfCommitMessage(paths=written)

    def commit(self, messages: list) -> None:
        return None

    def abort(self, messages: list) -> None:
        for msg in messages:
            if isinstance(msg, NetcdfCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
