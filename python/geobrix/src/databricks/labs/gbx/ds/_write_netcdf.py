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
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


@dataclass
class NetcdfCommitMessage(WriterCommitMessage):
    paths: List[str] = field(default_factory=list)
    # singleFile mode: path of this partition's feather fragment in scratch.
    # Empty string means the partition was empty (no rows).
    frag_path: str = ""


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
        self.single_file = str(options.get("singleFile", "false")).lower() == "true"
        # Use self.path (scheme stripped), NOT the raw path: a dbfs:/file:-qualified
        # path makes os.path.isdir(path) False, silently skipping overwrite cleanup.
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.nc")):
                try:
                    os.remove(stale)
                except OSError:
                    pass
        # singleFile mode: per-write scratch dir for grid fragments.
        if self.single_file:
            from databricks.labs.gbx.ds import _scratch

            self.scratch_dir = _scratch.new_scratch_dir(self.path)
        else:
            self.scratch_dir = ""

    def _resolve_var(self, row, source) -> str:
        """Resolve the variable name: varNameCol override -> source selector -> data."""
        var: Optional[str] = None
        if self.var_name_col and row[self.var_name_col]:
            var = os.path.basename(str(row[self.var_name_col]))
        return var or _var_from_source(source) or "data"

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import numpy as np
        from netCDF4 import Dataset
        from rasterio.io import MemoryFile

        if self.single_file:
            return self._write_single(iterator)

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
            # variable name: varNameCol takes precedence as an explicit override,
            # then source selector, then "data".
            var: Optional[str] = None
            if self.var_name_col and row[self.var_name_col]:
                var = os.path.basename(str(row[self.var_name_col]))
            var = var or _var_from_source(source) or "data"
            # filename: nameCol (basename) -> resolved var name -> uuid fallback.
            # varNameCol also drives the stem when no nameCol is set, for consistency.
            if self.name_col and row[self.name_col]:
                stem = os.path.basename(str(row[self.name_col]))
            else:
                stem = (
                    var
                    if (self.var_name_col or _var_from_source(source))
                    else uuid.uuid4().hex[:12]
                )

            tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
            tmp.close()
            try:
                nc = Dataset(tmp.name, "w")
                try:
                    nc.createDimension("lat", h)
                    nc.createDimension("lon", w)
                    vlat = nc.createVariable("lat", "f8", ("lat",))
                    vlat.standard_name = "latitude"
                    # This writer targets GEOGRAPHIC (lon/lat degrees) grids — the
                    # round-trip contract of the netcdf_gbx reader. Callers with
                    # projected-CRS tiles should reproject to EPSG:4326 before writing
                    # (projected coords would be mislabeled as degrees here).
                    vlat.units = "degrees_north"
                    vlat[:] = lat
                    vlon = nc.createVariable("lon", "f8", ("lon",))
                    vlon.standard_name = "longitude"
                    vlon.units = "degrees_east"
                    vlon[:] = lon
                    kw = {} if nodata is None else {"fill_value": nodata}
                    dv = nc.createVariable(var, arr.dtype.str[1:], ("lat", "lon"), **kw)
                    if epsg and epsg != 4326:
                        # Preserve the source EPSG via a CF grid_mapping variable so
                        # _netcdf._crs_string can recover it on re-read (reads spatial_epsg).
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

    # ------------------------------------------------------------------
    # singleFile mode (two-phase: executor fragment -> driver merge)
    # ------------------------------------------------------------------

    def _write_single(self, iterator: Iterator) -> WriterCommitMessage:
        """Per (source, tile) row, decode the tile and write a feather fragment
        into scratch capturing {varname, array, width, height, transform, crs, nodata}.

        The 2-D array is flattened to one feather column; grid metadata rides in
        the table schema metadata so commit() can reopen and run the grid gate.
        """
        import numpy as np
        import pyarrow as pa
        import pyarrow.feather as feather
        from rasterio.io import MemoryFile

        os.makedirs(self.scratch_dir, exist_ok=True)
        frags: List[str] = []
        for row in iterator:
            source = row["source"]
            raster_bytes = bytes(row["tile"]["raster"])
            with MemoryFile(raster_bytes) as mf, mf.open() as ds:
                arr = ds.read(1)
                transform = ds.transform
                epsg = ds.crs.to_epsg() if ds.crs else None
                nodata = ds.nodata
            h, w = int(arr.shape[-2]), int(arr.shape[-1])
            var = self._resolve_var(row, source)
            flat = np.asarray(arr).reshape(-1)
            # transform is an affine.Affine: (a, b, c, d, e, f) via [:6]
            t = tuple(float(x) for x in tuple(transform)[:6])
            meta = {
                b"varname": var.encode(),
                b"width": str(w).encode(),
                b"height": str(h).encode(),
                b"dtype": arr.dtype.str.encode(),
                b"transform": ",".join(repr(x) for x in t).encode(),
                b"crs_epsg": (b"" if epsg is None else str(int(epsg)).encode()),
                b"nodata": (b"" if nodata is None else repr(float(nodata)).encode()),
            }
            tbl = pa.table(
                {"value": pa.array(flat)},
                metadata=meta,
            )
            frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
            feather.write_feather(tbl, frag)
            frags.append(frag)
        # One partition may yield several tiles; return the first frag plus any
        # extras. NetcdfCommitMessage carries a single frag_path, so stash extras
        # in paths (they are scratch fragment paths here, not final outputs).
        if not frags:
            return NetcdfCommitMessage(paths=[], frag_path="")
        return NetcdfCommitMessage(paths=frags[1:], frag_path=frags[0])

    def _commit_single(self, messages: list) -> None:
        import numpy as np
        from netCDF4 import Dataset

        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        # Collect ALL fragment paths (frag_path + any extras in paths).
        frags: List[str] = []
        for m in messages:
            if isinstance(m, NetcdfCommitMessage):
                if m.frag_path:
                    frags.append(m.frag_path)
                frags.extend(m.paths)
        if not frags:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
            return None

        import pyarrow.feather as feather

        # --- Grid-compatibility gate ---
        fragments = (
            []
        )  # list of (varname, width, height, dtype, transform, epsg, nodata)
        for frag in frags:
            tbl = feather.read_table(frag, columns=[])  # schema/metadata only
            md = tbl.schema.metadata or {}
            varname = md.get(b"varname", b"data").decode()
            width = int(md.get(b"width", b"0"))
            height = int(md.get(b"height", b"0"))
            dtype = md.get(b"dtype", b"<f4").decode()
            transform = tuple(
                float(x) for x in md.get(b"transform", b"").decode().split(",")
            )
            epsg_raw = md.get(b"crs_epsg", b"").decode()
            epsg = int(epsg_raw) if epsg_raw else None
            nodata_raw = md.get(b"nodata", b"").decode()
            nodata = float(nodata_raw) if nodata_raw else None
            fragments.append(
                (frag, varname, width, height, dtype, transform, epsg, nodata)
            )

        merge_pointer = (
            "to mosaic window-tiles of one variable into a single grid, use "
            "gbx_rst_merge_agg / gbx_rst_merge before writing."
        )
        ref = fragments[0]
        _, _, ref_w, ref_h, _, ref_t, ref_epsg, _ = ref
        seen_vars = set()
        for _, varname, width, height, _, transform, epsg, _ in fragments:
            if (width, height) != (ref_w, ref_h):
                shutil.rmtree(self.scratch_dir, ignore_errors=True)
                raise ValueError(
                    f"netcdf_gbx singleFile: tiles have incompatible grids "
                    f"(sizes {(ref_w, ref_h)} vs {(width, height)}); {merge_pointer}"
                )
            if epsg != ref_epsg:
                shutil.rmtree(self.scratch_dir, ignore_errors=True)
                raise ValueError(
                    f"netcdf_gbx singleFile: tiles have incompatible grids "
                    f"(CRS EPSG {ref_epsg} vs {epsg}); {merge_pointer}"
                )
            if not np.allclose(
                np.array(transform), np.array(ref_t), rtol=1e-9, atol=0.0
            ):
                shutil.rmtree(self.scratch_dir, ignore_errors=True)
                raise ValueError(
                    f"netcdf_gbx singleFile: tiles have incompatible grids "
                    f"(geotransform {ref_t} vs {transform}); {merge_pointer}"
                )
            if varname in seen_vars:
                shutil.rmtree(self.scratch_dir, ignore_errors=True)
                raise ValueError(
                    f"netcdf_gbx singleFile: duplicate variable {varname!r} across "
                    f"tiles (window-tiles are a mosaic, not a multi-variable merge); "
                    f"{merge_pointer}"
                )
            seen_vars.add(varname)

        # --- Compatible + distinct varnames: write ONE CF grid .nc ---
        # Shared lat/lon derived from the common (north-up) transform.
        a, b, c, d, e, f = ref_t
        lon = np.array([c + a * (i + 0.5) for i in range(ref_w)])
        lat = np.array([f + e * (j + 0.5) for j in range(ref_h)])

        target = _resolve_single_file_output(self.path, None, ".nc")

        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.close()
        try:
            nc = Dataset(tmp.name, "w")
            try:
                nc.createDimension("lat", ref_h)
                nc.createDimension("lon", ref_w)
                vlat = nc.createVariable("lat", "f8", ("lat",))
                vlat.standard_name = "latitude"
                vlat.units = "degrees_north"
                vlat[:] = lat
                vlon = nc.createVariable("lon", "f8", ("lon",))
                vlon.standard_name = "longitude"
                vlon.units = "degrees_east"
                vlon[:] = lon
                if ref_epsg and ref_epsg != 4326:
                    crs_var = nc.createVariable("crs", "i4")
                    crs_var.grid_mapping_name = "latitude_longitude"
                    crs_var.spatial_epsg = int(ref_epsg)
                for frag, varname, width, height, dtype, _, epsg, nodata in fragments:
                    tbl = feather.read_table(frag)
                    arr = np.array(
                        tbl.column("value").to_pylist(), dtype=dtype
                    ).reshape(height, width)
                    kw = {} if nodata is None else {"fill_value": nodata}
                    dv = nc.createVariable(varname, dtype[1:], ("lat", "lon"), **kw)
                    if ref_epsg and ref_epsg != 4326:
                        dv.grid_mapping = "crs"
                    dv[:] = arr
            finally:
                nc.close()
            shutil.copyfile(tmp.name, target)
        finally:
            os.unlink(tmp.name)
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        return None

    def commit(self, messages: list) -> None:
        if self.single_file:
            return self._commit_single(messages)
        return None

    def abort(self, messages: list) -> None:
        for msg in messages:
            if isinstance(msg, NetcdfCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
                if msg.frag_path:
                    try:
                        os.remove(msg.frag_path)
                    except OSError:
                        pass
        if self.scratch_dir:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)


class NetcdfVectorGbxWriter(DataSourceWriter):
    """Write point rows to CF DSG .nc (featureType='point').

    Default (singleFile=false): one ``part-<uuid>.nc`` per Spark partition.
    singleFile=true: two-phase — executors write feather fragments to a shared
    scratch dir; the driver merges them into ONE .nc with an UNLIMITED obs
    dimension, streaming fragment by fragment to bound driver memory.

    Inverts the netcdf_gbx vector reader: consumes the dynamic vector schema
    (attribute columns + geom_0 WKB + geom_0_srid + geom_0_srid_proj) and
    writes latitude/longitude coord vars plus one data var per attribute column.
    Serverless-safe: no spark.conf/_jvm/.rdd/cache/persist.
    """

    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path

        names = [f.name for f in schema.fields]
        if "geom_0" not in names or "geom_0_srid" not in names:
            raise ValueError(
                "netcdf_gbx vector writer requires the vector schema "
                "(attributes + geom_0 + geom_0_srid[+ geom_0_srid_proj]); got "
                f"{names}"
            )
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.single_file = str(options.get("singleFile", "false")).lower() == "true"
        # Exclude geom columns AND the nameCol: nameCol is file-naming metadata
        # only and must not be written as a netCDF4 data variable (it may be
        # a StringType, which netCDF4 cannot assign to a numeric variable).
        excluded = {n for n in names if n.startswith("geom_0")}
        if self.name_col:
            excluded.add(self.name_col)
        self.attr_cols = [n for n in names if n not in excluded]
        self.dtypes = {f.name: f.dataType for f in schema.fields}
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.nc")):
                try:
                    os.remove(stale)
                except OSError:
                    pass
        # singleFile mode: set up a per-write scratch dir for feather fragments.
        if self.single_file:
            from databricks.labs.gbx.ds import _scratch

            self.scratch_dir = _scratch.new_scratch_dir(self.path)
        else:
            self.scratch_dir = ""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _np_dtype(dt) -> str:
        from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType

        if isinstance(dt, IntegerType):
            return "i4"
        if isinstance(dt, LongType):
            return "i8"
        if isinstance(dt, FloatType):
            return "f4"
        if isinstance(dt, DoubleType):
            return "f8"
        return "f8"

    @staticmethod
    def _is_float_dtype(d: str) -> bool:
        return d in ("f4", "f8")

    def _write_nc(
        self,
        tmp_path: str,
        lons: list,
        lats: list,
        attrs: dict,
        srids: set,
        *,
        unlimited_obs: bool = False,
    ) -> None:
        """Write lon/lat/attrs to a CF DSG point .nc at ``tmp_path``."""
        import netCDF4 as _nc4
        import numpy as np
        from netCDF4 import Dataset

        nc = Dataset(tmp_path, "w")
        try:
            nc.featureType = "point"
            n = len(lons)
            if unlimited_obs:
                nc.createDimension("obs", None)  # UNLIMITED
            else:
                nc.createDimension("obs", n)
            vlat = nc.createVariable("latitude", "f8", ("obs",))
            vlat.standard_name = "latitude"
            vlat.units = "degrees_north"
            vlat[:] = np.array(lats)
            vlon = nc.createVariable("longitude", "f8", ("obs",))
            vlon.standard_name = "longitude"
            vlon.units = "degrees_east"
            vlon[:] = np.array(lons)
            if len(srids) == 1 and next(iter(srids)) not in ("4326", None):
                crs = nc.createVariable("crs", "i4")
                crs.spatial_epsg = int(next(iter(srids)))
            for c in self.attr_cols:
                d = self._np_dtype(self.dtypes[c])
                vals = attrs[c]
                if self._is_float_dtype(d):
                    arr = np.array(
                        [v if v is not None else np.nan for v in vals], dtype=d
                    )
                    dv = nc.createVariable(c, d, ("obs",), fill_value=np.nan)
                else:
                    fv = _nc4.default_fillvals[d]
                    arr = np.array([v if v is not None else fv for v in vals], dtype=d)
                    dv = nc.createVariable(c, d, ("obs",), fill_value=fv)
                dv.coordinates = "latitude longitude"
                dv[:] = arr
        finally:
            nc.close()

    # ------------------------------------------------------------------
    # DataSourceWriter API
    # ------------------------------------------------------------------

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import shapely

        os.makedirs(self.path, exist_ok=True)
        lons: list = []
        lats: list = []
        attrs = {c: [] for c in self.attr_cols}
        srids: set = set()
        name = None
        for row in iterator:
            if name is None and self.name_col and row[self.name_col]:
                name = os.path.basename(str(row[self.name_col]))
            pt = shapely.from_wkb(bytes(row["geom_0"]))
            lons.append(pt.x)
            lats.append(pt.y)
            if row["geom_0_srid"] is not None:
                srids.add(str(row["geom_0_srid"]))
            for c in self.attr_cols:
                attrs[c].append(row[c])

        if not lons:
            # Empty partition: return a no-op message regardless of mode.
            return NetcdfCommitMessage(paths=[], frag_path="")

        if self.single_file:
            # --- two-phase: write feather fragment into scratch ---
            import pyarrow as pa
            import pyarrow.feather as feather
            from pyspark.sql.pandas.types import to_arrow_type

            os.makedirs(self.scratch_dir, exist_ok=True)
            # Build Arrow table: longitude, latitude, then each attr col.
            # Carry the resolved srid in table metadata so commit() can reconcile.
            resolved_srid = (
                next(iter(srids))
                if len(srids) == 1
                else ("4326" if not srids else "mixed")
            )
            cols_data = {"longitude": lons, "latitude": lats}
            for c in self.attr_cols:
                cols_data[c] = attrs[c]

            # Build Arrow schema: lon/lat as f8, attr cols typed from Spark schema.
            arrow_fields = [
                pa.field("longitude", pa.float64()),
                pa.field("latitude", pa.float64()),
            ]
            for c in self.attr_cols:
                arrow_fields.append(pa.field(c, to_arrow_type(self.dtypes[c])))
            arr_schema = pa.schema(
                arrow_fields,
                metadata={"srid": resolved_srid},
            )
            arrays = [pa.array(cols_data[f.name]) for f in arrow_fields]
            tbl = pa.table(
                dict(zip([f.name for f in arrow_fields], arrays)), schema=arr_schema
            )
            frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
            feather.write_feather(tbl, frag)
            return NetcdfCommitMessage(paths=[], frag_path=frag)

        # --- default (parts) mode: write one part-*.nc ---
        stem = name or f"part-{uuid.uuid4().hex[:8]}"
        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.close()
        try:
            self._write_nc(tmp.name, lons, lats, attrs, srids)
            out = os.path.join(self.path, f"{stem}.nc")
            # shutil.copyfile is FUSE-safe: no chmod (copy2 sets perms, FUSE
            # rejects); no cross-device rename (os.rename fails on FUSE).
            shutil.copyfile(tmp.name, out)
        finally:
            os.unlink(tmp.name)
        return NetcdfCommitMessage(paths=[out])

    def commit(self, messages: list) -> None:
        if not self.single_file:
            return None  # parts mode: executors already wrote the files

        # singleFile mode: merge all feather fragments into one CF-DSG .nc.
        import netCDF4 as _nc4
        import numpy as np
        import pyarrow.feather as feather

        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, NetcdfCommitMessage) and m.frag_path
        ]
        if not frags:
            # All partitions were empty.
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
            return None

        # Resolve target path (single .nc file).
        # _resolve_single_file_output expects ext WITH a leading dot.
        name_col_val = None  # nameCol not threaded through singleFile merge
        target = _resolve_single_file_output(self.path, name_col_val, ".nc")

        # Reconcile srid across fragments: use the first non-4326/non-mixed srid.
        srids: set = set()
        for frag in frags:
            tbl = feather.read_table(frag, columns=[])  # schema only
            s = (tbl.schema.metadata or {}).get(b"srid", b"4326").decode()
            if s not in ("4326", "mixed", ""):
                srids.add(s)
        resolved_srid = next(iter(srids)) if len(srids) == 1 else "4326"

        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.close()
        try:
            nc = _nc4.Dataset(tmp.name, "w")
            try:
                nc.featureType = "point"
                nc.createDimension("obs", None)  # UNLIMITED
                vlat = nc.createVariable("latitude", "f8", ("obs",))
                vlat.standard_name = "latitude"
                vlat.units = "degrees_north"
                vlon = nc.createVariable("longitude", "f8", ("obs",))
                vlon.standard_name = "longitude"
                vlon.units = "degrees_east"
                if resolved_srid not in ("4326", "mixed", ""):
                    crs_var = nc.createVariable("crs", "i4")
                    crs_var.spatial_epsg = int(resolved_srid)
                # Create data vars with fill values.
                data_vars = {}
                for c in self.attr_cols:
                    d = self._np_dtype(self.dtypes[c])
                    if self._is_float_dtype(d):
                        dv = nc.createVariable(c, d, ("obs",), fill_value=np.nan)
                    else:
                        fv = _nc4.default_fillvals[d]
                        dv = nc.createVariable(c, d, ("obs",), fill_value=fv)
                    dv.coordinates = "latitude longitude"
                    data_vars[c] = dv

                # Stream fragments one at a time: append rows to UNLIMITED obs.
                start = 0
                for frag in frags:
                    tbl = feather.read_table(frag)
                    k = tbl.num_rows
                    if k == 0:
                        continue
                    vlat[start : start + k] = tbl.column("latitude").to_pylist()
                    vlon[start : start + k] = tbl.column("longitude").to_pylist()
                    for c in self.attr_cols:
                        if c not in tbl.schema.names:
                            continue
                        d = self._np_dtype(self.dtypes[c])
                        vals = tbl.column(c).to_pylist()
                        if self._is_float_dtype(d):
                            arr = np.array(
                                [v if v is not None else np.nan for v in vals], dtype=d
                            )
                        else:
                            fv = _nc4.default_fillvals[d]
                            arr = np.array(
                                [v if v is not None else fv for v in vals], dtype=d
                            )
                        data_vars[c][start : start + k] = arr
                    start += k
            finally:
                nc.close()
            # FUSE-safe copy: content only, no chmod.
            shutil.copyfile(tmp.name, target)
        finally:
            os.unlink(tmp.name)
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        return None

    def abort(self, messages: list) -> None:
        for msg in messages:
            if isinstance(msg, NetcdfCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
                if msg.frag_path:
                    try:
                        os.remove(msg.frag_path)
                    except OSError:
                        pass
        if self.scratch_dir:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
