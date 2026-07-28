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

from databricks.labs.gbx.ds import _scratch


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


# ----------------------------------------------------------------------------
# Shared merge cores (used by BOTH singleFile fragments AND merge .nc files)
#
# The only thing that differs between singleFile and post-hoc directory merge
# is the INPUT source: singleFile feeds feather fragments from scratch, merge
# feeds on-disk .nc part files. The merge / validate / copy / verify / delete
# tail is identical (``_publish_merged`` + the caller's part-delete step).
# ----------------------------------------------------------------------------

_MERGE_POINTER = (
    "to mosaic window-tiles of one variable into a single grid, use "
    "gbx_rst_merge_agg / gbx_rst_merge before writing."
)

_FLOAT_NP_DTYPES = ("f4", "f8")


def _glob_merge_inputs(path: str, target: str) -> List[str]:
    """Glob ``<path>/*.nc`` excluding the resolved output file.

    ``glob`` is non-recursive so it never descends into the hidden
    ``.gbx_scratch`` container; we additionally exclude the resolved merge
    output name so a re-merge does not fold a prior merged file into itself.
    """
    target_name = os.path.basename(target)
    return sorted(
        p
        for p in glob.glob(os.path.join(path, "*.nc"))
        if os.path.basename(p) != target_name
    )


def _publish_merged(tmp_path: str, target: str, expected_count: int, count_fn) -> None:
    """Data-safe publish of a merged temp .nc to ``target``.

    Order (never lose parts to a failed merge): (1) the merge already wrote
    ``tmp_path``; (2) VALIDATE — reopen cleanly via netCDF4 AND element count
    (via ``count_fn``) == ``expected_count``; (3) ``shutil.copyfile`` temp ->
    target; (4) VERIFY target exists and byte size == temp size. Any failure
    raises BEFORE the caller deletes any source part.
    """
    import netCDF4

    # (2) validate: reopen + element count.
    with netCDF4.Dataset(tmp_path, "r") as nc:
        actual = count_fn(nc)
    if actual != expected_count:
        raise ValueError(
            f"netcdf_gbx merge: validation failed — merged file has {actual} "
            f"elements but {expected_count} were expected; source parts left intact."
        )
    # (3) copy temp -> target (FUSE-safe: content only, no chmod).
    shutil.copyfile(tmp_path, target)
    # (4) verify target exists + byte size matches. On any verify failure, remove
    # the partial target (best-effort) BEFORE raising so a truncated FUSE copy
    # does not leave a corrupt-but-valid-looking output; source parts stay intact
    # because the caller only deletes them AFTER this returns cleanly.
    if not os.path.exists(target):
        raise ValueError(
            f"netcdf_gbx merge: target {target} missing after copy; parts intact."
        )
    if os.path.getsize(target) != os.path.getsize(tmp_path):
        if os.path.exists(target):
            try:
                os.remove(target)
            except OSError:
                pass
        raise ValueError(
            f"netcdf_gbx merge: target {target} size mismatch after copy; parts intact."
        )


def _count_raster_data_vars(nc) -> int:
    return len([v for v in nc.variables if v not in ("lat", "lon", "crs")])


def _count_vector_obs(nc) -> int:
    return int(nc.dimensions["obs"].size)


# ---- Raster grid records --------------------------------------------------


def _raster_records_from_frags(frags: List[str]) -> List[dict]:
    """Read grid-fragment feather files into normalized grid records."""
    import numpy as np
    import pyarrow.feather as feather

    records: List[dict] = []
    for frag in frags:
        tbl = feather.read_table(frag)
        md = tbl.schema.metadata or {}
        dtype = md.get(b"dtype", b"<f4").decode()
        width = int(md.get(b"width", b"0"))
        height = int(md.get(b"height", b"0"))
        transform = tuple(
            float(x) for x in md.get(b"transform", b"").decode().split(",")
        )
        epsg_raw = md.get(b"crs_epsg", b"").decode()
        nodata_raw = md.get(b"nodata", b"").decode()
        arr = np.array(tbl.column("value").to_pylist(), dtype=dtype).reshape(
            height, width
        )
        records.append(
            {
                "varname": md.get(b"varname", b"data").decode(),
                "width": width,
                "height": height,
                "dtype": dtype,
                "transform": transform,
                "epsg": int(epsg_raw) if epsg_raw else None,
                "nodata": float(nodata_raw) if nodata_raw else None,
                "array": arr,
            }
        )
    return records


def _raster_records_from_ncs(paths: List[str]) -> List[dict]:
    """Read on-disk grid .nc part files into normalized grid records (one per
    data variable). Reconstructs the north-up geotransform from lat/lon."""
    import netCDF4
    import numpy as np

    records: List[dict] = []
    for p in paths:
        nc = netCDF4.Dataset(p, "r")
        try:
            lons = np.asarray(nc.variables["lon"][:])
            lats = np.asarray(nc.variables["lat"][:])
            a = float(lons[1] - lons[0]) if len(lons) > 1 else 1.0
            e = float(lats[1] - lats[0]) if len(lats) > 1 else -1.0
            c = float(lons[0]) - a / 2.0
            f = float(lats[0]) - e / 2.0
            transform = (a, 0.0, c, 0.0, e, f)
            epsg = None
            if "crs" in nc.variables:
                se = getattr(nc.variables["crs"], "spatial_epsg", None)
                if se is not None:
                    epsg = int(se)
            for name in nc.variables:
                if name in ("lat", "lon", "crs"):
                    continue
                v = nc.variables[name]
                if tuple(v.dimensions) != ("lat", "lon"):
                    continue
                v.set_auto_maskandscale(False)
                arr = np.array(v[:])
                nodata = getattr(v, "_FillValue", None)
                records.append(
                    {
                        "varname": name,
                        "width": int(len(lons)),
                        "height": int(len(lats)),
                        "dtype": np.dtype(v.dtype).str,
                        "transform": transform,
                        "epsg": epsg,
                        "nodata": float(nodata) if nodata is not None else None,
                        "array": arr,
                    }
                )
        finally:
            nc.close()
    return records


def _merge_raster_grids(records: List[dict], tmp_path: str) -> int:
    """Merge distinct same-grid data-var records into one CF grid .nc at
    ``tmp_path``. Runs the grid-compatibility gate (raises ValueError with the
    rst_merge_agg pointer on incompatible grids or duplicate vars). Returns the
    number of data variables written (== len(records))."""
    import numpy as np
    from netCDF4 import Dataset

    if not records:
        raise ValueError("netcdf_gbx merge: no grid records to merge.")
    ref = records[0]
    ref_w, ref_h, ref_epsg, ref_t = (
        ref["width"],
        ref["height"],
        ref["epsg"],
        ref["transform"],
    )
    seen_vars: set = set()
    for r in records:
        if (r["width"], r["height"]) != (ref_w, ref_h):
            raise ValueError(
                f"netcdf_gbx merge: tiles have incompatible grids (sizes "
                f"{(ref_w, ref_h)} vs {(r['width'], r['height'])}); {_MERGE_POINTER}"
            )
        if r["epsg"] != ref_epsg:
            raise ValueError(
                f"netcdf_gbx merge: tiles have incompatible grids (CRS EPSG "
                f"{ref_epsg} vs {r['epsg']}); {_MERGE_POINTER}"
            )
        if not np.allclose(
            np.array(r["transform"]), np.array(ref_t), rtol=1e-9, atol=0.0
        ):
            raise ValueError(
                f"netcdf_gbx merge: tiles have incompatible grids (geotransform "
                f"{ref_t} vs {r['transform']}); {_MERGE_POINTER}"
            )
        if r["varname"] in seen_vars:
            raise ValueError(
                f"netcdf_gbx merge: duplicate variable {r['varname']!r} across "
                f"tiles (window-tiles are a mosaic, not a multi-variable merge); "
                f"{_MERGE_POINTER}"
            )
        seen_vars.add(r["varname"])

    a, b, c, d, e, f = ref_t
    lon = np.array([c + a * (i + 0.5) for i in range(ref_w)])
    lat = np.array([f + e * (j + 0.5) for j in range(ref_h)])

    nc = Dataset(tmp_path, "w")
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
        for r in records:
            kw = {} if r["nodata"] is None else {"fill_value": r["nodata"]}
            dv = nc.createVariable(r["varname"], r["dtype"][1:], ("lat", "lon"), **kw)
            if ref_epsg and ref_epsg != 4326:
                dv.grid_mapping = "crs"
            dv[:] = r["array"]
    finally:
        nc.close()
    return len(records)


# ---- Vector point batches -------------------------------------------------


def _vector_batches_from_frags(frags: List[str], attr_cols: List[str]):
    """Yield (lons, lats, attrs) point batches from feather fragments."""
    import pyarrow.feather as feather

    for frag in frags:
        tbl = feather.read_table(frag)
        names = set(tbl.schema.names)
        lons = tbl.column("longitude").to_pylist()
        lats = tbl.column("latitude").to_pylist()
        attrs = {c: tbl.column(c).to_pylist() for c in attr_cols if c in names}
        yield lons, lats, attrs


def _vector_batches_from_ncs(paths: List[str], attr_cols: List[str]):
    """Yield (lons, lats, attrs) point batches from on-disk CF-DSG .nc files."""
    import netCDF4
    import numpy as np

    for p in paths:
        with netCDF4.Dataset(p, "r") as nc:
            lons = [float(x) for x in np.asarray(nc.variables["longitude"][:])]
            lats = [float(x) for x in np.asarray(nc.variables["latitude"][:])]
            attrs = {}
            for name in attr_cols:
                if name not in nc.variables:
                    continue
                col = nc.variables[name][:]
                data = np.array(getattr(col, "data", col)).tolist()
                mask = np.ma.getmaskarray(col).tolist()
                attrs[name] = [None if m else v for v, m in zip(data, mask)]
            yield lons, lats, attrs


def _vector_nc_attr_specs(paths: List[str]):
    """Inspect the first readable part for (attr_specs, resolved_srid).

    attr_specs is an ordered list of (name, numpy-dtype like 'f8'/'i4') for the
    obs-dimension data variables (excluding latitude/longitude/crs).
    """
    import netCDF4
    import numpy as np

    for p in paths:
        with netCDF4.Dataset(p, "r") as nc:
            specs = []
            for name in nc.variables:
                if name in ("latitude", "longitude", "crs"):
                    continue
                v = nc.variables[name]
                if tuple(v.dimensions) != ("obs",):
                    continue
                specs.append((name, np.dtype(v.dtype).str[1:]))
            srid = "4326"
            if "crs" in nc.variables:
                se = getattr(nc.variables["crs"], "spatial_epsg", None)
                if se is not None:
                    srid = str(int(se))
            return specs, srid
    return [], "4326"


def _merge_vector_points(batches, tmp_path: str, attr_specs, resolved_srid) -> int:
    """Stream point batches into one CF-DSG point .nc with an UNLIMITED obs
    dim at ``tmp_path``. attr_specs is an ordered list of (name, numpy-dtype).
    Returns the total number of obs written."""
    import netCDF4 as _nc4
    import numpy as np
    from netCDF4 import Dataset

    nc = Dataset(tmp_path, "w")
    total = 0
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
        data_vars = {}
        for name, d in attr_specs:
            if d in _FLOAT_NP_DTYPES:
                dv = nc.createVariable(name, d, ("obs",), fill_value=np.nan)
            else:
                dv = nc.createVariable(
                    name, d, ("obs",), fill_value=_nc4.default_fillvals[d]
                )
            dv.coordinates = "latitude longitude"
            data_vars[name] = (dv, d)

        start = 0
        for lons, lats, attrs in batches:
            k = len(lons)
            if k == 0:
                continue
            vlat[start : start + k] = np.asarray(lats)
            vlon[start : start + k] = np.asarray(lons)
            for name, (dv, d) in data_vars.items():
                vals = attrs.get(name)
                if vals is None:
                    vals = [None] * k
                if d in _FLOAT_NP_DTYPES:
                    arr = np.array(
                        [v if v is not None else np.nan for v in vals], dtype=d
                    )
                else:
                    fv = _nc4.default_fillvals[d]
                    arr = np.array([v if v is not None else fv for v in vals], dtype=d)
                dv[start : start + k] = arr
            start += k
            total += k
    finally:
        nc.close()
    return total


class NetcdfRasterGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path
        from databricks.labs.gbx.ds.writer import assert_write_schema

        self.merge = str(options.get("merge", "false")).lower() == "true"
        # merge IGNORES the DataFrame rows (it folds the .nc files already on
        # disk), so a non-conforming DataFrame is legal on the merge path — skip
        # the (source, tile) schema gate there. See docs/readers/netcdf.mdx.
        if not self.merge:
            assert_write_schema(schema)  # exact (source, tile)
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.var_name_col = options.get("varNameCol")
        self.single_file = str(options.get("singleFile", "false")).lower() == "true"
        self.keep_parts = str(options.get("keepParts", "false")).lower() == "true"
        self.file_name = options.get("fileName")
        self.part_prefix = options.get("partPrefix") or "part"
        # merge wins over singleFile if both are set.
        if self.merge:
            self.single_file = False
        # NOTE: self.merge is parsed FIRST (above) so the schema gate can be
        # skipped on the merge path.
        # Use self.path (scheme stripped), NOT the raw path: a dbfs:/file:-qualified
        # path makes os.path.isdir(path) False, silently skipping overwrite cleanup.
        # In merge mode the directory's .nc files ARE the inputs, so the overwrite
        # glob-delete must NOT run (it would delete the very inputs to merge).
        if not self.merge and overwrite and os.path.isdir(self.path):
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

        if self.merge:
            # merge mode: the directory's .nc files ARE the inputs. Do NOT
            # consume the DataFrame (no re-run); merge happens on the driver.
            return NetcdfCommitMessage(paths=[], frag_path="")
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
                    else f"{self.part_prefix}-{uuid.uuid4().hex[:8]}"
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
        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        # Collect ALL fragment paths (frag_path + any extras in paths).
        frags: List[str] = []
        for m in messages:
            if isinstance(m, NetcdfCommitMessage):
                if m.frag_path:
                    frags.append(m.frag_path)
                frags.extend(m.paths)
        if not frags:
            _scratch.remove_scratch_dir(self.scratch_dir)
            return None

        target = _resolve_single_file_output(self.path, self.file_name, ".nc")
        try:
            records = _raster_records_from_frags(frags)
            tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
            tmp.close()
            try:
                expected = _merge_raster_grids(records, tmp.name)
                _publish_merged(tmp.name, target, expected, _count_raster_data_vars)
            finally:
                os.unlink(tmp.name)
        finally:
            # Scratch fragments are always disposable (they are not user parts).
            _scratch.remove_scratch_dir(self.scratch_dir)
        return None

    def _commit_merge(self) -> None:
        """Post-hoc directory merge: fold existing .nc parts into one .nc."""
        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        target = _resolve_single_file_output(self.path, self.file_name, ".nc")
        parts = _glob_merge_inputs(self.path, target)
        if not parts:
            raise ValueError(
                f"netcdf_gbx merge: no .nc files to merge under {self.path} "
                f"(nothing to merge)."
            )
        records = _raster_records_from_ncs(parts)
        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.close()
        try:
            expected = _merge_raster_grids(records, tmp.name)
            _publish_merged(tmp.name, target, expected, _count_raster_data_vars)
        finally:
            os.unlink(tmp.name)
        # DATA-SAFETY: only delete parts AFTER validate+copy+verify all passed.
        if not self.keep_parts:
            for p in parts:
                try:
                    os.remove(p)
                except OSError:
                    pass

    def commit(self, messages: list) -> None:
        if self.merge:
            return self._commit_merge()
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
            _scratch.remove_scratch_dir(self.scratch_dir)


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

        self.merge = str(options.get("merge", "false")).lower() == "true"
        names = [f.name for f in schema.fields]
        # merge IGNORES the DataFrame rows (it folds the CF-DSG .nc files already
        # on disk), so a non-conforming DataFrame is legal on the merge path —
        # skip the geom_0 schema gate there. See docs/readers/netcdf.mdx.
        if not self.merge and ("geom_0" not in names or "geom_0_srid" not in names):
            raise ValueError(
                "netcdf_gbx vector writer requires the vector schema "
                "(attributes + geom_0 + geom_0_srid[+ geom_0_srid_proj]); got "
                f"{names}"
            )
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.name_col = options.get("nameCol")
        self.single_file = str(options.get("singleFile", "false")).lower() == "true"
        self.keep_parts = str(options.get("keepParts", "false")).lower() == "true"
        self.file_name = options.get("fileName")
        self.part_prefix = options.get("partPrefix") or "part"
        # merge wins over singleFile if both are set.
        if self.merge:
            self.single_file = False
        # Exclude geom columns AND the nameCol: nameCol is file-naming metadata
        # only and must not be written as a netCDF4 data variable (it may be
        # a StringType, which netCDF4 cannot assign to a numeric variable).
        excluded = {n for n in names if n.startswith("geom_0")}
        if self.name_col:
            excluded.add(self.name_col)
        self.attr_cols = [n for n in names if n not in excluded]
        self.dtypes = {f.name: f.dataType for f in schema.fields}
        # In merge mode the directory's .nc files ARE the inputs, so the overwrite
        # glob-delete must NOT run (it would delete the very inputs to merge).
        if not self.merge and overwrite and os.path.isdir(self.path):
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
    ) -> None:
        """Write lon/lat/attrs to a CF DSG point .nc at ``tmp_path``.

        Parts mode always writes a fixed-size ``obs`` dimension (one .nc per
        partition). The UNLIMITED-obs streaming merge lives in
        ``_merge_vector_points`` for singleFile / merge.
        """
        import netCDF4 as _nc4
        import numpy as np
        from netCDF4 import Dataset

        nc = Dataset(tmp_path, "w")
        try:
            nc.featureType = "point"
            n = len(lons)
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

        if self.merge:
            # merge mode: the directory's .nc files ARE the inputs. Do NOT
            # consume the DataFrame (no re-run); merge happens on the driver.
            return NetcdfCommitMessage(paths=[], frag_path="")

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

        # --- default (parts) mode: write one <partPrefix>-<uuid>.nc ---
        stem = name or f"{self.part_prefix}-{uuid.uuid4().hex[:8]}"
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
        if self.merge:
            return self._commit_merge()
        if not self.single_file:
            return None  # parts mode: executors already wrote the files
        return self._commit_single(messages)

    def _commit_single(self, messages: list) -> None:
        """singleFile: merge all feather fragments into one CF-DSG .nc."""
        import pyarrow.feather as feather

        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, NetcdfCommitMessage) and m.frag_path
        ]
        if not frags:
            # All partitions were empty.
            _scratch.remove_scratch_dir(self.scratch_dir)
            return None

        target = _resolve_single_file_output(self.path, self.file_name, ".nc")

        # Reconcile srid across fragments: use the first non-4326/non-mixed srid.
        srids: set = set()
        expected = 0
        for frag in frags:
            tbl = feather.read_table(frag, columns=[])  # schema/metadata only
            md = tbl.schema.metadata or {}
            s = (md or {}).get(b"srid", b"4326").decode()
            if s not in ("4326", "mixed", ""):
                srids.add(s)
        resolved_srid = next(iter(srids)) if len(srids) == 1 else "4326"
        for frag in frags:
            expected += feather.read_table(frag, columns=["longitude"]).num_rows

        # attr_specs come from the Spark schema (dynamic vector schema).
        attr_specs = [(c, self._np_dtype(self.dtypes[c])) for c in self.attr_cols]

        try:
            batches = _vector_batches_from_frags(frags, self.attr_cols)
            tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
            tmp.close()
            try:
                _merge_vector_points(batches, tmp.name, attr_specs, resolved_srid)
                _publish_merged(tmp.name, target, expected, _count_vector_obs)
            finally:
                os.unlink(tmp.name)
        finally:
            _scratch.remove_scratch_dir(self.scratch_dir)
        return None

    def _commit_merge(self) -> None:
        """Post-hoc directory merge: fold existing CF-DSG .nc parts into one."""
        import netCDF4

        from databricks.labs.gbx.ds.vector import _resolve_single_file_output

        target = _resolve_single_file_output(self.path, self.file_name, ".nc")
        parts = _glob_merge_inputs(self.path, target)
        if not parts:
            raise ValueError(
                f"netcdf_gbx merge: no .nc files to merge under {self.path} "
                f"(nothing to merge)."
            )
        # Discover attr specs + srid from the parts (schema is not threaded in).
        attr_specs, resolved_srid = _vector_nc_attr_specs(parts)
        attr_cols = [name for name, _ in attr_specs]
        expected = 0
        for p in parts:
            with netCDF4.Dataset(p, "r") as nc:
                expected += int(nc.dimensions["obs"].size)

        tmp = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
        tmp.close()
        try:
            batches = _vector_batches_from_ncs(parts, attr_cols)
            _merge_vector_points(batches, tmp.name, attr_specs, resolved_srid)
            _publish_merged(tmp.name, target, expected, _count_vector_obs)
        finally:
            os.unlink(tmp.name)
        # DATA-SAFETY: only delete parts AFTER validate+copy+verify all passed.
        if not self.keep_parts:
            for p in parts:
                try:
                    os.remove(p)
                except OSError:
                    pass

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
            _scratch.remove_scratch_dir(self.scratch_dir)
