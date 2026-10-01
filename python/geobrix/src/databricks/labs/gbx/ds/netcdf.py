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

import warnings
from typing import Dict, Iterator, List, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _encode, _listing, _netcdf
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition, reader_schema

# Estimated decoded-bytes ceiling for bandDim stacking before a soft-warn is emitted.
# 256 MiB is a conservative per-band-stack warn threshold; it is NOT the Serverless
# materialize cap (the Connect per-tile cap is 64 MiB via _connect_aware_lru_sizing).
# This constant is module-level so tests can lower it via monkeypatch.
_BANDDIM_WARN_BYTES: int = 256 * 1024 * 1024


def _parse_dim_index(raw: str) -> Dict[str, int]:
    """Parse the ``dimIndex`` option string (e.g. ``"time=2,level=1"``) into a
    mapping of dim name → integer index.

    Emits a ``UserWarning`` for duplicate dim names (last value wins).

    Raises:
        ValueError: if any entry is malformed or the index is not a valid integer.
    """
    result: Dict[str, int] = {}
    seen: set = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(
                f"netcdf_gbx: dimIndex entry {part!r} is invalid; expected 'dim=index'."
            )
        k, _, v = part.partition("=")
        k, v = k.strip(), v.strip()
        try:
            idx = int(v)
        except ValueError:
            raise ValueError(
                f"netcdf_gbx: dimIndex dim {k!r}: {v!r} is not a valid integer index."
            )
        if k in seen:
            warnings.warn(
                f"netcdf_gbx: dimIndex has duplicate dim {k!r}; "
                f"using the last value ({idx}).",
                UserWarning,
                stacklevel=2,
            )
        seen.add(k)
        result[k] = idx
    return result


def _parse_fanout(raw: str) -> List[str]:
    """Parse the ``fanout`` option string (e.g. ``"time,level"``) into a list of
    dim names.
    """
    return [s.strip() for s in raw.split(",") if s.strip()]


def _apply_slice_metadata(
    ds: "object", sel: Dict[str, int], meta: Dict[str, str]
) -> None:
    """Augment *meta* with sliceDims and sliceCoord_<dim> entries from *sel*."""
    meta["sliceDims"] = ",".join(f"{k}={v}" for k, v in sorted(sel.items()))
    for dim_name, idx in sorted(sel.items()):
        if dim_name in ds.coords:
            try:
                cv = float(ds.coords[dim_name].values[idx])
                meta[f"sliceCoord_{dim_name}"] = str(cv)
            except (IndexError, TypeError, ValueError):
                pass


def _encode_single_band_tile(
    ds: "object",
    var: str,
    sel: Dict[str, int],
    transform: "object",
    crs: str,
    nodata: "object",
    file_path: str,
) -> Tuple:
    """Encode one 2-D slice as a single-band GeoTIFF tile.

    Returns ``(source, (cellid, raster_bytes, meta))``.
    """
    from rasterio.io import MemoryFile

    arr = _netcdf.array_2d(ds, var, sel=sel if sel else None)

    if sel:
        suffix = ",".join(f"{k}={v}" for k, v in sorted(sel.items()))
        source = f'NETCDF:"{file_path}":{var}[{suffix}]'
    else:
        source = f'NETCDF:"{file_path}":{var}'

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
                source_path=file_path,
                all_parents="",
                tile_format="gtiff",
            )

    if sel:
        _apply_slice_metadata(ds, sel, meta)

    return source, (cellid, raster_bytes, meta)


def _encode_multiband_tile(
    ds: "object",
    var: str,
    sel: Dict[str, int],
    band_dim: str,
    n_bands: int,
    transform: "object",
    crs: str,
    nodata: "object",
    file_path: str,
) -> Tuple:
    """Stream-encode *n_bands* slices of *band_dim* into one multi-band GeoTIFF.

    One slice is decoded at a time to bound peak input RAM.
    Returns ``(source, (cellid, raster_bytes, meta))``.
    """
    import numpy as np
    from rasterio.io import MemoryFile

    # Decode the first slice to learn shape/dtype; write it to band 1.
    first_arr = _netcdf.array_2d(ds, var, sel={**sel, band_dim: 0})
    h, w = first_arr.shape[-2], first_arr.shape[-1]
    dtype = str(first_arr.dtype)

    # Soft memory warn: estimate total decoded bytes.
    est_bytes = n_bands * h * w * np.dtype(dtype).itemsize
    if est_bytes > _BANDDIM_WARN_BYTES:
        warnings.warn(
            f"netcdf_gbx: variable {var!r} bandDim={band_dim!r}: estimated "
            f"decoded stack ≈ {est_bytes // (1024 * 1024)} MiB "
            f"({n_bands} bands × {h}×{w} {dtype}); consider 'fanout' for "
            f"memory-safe per-slice rows.",
            UserWarning,
            stacklevel=3,
        )

    # Collect coordinate values for band descriptions.
    if band_dim in ds.coords:
        band_coords: List[float] = []
        for k in range(n_bands):
            try:
                band_coords.append(float(ds.coords[band_dim].values[k]))
            except (IndexError, TypeError, ValueError):
                band_coords.append(float(k))
    else:
        band_coords = [float(k) for k in range(n_bands)]

    # Source string: sel dims + bandDim marker (all sorted together).
    source_parts = [f"{k}={v}" for k, v in sorted(sel.items())]
    source_parts.append(f"bandDim={band_dim}")
    source = f'NETCDF:"{file_path}":{var}[{",".join(sorted(source_parts))}]'

    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=n_bands,
        dtype=dtype,
        crs=crs,
        transform=transform,
    )
    if nodata is not None:
        profile["nodata"] = nodata

    with MemoryFile() as mf:
        with mf.open(**profile) as out:
            out.write(first_arr.astype(dtype), 1)
            del first_arr
            for k in range(1, n_bands):
                arr_k = _netcdf.array_2d(ds, var, sel={**sel, band_dim: k})
                out.write(arr_k.astype(dtype), k + 1)
                del arr_k
            out.descriptions = tuple(f"{band_dim}={c}" for c in band_coords)
        with mf.open() as rds:
            cellid, raster_bytes, meta = _encode.encode_tile(
                rds,
                window=(0, 0, w, h),
                source_path=file_path,
                all_parents="",
                tile_format="gtiff",
            )

    meta["bandDim"] = band_dim
    meta["bandCoords"] = ",".join(str(c) for c in band_coords)
    meta["bandDescriptions"] = ",".join(f"{band_dim}={c}" for c in band_coords)
    if sel:
        _apply_slice_metadata(ds, sel, meta)

    return source, (cellid, raster_bytes, meta)


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
        from itertools import product as _product

        # Parse dimIndex, fanout, and bandDim options.
        dim_index = _parse_dim_index(self.options.get("dimIndex", ""))
        fanout_dims = _parse_fanout(self.options.get("fanout", ""))
        band_dim = self.options.get("bandDim", "").strip()

        # Overlap guards.
        overlap_df = set(dim_index) & set(fanout_dims)
        if overlap_df:
            raise ValueError(
                f"netcdf_gbx: dims {sorted(overlap_df)!r} appear in both dimIndex "
                "and fanout; a dimension cannot be both pinned and expanded."
            )
        if band_dim:
            if band_dim in dim_index:
                raise ValueError(
                    f"netcdf_gbx: bandDim={band_dim!r} also appears in dimIndex; "
                    "a dimension cannot be both stacked (bandDim) and pinned (dimIndex)."
                )
            if band_dim in fanout_dims:
                raise ValueError(
                    f"netcdf_gbx: bandDim={band_dim!r} also appears in fanout; "
                    "a dimension cannot be both stacked (bandDim) and expanded (fanout)."
                )

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = _netcdf.select_variables(ds, self.options, "raster")

            for var in variables:
                transform, crs = _netcdf.grid_transform_crs(ds, var)
                nodata = _netcdf.nodata_of(ds, var)
                var_ldims = dict(_netcdf.leading_dims(ds, var))

                # Per-variable typo warning (includes bandDim in the check).
                if var_ldims and (dim_index or fanout_dims or band_dim):
                    all_req = (
                        set(dim_index)
                        | set(fanout_dims)
                        | ({band_dim} if band_dim else set())
                    )
                    unknown = all_req - set(var_ldims)
                    if unknown:
                        warnings.warn(
                            f"netcdf_gbx: variable {var!r} has leading dims "
                            f"{sorted(var_ldims)!r} but requested dim(s) "
                            f"{sorted(unknown)!r} are not among them; "
                            f"falling through (unknown dims ignored).",
                            UserWarning,
                            stacklevel=2,
                        )

                band_dim_for_var = (
                    band_dim if (band_dim and band_dim in var_ldims) else None
                )
                active_fanout = [
                    (d, var_ldims[d]) for d in fanout_dims if d in var_ldims
                ]
                fanout_names = [d for d, _ in active_fanout]
                combos: List[Tuple] = (
                    list(_product(*[range(sz) for _, sz in active_fanout]))
                    if active_fanout
                    else [()]
                )

                for combo in combos:
                    sel: Dict[str, int] = {
                        k: v
                        for k, v in dim_index.items()
                        if k in var_ldims and k != band_dim_for_var
                    }
                    for dname, idx in zip(fanout_names, combo):
                        sel[dname] = idx

                    if band_dim_for_var:
                        n_bands = var_ldims[band_dim_for_var]
                        source, tile = _encode_multiband_tile(
                            ds,
                            var,
                            sel,
                            band_dim_for_var,
                            n_bands,
                            transform,
                            crs,
                            nodata,
                            partition.file_path,
                        )
                    else:
                        source, tile = _encode_single_band_tile(
                            ds,
                            var,
                            sel,
                            transform,
                            crs,
                            nodata,
                            partition.file_path,
                        )

                    yield (source, tile)


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
