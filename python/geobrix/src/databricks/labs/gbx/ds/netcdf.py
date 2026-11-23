"""netcdf_gbx — lightweight NetCDF reader.

One DataSource, two modes (the `mode` option, default "raster"):
  * raster — CF regular/projected grids -> (source, tile), tile = the V2 tile; bbox cuts the grid on read.
  * vector — DSG points, or any 2-D field (incl. curvilinear swath) coerced to
    per-cell points -> the light vector schema (attrs + geom_0 WKB + srid cols).

Class 4 (raw sensor geometry + GLT) is rejected in both modes.
Serverless-safe: registers a DataSource and builds Column output only (no runtime
Spark-config mutation or JVM-bridge access).
"""

from __future__ import annotations

import warnings
from typing import Dict, Iterator, List, Optional, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.core.crs import crs_to_canonical
from databricks.labs.gbx.ds import _encode, _listing, _netcdf, _quarantine
from databricks.labs.gbx.ds.raster import (
    RasterGbxReader,
    _FilePartition,
    _v2_tile_row,
    reader_schema_v2,
)

# Estimated decoded-bytes ceiling for bandDim stacking before a soft-warn is emitted.
# 256 MiB is a conservative per-band-stack warn threshold; it is NOT the Serverless
# materialize cap (the Connect per-tile cap is 64 MiB via _connect_aware_lru_sizing).
# This constant is module-level so tests can lower it via monkeypatch.
_BANDDIM_WARN_BYTES: int = 256 * 1024 * 1024


def _option(options: "object", name: str, default: str = "") -> str:
    """Option ``name`` of a reader, whatever the case of the key.

    Spark hands a Python reader its options case-insensitively: ``dict(options)`` of
    them has lowercase keys (``dimindex``), while the option names here are camel case
    (``dimIndex``). A plain dict (a test, a direct call) keeps the case it was given.
    """
    want = name.lower()
    for key, value in dict(options).items():
        if str(key).lower() == want:
            return value
    return default


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


def _parse_bbox(raw: str) -> Optional[Tuple[float, float, float, float]]:
    """The ``bbox`` option ``"minx,miny,maxx,maxy"`` as four floats; None when the option is absent."""
    if not raw or not raw.strip():
        return None
    try:
        parts = tuple(float(v) for v in raw.split(","))
    except ValueError:
        parts = ()
    if len(parts) != 4 or not (parts[0] < parts[2] and parts[1] < parts[3]):
        raise ValueError(
            f"netcdf_gbx: bbox must be 'minx,miny,maxx,maxy' with minx < maxx and miny < maxy; got {raw!r}."
        )
    return parts


def _bbox_window(
    ds, variable, transform, crs, bbox, bbox_crs
) -> Optional[Tuple[int, int, int, int]]:
    """North-up pixel window (col_off, row_off, width, height) of ``variable``'s grid that covers ``bbox`` (in
    ``bbox_crs``, or the grid's CRS when None), snapped outward to whole pixels and clipped to the grid
    (``ds._window.window_for_bbox``); None when the two do not overlap."""
    from types import SimpleNamespace

    from databricks.labs.gbx.ds._window import window_for_bbox

    height, width = _netcdf.grid_shape(ds, variable)
    win = window_for_bbox(
        SimpleNamespace(crs=crs, transform=transform, width=width, height=height),
        bbox,
        bbox_crs,
    )
    if win is None:
        return None
    return int(win.col_off), int(win.row_off), int(win.width), int(win.height)


def _refuse_vector_bbox(options) -> None:
    if _option(options, "bbox").strip():
        raise ValueError(
            "netcdf_gbx: the bbox option applies to raster mode only. "
            "Fix: drop it, and filter the points after the read (for example st_intersects on geom_0)."
        )


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
    window: Optional[Tuple[int, int, int, int]] = None,
) -> Tuple:
    """Encode one 2-D slice (the ``window`` of it, when set) as a single-band GeoTIFF tile.

    Returns ``(source, tile)``, tile in V2_TILE_SCHEMA order: materialized, no path or window.
    """
    from rasterio.io import MemoryFile

    arr = _netcdf.fill_kept(
        _netcdf.array_2d(ds, var, sel=sel if sel else None, window=window), nodata
    )

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
        with open_bytes(mf) as rds:
            cellid, raster_bytes, meta = _encode.encode_tile(
                rds,
                window=(0, 0, w, h),
                source_path=file_path,
                all_parents="",
                tile_format="gtiff",
            )
            tile_crs = crs_to_canonical(rds.crs)

    if sel:
        _apply_slice_metadata(ds, sel, meta)
    if window is not None:
        meta["sourceWindow"] = ",".join(str(v) for v in window)

    return source, _v2_tile_row(
        cellid, raster_bytes, path=None, window=None, metadata=meta, crs=tile_crs
    )


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
    window: Optional[Tuple[int, int, int, int]] = None,
) -> Tuple:
    """Stream-encode *n_bands* slices of *band_dim* (the ``window`` of each, when set) into one multi-band GeoTIFF.

    One slice is decoded at a time to bound peak input RAM.
    Returns ``(source, tile)``, tile in V2_TILE_SCHEMA order: materialized, no path or window.
    """
    import numpy as np
    from rasterio.enums import ColorInterp
    from rasterio.io import MemoryFile

    # Decode the first slice to learn shape/dtype; write it to band 1.
    first_arr = _netcdf.fill_kept(
        _netcdf.array_2d(ds, var, sel={**sel, band_dim: 0}, window=window), nodata
    )
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
            # The steps of a variable are plain bands, none of them alpha. GDAL labels band 4
            # of a 4-band uint8 GTiff alpha (and bands 1-3 of a 3-band one RGB) when it
            # creates one, and ignores a label set after the pixels are written.
            out.colorinterp = [ColorInterp.undefined] * n_bands
            out.write(first_arr.astype(dtype), 1)
            del first_arr
            for k in range(1, n_bands):
                arr_k = _netcdf.fill_kept(
                    _netcdf.array_2d(ds, var, sel={**sel, band_dim: k}, window=window),
                    nodata,
                )
                out.write(arr_k.astype(dtype), k + 1)
                del arr_k
            out.descriptions = tuple(f"{band_dim}={c}" for c in band_coords)
        with open_bytes(mf) as rds:
            cellid, raster_bytes, meta = _encode.encode_tile(
                rds,
                window=(0, 0, w, h),
                source_path=file_path,
                all_parents="",
                tile_format="gtiff",
            )
            tile_crs = crs_to_canonical(rds.crs)

    meta["bandDim"] = band_dim
    meta["bandCoords"] = ",".join(str(c) for c in band_coords)
    meta["bandDescriptions"] = ",".join(f"{band_dim}={c}" for c in band_coords)
    if sel:
        _apply_slice_metadata(ds, sel, meta)
    if window is not None:
        meta["sourceWindow"] = ",".join(str(v) for v in window)

    return source, _v2_tile_row(
        cellid, raster_bytes, path=None, window=None, metadata=meta, crs=tile_crs
    )


def _slicing_options(options) -> Tuple[Dict[str, int], List[str], str]:
    """The ``dimIndex``, ``fanout`` and ``bandDim`` options, parsed; raises ValueError when one dim is in two."""
    # Parse dimIndex, fanout, and bandDim options.
    dim_index = _parse_dim_index(_option(options, "dimIndex"))
    fanout_dims = _parse_fanout(_option(options, "fanout"))
    band_dim = _option(options, "bandDim").strip()

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
    return dim_index, fanout_dims, band_dim


class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode each CF grid variable to a GeoTIFF tile (one row per variable)."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex
        self.options = dict(options)
        self.group = options.get("group")
        self.bbox = _parse_bbox(_option(options, "bbox"))
        self.bbox_crs = _option(options, "bboxCrs").strip() or None

    def _grid_of(self, ds, var):
        """(transform, crs, nodata, leading dims, window) of one variable; window is None without a bbox. None when
        the bbox misses the variable's grid (then the variable gives no row)."""
        from rasterio.windows import Window
        from rasterio.windows import transform as window_transform

        transform, crs = _netcdf.grid_transform_crs(ds, var)
        nodata = _netcdf.nodata_of(ds, var)
        ldims = dict(_netcdf.leading_dims(ds, var))
        window = None
        if self.bbox is not None:
            window = _bbox_window(ds, var, transform, crs, self.bbox, self.bbox_crs)
            if window is None:
                return None
            transform = window_transform(Window(*window), transform)
        return transform, crs, nodata, ldims, window

    def partitions(self):
        # NetCDF raster reader emits one row PER VARIABLE per file, not per tile
        # window. The tile-window planning in RasterGbxReader.partitions() does not
        # apply here — return one legacy _FilePartition per file so read() receives
        # a file-scoped partition and can iterate over variables itself.
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        from itertools import product as _product

        if _listing.is_empty_plan(partition):
            return

        dim_index, fanout_dims, band_dim = _slicing_options(self.options)

        # With read_paths' on_error="skip" (a hook on the reader) a variable, or
        # one slice of it, that cannot be read is a bad record and the others
        # are still read; without one it fails the read.
        hook = _quarantine.hook_of(self)

        def _bad(label: str, exc: BaseException) -> None:
            if hook is None or isinstance(exc, _quarantine.PASS_THROUGH):
                raise exc
            _quarantine.reraise_environment(exc)  # memory, modules, I/O: not data
            hook(label, _quarantine.error_class(exc), _quarantine.describe(exc))

        if hook is not None:
            short = _netcdf.classic_truncation(partition.file_path)
            if short is not None:
                hook(None, _quarantine.RECORDS_NOT_READ, short)
                return

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = _netcdf.select_variables(ds, self.options, "raster")

            for var in variables:
                try:
                    grid = self._grid_of(ds, var)
                except BaseException as exc:  # noqa: BLE001 - see _bad
                    _bad(f"variable={var}", exc)
                    continue
                if grid is None:
                    continue  # the bbox misses this variable's grid: no row
                transform, crs, nodata, var_ldims, window = grid

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

                    try:
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
                                window,
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
                                window,
                            )
                    except BaseException as exc:  # noqa: BLE001 - see _bad
                        _bad(
                            f"variable={var}"
                            + "".join(f",{d}={i}" for d, i in sorted(sel.items())),
                            exc,
                        )
                        continue

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
            return reader_schema_v2()
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

            _refuse_vector_bbox(self.options)
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

            _refuse_vector_bbox(self.options)
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
