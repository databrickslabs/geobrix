"""A small ERA5-like Zarr cube for the sample bundle (docs, doc tests and notebooks).

``write_era5_cube`` writes the same synthetic weather cube twice: as a sharded Zarr v3
store and as a Zarr v2 store. The cube holds two days of hourly fields on a 1 degree global
grid (``time`` 48 x ``latitude`` 181 x ``longitude`` 360): 2 m temperature ``t2m``, the 10 m
wind components ``u10`` and ``v10``, and mean sea level pressure ``msl``, all float32. The
data is made up (smooth fields plus seeded noise), not reanalysis output, and loads in
seconds.

The output is deterministic: a fixed seed, no timestamps in the metadata, and the same
bytes on every run in one environment (``tree_digest`` tells). The values are rounded to
a coarse step (0.25 K, 0.25 m/s, 10 Pa), as ERA5's own packed files are, which keeps both
stores to a few MB in all.

Both stores carry consolidated metadata (the root's ``consolidated_metadata`` block in v3, ``.zmetadata`` in v2),
as a vendor's ``to_zarr`` leaves them, so ``xarray.open_zarr(path)`` opens them without the "failed to open with
consolidated metadata" warning.

Layout of each store (the bundle holds both under ``global/era5-weather/``):

- ``era5_like_v3.zarr``: Zarr v3, inner chunks ``(12, 91, 180)``, shards ``(24, 182, 360)``
  (two shards per variable, eight inner chunks in each), zstd.
- ``era5_like_v2.zarr``: Zarr v2, chunks ``(12, 91, 180)``, blosc-zstd, chunk keys such as
  ``t2m/0.0.0``.

The generated cube is not tracked in git (gitignored in the bundle): write it with
``gbx:data:generate-minimal-bundle --zarr-only`` or call ``write_era5_cube`` yourself.

``write_era5_cube`` also writes other sizes of the same cube (``hours``, ``step``, ``box``, ``chunks``,
``shards``, ``variables``): the Zarr size ladder of the benchmarks uses it (``bench/zarr_ladder.py``). Such a store
is written block by block, never held in memory, so it can be as big as the disk (``era5_block`` makes a block; the
noise is drawn per variable and hour, so a block holds the same numbers whatever surrounds it). With no
argument the call is the one above, file for file.

Needs ``xarray`` and ``zarr`` (the ``zarr_dbr19`` or ``zarr_env6`` extra). The synthetic
fields come from ``databricks.labs.gbx.zarr._testing``, which ships in the wheel for this
module, the Zarr tests and the bench harness: keep ``era5_like_dataset`` and
``make_era5_like`` there (``test_zarr_era5`` fails if they go).
"""

from __future__ import annotations

import hashlib
import itertools
import os
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from databricks.labs.gbx.ds._scratch import new_scratch_dir, remove_scratch_dir
from databricks.labs.gbx.pyrx.core.local_temp import local_temp_root

#: Where the cube sits in the bundle, relative to ``geobrix-examples``.
BUNDLE_SUBDIR = "global/era5-weather"
V3_NAME = "era5_like_v3.zarr"
V2_NAME = "era5_like_v2.zarr"

HOURS = 48
STEP_DEGREES = 1.0
VARIABLES = ("t2m", "u10", "v10", "msl")
#: Scale of the seeded noise (1.0 is the test cube's). Small: the cube stays compressible.
NOISE = 0.02
#: The step each variable is rounded to; powers of two (and whole pascals) are exact in float32.
QUANTUM = {"t2m": 0.25, "u10": 0.25, "v10": 0.25, "msl": 10.0}
CHUNKS = (12, 91, 180)
# Whole multiples of CHUNKS; the latitude shard (182) is wider than the 181 rows.
SHARDS = (24, 182, 360)

_ATTRS = {
    "title": "GeoBrix sample ERA5-like cube",
    "summary": (
        "Synthetic hourly weather fields on a 1 degree global grid, made for the GeoBrix "
        "docs and notebooks. Not reanalysis data."
    ),
    "Conventions": "CF-1.8",
}


def _build_dataset():
    """The cube as an in-memory ``xarray.Dataset``: seeded, rounded to ``QUANTUM``."""
    import numpy as np

    from databricks.labs.gbx.zarr._testing import era5_like_dataset

    ds = era5_like_dataset(
        hours=HOURS, step=STEP_DEGREES, variables=VARIABLES, noise=NOISE
    )
    for name, step in QUANTUM.items():
        rounded = np.round(ds[name].values.astype("float64") / step) * step
        ds[name] = ds[name].copy(data=rounded.astype("float32"))
    ds.attrs.update(_ATTRS)
    return ds


def tree_digest(path) -> str:
    """SHA-256 over every file under ``path``: its relative path, size and bytes, in path order."""
    root = Path(path)
    digest = hashlib.sha256()
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(f.relative_to(root).as_posix().encode())
        digest.update(b"\0%d\0" % f.stat().st_size)
        with open(f, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                digest.update(block)
    return digest.hexdigest()


def _consolidate(path, zarr_format: int) -> None:
    """Consolidate the metadata of the finished store at ``path`` (the root's ``consolidated_metadata`` block in
    v3, ``.zmetadata`` in v2), as xarray's ``to_zarr`` does by default. Call it once every array exists: the copy
    is a snapshot of the documents."""
    import warnings

    import zarr

    with (
        warnings.catch_warnings()
    ):  # zarr warns that v3 consolidation is not in the spec
        warnings.simplefilter("ignore")
        zarr.consolidate_metadata(str(path), zarr_format=zarr_format)


def write_era5_cube(
    out_dir,
    *,
    hours: Optional[int] = None,
    step: Optional[float] = None,
    box: Optional[Tuple[float, float, float, float]] = None,
    chunks: Optional[Tuple[int, int, int]] = None,
    shards: Optional[Tuple[int, int, int]] = None,
    variables: Optional[Tuple[str, ...]] = None,
    v3_name: str = V3_NAME,
    v2: Optional[bool] = None,
    workers: Optional[int] = None,
    noise: Optional[float] = None,
    precision: Optional[float] = None,
) -> Dict[str, Path]:
    """Write the cube into ``out_dir`` and return ``{name: path}``.

    With no argument (the default) it writes both stores, ``{V3_NAME: path, V2_NAME: path}``: the sample
    cube, 48 hours on a 1 degree global grid. An existing store of the same name is replaced. The stores are
    built on local disk and copied file by file, so ``out_dir`` can be a Unity Catalog Volume (FUSE).

    Any of ``hours``, ``step``, ``box``, ``chunks``, ``shards`` or ``variables`` writes a different cube, as a
    Zarr v3 store named ``v3_name`` only (``v2`` must stay unset), with the same variables, attributes, CF time
    (``hours since 1900-01-01`` from 2023-01-01) and conventions (latitude from 90 down, longitude from 0 up):

    - ``hours``: the length of ``time`` (default 48);
    - ``step``: the grid step in degrees (default 1.0), so the global grid is ``round(180 / step) + 1`` rows by
      ``round(360 / step)`` columns;
    - ``box``: ``(lat_high, lat_low, lon_west, lon_east)``, inclusive, in the grid's own labels, to keep only the
      cells of that window (default: the globe);
    - ``chunks`` and ``shards`` (default ``CHUNKS``, ``SHARDS``): the layout, shards whole multiples of chunks;
    - ``variables`` (default all four);
    - ``workers``: processes that write shards in parallel (default: up to 8);
    - ``noise``: the scale of the seeded noise (default ``NOISE``, 0.02: the cube compresses about 50 fold);
    - ``precision``: the rounding step of each variable, as a multiple of ``QUANTUM`` (default 1.0; 0.05 rounds
      20 times finer). A larger ``noise`` and a smaller ``precision`` make a cube that compresses like real
      weather data, 1.5 to 3 fold with zstd, which a benchmark of reads needs; the default keeps the sample cube.

    The cube is streamed one shard at a time (a shard of every variable per task), so its size is limited by the
    disk, not memory. Its values are those of ``era5_block``.
    """
    custom = any(
        x is not None
        for x in (hours, step, box, chunks, shards, variables, noise, precision)
    )
    if custom or v3_name != V3_NAME:
        if v2:
            raise ValueError(
                "v2=True writes the sample cube's own Zarr v2 twin; a cube of another size or layout is Zarr v3 only"
            )
        spec = _Spec(
            hours=HOURS if hours is None else int(hours),
            step=STEP_DEGREES if step is None else float(step),
            box=None if box is None else tuple(float(b) for b in box),
            chunks=CHUNKS if chunks is None else tuple(int(c) for c in chunks),
            shards=SHARDS if shards is None else tuple(int(c) for c in shards),
            variables=VARIABLES if variables is None else tuple(variables),
            noise=NOISE if noise is None else float(noise),
            precision=1.0 if precision is None else float(precision),
        )
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        return {v3_name: write_era5_store(out / v3_name, spec, workers)}
    if v2 is False:
        raise ValueError("the sample cube is both stores; v2=False asks for half of it")
    return _write_sample_cube(out_dir)


def _write_sample_cube(out_dir) -> Dict[str, Path]:
    """The sample cube, both stores (see ``write_era5_cube``)."""
    import numcodecs

    from databricks.labs.gbx.zarr._testing import make_era5_like

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ds = _build_dataset()
    with tempfile.TemporaryDirectory(dir=local_temp_root()) as tmp:
        built = {
            V3_NAME: make_era5_like(
                Path(tmp) / V3_NAME,
                zarr_format=3,
                dataset=ds,
                chunks=CHUNKS,
                shards=SHARDS,
            ),
            V2_NAME: make_era5_like(
                Path(tmp) / V2_NAME,
                zarr_format=2,
                dataset=ds,
                chunks=CHUNKS,
                compressor=numcodecs.Blosc(cname="zstd", clevel=3),
            ),
        }
        _consolidate(built[V3_NAME], 3)
        _consolidate(built[V2_NAME], 2)
        written = {}
        for name, src in built.items():
            dest = out / name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest, copy_function=shutil.copyfile)
            written[name] = dest
    return written


# --- other sizes: streamed -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Spec:
    """One cube: its length, grid, window, layout and variables."""

    hours: int
    step: float
    box: Optional[Tuple[float, float, float, float]]
    chunks: Tuple[int, int, int]
    shards: Tuple[int, int, int]
    variables: Tuple[str, ...]
    noise: float = NOISE
    precision: float = 1.0

    def axes(self):
        """``(latitudes, longitudes)`` of the cube, the labels ``era5_like_dataset`` gives the full grid,
        cut to ``box``."""
        import numpy as np

        n_lat, n_lon = round(180 / self.step) + 1, round(360 / self.step)
        lat = np.linspace(90.0, -90.0, n_lat)
        lon = np.arange(n_lon) * self.step
        if self.box is not None:
            hi, lo, west, east = self.box
            tol = self.step * 1e-6
            lat = lat[(lat <= hi + tol) & (lat >= lo - tol)]
            lon = lon[(lon >= west - tol) & (lon <= east + tol)]
            if not lat.size or not lon.size:
                raise ValueError(
                    f"box {self.box} holds no cell of the {self.step} degree grid. "
                    "Fix: give (lat_high, lat_low, lon_west, lon_east) in the grid's labels."
                )
        return lat, lon

    @property
    def shape(self) -> Tuple[int, int, int]:
        lat, lon = self.axes()
        return (self.hours, lat.size, lon.size)


def era5_block(
    variables,
    t0,
    t1,
    latitude,
    longitude,
    *,
    noise: float = NOISE,
    precision: float = 1.0,
) -> Dict[str, "np.ndarray"]:  # noqa: F821
    """Hours ``[t0, t1)`` of ``variables`` over the given latitudes and longitudes: float32, rounded to ``QUANTUM``
    (times ``precision``) like the sample cube. ``{name: (t1 - t0, ny, nx)}``; the numbers of a block do not
    depend on its neighbours. With the default ``noise`` and ``precision`` they are the sample cube's.
    """
    import numpy as np

    from databricks.labs.gbx.zarr._testing import era5_like_block

    raw = era5_like_block(variables, t0, t1, latitude, longitude, noise=noise)
    step = {name: QUANTUM[name] * precision for name in raw}
    return {
        name: (np.round(a.astype("float64") / step[name]) * step[name]).astype(
            "float32"
        )
        for name, a in raw.items()
    }


def _write_skeleton(path: str, spec: _Spec) -> None:
    """The store's metadata and coordinates, and its variables empty (no chunk written)."""
    import pandas as pd
    import xarray as xr
    import zarr
    from zarr.codecs import ZstdCodec

    from databricks.labs.gbx.zarr._testing import _ATTRS as VAR_ATTRS
    from databricks.labs.gbx.zarr._testing import TIME_UNITS

    lat, lon = spec.axes()
    time = pd.date_range("2023-01-01", periods=spec.hours, freq="h")
    ds = xr.Dataset(
        coords={
            "time": ("time", time, {"long_name": "time"}),
            "latitude": (
                "latitude",
                lat,
                {"units": "degrees_north", "long_name": "latitude"},
            ),
            "longitude": (
                "longitude",
                lon,
                {"units": "degrees_east", "long_name": "longitude"},
            ),
        },
        attrs=_ATTRS,
    )
    ds.to_zarr(
        path,
        mode="w",
        zarr_format=3,
        consolidated=False,
        encoding={
            "time": {"units": TIME_UNITS, "calendar": "standard", "dtype": "int64"},
            "latitude": {"_FillValue": None},
            "longitude": {"_FillValue": None},
        },
    )
    group = zarr.open_group(path, mode="a")
    for name in spec.variables:
        group.create_array(
            name,
            shape=spec.shape,
            chunks=spec.chunks,
            shards=spec.shards,
            dtype="float32",
            fill_value=float("nan"),
            dimension_names=("time", "latitude", "longitude"),
            compressors=ZstdCodec(level=3),
            attributes=dict(VAR_ATTRS[name]),
        )


def _write_shard(job) -> int:
    """Make and write one shard of every variable (a task of the pool); returns its cells."""
    import numpy as np
    import xarray as xr

    path, spec, idx = job
    shape = spec.shape
    sl = tuple(
        slice(i * c, min((i + 1) * c, n)) for i, c, n in zip(idx, spec.shards, shape)
    )
    lat, lon = spec.axes()
    block = era5_block(
        spec.variables,
        sl[0].start,
        sl[0].stop,
        lat[sl[1]],
        lon[sl[2]],
        noise=spec.noise,
        precision=spec.precision,
    )
    dims = ("time", "latitude", "longitude")
    xr.Dataset({name: (dims, a) for name, a in block.items()}).to_zarr(
        path,
        region={"time": sl[0], "latitude": sl[1], "longitude": sl[2]},
        mode="r+",
        consolidated=False,
    )
    return int(np.prod([x.stop - x.start for x in sl]))


def write_era5_store(path, spec: _Spec, workers: Optional[int] = None) -> Path:
    """Write ``spec``'s cube at ``path`` shard by shard, then move it into place; an existing store is replaced.

    Shards are separate files, so tasks that follow the shard grid never meet. On a Unity Catalog Volume (FUSE)
    the store is built on local disk and copied file by file; elsewhere beside ``path`` and renamed.
    """
    dest = Path(path)
    fuse = str(dest).startswith("/Volumes/")
    work = (
        Path(new_scratch_dir(local_temp_root())) / dest.name
        if fuse
        else dest.with_name(dest.name + ".partial")
    )
    shutil.rmtree(work, ignore_errors=True)
    try:
        _write_skeleton(str(work), spec)
        grid = [-(-n // c) for n, c in zip(spec.shape, spec.shards)]
        jobs = [
            (str(work), spec, idx)
            for idx in itertools.product(*(range(k) for k in grid))
        ]
        n = workers or min(os.cpu_count() or 1, 8)
        if n > 1 and len(jobs) > 1:
            with ProcessPoolExecutor(n) as pool:
                cells = sum(pool.map(_write_shard, jobs, chunksize=1))
        else:
            cells = sum(_write_shard(j) for j in jobs)
        if cells != spec.hours * spec.shape[1] * spec.shape[2]:
            raise RuntimeError(
                f"wrote {cells} cells, expected {spec.hours * spec.shape[1] * spec.shape[2]}"
            )
        _consolidate(work, 3)  # last: every array and shard exists
        if dest.exists():
            shutil.rmtree(dest)
        if fuse:
            shutil.copytree(work, dest, copy_function=shutil.copyfile)
        else:
            os.rename(work, dest)
    finally:
        if fuse:
            remove_scratch_dir(str(work.parent))
        else:
            shutil.rmtree(work, ignore_errors=True)
    return dest
