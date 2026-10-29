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

Layout of each store (the bundle holds both under ``global/era5-weather/``):

- ``era5_like_v3.zarr``: Zarr v3, inner chunks ``(12, 91, 180)``, shards ``(24, 182, 360)``
  (two shards per variable, eight inner chunks in each), zstd.
- ``era5_like_v2.zarr``: Zarr v2, chunks ``(12, 91, 180)``, blosc-zstd, chunk keys such as
  ``t2m/0.0.0``.

The generated cube is not tracked in git (gitignored in the bundle): write it with
``gbx:data:generate-minimal-bundle --zarr-only`` or call ``write_era5_cube`` yourself.

Needs ``xarray`` and ``zarr`` (the ``zarr_dbr19`` or ``zarr_env6`` extra). The synthetic
fields come from ``databricks.labs.gbx.zarr._testing``, which ships in the wheel for this
module, the Zarr tests and the bench harness: keep ``era5_like_dataset`` and
``make_era5_like`` there (``test_zarr_era5`` fails if they go).
"""

from __future__ import annotations

import hashlib
import shutil
import tempfile
from pathlib import Path
from typing import Dict

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


def write_era5_cube(out_dir) -> Dict[str, Path]:
    """Write both stores into ``out_dir`` and return ``{V3_NAME: path, V2_NAME: path}``.

    An existing store of the same name is replaced. The stores are built on local disk and
    copied file by file, so ``out_dir`` can be a Unity Catalog Volume (FUSE).
    """
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
        written = {}
        for name, src in built.items():
            dest = out / name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(src, dest, copy_function=shutil.copyfile)
            written[name] = dest
    return written
