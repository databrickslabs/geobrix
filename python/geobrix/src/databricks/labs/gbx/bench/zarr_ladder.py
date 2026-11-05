"""Size-ladder Zarr stores: the weather-features notebook's cube at county and global scale (``gbx:bench:gen-data --zarr-ladder``).

Every store is what ``databricks.labs.gbx.sample.zarr_era5.write_era5_cube`` writes (the cube of the
``zarr-weather-features`` notebook): Zarr v3, sharded, zstd, float32, hourly from 2023-01-01 with CF times, the four
variables ``t2m``, ``u10``, ``v10`` and ``msl`` over ``time x latitude x longitude`` (latitude from the north,
longitude from 0 east), with the notebook's attributes and rounding. Two families grow along time to the size of
each rung, which is the uncompressed logical size of the four variables, in decimal MB / GB (``geometry`` rounds the
hours to the nearest whole hour; ``generate`` reports the store's real logical and on-disk sizes):

* ``county_wind``: a 0.01 degree grid over a 1 x 1 degree county box (40N..39N, 255E..256E: 101 x 101). Chunks are 24 h x
  the whole box, shards 168 h.
* ``global_11km``: the 0.1 degree global grid (1801 x 3600). Chunks are 24 x 90 x 90, shards 168 x 360 x 360. One hour
  of the globe is 103.7 MB, so the rungs below that (10, 50 and 100 MB) are *regional cuts*: one hour of a window of
  the same grid with the globe's 1:2 shape, centred on 0N, 180E. From 500 MB the rungs are whole-globe hours.

The stores are streamed (``write_era5_cube`` writes one shard at a time), so no rung is held in memory.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Tuple

__all__ = [
    "FAMILIES",
    "RUNGS",
    "Geometry",
    "disk_bytes",
    "generate",
    "geometry",
    "parse_rung",
    "store_path",
    "write_skeleton",
]

RUNGS = ("10MB", "50MB", "100MB", "500MB", "1GB", "2GB", "5GB", "10GB")
VARIABLES = ("t2m", "u10", "v10", "msl")
# The ladder's values compress like real weather data (zstd 1.5 to 3 fold; the sample cube's 50 fold would make
# every read look cheaper than it is): more noise, rounded 20 times finer than the sample cube.
LADDER_NOISE = 1.0
LADDER_PRECISION = 0.05
_ITEM = 4  # float32
_UNITS = {"MB": 1e6, "GB": 1e9}


@dataclass(frozen=True)
class Family:
    name: str
    step: float
    box: Optional[
        Tuple[float, float, float, float]
    ]  # (lat_high, lat_low, lon_west, lon_east); None: the globe
    chunks: Tuple[int, int, int]
    shards: Tuple[int, int, int]

    @property
    def grid(self) -> Tuple[int, int]:
        """Rows and columns of the family's full extent."""
        if self.box is None:
            return round(180 / self.step) + 1, round(360 / self.step)
        hi, lo, west, east = self.box
        return round((hi - lo) / self.step) + 1, round((east - west) / self.step) + 1


FAMILIES: Dict[str, Family] = {
    "county_wind": Family(
        "county_wind", 0.01, (40.0, 39.0, 255.0, 256.0), (24, 101, 101), (168, 101, 101)
    ),
    "global_11km": Family("global_11km", 0.1, None, (24, 90, 90), (168, 360, 360)),
}


@dataclass(frozen=True)
class Geometry:
    """One rung's store: the hours and the window of the family's grid."""

    family: str
    rung: str
    hours: int
    box: Optional[
        Tuple[float, float, float, float]
    ]  # the window (lat_high, lat_low, lon_west, lon_east)
    ny: int
    nx: int

    @property
    def fam(self) -> Family:
        return FAMILIES[self.family]

    @property
    def shape(self) -> Tuple[int, int, int]:
        return (self.hours, self.ny, self.nx)

    @property
    def logical_bytes(self) -> int:
        return self.hours * self.ny * self.nx * _ITEM * len(VARIABLES)

    @property
    def regional(self) -> bool:
        return (self.ny, self.nx) != self.fam.grid

    @property
    def spec(self):
        from databricks.labs.gbx.sample.zarr_era5 import _Spec

        return _Spec(
            hours=self.hours,
            step=self.fam.step,
            box=self.box,
            chunks=self.fam.chunks,
            shards=self.fam.shards,
            variables=VARIABLES,
            noise=LADDER_NOISE,
            precision=LADDER_PRECISION,
        )

    def axes(self):
        """``(latitudes, longitudes)`` of the store."""
        return self.spec.axes()

    def n_chunks(self) -> int:
        """Inner chunks of the store, all variables."""
        return math.prod(-(-n // c) for n, c in zip(self.shape, self.fam.chunks)) * len(
            VARIABLES
        )

    def n_shards(self) -> int:
        return math.prod(-(-n // c) for n, c in zip(self.shape, self.fam.shards)) * len(
            VARIABLES
        )


def parse_rung(rung: str) -> float:
    """Bytes of a rung name such as ``100MB`` or ``1GB`` (decimal)."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(MB|GB)", rung.strip().upper())
    if not m:
        raise ValueError(
            f"rung {rung!r} is not <number>MB or <number>GB. Fix: use one of {', '.join(RUNGS)}."
        )
    return float(m.group(1)) * _UNITS[m.group(2)]


def geometry(family: str, rung: str) -> Geometry:
    """The store of ``rung`` for ``family`` (see the module doc)."""
    if family not in FAMILIES:
        raise ValueError(
            f"unknown family {family!r}. Fix: use one of {', '.join(FAMILIES)}."
        )
    from databricks.labs.gbx.sample.zarr_era5 import _Spec

    f = FAMILIES[family]
    target = parse_rung(rung)
    ny, nx = f.grid
    hour_bytes = ny * nx * _ITEM * len(VARIABLES)
    if target >= hour_bytes:
        return Geometry(family, rung, max(1, round(target / hour_bytes)), f.box, ny, nx)
    # below one hour of the extent: one hour of a centred window with the extent's shape
    cells = target / (_ITEM * len(VARIABLES))
    wy = max(1, min(ny, round(math.sqrt(cells * ny / nx))))
    wx = max(1, min(nx, round(cells / wy)))
    lat, lon = _Spec(1, f.step, f.box, f.chunks, f.shards, VARIABLES).axes()
    r0, c0 = (ny - wy) // 2, (nx - wx) // 2
    box = (
        float(lat[r0]),
        float(lat[r0 + wy - 1]),
        float(lon[c0]),
        float(lon[c0 + wx - 1]),
    )
    return Geometry(family, rung, 1, box, wy, wx)


def store_path(root: str, family: str, rung: str) -> str:
    return os.path.join(root, family, f"{rung}.zarr")


def write_skeleton(path: str, g: Geometry) -> None:
    """The store of ``g`` with its metadata and coordinates only (no chunk): enough to plan a read."""
    from databricks.labs.gbx.sample.zarr_era5 import _write_skeleton

    _write_skeleton(path, g.spec)


def disk_bytes(path: str) -> int:
    """Bytes of every file under ``path``."""
    return sum(
        os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs
    )


def generate(
    root: str,
    family: str,
    rung: str,
    *,
    workers: Optional[int] = None,
    overwrite: bool = False,
) -> dict:
    """Write the store of ``rung`` for ``family`` under ``root`` and return its record.

    A store that exists is kept unless ``overwrite``. The record has the path, shape, layout, the logical and
    on-disk bytes, the chunk and shard counts and the seconds taken.
    """
    from databricks.labs.gbx.sample.zarr_era5 import write_era5_cube

    g = geometry(family, rung)
    path = store_path(root, family, rung)
    t0 = time.perf_counter()
    kept = (
        os.path.exists(os.path.join(path, VARIABLES[0], "zarr.json")) and not overwrite
    )
    if not kept:
        write_era5_cube(
            os.path.join(root, family),
            hours=g.hours,
            step=g.fam.step,
            box=g.box,
            chunks=g.fam.chunks,
            shards=g.fam.shards,
            v3_name=f"{rung}.zarr",
            workers=workers,
            noise=LADDER_NOISE,
            precision=LADDER_PRECISION,
        )
    return {
        "family": family,
        "rung": rung,
        "path": path,
        "kept": kept,
        "shape": list(g.shape),
        "regional": g.regional,
        "variables": list(VARIABLES),
        "chunks": list(g.fam.chunks),
        "shards": list(g.fam.shards),
        "logical_bytes": g.logical_bytes,
        "disk_bytes": disk_bytes(path),
        "n_chunks": g.n_chunks(),
        "n_shards": g.n_shards(),
        "gen_s": round(time.perf_counter() - t0, 2),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Generate the Zarr size-ladder stores.")
    ap.add_argument("--out", required=True, help="root directory of the stores")
    ap.add_argument("--rungs", default=",".join(RUNGS), help="comma-separated rungs")
    ap.add_argument(
        "--families", default=",".join(FAMILIES), help="comma-separated families"
    )
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--overwrite", action="store_true")
    a = ap.parse_args(argv)
    records = []
    for family in [f for f in a.families.split(",") if f]:
        for rung in [r for r in a.rungs.split(",") if r]:
            r = generate(a.out, family, rung, workers=a.workers, overwrite=a.overwrite)
            records.append(r)
            print(
                f"[zarr-ladder] {family} {rung}: shape {tuple(r['shape'])}"
                f"{' (regional cut)' if r['regional'] else ''}, logical {r['logical_bytes'] / 1e6:.1f} MB, "
                f"disk {r['disk_bytes'] / 1e6:.1f} MB, {r['n_chunks']} chunks / {r['n_shards']} shards, "
                f"{'kept' if r['kept'] else str(r['gen_s']) + ' s'}",
                flush=True,
            )
    manifest = os.path.join(a.out, "ladder_manifest.json")
    old = {}
    if os.path.exists(manifest):
        with open(manifest) as fh:
            old = {(r["family"], r["rung"]): r for r in json.load(fh)}
    old.update({(r["family"], r["rung"]): r for r in records})
    with open(manifest, "w") as fh:
        json.dump(list(old.values()), fh, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
