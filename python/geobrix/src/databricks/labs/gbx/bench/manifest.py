"""Corpus manifest model: tiles + scale metadata, JSON (de)serialized."""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class TileEntry:
    path: str  # path relative to the corpus root
    cellid: int
    srid: int
    dtype: str  # "uint8" | "int16" | "float32"
    bands: int
    tile_px: int  # square tile edge in pixels
    nodata_frac: float
    # Which functions consume this tile in the pure-core sweep. Default "sweep":
    # the ordinary size-sweep tile every function benchmarks. "bng_gb": a tile
    # whose extent overlaps Great Britain when warped to EPSG:27700 -- consumed
    # ONLY by the BNG raster->grid / tessellate functions (which reproject to
    # 27700 internally and would otherwise bin an EMPTY grid on the NYC-ish sweep
    # tiles), and SKIPPED by every other function (so their tile selection is
    # unchanged). Both runners route on this so the two tiers stay symmetric.
    role: str = "sweep"


@dataclass(frozen=True)
class RowPool:
    tile_px: int
    bands: int
    dtype: str
    tiles: List[TileEntry]  # ordered; runner takes the first N for each row count


@dataclass(frozen=True)
class Corpus:
    seed: int
    size_sweep: List[
        TileEntry
    ]  # one tile per (tile_px, bands, dtype, srid, nodata_frac) point
    row_pool: RowPool  # tiles for the spark-path row-count sweep

    def write(self, path) -> None:
        Path(path).write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def read(cls, path) -> "Corpus":
        d = json.loads(Path(path).read_text())
        return cls(
            seed=d["seed"],
            size_sweep=[TileEntry(**t) for t in d["size_sweep"]],
            row_pool=RowPool(
                tile_px=d["row_pool"]["tile_px"],
                bands=d["row_pool"]["bands"],
                dtype=d["row_pool"]["dtype"],
                tiles=[TileEntry(**t) for t in d["row_pool"]["tiles"]],
            ),
        )


# --- geometry corpus --------------------------------------------------------
# Geometry-input raster functions (rst_clip / rst_rasterize / rst_dtmfromgeoms /
# the geometry aggregators) need a deterministic, CRS-correct geometry set that
# BOTH benchmark engines read identically. WKB carries no CRS, so the srid is
# recorded on the set; geometry coordinates are in that CRS. WKB bytes are
# base64-encoded for JSON transport (JSON has no byte type), so the manifest
# round-trips byte-identically across the heavy/light process boundary.


def _enc(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _dec(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


@dataclass(frozen=True)
class GeometrySet:
    """One tile's geometry corpus: boxes + points (with burn values) + z-points.

    ``boxes`` / ``points`` are lists of ``(wkb_bytes, value)``; ``zpoints`` are
    bare WKB bytes of 3-D points (Z sampled from the source tile). All geometry
    is in the tile's CRS (``srid``); ``source_tile`` is the corpus-relative path
    of the tile the geometry was derived from.
    """

    srid: int
    source_tile: str
    boxes: List[Tuple[bytes, float]]
    points: List[Tuple[bytes, float]]
    zpoints: List[bytes]

    def to_json(self) -> dict:
        return {
            "srid": self.srid,
            "source_tile": self.source_tile,
            "boxes": [[_enc(wkb), v] for wkb, v in self.boxes],
            "points": [[_enc(wkb), v] for wkb, v in self.points],
            "zpoints": [_enc(wkb) for wkb in self.zpoints],
        }

    @classmethod
    def from_json(cls, d: dict) -> "GeometrySet":
        return cls(
            srid=d["srid"],
            source_tile=d["source_tile"],
            boxes=[(_dec(s), float(v)) for s, v in d["boxes"]],
            points=[(_dec(s), float(v)) for s, v in d["points"]],
            zpoints=[_dec(s) for s in d["zpoints"]],
        )


@dataclass(frozen=True)
class GeometryCorpus:
    """Container of named ``GeometrySet``s persisted as ``geometry.json``.

    ``source_tile`` / ``srid`` are the manifest-level provenance (the
    representative tile + CRS); per-set ``GeometrySet`` carries its own copy so a
    consumer can read a single set without the container.
    """

    seed: int
    srid: int
    source_tile: str
    sets: Dict[str, GeometrySet] = field(default_factory=dict)

    def write(self, path) -> None:
        d = {
            "seed": self.seed,
            "srid": self.srid,
            "source_tile": self.source_tile,
            "sets": {k: v.to_json() for k, v in self.sets.items()},
        }
        Path(path).write_text(json.dumps(d, indent=2))

    @classmethod
    def read(cls, path) -> "GeometryCorpus":
        d = json.loads(Path(path).read_text())
        return cls(
            seed=d["seed"],
            srid=d["srid"],
            source_tile=d["source_tile"],
            sets={k: GeometrySet.from_json(v) for k, v in d["sets"].items()},
        )
