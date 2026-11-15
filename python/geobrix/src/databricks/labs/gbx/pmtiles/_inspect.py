"""Driver-side PMTiles inspector. Spark-side PMTiles read is unsupported, so a
local-driver header reader is broadly useful and is consumed by the gbx.vizx
viewers (type detection + static fallback). Uses the existing `pmtiles` dep."""

from __future__ import annotations

from typing import Union

from pmtiles.reader import MemorySource, Reader
from pmtiles.tile import Compression, TileType

from databricks.labs.gbx.pmtiles._directory import iter_tiles

# TileType / Compression enum -> the lowercase string keys the viewers branch on.
_TILE_TYPE_NAME = {
    TileType.MVT: "mvt",
    TileType.PNG: "png",
    TileType.JPEG: "jpeg",
    TileType.WEBP: "webp",
    TileType.AVIF: "avif",
    TileType.UNKNOWN: "unknown",
}
_COMPRESSION_NAME = {
    Compression.UNKNOWN: "unknown",
    Compression.NONE: "none",
    Compression.GZIP: "gzip",
    Compression.BROTLI: "brotli",
    Compression.ZSTD: "zstd",
}


def _strip_scheme(path: str) -> str:
    for scheme in ("dbfs:", "file:"):
        if path.startswith(scheme):
            path = path[len(scheme) :]
            break
    if path.startswith("//"):
        path = "/" + path.lstrip("/")
    return path


def _read_bytes(path_or_bytes: Union[str, bytes, bytearray]) -> bytes:
    if isinstance(path_or_bytes, (bytes, bytearray)):
        return bytes(path_or_bytes)
    with open(_strip_scheme(str(path_or_bytes)), "rb") as f:
        return f.read()


def pmtiles_info(path: Union[str, bytes, bytearray]) -> dict:
    """Parse a .pmtiles archive header into a plain dict.

    ``path`` is a filesystem path (Volume/DBFS scheme prefixes are stripped) or
    the archive bytes. Returns ``tile_type`` (lowercase string), ``min_zoom`` /
    ``max_zoom`` (int), ``bounds`` (min_lon, min_lat, max_lon, max_lat degrees),
    ``center`` (lon, lat, zoom), ``tile_count`` (int), ``metadata`` (dict), and
    ``tile_compression`` (lowercase string). Driver-side only.
    """
    data = _read_bytes(path)
    reader = Reader(MemorySource(data))
    h = reader.header()
    metadata = reader.metadata()
    # iter_tiles, not the library's all_tiles: it reads the uncompressed
    # directories the heavyweight writer stores, which all_tiles gunzips.
    tile_count = sum(1 for _ in iter_tiles(data))
    return {
        "tile_type": _TILE_TYPE_NAME.get(h["tile_type"], "unknown"),
        "tile_compression": _COMPRESSION_NAME.get(h["tile_compression"], "unknown"),
        "min_zoom": int(h["min_zoom"]),
        "max_zoom": int(h["max_zoom"]),
        "bounds": (
            h["min_lon_e7"] / 1e7,
            h["min_lat_e7"] / 1e7,
            h["max_lon_e7"] / 1e7,
            h["max_lat_e7"] / 1e7,
        ),
        "center": (
            h["center_lon_e7"] / 1e7,
            h["center_lat_e7"] / 1e7,
            int(h["center_zoom"]),
        ),
        "tile_count": int(tile_count),
        "metadata": dict(metadata) if metadata else {},
    }
