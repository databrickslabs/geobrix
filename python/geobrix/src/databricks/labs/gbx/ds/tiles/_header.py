"""PMTiles header assembly + tile-type sniffing from magic bytes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable

from databricks.labs.gbx.ds.tiles.grid import BBox, Grid, TileKey

if TYPE_CHECKING:
    # Runtime uses are function-local (pmtiles is an optional dep); these imports
    # exist only so static analysis resolves TileType/Compression in the module-level
    # annotations below. `from __future__ import annotations` keeps them lazy at runtime.
    from pmtiles.tile import Compression, TileType


def sniff_tile_type(data: bytes) -> TileType:
    """Detect tile encoding from magic bytes; default MVT for vector payloads."""
    from pmtiles.tile import TileType

    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return TileType.PNG
    if data[:3] == b"\xff\xd8\xff":
        return TileType.JPEG
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return TileType.WEBP
    if data[4:8] == b"ftyp" and b"avif" in data[8:20]:
        return TileType.AVIF
    # MVT is protobuf (often gzipped) with no reliable magic.
    return TileType.MVT


def _e7(v: float) -> int:
    return int(round(v * 1e7))


@dataclass
class HeaderInfo:
    tile_type: TileType
    tile_compression: Compression
    min_zoom: int
    max_zoom: int
    bbox: BBox
    metadata: Dict[str, object]

    def header_dict(self) -> Dict[str, object]:
        from pmtiles.tile import Compression

        minlon, minlat, maxlon, maxlat = self.bbox
        clon = (minlon + maxlon) / 2.0
        clat = (minlat + maxlat) / 2.0
        return {
            "tile_type": self.tile_type,
            "tile_compression": self.tile_compression,
            # Gzip the root/leaf directories + metadata explicitly. The PMTiles
            # spec stores directories gzipped and readers gzip-decompress them
            # unconditionally; relying on the pmtiles Writer's default is
            # version-dependent (some versions default to NONE, producing an
            # archive the reader can't parse — "Not a gzipped file").
            "internal_compression": Compression.GZIP,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "min_lon_e7": _e7(minlon),
            "min_lat_e7": _e7(minlat),
            "max_lon_e7": _e7(maxlon),
            "max_lat_e7": _e7(maxlat),
            "center_zoom": self.min_zoom,
            "center_lon_e7": _e7(clon),
            "center_lat_e7": _e7(clat),
        }


def build_header_info(
    tiles: Iterable[TileKey],
    grid: Grid,
    tile_type: TileType,
    tile_compression: Compression,
    metadata: Dict[str, object],
) -> HeaderInfo:
    """Compute min/max zoom + bbox over a set of (z,x,y) tiles.

    The bbox is framed by the FINEST-zoom (max-zoom) tiles only, not the union
    across all zooms. A coarse tile is large (a z6 web-mercator tile spans
    ~5.6 deg), so unioning it with its fine descendants balloons the extent well
    beyond the data -- which then makes an interactive viewer's fitBounds open
    far off the data (data appears offset and drifts as you zoom). The
    highest-zoom tiles bound the same data most tightly.
    """
    tiles = list(tiles)
    if not tiles:
        raise ValueError("build_header_info requires at least one tile")
    zs = [z for z, _, _ in tiles]
    zmax = max(zs)
    minlon = minlat = float("inf")
    maxlon = maxlat = float("-inf")
    for z, x, y in tiles:
        if z != zmax:
            continue
        bb = grid.tile_bbox(z, x, y)
        minlon, minlat = min(minlon, bb[0]), min(minlat, bb[1])
        maxlon, maxlat = max(maxlon, bb[2]), max(maxlat, bb[3])
    return HeaderInfo(
        tile_type=tile_type,
        tile_compression=tile_compression,
        min_zoom=min(zs),
        max_zoom=max(zs),
        bbox=(minlon, minlat, maxlon, maxlat),
        metadata=metadata,
    )
