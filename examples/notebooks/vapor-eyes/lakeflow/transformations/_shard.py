"""Pure tile-sharding math for the PMTiles fanout. No Spark, no side effects.

A shard is the ancestor XYZ tile at `shard_zoom` that contains a given
`(z, x, y)` tile. Grouping the MVT pyramid by shard key partitions the archive
into bounded per-shard PMTiles files (binary-free fanout — no `tile-join`).
`shard_bounds` maps a shard key back to its WGS84 lon/lat bounding box via the
standard slippy-map (Web-Mercator) tile math, for a spatial catalog of shards.
"""
import math


def tile_shard(z: int, x: int, y: int, shard_zoom: int) -> str:
    """Shard key = the ancestor tile at `shard_zoom` that contains (z, x, y).

    For z >= shard_zoom, right-shift the tile coords by the zoom delta to reach
    the ancestor: ``f"{shard_zoom}/{x >> (z - shard_zoom)}/{y >> (z - shard_zoom)}"``.
    For z < shard_zoom (coarser than a shard), the tile is its own shard key.
    """
    if z >= shard_zoom:
        delta = z - shard_zoom
        return f"{shard_zoom}/{x >> delta}/{y >> delta}"
    return f"{z}/{x}/{y}"


def _tile_nw_lonlat(z: int, x: int, y: int) -> tuple[float, float]:
    """North-west (top-left) corner lon/lat of XYZ tile (z, x, y)."""
    n = 2.0 ** z
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n))))
    return lon, lat


def shard_bounds(shard_key: str) -> tuple[float, float, float, float]:
    """WGS84 bbox (min_lon, min_lat, max_lon, max_lat) covered by a shard key.

    XYZ y increases southward, so tile row y is the NORTH edge and y+1 is the
    SOUTH edge; lon increases eastward with x.
    """
    z, x, y = (int(t) for t in shard_key.split("/"))
    west, north = _tile_nw_lonlat(z, x, y)
    east, south = _tile_nw_lonlat(z, x + 1, y + 1)
    return (west, south, east, north)
