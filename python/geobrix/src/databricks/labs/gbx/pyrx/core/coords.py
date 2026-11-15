"""Spark-free pixel <-> world coordinate transforms.

rasterio's DatasetReader.xy(row, col) returns the world coordinate of a pixel
center; index(x, y) returns (row, col). Note rasterio orders by (row, col)
while the GeoBrix API orders args (pixel_x=col, pixel_y=row). A NULL coordinate
gives NULL, as the JVM forms do.
"""

from typing import Optional


def raster_to_world_x(ds, pixel_x: int, pixel_y: int) -> Optional[float]:
    if pixel_x is None or pixel_y is None:
        return None
    x, _ = ds.xy(int(pixel_y), int(pixel_x))
    return float(x)


def raster_to_world_y(ds, pixel_x: int, pixel_y: int) -> Optional[float]:
    if pixel_x is None or pixel_y is None:
        return None
    _, y = ds.xy(int(pixel_y), int(pixel_x))
    return float(y)


def world_to_raster_x(ds, world_x: float, world_y: float) -> Optional[int]:
    if world_x is None or world_y is None:
        return None
    row, col = ds.index(float(world_x), float(world_y))
    return int(col)


def world_to_raster_y(ds, world_x: float, world_y: float) -> Optional[int]:
    if world_x is None or world_y is None:
        return None
    row, col = ds.index(float(world_x), float(world_y))
    return int(row)


def raster_to_world_coord(ds, pixel_x: int, pixel_y: int) -> Optional[dict]:
    """World coordinate of pixel (pixel_x=col, pixel_y=row) as {x, y} (doubles)."""
    if pixel_x is None or pixel_y is None:
        return None
    x, y = ds.xy(int(pixel_y), int(pixel_x))
    return {"x": float(x), "y": float(y)}


def world_to_raster_coord(ds, world_x: float, world_y: float) -> Optional[dict]:
    """Pixel (col, row) containing world (world_x, world_y) as {x, y} (ints)."""
    if world_x is None or world_y is None:
        return None
    row, col = ds.index(float(world_x), float(world_y))
    return {"x": int(col), "y": int(row)}
