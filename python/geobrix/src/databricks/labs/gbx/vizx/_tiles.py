"""Render raster tiles from a Spark DataFrame; resolve virtual/materialized payloads.

The tile->pixels boundary for VizX. Delegates opening to pyrx.core.open_tile so
virtual (path+window) tiles read their window and apply pending nodata/srid/bands
instructions, and v1/v2/bytes shapes all normalize identically — no reimplementation.
"""

from contextlib import contextmanager


def _extract_tile(tile_or_row, tile_col):
    """Pull the tile value out of a Row/dict that has a tile_col, else pass through."""
    # bytes / VirtualTile / a tile-struct dict|Row -> use as-is; a wrapper Row with
    # a tile_col field -> extract that field.
    if isinstance(tile_or_row, (bytes, bytearray)):
        return tile_or_row
    # dict/Row: if it has the tile_col AND that looks like a tile (struct), extract.
    d = None
    if hasattr(tile_or_row, "asDict"):
        d = tile_or_row.asDict()
    elif isinstance(tile_or_row, dict):
        d = tile_or_row
    if d is not None and tile_col in d and not ("raster" in d or "path" in d):
        return d[tile_col]
    return tile_or_row


@contextmanager
def resolve_tile_row(tile_or_row, tile_col="tile"):
    """Yield an open rasterio DatasetReader for a tile from any shape.

    Accepts a Spark Row (with ``tile_col``), a tile struct (dict/Row), a
    ``VirtualTile``, or raw GeoTIFF bytes. Virtual tiles are read via
    ``pyrx.core.open_tile`` (window + pending instructions applied); materialized
    tiles and bytes open directly. v1 (3-field) and v2 (8-field) both supported.
    """
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    tile = _extract_tile(tile_or_row, tile_col)
    with _ot.open_tile(_ot._to_virtual_tile(tile)) as ds:
        yield ds
