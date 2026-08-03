"""Tile struct schema and rasterio MemoryFile (de)serialization.

Both the lightweight (pyrx) and heavyweight (rasterx) tiers share the v2 tile
struct defined in ``pyrx.core.virtual_tile.V2_TILE_SCHEMA``:

    struct<cellid: bigint, raster: binary, path: string,
           window: struct<col_off,row_off,width,height>,
           clip_polygon: binary, clip_crs: string, crs: string,
           metadata: map<string,string>>

A materialized tile carries raster bytes (``raster`` is not null); the provenance
fields (``path``, ``window``, ``clip_polygon``, ``clip_crs``, ``crs``) are null for
a plain materialized tile.  The legacy ``TILE_SCHEMA`` below covers only the
three-field subset used by older build_tile / open_tile helpers.
"""

from contextlib import contextmanager
from typing import Dict, Iterator

from pyspark.sql.types import (
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rasterio.io import DatasetReader, MemoryFile

TILE_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=False),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


@contextmanager
def open_tile(raster_bytes: bytes) -> Iterator[DatasetReader]:
    """Open raster BINARY content as a rasterio DatasetReader (in-memory)."""
    with MemoryFile(bytes(raster_bytes)) as mf:
        with mf.open() as ds:
            yield ds


def build_tile(raster_bytes: bytes, driver: str, cellid: int = 0) -> Dict:
    """Construct a tile struct dict from raster BINARY content."""
    raster = bytes(raster_bytes)
    with open_tile(raster) as ds:
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
    return {"cellid": int(cellid), "raster": raster, "metadata": meta}
