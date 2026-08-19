"""Tile struct schema and rasterio MemoryFile (de)serialization.

Both the lightweight (pyrx) and heavyweight (rasterx) tiers share the v2 tile
struct defined in ``pyrx.core.virtual_tile.V2_TILE_SCHEMA``:

    struct<cellid: bigint, raster: binary, path: string,
           path_mode: string, window: struct<col_off,row_off,width,height>,
           clip_polygon: binary, clip_crs: string, crs: string,
           metadata: map<string,string>>

A materialized tile carries raster bytes (``raster`` is not null); the provenance
fields (``path``, ``window``, ``clip_polygon``, ``clip_crs``, ``crs``) are null for
a plain materialized tile. The ``build_tile`` helper now emits the full v2 struct.

``TILE_SCHEMA`` below is the legacy 3-field struct, retained **only as a
read/load INPUT schema** for the reader/writer v1 path (GeoBrix reads/loads v1 OR
v2 tiles, and OUTPUTS only v2). No tile-returning function emits it — outputs are
always ``V2_TILE_SCHEMA`` (enforced by G1, test_v2_tile_output_invariant.py).
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

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# Legacy 3-field tile struct — INPUT-ONLY (reader/writer v1 read/load path).
# NEVER a function output schema: every tile-returning UDF/UDTF emits
# V2_TILE_SCHEMA. See ds/raster.py::reader_schema (v1) vs reader_schema_v2.
TILE_SCHEMA = StructType(
    [
        StructField("cellid", LongType(), nullable=False),
        StructField("raster", BinaryType(), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


@contextmanager
def open_tile(raster_bytes: bytes) -> Iterator[DatasetReader]:
    """Open raster BINARY content as a rasterio DatasetReader (in-memory)."""
    with MemoryFile(bytes(raster_bytes)) as mf:
        with mf.open() as ds:
            yield ds


def build_error_tile(last_error: str, cellid: int = -1) -> Dict:
    """Empty/error tile: raster None (and no path) + last_error in metadata.

    Signals a swallowed failure that stays diagnosable. Mirrors heavy's
    RST_ErrorHandler error-tile row (raster NULL + errorMetadata).
    """
    return {
        "cellid": int(cellid),
        "raster": None,
        "metadata": {"last_error": last_error},
        "path_mode": None,
    }


def build_tile(raster_bytes: bytes, driver: str, cellid: int = 0) -> Dict:
    """Construct a **v2-materialized** tile struct dict from raster BINARY content.

    Opens the raster to record driver/width/height/count in ``metadata`` and
    returns the 9-field ``V2_TILE_SCHEMA`` shape with ``raster`` set and every
    provenance field (``path``/``window``/``clip_polygon``/``clip_crs``/``crs``/
    ``path_mode``) NULL — the canonical materialized tile. Nothing in the light
    tier emits the legacy 3-field struct anymore.
    """
    raster = bytes(raster_bytes)
    with open_tile(raster) as ds:
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
    return VirtualTile(cellid=int(cellid), raster=raster, metadata=meta).to_row()
