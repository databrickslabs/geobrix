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

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from pyspark.sql.types import (
    BinaryType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rasterio.io import DatasetReader, MemoryFile

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

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
        with open_bytes(mf) as ds:
            yield ds


def build_error_tile(last_error: str, cellid: int = -1) -> Dict:
    """Empty/error tile: raster None (and no path) + last_error in metadata.

    Signals a swallowed failure that stays diagnosable. Mirrors heavy's
    RST_ErrorHandler error-tile row (raster NULL + errorMetadata).
    """
    # Every V2_TILE_SCHEMA field: a UDTF row must supply each column (a
    # struct-returning UDF tolerates missing keys, a UDTF does not).
    row: Dict = {f.name: None for f in V2_TILE_SCHEMA.fields}
    row.update(cellid=int(cellid), metadata={"last_error": last_error})
    return row


def build_tile(
    raster_bytes: bytes,
    driver: str,
    cellid: int = 0,
    grid_system: Optional[str] = None,
    window=None,
    extra_metadata: Optional[Dict[str, str]] = None,
) -> Dict:
    """Construct a **v2-materialized** tile struct dict from raster BINARY content.

    Opens the raster to record driver/width/height/count in ``metadata`` and
    returns the 9-field ``V2_TILE_SCHEMA`` shape with ``raster`` set and every
    provenance field (``path``/``window``/``clip_polygon``/``clip_crs``/``crs``/
    ``path_mode``) NULL — the canonical materialized tile. Nothing in the light
    tier emits the legacy 3-field struct anymore.

    When *grid_system* is not None, ``metadata["gridSystem"]`` is set to the
    given string so consumers can disambiguate the DGGS a ``cellid`` belongs to.
    The tessellate UDTFs pass ``"h3"``, ``"quadbin"``, or ``"bng"``; other callers
    (e.g. rasterize_agg) omit it, leaving the key absent.

    A tile cut out of a parent records where it came from. *window* is the
    ``(col_off, row_off, width, height)`` pixel window within the parent (the
    retile family passes it); *extra_metadata* adds ``metadata`` entries (e.g. the
    1-based ``band`` that ``rst_separatebands`` stamps). Both default to absent.
    """
    raster = bytes(raster_bytes)
    with open_tile(raster) as ds:
        meta = {
            "driver": driver or ds.driver,
            "width": str(ds.width),
            "height": str(ds.height),
            "count": str(ds.count),
        }
    if grid_system is not None:
        meta["gridSystem"] = grid_system
    if extra_metadata:
        meta.update(extra_metadata)
    return VirtualTile(
        cellid=int(cellid), raster=raster, window=window, metadata=meta
    ).to_row()


def band_marker(tile):
    """The ``band`` marker of *tile* (the 1-based band of its parent that
    ``rst_separatebands`` records), or None when it has none."""
    from databricks.labs.gbx.pyrx.core.open_tile import _to_virtual_tile
    from databricks.labs.gbx.pyrx.core.virtual_tile import BAND_METADATA_KEY

    return (_to_virtual_tile(tile).metadata or {}).get(BAND_METADATA_KEY)


def inherit_provenance(row: Dict, tile) -> Dict:
    """Give *row* the provenance of the input *tile* it reproduces.

    Used for the one output of a retile-family function that covers its whole
    input: it IS the input, so it keeps the input's ``path``, ``window``,
    ``clip_polygon``/``clip_crs`` and ``band`` marker rather than a fresh window
    (which would read as "the first piece of its parent" and collide with the
    other parents' first pieces). Mutates and returns *row*.
    """
    from databricks.labs.gbx.pyrx.core.open_tile import _to_virtual_tile
    from databricks.labs.gbx.pyrx.core.virtual_tile import BAND_METADATA_KEY

    vt = _to_virtual_tile(tile)
    # Not ``vt.to_row()``: it casts cellid to int, and the input's cellid may be null.
    row["path"] = vt.path
    row["window"] = (
        None
        if vt.window is None
        else dict(zip(("col_off", "row_off", "width", "height"), vt.window))
    )
    row["clip_polygon"] = vt.clip_polygon
    row["clip_crs"] = vt.clip_crs
    band = (vt.metadata or {}).get(BAND_METADATA_KEY)
    if band is not None:
        row["metadata"][BAND_METADATA_KEY] = band
    return row
