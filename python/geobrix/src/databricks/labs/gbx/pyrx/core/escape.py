"""Python-only escape-hatches for users whose needs fall outside the rst_* surface.

NOT SQL-registered (tile_to_numpy returns a host object; rst_apply takes a Python
callable), so neither appears in registered_functions.txt / function-info.json.
"""

from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import DataType, DoubleType

from databricks.labs.gbx.pyrx._udf import _col


def tile_to_numpy(tile_or_bytes):
    """Read a tile's raster into a numpy ndarray (all bands).

    Accepts a tile struct (Row/dict, v1 or v2), a virtual tile (path+window),
    or raw bytes. Virtual tiles read their window (pending instructions applied).
    """
    from databricks.labs.gbx.pyrx.core import open_tile as _ot

    with _ot.open_tile(_ot._to_virtual_tile(tile_or_bytes)) as ds:
        return ds.read()


def rst_apply(tile_col, fn, returnType: DataType = DoubleType()) -> Column:
    """Apply an arbitrary rasterio function to each tile, returning one scalar/row.

    fn receives an open rasterio DatasetReader and returns a value of returnType
    (default DoubleType; any Spark DataType). The escape-hatch for "GeoBrix lacks
    function X — run your own rasterio per tile". Scalar return only. Null/empty
    tile -> null.
    """

    @udf(returnType=returnType)
    def _apply(tile):
        from databricks.labs.gbx.pyrx.core import open_tile as _ot

        # empty (no raster AND no path) -> null; else open (virtual reads window)
        if tile is None:
            return None
        d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
        if d.get("raster") is None and d.get("path") is None:
            return None
        with _ot.open_tile(_ot._to_virtual_tile(tile)) as ds:
            return fn(ds)

    return _apply(_col(tile_col))
