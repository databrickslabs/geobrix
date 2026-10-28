"""MVP-G probe forms: other ways to ask for the same rows (names ``gbx_mvp_*``, never documented).

The perf matrix times each shape of a generator, and the oracle holds every shape to the same rows.
The scale-out keeps the shapes that win and deletes the rest.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import hashlib
from collections import OrderedDict

from pyspark.sql import functions as f
from pyspark.sql.functions import udtf
from pyspark.sql.types import ArrayType, IntegerType

from databricks.labs.gbx.pygx.functions import _BngTessellateExplode
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
from databricks.labs.gbx.pyrx.functions import _cut_tile, _RstRetileUDTF, _tile_cellid

_WINDOW_KEYS = ("col_off", "row_off", "width", "height")


def _digest(data) -> str:
    return hashlib.blake2b(bytes(data), digest_size=16).hexdigest()


def _key(tile):
    """What identifies the dataset a tile opens: its bytes, or its file with the window, clip and
    instructions that change what is read from it."""
    if tile["raster"] is not None:
        return _digest(tile["raster"])
    win, clip = tile["window"], tile["clip_polygon"]
    return (
        tile["path"],
        None if win is None else tuple(win[k] for k in _WINDOW_KEYS),
        None if clip is None else _digest(clip),
        tile["clip_crs"],
        tuple(sorted((tile["metadata"] or {}).items())),
    )


class _OpenLru:
    """Per-Python-worker LRU of opened datasets: the re-open amortized over the pieces of one tile
    (workers are reused, so the datasets stay open between rows)."""

    def __init__(self, cap: int = 4):
        self.cap, self.items = cap, OrderedDict()

    def get(self, tile):
        key = _key(tile)
        if key in self.items:
            self.items.move_to_end(key)
            return self.items[key][1]
        ctx = ot._open(tile)
        ds = ctx.__enter__()
        self.items[key] = (ctx, ds)
        while len(self.items) > self.cap:
            _, (old, _ds) = self.items.popitem(last=False)
            old.__exit__(None, None, None)
        return ds


_LRU = _OpenLru()


def _windows(ds, w, h):
    return tiling.plan_grid_windows(ds.width, ds.height, int(w), int(h))


def _piece(tile, ds, win, w, h):
    """The sub-tile row `rst_retile` gives for `win`: the same row as the UDTF's."""
    whole = tiling.covers_whole_input(ds, w, h)
    return _cut_tile(
        tiling.cut_window(ds, win),
        _tile_cellid(tile),
        win,
        tile if whole else None,
        _serde.band_marker(tile),
    )


@f.udf(ArrayType(V2_TILE_SCHEMA))
def retile_array(tile, w, h):
    """All the sub-tiles of one tile as one array value (explode it for rows)."""
    if tile is None:
        return None
    ds = _LRU.get(tile)
    return [_piece(tile, ds, win, w, h) for win in _windows(ds, w, h)]


@f.udf(IntegerType())
def retile_count(tile, w, h):
    return None if tile is None else len(_windows(_LRU.get(tile), w, h))


@f.udf(V2_TILE_SCHEMA)
def retile_part(tile, w, h, i):
    """Sub-tile number `i`: with retile_count, rows without a collection per input."""
    if tile is None or i is None:
        return None
    ds = _LRU.get(tile)
    return _piece(tile, ds, _windows(ds, w, h)[int(i)], w, h)


# The same table functions as the plain UDTFs, with Arrow batches between the JVM and Python.
retile_udtf_arrow = udtf(_RstRetileUDTF.func, returnType=V2_TILE_SCHEMA, useArrow=True)
bng_tessellateexplode_udtf_arrow = udtf(
    _BngTessellateExplode.func,
    returnType="cellid: string, core: boolean, chip: binary",
    useArrow=True,
)

_L2 = "com.databricks.labs.gbx.kernels.udf."
_CHIPS = "array<struct<cellid:string,core:boolean,chip:binary>>"


def register_probe_forms(spark) -> None:
    """The light probes, under names that cannot collide with a product function."""
    spark.udf.register("gbx_mvp_rst_retile_array", retile_array)
    spark.udf.register("gbx_mvp_rst_retile_count", retile_count)
    spark.udf.register("gbx_mvp_rst_retile_part", retile_part)
    spark.udtf.register("gbx_rst_retile_mvp_arrow", retile_udtf_arrow)
    spark.udtf.register(
        "gbx_bng_tessellateexplode_mvp_arrow", bng_tessellateexplode_udtf_arrow
    )


def register_l2_probes(spark) -> None:
    """The Java UDF facades (they take raster BYTES, not a tile struct); needs the staged JAR."""
    spark.udf.registerJavaFunction(
        "gbx_mvp_bng_tessellate_l2", _L2 + "BngTessellateUdf", _CHIPS
    )
    spark.udf.registerJavaFunction(
        "gbx_mvp_rst_retile_array_l2", _L2 + "RstRetileArrayBytesUdf", "array<binary>"
    )
    spark.udf.registerJavaFunction(
        "gbx_mvp_rst_retile_count_l2", _L2 + "RstRetileCountBytesUdf", "int"
    )
    spark.udf.registerJavaFunction(
        "gbx_mvp_rst_retile_part_l2", _L2 + "RstRetilePartBytesUdf", "binary"
    )
