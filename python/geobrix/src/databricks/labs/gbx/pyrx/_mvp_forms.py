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


# A content hash of a 4096 x 4096 tile costs more than cutting all its windows, and the count+part shape asks
# once per window. The key of a materialized tile is its length, its first and last 64 KB and 64 blocks of 4 KB
# spread between them (all of it up to 512 KB). Two different rasters of one length that agree on every sampled
# byte would share an entry; the inputs of the oracle and the bench do not. (The JVM facades key the same way.)
_HEAD, _BLOCK, _BLOCKS, _WHOLE = 65536, 4096, 64, 524288


def _digest(data) -> str:
    raw = memoryview(data)
    n = len(raw)
    h = hashlib.blake2b(digest_size=16)
    h.update(n.to_bytes(8, "big"))
    if n <= _WHOLE:
        h.update(raw)
    else:
        h.update(raw[:_HEAD])
        h.update(raw[-_HEAD:])
        span = n - 2 * _HEAD - _BLOCK
        for i in range(_BLOCKS):
            off = _HEAD + span * i // (_BLOCKS - 1)
            h.update(raw[off : off + _BLOCK])
    return h.hexdigest()


def _key(tile):
    """What identifies the dataset a tile opens: a sample of its bytes, or its file with the window, clip and
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


def _task_attempt():
    """The id of the Spark task this code runs in (None outside one)."""
    from pyspark import TaskContext

    tc = TaskContext.get()
    return None if tc is None else tc.taskAttemptId()


class _OpenLru:
    """Per-Python-worker LRU of opened datasets: the re-open amortized over the pieces of one tile.

    A worker is reused, so what a task opened must not outlive it: the first call of a new task closes
    everything the last one left (a Python worker has no task-end hook, so a finished task's datasets
    stay open, at most `cap` of them, until the next task on this worker starts)."""

    def __init__(self, cap: int = 4):
        self.cap, self.items, self.task = cap, OrderedDict(), None

    def close(self):
        while self.items:
            _, (ctx, _ds) = self.items.popitem(last=False)
            ctx.__exit__(None, None, None)

    def get(self, tile):
        task = _task_attempt()
        if task != self.task:
            self.close()
            self.task = task
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
