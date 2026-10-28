"""MVP-G probe forms: other ways to ask for the same rows (names ``gbx_mvp_*``, never documented).

The perf matrix times each shape of a generator, and the oracle holds every shape to the same rows.
The scale-out keeps the shapes that win and deletes the rest.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import hashlib
from collections import OrderedDict
from contextlib import contextmanager

from pyspark.sql import functions as f
from pyspark.sql.functions import udtf
from pyspark.sql.types import ArrayType, IntegerType

from databricks.labs.gbx.pygx.functions import _BngTessellateExplode
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import budget
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
from databricks.labs.gbx.pyrx.functions import _cut_tile, _RstRetileUDTF, _tile_cellid

_WINDOW_KEYS = ("col_off", "row_off", "width", "height")


# A content hash of a 4096 x 4096 tile costs more than cutting all its windows, and the count+part shape asks
# once per window. So the key of a materialized tile is only a BUCKET: its length, its first and last 64 KB and
# 64 blocks of 4 KB spread between them (all of it up to 512 KB). It covers well under 1 % of a large raster, so
# two different rasters of one size often share it (an edited copy next to its original); an entry therefore
# keeps the bytes it was opened from, and a hit is confirmed by comparing them all (a memcmp, about half the cost
# of the argument copy every call already pays). A mismatch is a miss. (The JVM facades do the same.)
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
    """The bucket of the dataset a tile opens: a sample of its bytes (confirmed by `_OpenLru` against all of
    them), or its file with the window, clip and instructions that change what is read from it (exact).
    """
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
    """The dataset a tile opened, kept for the next call: the re-open amortized over the pieces of one tile.

    The pieces of one input row are cut one after the other, so ONE entry is all the shape can use; a second
    never hits and costs a whole raster of memory. An entry is (context, dataset, the bytes it was opened from,
    None for a virtual tile). It is dropped when the caller says the last piece is cut, when another raster
    takes its place, and when a new task starts on this worker (a Python worker has no task-end hook, so
    without those the last raster would stay resident on an idle worker).

    A raster is admitted only if its bytes fit the decoded-memory budget of this runtime
    (`budget.decoded_budget_bytes`); a larger one is opened for the call alone, so every piece pays the
    open, and nothing stays resident."""

    def __init__(self, cap: int = 1):
        self.cap, self.items, self.task = cap, OrderedDict(), None

    def close(self):
        while self.items:
            _, (ctx, _ds, _raw) = self.items.popitem(last=False)
            ctx.__exit__(None, None, None)

    def _drop(self, key):
        entry = self.items.pop(key, None)
        if entry is not None:
            entry[0].__exit__(None, None, None)

    @contextmanager
    def opened(self, tile):
        """Yield `(dataset, release)`; call `release()` once the last piece of this tile is cut."""
        task = _task_attempt()
        if task != self.task:
            self.close()
            self.task = task
        key, raw = _key(tile), tile["raster"]
        hit = self.items.get(key)
        if hit is not None:
            # the key of bytes is a sample: only the same bytes are the same raster
            if raw is None or hit[2] == raw:
                self.items.move_to_end(key)
                released = []
                try:
                    yield hit[1], lambda: released.append(True)
                finally:
                    if released:
                        self._drop(key)
                return
            self._drop(
                key
            )  # same bucket, other bytes: a miss, and the old entry is not wanted
        ctx = ot._open(tile)
        ds = ctx.__enter__()
        if raw is not None and len(raw) > budget.decoded_budget_bytes("auto"):
            try:  # too big to keep: this call's piece only
                yield ds, lambda: None
            finally:
                ctx.__exit__(None, None, None)
            return
        self.items[key] = (ctx, ds, raw)
        while len(self.items) > self.cap:
            _, (old, _ds, _raw) = self.items.popitem(last=False)
            old.__exit__(None, None, None)
        released = []
        try:
            yield ds, lambda: released.append(True)
        finally:
            if released:
                self._drop(key)


_LRU = _OpenLru()


def _windows(ds, w, h):
    tiling.require_window(w, h)
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
    with _LRU.opened(tile) as (ds, release):
        pieces = [_piece(tile, ds, win, w, h) for win in _windows(ds, w, h)]
        release()  # one call per input row: nothing to keep for the next
        return pieces


@f.udf(IntegerType())
def retile_count(tile, w, h):
    if tile is None:
        return None
    with _LRU.opened(tile) as (ds, _release):
        return len(_windows(ds, w, h))  # the parts that follow find it open


@f.udf(V2_TILE_SCHEMA)
def retile_part(tile, w, h, i):
    """Sub-tile number `i`: with retile_count, rows without a collection per input."""
    if tile is None or i is None:
        return None
    with _LRU.opened(tile) as (ds, release):
        windows = _windows(ds, w, h)
        piece = _piece(tile, ds, windows[int(i)], w, h)
        if int(i) == len(windows) - 1:
            release()  # the last piece of this row: the raster is not wanted again
        return piece


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
