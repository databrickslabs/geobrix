"""Probe forms for generators and aggregates: other ways to ask for the same rows (names ``gbx_mvp_*``, never documented).

The perf matrix times each shape of a generator, and the oracle holds every shape to the same rows.
The scale-out keeps the shapes that win and deletes the rest.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import hashlib
import os
from collections import OrderedDict
from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
from pyspark.sql import functions as f
from pyspark.sql.functions import udtf
from pyspark.sql.types import ArrayType, BinaryType, IntegerType

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.pygx.functions import _BngTessellateExplode
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import budget
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA
from databricks.labs.gbx.pyrx.functions import (
    _burn_best,
    _cut_tile,
    _keep_larger,
    _RstRetileUDTF,
    _tile_cellid,
)

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

# --- MVP-IO: decoding the virtual tiles of a bytes-free read ---------------------------------------------------------
#
# The light reader's virtual tiles carry a path and a window, not bytes. Three ways to turn them into tiles with bytes
# are held to one answer (`oracles.MVP_IO`): the reader with bytes (`b`), this row-wise UDF (L3), and the same decode in
# one Python call per Arrow batch, through pyrx (`d-py`).

_HEADERS: dict = (
    {}
)  # (path, size, mtime) -> (width, height, count, dtype, crs): one header read per file version per worker
_HEADERS_MAX = 4096


def _stamp(path: str):
    """The key of a file's version: a rewritten file is another key, so it is never served another file's header."""
    from databricks.labs.gbx.ds._listing import to_local_path

    try:
        st = os.stat(to_local_path(path))
    except OSError:  # not a local file (the gate judges it when the header is opened)
        return (path, None, None)
    return (path, st.st_size, st.st_mtime_ns)


def _source_header(path: str):
    """(width, height, count, dtype, crs) of the file at `path`, opened as every light read is (the driver policy and
    the remote-path gate). Cached per worker: the many windows of one file read its header once.
    """
    key = _stamp(path)
    hit = _HEADERS.get(key)
    if hit is None:
        from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

        with ot.open_header(VirtualTile(cellid=0, path=path)) as h:
            hit = (h.width, h.height, h.count, str(h.dtypes[0]), h.crs)
        if len(_HEADERS) >= _HEADERS_MAX:
            _HEADERS.clear()
        _HEADERS[key] = hit
    return hit


def _decoded_bytes(tile) -> int:
    """What decoding `tile` holds in memory: the pixels of its window, of the bands it keeps (a pending band select),
    doubled when a pending reprojection holds the window it reads and the warped copy of it. A Zarr-backed tile has no
    file header: its own header view (window and bands applied) is what `open_header` gives.
    """
    vt = ot._to_virtual_tile(tile)
    if ot._zarr_tiles.is_zarr_tile(vt):
        with ot.open_header(vt) as h:
            return h.width * h.height * h.count * int(np.dtype(h.dtypes[0]).itemsize)
    width, height, count, dtype, crs = _source_header(vt.path)
    if vt.window is not None:
        _, _, width, height = vt.window
    bands = ot._parse_pending(vt.metadata)[0]
    size = width * height * (len(bands) if bands else count)
    size *= int(np.dtype(dtype).itemsize)
    return (
        size * 2 if vt.crs and ot._warp_pending(vt, SimpleNamespace(crs=crs)) else size
    )


def _gate(tile, cap_bytes) -> None:
    """The materialize gate of a worker (`materialize_decision`, kind `ingest`): a tile decoded over this compute's cap
    fails fast with its fix, never an OOM. `cap_bytes` is the driver's connect-aware cap (a worker cannot measure it).
    """
    from databricks.labs.gbx.ds.file_gbx import materialize_decision

    size = _decoded_bytes(tile)
    if materialize_decision(size, "ingest", cap_bytes=cap_bytes) == "error":
        raise ValueError(
            f"gbx_mvp_rst_materialize: the virtual tile is {size // 2**20} MiB decoded, over this compute's read cap. "
            "Fix: retile it first (gbx_rst_retile) so each tile is smaller"
        )


def _decode_py(tile, cap_bytes) -> dict:
    """The materialized row of a virtual tile through pyrx (`open_tile`: the driver policy, the remote-path gate, FUSE
    staging): the bytes, the pending instructions consumed, and `path` cleared, as the JVM's decode leaves the tile.
    """
    _gate(tile, cap_bytes)
    row = ot.materialize_to_bytes(ot._to_virtual_tile(tile)).to_row()
    row["path"] = None
    return row


def _materialized(tile, cap_bytes) -> dict:
    """The row for a virtual tile: decoded, or an error tile when it cannot be (a bad file, a refused location, a tile over
    the cap). That is the convention of the light `rst_*` functions (`RST_TransformCrs` and the rest): a bad input is a
    row of data, the reason under `metadata["last_error"]`, as the JVM's `safeEval` gives; resource exhaustion still
    fails the task.
    """
    try:
        return _decode_py(tile, cap_bytes)
    except (MemoryError, RecursionError):
        raise
    except Exception as e:  # noqa: BLE001
        return _serde.build_error_tile(
            f"gbx_mvp_rst_materialize: {e}", _tile_cellid(tile)
        )


def _build_materialize_udf(read_cap=None):
    """L3 gbx_mvp_rst_materialize: one row at a time (the baseline for the batch form). `read_cap` is the driver's
    connect-aware read cap, which the registrar captures (`factory_args: read_cap`)."""

    @f.udf(V2_TILE_SCHEMA)
    def materialize_udf(tile):
        if tile is None or tile["raster"] is not None or tile["path"] is None:
            return tile
        return _materialized(tile, read_cap)

    return materialize_udf


materialize_udf = _build_materialize_udf()


def keep_virtual(tile, cap_bytes) -> dict:
    """The decode of the IO floor form: the tile as it came, still virtual. `materialize_batches(decode=keep_virtual)`
    is the d-py shell (Arrow rows to Python and back, the metadata made a dict) with the decode skipped.
    """
    return tile


# The decoded bytes one output batch may hold. A batch of every tile of an Arrow batch (1000 tiles of 4 MiB) is a
# 4 GiB column: Arrow cannot build a binary column past 2 GiB ("Cannot convert ChunkedArray to Array"), and a
# Serverless UDF is limited to 1 GiB.
_OUT_CHUNK_BYTES = 64 << 20


def materialize_batches(batches, cap_bytes=None, decode=None):
    """d-py: decode every virtual tile of an Arrow batch in one Python call (no per-row UDF hop): the `mapInArrow`
    body over a one-column `tile` DataFrame. `cap_bytes` is the driver's read cap (`report_detected_cap(spark)`).
    `decode` (default `_materialized`) is the per-tile step; the bench's floor form passes `keep_virtual` to time the shell.

    A map column is a list of (key, value) pairs in `to_pylist()`: each tile's metadata is made a dict once for the
    decode, and written back in the schema's form.
    """
    import pyarrow as pa

    for b in batches:
        rows = b.to_pylist()
        start, held = 0, 0
        for i, r in enumerate(rows):
            t = r["tile"]
            if t is None or t["raster"] is not None or t["path"] is None:
                continue
            t["metadata"] = dict(t["metadata"] or ())
            out = (decode or _materialized)(t, cap_bytes)
            out["metadata"] = list((out["metadata"] or {}).items())
            r["tile"] = out
            held += len(out["raster"] or b"")
            if (
                held >= _OUT_CHUNK_BYTES
            ):  # a bounded slice out, not one batch of every decoded tile
                yield pa.RecordBatch.from_pylist(rows[start : i + 1], schema=b.schema)
                for done in rows[start : i + 1]:
                    done["tile"] = None  # let the decoded bytes go
                start, held = i + 1, 0
        if start < len(rows) or not rows:
            yield pa.RecordBatch.from_pylist(rows[start:], schema=b.schema)


# --- Aggregates as a finalize over collect_list and a tree of finalizes ------------------------------------
# (the table function over `TABLE(...) PARTITION BY` is the product's `gbx_rst_merge_tvf` / `gbx_rst_bng_rasterize_tvf`)
#
# Every form here keeps the contract of `gbx_rst_merge_agg` / `gbx_rst_bng_rasterize_agg` on the light tier, so
# whichever wins can take the aggregate's place: a repeated cell keeps its largest value, a null value (None or
# NaN) never wins over a number, and a cell with nothing but nulls burns NaN (`_max_value_per_cell`). They give
# BINARY, which has no place for an error tile: a group of nothing but error tiles gives NULL.


@f.udf(BinaryType())
def bng_rasterize_cells(cells, xmin, ymin, xmax, ymax, width, height):
    """The finalize of `collect_list(struct(cellid, value))` on an explicit canvas."""
    best = {}
    for c in cells or []:
        if c["cellid"] is not None:
            _keep_larger(best, c["cellid"], c["value"])
    return _burn_best(best, xmin, ymin, xmax, ymax, width, height) if best else None


@f.udf(BinaryType())
def canvas_max(rasters):
    """Per-pixel max of aligned partial canvases: the tree step above `bng_rasterize_cells`.

    A pixel is the largest number any partial has; NaN there if a partial burned a NaN and none a number; NoData
    if none has the cell. Associative, so merging partials in any shape equals one pass over the group.
    """
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core.compression import write_gtiff

    best, seen, profile = None, None, None
    for r in rasters or []:
        with MemoryFile(bytes(r)) as mf, open_bytes(mf) as ds:
            a = ds.read(1).astype("float64")
            present = a != ds.nodata  # a NaN is a cell with a null value: present
            if best is None:
                best, seen, profile = (
                    np.full(a.shape, -np.inf),
                    np.zeros(a.shape, bool),
                    ds.profile.copy(),
                )
            best = np.fmax(
                best, np.where(present, a, -np.inf)
            )  # fmax: a NaN never beats a number
            seen |= present
    if best is None:
        return None
    out = np.where(best > -np.inf, best, np.where(seen, np.nan, profile["nodata"]))
    return write_gtiff(profile, out.astype(profile["dtype"])[None, :, :])


_L1_AGGREGATES = ("gbx_rst_bng_rasterize_agg",)


def register_probe_forms(spark) -> None:
    """The light probes, under names that cannot collide with a product function."""
    spark.udf.register("gbx_mvp_rst_retile_array", retile_array)
    spark.udf.register("gbx_mvp_rst_retile_count", retile_count)
    spark.udf.register("gbx_mvp_rst_retile_part", retile_part)
    spark.udf.register("gbx_mvp_rst_bng_rasterize_cells", bng_rasterize_cells)
    spark.udf.register("gbx_mvp_rst_canvas_max", canvas_max)
    spark.udtf.register("gbx_rst_retile_mvp_arrow", retile_udtf_arrow)
    spark.udtf.register(
        "gbx_bng_tessellateexplode_mvp_arrow", bng_tessellateexplode_udtf_arrow
    )


def register_l2_probes(spark) -> None:
    """The Java UDF facade of the BNG rasterize finalize (it takes raster BYTES, not a tile struct); needs the
    staged JAR."""
    spark.udf.registerJavaFunction(
        "gbx_mvp_rst_bng_rasterize_cells_l2",
        "com.databricks.labs.gbx.rasterx.expressions.agg.mvp.BngRasterizeCellsBytesUdf",
        "binary",
    )


def register_l1_udafs(spark) -> None:
    """The Catalyst aggregates under their product names; needs the staged JAR (a classic session)."""
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manager import (
        jvm,  # the JVM bridge is the manager's, not the light product's
    )

    for name in _L1_AGGREGATES:
        companion = manifest.load().functions[name].levels["L1"]["companion"]
        why = jvm.install_companion(spark, companion, name)
        if why:
            raise RuntimeError(f"{name}: L1 install gave {why}")
