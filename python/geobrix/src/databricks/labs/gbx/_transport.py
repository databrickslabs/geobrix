"""How a light row UDF's rows cross the JVM <-> Python boundary: Arrow batches or pickles, chosen by the session.

Its own module, outside ``pyrx``, so that the GDAL spec engine (``engine/l3.py``), the light raster functions and the
light vector functions share one implementation: a worker that runs only the engine never imports ``pyrx/functions.py``,
and one that runs only vector functions never imports rasterio. Nothing here is public API.
"""

from __future__ import annotations

import functools
import math
import threading
from typing import Callable, Optional, Sequence

from pyspark.sql import functions as f

TILE = "tile"  # the return type of a UDF that returns a v2 tile (``pyrx._serde.V2_TILE_SCHEMA``, read when built)


class _LossyCellId(ValueError):
    """A member's cell id arrived as a float64 past 2**53: it may no longer be the id that was sent."""


_SCALAR_FIX = (
    "Fix: filter the NULL tiles out first (WHERE tile IS NOT NULL) and give every tile a cellid, or run on Spark 4.1 "
    "or later with spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false, which hands the integers "
    "over exactly."
)

_ARRAY_FIX = (
    "Fix: take the NULL tiles out of the array first, as in "
    "gbx_rst_combineavg(filter(tiles, t -> t IS NOT NULL)), and give every tile a cellid."
)


def _whole(v):
    """A float64 that stands for an integer, back as one (NaN, a NULL in pandas' hands, is None)."""
    if isinstance(v, float):
        return None if math.isnan(v) else int(v)
    return v


def _exact_member(tile, fix: str = _ARRAY_FIX):
    """One Arrow-delivered tile (or cell) struct with its integers as integers.

    pyspark 4.0 hands a batch to an Arrow-optimized UDF through pandas: an INT or BIGINT field that holds a NULL
    arrives as float64 (a NULL ``cellid`` or ``window`` taints every row of the batch, a NULL member taints its own
    array), and the NULL itself as NaN. A whole number below 2**53 is exact and is restored; a cell id at or past
    2**53 (H3, quadbin and custom ids are) may have been rounded to another cell and is refused. pyspark 4.1 converts
    the integers exactly: the struct passes through here as it came.
    """
    if tile is None or isinstance(tile, (bytes, bytearray)):
        return tile
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    cellid = d.get("cellid")
    window = d.get("window")
    w = None
    if window is not None:
        w = window.asDict() if hasattr(window, "asDict") else dict(window)
    if not isinstance(cellid, float) and not (
        w is not None and any(isinstance(v, float) for v in w.values())
    ):
        return tile  # nothing came through a float
    if isinstance(cellid, float):
        if not math.isnan(cellid) and abs(cellid) >= 2**53:
            raise _LossyCellId(
                f"a tile's cell id came through a float and may have been rounded to another cell ({cellid!r}). "
                + fix
            )
        d["cellid"] = _whole(cellid)
    if w is not None:
        d["window"] = {k: _whole(v) for k, v in w.items()}
    return d


class SessionTileUdf:
    """A light row UDF over tiles (or large BINARY) whose TRANSPORT the session that uses it chooses: Arrow batches or
    pickles.

    The body is the same row function in both forms; only how its rows cross the JVM <-> Python boundary differs.
    A plain (pickled) UDF groups up to ``spark.sql.execution.python.udf.maxRecordsPerBatch`` (100) rows into ONE
    pickle that the executor JVM builds on its heap, and reads the outputs back the same way. With tiles of a few MB
    that pickle is the task's whole partition: 64 tiles of 2.3 MB (a 146 MB file) per task made the JVM spend a third
    to a half of each task collecting garbage, the Python worker grew to 0.9 GB and a node ran out of memory, so 4x the
    rows took 8x the time (dedicated DBR 19, 4 x m5d.2xlarge). Arrow keeps the rows in off-heap buffers, and a batch
    is bounded by ``spark.sql.execution.arrow.maxBytesPerBatch`` (32 MiB on DBR 19): the same op took 3.6x the time
    for 4x the rows and the worker stayed under 0.5 GB. The input side counts as much as the output: an accessor that
    takes a tile and returns a number pickles the tile all the same.

    Spark 4.0 hands an Arrow UDF an INT or BIGINT field as float64 when a NULL rides in the batch, which can round an
    H3-sized cell id to another cell, and so does a later Spark whose Arrow UDFs convert through pandas
    (``spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled``, on by default on dedicated DBR 19, where a
    NULL argument also arrives as NaN). So the Arrow form runs where the ``arrow_tile_transport`` capability holds
    (server Spark 4.1+ with that conversion off: Serverless environment 6; ``GBX_CAP_ARROW_TILE_TRANSPORT`` turns it
    off) and the pickled form elsewhere; the Arrow form
    still restores whole numbers below 2**53 in the tile arguments (``tiles``: positions of tile structs;
    ``tile_arrays``: positions of arrays of tile or cell structs) and refuses a cell id past that. A UDF that returns a
    tile refuses it as an error tile naming the fix (``on_lossy``, by default); any other raises it.

    To the machinery that reads a UDF object (``is_light_udf``, ``carrying``, the oracles' plain-function leg) it looks
    like the pickled form; a registrar and a Column-API call ask for the session's form with :meth:`for_session`.
    """

    def __init__(
        self,
        func,
        return_type=TILE,
        *,
        tiles: Sequence[int] = (0,),
        tile_arrays: Sequence[int] = (),
        on_lossy: Optional[Callable[[Exception], object]] = None,
    ):
        if isinstance(return_type, str) and return_type == TILE:
            from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

            return_type = V2_TILE_SCHEMA
        if on_lossy is None and _is_tile_type(return_type):
            on_lossy = _lossy_error_tile
        self._func = func
        self._return_type = return_type
        self._tiles = tuple(tiles)
        self._tile_arrays = tuple(tile_arrays)
        self._on_lossy = on_lossy
        self._forms = {"pickle": f.udf(func, return_type)}
        self._lock = threading.Lock()
        functools.update_wrapper(self, func)

    def form(self, label: str):
        """The UDF of transport *label* (``pickle`` or ``arrow``), built once."""
        with self._lock:
            if label not in self._forms:
                if label != "arrow":
                    raise ValueError(
                        f"no tile UDF transport {label!r}; known: arrow, pickle"
                    )
                func, tiles, arrays, on_lossy = (
                    self._func,
                    self._tiles,
                    self._tile_arrays,
                    self._on_lossy,
                )

                @functools.wraps(func)
                def exact(*args):
                    args = list(args)
                    try:
                        for i in tiles:
                            if i < len(args):
                                args[i] = _exact_member(args[i], _SCALAR_FIX)
                        for i in arrays:
                            if i < len(args) and args[i] is not None:
                                args[i] = [
                                    _exact_member(t, _ARRAY_FIX) for t in args[i]
                                ]
                    except _LossyCellId as exc:
                        if on_lossy is None:
                            raise
                        return on_lossy(exc)
                    return func(*args)

                self._forms[label] = f.udf(exact, self._return_type, useArrow=True)
            return self._forms[label]

    def for_session(self, spark):
        """The UDF form *spark* runs: the capability registry's choice of transport for that session."""
        from databricks.labs.gbx.manager import capabilities

        return self.form(capabilities.path(spark, "tile_udf").split(":", 1)[1])

    def __call__(self, *cols):
        from databricks.labs.gbx.ds.file_gbx import _resolve_session_for_cap

        spark = _resolve_session_for_cap()
        udf = self._forms["pickle"] if spark is None else self.for_session(spark)
        return udf(*cols)

    def __getattr__(self, name):
        # the pickled form's attributes (func, returnType, evalType, ...): only for a name not found on the instance
        forms = self.__dict__.get("_forms")
        if forms is None or name.startswith("__"):
            raise AttributeError(name)
        return getattr(forms["pickle"], name)


def _is_tile_type(return_type) -> bool:
    """True for the v2 tile struct: the return type whose refusal is an error tile."""
    from databricks.labs.gbx.engine._tile import TILE_FIELDS

    names = [fl.name for fl in getattr(return_type, "fields", None) or ()]
    return tuple(names) == tuple(TILE_FIELDS)


def _lossy_error_tile(exc):
    from databricks.labs.gbx.pyrx import _serde

    return _serde.build_error_tile(str(exc), 0)


def tile_udf(
    func=None,
    *,
    returns=TILE,
    tiles: Sequence[int] = (0,),
    tile_arrays: Sequence[int] = (),
):
    """Decorator: *func* as a :class:`SessionTileUdf`. Bare (``@tile_udf``) for a row function ``(tile, ...)`` returning
    a v2 tile; with keywords for another return type (``returns``) or other tile arguments (``tiles``, ``tile_arrays``:
    positions; ``tiles=()`` for one whose large argument is BINARY or a path)."""

    def wrap(fn):
        return SessionTileUdf(fn, returns, tiles=tiles, tile_arrays=tile_arrays)

    return wrap if func is None else wrap(func)
