"""How a light row UDF's (or row UDTF's) rows cross the JVM <-> Python boundary: Arrow batches or pickles, chosen by
the session.

Its own module, outside ``pyrx``, so that the GDAL spec engine (``engine/l3.py``), the light raster functions and the
light vector functions share one implementation: a worker that runs only the engine never imports ``pyrx/functions.py``,
and one that runs only vector functions never imports rasterio. Nothing here is public API.
"""

from __future__ import annotations

import functools
import math
import threading
from typing import Callable, Dict, Optional, Sequence

from pyspark.sql import functions as f

TILE = "tile"  # the return type of a UDF that returns a v2 tile (``pyrx._serde.V2_TILE_SCHEMA``, read when built)

# The transports of a SessionTileUdf: ``pickle`` (a row UDF, useArrow=False); ``arrow`` (an ``arrow_udf``: pyarrow
# arrays in and out, Spark 4.1+); ``arrow_row`` (the Arrow-optimized row UDF, useArrow=True: its arguments go through
# pandas on Spark 4.0 and where the legacy pandas conversion conf is on).
FORMS = ("pickle", "arrow", "arrow_row")


class _LossyCellId(ValueError):
    """A member's cell id arrived as a float64 past 2**53: it may no longer be the id that was sent."""


_SCALAR_FIX = (
    "Fix: filter the NULL tiles out first (WHERE tile IS NOT NULL) and give every tile a cellid, or run on Spark 4.1 "
    "or later, which hands the integers over exactly."
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
    """One tile (or cell) struct delivered by the Arrow-optimized row UDF, with its integers as integers.

    pyspark 4.0 hands a batch to an Arrow-optimized row UDF through pandas: an INT or BIGINT field that holds a NULL
    arrives as float64 (a NULL ``cellid`` or ``window`` taints every row of the batch, a NULL member taints its own
    array), and the NULL itself as NaN. A whole number below 2**53 is exact and is restored; a cell id at or past
    2**53 (H3, quadbin and custom ids are) may have been rounded to another cell and is refused. A struct that came
    exactly (the ``arrow`` and ``pickle`` forms) passes through as it came.
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


# --- the pyarrow form: Arrow arrays in, the row function per row, an Arrow array out ------------------------------------

# arrow type text -> the converter of a column's Python values (None: as ``to_pylist`` gives them). Filled on the worker.
_IN_CONVERTERS: Dict[str, Optional[Callable]] = {}


def _own_input_converter(atype):
    """The values of an Arrow column as the pickled form gives them, without pyspark's converter: a struct as a
    ``Row`` (nested ones too), a map as a dict, a list as a list. None when ``to_pylist`` already gives them.
    """
    import pyarrow as pa
    from pyspark.sql import Row

    if pa.types.is_struct(atype):
        names = [atype.field(i).name for i in range(atype.num_fields)]
        convs = [
            _own_input_converter(atype.field(i).type) for i in range(atype.num_fields)
        ]
        make = Row(*names)

        def struct(v):
            if v is None:
                return None
            return make(*[v[n] if c is None else c(v[n]) for n, c in zip(names, convs)])

        return struct
    if pa.types.is_map(atype):
        kc, ic = _own_input_converter(atype.key_type), _own_input_converter(
            atype.item_type
        )

        def mapping(v):
            if v is None:
                return None
            return {
                (k if kc is None else kc(k)): (x if ic is None else ic(x)) for k, x in v
            }

        return mapping
    if (
        pa.types.is_list(atype)
        or pa.types.is_large_list(atype)
        or pa.types.is_fixed_size_list(atype)
    ):
        ec = _own_input_converter(atype.value_type)
        if ec is None:
            return None
        return lambda v: None if v is None else [ec(x) for x in v]
    return None


def _input_converter(atype):
    """The converter of an Arrow column of type *atype* to the Python values the pickled form gives: pyspark's own (the
    one its Arrow-optimized row UDF uses where the legacy pandas conversion is off), else ``_own_input_converter``.
    """
    key = str(atype)
    if key not in _IN_CONVERTERS:
        try:
            from pyspark.sql.conversion import ArrowTableToRowsConversion
            from pyspark.sql.pandas.types import from_arrow_type

            conv = ArrowTableToRowsConversion._create_converter(
                from_arrow_type(atype), none_on_identity=True, binary_as_bytes=True
            )
        except (
            Exception
        ):  # noqa: BLE001 - a pyspark without that converter (or its signature)
            conv = _own_input_converter(atype)
        _IN_CONVERTERS[key] = conv
    return _IN_CONVERTERS[key]


def _column_values(col):
    """The Python values of one Arrow argument column, exactly: an INT or BIGINT is an int and a NULL is None."""
    conv = _input_converter(col.type)
    vals = col.to_pylist()
    return vals if conv is None else [conv(v) for v in vals]


def _output_converter(return_type):
    """``rows -> pyarrow.Array`` of *return_type*: pyspark's converter of Python values (a Row, tuple or dict for a
    struct, a dict for a map) where it has one, then ``pyarrow.array`` of the Arrow type Spark expects.
    """
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type

    atype = to_arrow_type(return_type, prefers_large_types=False)
    try:
        from pyspark.sql.conversion import LocalDataToArrowConversion

        conv = LocalDataToArrowConversion._create_converter(
            return_type, none_on_identity=True
        )
    except (
        Exception
    ):  # noqa: BLE001 - a pyspark without that converter: pyarrow takes dicts and tuples itself
        conv = None

    def out(rows):
        vals = rows if conv is None else [conv(r) for r in rows]
        try:
            return pa.array(vals, type=atype)
        except pa.lib.ArrowInvalid:
            return pa.array(vals).cast(target_type=atype, safe=False)

    return out


def _arrow_batch(func, return_type):
    """*func* (a row function) as the body of a SCALAR ``arrow_udf``: the batch's columns as Python values, one call per
    row, the results as one Arrow array. No pandas: the legacy pandas conversion conf does not apply to it.
    """
    state = {}

    def batch(*cols):
        out = state.get("out")
        if out is None:
            out = state["out"] = _output_converter(return_type)
        args = [_column_values(c) for c in cols]
        return out([func(*row) for row in zip(*args)])

    # the name and doc only: no annotations, which pyspark would read as the UDF's type hints
    for attr in ("__module__", "__name__", "__qualname__", "__doc__"):
        try:
            setattr(batch, attr, getattr(func, attr))
        except (AttributeError, TypeError):
            pass
    return batch


def has_arrow_udf() -> bool:
    """This pyspark has ``arrow_udf`` with the SCALAR eval type (pyspark 4.1+)."""
    try:
        from pyspark.sql.functions import arrow_udf  # noqa: F401
        from pyspark.sql.pandas.functions import ArrowUDFType

        return hasattr(ArrowUDFType, "SCALAR")
    except ImportError:
        return False


class SessionTileUdf:
    """A light row UDF over tiles (or large BINARY) whose TRANSPORT the session that uses it chooses: Arrow batches or
    pickles.

    The body is the same row function in every form; only how its rows cross the JVM <-> Python boundary differs.
    A plain (pickled) UDF groups up to ``spark.sql.execution.python.udf.maxRecordsPerBatch`` (100) rows into ONE
    pickle that the executor JVM builds on its heap, and reads the outputs back the same way. With tiles of a few MB
    that pickle is the task's whole partition: 64 tiles of 2.3 MB (a 146 MB file) per task made the JVM spend a third
    to a half of each task collecting garbage, the Python worker grew to 0.9 GB and a node ran out of memory, so 4x the
    rows took 8x the time (dedicated DBR 19, 4 x m5d.2xlarge). Arrow keeps the rows in off-heap buffers, and a batch
    is bounded by ``spark.sql.execution.arrow.maxBytesPerBatch`` (32 MiB on DBR 19): the same op took 3.6x the time
    for 4x the rows and the worker stayed under 0.5 GB. The input side counts as much as the output: an accessor that
    takes a tile and returns a number pickles the tile all the same.

    The Arrow form is an ``arrow_udf`` (pyspark 4.1+): pyarrow arrays in and out, each row's values handed to the row
    function as the pickled form hands them (``_column_values``). No pandas runs, so an INT or BIGINT (a cell id up to
    2**63) is an int and a NULL is None on every runtime, whatever
    ``spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled`` says (on by default on classic DBR 19, where an
    Arrow-optimized row UDF gets a NULL INT as NaN). It runs where the ``arrow_tile_transport`` capability holds (server
    Spark 4.1+ and a client with ``arrow_udf``; ``GBX_CAP_ARROW_TILE_TRANSPORT=0`` turns it off), and the *fallback*
    form elsewhere: ``pickle`` by default, or ``arrow_row`` for a UDF that must not pickle even on Spark 4.0 (a whole
    group of tiles in one row). ``arrow_row`` restores whole numbers below 2**53 in the tile arguments (``tiles``:
    positions of tile structs; ``tile_arrays``: positions of arrays of tile or cell structs) and refuses a cell id past
    that. A UDF that returns a tile refuses it as an error tile naming the fix (``on_lossy``, by default); any other
    raises it.

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
        fallback: str = "pickle",
    ):
        if isinstance(return_type, str) and return_type == TILE:
            from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

            return_type = V2_TILE_SCHEMA
        if on_lossy is None and _is_tile_type(return_type):
            on_lossy = _lossy_error_tile
        if fallback not in ("pickle", "arrow_row"):
            raise ValueError(
                f"no fallback transport {fallback!r}; known: pickle, arrow_row"
            )
        self._func = func
        self._return_type = return_type
        self._tiles = tuple(tiles)
        self._tile_arrays = tuple(tile_arrays)
        self._on_lossy = on_lossy
        self._fallback = fallback
        self._forms = {"pickle": f.udf(func, return_type, useArrow=False)}
        self._lock = threading.Lock()
        functools.update_wrapper(self, func)

    @property
    def fallback(self) -> str:
        """The form a session without the Arrow tile transport runs."""
        return self._fallback

    def form(self, label: str):
        """The UDF of transport *label* (``pickle``, ``arrow`` or ``arrow_row``), built once."""
        with self._lock:
            if label not in self._forms:
                if label == "arrow":
                    from pyspark.sql.functions import arrow_udf

                    self._forms[label] = arrow_udf(
                        _arrow_batch(self._func, self._return_type), self._return_type
                    )
                elif label == "arrow_row":
                    self._forms[label] = f.udf(
                        self._guarded(), self._return_type, useArrow=True
                    )
                else:
                    raise ValueError(
                        f"no tile UDF transport {label!r}; known: {', '.join(FORMS)}"
                    )
            return self._forms[label]

    def _guarded(self):
        """The row function behind ``arrow_row``: the tile arguments' integers restored, a lossy cell id refused."""
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
                        args[i] = [_exact_member(t, _ARRAY_FIX) for t in args[i]]
            except _LossyCellId as exc:
                if on_lossy is None:
                    raise
                return on_lossy(exc)
            return func(*args)

        return exact

    def label_for(self, spark) -> str:
        """The transport *spark* runs: ``arrow`` where the capability registry chooses it, else the fallback."""
        from databricks.labs.gbx.manager import capabilities

        label = capabilities.path(spark, "tile_udf").split(":", 1)[1]
        return label if label == "arrow" else self._fallback

    def for_session(self, spark):
        """The UDF form *spark* runs: the capability registry's choice of transport for that session."""
        return self.form(self.label_for(spark))

    def __call__(self, *cols):
        from databricks.labs.gbx.ds.file_gbx import _resolve_session_for_cap

        spark = _resolve_session_for_cap()
        udf = self.form(self._fallback) if spark is None else self.for_session(spark)
        return udf(*cols)

    def __getattr__(self, name):
        # the pickled form's attributes (func, returnType, evalType, ...): only for a name not found on the instance
        forms = self.__dict__.get("_forms")
        if forms is None or name.startswith("__"):
            raise AttributeError(name)
        return getattr(forms["pickle"], name)


class SessionTileUdtf:
    """A light row UDTF over tiles (or large BINARY) whose transport the session chooses: the Arrow-optimized UDTF
    (``useArrow=True``) or the pickled one (``useArrow=False``), over the same handler class and return type.

    A pickled UDTF's input rows are pickled on the executor heap in batches, as a pickled UDF's are; the Arrow one
    reads them as Arrow batches. A row UDTF sits on the right of a LATERAL join, which the pyarrow UDTF
    (``arrow_udtf``) cannot (``LATERAL_JOIN_WITH_ARROW_UDTF_UNSUPPORTED``), so the Arrow form is the Arrow-optimized
    row UDTF. It converts its arguments through pandas on Spark 4.0 (a NULL INT as NaN, a BIGINT as float64) and on a
    later Spark with ``spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled`` on (off on DBR 19 and
    Serverless), so it runs where the ``arrow_tile_udtf_transport`` capability holds and the pickled form elsewhere.

    It reads as the pickled form (``func``, ``returnType``, ``evalType``); a registrar asks for :meth:`for_session`.
    """

    def __init__(self, cls, return_type):
        from pyspark.sql.functions import udtf

        self._cls = cls
        self._return_type = return_type
        self._forms = {"pickle": udtf(cls, returnType=return_type, useArrow=False)}
        self._lock = threading.Lock()
        functools.update_wrapper(self, cls, updated=())

    def form(self, label: str):
        """The UDTF of transport *label* (``pickle`` or ``arrow``), built once."""
        with self._lock:
            if label not in self._forms:
                if label != "arrow":
                    raise ValueError(
                        f"no tile UDTF transport {label!r}; known: arrow, pickle"
                    )
                from pyspark.sql.functions import udtf

                self._forms[label] = udtf(
                    self._cls, returnType=self._return_type, useArrow=True
                )
            return self._forms[label]

    def for_session(self, spark):
        """The UDTF form *spark* runs: the capability registry's choice of transport for that session."""
        from databricks.labs.gbx.manager import capabilities

        return self.form(capabilities.path(spark, "tile_udtf").split(":", 1)[1])

    def __call__(self, *cols):
        from databricks.labs.gbx.ds.file_gbx import _resolve_session_for_cap

        spark = _resolve_session_for_cap()
        fn = self._forms["pickle"] if spark is None else self.for_session(spark)
        return fn(*cols)

    def __getattr__(self, name):
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
    fallback: str = "pickle",
):
    """Decorator: *func* as a :class:`SessionTileUdf`. Bare (``@tile_udf``) for a row function ``(tile, ...)`` returning
    a v2 tile; with keywords for another return type (``returns``) or other tile arguments (``tiles``, ``tile_arrays``:
    positions; ``tiles=()`` for one whose large argument is BINARY or a path), or the ``arrow_row`` fallback.
    """

    def wrap(fn):
        return SessionTileUdf(
            fn, returns, tiles=tiles, tile_arrays=tile_arrays, fallback=fallback
        )

    return wrap if func is None else wrap(func)


def tile_udtf(returnType):
    """Decorator: a UDTF handler class as a :class:`SessionTileUdtf` of *returnType*."""

    def wrap(cls):
        return SessionTileUdtf(cls, returnType)

    return wrap
