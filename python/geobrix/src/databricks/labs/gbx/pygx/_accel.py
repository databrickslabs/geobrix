"""Faster rungs for gbx_bng_eastnorthasbng (Addendum 4 ladder): c1 vectorized encode, d Rust kernel.

Same results as c0 (pygx.functions._bng_eastnorthasbng_udf), row for row, errors included; the MVP-S
oracle runs all three. The contract they share:
  * a NULL (or NaN) coordinate or resolution is NULL, and a NULL coordinate is NULL before its
    resolution is read, so a bad resolution on such a row is not an error;
  * an integral float resolution (an INT column holding a NULL arrives as 3.0) is the integer;
  * a resolution BNG does not define, or a coordinate that is not a number, raises and names the value.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import logging

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from databricks.labs.gbx import _rs
from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pygx.functions import _norm_res

_LOG = logging.getLogger(__name__)
_WARNED = False


def _groups(res: pd.Series, ok: np.ndarray):
    """(resolution, row mask) per distinct resolution among the rows whose coordinates are both present.

    Usually one group (a literal). A bad value on a row that has coordinates raises, naming it.
    """
    for raw in pd.unique(res[ok].dropna()):
        raw = raw.item() if hasattr(raw, "item") else raw  # plain Python in messages
        yield _bng.get_resolution(_norm_res(raw)), ok & (res == raw).to_numpy()


def _floats(s: pd.Series) -> np.ndarray:
    return s.to_numpy(dtype="float64", na_value=np.nan)


def _python_cells(ev: np.ndarray, nv: np.ndarray, r: int):
    return [_bng.format(int(i)) for i in _bng.point_to_cell_id_vec(ev, nv, r)]


def _rust_cells(ev: np.ndarray, nv: np.ndarray, r: int) -> np.ndarray:
    return _rs.bng_cells(ev, nv, r).to_numpy(zero_copy_only=False)


def _kernel():
    """The Rust cells function; the c1 one when this process has no usable build.

    The driver picks `d` from its own build. A worker whose build is missing or does not load (another
    platform, an old glibc) answers each batch through c1 instead of failing every row.
    """
    global _WARNED
    if _rs.unavailable_reason() is None:
        return _rust_cells
    if not _WARNED:
        _WARNED = True
        _LOG.warning(
            "geobrix: Rust kernels unavailable on this worker (%s); using the vectorized Python rung",
            _rs.unavailable_reason(),
        )
    return _python_cells


def _encode(ev: np.ndarray, nv: np.ndarray, res: pd.Series, cells) -> np.ndarray:
    out = np.full(len(ev), None, dtype=object)
    for r, mask in _groups(res, ~np.isnan(ev) & ~np.isnan(nv)):
        out[mask] = cells(ev[mask], nv[mask], r)
    return out


def _c1_batch(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    return pd.Series(_encode(_floats(e), _floats(n), res, _python_cells))


def _d_batch(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    return pd.Series(_encode(_floats(e), _floats(n), res, _kernel()))


bng_eastnorthasbng_c1 = pandas_udf(StringType())(_c1_batch)
_d_pandas = pandas_udf(StringType())(_d_batch)


def _arrow_floats(a):
    """float64 numpy (NaN = NULL) for a numeric Arrow array; None for any other type (strings, ...)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    t = a.type
    if not (
        pa.types.is_floating(t)
        or pa.types.is_integer(t)
        or pa.types.is_decimal(t)
        or pa.types.is_boolean(t)
    ):
        return None
    # safe=False rounds a BIGINT beyond 2**53 as Python's float() does. The cast comes BEFORE
    # fill_null: an INT column cannot hold the NaN.
    return (
        pc.cast(a, pa.float64(), safe=False)
        .fill_null(float("nan"))
        .to_numpy(zero_copy_only=False)
    )


def _d_arrow(e, n, res):
    """PySpark 4.1+: pyarrow in, pyarrow out, no pandas hop for numeric coordinates."""
    import pyarrow as pa
    import pyarrow.compute as pc

    ev, nv = _arrow_floats(e), _arrow_floats(n)
    if (
        ev is None or nv is None
    ):  # not numbers: c0's own conversion, so its error names the value
        return pa.array(
            _d_batch(e.to_pandas(), n.to_pandas(), res.to_pandas()), type=pa.string()
        )
    if not (~np.isnan(ev) & ~np.isnan(nv)).any():
        return pa.nulls(
            len(ev), pa.string()
        )  # every row NULL: the resolution is never read
    if res.null_count == 0 and _rs.unavailable_reason() is None:
        one = pc.unique(res)
        if len(one) == 1:  # a literal resolution: the whole batch in one kernel call
            r = _bng.get_resolution(_norm_res(one[0].as_py()))
            return _rs.bng_cells(ev, nv, r)
    return pa.array(_encode(ev, nv, res.to_pandas(), _kernel()), type=pa.string())


def _build_d():
    arrow_udf = getattr(
        F, "arrow_udf", None
    )  # PySpark 4.1+: pyarrow in / out, no pandas hop
    return _d_pandas if arrow_udf is None else arrow_udf(StringType())(_d_arrow)


bng_eastnorthasbng_d = _build_d()
