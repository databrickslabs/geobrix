"""Faster rungs for gbx_bng_eastnorthasbng (Addendum 4 ladder): c1 vectorized encode, d Rust kernel.

Same results as c0 (pygx.functions._bng_eastnorthasbng_udf), row for row; the MVP-S oracle runs all three.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from databricks.labs.gbx import _rs
from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pygx.functions import _norm_res


def _groups(res: pd.Series, ok: np.ndarray):
    """(resolution, row mask) per distinct resolution among the rows whose coordinates are both present.

    Usually one group (a literal). Like c0, a row with a NULL coordinate is NULL before its resolution is
    read, and an integral float (an INT column with a NULL arrives as 3.0) is the integer. A bad value
    on a row that has coordinates raises, naming it (parameter).
    """
    for raw in pd.unique(res[ok].dropna()):
        yield _bng.get_resolution(_norm_res(raw)), ok & (res == raw).to_numpy()


def _floats(s: pd.Series) -> np.ndarray:
    return s.to_numpy(dtype="float64", na_value=np.nan)


@pandas_udf(StringType())
def bng_eastnorthasbng_c1(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    out = np.full(len(e), None, dtype=object)
    ev, nv = _floats(e), _floats(n)
    for r, mask in _groups(res, ~np.isnan(ev) & ~np.isnan(nv)):
        cells = _bng.point_to_cell_id_vec(ev[mask], nv[mask], r)
        out[mask] = [_bng.format(int(i)) for i in cells]
    return pd.Series(out)


@pandas_udf(StringType())
def _d_pandas(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    out = np.full(len(e), None, dtype=object)
    ev, nv = _floats(e), _floats(n)
    for r, mask in _groups(res, ~np.isnan(ev) & ~np.isnan(nv)):
        out[mask] = _rs.bng_cells(ev[mask], nv[mask], r).to_numpy(zero_copy_only=False)
    return pd.Series(out)


def _build_d():
    arrow_udf = getattr(
        F, "arrow_udf", None
    )  # PySpark 4.1+: pyarrow in / out, no pandas hop
    if arrow_udf is None:
        return _d_pandas

    @arrow_udf(StringType())
    def _d_arrow(e, n, res):
        import pyarrow as pa
        import pyarrow.compute as pc

        ev = e.fill_null(float("nan")).to_numpy(zero_copy_only=False)
        nv = n.fill_null(float("nan")).to_numpy(zero_copy_only=False)
        uniq = pc.unique(res.drop_null()).to_pylist()
        if len(uniq) == 1 and res.null_count == 0:
            return _rs.bng_cells(ev, nv, _bng.get_resolution(_norm_res(uniq[0])))
        return pa.array(_d_pandas.func(pd.Series(ev), pd.Series(nv), res.to_pandas()))

    return _d_arrow


bng_eastnorthasbng_d = _build_d()
