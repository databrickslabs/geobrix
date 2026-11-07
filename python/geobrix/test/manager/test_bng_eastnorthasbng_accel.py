"""The BNG easting/northing function's acceleration rungs: the Rust build and its fallback, c1 and d held to c0's contract
on every input shape, and the UC batch handler."""

import pytest

from databricks.labs.gbx import _rs, manifest
from databricks.labs.gbx.bench import oracles


def test_uc_batch_handler_answers_the_bng_rows_like_the_scalar_handler():
    """The Rust-backed PARAMETER STYLE PANDAS handler (the scalar one when this wheel has no build)."""
    import pandas as pd

    from databricks.labs.gbx.uc.handlers import bng_eastnorthasbng_batch

    rows = [
        c
        for c in oracles.MVP_S
        if c.fn == "gbx_bng_eastnorthasbng"
        and "uc" in c.surfaces
        and c.kind in ("exact", "null")
    ]
    e, n, r = (list(col) for col in zip(*[c.args for c in rows]))
    res = pd.Series([None if x is None else str(x) for x in r], dtype=object)
    (out,) = bng_eastnorthasbng_batch(
        [(pd.Series(e, dtype="float64"), pd.Series(n, dtype="float64"), res)]
    )
    assert list(out) == [c.expect if c.kind == "exact" else None for c in rows]


def test_a_missing_or_unloadable_rust_build_falls_back_to_python(tmp_path, monkeypatch):
    """The loader says why (no raise), the registrar picks c1, and the batch handler answers row by row."""
    import pandas as pd

    from databricks.labs.gbx.manager import l3
    from databricks.labs.gbx.uc.handlers import bng_eastnorthasbng_batch

    fn = manifest.load().functions["gbx_bng_eastnorthasbng"]
    for build, said in (("none", "no Rust build"), ("corrupt", "failed to load")):
        rs_dir = tmp_path / build
        rs_dir.mkdir()
        if build == "corrupt":
            (rs_dir / "_gbxrs.abi3.so").write_bytes(b"not a shared object")
        monkeypatch.setattr(_rs, "_rs_dir", lambda d=rs_dir: d)
        monkeypatch.setattr(_rs, "_MOD", None)
        monkeypatch.setattr(_rs, "_WHY", None)
        assert said in _rs.unavailable_reason()
        variant, skip = l3.l3_variant(fn)
        assert variant == "c1" and _rs.unavailable_reason() in skip
        cols = (
            pd.Series([530000.0, None]),
            pd.Series([180000.0, 1.0]),
            pd.Series(["3", "3"]),
        )
        assert list(next(bng_eastnorthasbng_batch([cols]))) == ["TQ3080", None]


def test_a_linux_wheel_without_the_native_payload_is_not_an_error(monkeypatch):
    """No `_native` module in the wheel ("cannot import name", an ImportError) means no Rust build, whatever
    payload GBX_NATIVE_ROOT names in this environment."""
    import sys

    import databricks.labs.gbx as gbx

    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(_rs, "_MOD", None)
    monkeypatch.setattr(_rs, "_WHY", None)
    # the wheel without the payload: its `_native` module is absent
    monkeypatch.delattr(gbx, "_native", raising=False)
    monkeypatch.setitem(sys.modules, "databricks.labs.gbx._native", None)
    assert _rs._rs_dir() is None
    assert "no Rust build" in _rs.unavailable_reason()


def _arrow_column(values, all_null=None):
    """A column typed the way Spark hands it to an Arrow UDF: strings, INTs and DOUBLEs."""
    import pyarrow as pa

    if any(isinstance(v, str) for v in values):
        return pa.array(values, pa.string())
    if any(isinstance(v, int) for v in values) and all(
        v is None or isinstance(v, int) for v in values
    ):
        return pa.array(values, pa.int32())
    return pa.array(values, pa.float64() if all_null is None else all_null)


def _bng_batches():
    """Every BNG oracle row as one batch of Arrow columns, plus the shapes only an Arrow UDF sees."""
    import pyarrow as pa

    batches = {}
    for c in oracles.MVP_S:
        if c.fn == "gbx_bng_eastnorthasbng":
            rows = c.args if c.kind == "exact_rows" else (c.args,)
            e, n, r = (list(col) for col in zip(*rows))
            batches[c.cid] = (
                _arrow_column(e),
                _arrow_column(n),
                _arrow_column(r, pa.int32()),
            )
    batches["int_coordinates"] = tuple(  # the SQL literal 530000 is an INT
        _arrow_column(v) for v in ([530000, 325000], [180000, 673000], [3, 3])
    )
    batches["bigint_coordinates_with_null"] = (
        pa.array([530000, None, 325000], pa.int64()),
        pa.array([180000, 5, 673000], pa.int64()),
        pa.array([3, 3, 3], pa.int32()),
    )
    batches["null_coordinates_bad_resolution"] = (
        pa.array([None, None], pa.float64()),
        pa.array([None, 1.0], pa.float64()),
        pa.array(["3km", "3km"]),
    )
    batches["float_resolution_with_null"] = (
        pa.array([530000.0, 530000.0, 325000.0], pa.float64()),
        pa.array([180000.0, 180000.0, 673000.0], pa.float64()),
        pa.array([3.0, None, 3.0], pa.float64()),
    )
    batches["fractional_resolution"] = tuple(
        _arrow_column(v) for v in ([530000.0], [180000.0], [3.5])
    )
    # An infinity is NULL like NaN, even beside a resolution that would raise. A huge finite
    # coordinate is outside GB: no right cell exists, so every rung answers what c0 answers (int64
    # wrap-around, no panic in a debug build). Beyond 2**63 the cast is the CPU's, so those stay out.
    batches["non_finite_coordinates"] = (
        pa.array(
            [float("inf"), float("-inf"), 530000.0, 530000.0, float("nan")],
            pa.float64(),
        ),
        pa.array(
            [180000.0, float("inf"), float("inf"), float("-inf"), float("inf")],
            pa.float64(),
        ),
        pa.array(["3", "3km", "3", "3km", "3"]),
    )
    batches["huge_finite_coordinates"] = (
        pa.array([5e12, -1e13, 1e15, 9e18, 530000.0], pa.float64()),
        pa.array([180000.0, 180000.0, -1e13, 5e12, 9e18], pa.float64()),
        pa.array([3, -3, 6, 1, -1], pa.int32()),
    )
    batches["empty"] = (
        pa.array([], pa.float64()),
        pa.array([], pa.float64()),
        pa.array([], pa.int32()),
    )
    return batches


def _outcome(call):
    import numpy as np

    try:
        with np.errstate(all="ignore"):  # int64 overflow of a huge coordinate warns
            return ("ok", list(call()))
    except Exception as exc:  # noqa: BLE001 - the error is part of the contract
        return ("raises", type(exc).__name__, str(exc))


@pytest.mark.parametrize("build", ["loaded", "load-failed"])
def test_c1_and_d_keep_c0s_contract_on_every_input_shape(build, monkeypatch):
    """c1, d (pandas) and d (the arrow_udf branch of PySpark 4.1+, run through a stub) answer, or raise,
    exactly as c0 does for the same Arrow batches. `load-failed` is a worker whose Rust build is missing.
    """
    from databricks.labs.gbx.pygx import _accel
    from databricks.labs.gbx.pygx import functions as pygx

    if build == "loaded" and _rs.unavailable_reason():
        pytest.skip(_rs.unavailable_reason())
    if build == "load-failed":
        monkeypatch.setattr(_rs, "_MOD", None)
        monkeypatch.setattr(_rs, "_WHY", "stub: no build on this worker")
    monkeypatch.setattr(_accel.F, "arrow_udf", lambda t: lambda f: f, raising=False)
    d_arrow = _accel._build_d()
    assert d_arrow is not _accel._d_pandas, "the arrow branch did not build"
    rungs = {
        "c1": lambda e, n, r: _accel._c1_batch(
            e.to_pandas(), n.to_pandas(), r.to_pandas()
        ),
        "d-pandas": lambda e, n, r: _accel._d_batch(
            e.to_pandas(), n.to_pandas(), r.to_pandas()
        ),
        "d-arrow": lambda e, n, r: d_arrow(e, n, r).to_pylist(),
    }
    differ = {}
    for cid, (e, n, r) in _bng_batches().items():
        want = _outcome(
            lambda: pygx._bng_eastnorthasbng_udf.func(
                e.to_pandas(), n.to_pandas(), r.to_pandas()
            )
        )
        for rung, call in rungs.items():
            got = _outcome(lambda: call(e, n, r))
            if got != want:
                differ[f"{cid}/{rung}"] = (want, got)
    assert differ == {}


def test_a_memory_error_while_loading_is_not_cached(tmp_path, monkeypatch):
    """Resource exhaustion is never "no build": it propagates, and the next call tries again."""

    class Exhausted:
        def __init__(self, *args):
            pass

        def create_module(self, spec):
            return None

        def exec_module(self, module):
            raise MemoryError("while loading")

    (tmp_path / "_gbxrs.abi3.so").write_bytes(b"x")
    monkeypatch.setattr(_rs, "_rs_dir", lambda: tmp_path)
    monkeypatch.setattr(_rs.importlib.machinery, "ExtensionFileLoader", Exhausted)
    monkeypatch.setattr(_rs, "_MOD", None)
    monkeypatch.setattr(_rs, "_WHY", None)
    for _ in range(2):
        with pytest.raises(MemoryError):
            _rs.unavailable_reason()
    assert _rs._WHY is None
