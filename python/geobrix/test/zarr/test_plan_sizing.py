"""How a read is cut into units: the size target, the task slots, the budget cap, and the time
slabs of a sample. Spark-free; the fixtures are the ERA5-like stores (grid 4 x 2 x 2 chunks).
"""

import dataclasses
import types

import numpy as np
import pytest

pytest.importorskip("zarr")

from databricks.labs.gbx.pyrx.core import budget  # noqa: E402
from databricks.labs.gbx.zarr import _read, _sample  # noqa: E402
from databricks.labs.gbx.zarr._plan import chunk_bytes, plan_units  # noqa: E402
from databricks.labs.gbx.zarr._select import resolve  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402

_HUGE = 10**12


@pytest.fixture
def meta(era5_v3_unsharded):
    _, _, arrays = plain_store_rows(era5_v3_unsharded)
    return arrays["t2m"]


def _axes(meta):
    return resolve(meta.dims, meta.shape, {}, None, None)


def _sizes(meta, **kw):
    return [len(u.chunks) for u in plan_units(meta, _axes(meta), {}, **kw)]


def test_read_unit_bytes_is_the_target_lowered_to_the_slots_under_the_budget():
    mib = 1 << 20
    t, f = budget.ZARR_READ_UNIT_TARGET, budget.ZARR_READ_UNIT_FLOOR
    assert budget.read_unit_bytes(1536 * mib, 10 * 1024 * mib) == t  # big read: target
    assert budget.read_unit_bytes(1536 * mib, 10 * 1024 * mib, slots=8) == t
    assert budget.read_unit_bytes(1536 * mib, 64 * mib, slots=8) == 8 * mib  # per slot
    assert (
        budget.read_unit_bytes(1536 * mib, 8 * mib, slots=8) == f
    )  # never below floor
    assert budget.read_unit_bytes(2 * mib, 10 * 1024 * mib) == 2 * mib  # budget caps
    assert budget.read_unit_bytes(0, mib) == 1  # never zero


def test_units_are_cut_to_the_target_not_the_budget(meta, monkeypatch):
    """A classic-sized budget must not put a whole read in one task."""
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 3 * per)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", 1)
    sizes = _sizes(meta, budget_bytes=_HUGE)
    assert sum(sizes) == 16 and max(sizes) == 3 and len(sizes) == 6


def test_units_fill_the_slots_when_there_are_chunks_for_them(meta, monkeypatch):
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", per)
    assert len(_sizes(meta, budget_bytes=_HUGE, slots=1)) == 1  # 2 MiB: one 16 MiB unit
    assert len(_sizes(meta, budget_bytes=_HUGE, slots=8)) >= 8
    assert len(_sizes(meta, budget_bytes=_HUGE, slots=64)) >= 16  # a chunk a unit


def test_the_budget_still_caps_a_unit(meta, monkeypatch):
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 100 * per)
    assert max(_sizes(meta, budget_bytes=2 * per)) == 2
    assert set(_sizes(meta, budget_bytes=1)) == {
        1
    }  # under one chunk: one chunk, never none
    assert len(_sizes(meta, budget_bytes=_HUGE, slots=1)) == 1


def test_a_table_unit_counts_every_variable(meta, monkeypatch):
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 4 * per)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", 1)
    assert max(_sizes(meta, budget_bytes=_HUGE, weight=1)) == 4
    assert max(_sizes(meta, budget_bytes=_HUGE, weight=2)) == 2


def test_a_chunk_counts_the_cells_inside_the_array(meta, monkeypatch):
    """A 24-step inner chunk of a 1-step array holds one step, not 24."""
    short = dataclasses.replace(meta, shape=(1, *meta.shape[1:]), chunks=(24, 37, 72))
    one_step = 37 * 72 * short.itemsize
    assert chunk_bytes(short, (0, 0, 0)) == one_step
    assert chunk_bytes(short) == 24 * one_step  # the whole inner chunk, as before
    # the 74 x 144 grid is 2 x 2 chunks: four chunks of one step fit three whole-chunk budgets
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 4 * one_step)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", 1)
    assert _sizes(short, budget_bytes=_HUGE) == [4]
    # an edge chunk of a longer array counts only what lies inside the shape
    edge = dataclasses.replace(meta, shape=(30, *meta.shape[1:]), chunks=(24, 37, 72))
    assert chunk_bytes(edge, (1, 0, 0)) == 6 * 37 * 72 * edge.itemsize


@pytest.mark.parametrize(
    "conf, expected",
    [
        ({"spark.default.parallelism": "16"}, 16),
        ({"spark.executor.cores": "4", "spark.executor.instances": "3"}, 12),
        ({"spark.executor.cores": "4"}, 4),
        ({"spark.master": "local[6]"}, 6),
        ({"spark.master": "local[6,2]"}, 6),
        ({"spark.master": "local"}, 1),
        ({"spark.default.parallelism": "auto", "spark.master": "local[3]"}, 3),
        ({}, 1),
    ],
)
def test_task_slots_come_from_session_confs_only(conf, expected):
    spark = types.SimpleNamespace(
        conf=types.SimpleNamespace(get=lambda k, d=None: conf.get(k, d))
    )
    assert _read._task_slots(spark) == expected


def test_task_slots_survive_a_session_that_refuses_a_conf():
    """Spark Connect refuses confs it does not expose; that is no slots, not an error."""

    def refuse(key, default=None):
        raise RuntimeError("CONFIG_NOT_AVAILABLE")

    assert (
        _read._task_slots(types.SimpleNamespace(conf=types.SimpleNamespace(get=refuse)))
        == 1
    )


def _steps_cfg(meta, n=48):
    """The parts of a sample config the time split reads: one other dimension (time)."""
    return types.SimpleNamespace(
        other=(0,),
        steps=np.arange(n, dtype=np.int64)[None, :],
        n_steps=n,
        variables=("t2m",),
        read=types.SimpleNamespace(arrays={"t2m": meta}),
    )


def test_a_sample_is_cut_along_time_at_chunk_boundaries(meta, monkeypatch):
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", 1)
    cfg = _steps_cfg(meta)  # 48 steps in four 12-step chunks
    total, slabs = _sample._split_steps(cfg, 1, 1, _HUGE)
    assert (total, slabs) == (4 * per, ())  # a unit target is far above 4 chunks
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", per)
    _, slabs = _sample._split_steps(cfg, 1, 1, _HUGE)
    assert slabs == ((0, 12), (12, 24), (24, 36), (36, 48))
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 2 * per)
    _, slabs = _sample._split_steps(cfg, 1, 1, _HUGE)
    assert slabs == ((0, 24), (24, 48))
    # four spatial chunks already make four units of two time chunks: no cut needed
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 4 * per)
    assert _sample._split_steps(cfg, 4, 1, _HUGE)[1] == ()
    # slots ask for more units than the target does
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", 100 * per)
    assert len(_sample._split_steps(cfg, 1, 4, _HUGE)[1]) == 4
    # the budget caps a unit below the target
    assert len(_sample._split_steps(cfg, 1, 1, 2 * per)[1]) == 2


def test_the_time_slabs_cover_every_step_once(meta, monkeypatch):
    per = chunk_bytes(meta)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_FLOOR", 1)
    monkeypatch.setattr(budget, "ZARR_READ_UNIT_TARGET", per)
    cfg = _steps_cfg(meta, 30)  # a short last chunk
    _, slabs = _sample._split_steps(cfg, 1, 1, _HUGE)
    cols = np.concatenate([np.arange(a, b) for a, b in slabs])
    assert cols.tolist() == list(range(30)) and len(slabs) == 3
