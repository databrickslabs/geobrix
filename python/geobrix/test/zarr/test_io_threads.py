"""The thread pool zarrs decodes on is sized for reads that wait on a Volume."""

import os
import subprocess
import sys
import types

import pytest

pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import _engine, _read  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402

MIB = 1 << 20


@pytest.mark.parametrize(
    "chunk, budget, cpus, expected",
    [
        (1 * MIB, 64 * MIB, 4, 32),  # eight per core
        (1 * MIB, 1536 * MIB, 12, 64),  # capped
        (1 * MIB, 64 * MIB, 12, 32),  # the budget holds 64 chunks, two per thread
        (4 * MIB, 64 * MIB, 4, 8),  # the budget holds sixteen chunks, two per thread
        (64 * MIB, 64 * MIB, 4, 4),  # chunks too big for the share: the core count
        (1 * MIB, 1536 * MIB, 2, 16),
    ],
)
def test_pool_threads_follow_cores_and_the_memory_the_chunks_may_hold(
    chunk, budget, cpus, expected
):
    assert _engine.plain_io_threads(chunk, budget, cpus) == expected


@pytest.fixture
def fresh_pool(monkeypatch):
    monkeypatch.setattr(_engine, "_pool_started", False)
    monkeypatch.setattr(_engine, "_pool_threads", None)
    monkeypatch.delenv(_engine._RAYON_ENV, raising=False)


def test_the_first_open_sizes_the_pool_through_the_environment_and_puts_it_back(
    fresh_pool,
):
    with _engine._zarrs_pool(24):
        assert os.environ[_engine._RAYON_ENV] == "24"
    assert _engine._RAYON_ENV not in os.environ
    assert _engine.zarrs_pool_threads() == 24
    with _engine._zarrs_pool(8):  # the pool exists: its size is fixed
        assert _engine._RAYON_ENV not in os.environ
    assert _engine.zarrs_pool_threads() == 24


def test_a_user_set_pool_size_wins_and_an_open_without_a_hint_leaves_it(
    fresh_pool, monkeypatch
):
    monkeypatch.setenv(_engine._RAYON_ENV, "3")
    with _engine._zarrs_pool(24):
        assert os.environ[_engine._RAYON_ENV] == "3"
    assert os.environ[_engine._RAYON_ENV] == "3"
    assert _engine.zarrs_pool_threads() is None


def test_an_open_without_a_hint_keeps_the_default_size(fresh_pool):
    with _engine._zarrs_pool(None):
        assert _engine._RAYON_ENV not in os.environ
    assert _engine.zarrs_pool_threads() is None


def test_a_task_opens_its_arrays_with_the_hint(era5_v3_sharded, monkeypatch):
    _, _, arrays = plain_store_rows(era5_v3_sharded)
    seen = {}

    def spy(path, name, **kw):
        seen.update(kw)
        raise RuntimeError("stop")

    monkeypatch.setattr(_engine, "open_plain_array", spy)
    cfg = types.SimpleNamespace(
        kind="plain", root=str(era5_v3_sharded), arrays=arrays, access=None
    )
    reader = _read._UnitReader(cfg, types.SimpleNamespace(rows=()))
    with pytest.raises(RuntimeError, match="stop"):
        reader.array("t2m")
    assert seen["meta"] is arrays["t2m"]
    assert seen["budget"] == _read._budget_bytes()


_CHILD = """
import os, sys
from databricks.labs.gbx.zarr import _engine
from databricks.labs.gbx.zarr._testing import plain_store_rows
_, _, arrays = plain_store_rows(sys.argv[1])
hint = sys.argv[2] == "hint"
_engine.open_plain_array(
    sys.argv[1], "t2m", engine="zarr", pipeline="zarrs",
    **({"meta": arrays["t2m"], "budget": 1 << 30} if hint else {}),
)
print(len(os.listdir("/proc/self/task")), _engine.zarrs_pool_threads(), os.environ.get("RAYON_NUM_THREADS"))
"""


@pytest.mark.skipif(not os.path.isdir("/proc/self/task"), reason="counts OS threads")
def test_the_zarrs_pool_really_has_more_threads_with_the_hint(era5_v3_sharded):
    """Fail-on-revert: the pool is process-wide and built at the first open, so it is
    measured in a fresh process, by the threads the process has after the open."""
    pytest.importorskip("zarrs")
    env = {k: v for k, v in os.environ.items() if k != "RAYON_NUM_THREADS"}
    env["PYTHONPATH"] = os.pathsep.join(sys.path)

    def run(mode):
        out = subprocess.run(
            [sys.executable, "-c", _CHILD, str(era5_v3_sharded), mode],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        ).stdout.split()
        return int(out[0]), out[1], out[2]

    plain, plain_size, _ = run("plain")
    hinted, hinted_size, restored = run("hint")
    assert plain_size == "None" and int(hinted_size) > (os.cpu_count() or 1) - 1
    assert hinted - plain >= max(1, int(hinted_size) - (os.cpu_count() or 1)) - 1
    assert restored == "None"  # put back after the open
