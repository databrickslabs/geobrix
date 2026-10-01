"""Tests for the budget_for() authority in pyrx/core/budget.py.

Phase 1 covers driver_merge intent only.  Duplicate coverage of the
_merge_budget_bytes helper in test_lidar_writer.py is intentional — Task 2
removes those originals once the lidar writer delegates here.
"""

import pytest

_MIB = 1024 * 1024
_MID_CLOUD_BYTES = 1024 * _MIB  # ~1 GiB mid-size cloud


def _patch_infra(monkeypatch, avail_mb, gpu):
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: {
            "host_ram_available_mb": avail_mb,
            "host_ram_mb": avail_mb,
            "gpu_count": gpu,
            "per_gpu_vram_mb": 0,
            "cores": 8,
        },
    )


def test_driver_merge_override_wins(monkeypatch):
    from databricks.labs.gbx.pyrx.core.budget import budget_for

    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: (_ for _ in ()).throw(RuntimeError("probe must not be called")),
    )
    d = budget_for("driver_merge", 0, override_mb="100")
    assert d.budget_bytes == 100 * _MIB


def test_driver_merge_high_ram_gpu_large_budget(monkeypatch):
    from databricks.labs.gbx.pyrx.core.budget import _MERGE_FALLBACK_BYTES, budget_for

    _patch_infra(monkeypatch, 64 * 1024, 2)
    d = budget_for("driver_merge", _MID_CLOUD_BYTES, override_mb=None)
    assert d.action == "ok"
    assert d.budget_bytes > _MERGE_FALLBACK_BYTES
    assert d.budget_bytes > _MID_CLOUD_BYTES


def test_driver_merge_tiny_ram_floor_and_error(monkeypatch):
    from databricks.labs.gbx.pyrx.core.budget import _MERGE_FALLBACK_BYTES, budget_for

    _patch_infra(monkeypatch, 1024, 0)  # 1 GiB < 2 GiB reserve → usable 0 → floor
    d = budget_for("driver_merge", _MID_CLOUD_BYTES, override_mb=None)
    assert d.budget_bytes == _MERGE_FALLBACK_BYTES
    assert d.action == "error"  # 1 GiB cloud > 512 MiB floor
    assert "exceeds the compute-aware budget" in d.reason


def test_driver_merge_probe_failure_floor(monkeypatch):
    from databricks.labs.gbx.pyrx.core.budget import _MERGE_FALLBACK_BYTES, budget_for

    def _raise():
        raise RuntimeError("nvidia-smi: command not found")

    monkeypatch.setattr("databricks.labs.gbx.pyrx.mvs.gpu_infra", _raise)
    d = budget_for("driver_merge", 0, override_mb=None)
    assert d.budget_bytes == _MERGE_FALLBACK_BYTES


def test_driver_merge_guard_raises_in_task_context(monkeypatch):
    from databricks.labs.gbx.pyrx.core import budget as _b

    _patch_infra(monkeypatch, 64 * 1024, 0)

    class _FakeTaskContext:
        @staticmethod
        def get():
            return object()  # non-None → simulate executor/task context

    monkeypatch.setattr(_b, "_import_task_context", lambda: _FakeTaskContext)
    with pytest.raises(RuntimeError) as ei:
        _b.budget_for("driver_merge", 0)
    assert "driver-only" in str(ei.value)


def test_unknown_intent_raises():
    from databricks.labs.gbx.pyrx.core.budget import budget_for

    with pytest.raises(ValueError):
        budget_for("worker_read", 0)  # Phase 2 intent — not implemented in Phase 1


# --- dense_alloc intent (Task 1) ---

_INFRA_GPU1 = {
    "host_ram_available_mb": 30 * 1024,  # 30 GiB available
    "host_ram_mb": 32 * 1024,
    "gpu_count": 1,
    "per_gpu_vram_mb": 24000,
    "cores": 16,
}


def test_dense_alloc_injected_infra():
    from databricks.labs.gbx.pyrx.core.budget import budget_for

    d = budget_for("dense_alloc", infra=_INFRA_GPU1)
    # usable = max(0, 30*1024 - 6*1024) MiB = 24*1024 MiB
    assert d.action == "ok"
    assert d.budget_bytes == (30 * 1024 - 6 * 1024) * _MIB


def test_dense_alloc_driver_guard(monkeypatch):
    from databricks.labs.gbx.pyrx.core import budget as _b

    class _FakeTaskContext:
        @staticmethod
        def get():
            return object()  # non-None → simulate executor

    monkeypatch.setattr(_b, "_import_task_context", lambda: _FakeTaskContext)
    with pytest.raises(RuntimeError) as ei:
        _b.budget_for("dense_alloc", infra=_INFRA_GPU1)
    assert "driver-only" in str(ei.value)


def test_dense_alloc_override_mb_wins(monkeypatch):
    from databricks.labs.gbx.pyrx.core.budget import budget_for

    # override_mb must win without touching the infra probe
    monkeypatch.setattr(
        "databricks.labs.gbx.pyrx.mvs.gpu_infra",
        lambda: (_ for _ in ()).throw(RuntimeError("probe must not be called")),
    )
    d = budget_for("dense_alloc", override_mb="200")
    assert d.budget_bytes == 200 * _MIB
    assert d.action == "ok"
