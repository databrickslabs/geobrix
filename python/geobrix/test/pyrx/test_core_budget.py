import pytest

from databricks.labs.gbx.pyrx.core import budget

_MIB = 1024 * 1024
_1_GIB = 1024 * _MIB


def test_resolve_strategy_passthrough():
    assert budget.resolve_strategy("serverless") == "serverless"
    assert budget.resolve_strategy("classic") == "classic"
    assert budget.resolve_strategy("none") == "none"


def test_resolve_auto_is_serverless_or_classic():
    assert budget.resolve_strategy("auto") in ("serverless", "classic")


def test_budget_values(monkeypatch):
    # serverless: 64 MiB decoded/tile fallback when no cgroup limit detected.
    # Empirically bisected on an 8-core Serverless worker (0.5 GiB striped
    # source, one-tile-per-partition): 96 MiB/tile passes, 128 MiB fails against
    # the 1 GB PySpark UDF cap.  64 MiB gives ~50% margin under the proven-safe 96
    # for multiband/dtype/worker variation.
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: None)
    assert budget.decoded_budget_bytes("serverless") == 64 * 1024 * 1024
    assert budget.decoded_budget_bytes("classic") == 1536 * 1024 * 1024
    assert budget.decoded_budget_bytes("none") == 0


# ---------------------------------------------------------------------------
# cgroup task-limit helper: worker-safe per-task memory probe
# ---------------------------------------------------------------------------


def test_cgroup_v2_max_returns_none(monkeypatch):
    """cgroup v2 'max' sentinel value → None (unlimited)."""

    def _mock_read(path):
        if path == "/sys/fs/cgroup/memory.max":
            return "max"
        raise OSError("no such file")

    monkeypatch.setattr(budget, "_read_cgroup_file", _mock_read)
    assert budget._cgroup_task_limit_bytes() is None


def test_cgroup_v2_numeric_returns_value(monkeypatch):
    """cgroup v2 with a numeric limit → that integer."""

    def _mock_read(path):
        if path == "/sys/fs/cgroup/memory.max":
            return "1073741824"  # 1 GiB
        raise OSError("no such file")

    monkeypatch.setattr(budget, "_read_cgroup_file", _mock_read)
    assert budget._cgroup_task_limit_bytes() == 1073741824


def test_cgroup_v1_numeric_returns_value(monkeypatch):
    """v2 unreadable; cgroup v1 with a finite limit → that integer."""

    def _mock_read(path):
        if path == "/sys/fs/cgroup/memory/memory.limit_in_bytes":
            return "2147483648"  # 2 GiB
        raise OSError("no such file")

    monkeypatch.setattr(budget, "_read_cgroup_file", _mock_read)
    assert budget._cgroup_task_limit_bytes() == 2147483648


def test_cgroup_v1_unlimited_sentinel_returns_none(monkeypatch):
    """cgroup v1 with the 2^63-ish unlimited sentinel → None."""

    def _mock_read(path):
        if path == "/sys/fs/cgroup/memory/memory.limit_in_bytes":
            return "9223372036854771712"  # ~2^63, v1 unlimited sentinel
        raise OSError("no such file")

    monkeypatch.setattr(budget, "_read_cgroup_file", _mock_read)
    assert budget._cgroup_task_limit_bytes() is None


def test_cgroup_oserror_both_returns_none(monkeypatch):
    """Both cgroup files unreadable (OSError) → None, no exception raised."""

    def _mock_read(path):
        raise OSError("no such file")

    monkeypatch.setattr(budget, "_read_cgroup_file", _mock_read)
    assert budget._cgroup_task_limit_bytes() is None


# ---------------------------------------------------------------------------
# decoded_budget_bytes: serverless strategy wired to cgroup limit
# ---------------------------------------------------------------------------


def test_decoded_budget_serverless_with_cgroup_limit(monkeypatch):
    """1 GiB cgroup limit → max(int(1GiB * 0.15), 32 MiB)."""
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: _1_GIB)
    expected = max(int(_1_GIB * 0.15), 32 * _MIB)
    assert budget.decoded_budget_bytes("serverless") == expected


def test_decoded_budget_serverless_without_cgroup_limit(monkeypatch):
    """No cgroup limit (None) → 64 MiB empirical fallback."""
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: None)
    assert budget.decoded_budget_bytes("serverless") == 64 * _MIB


@pytest.mark.parametrize(
    "strategy, kind, limit, ceiling, want",
    [
        # 1/16 of the tile budget: 64 MiB serverless, 1536 MiB classic (under the 128 MiB ceiling)
        ("serverless", None, None, None, 4 * _MIB),
        ("classic", None, None, None, 96 * _MIB),
        ("serverless", None, 64 * _1_GIB, None, 128 * _MIB),  # a big pod: the ceiling
        ("serverless", None, _1_GIB, None, int(_1_GIB * 0.15) // 16),  # follows the pod
        # no tile splitting says nothing about memory: sized for the runtime
        ("none", "serverless", None, None, 4 * _MIB),
        ("none", "classic", None, None, 96 * _MIB),
        # the streaming COG copy's ceiling: the same on both runtimes
        ("serverless", None, None, budget.STREAMING_BLOCK_CACHE_MAX, 4 * _MIB),
        ("classic", None, None, budget.STREAMING_BLOCK_CACHE_MAX, 4 * _MIB),
    ],
)
def test_block_cache_is_a_share_of_the_tile_budget_under_a_ceiling(
    monkeypatch, strategy, kind, limit, ceiling, want
):
    """The size every site that caps GDAL's block cache takes: 1/16 of the decoded bytes a tile
    may take, never above the ceiling (128 MiB; the streaming COG copy passes a lower one).
    """
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: limit)
    if kind:
        monkeypatch.setattr(budget, "runtime_kind", lambda: kind)
    kwargs = {} if ceiling is None else {"ceiling": ceiling}
    assert budget.block_cache_bytes(strategy, **kwargs) == want


def test_a_folds_block_cache_is_never_smaller_than_one_large_block():
    """A fold within a tiny budget (the rows that lower the read cap to a few hundred bytes) still gets 1 MiB of GDAL
    cache, the block of a 512-pixel float32 tile; a real budget gets its 1/16 share, under the ceiling.
    """
    assert budget.block_cache_for(0) == budget.block_cache_for(300) == _MIB
    assert budget.block_cache_for(64 * _MIB) == 4 * _MIB
    assert budget.block_cache_for(64 * _MIB, ceiling=_MIB // 2) == _MIB // 2


def test_none_budget_single_tile():
    plan = budget.plan_layout(2000, 2000, 1, 1, False, None, None, 0)
    assert plan.tiles == [(0, 0, 2000, 2000)]
    assert plan.degraded is False


def test_striped_yields_full_width_row_bands():
    # 10000x10000 uint8 1-band = 100MB decoded; 32MB budget -> row bands.
    plan = budget.plan_layout(10000, 10000, 1, 1, False, None, None, 32 * 1024 * 1024)
    # Every tile spans full width (never column-split) and starts at col 0.
    assert all(t[0] == 0 and t[2] == 10000 for t in plan.tiles)
    # Each band's decoded size <= budget.
    assert all(t[2] * t[3] * 1 * 1 <= 32 * 1024 * 1024 for t in plan.tiles)
    # Row bands tile the full height with no gaps/overlaps.
    assert plan.tiles[0][1] == 0
    assert sum(t[3] for t in plan.tiles) == 10000


def test_decoded_budget_not_encoded():
    # Tiny "encoded" notion is irrelevant: a big decoded raster must split even
    # though a compressed version would be small. plan_layout only sees decoded.
    plan = budget.plan_layout(20000, 20000, 3, 2, False, None, None, 64 * 1024 * 1024)
    assert len(plan.tiles) > 1


def test_tiled_grid_snaps_to_blocks():
    # 4096x4096, 512 blocks. Row-band vs grid: tiled path uses square-ish grid,
    # each tile dim is a multiple of the block size (except final edge tile).
    plan = budget.plan_layout(4096, 4096, 1, 4, True, 512, 512, 16 * 1024 * 1024)
    assert len(plan.tiles) > 1
    for col_off, row_off, w, h in plan.tiles:
        assert col_off % 512 == 0 and row_off % 512 == 0


def test_max_tiles_cap_sets_degraded():
    # Absurdly small budget vs huge raster -> would need >512 tiles -> capped+degraded.
    plan = budget.plan_layout(100000, 100000, 1, 4, False, None, None, 1024)
    assert len(plan.tiles) <= 512
    assert plan.degraded is True


# ---------------------------------------------------------------------------
# the read cap: decoded size from a header, the cap a call carries, the refusal
# ---------------------------------------------------------------------------


def test_grid_bytes_is_the_pixels_times_each_bands_type_size():
    assert budget.grid_bytes(10, 20, ["uint8", "float32", "int16"]) == 200 * (1 + 4 + 2)
    # numpy has no dtype for GDAL's complex integers; the table gives their sizes
    assert budget.grid_bytes(4, 4, ["complex_int16", "complex_int32"]) == 16 * (4 + 8)
    assert budget.grid_bytes(7, 3, []) == 0
    assert budget.grid_bytes("5", "4", ["uint8"]) == 20


def test_the_tile_read_cap_is_the_carried_cap_else_the_process_budget(monkeypatch):
    from databricks.labs.gbx import _gdal_policy

    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: 123)
    monkeypatch.setattr(budget, "process_read_cap", lambda: 456)
    assert budget.tile_read_cap() == 123
    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: None)
    assert budget.tile_read_cap() == 456
    # a carried cap of 0 is a cap, not "none carried"
    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: 0)
    assert budget.tile_read_cap() == 0


def test_a_window_over_the_read_cap_is_refused_from_its_header(monkeypatch):
    monkeypatch.setattr(budget, "tile_read_cap", lambda: 1000)
    budget.refuse_window_over_cap(100, 10, ["uint8"])  # exactly the cap: allowed
    budget.refuse_window_over_cap(10, 10, ["uint8", "uint8"])
    with pytest.raises(budget.WindowOverReadCap) as e:
        budget.refuse_window_over_cap(40, 30, ["uint8"])
    assert str(e.value) == budget.read_cap_refusal(1200, 1000)
    with pytest.raises(budget.WindowOverReadCap):
        budget.refuse_window_over_cap(100, 10, ["uint8", "uint8"])
    assert issubclass(budget.WindowOverReadCap, ValueError)


def test_the_refusal_names_the_decoded_size_and_the_cap_in_whole_units():
    text = budget.read_cap_refusal(5 * _MIB + 17, 2 * _MIB)
    assert "5 MiB decoded" in text and "(2 MiB)" in text and "Fix: retile" in text
    assert budget.size_text(3 * 1024) == "3 KiB"
    assert budget.size_text(_MIB) == "1 MiB"


def test_a_serverless_or_cgroup_worker_keeps_its_own_call_budget(monkeypatch):
    """The Serverless flag and a task cgroup limit are memory signals of the worker's own: a carried cap holds only up
    to its own budget. With neither, the carried cap is the budget (standard access mode).
    """
    from databricks.labs.gbx import _gdal_policy

    for k in (*budget.CLASSIC_MEMORY_ENV, "IS_SERVERLESS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: None)
    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: 4096 * _MIB)
    assert budget.call_decoded_budget() == 4096 * _MIB
    monkeypatch.setenv("IS_SERVERLESS", "true")
    assert budget.call_decoded_budget() == 64 * _MIB
    monkeypatch.delenv("IS_SERVERLESS")
    monkeypatch.setattr(budget, "_cgroup_task_limit_bytes", lambda: _1_GIB)
    assert budget.call_decoded_budget() == int(_1_GIB * 0.15)
    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: 32 * _MIB)
    assert (
        budget.call_decoded_budget() == 32 * _MIB
    )  # a cap lowered on the driver still holds
    monkeypatch.setattr(_gdal_policy, "carried_read_cap", lambda: None)
    assert budget.call_decoded_budget() == int(_1_GIB * 0.15)
