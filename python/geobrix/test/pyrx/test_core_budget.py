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
