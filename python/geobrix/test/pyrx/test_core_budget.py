from databricks.labs.gbx.pyrx.core import budget


def test_resolve_strategy_passthrough():
    assert budget.resolve_strategy("serverless") == "serverless"
    assert budget.resolve_strategy("classic") == "classic"
    assert budget.resolve_strategy("none") == "none"


def test_resolve_auto_is_serverless_or_classic():
    assert budget.resolve_strategy("auto") in ("serverless", "classic")


def test_budget_values():
    # serverless: 96 MiB decoded/tile. Sized against TOTAL worker RSS (not delta):
    # a 96 MiB tile's driver="COG" encode peaks ~514 MiB total locally, leaving
    # headroom for serverless Spark/Arrow/worker overhead under the 1 GB PySpark UDF
    # hard cap. (256 MiB was too large — ~974 MiB total local, OOM'd on serverless.)
    assert budget.decoded_budget_bytes("serverless") == 96 * 1024 * 1024
    assert budget.decoded_budget_bytes("classic") == 1536 * 1024 * 1024
    assert budget.decoded_budget_bytes("none") == 0


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
