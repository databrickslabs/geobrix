"""gbx_rst_stats_partial: the mergeable state of each band, the merge across tiles, and what the rows do not say.

The per-tile rows and the merge recipe are RASTER_STAT's (run on every level and on the UC handler by
test_family_cross_tier.py). This file holds the merge against the session's statistics of the union, the mechanisms
behind the rows with a revert that turns them red, and the pinned limits.
"""

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.pyrx.core import accessors, gridagg
from databricks.labs.gbx.uc import tier_b

FN = "gbx_rst_stats_partial"
MAN = manifest.load()


def _tile(name, tmp_path):
    return oracles._build(name, str(tmp_path))


def _states(name, tmp_path):
    return tier_b.rst_stats_partial(_tile(name, tmp_path))


def _merge(*arrays):
    """Chan's update over the per-band states of several tiles: (count, sum, m2, min, max) of each band."""
    out = {}
    for rows in arrays:
        for r in rows:
            if not r["count"]:
                continue
            cur = out.get(r["band"])
            if cur is None:
                out[r["band"]] = (r["count"], r["sum"], r["m2"], r["min"], r["max"])
                continue
            n, s, m2, lo, hi = cur
            n2 = n + r["count"]
            delta = r["sum"] / r["count"] - s / n
            out[r["band"]] = (
                n2,
                s + r["sum"],
                m2 + r["m2"] + delta * delta * n * r["count"] / n2,
                min(lo, r["min"]),
                max(hi, r["max"]),
            )
    return out


def test_the_function_is_a_uc_function_with_its_handler_and_the_state_columns():
    fn = MAN.functions[FN]
    assert manifest.resolve_ref(fn.levels["L4"]["handler"]) is tier_b.rst_stats_partial
    assert fn.returns_uc == (
        "array<struct<band:int,count:double,sum:double,m2:double,min:double,max:double>>"
    )


def test_two_tiles_merge_to_the_session_statistics_of_their_union(tmp_path):
    """The states of the two halves of a tile, merged, give the count, mean, least and greatest the session functions
    give for the whole tile, and a variance equal to numpy's over the union."""
    from rasterio.io import MemoryFile

    whole = oracles._raster_of(_tile("rtg_ll", tmp_path))
    merged = _merge(_states("rtg_ll_west", tmp_path), _states("rtg_ll_east", tmp_path))
    with MemoryFile(whole) as mf, mf.open() as ds:
        avg, lo, hi = accessors.avg(ds), accessors.minimum(ds), accessors.maximum(ds)
        counts = accessors.pixelcount(ds)
        for b in range(1, ds.count + 1):
            n, s, m2, a, z = merged[b]
            assert n == counts[b - 1]
            assert s / n == pytest.approx(avg[b - 1], rel=1e-12)
            assert (a, z) == (lo[b - 1], hi[b - 1])
            v = ds.read(b, masked=True).compressed().astype("float64")
            assert m2 / n == pytest.approx(v.var(), rel=1e-9)


def test_overlapping_tiles_count_a_pixel_twice(tmp_path):
    """Pinned limit: the state has no pixel identity, so a pixel in two tiles is in the count twice."""
    once = _states("rtg_ll_west", tmp_path)
    twice = _merge(once, once)
    for r in once:
        assert twice[r["band"]][0] == 2 * r["count"]
        assert twice[r["band"]][1] == pytest.approx(2 * r["sum"])


def test_the_state_is_the_kernel_of_the_grid_partial_with_the_band_as_the_group(
    monkeypatch, tmp_path
):
    """The one kernel: `gridagg._grouped_measures` makes the state of a band, as it makes the state of a cell."""
    calls = []
    real = gridagg._grouped_measures

    def spy(cids, vals, agg):
        calls.append((len(set(cids.tolist())), agg))
        return real(cids, vals, agg)

    monkeypatch.setattr(gridagg, "_grouped_measures", spy)
    rows = _states("rtg_ll", tmp_path)
    assert len(rows) == 2 and calls == [(1, "partial")] * 2


def test_the_state_changes_when_the_shared_kernel_does(monkeypatch, tmp_path):
    """Fail-on-revert of the shared kernel: a kernel that drops the squared deviations changes `m2`, and the per-tile
    rows (which recompute it with numpy) go red."""
    rows = [
        c
        for c in oracles.TABLES["RASTER_STAT"]
        if c.fn == FN and "uc" in c.surfaces and c.sql is None
    ]
    assert [m for _, m in oracles.run_handlers(rows, tmpdir=str(tmp_path)) if m] == []
    real = gridagg._grouped_measures

    def no_m2(cids, vals, agg):
        uniq, states = real(cids, vals, agg)
        return uniq, [(n, s, 0.0, lo, hi) for n, s, _m, lo, hi in states]

    monkeypatch.setattr(gridagg, "_grouped_measures", no_m2)
    red = {c.cid for c, m in oracles.run_handlers(rows, tmpdir=str(tmp_path)) if m}
    assert {"stats_partial", "stats_partial_two_band", "stats_partial_float32"} <= red


@pytest.fixture(scope="module")
def session(spark):
    register(spark, only=[FN], _levels={FN: ("L3", "c0")})
    return spark


def test_the_merge_recipe_fails_when_the_between_tile_term_is_dropped(
    session, tmp_path
):
    """Fail-on-revert of the Chan update: summing the per-tile `m2` alone under-states the variance of a band that spans
    two tiles, and the recipe's answer leaves the union's."""
    from dataclasses import replace

    recipe = next(
        c
        for c in oracles.TABLES["RASTER_STAT"]
        if c.cid == "recipe_stats_partial_merge"
    )
    assert oracles.run_recipe(session, recipe, "", str(tmp_path)) is None
    term = "try_divide(sum(m2) + sum(count * pow(try_divide(sum, count) - mu, 2)), sum(count))"
    assert term in recipe.sql
    reverted = replace(
        recipe, sql=recipe.sql.replace(term, "try_divide(sum(m2), sum(count))")
    )
    assert "variance" in str(oracles.run_recipe(session, reverted, "", str(tmp_path)))


def test_a_band_with_no_valid_pixel_has_a_zero_count_and_null_state(tmp_path):
    rows = _states("acc_all_nodata", tmp_path)
    assert rows == [
        {"band": 1, "count": 0.0, "sum": None, "m2": None, "min": None, "max": None}
    ]
