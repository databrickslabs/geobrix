"""The H3 raster-to-grid partial and its table functions: what the oracle rows (bench/oracle_tables/raster_to_grid.py) do
not say. The rows hold the values, the refusals and the merge across tiles; this file holds the mechanisms: one pass
serves every statistic, the SQL projection equals its Python twin on every state a cell can have, the merge needs
Chan's term, the cap refuses and never truncates, and the DDL text that only the workspace can run.
"""

import itertools

import h3
import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracle_tables import raster_to_grid as rtg
from databricks.labs.gbx.pyrx.core import gridagg
from databricks.labs.gbx.uc import ddl, tier_b

MAN = manifest.load()
H3P = "gbx_rst_h3_rastertogrid_partial"
SPLIT = rtg.H3_SPLIT


def _open(build, tmp_path):
    from rasterio.io import MemoryFile

    tile = oracles._build(build, str(tmp_path))
    return MemoryFile(tile["raster"]), tile


@pytest.fixture
def ll(tmp_path):
    mf, tile = _open("rtg_ll", tmp_path)
    with mf, mf.open() as ds:
        yield ds, tile


# --- one pass serves every statistic ------------------------------------------------------------------------------------
@pytest.mark.parametrize("stat", SPLIT)
def test_a_splitting_statistic_is_one_partial_pass_and_the_median_has_its_own(
    ll, monkeypatch, stat
):
    """Every statistic but the median is read from the partial state of ONE pass over the pixels; reverting a statistic
    to a pass of its own makes this fail (the pass would be asked for `stat`, not for the state).
    """
    ds, _ = ll
    passes = []
    real = gridagg._raster_to_grid

    def spy(ds_, resolution, grid, agg, *a, **kw):
        passes.append(agg)
        return real(ds_, resolution, grid, agg, *a, **kw)

    monkeypatch.setattr(gridagg, "_raster_to_grid", spy)
    gridagg.raster_to_grid(ds, 5, "h3", stat)
    assert passes == ["partial"]
    passes.clear()
    gridagg.raster_to_grid_partial(ds, 5, "h3")
    assert passes == ["partial"]
    passes.clear()
    gridagg.raster_to_grid(ds, 5, "h3", "median")
    assert passes == ["median"]  # the median needs the values: its own pass


@pytest.mark.parametrize("stat", SPLIT)
@pytest.mark.parametrize(
    "coverage,assignment",
    list(itertools.product(("complete", "sparse"), ("centroid", "covering"))),
)
def test_the_statistics_read_from_the_partial_are_the_values_of_the_pass_of_their_own(
    ll, stat, coverage, assignment
):
    """What the session gave before the partial existed (a pass per statistic) and what it gives now are the same
    numbers, bit for bit, for every coverage and assignment."""
    ds, _ = ll
    # a coarse resolution keeps the per-pixel covering pass short
    now = gridagg.raster_to_grid(ds, 3, "h3", stat, coverage, assignment)
    before = gridagg._raster_to_grid(ds, 3, "h3", stat, coverage, assignment)
    assert now == before


GRIDS = (
    "h3",
    "quadbin",
    "bng",
    "custom",
)  # every grid has a kernel of its own for each statistic
ALL_STATS = rtg.STATS


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("stat", ALL_STATS)
def test_each_table_function_is_inline_of_a_private_routine_of_its_own_statistic(
    grid, stat
):
    fn = MAN.functions[f"gbx_rst_{grid}_rastertogrid{stat}"]
    private = f"gbx__rst_{grid}_rastertogrid{stat}_cells"
    l4 = fn.levels["L4"]
    assert l4["tvf_over"] == private and "handler" not in l4 and "tvf_select" not in l4
    assert MAN.private[private].handler.endswith(
        f":rst_{grid}_rastertogrid{stat}_cells"
    )
    (stmt,) = ddl.function_ddl(
        fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
    )
    args = ", ".join(p.name for p in fn.params)
    assert stmt.endswith(f"RETURN SELECT inline(`c`.`s`.{private}({args}))")
    assert "LANGUAGE PYTHON" not in stmt


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("stat", ALL_STATS)
def test_a_table_function_makes_only_its_own_statistic(ll, monkeypatch, grid, stat):
    """The pass of a table function is asked for `stat` and for nothing else: not for the partial state (count, sum, M2, min
    and max), which costs about twice a sum or a count. Reverting the routine to read the partial makes this fail.
    """
    _, tile = (
        ll if grid in ("h3", "quadbin") else (None, oracles._build("rtg_bng", "/tmp"))
    )
    passes = []
    real = gridagg._raster_to_grid

    def spy(ds_, resolution, grid_, agg, *a, **kw):
        passes.append(agg)
        return real(ds_, resolution, grid_, agg, *a, **kw)

    monkeypatch.setattr(gridagg, "_raster_to_grid", spy)
    handler = manifest.resolve_ref(
        MAN.private[f"gbx__rst_{grid}_rastertogrid{stat}_cells"].handler
    )
    rows = handler(*_args(grid, tile))
    assert rows and passes == [stat]


def _args(grid, tile):
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import CUSTOM_GRID as G

    return {
        "h3": (tile, 5, "complete", "centroid"),
        "quadbin": (tile, 11, "complete", "centroid"),
        "bng": (tile, "1km", "complete", "centroid"),
        "custom": (tile, G, 1, "complete", "centroid"),
    }[grid]


# (the tile, the resolution, the grid argument) of a coarse call per grid: the covering pass is per pixel
_COARSE = {
    "h3": ("rtg_ll", 3, None),
    "quadbin": ("rtg_ll", 8, None),
    "bng": ("rtg_bng", 2, None),
    "custom": ("rtg_bng", 0, "G"),
}


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("stat", ALL_STATS)
@pytest.mark.parametrize(
    "coverage,assignment",
    list(itertools.product(("complete", "sparse"), ("centroid", "covering"))),
)
def test_the_single_statistic_pass_is_the_session_value_bit_for_bit(
    tmp_path, grid, stat, coverage, assignment
):
    """`raster_to_grid_single` (a pass of the statistic alone) and `raster_to_grid` (the partial state, or the median's own
    pass) give the same cells and the same doubles, for every grid."""
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import CUSTOM_GRID

    build, res, with_grid = _COARSE[grid]
    mf, _ = _open(build, tmp_path)
    kw = {"grid_conf": CUSTOM_GRID} if with_grid else {}
    with mf, mf.open() as ds:
        assert gridagg.raster_to_grid_single(
            ds, res, grid, stat, coverage, assignment, **kw
        ) == gridagg.raster_to_grid(ds, res, grid, stat, coverage, assignment, **kw)


@pytest.mark.parametrize("grid", ("quadbin", "bng", "custom"))
def test_a_partial_is_public_for_each_other_grid_with_the_h3_shape(grid):
    fn = MAN.functions[f"gbx_rst_{grid}_rastertogrid_partial"]
    names = [p.name for p in fn.params]
    assert names == (
        ["tile", "grid", "resolution", "coverage", "assignment"]
        if grid == "custom"
        else ["tile", "resolution", "coverage", "assignment"]
    )
    cell = "string" if grid == "bng" else "bigint"
    assert fn.returns_uc == (
        f"array<struct<band:int,cellid:{cell},count:double,sum:double,m2:double,min:double,max:double>>"
    )
    assert fn.levels["L4"]["handler"].endswith(f":rst_{grid}_rastertogrid_partial")


@pytest.mark.parametrize("grid", ("quadbin", "bng", "custom"))
def test_the_count_and_average_of_two_tiles_merge_to_the_session_statistic(
    spark, tmp_path, grid
):
    """The count and the average across two halves of a tile, merged by a native GROUP BY, equal the statistic of the whole
    (the recipe rows hold the same claim on the warehouse)."""
    p = f"gbx_rst_{grid}_rastertogrid_partial"
    register(spark, only=[p], _levels={p: ("L3", "c0")})
    short = rtg._SHORT[grid]
    for stat in ("count", "avg"):
        case = next(
            c for c in rtg.RASTER_TO_GRID if c.cid == f"recipe_{short}_merge_{stat}"
        )
        assert oracles.run_recipe(spark, case, "", str(tmp_path)) is None, case.cid


def test_the_partial_stays_public_and_unchanged_for_the_several_statistic_route():
    fn = MAN.functions[H3P]
    assert fn.levels["L4"]["handler"].endswith(":rst_h3_rastertogrid_partial")
    assert fn.kind == "scalar" and [p.name for p in fn.params] == [
        "tile", "resolution", "coverage", "assignment",
    ]  # fmt: skip


def test_the_table_function_text_has_the_defaults_and_the_columns():
    fn = MAN.functions["gbx_rst_h3_rastertogridvariance"]
    (stmt,) = ddl.function_ddl(
        fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
    )
    assert stmt.endswith(
        "RETURN SELECT inline(`c`.`s`.gbx__rst_h3_rastertogridvariance_cells(tile, resolution, coverage, assignment))"
    )
    assert "RETURNS TABLE (band INT, cellID BIGINT, measure DOUBLE)" in stmt
    assert (
        "coverage STRING DEFAULT 'complete', assignment STRING DEFAULT 'centroid')"
        in stmt
    )


def test_tvf_select_needs_the_rows_it_projects():
    raw = {
        "sql_name": "gbx_x",
        "kind": "generator",
        "levels": {"L4": {"handler": "a:b", "tvf_select": "band"}},
    }
    with pytest.raises(ValueError, match="tvf_select needs tvf_over"):
        manifest._check_sql_levels("gbx_x", raw["levels"], "generator")


# --- the merge across tiles needs Chan's term ----------------------------------------------------------------------------
def test_dropping_the_between_tiles_term_fails_the_merge_row(spark, tmp_path):
    """The recipe's variance is `(sum(m2) + sum(count * (mean - mu)^2)) / sum(count)`. Without the second sum the answer
    is the average of the tiles' variances: the oracle row for the merged variance fails, so the row sees the term.
    """
    register(spark, only=[H3P], _levels={H3P: ("L3", "c0")})
    good = next(c for c in rtg.RASTER_TO_GRID if c.cid == "recipe_h3_merge_variance")
    assert (
        oracles.run_recipe(spark, good, "", str(tmp_path)) is None
    )  # a session partial stands in for the UC one
    term = "+ sum(count * pow(try_divide(sum, count) - mu, 2))"
    assert term in good.sql
    reverted = oracles.replace(good, sql=good.sql.replace(term, ""))
    msg = oracles.run_recipe(spark, reverted, "", str(tmp_path))
    assert msg and "differ" in msg


# --- the size limits: refused, never truncated ---------------------------------------------------------------------------
def test_a_partial_over_the_array_cap_is_refused_with_a_fix_and_never_cut(
    monkeypatch, ll
):
    ds, tile = ll
    from databricks.labs.gbx.uc import handlers

    whole = tier_b.rst_h3_rastertogrid_partial(tile, 5, "complete", "centroid")
    assert whole and len(whole) > 2
    monkeypatch.setattr(handlers, "_array_cap", lambda: 2 * tier_b._PARTIAL_ROW_BYTES)
    with pytest.raises(ValueError, match=r"over the .*Fix: use a coarser resolution"):
        tier_b.rst_h3_rastertogrid_partial(tile, 5, "complete", "centroid")
    monkeypatch.undo()
    assert tier_b.rst_h3_rastertogrid_partial(tile, 5, "complete", "centroid") == whole


# the tile that covers a very large number of cells at a fine resolution, per grid: (the tile, the call after it)
_FINE = {
    "h3": ("rtg_ll_world", (15, "complete", "centroid")),
    "quadbin": ("rtg_ll_world", (20, "complete", "centroid")),
    "bng": ("rtg_bng_wide", (6, "complete", "centroid")),
    "custom": ("rtg_bng_wide", (8, "complete", "centroid")),
}


@pytest.mark.parametrize("grid", GRIDS)
@pytest.mark.parametrize("routine", ("cells", "partial"))
def test_a_fine_resolution_over_a_big_extent_is_refused_before_any_pixel_is_binned(
    monkeypatch, tmp_path, grid, routine
):
    """The cells a tile can make are bounded from its extent and the grid's cell size BEFORE the pass, for every grid: the
    pass is never run (the binning is not called) and the refusal has the shape and Fix of the check after it. Dropping
    the estimate for a grid runs the pass first: the spy records it, and this fails."""
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import CUSTOM_GRID

    tile = oracles._build(_FINE[grid][0], str(tmp_path))
    res, *rest = _FINE[grid][1]
    passes = []

    def spy(
        *a, **kw
    ):  # (the tile reader turns a raised error into a NULL, so it records and gives no cells)
        passes.append(a)
        return []

    monkeypatch.setattr(gridagg, "_raster_to_grid", spy)
    if routine == "cells":
        ref = MAN.private[f"gbx__rst_{grid}_rastertogridcount_cells"].handler
        name = f"gbx_rst_{grid}_rastertogridcount"  # the function the Fix names
    else:
        name = f"gbx_rst_{grid}_rastertogrid_partial"
        ref = MAN.functions[name].levels["L4"]["handler"]
    handler = manifest.resolve_ref(ref)
    args = (tile, CUSTOM_GRID, res, *rest) if grid == "custom" else (tile, res, *rest)
    with pytest.raises(
        ValueError,
        match=rf"{name}: the cells of the tile \(an upper estimate\) is .*, over the .*"
        r"Fix: use a coarser resolution, or retile the raster first \(gbx_rst_retile\)",
    ):
        handler(*args)
    assert passes == []


def test_a_tile_decoded_over_the_cap_is_refused_and_a_virtual_tile_too(monkeypatch, ll):
    _, tile = ll
    monkeypatch.setattr(tier_b, "DECODED_CAP", 1024)
    with pytest.raises(ValueError, match=rf"{H3P}: the tile is .* decoded, over the"):
        tier_b.rst_h3_rastertogrid_partial(tile, 5, "complete", "centroid")
    monkeypatch.undo()
    virtual = {**tile, "raster": None, "path": "/Volumes/c/s/v/a.tif"}
    with pytest.raises(ValueError, match="virtual tiles"):
        tier_b.rst_h3_rastertogrid_partial(virtual, 5, "complete", "centroid")


def test_a_bad_argument_raises_before_the_tile_is_read(ll):
    _, tile = ll
    for args in (
        (99, "complete", "centroid"),
        (5, "sideways", "centroid"),
        (5, "complete", "sideways"),
    ):
        with pytest.raises(ValueError, match="Fix: pass a resolution"):
            tier_b.rst_h3_rastertogrid_partial({**tile, "raster": b"junk"}, *args)
    assert (
        tier_b.rst_h3_rastertogrid_partial({**tile, "raster": b"junk"}, 5, None, None)
        is None
    )  # unreadable bytes are NULL (the defaults a NULL means)


def test_a_tile_across_the_antimeridian_has_a_positive_estimate_not_under_the_cells():
    """EPSG:32660 from x 600000 to 1000000 reaches past 180 degrees: transform_bounds gives west > east, which would make
    the width negative and the pre-check blind."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.arange(1600, dtype="float32").reshape(1, 40, 40)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=40,
            height=40,
            count=1,
            dtype="float32",
            crs="EPSG:32660",
            transform=from_origin(600000, 5000000, 10000, 10000),
        ) as w:
            w.write(data)
        with mf.open() as ds:
            west, south, east, north = tier_b._lonlat_bounds(ds)
            assert 0 < east - west <= 360  # was negative
            # the kernel cannot bin a tile across the antimeridian, so the cells are those of the pixel centres
            from rasterio.warp import transform

            cols, rows = np.meshgrid(np.arange(40) + 0.5, np.arange(40) + 0.5)
            xs, ys = ds.transform * (cols.ravel(), rows.ravel())
            lons, lats = transform(ds.crs, "EPSG:4326", list(xs), list(ys))
            for res in (2, 3):
                bound = tier_b.h3_cover_cells_bound(ds, res, "complete", "centroid")
                made = len(
                    {h3.latlng_to_cell(la, lo, res) for lo, la in zip(lons, lats)}
                )
                assert bound > 0 and bound >= made, (res, bound, made)
