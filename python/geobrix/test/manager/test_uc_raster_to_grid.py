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


def test_each_table_function_reads_the_one_partial_and_has_no_kernel_of_its_own():
    partial_impl = f"gbx__{H3P[len('gbx_'):]}"
    for stat in SPLIT:
        fn = MAN.functions[f"gbx_rst_h3_rastertogrid{stat}"]
        l4 = fn.levels["L4"]
        assert l4["tvf_over"] == H3P and "handler" not in l4
        (stmt,) = ddl.function_ddl(
            fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
        )
        # one call of the Python function; a generator in a subquery; the projection outside it
        assert stmt.count(partial_impl) == 1 and "LANGUAGE PYTHON" not in stmt
        assert (
            f"FROM (SELECT inline(`c`.`s`.{partial_impl}(tile, resolution, coverage, assignment)))"
            in stmt
        )


def test_the_projected_table_function_text_waits_for_the_workspace():
    """Spec: a SQL table function whose body projects over the generator of a PYTHON function. UC accepts a bare
    `SELECT inline(python_fn(...))` (the other table functions); the projection over it is proven only on the workspace.
    The text is pinned so the pass runs what the host judged."""
    fn = MAN.functions["gbx_rst_h3_rastertogridvariance"]
    (stmt,) = ddl.function_ddl(
        fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
    )
    assert stmt.endswith(
        "RETURN SELECT band, cellid AS cellID, try_divide(m2, count) AS measure "
        "FROM (SELECT inline(`c`.`s`.gbx__rst_h3_rastertogrid_partial(tile, resolution, coverage, assignment)))"
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


# --- the SQL projection is the Python twin ------------------------------------------------------------------------------
STATES = [
    (4.0, 10.0, 5.0, 1.0, 4.0),  # a cell of four pixels
    (1.0, 7.5, 0.0, 7.5, 7.5),  # one pixel: variance 0
    (2.5, 12.5, 3.25, 1.0, 9.0),  # a covering cell: fractional weights
    (0.0, None, None, None, None),  # covered, no valid pixel
]


@pytest.mark.parametrize("stat", SPLIT)
def test_the_sql_projection_of_a_table_function_equals_measure_of_on_every_cell_state(
    spark, stat
):
    """The warehouse runs `tvf_select`; the oracle's handler rows read the measure from `gridagg.measure_of`. They are the
    same function on every state a cell can have, the empty one included (count 0.0, every other measure NULL).
    """
    from pyspark.sql import Row

    select = MAN.functions[f"gbx_rst_h3_rastertogrid{stat}"].levels["L4"]["tvf_select"]
    rows = [
        Row(band=1, cellid=i, **dict(zip(gridagg.PARTIAL_FIELDS, s)))
        for i, s in enumerate(STATES)
    ]
    schema = "band int, cellid bigint, count double, sum double, m2 double, min double, max double"
    spark.createDataFrame(rows, schema).createOrReplaceTempView("_gbx_partial")
    got = {
        r["cellID"]: r["measure"]
        for r in spark.sql(f"SELECT {select} FROM _gbx_partial").collect()
    }
    for i, state in enumerate(STATES):
        want = gridagg.measure_of(stat, state)
        assert got[i] == want or (
            want is not None and abs(got[i] - want) <= 1e-15 * max(1.0, abs(want))
        ), (stat, state, got[i], want)


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
