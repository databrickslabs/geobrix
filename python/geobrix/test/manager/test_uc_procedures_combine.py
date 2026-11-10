"""``gbx_combine_by_key``: the ops, the combine function of each level, the final division, and the algebra of the partial
tiles (count, sum, sum of squares) run on the real Python cores against the single combine of all members.

Fail-on-revert check: the partial tiles. A wrong denominator, a missing cast to Float64 or a sum of squares taken
without the count makes ``test_the_partial_tiles_combine_to_what_the_session_combine_returns`` fail.
"""

import re

import numpy as np
import pytest

from .test_uc_procedures_tree import _bucket_formula
from .test_uc_procedures_tree import _text as _tree_text

OPS = ("avg", "sum", "count", "min", "max", "stddev")


def _text(name="gbx_combine_by_key"):
    return _tree_text(name)


# --- combine_by_key -------------------------------------------------------------------------------------------------


def test_combine_by_key_takes_the_six_ops_and_refuses_the_others_with_a_fix():
    text = _text()
    assert [n for n, _d in _params(text)] == ["source_table", "target_table", "op"]
    assert "DECLARE v_op STRING DEFAULT lower(trim(op));" in text
    for op in OPS:
        assert f"v_op = '{op}'" in text, op
    refusal = re.search(
        r"v_op = 'median'.*?MESSAGE_TEXT = '([^']*)'", text, re.S
    ).group(1)
    assert "median" in refusal and "gbx_rst_combinemedian" in refusal
    other = re.search(
        r"NOT IN \('avg', 'sum', 'count', 'min', 'max', 'stddev'\) THEN\s+SIGNAL SQLSTATE '22023' "
        r"SET MESSAGE_TEXT = '([^']*)'",
        text,
    )
    assert other and "avg, sum, count, min, max or stddev" in other.group(1)
    # the refusals come before the scratch table exists
    assert text.index("v_op = 'median'") < text.index(
        "CREATE TABLE IDENTIFIER(v_scratch)"
    )


def _params(text):
    head = text.split("\n)\nLANGUAGE", 1)[0]
    return [
        (m.group(1), m.group(0))
        for m in re.finditer(r"^  IN (\w+) [^\n]*$", head, re.M)
    ]


def _chain(text, marker):
    """The IF / ELSEIF branches that follow ``marker`` in the text, as {op: statement}."""
    start = text.index(marker)
    end = text.index("END IF;", start)
    out = {}
    for m in re.finditer(
        r"v_op = '(\w+)' THEN\n(.*?)(?=\n\s*ELSEIF|\Z)", text[start:end], re.S
    ):
        out[m.group(1)] = m.group(2)
    return out


def test_each_op_uses_its_combine_function_at_every_level():
    text = _text()
    q = "`cat`.`sch`."
    first = _chain(text, "-- level 1")
    upper = _chain(text, "-- next level")
    assert set(first) == set(upper) == set(OPS)
    for op, fn in (("min", "combinemin"), ("max", "combinemax"), ("sum", "combinesum")):
        assert f"{q}gbx_rst_{fn}(collect_list(tile)) AS p1" in first[op]
        assert f"{q}gbx_rst_{fn}(collect_list(p1)) AS p1" in upper[op]
    # a count is a count at the first level and a sum of counts above it
    assert f"{q}gbx_rst_combinecount(collect_list(tile)) AS p1" in first["count"]
    assert f"{q}gbx_rst_combinesum(collect_list(p1)) AS p1" in upper["count"]
    # avg and stddev carry floating-point partials (count, sum[, sum of squares]); the members are cast first
    for op, parts in (("avg", 2), ("stddev", 3)):
        assert "gbx_rst_updatetype(tile, 'Float64')" in first[op]
        for p in range(1, parts + 1):
            assert f"collect_list(p{p})) AS p{p}" in upper[op]
        assert f"p{parts + 1}" not in upper[op]
    assert "gbx_rst_mapalgebra(array(u), 'A * A')" in first["stddev"]
    assert "combinecount(collect_list(u))" in first["avg"]


def _final_expr(op):
    text = _text()
    chain = _chain(text, "-- result")
    m = re.search(r"gbx_rst_mapalgebra\(array\([^)]*\), '([^']*)'\)", chain[op])
    return m.group(1) if m else None


def test_the_final_step_divides_the_partials_and_leaves_the_others_as_they_are():
    assert _final_expr("avg") == "B / A"
    assert _final_expr("stddev") == "sqrt(abs(C / A - (B / A) ^ 2))"
    for op in ("sum", "count", "min", "max"):
        assert _final_expr(op) is None


# --- the partial algebra, run on the real cores -------------------------------------------------------------------


def _gtiff(arr, nodata=-9999.0, dtype="float32"):
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=arr.shape[-1],
            height=arr.shape[-2],
            count=1,
            dtype=dtype,
            nodata=nodata,
            crs="EPSG:3857",
            transform=from_origin(0.0, 40.0, 10.0, 10.0),
        ) as dst:
            dst.write(
                arr.astype(dtype)[np.newaxis] if arr.ndim == 2 else arr.astype(dtype)
            )
        return mf.read()


def _pixels(blob):
    from rasterio.io import MemoryFile

    with MemoryFile(blob) as mf, mf.open() as ds:
        data = ds.read(1).astype("float64")
        mask = ds.read_masks(1) != 0
    return np.where(mask, data, np.nan)


def _members(n, seed):
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(n):
        arr = rng.integers(0, 50, size=(4, 4)).astype("float64")
        arr[rng.random((4, 4)) < 0.15] = (
            -9999.0
        )  # some pixels with no data in some members
        arr[0, 0] = -9999.0  # one pixel with no data in every member
        out.append(arr)
    return out


def _tree(op, tiles):
    """The tree of ``gbx_combine_by_key`` for one key, level by level, with the cores behind the UC functions."""
    from databricks.labs.gbx.pyrx.core import agg

    fan, bucket = _bucket_formula(_text())
    simple = {
        "min": agg.combine_min_tiles,
        "max": agg.combine_max_tiles,
        "sum": agg.combine_sum_tiles,
    }

    def groups(rows):
        n = len(rows)
        by = {}
        for rn, row in enumerate(rows, 1):
            by.setdefault(bucket(rn, n), []).append(row)
        return [by[b] for b in sorted(by)]

    def f64(arr):
        return _gtiff(arr, dtype="float64")

    if op in simple:
        rows = [(_gtiff(a),) for a in tiles]
        level = lambda g: (simple[op]([r[0] for r in g]),)  # noqa: E731
        up = level
    elif op == "count":
        rows = [(_gtiff(a),) for a in tiles]
        level = lambda g: (agg.combine_count_tiles([r[0] for r in g]),)  # noqa: E731
        up = lambda g: (agg.combine_sum_tiles([r[0] for r in g]),)  # noqa: E731
    else:
        rows = [(f64(a),) for a in tiles]  # gbx_rst_updatetype(tile, 'Float64')

        def level(g):
            u = [r[0] for r in g]
            parts = [agg.combine_count_tiles(u), agg.combine_sum_tiles(u)]
            if op == "stddev":
                sq = [_mapalgebra([x], "A * A") for x in u]
                parts.append(agg.combine_sum_tiles(sq))
            return tuple(parts)

        def up(g):
            return tuple(
                agg.combine_sum_tiles([r[p] for r in g]) for p in range(len(g[0]))
            )

    first, depth = True, 0
    while len(rows) > 1:
        rows = [(level if first else up)(g) for g in groups(rows)]
        first, depth = False, depth + 1
    assert depth >= 1
    final = _final_expr(op)
    if final is None:
        return rows[0][0]
    # count is A, the sum B, the sum of squares C
    return _mapalgebra(list(rows[0]), final)


def _mapalgebra(rasters, expression):
    """gbx_rst_mapalgebra as the procedure calls it on UC: the engine's UC handler (GDAL's raster calc, muparser)."""
    from databricks.labs.gbx.bench.oracles import _tile
    from databricks.labs.gbx.engine import uc

    return uc.rst_mapalgebra([_tile(r) for r in rasters], expression)["raster"]


def _flat(op, tiles):
    from databricks.labs.gbx.pyrx.core import agg

    fn = {
        "avg": lambda r: agg.combineavg_tiles(r),
        "sum": agg.combine_sum_tiles,
        "count": agg.combine_count_tiles,
        "min": agg.combine_min_tiles,
        "max": agg.combine_max_tiles,
        "stddev": agg.combine_stddev_tiles,
    }[op]
    return fn([_gtiff(a) for a in tiles])


@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("n", [2, 9, 23, 70])
def test_the_partial_tiles_combine_to_what_the_session_combine_returns(op, n):
    """One key of ``n`` aligned members with holes: the levels of the tree equal the single combine of all of them."""
    from databricks.labs.gbx.engine import run

    if op in ("avg", "stddev") and run.unavailable_reason():
        pytest.skip(
            f"the final step is gbx_rst_mapalgebra, the GDAL spec engine: {run.unavailable_reason()}"
        )
    tiles = _members(n, seed=n)
    got, want = _pixels(_tree(op, tiles)), _pixels(_flat(op, tiles))
    assert np.array_equal(np.isnan(got), np.isnan(want)), (op, n)
    np.testing.assert_allclose(got, want, rtol=1e-5, atol=1e-4, equal_nan=True)
    assert np.isnan(got[0, 0])  # the pixel no member has stays without data


def test_an_avg_without_the_float_cast_would_not_match_the_session_on_wide_sums():
    """Why the members are cast to Float64 first: eight uint8 members of 200 sum past 255 in their own type."""
    from databricks.labs.gbx.pyrx.core import agg

    tiles = [_gtiff(np.full((4, 4), 200), nodata=None, dtype="uint8") for _ in range(8)]
    wrapped = _pixels(agg.combine_sum_tiles(tiles))
    assert not np.allclose(wrapped, 1600.0)  # the sum in the member type is wrong
    assert np.allclose(
        _pixels(agg.combineavg_tiles(tiles)), 200.0
    )  # the session mean is not
