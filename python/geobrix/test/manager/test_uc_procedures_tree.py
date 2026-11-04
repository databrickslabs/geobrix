"""The tree merge of ``gbx_mosaic_by_key`` (and the tree shared with ``gbx_combine_by_key``): the fan-in, the end of the
loop, the one write to the target and the scratch table that is dropped on every path. Procedure bodies are SQL text: the
host asserts the generated text and runs the formula the text relies on (the bucket of each member) in Python. What the
SQL returns on a warehouse is the workspace pass (``bench.probes.run_procedures``), not this file.

Fail-on-revert checks: the fan-in and the termination (a bucket formula that groups more than 8, or leaves a lone
member, or a loop with no cap), the scratch cleanup (no handler, or a handler declared after the first statement).
"""

import math
import re

import pytest

from databricks.labs.gbx.uc import procedures

# the tree procedures that exist (the combine joins the mosaic)
TREES = [
    n for n in ("gbx_mosaic_by_key", "gbx_combine_by_key") if n in procedures.RECIPES
]


def _text(name):
    return procedures.RECIPES[name]("cat", "sch")


def _statements(text):
    """The statements of a procedure body, in order, split at the semicolons that end a line."""
    body = text.split("\nAS BEGIN\n", 1)[1].rsplit("\nEND", 1)[0]
    return [s.strip() for s in re.split(r";\n", body + "\n") if s.strip()]


# --- the tree: fan-in and termination -----------------------------------------------------------------------------


def _bucket_formula(text):
    """The bucket expression of the tree as a Python function of (rn, n), read from the procedure's own text."""
    m = re.search(
        r"\(\(rn - 1\) \* \(\(n \+ (\d+)\) DIV (\d+)\)\) DIV n AS bucket", text
    )
    assert m, "the tree has no bucket formula"
    add, fan = int(m.group(1)), int(m.group(2))
    return fan, lambda rn, n: ((rn - 1) * ((n + add) // fan)) // n


@pytest.mark.parametrize("name", TREES)
def test_the_tree_groups_members_by_at_most_eight_and_never_leaves_a_lone_member(name):
    fan, bucket = _bucket_formula(_text(name))
    assert fan == procedures.TREE_FAN_IN == 8
    for n in list(range(2, 400)) + [4095, 4096, 4097, 65_537, 1_000_003]:
        sizes = {}
        last = -1
        for rn in range(1, n + 1) if n < 5000 else (1, n // 3, n // 2, n):
            b = bucket(rn, n)
            assert b >= last  # contiguous: the order of the members is kept
            last = b
            sizes[b] = sizes.get(b, 0) + 1
        if n < 5000:
            assert max(sizes.values()) <= fan, (n, sizes)
            # a lone member would reach a combine function, which returns a single tile as it is (not a count)
            assert min(sizes.values()) >= 2, (n, sizes)
            assert len(sizes) == math.ceil(n / fan)
        else:
            assert bucket(n, n) + 1 == math.ceil(n / fan)


@pytest.mark.parametrize("name", TREES)
def test_the_tree_levels_end_and_the_loop_is_bounded(name):
    text = _text(name)
    fan, bucket = _bucket_formula(text)
    # a level has ceil(n / fan) rows: the loop ends at one row per key within the cap, for any realistic key
    n, level = 10_000_000, 1
    while n > 1:
        n = bucket(n, n) + 1
        level += 1
    assert level <= procedures.TREE_MAX_LEVELS
    loop = re.search(r"WHILE EXISTS \((.*?)\) DO\n(.*?)END WHILE;", text, re.S)
    assert loop, "no WHILE loop"
    assert "GROUP BY tile_key HAVING count(*) > 1" in loop.group(1)
    assert f"v_lvl >= {procedures.TREE_MAX_LEVELS}" in loop.group(2)
    assert "SET v_lvl = v_lvl + 1;" in loop.group(2)


def test_mosaic_by_key_has_no_tile_count_refusal_but_a_result_size_refusal():
    text = _text("gbx_mosaic_by_key")
    assert "HAVING count(*) > 10" not in text
    assert "has more than 10 tiles" not in text and not hasattr(
        procedures, "MOSAIC_MAX_TILES"
    )
    size = 64 * 1024 * 1024
    assert f"length(p1.raster) > {size}" in text
    refusal = re.search(r"MESSAGE_TEXT = '([^']*over 64 MiB[^']*)'", text).group(1)
    assert "Fix:" in refusal and "mint_vrt" in refusal
    # the members are merged in a fixed order (by content), so a re-run merges the same way
    assert "ORDER BY xxhash64(tile.raster, tile.path)" in text


def test_the_tree_procedures_write_the_target_only_in_one_final_merge():
    for name in TREES:
        text = _text(name)
        # one merge per op in the combine (the branches of one IF), one in the mosaic; nothing else writes the target
        want = 6 if name == "gbx_combine_by_key" else 1
        assert text.count("MERGE INTO IDENTIFIER(target_table)") == want, name
        assert text.count("ON t.tile_key <=> s.tile_key") == want, name
        assert "INSERT INTO IDENTIFIER(target_table)" not in text, name
        # a key with one tile never enters the tree: it goes to the merge as it is
        assert text.count("LEFT ANTI JOIN") == want, name
        assert text.count("ON s.tile_key <=> k.tile_key") == want, name
        # the target is made without calling a function (a column list over the source with no row)
        create = next(
            s for s in _statements(text) if s.startswith("CREATE TABLE IF NOT EXISTS")
        )
        assert "gbx_" not in create and "WHERE false" in create, name


# --- the scratch table is dropped on every path ---------------------------------------------------------------------


@pytest.mark.parametrize("name", TREES)
def test_the_scratch_table_is_dropped_after_an_error_and_after_success(name):
    text = _text(name)
    handler = text.index("DECLARE EXIT HANDLER FOR SQLEXCEPTION")
    # the handler is declared before any statement that creates or fills the scratch table
    assert handler < text.index("CREATE TABLE IDENTIFIER(v_scratch)")
    body = text[handler : text.index("END;", handler)]
    assert "DROP TABLE IF EXISTS IDENTIFIER(v_scratch);" in body
    assert body.rstrip().endswith("RESIGNAL;")  # the caller still gets the error
    assert text.rstrip().endswith("DROP TABLE IF EXISTS IDENTIFIER(v_scratch);\nEND")
    # the scratch table sits beside the target, named once per call
    assert (
        "DECLARE v_scratch STRING DEFAULT concat(target_table, '_scratch_', replace(uuid(), '-', ''));"
        in text
    )
