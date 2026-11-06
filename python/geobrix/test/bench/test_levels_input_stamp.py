"""An input table's name holds a stamp of its recipe, builder and corpus: a changed generator or corpus is another table."""

import types

from databricks.labs.gbx.bench import levels_forms as lf


class FakeSpark:
    def __init__(self, existing=()):
        self.tables, self.written, self.sql_calls = set(existing), [], []
        spark = self

        class Writer:
            def __init__(self, df):
                self.df = df

            def format(self, f):
                return self

            def saveAsTable(self, name):
                spark.written.append(name)
                spark.tables.add(name)

        self.Writer = Writer
        self.catalog = types.SimpleNamespace(tableExists=lambda n: n in spark.tables)

    def sql(self, q):
        self.sql_calls.append(q)

    def df(self):
        return types.SimpleNamespace(write=self.Writer(None))


def builder_a(spark, n, corpus, schema):
    return lf._table(
        spark, schema, f"mvp_in_a_v{lf.RECIPE_VERSION}_{n}", lambda: spark.df()
    )


def builder_b(spark, n, corpus, schema):
    # a different recipe body: the same input name, other code
    return lf._table(
        spark, schema, f"mvp_in_a_v{lf.RECIPE_VERSION}_{n}", lambda: spark.df()
    )


def builder_rows(spark, n, corpus, schema):
    lf._corpus_rows(corpus)  # reads the corpus rows: its tiles are part of the stamp
    return lf._table(
        spark, schema, f"mvp_in_r_v{lf.RECIPE_VERSION}_{n}", lambda: spark.df()
    )


def test_the_same_builder_and_corpus_share_one_table_and_a_second_form_builds_nothing():
    spark = FakeSpark()
    first = builder_a(spark, 10, "/c", "c.s")
    again = builder_a(spark, 10, "/c", "c.s")
    assert (
        first == again
        and first.startswith("c.s.mvp_in_a_v")
        and spark.written == [first]
    )  # built once


def test_a_changed_builder_or_recipe_version_is_another_table_and_the_old_one_is_left_alone(
    monkeypatch,
):
    spark = FakeSpark()
    a, b = builder_a(spark, 10, "/c", "c.s"), builder_b(spark, 10, "/c", "c.s")
    assert (
        a != b and a.rsplit("_", 1)[0] == b.rsplit("_", 1)[0]
    )  # the same input, two stamps
    monkeypatch.setattr(lf, "RECIPE_VERSION", lf.RECIPE_VERSION + 1)
    assert builder_a(spark, 10, "/c", "c.s") not in (a, b)
    assert (
        spark.written and not spark.sql_calls
    )  # nothing was dropped: other runs may be reading the old tables
    assert {a, b} <= spark.tables


def test_a_builder_that_reads_the_corpus_follows_its_tiles_and_one_that_does_not_ignores_them(
    monkeypatch,
):
    monkeypatch.setattr(
        lf, "_corpus_rows", lambda corpus: ["/c/rows/1.tif", "/c/rows/2.tif"]
    )
    lf._STAMP_CORPUS[0] = "/c"
    spark = FakeSpark()
    before_r, before_a = builder_rows(spark, 10, "/c", "c.s"), builder_a(
        spark, 10, "/c", "c.s"
    )
    monkeypatch.setattr(
        lf,
        "_corpus_rows",
        lambda corpus: ["/c/rows/1.tif", "/c/rows/2.tif", "/c/rows/3.tif"],
    )
    assert (
        builder_rows(spark, 10, "/c", "c.s") != before_r
    )  # a tile was added: the input is rebuilt
    assert (
        builder_a(spark, 10, "/c", "c.s") == before_a
    )  # a synthetic input does not care


def test_a_temp_view_keeps_its_name_and_a_towers_companion_table_follows_the_towers_stamp(
    spark_session_local, monkeypatch
):
    assert lf.stamped_name(None, "mvp_in_x_v2_10") == "mvp_in_x_v2_10"
    monkeypatch.setattr(lf.os.path, "isdir", lambda p: True)
    monkeypatch.setattr(
        "databricks.labs.gbx.bench.oracles.dsm_tile_rows", lambda *a, **k: []
    )
    made = []
    monkeypatch.setattr(
        lf,
        "_table",
        lambda spark, schema, name, build, exact=False: made.append((name, exact))
        or name,
    )
    towers = lf._dsm_towers(spark_session_local, 4, "/c", "c.s")
    assert (
        towers.startswith("mvp_in_dsm_towers_v") and towers.rsplit("_", 1)[1] != "tiles"
    )  # carries its stamp
    assert (towers + "_tiles", True) in made and (
        towers,
        True,
    ) in made  # `<towers table>_tiles`, as the form reads it


def test_a_reformat_or_a_reworded_comment_does_not_make_a_new_input(tmp_path):
    import importlib.util

    def load(name, text):
        path = tmp_path / f"{name}.py"
        path.write_text(text)
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.build

    a = load(
        "a",
        "def build(spark, n):\n    '''one doc'''\n    # a comment\n    return n + 1\n",
    )
    b = load(
        "b",
        "def build(spark, n):\n    '''another doc'''\n    return (n   +   1)  # reworded\n",
    )
    c = load("c", "def build(spark, n):\n    return n + 2\n")
    assert lf._source_of(a) == lf._source_of(b) != lf._source_of(c)


def test_the_corpus_fingerprint_is_computed_once_per_listing_and_follows_the_listing(
    monkeypatch,
):
    rows = ["/c/rows/1.tif", "/c/rows/2.tif"]
    calls = []
    monkeypatch.setattr(lf, "_corpus_rows", lambda corpus: calls.append(1) or rows)
    first = lf._corpus_fingerprint("/c")
    assert lf._corpus_fingerprint("/c") == first
    rows2 = rows + ["/c/rows/3.tif"]
    monkeypatch.setattr(lf, "_corpus_rows", lambda corpus: rows2)
    assert lf._corpus_fingerprint("/c") != first  # another listing, another fingerprint


def test_the_bng_tile_memo_follows_the_corpus(monkeypatch):
    import contextlib

    opened = []
    monkeypatch.setattr(
        lf,
        "_corpus_rows",
        lambda corpus: (
            ["/c/rows/1.tif", "/c/rows/2.tif"] if corpus == "/c" else ["/c2/rows/9.tif"]
        ),
    )
    monkeypatch.setattr(lf, "corpus_guard", lambda *a, **k: None)
    monkeypatch.setattr(
        lf,
        "open_data",
        lambda p: opened.append(p)
        or contextlib.nullcontext(types.SimpleNamespace(crs=None)),
    )
    lf._GB_FOUND.clear()
    spark = types.SimpleNamespace(
        catalog=types.SimpleNamespace(tableExists=lambda n: False)
    )
    for corpus in ("/c", "/c2"):
        assert (
            lf._gb_tiles(False)(spark, 1, corpus, "c.s") is None
        )  # no BNG tile in either
    assert len({k[2] for k in lf._GB_FOUND}) == 2  # one memo per corpus listing
    assert any(
        "/c2/" in p for p in opened
    )  # the second corpus was read, not served from the first's memo


def test_superseded_stamps_are_listed_newest_kept_and_dropped_only_when_asked():
    class Rows:
        def __init__(self, rows):
            self.rows = rows

        def collect(self):
            return self.rows

    created = {
        "mvp_in_x_v2_10_aaaaaaaa": 1,
        "mvp_in_x_v2_10_bbbbbbbb": 2,
        "mvp_in_x_v2_10_cccccccc": 3,
        "mvp_in_y_v2_10_aaaaaaaa": 1,
        "mvp_in_x_v2_10_cccccccc_tiles": 3,
        "mvp_in_x_v2_10_aaaaaaaa_tiles": 1,
    }
    dropped = []

    class Spark:
        def sql(self, q):
            if q.startswith("SHOW TABLES"):
                return Rows(
                    [("s", n, False) for n in list(created) + ["mvp_in_unstamped"]]
                )
            if q.startswith("DESCRIBE DETAIL"):
                return Rows([{"createdAt": created[q.rsplit(".", 1)[1]]}])
            dropped.append(q)

    old = lf.superseded_inputs(Spark(), "c.s")
    assert old == [
        "c.s.mvp_in_x_v2_10_aaaaaaaa",
        "c.s.mvp_in_x_v2_10_aaaaaaaa_tiles",
        "c.s.mvp_in_x_v2_10_bbbbbbbb",
    ]  # y has one, "unstamped" is not ours
    assert (
        lf.drop_superseded(Spark(), "c.s") == old and dropped == []
    )  # a dry run by default
    lf.drop_superseded(Spark(), "c.s", dry_run=False)
    assert dropped == [f"DROP TABLE IF EXISTS {t}" for t in old]
