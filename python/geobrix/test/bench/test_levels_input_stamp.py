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
    held = lf._corpus_rows(
        corpus
    )  # reads the corpus rows: the corpus's tiles are part of the stamp
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
