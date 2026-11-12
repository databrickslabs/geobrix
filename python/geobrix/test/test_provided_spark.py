"""The provided-session switch (test/_provided_spark.py): when it applies, and what it changes. No JVM: fake classes."""

import sys
import types
from test import _provided_spark as ps

import pytest


@pytest.mark.parametrize(
    "env, expected",
    [
        ({}, False),
        ({"DATABRICKS_RUNTIME_VERSION": "19.0"}, True),
        ({"DATABRICKS_RUNTIME_VERSION": "19.0", "GBX_TEST_PROVIDED_SPARK": "0"}, False),
        ({"GBX_TEST_PROVIDED_SPARK": "1"}, True),
        (
            {"GBX_TEST_PROVIDED_SPARK": "false", "DATABRICKS_RUNTIME_VERSION": "19.0"},
            False,
        ),
        ({"GBX_TEST_PROVIDED_SPARK": ""}, False),
    ],
)
def test_it_applies_on_databricks_or_when_asked(env, expected):
    assert ps.wanted(env) is expected


def _fake_pyspark(monkeypatch):
    class Builder:
        def __init__(self):
            self.calls = []

        def master(self, m):
            self.calls.append(("master", m))
            return self

        def appName(self, n):
            self.calls.append(("appName", n))
            return self

        def config(self, key=None, value=None):
            self.calls.append(("config", key))
            return self

        def getOrCreate(self):
            return "the provided session"

    class SparkSession:
        stopped = 0

        def stop(self):
            SparkSession.stopped += 1

    SparkSession.Builder = Builder
    session = types.ModuleType("pyspark.sql.session")
    session.SparkSession = SparkSession
    sql = types.ModuleType("pyspark.sql")
    sql.session = session
    py = types.ModuleType("pyspark")
    py.sql = sql
    for name, mod in (
        ("pyspark", py),
        ("pyspark.sql", sql),
        ("pyspark.sql.session", session),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect", None)  # no Connect client
    monkeypatch.setattr(ps, "_installed", False)
    monkeypatch.setattr(ps, "IGNORED", [])
    return SparkSession


def test_nothing_changes_when_it_does_not_apply(monkeypatch):
    spark_cls = _fake_pyspark(monkeypatch)
    assert ps.install({}) is False
    b = spark_cls.Builder()
    assert b.master("local[2]") is b and b.calls == [
        ("master", "local[2]")
    ]  # still the original method
    spark_cls().stop()
    assert spark_cls.stopped == 1


def test_a_fixture_gets_the_provided_session_and_cannot_stop_it(monkeypatch):
    spark_cls = _fake_pyspark(monkeypatch)
    assert ps.install({"DATABRICKS_RUNTIME_VERSION": "19.0"}) is True
    b = spark_cls.Builder()
    got = (
        b.master("local[2]")
        .appName("t")
        .config("spark.jars", "x.jar")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    assert got == "the provided session"
    assert b.calls == []  # none of the local settings reached the builder
    assert [name for name, _ in ps.IGNORED] == ["master", "appName", "config", "config"]
    spark_cls().stop()
    assert spark_cls.stopped == 0


def test_installing_twice_does_not_wrap_twice(monkeypatch):
    spark_cls = _fake_pyspark(monkeypatch)
    ps.install({"GBX_TEST_PROVIDED_SPARK": "1"})
    first = spark_cls.Builder.master
    monkeypatch.setattr(ps, "_installed", False)
    ps.install({"GBX_TEST_PROVIDED_SPARK": "1"})
    assert spark_cls.Builder.master is first


def test_a_bare_local_path_is_read_as_a_file_uri_and_other_paths_are_left_alone(
    tmp_path,
):
    f = tmp_path / "r0.tif"
    f.write_bytes(b"x")
    assert ps.local_uri(str(f)) == "file:" + str(f)
    assert ps.local_uri(f) == "file:" + str(f)  # a Path too
    for kept in (
        "/Volumes/c/s/v/r0.tif",
        "/dbfs/x",
        "/Workspace/x",
        "dbfs:/x",
        "file:/x",
        "s3://b/k",
        "rel/x",
        str(tmp_path / "absent.tif"),
    ):
        assert ps.local_uri(kept) == kept


def test_the_reader_qualifies_one_path_or_a_list(monkeypatch, tmp_path):
    seen = []

    class Reader:
        def load(self, path=None, format=None, **options):
            seen.append(path)
            return "df"

    f = tmp_path / "a.tif"
    f.write_bytes(b"x")
    ps._patch_load(Reader)
    ps._patch_load(Reader)  # twice: still one wrapper
    assert Reader().load(str(f)) == "df"
    Reader().load([str(f), "/Volumes/c/s/v/b.tif"])
    Reader().load()  # a reader with the path in its options
    assert seen == [
        "file:" + str(f),
        ["file:" + str(f), "/Volumes/c/s/v/b.tif"],
        None,
    ]


def test_the_reader_formats_qualify_their_path_too(tmp_path):
    """`spark.read.parquet(p)` does not go through `load` in the classic API, so each format method is changed."""
    seen = {}

    class Reader:
        def parquet(self, *paths, **options):
            seen["parquet"] = paths
            return "df"

        def csv(self, path, schema=None, **options):
            seen["csv"] = (path, schema)
            return "df"

        def json(self, path, **options):
            seen["json"] = path
            return "df"

    d = tmp_path / "d"
    d.mkdir()
    ps._patch_load(Reader)
    Reader().parquet(
        str(d), "/Volumes/c/s/v/x"
    )  # every positional argument of parquet(*paths)
    Reader().csv([str(d)], "a INT")  # the schema is not a path
    Reader().json(path=str(d))
    assert seen == {
        "parquet": ("file:" + str(d), "/Volumes/c/s/v/x"),
        "csv": (["file:" + str(d)], "a INT"),
        "json": "file:" + str(d),
    }


def test_the_stream_reader_qualifies_only_the_path_of_parquet(tmp_path):
    """`DataStreamReader.parquet(path, mergeSchema, pathGlobFilter, ...)` takes options after its path: only the path is one."""
    seen = {}

    class StreamReader:
        def parquet(self, path, mergeSchema=None, pathGlobFilter=None):
            seen["parquet"] = (path, mergeSchema, pathGlobFilter)
            return "df"

    d = tmp_path / "d"
    d.mkdir()
    ps._patch_stream_load(StreamReader)
    StreamReader().parquet(str(d), None, str(d))
    assert seen == {"parquet": ("file:" + str(d), None, str(d))}


def test_a_write_to_a_bare_local_path_lands_on_the_local_disk(tmp_path):
    """The path need not exist yet: the folder it goes in decides. Platform paths, URIs and the root are left alone."""
    out = tmp_path / "new" / "out"  # neither "new" nor "out" exists: tmp_path does
    assert ps.local_write_uri(str(out)) == "file:" + str(out)
    assert ps.local_write_uri(out) == "file:" + str(out)  # a Path too
    for kept in (
        "/Volumes/c/s/v/out",
        "/dbfs/x",
        "/Workspace/x",
        "dbfs:/x",
        "file:/x",
        "s3://b/k",
        "rel/out",
        "/out",  # its folder is the root: not a place a test writes
    ):
        assert ps.local_write_uri(kept) == kept


def test_the_writers_qualify_save_the_formats_and_start(tmp_path):
    seen = []

    class Writer:
        def save(self, path=None, format=None, mode=None, **options):
            seen.append(("save", path))

        def parquet(self, path, mode=None, partitionBy=None, compression=None):
            seen.append(("parquet", path, mode))

        def start(self, path=None, format=None, **options):
            seen.append(("start", path))

    ps._patch_save(Writer)
    ps._patch_start(Writer)
    ps._patch_save(Writer)  # twice: still one wrapper
    out = str(tmp_path / "w")
    Writer().save(out, "delta", "append")
    Writer().save(path=out)
    Writer().parquet(out, "overwrite")
    Writer().start(out)
    Writer().save()  # a writer with the path in its options
    Writer().save("/Volumes/c/s/v/w")
    uri = "file:" + out
    assert seen == [
        ("save", uri),
        ("save", uri),
        ("parquet", uri, "overwrite"),
        ("start", uri),
        ("save", None),
        ("save", "/Volumes/c/s/v/w"),
    ]


def test_a_delta_path_in_sql_text_is_qualified_like_a_write(tmp_path):
    """``spark.sql("... delta.`<path>` ...")`` names a table by path, as the zarr ownership ledger
    (``<root>/_gbx_owner``) does. On Databricks a bare local path there is a DBFS path, so the
    ledger landed in DBFS while the test (and GeoBrix's own listing) looked for it on the local
    disk. It gets the writers' rule: a bare local path whose folder exists is a ``file:`` URI.
    """
    seen = []

    class Session:
        def sql(self, sqlQuery, args=None, **kwargs):
            seen.append(sqlQuery)

    ps._patch_sql(Session)
    ps._patch_sql(Session)  # twice: still one wrapper
    ledger = str(tmp_path / "root" / "_gbx_owner")  # neither exists yet: tmp_path does
    odd = str(tmp_path / "a`b")
    Session().sql(f"CREATE TABLE delta.`{ledger}` AS SELECT 1")
    Session().sql(sqlQuery=f"SELECT * FROM delta.`{ledger}` JOIN delta.`{ledger}`")
    Session().sql("SELECT * FROM delta.`" + odd.replace("`", "``") + "`")
    for kept in (
        "SELECT * FROM delta.`/Volumes/c/s/v/t`",
        "SELECT * FROM delta.`file:/x`",
        "SELECT * FROM delta.`dbfs:/x`",
        "SELECT '/local' AS p, delta_t.x FROM delta_t",
    ):
        Session().sql(kept)
    uri = f"delta.`file:{ledger}`"
    assert seen == [
        f"CREATE TABLE {uri} AS SELECT 1",
        f"SELECT * FROM {uri} JOIN {uri}",
        "SELECT * FROM delta.`file:" + odd.replace("`", "``") + "`",
        "SELECT * FROM delta.`/Volumes/c/s/v/t`",
        "SELECT * FROM delta.`file:/x`",
        "SELECT * FROM delta.`dbfs:/x`",
        "SELECT '/local' AS p, delta_t.x FROM delta_t",
    ]


def test_the_class_of_the_session_given_is_covered_too(tmp_path):
    """A runtime's session may be a subclass that defines its own ``sql`` (the classic and Connect
    sessions of newer Spark lines do), which a patch of the base class never reaches. ``cover``
    patches the class of the session a fixture got."""
    seen = []

    class Base:
        def sql(self, sqlQuery, args=None, **kwargs):
            seen.append(("base", sqlQuery))

    class Runtime(Base):
        def sql(self, sqlQuery, args=None, **kwargs):
            seen.append(("runtime", sqlQuery))

    ps._patch_sql(Base)
    ledger = str(tmp_path / "_gbx_owner")
    session = Runtime()
    assert ps.cover(session) is session
    ps.cover(session)  # twice: still one wrapper
    session.sql(f"SELECT * FROM delta.`{ledger}`")
    assert seen == [("runtime", f"SELECT * FROM delta.`file:{ledger}`")]
