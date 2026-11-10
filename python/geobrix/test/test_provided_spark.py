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
