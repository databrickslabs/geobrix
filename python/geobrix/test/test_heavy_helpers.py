"""The helpers of ``test/_heavy.py`` that a test run depends on without calling them by name."""

import sys
from test._heavy import forget_generators, light_stack_available, session_has_jar

import pytest


class _Spark:
    """A session stand-in: records the SQL it is asked to run."""

    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def test_forget_generators_drops_and_unmarks_only_the_generators_under_the_prefix():
    """A generator a module registered as heavy is dropped from the session and from the manager's
    "done" and "chosen" records, so the next call registers the level the manager picks. A scalar
    under the same prefix, and a generator under another prefix, are left as they were.
    """
    from databricks.labs.gbx import _api, manifest

    fns = manifest.load().functions
    generator = next(
        n
        for n, f in sorted(fns.items())
        if f.kind == "generator" and n.startswith("gbx_rst_")
    )
    scalar = next(
        n
        for n, f in sorted(fns.items())
        if f.kind != "generator" and n.startswith("gbx_rst_")
    )
    other = next(
        n
        for n, f in sorted(fns.items())
        if f.kind == "generator" and not n.startswith("gbx_rst_")
    )
    spark = _Spark()
    names = {generator, scalar, other}
    _api._done(spark).update(names)
    _api.chosen(spark).update({n: ("L1", "-") for n in names})

    forget_generators(spark, "gbx_rst_")

    assert spark.statements
    assert all(
        s.startswith("DROP TEMPORARY FUNCTION IF EXISTS ") for s in spark.statements
    )
    assert f"DROP TEMPORARY FUNCTION IF EXISTS {generator}" in spark.statements
    assert not any(
        s.endswith(f" {scalar}") or s.endswith(f" {other}") for s in spark.statements
    )
    assert _api._done(spark) == {scalar, other}
    assert set(_api.chosen(spark)) == {scalar, other}


def test_light_stack_available_follows_whether_the_light_packages_import(monkeypatch):
    """True where pandas and rasterio import (the dev container); False as soon as either does not
    (the heavyweight CI env), which is what makes the tests that need them skip there.
    """
    pytest.importorskip("pandas")
    pytest.importorskip("rasterio")
    assert light_stack_available() is True
    for blocked in ("pandas", "rasterio"):
        with monkeypatch.context() as m:
            m.setitem(sys.modules, blocked, None)  # an import of it raises ImportError
            assert light_stack_available() is False, blocked


class _Conf:
    def __init__(self, jars):
        self.jars = jars

    def get(self, key, default=None):
        return self.jars if key == "spark.jars" else default


class _JavaPackage:
    """py4j's stand-in for a name the JVM has no class for: any attribute is another package."""

    def __getattr__(self, name):
        return _JavaPackage()


class _BuildInfo:
    @staticmethod
    def identity():
        return "0.6.0|abc"


def _jvm(with_jar):
    root = _JavaPackage()
    if not with_jar:
        return root
    kernels = type("K", (), {"BuildInfo": _BuildInfo})()
    gbx = type("G", (), {"kernels": kernels})()
    return type("J", (), {"com": type("C", (), {"databricks": type("D", (), {"labs": type("L", (), {"gbx": gbx})()})()})()})()


class _Session:
    def __init__(self, jars="", with_jar=False, connect=False):
        self.conf = _Conf(jars)
        self._with_jar, self._connect = with_jar, connect

    @property
    def _jvm(self):
        if self._connect:
            raise AttributeError("Spark Connect has no JVM")
        return _jvm(self._with_jar)


def test_session_has_jar_sees_spark_jars_and_a_jar_the_cluster_installed_as_a_library():
    jar = "/repo/python/geobrix/lib/geobrix-0.6.0-jar-with-dependencies.jar"
    assert session_has_jar(_Session(jars=jar), jar), "the local session started with spark.jars"
    assert session_has_jar(_Session(with_jar=True), jar), "a pool cluster: the JAR is a cluster library, spark.jars names nothing"
    assert not session_has_jar(_Session(), jar), "a JAR-free JVM: py4j resolves the class name to a package"
    assert not session_has_jar(_Session(connect=True), jar), "a Connect session has no JVM to ask"
