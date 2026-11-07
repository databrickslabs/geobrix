"""The helpers of ``test/_heavy.py`` that a test run depends on without calling them by name."""

import sys
from test._heavy import forget_generators, light_stack_available

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
