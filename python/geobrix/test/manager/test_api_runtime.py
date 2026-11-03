"""The runtime of the generated Python API (``_api``): the registration hand-off with the manager, the
refusal when a level cannot run, SQL literals, and the argument rules of the table-function wrapper.
"""

import dataclasses
import string

import pytest

from databricks.labs.gbx import _api, manager, manifest
from databricks.labs.gbx.manifest import Param

from .connect_stub import ConnectStub


def test_a_real_register_marks_the_names_so_ensure_does_not_register_again(
    monkeypatch,
):
    stub = ConnectStub()
    manager.register(stub, only=["gbx_st_makevalid"])
    assert "gbx_st_makevalid" in _api._done(stub)
    monkeypatch.setattr(
        manager,
        "register",
        lambda *a, **k: pytest.fail("a marked name registers again"),
    )
    _api.ensure(stub, "gbx_st_makevalid")


def test_ensure_refuses_a_name_the_manager_did_not_register(monkeypatch):
    stub, seen = ConnectStub(), []
    monkeypatch.setattr(
        manager,
        "register",
        lambda s, target="session", only=None, **kw: seen.append(only) or {},
    )
    for _ in range(2):
        with pytest.raises(RuntimeError, match="cannot run on this compute"):
            _api.ensure(stub, "gbx_bng_eastnorthasbng")
    assert "gbx_bng_eastnorthasbng" not in _api._done(stub)
    assert len(seen) == 2  # the refusal is not cached: a later call tries again


def test_the_registration_cache_is_per_session(monkeypatch):
    seen = []
    monkeypatch.setattr(
        manager,
        "register",
        lambda s, target="session", only=None, **kw: seen.append(s)
        or {n: 1 for n in only},
    )
    a, b = ConnectStub(), ConnectStub()
    _api.ensure(a, "gbx_st_makevalid")
    _api.ensure(b, "gbx_st_makevalid")
    _api.ensure(a, "gbx_st_makevalid")
    assert seen == [a, b]


def test_a_session_that_cannot_be_weakly_referenced_still_remembers_its_names():
    class Slotted:
        __slots__ = ()

    s = Slotted()
    _api.mark_registered(s, ["gbx_x"])
    assert "gbx_x" in _api._done(s)
    assert "gbx_x" not in _api._done(Slotted())


P_DOUBLE, P_INT = Param("p", ("DOUBLE",), "DOUBLE"), Param("p", ("INT",), "INT")


@pytest.mark.parametrize(
    "value,param,text",
    [
        (float("nan"), P_DOUBLE, "CAST('nan' AS DOUBLE)"),
        (float("inf"), P_DOUBLE, "CAST('inf' AS DOUBLE)"),
        (1.5, P_DOUBLE, "1.5D"),
        (b"\x01\xff", P_INT, "X'01ff'"),
        (None, P_INT, "CAST(NULL AS INT)"),
        ("o'k", P_INT, "'o''k'"),
    ],
)
def test_sql_literals(value, param, text):
    assert _api._sql_literal(value, param) == text


def test_an_unsupported_literal_type_is_a_clear_error():
    with pytest.raises(TypeError, match="object is not a SQL literal"):
        _api._sql_literal(object(), P_INT)


def test_too_many_arguments_are_refused(spark):
    fn = manifest.load().functions["gbx_st_makevalid"]
    with pytest.raises(TypeError, match="takes at most 2 arguments"):
        _api._columns(fn, ("g", "ogc", "x"), fill_all=False)
    with pytest.raises(TypeError, match="takes at most 3 arguments"):
        _api.table(_Df([]), "gbx_rst_retile", "t", 32, 32, 1)


class _Spark:
    def __init__(self):
        self.calls = []

    def sql(self, text, **kw):
        self.calls.append((text, kw))
        return "df"


class _Df:
    def __init__(self, columns):
        self.columns, self.sparkSession = list(columns), _Spark()


@pytest.fixture
def registered(monkeypatch):
    monkeypatch.setattr(_api, "ensure", lambda spark, name: None)


def test_table_refuses_a_column_argument_and_names_the_fix(spark):
    from pyspark.sql import functions as F

    with pytest.raises(TypeError, match="as a str, not a Column") as e:
        _api.table(_Df([]), "gbx_rst_retile", F.col("t"), 32, 32)
    assert "Fix:" in str(e.value)


def test_table_refuses_an_input_column_named_like_an_output_column(registered):
    with pytest.raises(ValueError, match=r"returns the column\(s\) \['raster'\]") as e:
        _api.table(_Df(["id", "Raster"]), "gbx_rst_retile", "t", 32, 32)
    assert "Fix: rename" in str(e.value)


def test_table_escapes_braces_in_a_column_name(registered):
    df = _Df(["a{b}"])
    _api.table(df, "gbx_bng_tessellateexplode", "a{b}", 1000)
    text, kw = df.sparkSession.calls[0]
    # spark.sql formats the text with str.format: the column name must come out as written
    shown = string.Formatter().format(text, gbx_in="IN")
    assert "gbx_in.`a{b}`" in shown and "FROM IN AS gbx_in" in shown


def test_table_gives_a_skipped_optional_its_default_before_a_later_one(
    registered, monkeypatch
):
    base = manifest.load().functions["gbx_bng_tessellateexplode"]
    two = dataclasses.replace(
        base,
        params=base.params + (Param("flag", ("BOOLEAN",), "BOOLEAN", "true"),),
    )
    monkeypatch.setattr(_api, "_fn", lambda n: two)
    df = _Df([])
    _api.table(df, base.sql_name, "g", 1000, None, False)
    text = df.sparkSession.calls[0][0]
    assert "(gbx_in.`g`, 1000, CAST(false AS BOOLEAN), FALSE)" in text
    df2 = _Df([])
    _api.table(df2, base.sql_name, "g", 1000, None, None)
    assert "(gbx_in.`g`, 1000)" in df2.sparkSession.calls[0][0]


def test_a_table_call_with_the_first_optional_left_out_sends_its_default_not_a_shifted_value(
    registered,
):
    """`mode` is left None and `coverage` is set: SQL binds by position, so `mode` must carry its manifest default
    and `coverage` must stay in its own place."""
    df = _Df([])
    _api.table(df, "gbx_bng_geomkringexplode", "g", 3, 1, None, "polyfill")
    ((text, _),) = df.sparkSession.calls
    assert "CAST('boundary-out' AS STRING), 'polyfill'" in text, text


def test_a_jar_companion_a_test_installs_stays_over_a_later_ensure(monkeypatch):
    """``install_jvm_scalars`` records each companion it installs as done at L1, so the generated wrapper's ``ensure``
    does not register the light function over it, and a companion's source is read up to the next top-level
    declaration, whatever its modifiers."""
    from test import _heavy
    from types import SimpleNamespace

    installed = []
    installer = SimpleNamespace(
        install=lambda s, name, fqn: installed.append(name) or "ok"
    )
    spark = SimpleNamespace(
        _jsparkSession=None,
        _jvm=SimpleNamespace(
            com=SimpleNamespace(
                databricks=SimpleNamespace(
                    labs=SimpleNamespace(
                        gbx=SimpleNamespace(
                            kernels=SimpleNamespace(L1Installer=installer)
                        )
                    )
                )
            )
        ),
    )
    name = "gbx_st_triangulate"
    monkeypatch.setattr(_heavy, "_companions", lambda: ["x.Y"])
    monkeypatch.setattr(_heavy, "_sql_name", lambda fqn: name)
    assert _heavy.install_jvm_scalars(spark, ("gbx_st_",)) == [name]
    assert installed == [name]
    assert name in _api._done(spark) and _api.chosen(spark)[name] == ("L1", "-")
    monkeypatch.setattr(
        manager, "register", lambda *a, **k: pytest.fail("registered over it")
    )
    _api.ensure(spark, name)

    for decl in (
        "case object B",
        "private object B",
        "private[gbx] object B",
        "implicit class B",
        "package object b",
        "sealed trait B",
    ):
        assert _heavy._TOP_LEVEL.search(f"object A {{ }}\n{decl} {{ }}\n"), decl
    # a nested declaration is not at column 0
    assert not _heavy._TOP_LEVEL.search("{\n  private object B\n}\n")
