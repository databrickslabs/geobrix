"""The runtime of the generated Python API (``_api``): the registration hand-off with the manager, the
refusal when a level cannot run, SQL literals, and the argument rules of the table-function wrapper.
"""

import dataclasses
import importlib.util
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


def test_a_missing_light_dependency_is_named_with_a_fix(monkeypatch):
    # A Connect session has no JVM level, so the name is served by the light one; its module cannot import pandas.
    from databricks.labs.gbx.manager import l3

    def no_pandas(ref):
        raise ModuleNotFoundError("No module named 'pandas'", name="pandas")

    monkeypatch.setattr(l3, "resolve_ref", no_pandas)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)  # truly absent
    with pytest.raises(ImportError) as err:
        manager.register(ConnectStub(), only=["gbx_bng_eastnorthasbng"])
    text = str(err.value)
    assert "lightweight functions need pandas" in text
    assert "gbx_bng_eastnorthasbng" in text and "grid-scalar" in text
    assert "Fix: install the geobrix env wheel" in text and "pip install pandas" in text
    assert isinstance(err.value.__cause__, ModuleNotFoundError)


@pytest.mark.parametrize(
    "error",
    [
        ImportError("cannot import name 'x' from 'pandas'", name="pandas"),
        ModuleNotFoundError("No module named 'pandas.nope'", name="pandas.nope"),
    ],
    ids=["cannot-import-name", "missing-submodule-of-an-installed-package"],
)
def test_an_import_error_of_an_installed_package_passes_through_unchanged(
    monkeypatch, error
):
    # pandas is installed here: the error is its own, not "pandas is not installed", so it is not rewritten
    from databricks.labs.gbx.manager import l3

    def broken(ref):
        raise error

    monkeypatch.setattr(l3, "resolve_ref", broken)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    with pytest.raises(ImportError) as err:
        manager.register(ConnectStub(), only=["gbx_bng_eastnorthasbng"])
    assert err.value is error and "lightweight functions need" not in str(err.value)


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


class _CountingSession:
    """A real session whose ``sql`` calls are recorded: on Spark Connect each one is a network round trip."""

    def __init__(self, spark):
        self._spark, self.statements = spark, []

    def sql(self, text, *a, **k):
        self.statements.append(text)
        return self._spark.sql(text, *a, **k)

    def __getattr__(self, name):
        return getattr(self._spark, name)


def test_the_sql_function_probe_runs_once_per_session():
    """Every installer asked "does this session create SQL functions?" with 2 statements, ~80 times in a full
    registration (160 round trips on Spark Connect). The answer is a property of the session.
    """
    from databricks.labs.gbx.manager import capabilities

    class Spark:
        def __init__(self):
            self.statements = []

        def sql(self, text):
            self.statements.append(text)

    spark = Spark()
    assert all(capabilities._sql_functions(spark) for _ in range(5))
    assert len(spark.statements) == 2
    capabilities.forget(spark)  # a forced re-registration probes again
    assert capabilities._sql_functions(spark)
    assert len(spark.statements) == 4

    class NoSqlFunctions(Spark):
        def sql(self, text):
            raise RuntimeError("no SQL UDFs")

    refused = NoSqlFunctions()
    assert not capabilities._sql_functions(refused)
    assert not capabilities._sql_functions(refused)  # a refusal is remembered too


def test_a_wrapper_over_a_free_name_is_one_statement_and_over_a_light_udf_drops_first():
    from databricks.labs.gbx.manager import jvm

    fn = manifest.load().functions["gbx_bng_eastnorthasbng"]

    class Spark:
        def __init__(self, refuse_first):
            self.statements, self.refuse = [], refuse_first

        def sql(self, text):
            self.statements.append(text)
            if self.refuse and text.startswith("CREATE"):
                self.refuse = False
                raise RuntimeError("CANNOT_REPLACE_NON_SQL_UDF_WITH_SQL_UDF")

    free = Spark(False)
    jvm._create_wrapper(free, fn, "CREATE OR REPLACE TEMPORARY FUNCTION x() ...")
    assert free.statements == ["CREATE OR REPLACE TEMPORARY FUNCTION x() ..."]
    held = Spark(True)
    jvm._create_wrapper(held, fn, "CREATE OR REPLACE TEMPORARY FUNCTION x() ...")
    assert held.statements == [
        "CREATE OR REPLACE TEMPORARY FUNCTION x() ...",
        "DROP TEMPORARY FUNCTION IF EXISTS gbx_bng_eastnorthasbng",
        "CREATE OR REPLACE TEMPORARY FUNCTION x() ...",
    ]


def test_a_family_registration_makes_one_statement_per_wrapper_plus_one_probe(spark):
    """Count assertion on the SQL round trips of a registration at the light level: one probe (2 statements) and
    one CREATE per SQL wrapper; no DROP, no further probe."""
    names = manager.names_at_level(("gbx_bng_", "gbx_st_", "gbx_rst_"), "L3")
    for n in names:  # nothing of an earlier test holds a name
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")
    counting = _CountingSession(spark)
    manager.register_family(counting, ("gbx_bng_", "gbx_st_", "gbx_rst_"), level="L3")
    probe = [s for s in counting.statements if "gbx__probe_sqlfn" in s]
    creates = [
        s
        for s in counting.statements
        if s.startswith("CREATE") and "gbx__probe_sqlfn" not in s
    ]
    assert len(probe) == 2
    assert (
        len(creates) >= 10
    ), "the family has SQL wrappers; the count below would prove nothing"
    others = [s for s in counting.statements if s not in probe and s not in creates]
    assert not [s for s in others if s.startswith("DROP")], others
    assert len(counting.statements) == 2 + len(creates) + len(others)
    assert len(others) <= 1, others  # the one SELECT: the FILE-reference check


def _serverless(monkeypatch):
    """The environment a Serverless notebook reports (no JAR can be added there)."""
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")


def test_a_full_registration_on_connect_stays_under_its_round_trip_budget(
    monkeypatch, capsys
):
    """``register(spark)`` on Serverless made 229 ``spark.sql`` calls (150 of them one repeated probe) and ~100 s of
    network round trips. Count them on a Connect look-alike: one probe (2), one CREATE per SQL wrapper, nothing else.
    """
    _serverless(monkeypatch)
    stub = ConnectStub()
    out = manager.register(stub)
    assert len(out) > 200
    probes = [s for s in stub.sql_text if "gbx__probe_sqlfn" in s]
    assert len(probes) == 2
    drops = [s for s in stub.sql_text if s.startswith("DROP")]
    assert drops == probes[1:], "a wrapper over a free name needs no DROP"
    assert len(stub.sql_text) <= 60, len(stub.sql_text)
    assert stub.sources, "the light data sources went in"


class _SlowStub(ConnectStub):
    """A Connect look-alike whose registrations take a moment, and which records how many ran at once."""

    def __init__(self):
        super().__init__()
        import threading

        self.lock, self.running, self.peak = threading.Lock(), 0, 0
        self.udf = _SlowRegistration(self)
        self.udtf = _SlowRegistration(self)

    def hold(self):
        import time

        with self.lock:
            self.running += 1
            self.peak = max(self.peak, self.running)
        time.sleep(0.02)
        with self.lock:
            self.running -= 1


class _SlowRegistration:
    def __init__(self, owner):
        self.owner = owner

    def register(self, name, f, returnType=None):
        self.owner.hold()
        self.owner.registered.append(name)


def test_registrations_overlap_on_connect_and_one_after_the_other_when_asked(
    monkeypatch, capsys
):
    """Each registration is a round trip on Spark Connect, so they overlap there (the result is the same: the
    same levels, in the manifest's order); ``GBX_REGISTER_WORKERS=1`` is the one-after-the-other loop.
    """
    from databricks.labs.gbx.manager import capabilities

    _serverless(monkeypatch)
    monkeypatch.setattr(
        capabilities, "is_connect", lambda s: True
    )  # _SlowStub lives outside a "connect" module
    names = manager.names_at_level(("gbx_bng_",), "L3")[:24]
    overlapped = _SlowStub()
    got = manager.register(overlapped, only=names)
    assert overlapped.peak > 1, "the registrations of one call did not overlap"
    monkeypatch.setenv("GBX_REGISTER_WORKERS", "1")
    serial = _SlowStub()
    again = manager.register(serial, only=names)
    assert serial.peak == 1
    assert list(got) == list(again) == names
    assert {n: (c.level, c.variant) for n, c in got.items()} == {
        n: (c.level, c.variant) for n, c in again.items()
    }


def test_a_classic_session_installs_one_after_the_other():
    from databricks.labs.gbx.manager import _workers

    assert _workers(object(), 50) == 1  # a py4j call is local: nothing to overlap


def test_the_first_failure_in_manifest_order_is_the_one_raised(monkeypatch):
    """Overlapped installs finish in any order; the error raised is still the first name's, as in the loop."""
    from databricks.labs.gbx.manager import _each

    _serverless(monkeypatch)

    def work(i):
        if i in (3, 5):
            raise ValueError(f"item {i}")
        return i

    with pytest.raises(ValueError, match="item 3"):
        _each(ConnectStub(), list(range(8)), work)
    assert _each(ConnectStub(), list(range(8)), lambda i: i * 2) == [
        i * 2 for i in range(8)
    ]
