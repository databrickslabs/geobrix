"""The Python API's first-use registration across sessions and Lakeflow flows.

A Lakeflow pipeline runs every dataset function on the same Spark Connect session object, each under its own
``PipelineAnalysisContext`` (a thread-local user-context extension naming the flow). A function registered while
one flow's body ran does not resolve in another flow's analysis, so the registration cache of ``_api.ensure`` is
kept per flow, not only per session (a body that called ``pmtiles_agg()`` after another body's ``register(spark)``
failed with ``UNRESOLVED_ROUTINE gbx_pmtiles_agg``).
"""

import pytest

from databricks.labs.gbx import _api, manager

from .connect_stub import FlowStub, _Ext

NAME = "gbx_pmtiles_agg"


def _serverless(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")


def _recording_register(monkeypatch, stub):
    """``manager.register`` replaced by one that records the flow each call ran in."""
    calls = []

    def fake(s, target="session", only=None, **kw):
        calls.append(s.client.current())
        return {n: 1 for n in only}

    monkeypatch.setattr(manager, "register", fake)
    return calls


def test_a_name_registered_in_one_flow_registers_again_in_the_next(monkeypatch):
    stub = FlowStub()
    with stub.client.flow("portfolio_mvt_tiles"):
        manager.register(stub, only=[NAME])  # the body of one flow registers
    calls = _recording_register(monkeypatch, stub)
    with stub.client.flow("pmtiles_shards"):  # another flow's body calls pmtiles_agg()
        _api.ensure(stub, NAME)
        _api.ensure(stub, NAME)
    with stub.client.flow("portfolio_mvt_tiles"):
        _api.ensure(stub, NAME)  # the flow that registered it needs nothing more
    assert calls == ["pmtiles_shards"]


def test_a_full_registration_in_one_flow_does_not_mark_the_names_of_another(
    monkeypatch,
):
    _serverless(monkeypatch)
    stub = FlowStub()
    with stub.client.flow("bronze"):
        manager.register(stub)
    calls = _recording_register(monkeypatch, stub)
    with stub.client.flow("bronze"):
        _api.ensure(stub, NAME)
    with stub.client.flow("overview_manifest"):
        _api.ensure(stub, NAME)
    assert calls == ["overview_manifest"]


def test_every_request_of_a_full_registration_in_a_flow_carries_the_flow(
    monkeypatch,
):
    """On Connect the registrations overlap on worker threads; a thread-local extension does not follow a request to
    another thread unless it is carried there."""
    _serverless(monkeypatch)
    stub = FlowStub()
    with stub.client.flow("silver"):
        manager.register(stub)
    assert stub.round_trips() > 100
    assert set(stub.flows_seen) == {"silver"}, set(stub.flows_seen)
    assert stub.client.current() is None  # nothing is left on the calling thread
    assert not stub.client.global_user_context_extensions


def test_on_connect_a_known_name_costs_no_round_trip_and_a_new_flow_one(
    monkeypatch,
):
    _serverless(monkeypatch)
    stub = FlowStub()
    with stub.client.flow("a"):
        manager.register(stub)
        before = stub.round_trips()
        for _ in range(100):
            _api.ensure(stub, NAME)
        assert stub.round_trips() == before
    with stub.client.flow("b"):
        _api.ensure(stub, NAME)
        assert stub.round_trips() == before + 1
        _api.ensure(stub, NAME)
        assert stub.round_trips() == before + 1


def test_outside_a_pipeline_one_cache_serves_the_session(monkeypatch):
    """A Connect session with no pipeline context, or with an extension of another kind, keeps one cache: the
    round-trip budget of a notebook is unchanged."""
    stub = FlowStub()
    stub.client.add_global_user_context_extension(
        _Ext("type.googleapis.com/spark.connect.SomethingElse", b"x")
    )
    manager.register(stub, only=[NAME])
    calls = _recording_register(monkeypatch, stub)
    _api.ensure(stub, NAME)
    assert calls == []


def test_a_new_classic_session_lacks_the_function_and_the_api_registers_it(spark):
    """The cache is per session object: ``spark.newSession()`` shares the SparkContext, not the functions."""
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {NAME}")
    choices = manager.register(spark, only=[NAME])
    if NAME not in choices:
        pytest.skip(f"{NAME} has no level on this compute")
    other = spark.newSession()
    assert spark.catalog.functionExists(NAME)
    assert not other.catalog.functionExists(NAME)
    _api.ensure(other, NAME)
    assert other.catalog.functionExists(NAME)
