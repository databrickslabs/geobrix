"""The caps a standard access mode driver (Spark Connect over classic compute) decides and carries to its workers, judged
by the signals read on a standard-access DBR 19 cluster (``test._standard_access``): the classic read cap (1536 MiB
decoded) and the classic stream cap (256 MiB), never Serverless's 64 MiB; a Serverless driver keeps 64 MiB.
"""

from test import _standard_access as _std_access

import pytest

from databricks.labs.gbx.pyrx.core import budget as _budget

from .connect_stub import ConnectStub

_MIB = 1024 * 1024


def test_the_observed_standard_access_driver_is_not_serverless(monkeypatch):
    from databricks.labs.gbx.ds.file_gbx import session_read_cap

    _std_access.apply(monkeypatch, "driver")
    stub = ConnectStub()
    assert not _budget.session_on_serverless(stub)
    assert session_read_cap(stub) == 1536 * _MIB
    assert stub.touched == []  # nothing is read from the Connect session


def test_a_standard_access_driver_sizes_its_caps_as_classic(monkeypatch):
    """The driver of a standard-access DBR 19 cluster, by the signals read on one (``test._standard_access``): a Spark
    Connect session over classic compute. It carries the classic read cap (1536 MiB) to the workers, and its readers and
    materialize gates take the classic stream cap (256 MiB), not Serverless's 64 MiB."""
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds.file_gbx import (
        lazy_read_cap,
        materialize_decision,
        report_detected_cap,
        session_read_cap,
    )

    _std_access.apply(monkeypatch, "driver")
    stub = ConnectStub()
    assert session_read_cap(stub) == 1536 * _MIB
    assert _gdal_policy.carrying_session(stub)._cap == 1536 * _MIB
    assert report_detected_cap(stub) == 256 * _MIB
    assert materialize_decision(110 * _MIB, "ingest", spark=stub) == "stream"
    assert materialize_decision(300 * _MIB, "ingest", spark=stub) == "error"
    monkeypatch.setattr(
        "databricks.labs.gbx.ds.file_gbx._resolve_session_for_cap", lambda: stub
    )
    assert lazy_read_cap() == 256 * _MIB
    assert stub.touched == []


@pytest.mark.parametrize(
    "env",
    [{"DATABRICKS_RUNTIME_VERSION": "client.4.1"}, {}],
    ids=["serverless", "no-driver-signal"],
)
def test_a_serverless_connect_driver_keeps_the_serverless_stream_cap(monkeypatch, env):
    from databricks.labs.gbx.ds.file_gbx import report_detected_cap

    for k in (
        *_budget.CLASSIC_MEMORY_ENV,
        "IS_SERVERLESS",
        "DATABRICKS_RUNTIME_VERSION",
    ):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert report_detected_cap(ConnectStub()) == 64 * _MIB


def test_a_standard_access_driver_carries_the_classic_stream_cap_into_raster_gbx(
    monkeypatch,
):
    """``raster_gbx`` decodes at read under the cap its registration carried (a planner or worker process cannot tell
    it): on a standard-access driver that is the classic 256 MiB, so a 110 MiB GeoTIFF reads as a materialized tile.
    """
    from databricks.labs.gbx import manager

    _std_access.apply(monkeypatch, "driver")
    stub, classes = ConnectStub(), []
    stub.dataSource.register = classes.append
    manager.register_sources(stub, only=["raster_gbx"])
    (cls,) = classes
    source = cls({"path": "/Volumes/c/s/v/a.tif", "virtualTiles": "false"})
    reader = source.reader(source.schema())
    _std_access.apply(
        monkeypatch, "worker"
    )  # the reader runs where nothing says classic
    assert reader._read_cap == 256 * _MIB


def test_a_standard_access_driver_gives_a_grouped_merge_the_classic_budgets(
    monkeypatch,
):
    """The grouped raster helpers take their caps on the driver: on standard access, classic's."""
    from types import SimpleNamespace

    from databricks.labs.gbx.rasterx import helpers

    _std_access.apply(monkeypatch, "driver")
    df = SimpleNamespace(sparkSession=ConnectStub())
    assert helpers._cap_bytes(df) == 256 * _MIB
    assert helpers._decoded_budget(df) == 1536 * _MIB
    monkeypatch.setenv(
        "DATABRICKS_RUNTIME_VERSION", "client.4.1"
    )  # the same session on Serverless
    assert helpers._cap_bytes(df) == 64 * _MIB
    assert helpers._decoded_budget(df) == _budget.decoded_budget_bytes("serverless")
