"""Registrars hand their functions to Spark through ``_gdal_policy.carrying_session``: each function carries the
driver's remote-path switch of the call that registered it, and the session proxy is transparent to Spark Connect
detection. (Name selection and the data-source name normalizer are tested with ``manager.register_family`` and
``manager.register_sources`` in ``manager/test_contracts.py``.)"""

import threading

import pytest


class _Registry:
    """A session's ``udf`` / ``udtf``: what each registration was handed."""

    def __init__(self):
        self.got = {}

    def register(self, name, f, *args, **kwargs):
        self.got[name] = f
        return f


def test_overlapping_registrations_each_carry_their_own_switch_and_wrap_nothing_else(
    monkeypatch,
):
    """Two registrations overlapping on threads (the first ends first) keep each one's remote-path switch; a
    UDF the user registers on the session meanwhile is registered as given; Spark's registration class is left as
    it was."""
    from databricks.labs.gbx import _gdal_policy

    class Session:
        def __init__(self):
            self.udf, self.udtf = _Registry(), _Registry()

    spark, original = Session(), _Registry.register

    def probe():
        return _gdal_policy._CARRIED.get()

    def user_fn():
        return "mine"

    def registrar(tag, entered, release):
        def reg(spark):  # what a registrar does with the session it is given
            s = _gdal_policy.carrying_session(spark)  # reads the switch now
            s.udf.register(f"{tag}1", probe)
            entered.set()
            assert release.wait(10)
            s.udf.register(f"{tag}2", probe)

        return reg

    events = {t: (threading.Event(), threading.Event()) for t in ("a", "b")}
    threads = {}
    for tag, switch in (("a", "true"), ("b", "false")):
        monkeypatch.setenv(
            _gdal_policy.REMOTE_ENV, switch
        )  # read as each registration starts
        threads[tag] = threading.Thread(
            target=registrar(tag, *events[tag]), args=(spark,)
        )
        threads[tag].start()
        assert events[tag][0].wait(10)
    spark.udf.register("user_fn", user_fn)  # both registrations are inside their blocks
    for tag in ("a", "b"):  # the first one ends first
        events[tag][1].set()
        threads[tag].join(10)
    got = spark.udf.got
    assert {n: got[n]() for n in ("a1", "a2", "b1", "b2")} == {
        "a1": True,
        "a2": True,
        "b1": False,
        "b2": False,
    }
    assert got["user_fn"] is user_fn
    assert _Registry.register is original and "register" not in vars(spark.udf)


def test_connect_detection_sees_the_session_behind_the_registrars_proxy(monkeypatch):
    """A registrar gets the carrying session proxy; telling Spark Connect from a classic session (by the session's
    type) still judges the session behind it."""
    pytest.importorskip("rasterio")  # ds (file_gbx) is the light readers' package
    from databricks.labs.gbx import _gdal_policy
    from databricks.labs.gbx.ds import file_gbx
    from databricks.labs.gbx.manager import capabilities

    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.delenv("GBX_LRU_MAX_BYTES", raising=False)
    # a Connect session on Serverless: the driver has no classic memory environment (a DBR node has one)
    from databricks.labs.gbx.pyrx.core import budget

    for k in budget.CLASSIC_MEMORY_ENV:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("IS_SERVERLESS", "true")

    class ConnectSession:
        pass

    ConnectSession.__module__ = "pyspark.sql.connect.session"
    real = ConnectSession()
    proxy = _gdal_policy.carrying_session(real, allowed=False)
    assert capabilities.is_connect(real) and capabilities.is_connect(proxy)
    assert file_gbx._connect_aware_lru_sizing(
        proxy
    ) == file_gbx._connect_aware_lru_sizing(real)
    assert file_gbx._connect_aware_lru_sizing(real) != file_gbx._lru_sizing(False)
