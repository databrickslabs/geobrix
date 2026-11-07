"""The Spark-feature capability registry (`manager.capabilities`): the matrix once, against fake sessions.

A capability is used where the SESSION proves it has it: an override, the client API, the SERVER's Spark version, and a
functional probe only where a version cannot decide. These tests fake the server version, the client check and the probe, so
they run the same on every pyspark. Each consumer has one test of its own (test_core_agg.py).
"""

import dataclasses
import os
import subprocess
import sys
import time

import pytest

from databricks.labs.gbx.manager import capabilities as caps


class FakeConf:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class FakeSpark:
    """A session as the registry reads it: `.version` (the server's) and `.conf`."""

    def __init__(self, version="4.3.0", conf=None):
        self._version = version
        self.version_reads = 0
        self.conf = FakeConf(conf or {})

    @property
    def version(self):
        self.version_reads += 1
        if isinstance(self._version, Exception):
            raise self._version
        return self._version


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for var in list(os.environ):
        if var.startswith(caps.CAP_ENV_PREFIX) or var == "GBX_ARROW_AGG":
            monkeypatch.delenv(var)
    # the client has every API unless a test says otherwise; a probe passes unless a test says otherwise
    monkeypatch.setattr(caps, "_has_arrow_eval", lambda name: True)
    monkeypatch.setattr(caps, "_has_function", lambda name: True)
    for name, cap in caps.REGISTRY.items():
        if cap.probe is not None:
            monkeypatch.setitem(
                caps.REGISTRY,
                name,
                dataclasses.replace(cap, probe=lambda spark: (True, "fake")),
            )


def swap(monkeypatch, name, **changes):
    monkeypatch.setitem(
        caps.REGISTRY, name, dataclasses.replace(caps.REGISTRY[name], **changes)
    )


def test_parse_version_reads_the_spark_minor_from_any_version_string():
    assert caps.parse_version("4.3.0") == (4, 3)
    assert caps.parse_version("4.3.0.dev0+databricks.connect.19.1") == (4, 3)
    assert caps.parse_version("4.0.0-SNAPSHOT") == (4, 0)
    assert caps.parse_version("") is None
    assert caps.parse_version(None) is None
    assert caps.parse_version("client.3") is None


def test_every_capability_has_a_why_and_starts_with_no_verified_runs():
    # `verified_on` is filled from probe runs on a runtime; until then every capability reads "unverified"
    for cap in caps.REGISTRY.values():
        assert cap.why
        assert cap.verified_on == ()
    assert caps.REGISTRY["arrow_grouped_agg"].aliases == ("GBX_ARROW_AGG",)


@pytest.mark.parametrize("name", sorted(caps.REGISTRY))
def test_forced_on_and_off_and_the_default_on_a_server_that_has_it(monkeypatch, name):
    cap = caps.REGISTRY[name]
    var = caps.CAP_ENV_PREFIX + name.upper()
    new = FakeSpark("4.9.0")

    assert caps.has(new, name) is cap.default_when_present  # not overridden
    assert caps.available(new, name) is True

    monkeypatch.setenv(var, "1")
    caps.forget(new)
    assert caps.has(new, name) is True
    assert caps.resolve(new, name).source == "override"

    monkeypatch.setenv(var, "0")
    caps.forget(new)
    assert caps.has(new, name) is False
    assert caps.resolve(new, name).source == "override"


@pytest.mark.parametrize("name", sorted(caps.REGISTRY))
def test_forcing_a_capability_on_never_makes_it_available(monkeypatch, name):
    """ON asks for the newer path where it exists: a server that lacks it keeps the older path (and says so)."""
    cap = caps.REGISTRY[name]
    old = FakeSpark("3.5.0")  # older than every capability's minimum
    if cap.min_server is None:  # a probe-only capability: the probe says no
        swap(monkeypatch, name, probe=lambda spark: (False, "fake: fails"))
    monkeypatch.setenv(caps.CAP_ENV_PREFIX + name.upper(), "1")
    with pytest.warns(RuntimeWarning, match=f"unset {caps.CAP_ENV_PREFIX}"):
        r = caps.resolve(old, name)
    assert r.value is False and r.available is False
    assert r.source in ("server", "probe")


def test_a_newer_client_with_an_older_server_never_selects_the_newer_eval_type(
    monkeypatch,
):
    """The env6 case: the client library names GROUPED_AGG_ITER, the server is Spark 4.1 (no iterator aggregate)."""
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG_ITER", "1")
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG", "1")
    server = FakeSpark("4.1.1")
    with pytest.warns(RuntimeWarning, match="arrow_grouped_agg_iter"):
        r = caps.resolve(server, "arrow_grouped_agg_iter")
    assert (r.value, r.available, r.source) == (False, False, "server")
    assert (
        caps.has(server, "arrow_grouped_agg") is True
    )  # 4.1 has the plain Arrow aggregate
    assert caps.path(server, "merge_agg") == "merge_agg:arrow"  # never arrow_iter


def test_a_client_without_the_api_is_absent_whatever_the_server_is(monkeypatch):
    monkeypatch.setattr(caps, "_has_arrow_eval", lambda name: False)
    r = caps.resolve(FakeSpark("4.9.0"), "arrow_grouped_agg")
    assert (r.available, r.source) == (False, "client")


@pytest.mark.parametrize("version", ["not a version", RuntimeError("no server")])
def test_a_server_that_cannot_say_its_version_is_a_server_without_the_feature(
    version,
):
    r = caps.resolve(FakeSpark(version), "arrow_grouped_agg")
    assert (r.value, r.available, r.source) == (False, False, "server")
    assert "unknown" in r.detail


def test_the_legacy_gbx_arrow_agg_still_turns_the_arrow_aggregate_on(monkeypatch):
    server = FakeSpark("4.2.0")
    assert caps.has(server, "arrow_grouped_agg") is False  # off by default
    monkeypatch.setenv("GBX_ARROW_AGG", "1")
    caps.forget(server)
    r = caps.resolve(server, "arrow_grouped_agg")
    assert r.value is True and "GBX_ARROW_AGG" in r.detail
    # the new name wins over the alias when both are set
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG", "0")
    caps.forget(server)
    assert caps.has(server, "arrow_grouped_agg") is False


def test_the_spark_conf_overrides_when_no_env_var_does(monkeypatch):
    server = FakeSpark("4.2.0", {"spark.geobrix.cap.arrow_grouped_agg": "true"})
    assert caps.has(server, "arrow_grouped_agg") is True
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG", "0")  # the env var is first
    caps.forget(server)
    assert caps.has(server, "arrow_grouped_agg") is False


def test_a_decision_is_made_once_per_session_and_forget_drops_it():
    a, b = FakeSpark("4.2.0"), FakeSpark("4.0.0")
    for _ in range(3):
        caps.resolve(a, "arrow_grouped_agg")
        caps.resolve(a, "arrow_grouped_agg_iter")
    assert (
        a.version_reads == 1
    )  # one round trip for the server version, whatever is asked
    assert caps.available(a, "arrow_grouped_agg_iter") is True
    assert (
        caps.available(b, "arrow_grouped_agg_iter") is False
    )  # another session, its own answer
    caps.forget(a)
    caps.resolve(a, "arrow_grouped_agg")
    assert a.version_reads == 2


def test_the_choice_is_logged_once_per_session(caplog):
    server = FakeSpark("4.2.0")
    with caplog.at_level("INFO", logger=caps.__name__):
        for _ in range(3):
            caps.has(server, "arrow_grouped_agg")
    mine = [r for r in caplog.records if "arrow_grouped_agg" in r.getMessage()]
    assert len(mine) == 1 and "default" in mine[0].getMessage()


def test_a_probe_runs_only_where_asked_and_once(monkeypatch):
    runs = []

    def probe(spark):
        runs.append(1)
        return True, "2 of 2 rows"

    swap(monkeypatch, "udtf_table_partition_ok", probe=probe)
    server = FakeSpark("4.2.0")
    shown = caps.report(server)  # a report starts no Spark work
    assert shown["udtf_table_partition_ok"]["source"] == "unprobed" and not runs
    assert shown["udtf_table_partition_ok"]["available"] is None
    assert caps.available(server, "udtf_table_partition_ok") is True  # a use probes
    assert caps.available(server, "udtf_table_partition_ok") is True
    assert len(runs) == 1
    assert caps.report(server)["udtf_table_partition_ok"]["source"] == "probe"


def test_a_probe_that_raises_or_hangs_counts_as_absent(monkeypatch):
    def boom(spark):
        raise RuntimeError("worker died")

    swap(monkeypatch, "udtf_table_partition_ok", probe=boom)
    r = caps.resolve(FakeSpark("4.2.0"), "udtf_table_partition_ok")
    assert (r.available, r.source) == (False, "probe") and "worker died" in r.detail

    def hang(spark):
        time.sleep(5)
        return True, ""

    swap(monkeypatch, "udtf_table_partition_noarrow_ok", probe=hang)
    monkeypatch.setattr(caps, "PROBE_TIMEOUT_S", 0.05)
    r = caps.resolve(FakeSpark("4.2.0"), "udtf_table_partition_noarrow_ok")
    assert r.available is False and "did not finish" in r.detail


def test_a_conf_capability_follows_the_version_or_a_backported_conf():
    key = "spark.sql.execution.python.udf.maxBytesPerBatch"
    assert caps.available(
        FakeSpark("4.3.0", {key: "-1"}), "python_udf_max_bytes_per_batch"
    )
    r = caps.resolve(FakeSpark("4.3.0", {key: "-1"}), "python_udf_max_bytes_per_batch")
    assert f"{key}=-1" in r.detail
    assert not caps.available(FakeSpark("4.2.0"), "python_udf_max_bytes_per_batch")
    backported = FakeSpark(
        "4.2.0", {key: "-1"}
    )  # a fork that carries the conf has the feature
    r = caps.resolve(backported, "python_udf_max_bytes_per_batch")
    assert (r.available, r.source) == (True, "probe")


@pytest.mark.parametrize(
    "iter_on,arrow_on,label",
    [
        (False, False, "pandas"),
        (False, True, "arrow"),
        (True, False, "arrow_iter"),
        (True, True, "arrow_iter"),
    ],
)
def test_merge_agg_path_label_follows_the_capabilities(
    monkeypatch, iter_on, arrow_on, label
):
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG_ITER", "1" if iter_on else "0")
    monkeypatch.setenv("GBX_CAP_ARROW_GROUPED_AGG", "1" if arrow_on else "0")
    assert caps.path(FakeSpark("4.3.0"), "merge_agg") == f"merge_agg:{label}"


def test_the_default_path_is_pandas_on_every_server():
    for v in ("4.0.0", "4.1.0", "4.2.0", "4.3.0"):
        assert caps.path(FakeSpark(v), "merge_agg") == "merge_agg:pandas"


def test_an_unknown_name_or_op_says_what_exists():
    with pytest.raises(ValueError, match="arrow_grouped_agg"):
        caps.has(FakeSpark(), "nope")
    with pytest.raises(ValueError, match="merge_agg"):
        caps.path(FakeSpark(), "nope")


def test_verified_marks_a_server_minor_that_a_run_confirmed(monkeypatch):
    run = caps.Verified("19.x-snapshot-scala2.13", "4.3.0", "x86_64", "2026-11-09")
    swap(monkeypatch, "arrow_grouped_agg_iter", verified_on=(run,))
    assert caps.resolve(FakeSpark("4.3.1"), "arrow_grouped_agg_iter").verified is True
    assert caps.resolve(FakeSpark("4.2.0"), "arrow_grouped_agg_iter").verified is False
    assert caps.report(FakeSpark("4.3.0"))["arrow_grouped_agg_iter"]["verified"] is True


def test_runtime_info_reads_the_server_the_client_and_the_runtime(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "19.1")
    monkeypatch.setenv("DATABRICKS_ENV_VERSION", "6")
    server = FakeSpark(
        "4.3.0",
        {"spark.databricks.clusterUsageTags.sparkVersion": "19.x-snapshot-scala2.13"},
    )
    info = caps.runtime_info(server)
    assert info["spark_version"] == "4.3.0"
    assert info["env_version"] == "6"
    assert (
        info["dbr_version"] == "19.x-snapshot-scala2.13"
    )  # a classic cluster names its runtime
    assert info["pyspark_version"]  # the client
    assert (
        caps.runtime_info()["spark_version"] == ""
    )  # without a session, the server is unknown
    monkeypatch.setenv(
        "DATABRICKS_RUNTIME_VERSION", "client.4"
    )  # Serverless: no conf read, the variable stands
    assert caps.runtime_info(server)["dbr_version"] == "client.4"


def test_nothing_is_decided_at_import():
    """Importing the pyrx functions (which build the merge aggregates) asks no capability and reads no session."""
    code = (
        "from databricks.labs.gbx.manager import capabilities as c\n"
        "calls = []\n"
        "for n in ('resolve', 'has', 'available', 'path', 'server_version', 'report'):\n"
        "    orig = getattr(c, n)\n"
        "    setattr(c, n, lambda *a, _o=orig, _n=n, **k: calls.append(_n) or _o(*a, **k))\n"
        "import databricks.labs.gbx.pyrx.functions\n"
        "assert not calls, calls\n"
        "assert not list(c._RESOLVED) and not list(c._SERVER)\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert done.returncode == 0, done.stderr[-2000:]


def test_the_udtf_partition_probe_runs_on_a_real_session(spark):
    """The probe itself, on the local session: both UDTF forms return the two rows of one partition here. (What a
    given server does is what the dogfood probe records.)"""
    for use_arrow in (None, False):
        ok, detail = caps._udtf_partition_probe(use_arrow)(spark)
        assert ok, detail
