"""The bench cluster pool (examples/tests/_cluster_pool.py): membership, up/status/down, the lease, the jar check.

A fake workspace records every call; no cluster is created or touched.
"""

import importlib
import io
import json
import sys
import types
from types import SimpleNamespace

import pytest

from conftest import REPO_ROOT

DENIED = "1016-152458-i5qbli6q"


class Rec(SimpleNamespace):
    def __init__(self, **kw):
        super().__init__(**kw)


class AlreadyExists(Exception):
    pass


class NotFound(Exception):
    pass


class FakeW:
    """clusters, files, libraries and jobs, as much of them as the pool uses."""

    def __init__(self):
        self.now = 1_000_000.0
        self.calls = []
        self.clusters_by_id = {}
        self.fs = {}
        self.libs = {}  # cluster -> {jar: status}
        self.runs = {}  # job run id -> life cycle state
        self.created = 0
        w = self

        class Clusters:
            def list(self):
                return list(w.clusters_by_id.values())

            def get(self, cid):
                return w.clusters_by_id[cid]

            def create(self, **kw):
                w.created += 1
                cid = f"new-{w.created}"
                w.calls.append(("create", kw))
                w.clusters_by_id[cid] = Rec(cluster_id=cid, state="RUNNING", **kw)
                return SimpleNamespace(result=lambda: w.clusters_by_id[cid])

            def ensure_cluster_is_running(self, cid):
                c = w.clusters_by_id[cid]
                if c.state != "RUNNING":
                    w.calls.append(("start", cid))
                    c.state = "RUNNING"

            def delete(self, cid):
                w.calls.append(("terminate", cid))
                w.clusters_by_id[cid].state = "TERMINATED"

            def permanent_delete(self, cid):
                w.calls.append(("delete", cid))
                del w.clusters_by_id[cid]

            def edit(self, cluster_id, **kw):
                c = w.clusters_by_id[cluster_id]
                w.calls.append(("edit", cluster_id, kw))
                for k, v in kw.items():
                    setattr(c, k, v)
                if c.state == "RUNNING":
                    w.calls.append(("restart", cluster_id))

        class Files:
            def create_directory(self, p):
                pass

            def upload(self, path, contents, overwrite=False):
                if path in w.fs and not overwrite:
                    raise AlreadyExists(f"{path} already exists")
                w.fs[path] = contents.read()

            def download(self, path):
                if path not in w.fs:
                    raise NotFound(path)
                return SimpleNamespace(contents=io.BytesIO(w.fs[path]))

            def delete(self, path):
                w.calls.append(("unlease", path))
                w.fs.pop(path, None)

            def get_metadata(self, path):
                if path not in w.fs and not path.endswith(".jar"):
                    raise NotFound(path)
                return SimpleNamespace(content_length=len(w.fs.get(path, b"jar")), last_modified=w.jar_mtime.get(path, "Mon, 01 Jan 2026 00:00:00 GMT"))

        class Libraries:
            def cluster_status(self, cid):
                return [
                    SimpleNamespace(library=SimpleNamespace(jar=j), status=st)
                    for j, st in w.libs.get(cid, {}).items()
                ]

            def uninstall(self, cid, libs):
                w.calls.append(("uninstall", cid, [lib.jar for lib in libs]))
                for lib in libs:
                    w.libs.get(cid, {}).pop(lib.jar, None)

            def install(self, cid, libs):
                w.calls.append(("install", cid, [lib.jar for lib in libs]))
                for lib in libs:
                    w.libs.setdefault(cid, {})[lib.jar] = "INSTALLED"

        class Jobs:
            def get_run(self, run_id):
                if run_id not in w.runs:
                    raise NotFound(f"run {run_id} does not exist")
                return SimpleNamespace(state=SimpleNamespace(life_cycle_state=w.runs[run_id]))

        self.jar_mtime = {}
        self.clusters, self.files, self.libraries, self.jobs = Clusters(), Files(), Libraries(), Jobs()

    def add(self, cid, name="p", state="RUNNING", **kw):
        tags = {"gbx_bench_pool": name} if name else {}
        tags.update(kw.pop("tags", {}))
        self.clusters_by_id[cid] = Rec(
            cluster_id=cid, cluster_name=f"c-{cid}", state=state, custom_tags=tags, spark_version="19.x-scala2.13",
            node_type_id="m5d.2xlarge", num_workers=4, autotermination_minutes=60, data_security_mode="SINGLE_USER",
            single_user_name="me", last_activity_time=(self.now - 600) * 1000, **kw,
        )  # fmt: skip
        return self.clusters_by_id[cid]


@pytest.fixture
def pool(monkeypatch):
    compute = types.ModuleType("databricks.sdk.service.compute")
    compute.Library = lambda jar=None: SimpleNamespace(jar=jar)
    compute.AwsAttributes = lambda **kw: SimpleNamespace(**kw)
    compute.AwsAvailability = SimpleNamespace(ON_DEMAND="ON_DEMAND")
    compute.DataSecurityMode = SimpleNamespace(SINGLE_USER="SINGLE_USER")
    for name, mod in (("databricks", types.ModuleType("databricks")), ("databricks.sdk", types.ModuleType("databricks.sdk")),
                      ("databricks.sdk.service", types.ModuleType("databricks.sdk.service")), ("databricks.sdk.service.compute", compute)):
        monkeypatch.setitem(sys.modules, name, mod)
    sys.modules["databricks.sdk.service"].compute = compute
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    return importlib.import_module("_cluster_pool")


VOL = "/Volumes/c/s/v"
INFO = {"run_id": "r1", "owner": "me"}


def lease(pool, w, wait_s=60, **kw):
    """lease_cluster on the fake's clock: waiting advances it, so a lease that cannot be had ends in PoolError at once."""
    return pool.lease_cluster(
        w, "p", VOL, INFO, wait_s=wait_s, poll_s=10, sleep=lambda s: setattr(w, "now", w.now + s),
        clock=lambda: w.now, say=lambda s: None, **kw,
    )  # fmt: skip


def test_a_cluster_is_the_pools_only_by_its_tag_and_the_deny_list_wins(pool):
    w = FakeW()
    w.add("a"), w.add("b", name="other"), w.add("c", name=None), w.add(DENIED)
    assert [c.cluster_id for c in pool.members(w, "p")] == ["a"]
    for cid in ("b", "c", DENIED):
        with pytest.raises(pool.PoolError):
            pool.require_member(w, cid, "p")
    assert pool.require_member(w, "a", "p").cluster_id == "a"


def test_up_makes_only_the_missing_clusters_with_a_60_minute_net_and_the_pool_tag(pool):
    w = FakeW()
    w.add("a")
    ids = pool.up(w, "p", 3, spark_version="19.x-scala2.13", node_type="m5d.2xlarge", workers=4, user="me")
    assert len(ids) == 3 and w.created == 2
    made = [kw for kind, *rest in w.calls if kind == "create" for kw in rest]
    assert {kw["autotermination_minutes"] for kw in made} == {60}
    assert all(kw["custom_tags"]["gbx_bench_pool"] == "p" and kw["data_security_mode"] == "SINGLE_USER" for kw in made)
    assert {kw["cluster_name"] for kw in made} == {"gbx-bench-p-1", "gbx-bench-p-2"}
    assert pool.up(w, "p", 3, spark_version="x", node_type="y", workers=1, user="me") == ids and w.created == 2  # nothing missing


def test_status_shows_state_and_idle_minutes_and_down_ends_only_the_pools_clusters(pool):
    w = FakeW()
    w.add("a"), w.add("t", state="TERMINATED"), w.add("b", name="other"), w.add(DENIED)
    rows = pool.status(w, "p", now=lambda: w.now)
    assert {(r["cluster_id"], r["state"], r["idle_min"]) for r in rows} == {("a", "RUNNING", 10.0), ("t", "TERMINATED", 10.0)}
    assert pool.down(w, "p") == ["a", "t"] and w.calls.count(("terminate", "a")) == 1
    assert w.clusters_by_id["b"].state == "RUNNING" and w.clusters_by_id[DENIED].state == "RUNNING"
    assert not any(c[1] in ("b", DENIED) for c in w.calls if c[0] in ("terminate", "delete"))
    pool.down(w, "p", delete=True)
    assert "a" not in w.clusters_by_id and "b" in w.clusters_by_id


def test_the_lease_is_exclusive_and_a_second_launch_takes_another_cluster(pool):
    w = FakeW()
    w.add("a"), w.add("b")
    first = lease(pool, w)
    second = lease(pool, w)
    assert {first, second} == {"a", "b"}
    with pytest.raises(pool.PoolError, match="every cluster of pool 'p' is leased"):
        lease(pool, w, wait_s=30)
    leases = pool.Leases(w, VOL, "p")
    leases.release(first)
    assert lease(pool, w) == first


def test_a_running_cluster_is_leased_before_a_terminated_one_and_a_terminated_one_is_started(pool):
    w = FakeW()
    w.add("t", state="TERMINATED"), w.add("r")
    assert lease(pool, w) == "r"
    assert not any(c[0] == "start" for c in w.calls)  # no start was paid
    assert lease(pool, w) == "t"
    assert ("start", "t") in w.calls and w.clusters_by_id["t"].state == "RUNNING"


def test_a_lease_is_reclaimed_only_when_its_holder_is_silent_and_its_job_run_is_over(pool):
    w = FakeW()
    w.add("a")
    leases = pool.Leases(w, VOL, "p", clock=lambda: w.now)
    assert leases.acquire("a", dict(INFO))
    leases.update("a", job_run_id=77)
    w.runs[77] = "TERMINATED"
    with pytest.raises(pool.PoolError):  # its launcher touched it a moment ago: alive, even between its two jobs
        lease(pool, w, wait_s=0)
    w.now += pool.TOUCH_GRACE_S + 1
    w.runs[77] = "RUNNING"
    with pytest.raises(pool.PoolError):  # silent, but its job is live: the job holds the cluster
        lease(pool, w, wait_s=0)
    w.runs[77] = "TERMINATED"
    assert lease(pool, w) == "a"  # silent and its job is over: reclaimed
    # a lease with no job run: held while touched, stale when silent
    leases.release("a")
    leases.acquire("a", dict(INFO))
    with pytest.raises(pool.PoolError):
        lease(pool, w, wait_s=0)
    w.now += pool.TOUCH_GRACE_S + 1
    assert lease(pool, w) == "a"


def test_the_entry_point_leases_syncs_jars_follows_the_job_and_releases_even_on_an_error(pool):
    w = FakeW()
    w.add("a", tags={"gbx_jar": "none"})
    jar = f"{VOL}/geobrix-x-tests.jar"
    path = pool.Leases(w, VOL, "p").path("a")
    with pool.leased_cluster(w, "p", VOL, {"run_id": "r1"}, say=lambda s: None, touch_every_s=0.01) as ls:
        assert ls.cluster_id == "a" and path in w.fs
        ls.sync_jars(None)  # not asked: nothing touched
        assert not any(c[0] in ("edit", "install") for c in w.calls)
        ls.sync_jars([jar], sleep=lambda s: None)
        ls.job_run(99)
        assert json.loads(w.fs[path])["job_run_id"] == 99
    assert path not in w.fs  # released
    with pytest.raises(RuntimeError):
        with pool.leased_cluster(w, "p", VOL, {"run_id": "r2"}, say=lambda s: None) as ls:
            raise RuntimeError("the launch failed")
    assert path not in w.fs
    with pool.leased_cluster(w, "p", VOL, {"run_id": "r3"}, say=lambda s: None) as ls:
        ls.keep = True  # --no-wait: the job still holds the cluster
    assert path in w.fs


def test_leasing_never_touches_a_cluster_that_is_not_the_pools(pool):
    w = FakeW()
    w.add("x", name="other"), w.add(DENIED)
    with pytest.raises(pool.PoolError, match="has no clusters"):
        lease(pool, w)
    assert not [c for c in w.calls if c[0] in ("start", "edit", "terminate")] and not w.fs


def test_the_jar_check_leaves_a_matching_cluster_alone_and_reinstalls_a_mismatched_one(pool):
    w = FakeW()
    c = w.add("a", tags={"gbx_jar": "none"})
    jar = f"{VOL}/geobrix/0.6.0/jar/geobrix-0.6.0-jar-with-dependencies.jar"
    # a clean cluster, no jar wanted: nothing to do
    assert pool.sync_jars(w, "a", [], say=lambda s: None)["action"] == "ok" and not w.calls
    # a jar wanted, none installed: install, record the fingerprint
    out = pool.sync_jars(w, "a", [jar], say=lambda s: None, sleep=lambda s: None)
    assert out["action"] == "reinstalled" and c.custom_tags["gbx_jar"] == out["fingerprint"] != "none"
    assert ("install", "a", [jar]) in w.calls and ("restart", "a") in w.calls
    # the edit kept the cluster as it was: its spec and the 60-minute net
    edit = next(x for x in w.calls if x[0] == "edit")[2]
    assert edit["autotermination_minutes"] == 60 and edit["node_type_id"] == "m5d.2xlarge" and edit["custom_tags"]["gbx_bench_pool"] == "p"
    # the same jar again: no call at all
    w.calls.clear()
    assert pool.sync_jars(w, "a", [jar], say=lambda s: None)["action"] == "ok" and w.calls == []
    # the jar file was overwritten (same path, new mtime): the fingerprint moves, the old one is removed, a restart happens
    w.jar_mtime[jar] = "Tue, 02 Jan 2026 00:00:00 GMT"
    assert pool.sync_jars(w, "a", [jar], say=lambda s: None, sleep=lambda s: None)["action"] == "reinstalled"
    kinds = [x[0] for x in w.calls]
    assert kinds.index("uninstall") < kinds.index("edit") < kinds.index("restart") < kinds.index("install")
    # a run that wants no jar on a cluster that has one drops it
    w.calls.clear()
    assert pool.sync_jars(w, "a", [], say=lambda s: None, sleep=lambda s: None)["action"] == "reinstalled"
    assert ("uninstall", "a", [jar]) in w.calls and c.custom_tags["gbx_jar"] == "none" and not w.libs["a"]


def test_a_jar_that_fails_to_install_is_an_error_not_a_hang(pool):
    w = FakeW()
    w.add("a", tags={"gbx_jar": "none"})
    jar = f"{VOL}/geobrix-x-tests.jar"
    w.libraries.install = lambda cid, libs: w.libs.setdefault(cid, {}).update({lib.jar: "FAILED" for lib in libs})
    with pytest.raises(pool.PoolError, match="failed"):
        pool.sync_jars(w, "a", [jar], say=lambda s: None, sleep=lambda s: None)


def test_the_command_brings_a_pool_up_shows_it_and_takes_it_down(pool, capsys, monkeypatch):
    import bench_pool

    w = FakeW()
    w.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="me"))
    assert bench_pool.main(["up", "--name", "p", "--n", "2"], w=w) == 0
    assert w.created == 2 and "auto-termination 60 min" in capsys.readouterr().out
    assert bench_pool.main(["status", "--name", "p"], w=w) == 0
    assert capsys.readouterr().out.count("RUNNING") == 2
    assert bench_pool.main(["down", "--name", "p"], w=w) == 0
    assert [c.state for c in w.clusters_by_id.values()] == ["TERMINATED", "TERMINATED"]
    monkeypatch.delenv("GBX_BENCH_POOL", raising=False)
    assert bench_pool.main(["status"], w=w) == 2  # no pool name: refused before any call
