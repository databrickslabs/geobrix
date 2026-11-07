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
        self.tokens = {}
        self.lose_first_response = False
        w = self

        class Clusters:
            def list(self):
                return list(w.clusters_by_id.values())

            def get(self, cid):
                return w.clusters_by_id[cid]

            def create(self, **kw):
                token = kw.get("idempotency_token")
                if token and token in w.tokens:  # the server returns the active cluster the token made
                    cid = w.tokens[token]
                    return SimpleNamespace(result=lambda: w.clusters_by_id[cid])
                w.created += 1
                cid = f"new-{w.created}"
                w.calls.append(("create", kw))
                w.clusters_by_id[cid] = Rec(cluster_id=cid, state="RUNNING", **kw)
                if token:
                    w.tokens[token] = cid
                if w.lose_first_response:
                    w.lose_first_response = False
                    raise ConnectionError("the response was lost")
                return SimpleNamespace(result=lambda: w.clusters_by_id[cid])

            def wait_get_cluster_running(self, cid):
                return w.clusters_by_id[cid]

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

            def list_runs(self, **kw):
                w.calls.append(("list_runs", kw))
                if w.list_runs_error:
                    raise w.list_runs_error
                if kw.get("active_only"):  # the runs that have not ended, whenever they started
                    return iter(w.active_run_list)
                return iter(w.run_list)

        self.jar_mtime = {}
        self.run_list = []  # what jobs.list_runs returns: runs with a state, an end time and the cluster they ran on
        self.list_runs_error = None
        self.active_run_list = []  # what jobs.list_runs(active_only=True) returns, besides the window's
        self.clusters, self.files, self.libraries, self.jobs = Clusters(), Files(), Libraries(), Jobs()

        def do(method, path, body=None):  # the REST create `up` posts: the SDK's clusters.create has no idempotency_token
            assert (method, path) == ("POST", "/api/2.1/clusters/create")
            return {"cluster_id": self.clusters.create(**body).result().cluster_id}

        self.api_client = SimpleNamespace(do=do)

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


def lease(pool, w, wait_s=60, leases=None, **kw):
    """lease_cluster on the fake's clock: waiting advances it, so a lease that cannot be had ends in PoolError at once."""
    return pool.lease_cluster(
        w, "p", VOL, INFO, wait_s=wait_s, poll_s=10, sleep=lambda s: setattr(w, "now", w.now + s),
        clock=lambda: w.now, say=lambda s: None, leases=leases or pool.Leases(w, VOL, "p", lambda: w.now), **kw,
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
    assert all(kw["aws_attributes"].zone_id == "auto" for kw in made)  # not pinned to one throttled zone
    assert {kw["cluster_name"] for kw in made} == {"gbx-bench-p-1", "gbx-bench-p-2"}
    assert pool.up(w, "p", 3, spark_version="x", node_type="y", workers=1, user="me") == ids and w.created == 2  # nothing missing


def test_status_shows_state_and_idle_minutes_and_down_ends_only_the_pools_clusters(pool):
    w = FakeW()
    w.add("a"), w.add("t", state="TERMINATED"), w.add("b", name="other"), w.add(DENIED)
    rows = pool.status(w, "p", now=lambda: w.now)
    assert {(r["cluster_id"], r["state"], r["idle_min"]) for r in rows} == {("a", "RUNNING", 10.0), ("t", "TERMINATED", None)}
    assert pool.down(w, "p") == ["a", "t"] and w.calls.count(("terminate", "a")) == 1
    assert w.clusters_by_id["b"].state == "RUNNING" and w.clusters_by_id[DENIED].state == "RUNNING"
    assert not any(c[1] in ("b", DENIED) for c in w.calls if c[0] in ("terminate", "delete"))
    pool.down(w, "p", delete=True)
    assert "a" not in w.clusters_by_id and "b" in w.clusters_by_id


def test_the_lease_is_exclusive_and_a_second_launch_takes_another_cluster(pool):
    w = FakeW()
    w.add("a"), w.add("b")
    mine = pool.Leases(w, VOL, "p", lambda: w.now)
    first = lease(pool, w, leases=mine)
    second = lease(pool, w)
    assert {first, second} == {"a", "b"}
    with pytest.raises(pool.PoolError, match="every cluster of pool 'p' is leased"):
        lease(pool, w, wait_s=30)
    mine.release(first)
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


# ---- review: tokens, compare-and-swap reclaim, skipped clusters, jar paths, the full spec, settle before release


def test_a_holder_never_writes_a_lease_that_is_not_its_own_or_that_it_could_not_read(pool):
    w = FakeW()
    w.add("a")
    a = pool.Leases(w, VOL, "p", lambda: w.now)
    assert a.acquire("a", dict(INFO))
    path = a.path("a")
    mine = w.fs[path]
    # the lease was reclaimed by another launch: this holder's refresh writes nothing
    w.fs[path] = json.dumps({"run_id": "other", "token": "theirs", "touched_at": w.now}).encode()
    theirs = w.fs[path]
    with pytest.raises(pool.LeaseLost):
        a.update("a")
    a.release("a")  # and its release leaves the replacement's lease alone
    assert w.fs[path] == theirs
    # the lease cannot be read (the content is not downloadable): nothing is written over it, no sentinel included
    w.fs[path] = mine
    real = w.files.download
    w.files.download = lambda p: (_ for _ in ()).throw(PermissionError("not readable here"))
    with pytest.raises(pool.LeaseLost):
        a.update("a", job_run_id=5)
    w.files.download = real
    assert w.fs[path] == mine and b"unreadable" not in w.fs[path]


def test_two_launches_that_both_judge_a_lease_stale_do_not_both_take_it(pool):
    w = FakeW()
    w.add("a")
    old = pool.Leases(w, VOL, "p", lambda: w.now)
    assert old.acquire("a", dict(INFO))
    w.now += pool.TOUCH_GRACE_S + 1  # silent, no job run: stale
    a, b = (pool.Leases(w, VOL, "p", lambda: w.now) for _ in range(2))
    stale_seen = a.read("a")
    assert a.stale(stale_seen) and b.stale(b.read("a"))
    assert a.reclaim("a", stale_seen) and a.acquire("a", dict(INFO, run_id="A"))  # A is first
    # B judged the same stale lease; by now the file is A's fresh one: B must neither delete it nor take the cluster
    assert not b.reclaim("a", stale_seen)
    assert not b.acquire("a", dict(INFO, run_id="B"))
    assert json.loads(w.fs[a.path("a")])["run_id"] == "A"


def test_a_cluster_that_changed_since_the_listing_is_skipped_and_the_next_one_is_leased(pool):
    w = FakeW()
    w.add("a"), w.add("b")
    real_get = w.clusters.get

    def get(cid):
        if cid == "a":  # retagged between the listing and the lease
            c = real_get(cid)
            c.custom_tags = {"gbx_bench_pool": "elsewhere"}
            return c
        return real_get(cid)

    w.clusters.get = get
    assert lease(pool, w) == "b"


def test_a_jar_without_geobrix_in_its_path_is_still_the_clusters_to_manage(pool):
    w = FakeW()
    w.add("a", tags={"gbx_jar": "none"})
    jar = f"{VOL}/bench-extra.jar"
    assert pool.sync_jars(w, "a", [jar], say=lambda s: None, sleep=lambda s: None)["action"] == "reinstalled"
    w.calls.clear()
    assert pool.sync_jars(w, "a", [jar], say=lambda s: None)["action"] == "ok" and w.calls == []  # matched, not reinstalled


def test_the_edit_carries_every_setting_the_sdks_edit_takes_that_the_cluster_has(pool):
    w = FakeW()
    c = w.add("a", tags={"gbx_jar": "none"}, driver_instance_pool_id="pool-9", enable_local_disk_encryption=True, spark_conf={"k": "v"})

    def edit(cluster_id, spark_version, *, driver_instance_pool_id=None, enable_local_disk_encryption=None, spark_conf=None, custom_tags=None, autotermination_minutes=None, node_type_id=None, num_workers=None, data_security_mode=None, single_user_name=None, cluster_name=None):  # noqa: E501
        w.calls.append(("edit", cluster_id, dict(spark_version=spark_version, driver_instance_pool_id=driver_instance_pool_id, spark_conf=spark_conf, custom_tags=custom_tags, enc=enable_local_disk_encryption)))

    w.clusters.edit = edit
    pool.sync_jars(w, "a", [f"{VOL}/x.jar"], say=lambda s: None, sleep=lambda s: None)
    sent = next(x for x in w.calls if x[0] == "edit")[2]
    assert sent["spark_version"] == c.spark_version and sent["driver_instance_pool_id"] == "pool-9" and sent["spark_conf"] == {"k": "v"} and sent["enc"] is True  # a setting the fixed list lacks
    assert sent["custom_tags"]["gbx_bench_pool"] == "p" and sent["custom_tags"]["gbx_jar"] != "none"


def test_the_lease_is_released_only_when_the_job_it_submitted_has_ended(pool):
    w = FakeW()
    w.add("a")
    path = pool.Leases(w, VOL, "p").path("a")
    w.runs[5] = "RUNNING"
    with pool.leased_cluster(w, "p", VOL, {}, say=lambda s: None, settle_s=0, poll_s=1, sleep=lambda s: None) as ls:
        ls.job_run(5)
    assert path in w.fs  # the job is live: the lease stays with it
    held = json.loads(w.fs[path])
    assert held["job_run_id"] == 5
    w.fs.pop(path)
    w.runs[6] = "TERMINATED"
    with pool.leased_cluster(w, "p", VOL, {}, say=lambda s: None, settle_s=0) as ls:
        ls.job_run(6)
    assert path not in w.fs  # ended: released


def test_a_cluster_that_never_reaches_running_is_one_clear_error(pool):
    w = FakeW()

    def create(**kw):
        def result():
            raise TimeoutError("timed out after 0:20:00: Finding instances for new nodes")

        return SimpleNamespace(result=result)

    w.clusters.create = create
    with pytest.raises(pool.PoolError, match="still starting"):
        pool.up(w, "p", 1, spark_version="x", node_type="y", workers=1, user="me")


def test_a_notebook_run_on_a_leased_cluster_writes_its_job_run_on_the_lease(monkeypatch, pool):
    import pathlib

    import run_notebooks_serverless as rn

    class Stop(Exception):
        pass

    jobs_mod = types.ModuleType("databricks.sdk.service.jobs")
    jobs_mod.SubmitTask = jobs_mod.NotebookTask = lambda **kw: SimpleNamespace(**kw)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.jobs", jobs_mod)
    sys.modules["databricks.sdk.service"].jobs = jobs_mod
    monkeypatch.setattr(rn, "_import_notebook", lambda *a, **k: "/ws/nb")
    monkeypatch.setattr(rn, "submit_once", lambda w, **kw: SimpleNamespace(run_id=77))
    seen = []
    w = SimpleNamespace(jobs=SimpleNamespace(get_run=lambda *a, **k: (_ for _ in ()).throw(Stop())))
    with pytest.raises(Stop):
        rn.run_one(w, pathlib.Path("nb.ipynb"), "/ws", [], "6", 1, False, existing_cluster_id="0101-x", on_submit=seen.append)
    assert seen == [77]


def test_up_is_idempotent_a_lost_response_or_a_second_up_never_makes_a_second_cluster(pool):
    spec = dict(spark_version="x", node_type="y", workers=1, user="me")
    # the server makes the cluster and the response is lost: the retry finds it by its tag instead of creating another
    w = FakeW()
    w.lose_first_response = True
    ids = pool.up(w, "p", 1, **spec)
    assert w.created == 1 and ids == ["new-1"]
    # a second `up` running at the same time (it listed before the first created): the same slot token, the same cluster
    w2 = FakeW()
    first = pool.up(w2, "p", 1, **spec)
    w2.clusters.list = lambda: []  # the racer's listing predates the first create
    second = pool.up(w2, "p", 1, **spec)
    assert first == second and w2.created == 1
    # an existing cluster that ended is started by `up`, not duplicated
    w3 = FakeW()
    w3.add("t", state="TERMINATED")
    assert pool.up(w3, "p", 1, **spec) == ["t"] and w3.created == 0 and ("start", "t") in w3.calls


def test_an_unparseable_server_error_on_the_lease_create_reads_the_file_instead_of_crashing(pool, monkeypatch):
    # the SDK raises a TypeError of its own when the error body is not JSON: the lease file says what really happened
    monkeypatch.setattr(pool.time, "sleep", lambda s: None)
    w = FakeW()
    w.add("a")
    real = w.files.upload

    def other_got_it(path, contents, overwrite=False):  # another launch's create won; ours gets the unparseable error
        w.fs[path] = json.dumps({"token": "someone-else"}).encode()
        raise TypeError("object of type '_io.BytesIO' has no len()")

    w.files.upload = other_got_it
    assert pool.Leases(w, VOL, "p").acquire("a", {}) is False

    flaky = {"n": 0}

    def transient(path, contents, overwrite=False):  # nothing was created: a transient failure is tried again
        flaky["n"] += 1
        if flaky["n"] < 3:
            raise TypeError("object of type '_io.BytesIO' has no len()")
        return real(path, contents, overwrite=overwrite)

    w.fs.clear()
    w.files.upload = transient
    assert pool.Leases(w, VOL, "p").acquire("a", {}) is True and flaky["n"] == 3


def test_a_create_error_with_an_unreadable_lease_is_read_again_and_never_held_for_ever(pool, monkeypatch):
    # the create went through (our token is in the file) but the file cannot be read for a moment: the re-read finds it
    monkeypatch.setattr(pool.time, "sleep", lambda s: None)
    w = FakeW()
    w.add("a")
    real_upload, real_download = w.files.upload, w.files.download
    reads = {"n": 0}

    def created_then_error(path, contents, overwrite=False):
        real_upload(path, contents, overwrite=overwrite)
        raise TypeError("object of type '_io.BytesIO' has no len()")

    def unreadable_twice(path):
        reads["n"] += 1
        if reads["n"] <= 2:
            raise RuntimeError("503 try again")
        return real_download(path)

    w.files.upload, w.files.download = created_then_error, unreadable_twice
    leases = pool.Leases(w, VOL, "p")
    assert leases.acquire("a", {}) is True and "a" in leases.tokens

    # still unreadable after the re-reads: unknown, so not ours, and the caller moves on; no exception, a bounded wait
    w.fs.clear()
    slept = []
    monkeypatch.setattr(pool.time, "sleep", lambda s: slept.append(s))
    w.files.download = lambda path: (_ for _ in ()).throw(RuntimeError("503"))
    other = pool.Leases(w, VOL, "p")
    assert other.acquire("a", {}) is False and 0 < len(slept) <= 3
    # and the file it left is not held for ever: it goes stale by age and is reclaimed
    assert other.stale({"unreadable": True, "age_s": pool.UNREADABLE_STALE_S + 1}) is True


# ---- status: idle minutes come from real activity, not from last_activity_time alone


def _run(cluster, life="TERMINATED", end_s=None, task=False):
    inst = SimpleNamespace(cluster_id=cluster)
    run = SimpleNamespace(
        state=SimpleNamespace(life_cycle_state=life), end_time=int(end_s * 1000) if end_s else 0, start_time=1, cluster_instance=None, tasks=[]
    )
    if task:
        run.tasks = [SimpleNamespace(cluster_instance=inst)]
    else:
        run.cluster_instance = inst
    return run


def test_idle_is_counted_from_the_latest_real_activity_not_from_last_activity_time(pool):
    """last_activity_time does not move for a job run: a cluster whose run ended 5 min ago was reported idle for 112."""
    w = FakeW()
    c = w.add("a")
    c.last_activity_time = (w.now - 112 * 60) * 1000
    assert pool.status(w, "p", now=lambda: w.now)[0]["idle_min"] == 112.0 and pool.status(w, "p", now=lambda: w.now)[0]["idle_since"] == "cluster activity"
    w.run_list = [_run("a", end_s=w.now - 5 * 60), _run("a", end_s=w.now - 50 * 60, task=True), _run("other", end_s=w.now - 1)]
    row = pool.status(w, "p", now=lambda: w.now)[0]
    assert (row["idle_min"], row["idle_since"], row["busy"]) == (5.0, "job run", False)
    c.start_time = (w.now - 2 * 60) * 1000  # started after the run: the start is the latest signal
    row = pool.status(w, "p", now=lambda: w.now)[0]
    assert (row["idle_min"], row["idle_since"]) == (2.0, "cluster start")
    listed = [call[1] for call in w.calls if call[0] == "list_runs"][0]
    assert listed["start_time_from"] < (w.now - 3600) * 1000  # a window that reaches back past the autotermination


def test_a_job_run_that_started_before_the_window_or_beyond_the_newest_runs_still_makes_the_cluster_busy(pool):
    """The window (6 h) and the cap (500 newest) bound the finished runs; a live run older than both is asked for directly."""
    w = FakeW()
    w.add("a")
    old_live = _run("a", life="RUNNING")
    old_live.start_time = int((w.now - 30 * 3600) * 1000)  # started 30 h ago: outside the window
    w.active_run_list = [old_live]
    row = pool.status(w, "p", now=lambda: w.now)[0]
    assert row["busy"] is True and row["idle_min"] == 0.0
    assert any(c[0] == "list_runs" and c[1].get("active_only") for c in w.calls)


def test_the_touch_time_of_a_lease_counts_as_activity_and_a_stale_lease_is_not_busy(pool):
    w = FakeW()
    w.add("a").last_activity_time = (w.now - 112 * 60) * 1000
    leases = pool.Leases(w, VOL, "p", lambda: w.now)
    leases.acquire("a", dict(INFO, job_run_id=7))
    w.runs[7] = "RUNNING"
    w.now += 3 * 60
    row = pool.status(w, "p", now=lambda: w.now, leases=leases)[0]
    assert (row["busy"], row["idle_min"], row["idle_since"]) == (True, 0.0, "busy")
    assert row["holder"] == "r1 job 7"
    w.now += 10 * 60  # nobody refreshed it for 13 min, but its job run is live: still held
    assert pool.status(w, "p", now=lambda: w.now, leases=leases)[0]["busy"] is True
    w.runs[7] = "TERMINATED"  # stale now: free to take, and idle counts from the last touch
    row = pool.status(w, "p", now=lambda: w.now, leases=leases)[0]
    assert (row["busy"], row["idle_min"], row["idle_since"]) == (False, 13.0, "lease")
    assert row["holder"] == "r1 job 7 (stale)"


def test_a_live_job_run_makes_the_cluster_busy_with_no_lease_and_an_unreadable_run_list_is_no_failure(pool):
    w = FakeW()
    w.add("a"), w.add("t", state="TERMINATED")
    w.run_list = [_run("a", life="RUNNING"), _run("t", life="RUNNING")]
    rows = {r["cluster_id"]: r for r in pool.status(w, "p", now=lambda: w.now)}
    assert rows["a"]["busy"] is True and rows["a"]["idle_min"] == 0.0
    assert rows["t"]["busy"] is False and rows["t"]["idle_min"] is None  # a cluster that is not running is not busy
    w.run_list = []
    w.list_runs_error = RuntimeError("not allowed to list runs")
    row = pool.status(w, "p", now=lambda: w.now)[0]
    assert row["idle_min"] == 10.0 and row["busy"] is False  # the cluster's own signal still speaks


def test_status_says_busy_leased_by_the_run_for_a_leased_cluster(pool, monkeypatch, capsys):
    w = FakeW()
    w.add("a"), w.add("b")
    leases = pool.Leases(w, VOL, "p", lambda: w.now)
    leases.acquire("a", dict(INFO, job_run_id=7))
    w.runs[7] = "RUNNING"
    monkeypatch.setenv("GBX_BUNDLE_VOLUME_CATALOG", "c"), monkeypatch.setenv("GBX_BUNDLE_VOLUME_SCHEMA", "s"), monkeypatch.setenv("GBX_BUNDLE_VOLUME_NAME", "v")
    import bench_pool

    assert bench_pool.main(["status", "--name", "p"], w=w) == 0
    lines = {ln.split()[0]: ln for ln in capsys.readouterr().out.splitlines()}
    assert "busy (leased by r1 job 7)" in lines["a"]
    assert "idle " in lines["b"] and "(since cluster activity)" in lines["b"] and "lease free" in lines["b"]  # the real clock here
