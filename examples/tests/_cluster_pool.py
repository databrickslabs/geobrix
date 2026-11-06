"""A pool of long-lived clusters for work on Databricks, one run per cluster at a time (shared: `.claude/rules/cluster-work.md`).

A dedicated job cluster costs 2.5-5 minutes to start, and a launch pays it twice (the smoke is a job of its own). A pool
keeps N DBR19 clusters up for a working session; a launch LEASES one, runs its smoke and its main run on it, and gives it
back; the clusters end themselves after 60 idle minutes and are started again on the next lease.

The entry point for a launcher (``gbx:bench:cluster``, ``gbx:test:notebooks-serverless``, ``gbx:uc:tests``)::

    with leased_cluster(w, pool, volroot, info={"run_id": run_id}) as lease:
        lease.sync_jars(jar_paths)          # None: leave the cluster's jars alone; []: none wanted
        ... submit with existing_cluster_id=lease.cluster_id ...
        lease.job_run(run.run_id)           # so the lease follows the job, not this process
        lease.keep = True                   # a fire-and-forget launch: the job still holds the cluster after we exit

Safety rules, in code and not in convention:

* a cluster is the pool's only if its ``custom_tags`` say so (``gbx_bench_pool == <name>``); every call that touches a
  cluster checks that first, and ``DENY`` names clusters that are never touched whatever their tags say;
* ``down`` terminates the pool's clusters and nothing else.

The lease is a file on the Volume, created with the Files API ``upload(overwrite=False)`` (an exclusive create; FUSE
``open('x')`` is not guaranteed on a UC Volume). It names the launching run and, once submitted, the job run. It is stale
when its holder has not refreshed it for ``TOUCH_GRACE_S`` AND its job run (if it has one) is over: a live launcher
refreshes it every minute, a live job keeps it, and the next launcher reclaims it otherwise, so a crashed launcher never
holds a cluster for good. Cluster tags are not used for the lease: editing a
running cluster's tags restarts it.

JAR identity: a library installed on a running cluster cannot be replaced (the driver JVM keeps the classes it loaded),
so the jars a cluster carries are recorded in its ``gbx_jar`` tag as a fingerprint of (path, size, mtime) of each. A run
whose fingerprint differs uninstalls the cluster's geobrix jars, sets the tag (an edit, which restarts a running cluster)
and installs the run's jars. No jar wanted and none installed is the clean state.

Pure Python over an injected workspace client ``w``: tests give it a fake; no SDK import at module load.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import threading
import time
from typing import Callable, Dict, Iterable, List, Optional

POOL_TAG = "gbx_bench_pool"
JAR_TAG = "gbx_jar"
NO_JAR = "none"
DENY = frozenset({"1016-152458-i5qbli6q"})  # never touched, whatever its tags
AUTOTERMINATION_MIN = 60  # the safety net: an under-used or abandoned pool cluster ends itself
TOUCH_GRACE_S = 5 * 60  # a lease its holder has not refreshed for this long is stale unless its job run is live
TOUCH_EVERY_S = 60.0  # the holder refreshes its lease this often
UNREADABLE_STALE_S = 8 * 3600  # a lease that cannot be read is stale after this long without being refreshed
TERMINAL_RUN_STATES = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

# the ClusterDetails fields an `edit` must carry to leave the cluster as it was
_EDIT_FIELDS = (
    "cluster_name", "spark_version", "node_type_id", "driver_node_type_id", "num_workers", "autoscale",
    "autotermination_minutes", "data_security_mode", "single_user_name", "aws_attributes", "spark_conf",
    "spark_env_vars", "runtime_engine", "enable_elastic_disk", "init_scripts", "instance_pool_id",
    "driver_instance_pool_id", "policy_id", "apply_policy_default_values", "ssh_public_keys",
)  # fmt: skip


class PoolError(Exception):
    """A pool operation that must not go on (a cluster that is not the pool's, no cluster free in time, a failed install)."""


def _val(x):
    return getattr(x, "value", x)


def state_of(cluster) -> str:
    return str(_val(getattr(cluster, "state", "")) or "")


def tags_of(cluster) -> Dict[str, str]:
    return dict(getattr(cluster, "custom_tags", None) or {})


def is_member(cluster, name: str) -> bool:
    return (
        getattr(cluster, "cluster_id", None) not in DENY
        and tags_of(cluster).get(POOL_TAG) == name
    )


def members(w, name: str) -> list:
    """The clusters of pool `name`: tagged for it and not on the deny-list."""
    return [c for c in w.clusters.list() if is_member(c, name)]


def require_member(w, cluster_id: str, name: str):
    """The cluster, if it is the pool's; PoolError (before anything is done to it) when it is not."""
    if cluster_id in DENY:
        raise PoolError(f"{cluster_id} is on the deny-list: it is never touched")
    cluster = w.clusters.get(cluster_id)
    if not is_member(cluster, name):
        raise PoolError(f"{cluster_id} is not a member of pool '{name}' (tag {POOL_TAG} = {tags_of(cluster).get(POOL_TAG)!r})")
    return cluster


# ---- up / status / down -----------------------------------------------------------------------------------------


def up(w, name: str, n: int, *, spark_version: str, node_type: str, workers: int, user: str, on_demand: bool = True) -> List[str]:
    """Bring the pool to `n` clusters (creating the missing ones, waiting for them to run); their ids."""
    from databricks.sdk.service import compute

    have = members(w, name)
    taken = {c.cluster_name for c in have}
    made = []
    k = 0
    while len(have) + len(made) < n:
        k += 1
        cname = f"gbx-bench-{name}-{k}"
        if cname in taken:
            continue
        waiter = w.clusters.create(
            cluster_name=cname,
            spark_version=spark_version,
            node_type_id=node_type,
            num_workers=workers,
            autotermination_minutes=AUTOTERMINATION_MIN,
            data_security_mode=compute.DataSecurityMode.SINGLE_USER,
            single_user_name=user,
            custom_tags={POOL_TAG: name, "gbx_bench": "pool", JAR_TAG: NO_JAR},
            aws_attributes=(
                compute.AwsAttributes(availability=compute.AwsAvailability.ON_DEMAND, first_on_demand=workers + 1)
                if on_demand
                else None
            ),
        )
        made.append(waiter.result().cluster_id)
    return [c.cluster_id for c in have] + made


def status(w, name: str, now: Callable[[], float] = time.time) -> List[dict]:
    """One row per pool cluster: id, name, state, idle minutes (now - last activity), jar tag, autotermination."""
    rows = []
    for c in members(w, name):
        last = getattr(c, "last_activity_time", None) or getattr(c, "last_state_loss_time", None)
        idle = round((now() * 1000 - float(last)) / 60000.0, 1) if last else None
        rows.append(
            dict(
                cluster_id=c.cluster_id,
                name=c.cluster_name,
                state=state_of(c),
                idle_min=idle,
                jar=tags_of(c).get(JAR_TAG, NO_JAR),
                autotermination_min=getattr(c, "autotermination_minutes", None),
            )
        )
    return rows


def down(w, name: str, delete: bool = False) -> List[str]:
    """Terminate (or, with `delete`, remove) the pool's clusters; the ids. Nothing that is not the pool's."""
    done = []
    for c in members(w, name):
        require_member(w, c.cluster_id, name)  # read again, tags and all, just before acting
        (w.clusters.permanent_delete if delete else w.clusters.delete)(c.cluster_id)
        done.append(c.cluster_id)
    return done


# ---- the lease --------------------------------------------------------------------------------------------------


def _is_exists(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return any(t in text for t in ("alreadyexists", "already exists", "conflict", "409"))


def _is_missing(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return any(t in text for t in ("notfound", "not found", "does not exist", "404"))


class Leases:
    """The lease files of one pool under `<volroot>/bench-pool/<name>/`."""

    def __init__(self, w, volroot: str, name: str, clock: Callable[[], float] = time.time):
        self.w, self.dir, self.name, self.clock = w, f"{volroot.rstrip('/')}/bench-pool/{name}", name, clock

    def path(self, cluster_id: str) -> str:
        return f"{self.dir}/{cluster_id}.lease"

    def _put(self, cluster_id: str, info: dict, overwrite: bool) -> None:
        self.w.files.upload(self.path(cluster_id), io.BytesIO(json.dumps(info).encode("utf-8")), overwrite=overwrite)

    def acquire(self, cluster_id: str, info: dict) -> bool:
        """The exclusive create: True when the lease is ours, False when it exists."""
        info = dict(info, acquired_at=self.clock(), touched_at=self.clock())
        try:
            self.w.files.create_directory(self.dir)
        except Exception:  # noqa: BLE001 - it exists, or the upload below makes it
            pass
        try:
            self._put(cluster_id, info, overwrite=False)
            return True
        except Exception as exc:  # noqa: BLE001
            if _is_exists(exc):
                return False
            raise

    def update(self, cluster_id: str, **fields) -> None:
        """The holder adds what it learned (the job run id)."""
        info = self.read(cluster_id) or {}
        info.update({k: v for k, v in fields.items() if v is not None})
        info["touched_at"] = self.clock()
        self._put(cluster_id, info, overwrite=True)

    def read(self, cluster_id: str) -> Optional[dict]:
        """The lease's content; None when there is none; ``{"unreadable": True, "age_s": ...}`` when it cannot be read."""
        try:
            return json.loads(self.w.files.download(self.path(cluster_id)).contents.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            if _is_missing(exc):
                return None
            try:  # the file is there but its content is not readable here: its age still says something
                meta = self.w.files.get_metadata(self.path(cluster_id))
                return {"unreadable": True, "age_s": self._age(meta)}
            except Exception as exc2:  # noqa: BLE001
                if _is_missing(exc2):
                    return None
                raise

    def _age(self, meta) -> float:
        from email.utils import parsedate_to_datetime

        try:
            return self.clock() - parsedate_to_datetime(str(meta.last_modified)).timestamp()
        except Exception:  # noqa: BLE001
            return 0.0

    def release(self, cluster_id: str) -> None:
        try:
            self.w.files.delete(self.path(cluster_id))
        except Exception as exc:  # noqa: BLE001
            if not _is_missing(exc):
                raise

    def stale(self, info: dict) -> bool:
        """True when nothing holds the lease any more: its holder stopped refreshing it and its job run (if any) is over."""
        if info.get("unreadable"):
            return float(info.get("age_s", 0)) > UNREADABLE_STALE_S
        touched = float(info.get("touched_at", info.get("acquired_at", 0)))
        if self.clock() - touched <= TOUCH_GRACE_S:
            return False  # its launcher is alive
        run = info.get("job_run_id")
        if run is None:
            return True
        try:
            life = _val(self.w.jobs.get_run(run).state.life_cycle_state)
        except Exception as exc:  # noqa: BLE001
            return _is_missing(exc)  # a run that no longer exists holds nothing
        return str(life) in TERMINAL_RUN_STATES


def lease_cluster(
    w,
    name: str,
    volroot: str,
    info: dict,
    *,
    wait_s: float = 1800.0,
    poll_s: float = 15.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
    say: Callable[[str], None] = print,
) -> str:
    """Lease one cluster of the pool and make it run: the id.

    Running clusters are tried before terminated ones (no start to pay). A lease whose holder is gone is reclaimed.
    A terminated cluster is started and waited for. PoolError after `wait_s` with every cluster held.
    """
    leases = Leases(w, volroot, name, clock)
    end = clock() + wait_s
    while True:
        pool = members(w, name)
        if not pool:
            raise PoolError(f"pool '{name}' has no clusters: gbx:bench:pool up --name {name} --n <n>")
        order = {"RUNNING": 0, "TERMINATED": 1}
        for c in sorted(pool, key=lambda c: order.get(state_of(c), 2)):
            require_member(w, c.cluster_id, name)
            if _try_lease(leases, c.cluster_id, info):
                say(f"pool {name}: leased {c.cluster_id} ({state_of(c) or '?'})")
                w.clusters.ensure_cluster_is_running(c.cluster_id)  # starts it when terminated, waits when pending
                return c.cluster_id
        if clock() >= end:
            raise PoolError(f"every cluster of pool '{name}' is leased; none came free in {wait_s:.0f}s")
        say(f"pool {name}: every cluster is leased; waiting {poll_s:.0f}s")
        sleep(poll_s)


def _try_lease(leases: Leases, cluster_id: str, info: dict) -> bool:
    if leases.acquire(cluster_id, info):
        return True
    held = leases.read(cluster_id)
    if held is None:  # released between the two calls
        return leases.acquire(cluster_id, info)
    if leases.stale(held):
        leases.release(cluster_id)
        return leases.acquire(cluster_id, info)  # whoever reclaims first wins
    return False


# ---- jars on a reused cluster -----------------------------------------------------------------------------------


def jar_fingerprint(w, jar_paths: Iterable[str]) -> str:
    """Identity of a set of jars on the Volume: path, size and mtime of each (an overwritten file changes it)."""
    parts = []
    for p in sorted(jar_paths):
        meta = w.files.get_metadata(p)
        parts.append(f"{p}|{getattr(meta, 'content_length', '')}|{getattr(meta, 'last_modified', '')}")
    return hashlib.sha1("\n".join(parts).encode()).hexdigest()[:16] if parts else NO_JAR


def installed_jars(w, cluster_id: str) -> List[str]:
    return [
        fs.library.jar
        for fs in w.libraries.cluster_status(cluster_id)
        if getattr(fs, "library", None) and getattr(fs.library, "jar", None) and "geobrix" in fs.library.jar.lower()
    ]


def sync_jars(
    w,
    cluster_id: str,
    jar_paths: Iterable[str],
    *,
    wait_s: float = 900.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
    say: Callable[[str], None] = print,
) -> dict:
    """Make the cluster carry exactly `jar_paths`: ``{"action": "ok" | "reinstalled", "fingerprint": ...}``.

    The cluster's tag says what it carries. A match changes nothing. Otherwise the geobrix jars it has are uninstalled,
    the tag is set (an edit: a running cluster restarts, which is what drops the classes of the old jar) and the wanted
    jars are installed and waited for.
    """
    from databricks.sdk.service import compute

    jar_paths = sorted(set(jar_paths))
    want = jar_fingerprint(w, jar_paths)
    cluster = w.clusters.get(cluster_id)
    have_tag = tags_of(cluster).get(JAR_TAG, NO_JAR)
    have = installed_jars(w, cluster_id)
    if have_tag == want and sorted(have) == jar_paths:
        return {"action": "ok", "fingerprint": want}
    say(f"pool: cluster {cluster_id} carries jar {have_tag} ({len(have)} installed), this run needs {want}: reinstalling")
    if have:
        w.libraries.uninstall(cluster_id, [compute.Library(jar=p) for p in have])
    fields = {f: getattr(cluster, f) for f in _EDIT_FIELDS if getattr(cluster, f, None) is not None}
    fields["custom_tags"] = {**tags_of(cluster), JAR_TAG: want}
    w.clusters.edit(cluster_id=cluster_id, **fields)  # restarts a running cluster; a terminated one is only edited
    w.clusters.ensure_cluster_is_running(cluster_id)
    if jar_paths:
        w.libraries.install(cluster_id, [compute.Library(jar=p) for p in jar_paths])
        _wait_installed(w, cluster_id, jar_paths, wait_s, sleep, clock)
    return {"action": "reinstalled", "fingerprint": want}


def _wait_installed(w, cluster_id, jar_paths, wait_s, sleep, clock) -> None:
    end = clock() + wait_s
    while True:
        st = {fs.library.jar: str(_val(fs.status)) for fs in w.libraries.cluster_status(cluster_id) if getattr(fs, "library", None) and getattr(fs.library, "jar", None)}
        bad = [p for p in jar_paths if st.get(p) == "FAILED"]
        if bad:
            raise PoolError(f"installing {bad[0]} on {cluster_id} failed")
        if all(st.get(p) == "INSTALLED" for p in jar_paths):
            return
        if clock() >= end:
            raise PoolError(f"{jar_paths} not installed on {cluster_id} within {wait_s:.0f}s: {st}")
        sleep(5)


# ---- the entry point --------------------------------------------------------------------------------------------


class Lease:
    """A leased cluster, as the launcher uses it. ``keep`` leaves the lease in place at exit (a launch that does not wait:
    its job still uses the cluster, and the job run id on the lease says so)."""

    def __init__(self, w, leases: Leases, cluster_id: str, say: Callable[[str], None]):
        self.w, self.leases, self.cluster_id, self.say = w, leases, cluster_id, say
        self.keep = False
        self.jars: Optional[dict] = None

    def sync_jars(self, jar_paths: Optional[Iterable[str]], **kw) -> Optional[dict]:
        """Make the cluster carry exactly these jars (None: do not look). Returns what was done."""
        if jar_paths is None:
            return None
        self.jars = sync_jars(self.w, self.cluster_id, jar_paths, say=self.say, **kw)
        return self.jars

    def job_run(self, run_id) -> None:
        """Write the job run on the lease: from here the lease follows the job, not this process."""
        self.leases.update(self.cluster_id, job_run_id=run_id)


@contextlib.contextmanager
def leased_cluster(
    w,
    name: str,
    volroot: str,
    info: Optional[dict] = None,
    *,
    wait_s: float = 1800.0,
    poll_s: float = 15.0,
    touch_every_s: float = TOUCH_EVERY_S,
    say: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
):
    """Lease a cluster of pool `name` for the length of the block; yields a `Lease`.

    The lease is refreshed in the background while the block runs, and released at its end (an error included) unless
    ``lease.keep`` is set. A cluster that has ended is started first.
    """
    cluster_id = lease_cluster(w, name, volroot, dict(info or {}), wait_s=wait_s, poll_s=poll_s, sleep=sleep, clock=clock, say=say)
    leases = Leases(w, volroot, name, clock)
    lease = Lease(w, leases, cluster_id, say)
    stop = threading.Event()

    def touch():
        while not stop.wait(touch_every_s):
            try:
                leases.update(cluster_id)
            except Exception:  # noqa: BLE001 - a missed refresh is covered by the grace
                pass

    thread = threading.Thread(target=touch, name="gbx-lease-touch", daemon=True)
    thread.start()
    try:
        yield lease
    finally:
        stop.set()
        thread.join(timeout=5)
        if not lease.keep:
            try:
                leases.release(cluster_id)
            except Exception as exc:  # noqa: BLE001 - the lease goes stale by itself
                say(f"pool {name}: could not release the lease of {cluster_id} ({type(exc).__name__}); it goes stale in {TOUCH_GRACE_S // 60} min")
