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
import uuid
from types import SimpleNamespace
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


def _slot_token(name: str, cname: str, user: str) -> str:
    """The idempotency token of one pool slot: the same for every call that wants that slot, so a create that is retried
    (by the SDK after a lost response, or by a second `up` running at the same time) returns the cluster the first made."""
    return hashlib.sha1(f"gbx-pool:{name}:{cname}:{user}".encode()).hexdigest()


def _create_cluster(w, spec: dict, token: str):
    """Create a cluster under an idempotency token and give the waiter for its RUNNING state. The SDK's
    `clusters.create` has no `idempotency_token` argument (it raises TypeError), so this posts the REST call itself."""

    def plain(v):
        if hasattr(v, "as_dict"):
            return v.as_dict()
        return v.value if hasattr(v, "value") else v

    body = {k: plain(v) for k, v in spec.items() if v is not None}
    body["idempotency_token"] = token
    cid = w.api_client.do("POST", "/api/2.1/clusters/create", body=body)["cluster_id"]
    return SimpleNamespace(result=lambda: w.clusters.wait_get_cluster_running(cid))


SINGLE_NODE_CONF = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*, 4]"}
SINGLE_NODE_TAGS = {"ResourceClass": "SingleNode"}


def up(w, name: str, n: int, *, spark_version: str, node_type: str, workers: int, user: str, on_demand: bool = True, single_node: bool = False) -> List[str]:
    """Bring the pool to `n` clusters and make them run; their ids. Idempotent: a cluster the pool already has is reused
    (and started if it ended), a missing slot is created once under a token of its own, and a create that failed after the
    server made the cluster finds it by its tag before it makes another."""
    from databricks.sdk.service import compute

    if single_node:
        workers = 0  # a single-node cluster is its driver alone
    have = members(w, name)
    ids = [c.cluster_id for c in have]
    for c in have:
        if state_of(c) == "TERMINATED":
            w.clusters.ensure_cluster_is_running(c.cluster_id)
    taken = {c.cluster_name for c in have}
    k = 0
    while len(ids) < n:
        k += 1
        cname = f"gbx-bench-{name}-{k}"
        if cname in taken:
            continue
        spec = dict(
            cluster_name=cname,
            spark_version=spark_version,
            node_type_id=node_type,
            num_workers=workers,
            autotermination_minutes=AUTOTERMINATION_MIN,
            data_security_mode=compute.DataSecurityMode.SINGLE_USER,
            single_user_name=user,
            custom_tags={POOL_TAG: name, "gbx_bench": "pool", JAR_TAG: NO_JAR, **(SINGLE_NODE_TAGS if single_node else {})},
            spark_conf=dict(SINGLE_NODE_CONF) if single_node else None,
            aws_attributes=(
                compute.AwsAttributes(
                    availability=compute.AwsAvailability.ON_DEMAND,
                    first_on_demand=workers + 1,
                    zone_id="auto",  # the workspace's default zone can be throttled for hours: let Databricks pick one
                )
                if on_demand
                else None
            ),
        )
        try:
            waiter = _create_cluster(w, spec, _slot_token(name, cname, user))
            ids.append(waiter.result().cluster_id)
        except TimeoutError as exc:
            raise PoolError(
                f"cluster {cname} did not reach RUNNING in time ({exc}); it is still starting, or the workspace has no "
                f"capacity for its nodes. `status --name {name}` shows it; `down --name {name}` ends it."
            ) from exc
        except Exception as exc:  # noqa: BLE001 - the server may have made it before the response was lost
            found = [c for c in members(w, name) if c.cluster_name == cname]
            if not found:
                raise
            say = f"pool {name}: create of {cname} failed after the server made it ({type(exc).__name__}); using {found[0].cluster_id}"
            print(say)
            w.clusters.ensure_cluster_is_running(found[0].cluster_id)
            ids.append(found[0].cluster_id)
    return ids


JOB_SCAN_WINDOW_S = 6 * 3600  # job runs started this long ago are looked at for what ran on a cluster
JOB_SCAN_MAX_RUNS = 500  # and no more than this many of them (the newest first): a busy workspace has thousands


def _ms(x) -> Optional[float]:
    try:
        return float(x) if x else None
    except (TypeError, ValueError):
        return None


def job_activity(w, now_s: float):
    """``(latest, live)`` from the workspace's recent job runs: ``latest`` is ``{cluster_id: epoch ms}`` of the last run
    activity on each cluster, ``live`` the set of clusters a run that has not ended is on.

    A run that has not ended counts as active now; a finished one counts at its end time. The cluster's own
    ``last_activity_time`` is not enough: it does not move for a job run. Runs whose cluster cannot be told, or a list
    that cannot be read, leave the cluster's other signals to speak (nothing here).
    """
    out: Dict[str, float] = {}
    live: set = set()

    def scan(runs) -> None:
        for n, run in enumerate(runs):
            if n >= JOB_SCAN_MAX_RUNS:
                break
            life = str(_val(getattr(getattr(run, "state", None), "life_cycle_state", None)))
            ended = life in TERMINAL_RUN_STATES
            when = (_ms(getattr(run, "end_time", None)) or _ms(getattr(run, "start_time", None)) or 0.0) if ended else now_s * 1000
            instances = [getattr(run, "cluster_instance", None)] + [
                getattr(t, "cluster_instance", None) for t in (getattr(run, "tasks", None) or [])
            ]
            for inst in instances:
                cid = getattr(inst, "cluster_id", None)
                if cid:
                    out[cid] = max(out.get(cid, 0.0), when)
                    if not ended:
                        live.add(cid)

    # The window and the cap bound what finished; a run that is still going may have started before the window or sit
    # beyond the newest runs of a busy workspace, so the runs that have not ended are asked for directly as well.
    for kw in ({"start_time_from": int((now_s - JOB_SCAN_WINDOW_S) * 1000)}, {"active_only": True}):
        try:
            scan(w.jobs.list_runs(expand_tasks=True, **kw))
        except Exception:  # noqa: BLE001 - a missing signal, not a failure of the status
            pass
    return out, live


def status(w, name: str, now: Callable[[], float] = time.time, leases: Optional["Leases"] = None) -> List[dict]:
    """One row per pool cluster: id, name, state, jar tag, autotermination, and how busy it is.

    ``busy`` is True while a live lease holds the cluster or a job run is on it; ``holder`` is then the lease (run id and
    job run id). ``idle_min`` is the minutes since the cluster's latest real activity (None for a cluster that is not
    running): the latest of the cluster's ``last_activity_time`` (which does not move for a job run), its start, the end
    of the last job run on it, and the touch time of its lease. ``idle_since`` names the signal that was latest.
    """
    rows = []
    t = now()
    jobs, live = job_activity(w, t)
    for c in members(w, name):
        signals = {
            "cluster activity": _ms(getattr(c, "last_activity_time", None)),
            "cluster start": _ms(getattr(c, "start_time", None)),
            "job run": jobs.get(c.cluster_id),
        }
        holder, busy = None, c.cluster_id in live
        info = leases.read(c.cluster_id) if leases is not None else None
        if info is not None and info.get("unreadable"):
            holder, busy = "unreadable lease", busy or not leases.stale(info)
        elif info is not None:
            stale = leases.stale(info)
            busy = busy or not stale
            holder = f"{info.get('run_id', '?')} job {info.get('job_run_id', '-')}" + (" (stale)" if stale else "")
            touched = _ms(info.get("touched_at", info.get("acquired_at")))
            signals["lease"] = touched * 1000 if touched else None
        running = state_of(c) == "RUNNING"
        busy = busy and running
        known = {k: v for k, v in signals.items() if v}
        idle, since = None, None
        if running and known:
            since, last = max(known.items(), key=lambda kv: kv[1])
            idle = round(max(0.0, t * 1000 - last) / 60000.0, 1)
        rows.append(
            dict(
                cluster_id=c.cluster_id,
                name=c.cluster_name,
                state=state_of(c),
                busy=busy,
                holder=holder,
                idle_min=0.0 if busy else idle,
                idle_since="busy" if busy else since,
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


class LeaseLost(PoolError):
    """The lease is no longer this holder's (it was reclaimed and taken by another launch, or cannot be read)."""


class Leases:
    """The lease files of one pool under `<volroot>/bench-pool/<name>/`.

    Every lease carries the holder's own token. A holder writes, refreshes and releases a lease only while the file still
    holds its token (a launch that was judged gone and replaced must not overwrite or delete its replacement's), and
    never writes anything it could not read.
    """

    def __init__(self, w, volroot: str, name: str, clock: Callable[[], float] = time.time):
        self.w, self.dir, self.name, self.clock = w, f"{volroot.rstrip('/')}/bench-pool/{name}", name, clock
        self.tokens: Dict[str, str] = {}

    def path(self, cluster_id: str) -> str:
        return f"{self.dir}/{cluster_id}.lease"

    def _put(self, cluster_id: str, info: dict, overwrite: bool) -> None:
        self.w.files.upload(self.path(cluster_id), io.BytesIO(json.dumps(info).encode("utf-8")), overwrite=overwrite)

    def acquire(self, cluster_id: str, info: dict) -> bool:
        """The exclusive create: True when the lease is ours (its token is kept), False when it exists."""
        token = uuid.uuid4().hex
        info = dict(info, acquired_at=self.clock(), touched_at=self.clock(), token=token)
        try:
            self.w.files.create_directory(self.dir)
        except Exception:  # noqa: BLE001 - it exists, or the upload below makes it
            pass
        for attempt in range(4):
            try:
                self._put(cluster_id, info, overwrite=False)
                break
            except Exception as exc:  # noqa: BLE001
                if _is_exists(exc):
                    return False
                # The SDK cannot always parse a server error (a non-JSON body makes it raise a TypeError of its own),
                # so the exception does not say what happened. The file does: someone else's lease means "taken",
                # our own token means the create went through, nothing there means a transient failure to try again.
                held = self.read(cluster_id)
                for _ in range(3):  # a file that exists but cannot be read yet (just made, not yet consistent): read again
                    if not (held and held.get("unreadable")):
                        break
                    time.sleep(1.0)
                    held = self.read(cluster_id)
                if held is not None:
                    if held.get("token") == token:
                        break
                    # Another launch's lease, or a file still unreadable after the re-reads: which of the two is not
                    # known, so this cluster is not ours to take and the caller tries the next one. An unreadable file
                    # is not held for ever: `stale` ends it by age (UNREADABLE_STALE_S) and `reclaim` replaces it.
                    return False
                if attempt == 3:
                    raise
                time.sleep(2.0 * (attempt + 1))
        self.tokens[cluster_id] = token
        return True

    def _mine(self, cluster_id: str) -> dict:
        """The lease's content, when it is this holder's; LeaseLost when it is another's, gone, or unreadable."""
        held = self.read(cluster_id)
        if not held or held.get("unreadable") or held.get("token") != self.tokens.get(cluster_id):
            raise LeaseLost(f"the lease of {cluster_id} is no longer this launch's")
        return held

    def update(self, cluster_id: str, **fields) -> None:
        """The holder adds what it learned (the job run id) and refreshes the lease. LeaseLost, and nothing written, when the
        lease is not this holder's."""
        info = self._mine(cluster_id)
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
        """Delete the lease, if it is still this holder's (another launch's replacement is left alone)."""
        try:
            self._mine(cluster_id)
        except LeaseLost:
            return
        self._delete(cluster_id)

    def _delete(self, cluster_id: str) -> None:
        try:
            self.w.files.delete(self.path(cluster_id))
        except Exception as exc:  # noqa: BLE001
            if not _is_missing(exc):
                raise

    def reclaim(self, cluster_id: str, stale_info: dict) -> bool:
        """Take over a lease judged stale: delete exactly that lease (not a fresher one a faster launch put in its place),
        then create ours exclusively. False (back off) when it changed under us or another launch created first."""
        now = self.read(cluster_id)
        if now is None:
            return True
        same = (now.get("unreadable") and stale_info.get("unreadable")) or now == stale_info
        if not same:
            return False  # another launch reclaimed it, or its holder is back: not the lease we judged
        self._delete(cluster_id)
        return True

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
    leases: Optional["Leases"] = None,
    on_leased: Optional[Callable[[str], None]] = None,
) -> str:
    """Lease one cluster of the pool and make it run: the id. (`leases` is the holder's: it keeps the lease's token.)

    Running clusters are tried before terminated ones (no start to pay). A lease whose holder is gone is reclaimed.
    A terminated cluster is started and waited for. PoolError after `wait_s` with every cluster held.

    `on_leased(cluster_id)` is called the moment the lease is ours, before the cluster is started: the start of a
    terminated cluster can take longer than the grace a silent lease is given (TOUCH_GRACE_S), so the holder must be
    refreshing the lease by then.
    """
    leases = leases or Leases(w, volroot, name, clock)
    end = clock() + wait_s
    while True:
        pool = members(w, name)
        if not pool:
            raise PoolError(f"pool '{name}' has no clusters: gbx:bench:pool up --name {name} --n <n>")
        order = {"RUNNING": 0, "TERMINATED": 1}
        for c in sorted(pool, key=lambda c: order.get(state_of(c), 2)):
            try:
                require_member(w, c.cluster_id, name)  # read again, tags and all, just before acting
            except PoolError as exc:  # it changed since the listing (retagged, gone): this one is skipped, not the lease
                say(f"pool {name}: skipping {c.cluster_id}: {exc}")
                continue
            if _try_lease(leases, c.cluster_id, info):
                say(f"pool {name}: leased {c.cluster_id} ({state_of(c) or '?'})")
                if on_leased is not None:
                    on_leased(c.cluster_id)
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
    if leases.stale(held) and leases.reclaim(cluster_id, held):
        return leases.acquire(cluster_id, info)  # exclusive: whoever creates first wins, the other backs off
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
    """Every jar library of the cluster: a pool cluster carries only what a run put there, whatever the jar is called."""
    return [
        fs.library.jar
        for fs in w.libraries.cluster_status(cluster_id)
        if getattr(fs, "library", None) and getattr(fs.library, "jar", None)
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
    fields = _edit_fields(w, cluster)
    fields["custom_tags"] = {**tags_of(cluster), JAR_TAG: want}
    w.clusters.edit(cluster_id=cluster_id, **fields)  # restarts a running cluster; a terminated one is only edited
    w.clusters.ensure_cluster_is_running(cluster_id)
    if jar_paths:
        w.libraries.install(cluster_id, [compute.Library(jar=p) for p in jar_paths])
        _wait_installed(w, cluster_id, jar_paths, wait_s, sleep, clock)
    return {"action": "reinstalled", "fingerprint": want}


def _edit_fields(w, cluster) -> dict:
    """The cluster's whole spec as the keyword arguments of `clusters.edit` (which replaces the spec): every setting the
    SDK's edit takes that the cluster has, so an edit of the tag leaves the rest as it was. A client whose `edit` takes
    only **kwargs (a fake) gets the fixed list."""
    import inspect

    try:
        params = inspect.signature(w.clusters.edit).parameters
        names = [n for n, p in params.items() if n not in ("self", "cluster_id") and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)]
    except (TypeError, ValueError):
        names = []
    names = names or list(_EDIT_FIELDS)
    return {n: getattr(cluster, n) for n in names if getattr(cluster, n, None) is not None}


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
        self.job_runs: list = []

    def sync_jars(self, jar_paths: Optional[Iterable[str]], **kw) -> Optional[dict]:
        """Make the cluster carry exactly these jars (None: do not look). Returns what was done."""
        if jar_paths is None:
            return None
        self.leases._mine(self.cluster_id)  # LeaseLost now, not after a reinstall and restart of a cluster another launch holds
        self.jars = sync_jars(self.w, self.cluster_id, jar_paths, say=self.say, **kw)
        return self.jars

    def job_run(self, run_id) -> None:
        """Write the job run on the lease: from here the lease follows the job, not this process."""
        self.job_runs.append(run_id)
        self.leases.update(self.cluster_id, job_run_id=run_id)

    def jobs_over(self) -> bool:
        """True when every job run this launch submitted on the cluster has ended."""
        for run in self.job_runs:
            try:
                if str(_val(self.w.jobs.get_run(run).state.life_cycle_state)) not in TERMINAL_RUN_STATES:
                    return False
            except Exception as exc:  # noqa: BLE001
                if not _is_missing(exc):
                    return False
        return True


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
    settle_s: float = 120.0,
    say: Callable[[str], None] = print,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
):
    """Lease a cluster of pool `name` for the length of the block; yields a `Lease`.

    The lease is refreshed in the background while the block runs, and released at its end (an error included) unless
    ``lease.keep`` is set. It is released only when the job runs it submitted have ended (a launch that stopped waiting
    leaves the cluster to its job: the lease goes stale by itself once the job is over); `settle_s` is how long it waits
    for them. A cluster that has ended is started first.
    """
    leases = Leases(w, volroot, name, clock)
    stop = threading.Event()
    threads: list = []

    def touch(cluster_id):
        while not stop.wait(touch_every_s):
            try:
                leases.update(cluster_id)
            except LeaseLost:
                say(f"pool {name}: the lease of {cluster_id} was lost (reclaimed by another launch)")
                return
            except Exception:  # noqa: BLE001 - a missed refresh is covered by the grace
                pass

    def start_touch(cluster_id):
        # Started as soon as the lease is ours, so the refresh covers everything that follows: the start of a terminated
        # cluster, the jar reinstall and restart, the smoke and the run. (Started only after the cluster was running, a
        # start slower than TOUCH_GRACE_S let another launch judge the lease stale and take it.)
        t = threading.Thread(target=touch, args=(cluster_id,), name="gbx-lease-touch", daemon=True)
        t.start()
        threads.append(t)

    def stop_touch():
        stop.set()
        for t in threads:
            t.join(timeout=5)

    try:
        cluster_id = lease_cluster(w, name, volroot, dict(info or {}), wait_s=wait_s, poll_s=poll_s, sleep=sleep, clock=clock, say=say, leases=leases, on_leased=start_touch)
    except BaseException:
        stop_touch()
        for held in list(leases.tokens):  # leased, but the cluster never started: do not hold it until the grace runs out
            try:
                leases.release(held)
            except Exception:  # noqa: BLE001 - the lease goes stale by itself
                pass
        raise
    lease = Lease(w, leases, cluster_id, say)
    try:
        yield lease
    finally:
        stop_touch()
        end = clock() + settle_s
        while not lease.keep and not lease.jobs_over() and clock() < end:
            sleep(min(5.0, poll_s))
        if not lease.keep and not lease.jobs_over():
            lease.keep = True  # a job is still on the cluster: its run id on the lease holds it until the job ends
            say(f"pool {name}: a job run is still live on {cluster_id}; the lease stays with it")
        if not lease.keep:
            try:
                leases.release(cluster_id)
            except Exception as exc:  # noqa: BLE001 - the lease goes stale by itself
                say(f"pool {name}: could not release the lease of {cluster_id} ({type(exc).__name__}); it goes stale in {TOUCH_GRACE_S // 60} min")
