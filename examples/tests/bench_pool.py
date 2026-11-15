"""gbx:bench:pool -- bring a pool of bench clusters up, show it, take it down.

    bench_pool.py up     --name P --n CAP [--warm K] [--spark-version 19.x-scala2.13] [--node-type m5d.2xlarge] [--workers 4 | --single-node]
    bench_pool.py status --name P
    bench_pool.py down   --name P [--delete]

A pool is a CAP on how many clusters are active at once, not a fixed set. ``up`` records the cap and the shape of a cluster
(next to the leases on the bundle Volume) and starts ``--warm`` clusters now (default 1); a launch that finds every cluster
leased starts a stopped one, else makes one, while fewer than the cap are active, else waits. The clusters carry the tag
``gbx_bench_pool=<P>`` and end themselves after 60 idle minutes. Only clusters with that tag are ever touched; see
``_cluster_pool.py`` for the rules and for how a launch leases one (``--pool P``).
"""

from __future__ import annotations

import argparse
import os
import sys

import _cluster_pool as pool


def _client(profile):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()


def main(argv=None, w=None) -> int:
    ap = argparse.ArgumentParser(prog="bench_pool.py", description=__doc__.split("\n\n")[0])
    ap.add_argument("action", choices=("up", "status", "down"))
    ap.add_argument("--name", default=os.environ.get("GBX_BENCH_POOL", ""), help="the pool's name (tag value)")
    ap.add_argument("--n", type=int, default=1, help="up: the pool's cap, the most clusters active at once")
    ap.add_argument("--warm", type=int, default=None, help="up: clusters to start now (default 1; 0 starts none; without a Volume configured, all of --n)")
    ap.add_argument("--spark-version", default="19.x-scala2.13")
    ap.add_argument("--node-type", default="m5d.2xlarge")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--single-node", action="store_true", help="up: single-node clusters (the driver alone; --workers is ignored)")
    ap.add_argument("--delete", action="store_true", help="down: remove the clusters, not only terminate them")
    ap.add_argument("--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE", ""))
    a = ap.parse_args(argv)
    if not a.name:
        print("ERROR: --name <pool> (or GBX_BENCH_POOL) is required.", file=sys.stderr)
        return 2
    w = w or _client(a.profile)
    try:
        if a.action == "up":
            me = w.current_user.me().user_name
            vol = _volroot()
            warm = a.warm if a.warm is not None else (1 if vol else None)
            if not vol:
                print("NOTE: no GBX_BUNDLE_VOLUME_* configured, so the cap cannot be recorded: starting all --n clusters, as before.", file=sys.stderr)
            ids = pool.up(w, a.name, a.n, spark_version=a.spark_version, node_type=a.node_type, workers=a.workers, user=me, single_node=a.single_node,
                          volroot=vol, warm=warm)
            cap = f"cap {a.n}, " if vol else ""
            print(f"pool {a.name}: {cap}{len(ids)} cluster(s): {', '.join(ids) or 'none started'} (auto-termination {pool.AUTOTERMINATION_MIN} min)")
        elif a.action == "status":
            vol = _volroot()
            leases = pool.Leases(w, vol, a.name) if vol else None
            rows = pool.status(w, a.name, leases=leases)
            if leases is not None:
                sm = pool.summary(w, a.name, leases=leases)
                cap = sm["cap"] if sm["cap"] is not None else "not recorded"
                print(f"pool {a.name}: cap {cap}, active {sm['active']}, idle {sm['idle']}, leased {sm['leased']}, terminated {sm['terminated']}")
            if not rows:
                print(f"pool {a.name}: no clusters")
            for r in rows:
                print(f"{r['cluster_id']}  {r['name']:<24} {r['state']:<12} {_activity(r)}  jar {r['jar']}")
        else:
            ids = pool.down(w, a.name, delete=a.delete)
            print(f"pool {a.name}: {'removed' if a.delete else 'terminated'} {len(ids)} cluster(s): {', '.join(ids)}")
    except pool.PoolError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


def _volroot() -> str:
    cat, sch, vol = (os.environ.get(k) for k in ("GBX_BUNDLE_VOLUME_CATALOG", "GBX_BUNDLE_VOLUME_SCHEMA", "GBX_BUNDLE_VOLUME_NAME"))
    return f"/Volumes/{cat}/{sch}/{vol}" if cat and sch and vol else ""


def _activity(r) -> str:
    """One cluster's line: busy (and who holds it), or how long it has been idle and by what signal, and the lease."""
    if r["busy"]:
        who = f"leased by {r['holder']}" if r["holder"] and "(stale)" not in r["holder"] else "a job run is on it"
        return f"busy ({who})"
    if r["idle_min"] is None:
        idle = "not running"
    else:
        idle = f"idle {r['idle_min']:g} min (since {r['idle_since']})"
    return f"{idle}, lease {r['holder'] or 'free'}"


if __name__ == "__main__":
    sys.exit(main())
