# gbx:bench:pool

A pool caps how many DBR 19 clusters are active at once and starts them on demand, so a run seldom pays a cluster start. A launch leases one free cluster (one run per cluster at a time). It runs its smoke and its main run there, then gives the cluster back.

## Usage

`bash scripts/commands/gbx-bench-pool.sh <up|status|down> --name <pool> [options] [--profile <name>]`

- `up --n <cap>`: record the pool's cap and the shape of its clusters, and start `--warm <k>` clusters now. The default is 1; `--warm 0` starts none. `--n` is the most clusters active at once, not how many to start. Without a Volume configured (`GBX_BUNDLE_VOLUME_*`) the cap cannot be recorded, so all n start. `--spark-version` (default `19.x-scala2.13`), `--node-type` (default `m5d.2xlarge`), `--workers` (default 4), `--single-node` (the driver alone: `--workers` is ignored). The clusters are dedicated (single user: you), on demand, tagged `gbx_bench_pool=<name>`, and end themselves after **60 idle minutes**.
- `status`: first a line with the cap, and how many clusters are active, idle, leased and terminated. Then each pool cluster's state, the jar fingerprint it carries, and either `busy (leased by <run> job <id>)` or how long it has been idle. A cluster is busy while a live lease holds it or a job run is on it. Its idle time counts from the latest of four signals. They are its own last activity, its start, the end of the last job run on it, and the last touch of its lease. The line names that signal (`since job run`). The cluster's own last-activity time does not move for a job run, so it is never the only signal. A cluster that is not running shows `not running`. The job runs are read from the workspace's runs of the last 6 hours (at most 500 are looked at).
- `down [--delete]`: terminate the pool's clusters (`--delete` removes them). Do this when the work settles.

## How a lease picks a cluster

- It takes a free running cluster first.
- Else it starts a free terminated pool cluster.
- Else, with fewer clusters active or starting than the cap, it makes one from the recorded shape.
- Else it waits (`--pool-wait-min`), printing a progress line each poll.
- A pool with no recorded cap never makes a cluster on a lease: it waits.

## Use a pool

- `gbx:bench:cluster ... --pool <name>` (also `--pool-wait-min <n>`, default 30) leases a cluster. A terminated pool cluster is started first. The jars the run needs (`--with-jar`, the bench tests jar for the heavy tier) are made the cluster's jars. A changed jar is reinstalled and the cluster restarted. The smoke and the main run use the same cluster.
- `gbx:test:notebooks-serverless ... --pool <name>` runs the notebooks on a leased cluster (`--jar` is made the cluster's jar, none otherwise).

## Rules in code

- Only clusters tagged `gbx_bench_pool=<name>` are touched. Cluster `1016-152458-i5qbli6q` is on a deny-list.
- The cap and shape are the file `bench-pool/<name>/pool.json` beside the leases. `up` writes it; a lease reads it.
- The lease is a file `bench-pool/<name>/<cluster_id>.lease` under the Volume root (`GBX_BUNDLE_VOLUME_*`), created with the Files API (exclusive). Its holder refreshes it every minute and writes the job run id on it. It is reclaimed when its holder has been silent for 5 minutes and its job run is over. The refresh starts the moment the lease is taken, so it covers the start of a terminated cluster, the jar reinstall and the restart.
- The jar identity is the (path, size, mtime) of each jar, kept in the cluster's `gbx_jar` tag. A mismatch uninstalls the old jars, sets the tag (which restarts a running cluster) and installs the run's jars.

The code is `examples/tests/_cluster_pool.py` (one module; `leased_cluster(...)` is the entry point for any launcher), and its tests `scripts/ci/test_bench_pool.py`.
