# CI: Hardened runner and cache

## Hardened runner groups (mixed strategy)

All jobs run on a Databricks-hardened runner group registered for the
**databrickslabs** org (Labs lockdown policy). We use a **mixed strategy** —
heavy build/test jobs on the larger machine, light orchestration jobs on the
standard machine. Both groups are hardened (org-level allowlist, ephemeral VMs,
constrained secret access).

### Heavy jobs — `larger-runners` / `larger`

Jobs that run mvn install, GDAL setup, or pytest:

```yaml
runs-on:
  group: larger-runners
  labels: larger
```

Currently used by: `build_main` build, `build_python` build, `build_scala`
build, `build_scala_by_package` test-package, `codecov-scala-parallel`
coverage-package, `codeql-analysis` analyze.

### Light jobs — `databrickslabs-protected-runner-group` / `linux-ubuntu-latest`

Jobs that just download artifacts, run a Python script, do `mvn` dep resolution,
or run npm/docusaurus:

```yaml
runs-on:
  group: databrickslabs-protected-runner-group
  labels: linux-ubuntu-latest
```

Currently used by: `build_main` update-doc-inventory,
`codecov-scala-parallel` merge-and-upload, `verify-maven-pgp` verify,
`deploy-docs` build + deploy.

### Common properties

Both groups select org-managed, ephemeral GitHub-hosted runners. A new VM is
created per job; nothing persists on the runner between runs, so there is
**no accumulation of storage or memory on the runner over time**. The
hardened groups additionally constrain which secrets/environments can target
them and which workflows can request them — set at the org level, not in this
repo.

The `larger` label maps to GitHub's larger-runner class (≥4 vCPU, ≥16 GB RAM,
~150 GB SSD); the standard `linux-ubuntu-latest` is smaller — sufficient for
artifact handling, dep resolution, and lightweight scripts. Exact specs are
maintained in the Databricks runner-hardening spreadsheet.

Reference: [GitHub Docs – Larger runners](https://docs.github.com/en/actions/reference/runners/larger-runners) and the Databricks Labs Repository Lockdown policy (runner-group section).

---

## What could “bog down” or slow builds

### 1. GitHub Actions cache (10 GB repo limit)

- **Maven:** `setup-java` with `cache: 'maven'` (keyed by `pom.xml`).  
- **Pip:** `setup-python` with `cache: 'pip'` (keyed by `.ci-pip-cache-key`, which varies with matrix).  
- **Apt:** `actions/cache` for `.cache/apt-archives` (keyed by workflow + hash of action files).

Total cache size for the repo is **capped** (default 10 GB; orgs can change this). When the limit is reached, **least-recently-used entries are evicted**. So over time you don’t get “infinite growth,” but you can get more cache misses if you have many matrix combinations (e.g. many python/numpy/gdal/spark keys), which can **slow** builds rather than “bog down” the runner.

**Practical:** Keep matrix small where possible; share one apt cache key across workflows (as you do today).

### 2. Memory in a single job

- `MAVEN_OPTS=-Xmx4g` gives the main Maven JVM 4 GB heap.  
- `mvn -T 1C` runs up to 4 parallel builders (1 per vCPU), each of which can start test JVMs.

On a 16 GB runner, 4 GB for Maven + several test JVMs can approach 16 GB. If you see **OOM or instability** in the Scala/scoverage job, consider:

- Slightly lowering heap, e.g. `-Xmx3g`, or  
- Reducing parallelism, e.g. `-T 2` instead of `-T 1C`.

### 3. Disk in a single job

- Maven `target/`, local repo (from cache), pip env, and apt packages all live on the workspace.  
- 150 GB free is enough for current usage; no cleanup is required within the job for “accumulation” (the VM is discarded at the end).

### 4. Artifacts

- Uploaded artifacts are stored by GitHub (with retention), not on the runner.  
- They do not consume runner disk or memory after the job.

---

## Summary

- **Runner:** 4 vCPU, 16 GB RAM, ~150 GB SSD; **ephemeral** — no storage/memory accumulation over time.  
- **Bog-down risk:** Not from disk/memory growth on the runner. Possible slowdowns from **cache eviction** (stay under repo cache limit) or **OOM** (tune `-Xmx` or `-T` if needed).
