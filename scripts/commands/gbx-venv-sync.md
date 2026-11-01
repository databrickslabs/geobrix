# gbx:venv:sync

Create or refresh the isolated `uv` venv (`.venv-pyrx/`, no system site-packages) for the light-tier tests (`gbx:test:light`, `gbx:test:pyrx`, zarr included) and the light benchmarks. It installs what the light CI phase installs: the hash-pinned lock `python/geobrix/requirements-light-env6-ci.txt` (read-only; every test dependency at CI's exact version, so test results match CI), then `python/geobrix` itself (editable, `--no-deps`). An extras install is not used: it floats, an older venv keeps stale versions (h3, pyogrio, numpy) that change test results, and the `test` extra's `mapbox-vector-tile<2.2` pin cannot be installed beside the env6 lock's 2.2. The command then checks that the venv sees its own rasterio, zarr and pyspark (4.0.x, which the Delta test jars that `test/zarr` loads need). Re-running refreshes an existing venv in place.

Set `GBX_PYRX_VENV=<dir>` or pass `--venv <dir>` to put the venv somewhere other than `.venv-pyrx`, for example to try a new lock without disturbing a venv another test run is using. The test commands honour the variable.

**Usage:** `bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--connect] [--match-container] [--venv <dir>] [--log <path>]`

**Options:**
- `--python <ver>` — Python version for a **new** venv (default 3.12). An existing venv keeps the Python it was made with: asking for another version fails with a hint to add `--recreate`, instead of being ignored
- `--recreate` — build a fresh venv beside the old one and swap it in only after everything installed (see below)
- `--venv <dir>` — put the venv at `<dir>` instead of `.venv-pyrx` (same as `GBX_PYRX_VENV`)
- `--connect` — also install the Spark Connect client deps, for the local Connect tier (see below)
- `--match-container` — build the venv to match the running `geobrix-dev` container instead of the CI lock (see below)
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Local Spark Connect tier (`--connect`, opt-in).** The CI lock carries no gRPC client, so `SparkSession.builder.remote(...)` cannot connect from the synced venv. `--connect` installs `pyspark[connect]==<the venv's pyspark>` (pyspark's own list for that version: `grpcio`, `grpcio-status`, `googleapis-common-protos`) plus `zstandard`, with the venv as it stands as a constraint file, so no locked version moves (a conflict fails the sync instead). The CI lock file is not touched. The `spark_connect_local` fixture (`python/geobrix/test/conftest.py`) then starts `remote("local[2]")` once per session and skips with a one-line reason when the deps are absent. The local server lives in the test process, so the tests that use the fixture (marked `connect` by the test conftest) cannot share a pytest process with classic-session tests; `gbx:test:pyrx` and `gbx:test:light` run them in a process of their own (see `gbx:test:pyrx`). The packages are installed through the host's configured `uv`; the command never prints or reads an index URL.

**Matching the container (`--match-container`).** The host venv used for work that needs no Docker must be the same environment the tests run in, or a pass on the host proves nothing about the container. `--match-container` reads the running container (it needs `geobrix-dev` up) and builds the venv on **its exact Python patch version** (uv fetches it; 3.12.3, where the host default is a newer 3.12.x) with **its exact version of every package** the CI light lock names (the lock's own version for the few the container lacks). It installs by exact version and **without hashes**: the lock's hashes are for the CI platform (Linux), not this one. Because older pins can pull in packages the lock does not name at their newest release, a second pass pins every package both environments have to the container's version. It runs `gbx:venv:check` and fails if anything still drifts: with `--recreate` the check judges the staging venv before the swap, so a drifting venv never replaces the old one. Use `--recreate` to rebuild an existing venv on another Python.

**Local-version packages.** The container's rasterio is `1.5.2+gbx1`, built from the env wheel's vendored payload, and no index serves a local version. The requirements built for the host pin the public base (`rasterio==1.5.2`). The host therefore differs from the container in the payload GDAL stack (gdal 3.13.3, geos 3.15.0, proj 9.9.0 there; PyPI wheels here). `gbx:venv:check` reports that as an expected divergence with a WARN, so the sync still ends green. Tests that depend on GDAL 3.13 or PROJ 9.9 behaviour must run in the container.

**Safe `--recreate`.** Nothing is deleted before its replacement is known to work.
- The new venv is built beside the old one (`<venv>.staging.<pid>`, made with `uv venv --relocatable`; only the staging venv is) and gets every install and the isolation check.
- With `--match-container`, the drift check runs on the staging venv. Drift fails the command and keeps the old venv.
- Only on success is the old venv moved aside, the new one moved into place, and the old one removed.
- On any failure the staging venv is removed, the old venv is untouched, and the command exits 1 with a `Fix:` line.
- An interrupt (INT, TERM) during the swap moves the old venv back and removes the staging venv; the command exits 130.
- `--venv` is resolved against the directory you ran the command from, and trailing slashes are dropped. The default sync (the CI lock) is unchanged, and so are CI's results: only a host run is curated to the container.

`--connect` also records the packages it declares in `<venv>/.gbx-connect-extras`, so `gbx:venv:check` lists them as declared connect-tier extras (the container does not carry them yet) rather than as strays.

**Examples:**
- `bash scripts/commands/gbx-venv-sync.sh`
- `bash scripts/commands/gbx-venv-sync.sh --recreate --log venv-sync.log`
- `bash scripts/commands/gbx-venv-sync.sh --connect`
- `bash scripts/commands/gbx-venv-sync.sh --match-container --recreate`
- `bash scripts/commands/gbx-venv-sync.sh --recreate --venv /tmp/throwaway-venv` (try a sync without touching `.venv-pyrx`)
