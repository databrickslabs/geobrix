# Stage sample data to a Volume

Uploads one dataset directory of the minimal bundle (default: the Zarr ERA5-like cube, `global/era5-weather/`) to a Unity Catalog Volume. Files go to `<volume-root>/geobrix-examples/<path>` with the Files API (streaming PUT), and every remote size is checked against the local one. Finder files (`.DS_Store`) are not uploaded. Nothing runs on a cluster, in Docker or in Maven, so there is no container lock.

## What it deletes

Only stale files **inside the uploaded part of each `*.zarr` store** (a chunk left by an older layout of the store would read wrongly). That is the whole store when the store, or a directory above it, is uploaded; with `--path <store>.zarr/<array>` it is only that array's directory, and the store's `zarr.json` and the other arrays on the Volume are not touched. It never deletes anything else: not other files in the dataset directory, not other datasets, not the rest of the Volume. A path with no `*.zarr` store (for example `--path nyc`) is uploaded over what is there and deletes nothing, so the minimal clip can overwrite a same-named full-size file but never removes the others. Before it deletes anything it checks that the remote listing of each pruned directory contains every file just uploaded, and it stops without deleting if not.

`--path` must be a relative directory with no `..` and no empty or `.` segment (`..`, `.`, an empty value, `/` and absolute paths are refused, as is a `--volume-root` that is not `/Volumes/<catalog>/<schema>/<volume>`). The check runs first, before any file or workspace is touched, `--dry-run` included.

Generate the data first: `gbx:data:generate-minimal-bundle --zarr-only`.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe [OPTIONS]
```

## Options

- `--profile <name>` — Databricks config profile. Required (except with `--dry-run`): the workspace is never chosen for you, and the command reads no config file.
- `--path <rel>` — dataset directory under `geobrix-examples`, relative, no `..` (default: `global/era5-weather`)
- `--src-root <dir>` — local `geobrix-examples` root (default: `sample-data/Volumes/main/default/test-data/geobrix-examples`)
- `--volume-root <dir>` — Volume to stage into (default: `/Volumes/geospatial_docs/geobrix/sample-data`)
- `--skip-existing` — upload only the files that are not on the Volume yet or have another size there, so a rerun after an interrupted upload sends only the rest
- `--bench-corpus` — stage the benchmark IO corpus; see [The benchmark IO corpus](#the-benchmark-io-corpus)
- `--dry-run` — list the files that would be uploaded and the stores that would be pruned; no workspace call, no auth, and it needs neither `uv` nor the SDK (it runs on plain `python3`)
- `--log <path>` — write output to a log file
- `--help` — show help (side-effect free)

## The benchmark IO corpus

`gbx:bench:gen-data` writes the corpus to `sample-data/Volumes/main/default/bench-corpus/` (`rows/` holds 10000 tiles by default, about 156 GB). `gbx:bench:cluster` reads `<corpus>/rows`, and the corpus is `/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus` unless `GBX_BENCH_CORPUS` says otherwise. `--bench-corpus` copies the one to the other:

```bash
bash scripts/commands/gbx-data-stage-sample.sh --bench-corpus --profile oauth-fe --log stage-corpus.log
```

- Source `sample-data/Volumes/main/default/bench-corpus/rows`, target `<volume-root>/bench-corpus/rows`. `--path` may name another directory, but it must be `bench-corpus` or below it. `--src-root` and `--volume-root` work as above.
- Only the files that are missing on the Volume, or have another size there, are uploaded, so it is safe to rerun after an interruption. Each upload is checked against its local size.
- It prints `uploaded N of M` every 250 files.
- At the end it lists the target directory and checks that every local file is there at its size. It prints `Volume holds N files in <dir> (expected M from the local copy)` and exits 1 if any is missing.
- It deletes nothing, not even a file in `rows/` that the local corpus lacks (it reports how many). Nothing outside the target directory is touched, including `corpus.json` and the other corpora beside `rows/`.
- `--dry-run` prints the count and the first five names; it makes no workspace call.
- `--profile` is required. To stage fewer tiles, generate fewer: `gbx:bench:gen-data --row-rows 1000`.

## Examples

```bash
bash scripts/commands/gbx-data-stage-sample.sh --dry-run
bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe --log stage-sample.log
```

## Exit codes and auth

Exit codes: `0` done, `1` an upload or check failed (nothing was deleted), `2` bad arguments or nothing to stage, `3` the profile cannot authenticate (the command never logs in: sign in to the profile yourself with the Databricks CLI, then run again), `4` the workspace could not be reached.

## Interpreter

Needs `databricks-sdk` and Python 3.11 or newer, chosen by the shared `gbx_python_with_sdk` helper in `common.sh` (see `gbx:data:stage-wheel`).
