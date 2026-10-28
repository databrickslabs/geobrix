# Stage sample data to a Volume

Uploads one dataset directory of the minimal bundle (default: the Zarr ERA5-like cube, `global/era5-weather/`) to a Unity Catalog Volume. Files go to `<volume-root>/geobrix-examples/<path>` with the Files API (streaming PUT), and every remote size is checked against the local one. Finder files (`.DS_Store`) are not uploaded. Nothing runs on a cluster, in Docker or in Maven, so there is no container lock.

## What it deletes

Only stale files **inside each `*.zarr` store directory it uploads** (a chunk left by an older layout of the store would read wrongly). It never deletes anything else: not other files in the dataset directory, not other datasets, not the rest of the Volume. A path with no `*.zarr` store (for example `--path nyc`) is uploaded over what is there and deletes nothing, so the minimal clip can overwrite a same-named full-size file but never removes the others. Before it deletes anything it checks that the remote listing of each store contains every file just uploaded, and it stops without deleting if not.

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
- `--dry-run` — list the files that would be uploaded and the stores that would be pruned; no workspace call, no auth, and it needs neither `uv` nor the SDK (it runs on plain `python3`)
- `--log <path>` — write output to a log file
- `--help` — show help (side-effect free)

## Examples

```bash
bash scripts/commands/gbx-data-stage-sample.sh --dry-run
bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe --log stage-sample.log
```

## Exit codes and auth

Exit codes: `0` done, `1` an upload or check failed (nothing was deleted), `2` bad arguments or nothing to stage, `3` the profile cannot authenticate (the command never logs in: sign in to the profile yourself with the Databricks CLI, then run again), `4` the workspace could not be reached.

## Interpreter

Needs `databricks-sdk` and Python 3.11 or newer, chosen by the shared `gbx_python_with_sdk` helper in `common.sh` (see `gbx:data:stage-wheel`).
