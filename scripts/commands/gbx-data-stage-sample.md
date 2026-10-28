# Stage sample data to a Volume

Uploads one dataset directory of the minimal bundle (default: the Zarr ERA5-like cube, `global/era5-weather/`) to a Unity Catalog Volume. Files go to `<volume-root>/geobrix-examples/<path>` with the Files API (streaming PUT); every remote size is checked against the local one, and remote files under that directory that the local copy lacks are removed (a store left over from an older layout would read wrongly). Nothing runs on a cluster, in Docker or in Maven, so there is no container lock.

Generate the data first: `gbx:data:generate-minimal-bundle --zarr-only`.

---

## Usage

```bash
bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe [OPTIONS]
```

## Options

- `--profile <name>` — Databricks config profile. Required (except with `--dry-run`): the workspace is never chosen for you, and the command reads no config file.
- `--path <rel>` — dataset directory under `geobrix-examples` (default: `global/era5-weather`)
- `--src-root <dir>` — local `geobrix-examples` root (default: `sample-data/Volumes/main/default/test-data/geobrix-examples`)
- `--volume-root <dir>` — Volume to stage into (default: `/Volumes/geospatial_docs/geobrix/sample-data`)
- `--dry-run` — list the files that would be uploaded; no workspace call and no auth
- `--log <path>` — write output to a log file
- `--help` — show help (side-effect free)

## Examples

```bash
bash scripts/commands/gbx-data-stage-sample.sh --dry-run
bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe --log stage-sample.log
```

## Auth

An auth failure stops the run with exit 3 and does not try to log in: sign in to the profile yourself with the Databricks CLI, then run again.

## Interpreter

Needs `databricks-sdk` and Python 3.11 or newer, chosen by the shared `gbx_python_with_sdk` helper in `common.sh` (see `gbx:data:stage-wheel`).
