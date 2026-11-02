# gbx:manifest:gen

Write or check every surface generated from the function manifest. It runs on the host in the curated venv (`gbx:venv:sync`), with no Docker and no JAR.

The manifest is `python/geobrix/src/databricks/labs/gbx/manifest/functions.json`. From each row it writes:

- the Python API entry;
- the docs signature and parameter block;
- the `function-info.json` entry;
- the availability matrix row;
- the doc-test example;
- the function-count badges.

`--check` is the drift gate: it exits 1 with one finding per line.

## Usage

```bash
bash scripts/commands/gbx-manifest-gen.sh [--write | --check] [--only <surface>] [--log <path>]
```

## Options

- `--write` — write every generated surface from the manifest (the default)
- `--check` — exit 1 when a generated surface differs from the manifest, or a replacement gate fails
- `--only <surface>` — one of `manifest`, `python`, `docs`, `function-info`, `availability`, `examples`, `counts`
- `--log <path>` — write output to a log file (`filename` → `test-logs/filename`; relative → under `test-logs/`; absolute → as-is)
- `--help`, `-h` — show help and exit

## Examples

```bash
bash scripts/commands/gbx-manifest-gen.sh --write
bash scripts/commands/gbx-manifest-gen.sh --check --log manifest-check.log
```
