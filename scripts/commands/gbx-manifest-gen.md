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
- `--check` — exit 1 when a generated surface differs from the manifest, names a machine path, or fails a replacement gate. `--only` skips the gates
- `--only <surface>` — one of `manifest`, `python`, `docs`, `function-info`, `availability`, `examples`, `counts`
- `--log <path>` — write output to a log file (`filename` → `test-logs/filename`; relative → under `test-logs/`; absolute → as-is)
- `--help`, `-h` — show help and exit

## Examples

```bash
bash scripts/commands/gbx-manifest-gen.sh --write
bash scripts/commands/gbx-manifest-gen.sh --check --log manifest-check.log
```

## The replacement gates

`--check` also runs the gates in `scripts/manifest/check_surfaces.py`, one finding per line:

- completeness: every surface of a row exists (oracle row, Python entry, docs section, function-info, UC handler, Scala companion and wrapper);
- session/UC overlap: the same parameters, order and defaults, and each UC type a form of the session type;
- parameter names: every surface spells them as the manifest does (the Python entry, the lightweight API, the Scala wrappers, the function-info usage);
- docs tabs: a tab only for a surface the function has, one Python tab, and each tab's example and output exist.
