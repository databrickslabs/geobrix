# Generate Function Info (DESCRIBE FUNCTION EXTENDED)

Regenerates `function-info.json` from the function manifest so `DESCRIBE FUNCTION EXTENDED <name>` stays in sync with the API docs. It runs `scripts/manifest/gen_surfaces.py --write --only function-info` in Docker, so the file names no host path.

## Usage

```bash
bash scripts/commands/gbx-docs-function-info.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## When to run

After changing a manifest row, an oracle example, or a hand-written SQL example in `docs/tests/python/api/*_functions_sql.py`.

Then **commit** the updated file:

- `src/main/resources/com/databricks/labs/gbx/function-info.json`

## Examples

```bash
# Regenerate from the manifest
gbx:docs:function-info

# With log
gbx:docs:function-info --log function-info.log
```

## Notes

- **Runs inside Docker**: Uses the `geobrix-dev` container so the generated file carries container paths, not the host's.
- Requires the container to be running (e.g. `./scripts/docker/start_docker_with_volumes.sh`).
