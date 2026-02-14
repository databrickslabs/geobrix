# Generate Function Info (DESCRIBE FUNCTION EXTENDED)

Regenerates `function-info.json` from doc SQL examples so `DESCRIBE FUNCTION EXTENDED <name>` stays in sync with the API docs (one-copy pattern).

## Usage

```bash
bash .cursor/commands/gbx-docs-function-info.sh [OPTIONS]
```

## Options

- `--log <path>` - Write output to log file (supports filename, relative, or absolute path)
- `--help` - Display help message

## When to run

After adding or changing SQL examples in:

- `docs/tests/python/api/rasterx_functions_sql.py`
- `docs/tests/python/api/gridx_functions_sql.py`

Then **commit** the updated file:

- `src/main/resources/com/databricks/labs/gbx/function-info.json`

## Examples

```bash
# Regenerate from current doc examples
gbx:docs:function-info

# With log
gbx:docs:function-info --log function-info.log
```

## Notes

- **Runs inside Docker**: Uses `geobrix-dev` container so the generator runs with the same layout and Python path as in CI (script adds `docs` to path automatically).
- Requires the container to be running (e.g. `./scripts/docker/start_docker_with_volumes.sh`).
