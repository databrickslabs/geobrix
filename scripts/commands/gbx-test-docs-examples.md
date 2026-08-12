# gbx:test:docs-examples

Structural guards for the RasterX documentation example tabs and output tables.

Runs two host-only checks (no Docker, no Spark — pure `ast`/regex parsing):

1. **Output tables** — every `*_example_output` constant in the doc-example modules
   (`docs/tests/python/api/*_sql.py`, `rasterx_*python_light.py`, `rasterx_functions.py`)
   has width- and tick-consistent ASCII result tables. Catches orphan separator rows and
   mismatched column widths (the class of bug behind the `rst_bng_tessellate` /
   `rst_quadbin_tessellate` output tables).
2. **Tab completeness** — every `<FunctionExamples>` tab declared in a function's
   `function-info.json` `bindings` array has BOTH an example function and an output
   constant in the referenced source module, and no single-tab `<CodeFromTest>` is used on
   a function whose bindings declare more than one tier. Catches missing `_output`
   constants (the `rst_rastertoworldcoordx` gap) and SQL-only-tab regressions.
3. **Annotation consistency** — tabs of one function whose output table is byte-identical
   must carry the same trailing prose annotation (e.g. `(aspect in compass degrees: 0=N...)`).
   A tier-specific `; light tier returns ...` clause is allowed to differ; the descriptive
   core must match. Tabs whose tables legitimately differ (the `rst_*_rastertogrid*` family,
   the SQL dual heavy/BINARY block) are never compared, so only true drift is flagged.

Exits non-zero if any gap is found. Runs on the host — no Docker needed.

## Usage

```bash
bash scripts/commands/gbx-test-docs-examples.sh [OPTIONS]
```

## Options

- `--log <path>` — write output to a log file (`filename` → `test-logs/filename`; relative → under `test-logs/`; absolute → as-is)
- `--help`, `-h` — show help and exit

## Examples

```bash
bash scripts/commands/gbx-test-docs-examples.sh
bash scripts/commands/gbx-test-docs-examples.sh --log docs-examples.log
```
