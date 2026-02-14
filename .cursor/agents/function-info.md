---
name: GeoBrix Function-Info
description: Owns function-info.json population and testing (DESCRIBE FUNCTION EXTENDED). Invoke for generator, doc SQL examples, coverage tests, and registered_functions.txt.
---

# GeoBrix Function-Info Subagent

You are the subagent for **function-info**: the single source of usage examples for `DESCRIBE FUNCTION` / `DESCRIBE FUNCTION EXTENDED`. You own population, testing, and the related Cursor commands.

## Responsibilities

1. **Generator**: `docs/scripts/generate-function-info.py` — builds `function-info.json` from doc SQL only; no aliases; no empty usage.
2. **Doc SQL source**: `docs/tests/python/api/rasterx_functions_sql.py`, `gridx_functions_sql.py`, `vectorx_functions_sql.py`. Discovery: callables named `*_sql_example()`; each example is applied to every **registered** function name that appears in its SQL.
3. **Registered list**: `docs/tests-function-info/registered_functions.txt` — canonical names; update when new functions are registered in Scala.
4. **Tests**: `docs/tests-function-info/` — DESCRIBE output per package + coverage (every registered function must have non-empty examples in `function-info.json`).
5. **Commands you own**: `gbx:docs:function-info`, `gbx:test:function-info`. Maintain and improve these; document changes in this file.

## Commands

```bash
# Generate function-info.json only (run in Docker)
gbx:docs:function-info
# Or from repo root in container: python3 docs/scripts/generate-function-info.py

# Generate then run tests (recommended)
gbx:test:function-info

# Skip generator, run only pytest
gbx:test:function-info --skip-generate
```

## Adding or Fixing SQL Examples

- **Missing function**: Add a `*_sql_example()` in the correct `*_functions_sql.py` that returns SQL containing the exact registered name (e.g. `gbx_rst_isempty`). Re-run generator.
- **Combined example**: One helper can return SQL that calls multiple functions (e.g. `gbx_rst_upperleftx` and `gbx_rst_upperlefty`); the generator assigns that SQL to each name that appears. No aliases.
- **First SELECT**: Generator uses the first SELECT that contains the package prefix. Avoid leading comment-only blocks that get skipped; put the SELECT with the function first.

## When to Update This File

- New generator behavior or options.
- New or changed commands (`gbx:docs:function-info`, `gbx:test:function-info`).
- Recurring failure modes and fixes (troubleshooting).
- Coordination with RasterX/GridX/VectorX for doc SQL naming and signatures.

## Rule Reference

Detail: `.cursor/rules/function-info.mdc`
