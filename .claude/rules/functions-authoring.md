---
paths:
  - "src/main/scala/com/databricks/labs/gbx/**"
  - "python/geobrix/src/databricks/labs/gbx/**/functions.py"
  - "docs/docs/api/*.mdx"
  - "src/main/resources/com/databricks/labs/gbx/function-info.json"
  - "docs/scripts/*.py"
  - "docs/tests/python/api/*_functions_sql.py"
  - "python/geobrix/src/databricks/labs/gbx/manifest/**"
  - "scripts/manifest/**"
---

# Adding or changing a GeoBrix function

## The rule: one manifest row + one oracle row, then generate

A function is ONE row in `python/geobrix/src/databricks/labs/gbx/manifest/functions.json` (the source of
truth: SQL name, parameters with session and UC types, defaults, returns, levels, docs text, Python entry,
since) plus at least ONE row in its family's oracle table
(`python/geobrix/src/databricks/labs/gbx/bench/oracle_tables/<family>.py`). Then run
`bash scripts/commands/gbx-manifest-gen.sh --write`, which writes every other surface:

- the Python API entry in `databricks.labs.gbx.<module>.functions` (a thin call of the registered SQL name);
- the docs Signature/parameter block under the function's `### <name>` heading;
- the `function-info.json` entry (DESCRIBE metadata; L1 reads it; UC's COMMENT comes from the same row);
- the availability matrix row (`docs/docs/api/availability.mdx`);
- the doc-test example from the oracle row (`docs/tests/python/api/generated_examples.py`);
- the function-count badges.

`bash scripts/commands/gbx-manifest-gen.sh --check` is the gate (CI runs it in the light pytest job; QC runs
it before a push). Never edit a GENERATED region by hand. The Python manager is the only registrar: no Scala
code registers SQL, and no per-name Scala wrapper is added. Prose, hand-written examples and the page
structure stay hand-written.

SQL binds positionally, so an arity change is a manifest change, and a level whose implementation cannot
honour the signature is not selected (the manager says why). Per-surface names and signatures may differ:
Python-only keywords (the force-output trio) are declared in the row's `python` block.

## SQL kernels, stubs and shared deps (UC forms)

- A function that needs no Python gets a SQL body in the manifest: L4 `sql` (the text after `RETURN`) plus `sql_helpers`
  (private `gbx__<x>` SQL functions, called as `{q}gbx__<x>(...)` in a body). `gbx.uc.install` makes it a
  `LANGUAGE SQL DETERMINISTIC` function ahead of a Scala or Python handler. The row keeps its `handler` as the oracle twin.
- The session uses the same text as an L0 level (a temporary SQL function): put the body in `levels.L0.sql` and set
  L4 `sql` to `"L0"`. L0 is never a default: add the name to `manager/levels.py` `PREFERENCE` only after a measurement.
- A table function over a SQL kernel inlines the body (UC refuses a SQL function inside a generator). A body must not
  use a parameter name as a lambda variable or alias: inlining renames identifiers outside quotes.
- A session-only scalar gets a stub with an L4 spec holding only `stub` (the Fix sentence, without a `Fix:` prefix).
  The row has no UC level; `gbx.uc.install(..., stubs=True)` creates the raising function. Aggregates and generators get none.
- `_array`, `_partial` and `_finalize` names are scalars; a `_finalize` needs its `_partial`. The collision gate
  (`scripts/manifest/check_surfaces.py` `form_findings`) enforces this and the helper names.
- A UC form with another name or shape than its session row takes `twin()` rows (`bench/oracle_tables/_uc_tile.py`),
  not `share()`.
- A SQL body that may beat a Scala or Python handler but is not yet measured goes in `uc/candidates.py`, not on the row
  (a row with a `sql` key installs as a SQL function at once). The M2 probe builds its `sql` leg from it; move the body
  onto the row only after the numbers and the oracle rows agree, and delete the candidate.
- Run a SQL kernel's UC rows on the host with `oracles.run_sql_kernels(spark, cases)` (the kernel becomes a temporary
  function with the UC signature; `sources={name: (body, helpers)}` judges a candidate the same way). The host has no
  `h3_*` or `st_*`, so a body that calls one is judged on the workspace, not there. A statement a user writes (not one
  function call) is a recipe row: `_uc_tile.recipe(cid, fn, "SELECT ... AS out", kind, expect)`, `{q}` the schema qualifier.
- Spark refuses a call to a SQL function whose argument is a lambda variable, inside a lambda (`transform(.., i -> f(i))`).
  A kernel that builds arrays spells the helper's expression inline in the lambda (see the quadbin ring kernels), and
  calls a helper only on the function's own parameters. Spark also shifts a BIGINT by the count modulo 64: guard a shift
  of 64 or more, and cast a count to BIGINT before `shiftleft(.., 52)`. Write a refusal text's semicolon as `chr(59)`.
- Keep `;` out of a probe function's text (a candidate body, helper summary or message): the warehouse statement file
  splits on it.
- Known limits (documented, not fixed): `install` does not check that a form the caller forces is one the row can be
  (the DDL builder raises instead); a kernel rename of a parameter reaches a helper's body only through inlining, so a
  helper that spells the same name for another purpose is rewritten too; code that reads `functions.json` directly sees
  an L4 `stub` row as a UC function (use `manifest.load()`); and an unforced L0 that fails falls back to L3 only, not
  to L1 or L2. Parameter matching in a body is case-sensitive: spell it exactly as declared.

## Naming

- SQL keeps the `gbx_` prefix; the Python entry has the same name without it (`gbx_bng_eastnorthasbng` is
  `bng_eastnorthasbng`). One canonical name per function: there are no aliases.
- Use `_geom` not `_geometry` (`bng_geomkring`). Keep the `_agg` suffix for aggregators.
- Parameter names are snake_case, matching SQL (`geom`, `resolution`, `size_in_mb`). Bare cell-id parameters
  use the single token `cellid` (`cellid1`, `cellid2`), not `cell_id`; chip parameters stay `left_chip`,
  `right_chip`, `input_chip`. `out_srid`, `*_col` and chip versus `cellid` are deliberate distinctions:
  do not "normalize" them.
- An argument is optional exactly when the row gives it a default (`min_args` is the count of required ones);
  `DESCRIBE FUNCTION` shows `name(a, b, [c])` (brackets around the name only, the comma outside).
- Cross-tier arity: when the lightweight function takes more arguments than the heavyweight one, the heavyweight
  signature is an identical PREFIX and the extras trail AFTER (never interleaved).

## What the gate checks

`gbx-manifest-gen.sh --check` (`scripts/manifest/check_surfaces.py`) is the one gate for a function: each
surface a row names exists in its implementation, the session and UC forms agree, parameter names are the
row's on every surface, a row has `since` and an oracle row, and the docs tabs match the availability matrix.
A signature change is a manifest change: edit the row, then `--write`. To change what `DESCRIBE FUNCTION
EXTENDED` prints, edit the row (usage args, summary) or its oracle example, then run `gbx:docs:function-info`
(in Docker, so the file names no host path); never edit `function-info.json` by hand. The DESCRIBE contract row
(`gbx:test:function-info`) checks that every row with an L1 companion shows its usage args.

## Light tier vs heavy tier

- **DESCRIBE FUNCTION is heavy-only** — accepted asymmetry; one canonical heavy signature.
- **Light tier may exceed heavy**: if there's no heavy equivalent, document it and use common
  defaults; diverge only if costly. Light-Python tile functions carry force-output kwargs
  (`virtualize_dir`/`prefix`/`materialize`) absent from SQL + heavy.
- Justify a function by **user utility, not Mosaic parity** (Mosaic is the algorithm reference).
