---
paths:
  - "src/main/scala/com/databricks/labs/gbx/**"
  - "python/geobrix/src/databricks/labs/gbx/**/functions.py"
  - "docs/docs/api/*.mdx"
  - "docs/tests-function-info/registered_functions.txt"
  - "src/main/resources/com/databricks/labs/gbx/function-info.json"
  - "docs/scripts/*.py"
  - "docs/tests/python/api/*_functions_sql.py"
---

# Adding or changing a GeoBrix function

Load-bearing rules for touching function expressions, bindings, signatures, and
generated metadata. Cross-cutting summary lives in `CLAUDE.md`; this is the detail.

## Cross-language naming consistency

Maintain consistent naming between Scala implementations and Python bindings. Typos across languages silently break bindings.

```
Scala Class:      Component_OperationName     (e.g. BNG_EastNorthAsBNG)
Scala API:        component_operationname     (e.g. bng_eastnorthasbng)
SQL (registered): gbx_<scala-api>             (e.g. gbx_bng_eastnorthasbng)
Python API:       same as Scala API           (e.g. bng_eastnorthasbng)
Test function:    test_<component>_<op>       (e.g. test_bng_eastnorthasbng)
```

- SQL keeps the `gbx_` prefix; the rest mirrors Scala.
- Use `_geom` not `_geometry` (e.g. `bng_geomkring`, not `bng_geometrykring`).
- Keep `_agg` suffix for aggregators (aligns with Databricks geospatial docs).
- Quick check: `grep -r "def bng_" python/geobrix/src/` should match `grep -r "gbx_bng_" src/main/scala/.../register`.
- **Binding parity is enforced.** `gbx:test:bindings` (→ `docs/scripts/check-binding-parity.py`) asserts every name in `registered_functions.txt` exists as a Scala `override def name` literal, a Python `functions.py` binding, and a `function-info.json` key — a function missing from any binding fails (it would surface at runtime as `UNRESOLVED_ROUTINE`). The QC judge runs this on every push via the `binding-parity` command check in `.claude/qc-judge/config.json`. When adding a function, add all three bindings, not just the canonical list.

## A signature change moves seven surfaces together (they fail SILENTLY)

Changing arity or a parameter's meaning touches up to seven places, and the ones that fail
*silently* are the dangerous ones — SQL binds **positionally**, so a wrapper passing an arg the
`builder()` doesn't accept is discarded with no error (this is exactly how `rst_maketiles`
advertised `(tile, tileWidth, tileHeight)` while really taking `(tile, sizeInMB)` — callers set a
megabyte budget believing they set pixel dimensions):

1. the expression case-class fields + `builder()` arity
2. the public Scala wrapper overloads in `<pkg>/functions.scala` — **arg count must match `builder()`**
3. the heavy Python shim (`python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py`)
4. the light Python binding (`.../pyrx|pyvx|pygx/functions.py`) + its registered UDF arity
5. the `**Signature:**` line in `docs/docs/api/*-functions.mdx`
6. the doc-test `*_sql_example()` (the generated example) — and its expected-output constant
7. signature metadata (`usageArgs`/`description`), then regenerate

Cross-check before declaring done: wrapper arg count vs `builder()` accepted range, and
whether each wrapper param name still denotes the quantity of the field it lands on
positionally. `check-binding-parity.py` compares **names only** and cannot see parameter
lists, so none of this is caught by CI today.

Cross-tier arity: when light takes more args than heavy, heavy must be an identical PREFIX with
the extras trailing AFTER (never interleaved).

## Function-info / DESCRIBE FUNCTION

Single-source pattern: doc SQL examples in `docs/tests/python/api/{rasterx,gridx,vectorx}_functions_sql.py` (functions named `*_sql_example()`) feed `docs/scripts/generate-function-info.py`, which writes `src/main/resources/com/databricks/labs/gbx/function-info.json`. The canonical registered-function list is `docs/tests-function-info/registered_functions.txt`.

- **No aliases.** Beta = we break API to stabilize. Fix upstream (Scala registration + `registered_functions.txt`) to a single canonical name.
- Run regeneration via `gbx:docs:function-info` or `gbx:test:function-info` (which also runs pytest). Host regen leaks `/Users/…` paths — regenerate in Docker.
- Tests assert every function in `registered_functions.txt` has a non-empty example in `function-info.json`. If coverage fails, fix upstream — never add placeholder/empty usage.

### Code examples are GENERATED — never hand-edit the JSON

`function-info.json` is a **build artifact**. Hand-editing it works until the next
`gbx:docs:function-info`, which silently overwrites your change. To fix what
`DESCRIBE FUNCTION EXTENDED` prints, edit the **source**, then regenerate:

| To change... | Edit this | Not this |
|---|---|---|
| the `Examples:` block | `docs/tests/python/api/*_functions_sql.py` → the function's `*_sql_example()` | ❌ `function-info.json` |
| `Usage:` / `Extended Usage:` | see "signature metadata" below | ❌ `function-info.json` |

How the example is extracted (`docs/scripts/generate-function-info.py`) — these
mechanics surprise people, so check them before wondering why your text vanished:

- Only the **first SQL statement** containing the package prefix is taken
  (`first_statement_containing`). A second query in the same `*_sql_example()` is
  ignored by `DESCRIBE FUNCTION` (it still renders in the docs page).
- `--` comments are **stripped**. Explanatory comments in the example never reach
  `DESCRIBE FUNCTION`; put that prose in the description metadata instead.
- One example can fill **several** functions: every registered name appearing in the
  statement inherits it, EXCEPT a name that has its own dedicated `*_sql_example()`
  (so `gbx_st_asmvt` and `gbx_st_asmvt_pyramid` don't cross-contaminate).
- Keys beginning `_` (e.g. `_package_rasterx`) are section markers, not functions.

### Canonical `usageArgs` style

`DESCRIBE FUNCTION` prints `name(<usageArgs>) - <description>`, describing the **SQL** surface.

- **Optional arguments use Style B: `[param]`** — brackets wrap only the parameter name, the
  comma stays outside. `geom, attrs_struct, min_z, max_z, layer_name, [extent]`. Multiple
  trailing optionals: `a, b, [c], [d]`. Do **not** use `geom, target_crs [, source_crs]`
  (comma inside) — that form is being retired.
- **Parameter names are snake_case**, matching SQL — `geom`, `resolution`, `size_in_mb`. Not
  the Scala camelCase (`geomWkb`, `cellId`) and not the internal `*Expr` field names.
  **Exception — `cellid`/`cellid1`/`cellid2`**: bare cell-id parameters use the single
  lowercase token `cellid` (not `cell_id`). This matches the chip-struct internal field,
  Databricks product naming, and Mosaic convention, and is intentional. Chip-struct
  parameters remain `left_chip`/`right_chip`/`input_chip` (snake_case, not affected by
  this exception).
- An argument is optional exactly when `builder()` has a shorter `case N =>` branch that
  injects a `Literal(...)` default. **34 functions** have optional args; rendering one as
  required is a bug, not a style nit.
- Don't parse the docs `**Signature:**` lines as truth — 63 of 173 use camelCase and at least
  one function has two conflicting lines. Validate against `builder()` arity instead.
- Param-naming domain distinctions are DELIBERATE: `out_srid`, `*_col`, and chip vs `cellid`
  are not typos — do not "normalize" them (e.g. chip→cellId is WRONG).

### Signature metadata derivation (automated from Scala)

As of v0.5.0, `usageArgs` and `description` are **derived from Scala case-class fields and builder
arity patterns**, not hand-maintained in `function-info.json`. This eliminates drift: parameter
names stay in sync with the actual Scala source, and optional parameter detection is validated
against real `builder()` branches.

**How it works:**

1. **`docs/scripts/extend-function-metadata.py`** (the parser):
   - Reads all Scala expression files under `src/main/scala/com/databricks/labs/gbx/{rasterx,vectorx,gridx}`.
   - For each function's case class, extracts field names and filters out internal state (e.g., `exprConfExpr`, aggregation buffer offsets).
   - Strips the `Expr` suffix from each field and converts to snake_case.
   - Inspects the `builder()` method: if `case N =>` and `case N+K =>` branches exist with `Literal(...)` defaults in the longer branch, marks args N+1…N+K as optional.
   - Outputs parsed metadata as JSON.

2. **`docs/scripts/generate-function-info.py`** (the generator):
   - Calls the parser to fetch `usage_args` for each function.
   - Merges parsed metadata into the JSON alongside examples (from `*_sql_example()` in docs).
   - Writes `src/main/resources/com/databricks/labs/gbx/function-info.json`.

3. **`WithExpressionInfo`** (the Scala consumer):
   - `getUsageArgs()` and `getDescription()` prefer JSON values (via `FunctionInfoLoader.get(name)`).
   - Fall back to Scala `usageArgs` / `description` overrides only if JSON is absent.
   - This allows legacy Scala overrides to coexist with generated metadata during migration.

**When adding or changing a function:**

- Update the **Scala case class** field names and `builder()` arity — the parser feeds from there.
- Run `gbx:docs:function-info` to regenerate the JSON (no manual edits needed).
- No Scala `usageArgs` override is normally required (it is derived). `description` still is — see below.
- If you must override (e.g., a builder arity is too irregular to parse), add `override def usageArgs` or `override def description` in the companion — the JSON loader respects it as a fallback, and the no-regression check will hold the derived value to it.

**Guardrails (these exist and are mutation-verified):**

- The parser **fails loudly** — it raises `SystemExit` rather than warning, and
  `generate-function-info.py` treats a parser failure as fatal instead of writing `{}`. A silent
  fallback is how an optional argument got published as required.
- **No-regression check** — a derived `usage_args` is compared against every hand-written
  `override def usageArgs`. Losing a bracket, or dropping a parameter the override listed, is a
  hard failure. Verified by mutating the bracket logic: the check caught all 5 override-backed
  functions and exited non-zero.
- **Multi-companion files are reported, not guessed.** When several companions share one SQL name
  (`ST_TransformCrs` + `ST_TransformCrs3` both register `gbx_st_transformcrs`), the parser
  describes the WIDEST case class so trailing optionals stay visible, and prints a note.
- Brace style must not matter: both `=> c.length match {` and `=> {` newline `c.length match {`
  are in use and parse identically.

Not yet wired: `gbx:test:function-info` does not assert usage coverage, and no lint checks bracket
syntax. `check-binding-parity.py` still compares **names only** — it cannot see a parameter list.

Currently **177 of 180** registered functions have derived `usage_args`. The 3 without
(`gbx_rst_fromfile`, `gbx_st_legacyaswkb`, `gbx_pmtiles_agg`) have irregular shapes and are left
absent so the Scala fallback applies. **`description` is still empty for all 180** — `DESCRIBE
FUNCTION` currently renders `name(args) - ` with a trailing dash. Populating descriptions is deferred.

`Usage:` is assembled in `WithExpressionInfo` as `name(usageArgs) - description`. Treat blank
metadata as a bug, not a default. See
`.superpowers/prompts/refactoring/2026-08-06-describe-function-metadata-drift-inventory.md`.

## Light tier vs heavy tier

- **DESCRIBE FUNCTION is heavy-only** — accepted asymmetry; one canonical heavy signature.
- **Light tier may exceed heavy**: if there's no heavy equivalent, document it and use common
  defaults; diverge only if costly. Light-Python tile functions carry force-output kwargs
  (`virtualize_dir`/`prefix`/`materialize`) absent from SQL + heavy.
- Justify a function by **user utility, not Mosaic parity** (Mosaic is the algorithm reference).
