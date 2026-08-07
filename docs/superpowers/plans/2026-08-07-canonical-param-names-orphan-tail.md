# Canonical Parameter Names — Orphan Tail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drive the param-name waiver from 30 → 8 by syncing the orphan-tail functions' parameter names to their canonical form (Scala field is canonical; plural for array inputs), fixing one guard parser false positive, and leaving 8 documented light-arity gaps permanently waived.

**Architecture:** Naming-only. Most renames touch only the Python surfaces (heavy shim + light binding); a handful also rename a Scala case-class field (D1/D2/D4). No builder-arity changes, no public function-name changes, no SQL-caller impact (SQL binds positionally). Each rename updates the four name-bearing surfaces in lockstep (Scala field where applicable, heavy shim, light binding, frozen fixture), then regenerates `function-info.json` and removes the function from the waiver.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 (heavy expressions), Python 3.12 (heavy shim `databricks.labs.gbx.*` + light `pyrx`/`pyvx`/`pygx`), the `gbx:*` command palette inside the `geobrix-dev` Docker container.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-08-07-canonical-param-names-orphan-tail-design.md` (ratified). This plan implements Classes 1, 1b, 2, 3; Class 4 stays waived.
- **No public function-name changes.** Only parameters. `geom` stays `geom` in function names.
- **No builder-arity changes.** SQL binds positionally; a wrapper arg count must still match `builder()` range. D3 ADDS a trailing optional to a heavy shim — the Scala builder already accepts it, so no builder change.
- **Governing principle:** Scala case-class field name (Expr-stripped, snake_cased) is canonical, EXCEPT array-input args take the plural (`tiles`). Fix the fixture only where it diverged from Scala.
- **`cellid` / `out_srid` / `*_array` / `*_chip` exceptions are already ratified** — do not touch them.
- **The frozen fixture** `docs/tests-function-info/canonical_param_names.txt` is the guard's source of truth. Update it in lockstep with each rename; it is NOT auto-generated.
- **The guard** `docs/scripts/check-param-names.py`: Invariant A (heavy params EXACTLY equal fixture; light params have fixture as an in-order PREFIX), Invariant B (light arity ≥ heavy arity). Waived functions are exempt in normal mode; `--report` ignores the waiver.
- **`function-info.json` is a GENERATED build artifact** — never hand-edit. Regenerate via `gbx:docs:function-info` after Python/Scala renames. Its `usageArgs` derive from the Scala field name.
- **All Docker/Maven/pytest/regen work runs via `gbx:*` inside the running `geobrix-dev` container.** Never ad-hoc `docker`/`mvn`/`pytest`. If a `gbx:*` command is broken, FIX it, don't route around it.
- **Commits:** explicit `git add` paths only — NEVER `git add -A` (sweeps untracked strays: `.isaac/`, `.tmp`, `scratchpad/`, zips). Subject ≤72 chars + WHY body. `Co-authored-by: Isaac` trailer.
- **No Databricks profile needed** — all local. Never run `databricks auth login`.
- **Verify before done:** `python3 docs/scripts/check-param-names.py` (normal mode, exit 0) after each task; affected pytest suites per task; a Scala compile after any Scala field rename.

## Name-bearing surfaces (per rename — the lockstep discipline)

For each renamed param, update every surface that carries the name, in one commit:
1. **Scala case-class field** (D1/D2/D4 only) — internal rename incl. all body usages; no arity change.
2. **Heavy-Python shim** `python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py` — signature + `Args:` docstring + `_col(...)` body ref.
3. **Light-Python binding** `.../pyrx|pyvx|pygx/functions.py` — signature + docstring + `_col(...)` body ref.
4. **Frozen fixture** `docs/tests-function-info/canonical_param_names.txt` (the fixture line for that function).
Then: regenerate `function-info.json`, remove the function from `param_name_waiver.txt`, and check for keyword-arg callers in `python/geobrix/test/` and `docs/tests/` (rename them too — SQL is positional but Python kwargs are not).

---

### Task 1: Guard parser fix (Class 1b) — `rst_evi` false positive

**Files:**
- Modify: `docs/scripts/check-param-names.py` (`_find_def`, ~line 61-64)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (remove `gbx_rst_evi`)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: a `_find_def` that strips inline `#` comments, so a `def foo(  # noqa: E741` line parses correctly. Later tasks rely on the guard being accurate.

**Context:** `rst_evi` is NOT a naming divergence. Light `rst_evi` is already canonical (`tile, red_idx, nir_idx, blue_idx, l, c1, c2, g, ...`). The guard's `_find_def` captures the parenthesized arg list with `re.search(rf"def\s+{...}\s*\((.*?)\)\s*(->|:)", text, re.S)`. For `pyrx/functions.py` line 2873 `def rst_evi(  # noqa: E741`, the capture begins with `# noqa: E741\n    tile: ColLike,\n ...`; the first comma-split token becomes `# noqa` and `tile` is dropped, yielding a false `[A]` prefix mismatch. It is the ONLY def in any guard-read surface with an inline comment (verified).

- [ ] **Step 1: Write the failing test**

Add to `docs/tests-function-info/test_param_names.py` (or wherever the guard's pytest lives — confirm path; it is wired via `gbx:test:bindings`). If no unit-test seam exists for `_find_def`, add a focused one:

```python
def test_find_def_strips_inline_comment():
    from importlib import import_module
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path("docs/scripts")))
    mod = import_module("check-param-names".replace("-", "_")) if False else None
    # check-param-names.py is not import-friendly (hyphens); test via extract_py_params on a temp file:
    import importlib.util
    spec = importlib.util.spec_from_file_location("cpn", "docs/scripts/check-param-names.py")
    cpn = importlib.util.module_from_spec(spec); spec.loader.exec_module(cpn)
    import tempfile, pathlib as pl
    src = 'def rst_evi(  # noqa: E741\n    tile,\n    red_idx,\n) -> int:\n    return 0\n'
    with tempfile.TemporaryDirectory() as d:
        p = pl.Path(d) / "functions.py"; p.write_text(src)
        params = cpn.extract_py_params(p, "gbx_rst_evi")
    assert params == ["tile", "red_idx"], f"inline-comment def misparsed: {params}"
```

- [ ] **Step 2: Run it to verify it fails**

Run inside container: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 -m pytest docs/tests-function-info/test_param_names.py::test_find_def_strips_inline_comment -q" --log evi-guard.log`
Expected: FAIL — `params` is `["# noqa", "red_idx"]` or `["red_idx"]` (tile dropped).

- [ ] **Step 3: Fix `_find_def` to strip inline comments**

In `docs/scripts/check-param-names.py`, change `_find_def` so the captured arg list has `#`-to-end-of-line comments removed before it is returned. Minimal:

```python
def _find_def(text: str, pyname: str) -> str | None:
    # Match `def <pyname>(` and capture the full parenthesized arg list across newlines.
    m = re.search(rf"def\s+{re.escape(pyname)}\s*\((.*?)\)\s*(->|:)", text, re.S)
    if m is None:
        return None
    # Strip inline `# ...` comments (e.g. `def f(  # noqa: E741`) that would otherwise
    # be tokenized as a bogus first parameter and drop the real first arg.
    return re.sub(r"#[^\n]*", "", m.group(1))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: same pytest node as Step 2. Expected: PASS (`["tile", "red_idx"]`).

- [ ] **Step 5: Remove `gbx_rst_evi` from the waiver and verify the guard**

Delete the `gbx_rst_evi` line from `docs/tests-function-info/param_name_waiver.txt`. Then:
Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 docs/scripts/check-param-names.py" --log evi-guard2.log`
Expected: `check-param-names: OK` (exit 0) — `rst_evi` no longer waived and no longer flagged.

- [ ] **Step 6: Commit**

```bash
git add docs/scripts/check-param-names.py docs/tests-function-info/test_param_names.py docs/tests-function-info/param_name_waiver.txt
git commit -m "fix(params): strip inline comments in guard _find_def (rst_evi false positive)

The param-name guard's _find_def captured the arg list verbatim, so a
def with an inline comment (rst_evi's \`# noqa: E741\`) tokenized the
comment as a bogus first parameter and dropped the real \`tile\` arg,
producing a phantom prefix mismatch. Strip \`#\` comments from the
captured arglist. rst_evi was never a naming divergence; unwaive it.

Co-authored-by: Isaac"
```

---

### Task 2: Class 1 — Python-only coord/transform/proximity/getsubdataset renames

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` (heavy shim)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (light binding)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (remove the 6 functions below)
- Docs: `docs/docs/api/rasterx-functions.mdx` (check/update `**Signature:**` lines for these functions)

**Interfaces:**
- Consumes: accurate guard from Task 1.
- Produces: canonical param names on these 6 functions. No Scala change (fields already canonical).

**Context:** These are pure Python drift; the Scala field and the fixture are already canonical. Rename map (heavy AND light where present):

| Function | old → new |
|---|---|
| `rst_rastertoworldcoord` / `x` / `y` | `pixel_x, pixel_y` → `x, y` |
| `rst_worldtorastercoord` / `x` / `y` | `world_x, world_y` → `x, y` |
| `rst_transform` | `target_srid` → `srid` |
| `rst_proximity` | `distunits` → `dist_units` |
| `rst_getsubdataset` | light `name` → `subset_name` (heavy already `subset_name`) |

Heavy shim current locations (verbatim): `rst_rastertoworldcoord`@1459, `rst_rastertoworldcoordx`@1475, `rst_rastertoworldcoordy`@1493, `rst_transform`@1511, `rst_worldtorastercoord`@1549, `rst_worldtorastercoordx`@1565, `rst_worldtorastercoordy`@1583, `rst_proximity`@2684, `rst_getsubdataset`@140.
Light locations: `rst_rastertoworldcoordx`@4210, `rst_rastertoworldcoordy`@4216, `rst_worldtorastercoordx`@4222, `rst_worldtorastercoordy`@4228, `rst_transform`@931, `rst_proximity`@1993, `rst_getsubdataset`@4332.

Each heavy signature has a matching `Args:` docstring line and a `_col(<param>)` body ref that must ALL be renamed (e.g. `rst_transform` body: `f.call_function("gbx_rst_transform", _col(tile), _col(target_srid))` → `_col(srid)`).

- [ ] **Step 1: Confirm fixture already canonical for these 6**

Run: `grep -E "gbx_rst_(rastertoworldcoord|worldtorastercoord|transform|proximity|getsubdataset)" docs/tests-function-info/canonical_param_names.txt`
Expected: fixture already shows `tile, x, y` / `tile, srid` / `tile, target_values, dist_units, max_distance` / `tile, subset_name`. (No fixture edit needed — confirm.)

- [ ] **Step 2: Find keyword-arg callers**

Run: `grep -rnE "pixel_x=|pixel_y=|world_x=|world_y=|target_srid=|distunits=|\bname=" python/geobrix/test/ docs/tests/ | grep -iE "rastertoworld|worldtoraster|transform|proximity|getsubdataset"`
Rename any hits found (there may be none — `target_srid=` at test_crs line 235 is a docstring, not a call).

- [ ] **Step 3: Apply the renames** (heavy shim + light binding, signature + docstring + `_col()` body) per the table above.

- [ ] **Step 4: Regenerate function-info.json**

Run: `bash scripts/commands/gbx-docs-function-info.sh --log ott-fi-2.log` (or `gbx:docs:function-info`). This runs inside the container; `usageArgs` derive from the Scala field (unchanged), so JSON for these should be stable — this step confirms no regression.

- [ ] **Step 5: Remove the 6 from the waiver; run the guard + affected pytest**

Delete `gbx_rst_rastertoworldcoord`, `...coordx`, `...coordy`, `gbx_rst_worldtorastercoord`, `...coordx`, `...coordy`, `gbx_rst_transform`, `gbx_rst_proximity`, `gbx_rst_getsubdataset` lines from the waiver.
Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 docs/scripts/check-param-names.py" --log ott-guard-2.log` → expect `OK`.
Run affected light pytest: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log ott-pyrx-2.log` (narrow further if the dir is slow; must stay green).

- [ ] **Step 6: Commit** (explicit paths: the 2 functions.py, the waiver, any test files, and function-info.json if it changed)

```bash
git commit -m "refactor(rasterx): canonicalize coord/transform/proximity param names

Python-surface drift only (Scala + fixture already canonical): pixel_x/pixel_y
and world_x/world_y -> x/y, target_srid -> srid, distunits -> dist_units,
light getsubdataset name -> subset_name. Unwaive the 6.

Co-authored-by: Isaac"
```

---

### Task 3: Class 1 — `bng_pointascell`, `bng_eastnorthasbng`, `custom_grid` (gridx Python)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/gridx/bng/functions.py` (heavy bng shim)
- Modify: `python/geobrix/src/databricks/labs/gbx/gridx/custom/functions.py` (heavy custom shim — likely already canonical, confirm)
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/functions.py` (light binding)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (remove the 3)
- Docs: `docs/docs/api/gridx-functions.mdx` (`**Signature:**` lines)

**Interfaces:**
- Consumes: accurate guard from Task 1.
- Produces: canonical gridx Python param names. Scala fields already canonical (`geom, resolution` for pointascell; `easting, northing, resolution` for eastnorthasbng; `boundXMin...` for custom_grid) — no Scala change.

**Context — rename map:**

| Function | Surface | old → new |
|---|---|---|
| `bng_pointascell` | heavy bng shim @222 | `point` → `geom` (light pygx @829 already `geom`) |
| `bng_eastnorthasbng` | heavy bng shim @138 | `east, north` → `easting, northing` |
| `bng_eastnorthasbng` | light pygx @834 | `e, n` → `easting, northing` |
| `custom_grid` | light pygx @982 | `x_min, x_max, y_min, y_max, cell_splits, root_x, root_y, srid` → `bound_x_min, bound_x_max, bound_y_min, bound_y_max, cell_splits, root_cell_size_x, root_cell_size_y, srid` |

Heavy `custom_grid` @49 is ALREADY canonical (`bound_x_min...root_cell_size_x`) — confirm, no change. `bng_pointascell` heavy body: `f.call_function("gbx_bng_pointascell", _col(point), _col(resolution))` → `_col(geom)`. Light `custom_grid` has 8 `_col(...)` body refs to rename (lines ~995-1004).

`bng_pointascell` `geom` acceptance criterion (from spec): the Scala `BNG_PointAsCell` dispatches `JTS.fromWKT`/`JTS.fromWKB` on `UTF8String`/`Array[Byte]` — verified — so `geom` is honest. (SRID-handling nuance: it uses centroid X/Y and requires EPSG:27700 — that is a `description` concern for follow-on spec #2, not this task.)

- [ ] **Step 1: Confirm fixture canonical for these 3**

Run: `grep -E "gbx_bng_pointascell|gbx_bng_eastnorthasbng|gbx_custom_grid" docs/tests-function-info/canonical_param_names.txt`
Expected: `geom, resolution` / `easting, northing, resolution` / `bound_x_min, bound_x_max, bound_y_min, bound_y_max, cell_splits, root_cell_size_x, root_cell_size_y, srid`. No fixture edit needed — confirm.

- [ ] **Step 2: Find keyword-arg callers**

Run: `grep -rnE "\bpoint=|\beast=|\bnorth=|\be=|\bn=|x_min=|x_max=|y_min=|y_max=|root_x=|root_y=" python/geobrix/test/pygx/ python/geobrix/test/ docs/tests/ | grep -iE "pointascell|eastnorth|custom_grid"`
NOTE: `test_custom_core.py` and `test_parity_custom.py` already call `custom_grid(bound_x_min=..., ...)` (the canonical heavy names) — those target the heavy shim and stay valid. If any test calls the LIGHT `pygx.custom_grid` with `x_min=`/`root_x=`, rename to the canonical names.

- [ ] **Step 3: Apply the renames** per the table (heavy bng shim, light pygx binding; confirm heavy custom unchanged).

- [ ] **Step 4: Regenerate function-info.json** (`gbx:docs:function-info`, `--log ott-fi-3.log`).

- [ ] **Step 5: Remove the 3 from waiver; guard + pytest**

Delete `gbx_bng_pointascell`, `gbx_bng_eastnorthasbng`, `gbx_custom_grid` from the waiver.
Run guard → expect `OK`. Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/ --log ott-pygx-3.log` → green.

- [ ] **Step 6: Commit** (explicit paths)

```bash
git commit -m "refactor(gridx): canonicalize bng/custom Python param names

Python drift only (Scala + fixture canonical): bng_pointascell point -> geom,
bng_eastnorthasbng east/north (heavy) and e/n (light) -> easting/northing,
light custom_grid x_min.../root_x -> bound_x_min.../root_cell_size_x. Unwaive 3.

Co-authored-by: Isaac"
```

---

### Task 4: Class 2 + Class 3 — Scala field renames (D1/D2/D4) + heavy shim renames + D3 added params

**Files:**
- Modify (Scala): `RST_NDVI.scala`, `RST_Clip.scala`, `RST_Merge.scala`, `RST_CombineAvg.scala`, `RST_MapAlgebra.scala` (all under `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/`)
- Modify (heavy shim): `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py`
- Modify (light binding): `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
- Modify (fixture): `docs/tests-function-info/canonical_param_names.txt`
- Modify (waiver): remove `gbx_rst_ndvi`, `gbx_rst_clip`, `gbx_rst_merge`, `gbx_rst_combineavg`, `gbx_rst_mapalgebra`, `gbx_rst_sample`, `gbx_rst_viewshed`, `gbx_rst_derivedband`, `gbx_rst_derivedband_agg`
- Test: `docs/tests/python/api/rasterx_functions_sql.py` (if any `*_sql_example()` names a renamed param)
- Docs: `docs/docs/api/rasterx-functions.mdx` (`**Signature:**` lines)

**Interfaces:**
- Consumes: accurate guard (Task 1); Task 2 left the rest of rasterx heavy shim canonical.
- Produces: fully canonical rasterx orphan tail. This is the only task touching Scala, so it needs a Scala compile.

**Context — this task groups every remaining rasterx orphan, split by sub-change. Apply all, then verify once.**

**D1 — `rst_ndvi` `_index` → `_idx`:**
- Scala `RST_NDVI.scala`: rename field `redIndex → redIdx`, `nirIndex → nirIdx` at lines 22, 23, 26, and the `eval`/`execute` method params + call sites at 38, 48, 70, 71 (all local `redIndex`/`nirIndex` → `redIdx`/`nirIdx`).
- Heavy shim `rst_ndvi`@1445: `red_band, nir_band` → `red_idx, nir_idx` (signature + docstring + body `_col(red_band)` → `_col(red_idx)`, `_col(nir_band)` → `_col(nir_idx)`).
- Light `rst_ndvi`@2779: rename `red_band/nir_band` → `red_idx/nir_idx` if present (confirm light current names first).
- Fixture line `gbx_rst_ndvi`: `tile, red_index, nir_index` → `tile, red_idx, nir_idx`.

**D2 — `rst_clip` cutline → `geom`:**
- Scala `RST_Clip.scala`: rename field `geometryExpr → geomExpr` at lines 24 and 30 (the `children` Seq). (The `eval` locals are already named `geom` — leave them.)
- Heavy shim `rst_clip`@1323: `clip` → `geom` (signature + docstring + body `_col(clip)` → `_col(geom)`).
- Light `rst_clip`@1276: rename `clip` → `geom` if present (confirm light current name).
- Fixture line `gbx_rst_clip`: `tile, geometry, cutline_all_touched, [clip_crs]` → `tile, geom, cutline_all_touched, [clip_crs]`.
- Acceptance: `RST_Clip` dispatches `JTS.fromWKT`/`fromWKB` on both types — verified, `geom` honest.

**D4 — `tile` → `tiles` on the 3 array-input scalar expressions:**
- Scala `RST_Merge.scala`: rename field `tile → tiles` at lines 17, 22, 26, 27 (case-class field + `arrayOfTileRasterType(...tile...)` + `arrayOfTileElementFieldCount(tile)` + `children` Seq). The doc-comment at line 20 may say "tile array" — update prose to "tiles array" for clarity but it is not load-bearing.
- Scala `RST_CombineAvg.scala`: same rename at lines 17, 22, 26, 27; class doc-comment line 15 mentions `tile`/`tiles` — align prose.
- Scala `RST_MapAlgebra.scala`: rename field `tile → tiles` at lines 22, 27, 31, 32 (keep `jsonSpecExpr` untouched).
- Heavy shim: `rst_merge`@1433 already `tiles` (no change); `rst_combineavg`@1339 already `tiles` (no change); `rst_mapalgebra`@1420 `tiles, expression` → keep `tiles`, rename `expression` → `json_spec` (signature + docstring + body `_col(expression)` → `_col(json_spec)`).
- Light: `rst_merge`@641, `rst_combineavg`@730, `rst_mapalgebra`@2496 — confirm current light names; light `merge`/`combineavg` likely already `tiles`; `mapalgebra` light `expression` → `json_spec` if present.
- Fixture: `gbx_rst_merge` `tile`→`tiles`; `gbx_rst_combineavg` `tile`→`tiles`; `gbx_rst_mapalgebra` `tile, json_spec`→`tiles, json_spec`.

**Class 1 (rasterx) leftover — `rst_derivedband` / `_agg`:**
- Heavy shim `rst_derivedband`@1364: `tile_expr, pyfunc` → `tile, python_func` (signature + docstring + body `_col(tile_expr)`→`_col(tile)`, `_col(pyfunc)`→`_col(python_func)`). `rst_derivedband_agg`@432: `pyfunc` → `python_func`.
- Light `rst_derivedband`@4408: rename `tile_expr, pyfunc` → `tile, python_func` if present.
- Scala fields already `pythonFuncExpr`/`funcNameExpr` (canonical) — no Scala change. Fixture already `tile, python_func, func_name` — confirm.

**Class 3 (D3) — add trailing optional to heavy shims (Scala builder already accepts):**
- Heavy `rst_clip`@1323: add `clip_crs: ColLike = None` as 4th param (also gets the D2 `geom` rename) + pass `_col(clip_crs)` to `f.call_function`. Confirm the Scala `RST_Clip` builder has a `case 4 =>`/`case 5 =>` branch injecting the clipCrs default — it does (field `clipCrsExpr` exists).
- Heavy `rst_sample`@2510: add `crs: ColLike = None` as 3rd param + `_col(crs)`. Scala `RST_Sample` has `crsExpr` + a 2-or-3-arg builder.
- Heavy `rst_viewshed`@2764: add `crs: ColLike = None` as trailing param + `_col(crs)`. Scala `RST_Viewshed` has `crsExpr`.
- Fixture already lists these (`...[clip_crs]`, `tile, geom, [crs]`, `...[crs]`) — confirm; unwaive `gbx_rst_sample`, `gbx_rst_viewshed`.

- [ ] **Step 1: Write/adjust the failing check — run the guard in report mode to snapshot the exact remaining divergences for this task's functions**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 docs/scripts/check-param-names.py --report" --log ott-report-4.log`
Confirm the report lists exactly the divergences enumerated above for ndvi/clip/merge/combineavg/mapalgebra/sample/viewshed/derivedband(_agg), and nothing unexpected.

- [ ] **Step 2: Confirm current LIGHT names** (the plan assumes some are already canonical)

Run: `grep -nA4 "^def rst_ndvi\|^def rst_clip\|^def rst_mapalgebra\|^def rst_merge\|^def rst_combineavg\|^def rst_derivedband" python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`
Record which light params still need renaming (ndvi red_band?, clip clip?, mapalgebra expression?, derivedband tile_expr/pyfunc?).

- [ ] **Step 3: Apply Scala field renames** (D1 RST_NDVI, D2 RST_Clip, D4 RST_Merge/RST_CombineAvg/RST_MapAlgebra) at the exact lines above. Preserve arity, `override` modifiers, and `jsonSpecExpr`.

- [ ] **Step 4: Apply heavy-shim + light-binding renames + D3 added params** per the context blocks.

- [ ] **Step 5: Update the fixture** for `gbx_rst_ndvi`, `gbx_rst_clip`, `gbx_rst_merge`, `gbx_rst_combineavg`, `gbx_rst_mapalgebra` (the D1/D2/D4 lines). Confirm sample/viewshed/derivedband fixture lines are already canonical.

- [ ] **Step 6: Find + rename keyword-arg callers**

Run: `grep -rnE "red_band=|nir_band=|\bclip=|\bexpression=|tile_expr=|pyfunc=" python/geobrix/test/ docs/tests/`
Rename any real call-site hits (skip docstring mentions).

- [ ] **Step 7: Compile Scala + regenerate function-info.json**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && mvn -q -DskipTests package" --log ott-compile-4.log` — must BUILD SUCCESS (the D1/D2/D4 field renames must compile).
Then: `gbx:docs:function-info` (`--log ott-fi-4.log`) — regenerates `usageArgs` from the renamed Scala fields; `rst_ndvi` should now show `red_idx, nir_idx`, `rst_clip` `geom`, `rst_merge`/`combineavg`/`mapalgebra` `tiles`.

- [ ] **Step 8: Remove the 9 from waiver; guard + Scala suite + light pytest**

Delete the 9 functions from `param_name_waiver.txt`.
Run guard → expect `OK`.
Run the affected Scala suites (rename touched NDVI/Clip/Merge/CombineAvg/MapAlgebra + the agg fix area): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.*' --log ott-scala-4.log` — narrow to the specific suites if the wildcard is too broad; must be green.
Run light pytest: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log ott-pyrx-4.log` — green.

- [ ] **Step 9: Commit** (explicit paths: 5 Scala files, 2 functions.py, fixture, waiver, function-info.json, any test files)

```bash
git commit -m "refactor(rasterx): canonicalize orphan-tail params (D1/D2/D3/D4)

D1 rst_ndvi red_index/nir_index -> red_idx/nir_idx (match evi/nbr/ndwi/savi).
D2 rst_clip cutline geometryExpr -> geomExpr, param -> geom (N1 convention).
D4 rst_merge/combineavg/mapalgebra Scala field tile -> tiles (array input);
   mapalgebra expression -> json_spec. D3 add trailing crs/clip_crs to the
   rst_clip/rst_sample/rst_viewshed heavy shims (Scala builder already
   accepts them). derivedband tile_expr/pyfunc -> tile/python_func. Unwaive 9.

Co-authored-by: Isaac"
```

---

### Task 5: Documented-gap close-out — finalize the 8 Class-4 waivers

**Files:**
- Modify: `docs/tests-function-info/param_name_waiver.txt` (add documented reasons for the 8 remaining)
- Modify: `prompts/refactoring/2026-08-07-param-naming-orphan-tail-followon.md` (mark orphan tail resolved; note the 8 permanent-waived arity gaps as the surviving follow-on)

**Interfaces:**
- Consumes: waiver now down to exactly the 8 Class-4 functions.
- Produces: final state — waiver = 8, each with a one-line documented reason; guard green.

**Context:** After Tasks 1-4 the waiver should contain exactly: `gbx_bng_kloopexplode`, `gbx_bng_kringexplode`, `gbx_bng_geomkloopexplode`, `gbx_bng_geomkringexplode`, `gbx_bng_tessellateexplode`, `gbx_rst_histogram`, `gbx_rst_xyzpyramid`, `gbx_bng_tessellate`. These are `[B]` light-arity gaps (feature parity, not naming) and stay permanently waived per the naming-only scope.

- [ ] **Step 1: Confirm the waiver is exactly the 8**

Run: `grep -vE '^\s*#|^\s*$' docs/tests-function-info/param_name_waiver.txt | sort`
Expected: exactly the 8 above. If any Class 1/2/3 function remains, a prior task missed it — fix before continuing.

- [ ] **Step 2: Annotate the waiver with per-function reasons**

Rewrite the waiver body so each of the 8 has an inline comment stating WHY it is permanently waived (LATERAL-only light stub, or the specific missing trailing light param), matching the spec's Class-4 table. Add a header line: `# Remaining 8 are PERMANENT light-arity [B] gaps (feature parity, not naming). See spec 2026-08-07-orphan-tail.`

- [ ] **Step 3: Verify final state**

Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 docs/scripts/check-param-names.py" --log ott-final-guard.log` → `OK`.
Run: `bash scripts/commands/gbx-docker-exec.sh "cd /root/geobrix && python3 docs/scripts/check-param-names.py --report" --log ott-final-report.log` → expect exactly the 8 `[B]` light-arity lines and NO `[A]` naming lines.
Run: `gbx:test:bindings` (`--log ott-bindings.log`) → binding parity green (180 functions).

- [ ] **Step 4: Update the follow-on scratch doc**

In `prompts/refactoring/2026-08-07-param-naming-orphan-tail-followon.md`, mark the naming buckets (1-4) RESOLVED with the commit range, and note the surviving follow-on is only the 8 light-arity `[B]` gaps (a light-tier feature-parity workstream, separate from naming).

- [ ] **Step 5: Commit**

```bash
git add docs/tests-function-info/param_name_waiver.txt prompts/refactoring/2026-08-07-param-naming-orphan-tail-followon.md
git commit -m "docs(params): finalize orphan-tail — waiver down to 8 documented arity gaps

Naming divergences resolved (Classes 1/1b/2/3). The remaining 8 waivers are
permanent light-tier arity [B] gaps (LATERAL-only bng explode stubs +
histogram/xyzpyramid/tessellate missing trailing light params) — feature
parity, not naming, deferred to a light-arity workstream.

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:** Task 1 = Class 1b (rst_evi). Tasks 2-3 = Class 1 (Python-only: 6 rasterx + 3 gridx). Task 4 = Class 2 (D1/D2/D4 Scala renames) + Class 1 rasterx leftover (derivedband) + Class 3 (D3 added params). Task 5 = Class 4 documentation. All 30 waived functions accounted for (22 leave the waiver, 8 stay documented). ✓

**Placeholder scan:** every rename gives exact old→new and file:line. Test code in Task 1 is concrete. Where a light current-name is uncertain, the plan has an explicit "confirm current light names" step (Task 4 Step 2) rather than a guess. ✓

**Type consistency:** `tiles` (plural) used consistently for D4 across Scala field, fixture, and (already-plural) Python. `red_idx`/`nir_idx` matches the ratified evi/nbr/ndwi/savi family. `geom` (not `geometry`) matches N1. `json_spec` matches the existing Scala `jsonSpecExpr`. No arity changes anywhere; D3 adds a param the builder already accepts. ✓

**Ordering:** Task 1 first (accurate guard is a precondition for trusting later `check-param-names` runs). Task 4 (the only Scala/compile task) isolated so a compile failure blocks only itself. Task 5 last (pure verification + docs).
