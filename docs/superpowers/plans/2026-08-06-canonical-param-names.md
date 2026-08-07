# Canonical Parameter Names Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring every GeoBrix function's parameter names to a single ratified canonical form across all surfaces, protected by CI guards so the surfaces cannot re-diverge.

**Architecture:** A frozen, hand-authored **canonical-names fixture** is the single source of truth for the *target* parameter names. Two CI guards (Invariant A: cross-tier Python name equality; Invariant B: arity parity) compare live surfaces to the fixture and start from an explicit **waiver baseline** of currently-divergent functions. Renames are executed one rule at a time; each rename task removes its functions from the waiver, driving the guard green for them. When the waiver is empty, migration is complete.

**Tech Stack:** Scala 2.13 / Spark 4.0 (heavy expressions), Python 3.12 (heavy shim + light pyrx/pyvx/pygx bindings), SQL (positional-only). Guards are pure-stdlib Python run on the host via `gbx:test:bindings`.

## Global Constraints

- **This is decided design.** The ratified rulings live in `docs/superpowers/specs/2026-08-06-canonical-param-names-design.md`. Do not re-litigate names; if a name is not covered by the fixture, STOP and flag it — do not guess.
- **SQL binds positionally** — no `SupportsNamedArguments` anywhere in `src/main/scala/`. Renames never affect SQL callers; argument *order* is load-bearing, not names. Never reorder parameters.
- **A signature change touches up to 7 surfaces** (Scala case-class field, Scala `functions.scala` wrapper, heavy Python shim, light Python binding, docs `**Signature:**` line, doc-test `*_sql_example()`, generated metadata). Wrapper arg count MUST match `builder()` accepted range or SQL silently drops the extra arg.
- **`function-info.json` is a GENERATED build artifact.** Never hand-edit it. Regenerate with `gbx:docs:function-info` after Scala field renames.
- **Only edit `python/geobrix/src/...`** — never `python/geobrix/build/lib/...` (build artifact) and never `target/`, `scripts/docker/m2/`, `docs/build-static-zip/`.
- **`cellid` is a deliberate single lowercase token** on every surface (params AND struct fields), NOT `cell_id`, NOT `cellId`. It matches the v1/v2 tile struct field, the source struct fields (`BNG.scala:35`, `RST_ExpressionUtil.scala:103`), the Databricks product, and Mosaic.
- **Chip ≠ cellid.** `left_chip`/`right_chip`/`input_chip` name a STRUCT `{cellid, core, chip}`; never rename these to `cellid*`.
- **Use `gbx:*` commands, never ad-hoc docker/mvn/pytest.** Heavy Scala tests need a built+staged JAR; light Python tests do not.
- **Commit per rule.** Do not batch rules. Small reviewable diffs.
- **Do not commit unless the task says to.** Run the affected package tests before each commit.

---

## File Structure

**New files:**
- `docs/tests-function-info/canonical_param_names.txt` — the frozen canonical fixture (all 183 functions).
- `docs/tests-function-info/param_name_waiver.txt` — functions not yet migrated; shrinks to empty.
- `docs/scripts/check-param-names.py` — Invariants A + B; run via `gbx:test:bindings`.
- `docs/tests-function-info/test_param_names.py` — pytest wrapper asserting the check passes.

**Modified during renames (per rule):**
- `src/main/scala/com/databricks/labs/gbx/<pkg>/**/*.scala` — case-class fields, `elementSchema` StructField literals.
- `src/main/scala/com/databricks/labs/gbx/<pkg>/functions.scala` — public Column wrappers.
- `python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py` — heavy shim.
- `python/geobrix/src/databricks/labs/gbx/{pyrx,pyvx,pygx}/functions.py` — light bindings.
- `docs/docs/api/{raster,gridx,vectorx}-functions.mdx` — `**Signature:**` lines + param bullet lists.
- `docs/tests/python/api/*_functions_sql.py` — doc-test examples where they hardcode a renamed name.
- `src/main/resources/com/databricks/labs/gbx/function-info.json` — regenerated, not hand-edited.
- `CLAUDE.md` (R4 task only) — record the `cellid` exception.
- `docs/docs/beta-release-notes.mdx` (R4 task only) — the `cellid` result-schema break.

---

## Phase 0 — Fixture and guards (the safety net)

### Task 1: Author the canonical-names fixture

**Files:**
- Create: `docs/tests-function-info/canonical_param_names.txt`
- Reference: `src/main/resources/com/databricks/labs/gbx/function-info.json` (current derived `usage_args`), `docs/tests-function-info/registered_functions.txt`, the ratified table in the design spec.

**Interfaces:**
- Produces: a fixture file whose format is one function per line: `gbx_name<TAB>p1, p2, [p3]`. Optional args keep their `[ ]` brackets (Style B). The checker strips brackets and whitespace when comparing *names*, but Invariant B reads brackets to derive the optional-arg count.

- [ ] **Step 1: Bootstrap from current derived usage_args.** Write a throwaway snippet that reads `function-info.json` and emits, for every name in `registered_functions.txt`, a line `name<TAB><usage_args verbatim>`. For the 3 functions lacking derived `usage_args` (`gbx_rst_fromfile`, `gbx_st_legacyaswkb`, `gbx_pmtiles_agg`), read the Scala `override def usageArgs` (or the wrapper) and fill by hand. Save as the initial `canonical_param_names.txt`.

- [ ] **Step 2: Apply the ruled corrections** to the bootstrapped file (these are the target names, which differ from current derived for divergent functions). Edit the fixture lines so:
  - **R1:** any `tile_expr` → `tile` (should already be `tile` from derivation; verify none say `tile_expr`).
  - **R3:** `gbx_rst_slope` line uses `xscale, yscale` (not a single `scale`). Read current `rst_slope` builder arity in `src/main/scala/com/databricks/labs/gbx/rasterx/**/RST_Slope*.scala` to get the full ordered param list and optional brackets; write the target line with `xscale, yscale` in place of `scale`.
  - **R4a/b:** every `cell_id`→`cellid`, `c1`/`c2`/`cell_a`/`cell_b`→`cellid1`/`cellid2`.
  - **R4c:** the chip functions (`gbx_bng_cellintersection`, `gbx_bng_cellunion`, and `_agg` forms) use `left_chip, right_chip` / `input_chip` — NOT `cellid`.
  - **R5:** every `res`→`resolution`.
  - **R6:** the 7 output-producing functions (`rst_rasterize`, `rst_gridfrompoints[_agg]`, `rst_dtmfromgeoms[_agg]`, `rst_{h3,quadbin,bng}_rasterize_agg`) use `out_srid`/`out_crs` (not bare `srid`).
  - **N1:** every `geom_wkb`→`geom`.
  - **N9:** `gbx_st_triangulate`, `gbx_st_interpolateelevationgeom`, `gbx_st_interpolateelevationbbox` use `points_array`/`breaklines_array`.

- [ ] **Step 3: Add a header comment** to the fixture explaining it is the frozen TARGET (post-rename) canonical, the source of truth for `check-param-names.py`, and documenting the exceptions (`cellid` single token; `*_chip` is a struct; `out_*` is output-direction; `*_array` is ArrayType). No code runs here.

- [ ] **Step 4: Sanity-check coverage.** Run:
```bash
cd /Users/mjohns/IdeaProjects/geobrix
comm -23 <(grep -v '^#' docs/tests-function-info/registered_functions.txt | sort) \
         <(cut -f1 docs/tests-function-info/canonical_param_names.txt | sort)
```
Expected: no output (every registered function has a fixture line).

- [ ] **Step 5: Commit**
```bash
git add docs/tests-function-info/canonical_param_names.txt
git commit -m "docs(params): author frozen canonical param-name fixture"
```

---

### Task 2: Invariant A — cross-tier Python name equality (with waiver)

**Files:**
- Create: `docs/scripts/check-param-names.py`
- Create: `docs/tests-function-info/param_name_waiver.txt`
- Create: `docs/tests-function-info/test_param_names.py`
- Reference: `docs/scripts/check-binding-parity.py` (regex + path patterns to reuse), heavy shim + light bindings under `python/geobrix/src/`.

**Interfaces:**
- Produces:
  - `extract_py_params(func_py_path, gbx_name) -> list[str] | None` — parses `def <pyname>(...)` and returns ordered parameter names (excluding `self`, `*`, and return annotation). `pyname` is `gbx_name` minus the `gbx_` prefix.
  - `load_fixture() -> dict[str, list[str]]` — maps `gbx_name` → ordered canonical names (brackets stripped).
  - `load_waiver() -> set[str]` — `gbx_name`s exempt from the guard while migration is in progress.
  - `check_invariant_a() -> list[str]` — returns violation messages (empty = pass).
  - CLI: `python docs/scripts/check-param-names.py` exits 0 on pass, 1 on any non-waived violation.

- [ ] **Step 1: Write the failing test.**
```python
# docs/tests-function-info/test_param_names.py
import subprocess, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

def test_param_names_check_passes():
    r = subprocess.run(
        [sys.executable, str(REPO / "docs/scripts/check-param-names.py")],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, f"check-param-names failed:\n{r.stdout}\n{r.stderr}"
```

- [ ] **Step 2: Run it to verify it fails.**
Run: `cd /Users/mjohns/IdeaProjects/geobrix && python -m pytest docs/tests-function-info/test_param_names.py -v`
Expected: FAIL (script does not exist yet).

- [ ] **Step 3: Seed the waiver with every currently-divergent function.** Create `param_name_waiver.txt` (one `gbx_name` per line, `#` comments allowed). Populate it by temporarily running the check in "report" mode after Step 4, then pasting every reported function in. (Practically: implement Step 4 first, run `check-param-names.py --report`, capture the failing names, write them into the waiver so the guard passes at baseline.)

- [ ] **Step 4: Implement the check.**
```python
#!/usr/bin/env python3
"""Invariant A (cross-tier Python param-name equality) + Invariant B (arity parity).

Compares live heavy-Python and light-Python signatures against the frozen
canonical fixture (docs/tests-function-info/canonical_param_names.txt). Functions
listed in param_name_waiver.txt are exempt while the rename migration is underway.
Pure stdlib; runs on the host. Exit 0 on pass, 1 on any non-waived violation.
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "docs/tests-function-info/canonical_param_names.txt"
WAIVER = REPO / "docs/tests-function-info/param_name_waiver.txt"
PY_SRC = REPO / "python/geobrix/src"

# Heavy shim files (databricks.labs.gbx.<pkg>) and light bindings (pyrx/pyvx/pygx).
# Exclude build/lib (build artifact).
HEAVY_GLOBS = ["databricks/labs/gbx/rasterx/functions.py",
               "databricks/labs/gbx/vectorx/functions.py",
               "databricks/labs/gbx/gridx/bng/functions.py",
               "databricks/labs/gbx/gridx/grid/functions.py",
               "databricks/labs/gbx/gridx/h3/functions.py"]
LIGHT_GLOBS = ["databricks/labs/gbx/pyrx/functions.py",
               "databricks/labs/gbx/pyvx/functions.py",
               "databricks/labs/gbx/pygx/functions.py"]

def _brackets_stripped(tokens: list[str]) -> list[str]:
    return [t.strip().strip("[]").strip() for t in tokens if t.strip()]

def load_fixture() -> dict[str, list[str]]:
    out = {}
    for line in FIXTURE.read_text().splitlines():
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        name, _, args = line.partition("\t")
        out[name.strip()] = _brackets_stripped(args.split(","))
    return out

def load_waiver() -> set[str]:
    if not WAIVER.exists():
        return set()
    return {l.strip() for l in WAIVER.read_text().splitlines()
            if l.strip() and not l.startswith("#")}

def _find_def(text: str, pyname: str) -> str | None:
    # Match `def <pyname>(` and capture the full parenthesized arg list across newlines.
    m = re.search(rf"def\s+{re.escape(pyname)}\s*\((.*?)\)\s*(->|:)", text, re.S)
    return m.group(1) if m else None

def extract_py_params(path: Path, gbx_name: str) -> list[str] | None:
    pyname = gbx_name[len("gbx_"):]
    if not path.exists():
        return None
    arglist = _find_def(path.read_text(), pyname)
    if arglist is None:
        return None
    params = []
    for raw in arglist.split(","):
        tok = raw.strip()
        if not tok or tok.startswith("*"):
            continue
        pname = tok.split(":")[0].split("=")[0].strip()
        if pname and pname != "self":
            params.append(pname)
    return params

def _first_existing(globs: list[str], gbx_name: str) -> list[str] | None:
    for g in globs:
        params = extract_py_params(PY_SRC / g, gbx_name)
        if params is not None:
            return params
    return None

def check_invariant_a(report: bool = False) -> list[str]:
    fixture, waiver = load_fixture(), load_waiver()
    violations = []
    for gbx_name, canon in fixture.items():
        heavy = _first_existing(HEAVY_GLOBS, gbx_name)
        light = _first_existing(LIGHT_GLOBS, gbx_name)
        # Only compare surfaces that exist (not every fn has both tiers).
        for tier, params in (("heavy", heavy), ("light", light)):
            if params is None:
                continue
            if params != canon:
                msg = (f"[A] {gbx_name}: {tier} params {params} != canonical {canon}")
                if report or gbx_name not in waiver:
                    violations.append(msg)
    return violations

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true",
                    help="list ALL violations ignoring the waiver")
    args = ap.parse_args()
    violations = check_invariant_a(report=args.report)
    if violations:
        print("\n".join(sorted(violations)))
        print(f"\n{len(violations)} param-name violation(s).")
        return 1
    print("check-param-names: OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 5: Seed the waiver from `--report`.**
Run: `cd /Users/mjohns/IdeaProjects/geobrix && python docs/scripts/check-param-names.py --report`
Copy each reported `gbx_name` (unique) into `param_name_waiver.txt` with a header comment: `# Functions pending canonical param-name migration; shrinks to empty. See spec 2026-08-06-canonical-param-names-design.md`.

- [ ] **Step 6: Verify the guard passes at baseline.**
Run: `python docs/scripts/check-param-names.py`
Expected: `check-param-names: OK` (exit 0) — all divergences waived.
Run: `python -m pytest docs/tests-function-info/test_param_names.py -v`
Expected: PASS.

- [ ] **Step 7: Wire into `gbx:test:bindings`.** Read `scripts/commands/gbx-test-bindings.sh`; add a line invoking `python docs/scripts/check-param-names.py` alongside the existing `check-binding-parity.py` call, propagating a non-zero exit. Run `bash scripts/commands/gbx-test-bindings.sh` and confirm it still passes.

- [ ] **Step 8: Commit**
```bash
git add docs/scripts/check-param-names.py docs/tests-function-info/param_name_waiver.txt \
        docs/tests-function-info/test_param_names.py scripts/commands/gbx-test-bindings.sh
git commit -m "test(params): add Invariant A cross-tier param-name guard with waiver"
```

---

### Task 3: Invariant B — arity parity (with waiver baseline)

**Files:**
- Modify: `docs/scripts/check-param-names.py` (add `check_invariant_b`)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (add a `# --- arity ---` section if needed)

**Interfaces:**
- Consumes: `extract_py_params` (from Task 2), `load_fixture`, `load_waiver`.
- Produces: `check_invariant_b(report=False) -> list[str]` — for each function present in BOTH tiers, assert the heavy and light **parameter counts** are equal (names already gated by A; this catches a tier that accepts a different number of args). Pre-existing arity divergences go in the waiver as the explicit baseline.

- [ ] **Step 1: Write the failing test.** Extend `test_param_names.py`:
```python
def test_invariant_b_present():
    import importlib.util, sys
    from pathlib import Path
    p = Path(__file__).resolve().parents[2] / "docs/scripts/check-param-names.py"
    spec = importlib.util.spec_from_file_location("cpn", p)
    m = importlib.util.module_from_spec(spec); sys.modules["cpn"] = m
    spec.loader.exec_module(m)
    assert hasattr(m, "check_invariant_b"), "check_invariant_b not implemented"
```

- [ ] **Step 2: Run to verify it fails.**
Run: `python -m pytest docs/tests-function-info/test_param_names.py::test_invariant_b_present -v`
Expected: FAIL (`check_invariant_b not implemented`).

- [ ] **Step 3: Implement `check_invariant_b`** in `check-param-names.py`:
```python
def check_invariant_b(report: bool = False) -> list[str]:
    fixture, waiver = load_fixture(), load_waiver()
    violations = []
    for gbx_name in fixture:
        heavy = _first_existing(HEAVY_GLOBS, gbx_name)
        light = _first_existing(LIGHT_GLOBS, gbx_name)
        if heavy is None or light is None:
            continue  # parity only meaningful when both tiers exist
        if len(heavy) != len(light):
            msg = (f"[B] {gbx_name}: heavy arity {len(heavy)} != light arity {len(light)}")
            if report or gbx_name not in waiver:
                violations.append(msg)
    return violations
```
And extend `main()` to run both: `violations = check_invariant_a(args.report) + check_invariant_b(args.report)`.

- [ ] **Step 4: Seed the arity waiver.**
Run: `python docs/scripts/check-param-names.py --report`
Add any `[B]` functions not already in the waiver (the ~32 pre-existing arity divergences). These are the explicit baseline — tracked, not fixed here.

- [ ] **Step 5: Verify green at baseline.**
Run: `python docs/scripts/check-param-names.py` → `check-param-names: OK`.
Run: `python -m pytest docs/tests-function-info/test_param_names.py -v` → PASS.

- [ ] **Step 6: Commit**
```bash
git add docs/scripts/check-param-names.py docs/tests-function-info/param_name_waiver.txt \
        docs/tests-function-info/test_param_names.py
git commit -m "test(params): add Invariant B arity-parity guard with baseline waiver"
```

---

## Phase 1 — Renames (one rule per commit)

**Every Phase-1 task follows the same shape.** For the rule's functions:
1. Read the CURRENT source on every surface before editing (never trust a name from memory).
2. Rename Scala case-class fields + `functions.scala` wrappers (keep arg count == `builder()` range).
3. Rename heavy shim + light binding params (`python/geobrix/src/...` only).
4. Update docs `**Signature:**` lines + param bullets + any doc-test `*_sql_example()` that hardcodes the name.
5. Regenerate metadata: `bash scripts/commands/gbx-docs-function-info.sh` (or `gbx:docs:function-info`).
6. Remove the rule's functions from `param_name_waiver.txt`.
7. Verify: `python docs/scripts/check-param-names.py` green; affected light-Python tests green (`bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/<pkg>/`); `bash scripts/commands/gbx-lint-python.sh --check`.
8. Commit with the rule id.

### Task 4: R1 — `tileExpr` → `tile` (Scala-internal)

**Files:** Scala rasterx expressions + `rasterx/functions.scala`; verify heavy/light Python already say `tile` (no change expected there). ~113 functions, ~750 sites.

**Interfaces:** Produces no signature change on Python/SQL (already `tile`); aligns Scala field name to the derived/published `tile`.

- [ ] **Step 1: Inventory the sites.**
```bash
cd /Users/mjohns/IdeaProjects/geobrix
grep -rl "tileExpr" src/main/scala/com/databricks/labs/gbx/rasterx | sort
```
- [ ] **Step 2: Rename `tileExpr` → `tile`** in each file (field declarations, `.eval` uses, `children` seqs, wrapper params). Use targeted edits per file; verify each still references `tile` consistently. Do NOT touch `*Expr` fields that are not `tileExpr` (e.g. `sizeInMBExpr`, `exprConfExpr`).
- [ ] **Step 3: Confirm no `tileExpr` remains.** `grep -rn "tileExpr" src/main/scala | grep -v target` → no output.
- [ ] **Step 4: Rebuild + stage JAR and regenerate metadata** (heavy tier needs the JAR): dispatch via `gbx:docker:exec` a `mvn -DskipTests package`, then `bash scripts/commands/gbx-docs-function-info.sh`. Confirm `function-info.json` still shows `tile` for rasterx functions (unchanged — derivation already produced it).
- [ ] **Step 5: Verify + commit.** `python docs/scripts/check-param-names.py` green (R1 functions were likely never waived since Python already said `tile` — confirm the waiver is unchanged or trim any `tile_expr` entries). Run affected Scala suite for one representative raster expression via `gbx:test:scala --suite`. Then:
```bash
git add -A && git commit -m "refactor(rasterx): rename tileExpr field to tile (R1)"
```

### Task 5: R5 — `res` → `resolution`

**Files:** ~18 functions across gridx (bng/h3/grid) + any rasterx grid fns; Scala fields+wrappers, heavy shim, light pygx/pyrx, docs, examples.

- [ ] **Step 1: Inventory.** `grep -rln -w "res" src/main/scala/com/databricks/labs/gbx/gridx python/geobrix/src` (filter to param contexts; ignore unrelated `res` substrings).
- [ ] **Step 2–7:** Apply the standard shape (rename `res`→`resolution` on all surfaces; regenerate; unwaive; verify light-Python tests for gridx; lint).
- [ ] **Step 8: Commit** `git commit -m "refactor(gridx): rename res param to resolution (R5)"`

### Task 6: R6 — `srid` → `out_srid` / `out_crs`

**Files:** the 7 output-producing functions (`rst_rasterize`, `rst_gridfrompoints[_agg]`, `rst_dtmfromgeoms[_agg]`, `rst_{h3,quadbin,bng}_rasterize_agg`); heavy shim (light already correct per recon — verify).

- [ ] **Step 1: Read the current builder + wrapper** for each of the 7 to confirm the arg is the OUTPUT crs and whether `out_crs` already coexists. Do NOT touch input-geometry SRIDs.
- [ ] **Step 2–7:** Rename heavy `srid`→`out_srid` (and `crs`→`out_crs` where paired); confirm light already matches; regenerate; unwaive; verify.
- [ ] **Step 8: Commit** `git commit -m "refactor(rasterx): rename output srid to out_srid/out_crs (R6)"`

### Task 7: N1 — `geomWkb` / `geom_wkb` → `geom`

**Files:** `gbx_st_*` geom-accepting functions in vectorx; Scala fields+wrappers, heavy+light Python, docs, examples. **Also update CLAUDE.md's canonical usageArgs example** (line ~168: `geom_wkb, attrs_struct, ...` → `geom, attrs_struct, ...`).

- [ ] **Step 1: Inventory.** `grep -rln "geomWkb\|geom_wkb" src/main/scala python/geobrix/src docs/docs/api`.
- [ ] **Step 2–7:** Rename to `geom` on all surfaces; regenerate; unwaive; verify vectorx light-Python tests.
- [ ] **Step 8: Update CLAUDE.md** the `geom_wkb`→`geom` canonical example, then commit:
```bash
git add -A && git commit -m "refactor(vectorx): rename geom_wkb param to geom (N1)"
```

### Task 8: N9 — `points_array` / `breaklines_array`

**Files:** `st_triangulate`, `st_interpolateelevationgeom`, `st_interpolateelevationbbox`. Scala already `pointsArray`/`breaklinesArray` (derives to `points_array`); FIX heavy Python `points_geom`/`breaklines_geom` → `points_array`/`breaklines_array`; verify light; update docs `**Signature:**` (currently `points`).

- [ ] **Step 1:** Confirm Scala field is `pointsArray` (`src/main/scala/com/databricks/labs/gbx/vectorx/expressions/ST_Triangulate.scala:32`) — no Scala change needed.
- [ ] **Step 2:** Rename heavy shim `points_geom`→`points_array`, `breaklines_geom`→`breaklines_array` in `vectorx/functions.py`; align light `pyvx`; update the 3 docs signatures + param bullets.
- [ ] **Step 3–7:** Regenerate; unwaive the 3; verify vectorx light-Python tests + the triangulate/interpolate doc-tests.
- [ ] **Step 8: Commit** `git commit -m "refactor(vectorx): rename points_geom to points_array (N9)"`

### Task 9: R4 — `cellid` everywhere (params + output struct fields)

**Files:** ALL grid cell-id functions across bng/h3/grid/quadbin; Scala fields + `elementSchema` StructField literals + wrappers; heavy shim; light pygx; docs; examples. **Preserve `left_chip`/`right_chip`/`input_chip`.** **Update CLAUDE.md** (line ~171-172: record `cellid`/`cellid1`/`cellid2` exception to snake_case). **Update `beta-release-notes.mdx`** (result-schema break: explode output field `cellId`→`cellid`).

- [ ] **Step 1: Inventory params.** `grep -rln "cell_id\|cellId\|\bc1\b\|\bc2\b\|cell_a\|cell_b" src/main/scala/com/databricks/labs/gbx/gridx python/geobrix/src` — classify each hit as bare-id (→`cellid`/`cellid1`/`cellid2`) vs chip-struct (leave as `*_chip`).
- [ ] **Step 2: Rename bare-id params** to `cellid` (single) / `cellid1`,`cellid2` (pairs) on all surfaces. `bng_distance` is the confirmed case: heavy `cell_id1,cell_id2` and light `cell_a,cell_b` both → `cellid1,cellid2`.
- [ ] **Step 3: Rename output struct fields** `StructField("cellId", ...)` → `StructField("cellid", ...)` in `elementSchema` (e.g. `BNG_KRingExplode.scala:42`, `BNG_TessellateExplode.scala:26`, `BNG_GeometryKRingExplode.scala`, `BNG_KLoopExplode.scala`, `BNG_GeometryKLoopExplode.scala`). Update docs signatures that show `cellId`.
- [ ] **Step 4: Confirm chip untouched.** `grep -rn "left_chip\|right_chip\|input_chip" python/geobrix/src src/main/scala` still present; no chip function renamed to `cellid*`.
- [ ] **Step 5: Rebuild+stage JAR, regenerate metadata.**
- [ ] **Step 6: Unwaive** all R4 functions; `python docs/scripts/check-param-names.py` green.
- [ ] **Step 7: Update CLAUDE.md + beta-release-notes**, run gridx light-Python tests + a representative gridx Scala suite + lint.
- [ ] **Step 8: Commit**
```bash
git add -A && git commit -m "refactor(gridx): standardize cellid naming across surfaces (R4)"
```

### Task 10: R3 — `rst_slope` anisotropic `xscale` / `yscale`

**Files:** `RST_Slope*.scala` (grow heavy to anisotropic), `rasterx/functions.scala` wrappers, heavy shim, light pyrx (already anisotropic — verify), docs, `rasterx_functions_sql.py` example.

- [ ] **Step 1: Read current heavy `rst_slope`** builder + overloads (`grep -rn "RST_Slope\|def rst_slope" src/main/scala`). Confirm current single `scale` and the builder arity branches.
- [ ] **Step 2: Add `yscale`** so heavy exposes `xscale` + `yscale` (matching light's shape); update `builder()` arity + wrapper overloads in lockstep (arg count must match). This is a real behavior change — read `[[terrain-crs-scale-gdal-normal]]` context; xscale/yscale map to GDAL's separate x/y scaling.
- [ ] **Step 3: Align surfaces** (heavy shim, docs signature, example); verify light already `xscale,yscale`.
- [ ] **Step 4: Rebuild+stage JAR, regenerate metadata; unwaive `gbx_rst_slope`.**
- [ ] **Step 5: Verify** `check-param-names.py` green; run the RST_Slope Scala suite (`gbx:test:scala --suite '*RST_Slope*'`) and the slope doc-test.
- [ ] **Step 6: Commit** `git commit -m "feat(rasterx): rst_slope anisotropic xscale/yscale (R3)"`

---

### Task 11: Close-out — waiver empty, guards hard

**Files:** `docs/tests-function-info/param_name_waiver.txt`

- [ ] **Step 1:** Confirm the name-migration section of the waiver is empty (only the ~32 pre-existing arity-B baseline entries may remain, tracked for the separate arity workstream). If any `[A]` entry remains, a rename was missed — flag it.
- [ ] **Step 2:** Run the full guard un-waived to see remaining known baseline: `python docs/scripts/check-param-names.py --report` — every `[A]` violation should be gone; only `[B]` baseline remains.
- [ ] **Step 3:** Run `bash scripts/commands/gbx-test-bindings.sh` (full parity + param-name guard) → green.
- [ ] **Step 4: Commit** any waiver trim: `git commit -m "test(params): empty the param-name migration waiver (A complete)"`

---

## Self-Review

**Spec coverage:** R1→T4, R2 already done, R3→T10, R4(a/b/c/d)→T9, R5→T5, R6→T6, N1→T7, N9→T8; fixture→T1; Invariant A→T2; Invariant B→T3; `cellid` CLAUDE.md + release-notes→T9; N1 CLAUDE.md example→T7; waiver close-out→T11. All ratified rulings covered.

**Placeholder scan:** Task 1 Step 1 and the Phase-1 "standard shape" reference reading current source rather than embedding 750 verbatim edits — this is deliberate (the edits are mechanical and file-specific; the plan gives the exact grep/verify commands and the exact target names). No `TBD`/`add error handling`/`similar to Task N` placeholders.

**Type consistency:** `extract_py_params`, `load_fixture`, `load_waiver`, `_first_existing`, `check_invariant_a`, `check_invariant_b` are named identically across Tasks 2–3 and the fixture format (tab-separated, bracket-stripped) is consistent between fixture authoring (T1) and the checker (T2).
