# RasterX Error Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every RasterX function (heavy Scala + light pyrx) degrade on degenerate input instead of raising or emitting a misleading sentinel, with a diagnosable `metadata.last_error` wherever the return type can carry one.

**Architecture:** Behavior is derived from the return type: scalar accessors → NULL; tile ops → empty tile + reason (already done in heavy); aggregators → skip corrupt member + record dropped-count; generators/UDTFs → exactly one error-tile row (light stops yielding zero rows). No new user-facing knob; heavy's `crashExpressions` Spark-conf escape hatch is untouched. Cross-tier parity is the spine — the same degenerate input must produce the same signal on both tiers.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / Java 17 (heavy); Python 3.12 + rasterio (light pyrx). All test/build via the `gbx:*` command palette inside the `geobrix-dev` Docker container.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-08-08-rasterx-error-handling-design.md`. RasterX ONLY — VectorX, GridX, and the geometry domain-check are OUT of scope; do not touch `vectorx/`, `gridx/`, or `pyvx`/`pygx`.
- **No new user-facing argument.** No `strict`/`debug` parameter on any function. SQL binds positionally and ~12 functions already use arg-count as their sole optionality discriminator — a new trailing arg is a hazard.
- **`crashExpressions` is untouched.** `ExpressionConfig.crashExpressions` (`spark.databricks.labs.gbx.expressions.crash.on.error`, default false) stays exactly as-is. A negative test must confirm it still raises.
- **Empty/error-tile shape (both tiers):** `raster=NULL` **AND** `path=NULL` (both required — `raster=NULL` alone is the *virtual-tile* signal; `path` must also be NULL), plus `metadata.last_error` set. Heavy uses `RasterSerializationUtil.tileToRow((cellId, null, errorMetadata), rasterType, null)`; light uses a `build_error_tile(...)` helper (Task 6) on the v2 nullable-raster schema.
- **`last_error` message format:** `"<RST_FnName>: <cause>"` — short, stable, greppable, same token both tiers. Not a stack trace.
- **`RST_SRID` guardrail:** `execute()` returning `0` for an authority-less CRS (e.g. ESRI:54008) is a LEGITIMATE answer and MUST be preserved. Only the error/swallow path (the old `getOrElse(0)`) becomes NULL. After this change: no-EPSG CRS → `0`; corrupt/unreadable tile → NULL. Never collapse the two.
- **Boxing (heavy numeric accessors):** primitive `Int`/`Long`/`Double` cannot be null. To return NULL, the `eval` return type becomes boxed (`java.lang.Integer`/`java.lang.Long`/`java.lang.Double`) and uses `.orNull`. Precedent: `RST_Min.scala` already does `java.lang.Double.valueOf(...)` + `.orNull`. `returnNullable = true` is already set in `InvokedExpression.invoke()`.
- **`git add` EXPLICIT paths only** — NEVER `git add -A` (strays: `.isaac/`, `.tmp`, `scratchpad/`, zips). Commit subject ≤72 chars + WHY body + `Co-authored-by: Isaac`.
- **Tests are real, not mocked** — real corrupt raster bytes; mock only external/expensive/flaky. Heavy Scala suites + light pyrx suites run in Docker via `gbx:test:scala` / `gbx:test:python`. Run ONLY the affected suites.
- **No Databricks profile needed.** NEVER run `databricks auth login`.
- **Facts that are NOT findings:** heavy needs no staged JAR for these in-container Scala eval suites (the test command builds what it needs); light is pure Python, no JAR. Both tiers register the same `gbx_rst_*` names.

---

### Task 1: Heavy numeric accessors → NULL (spike + roll-out)

Replace the error-path sentinels (`0` / `-1` / `-1L` / `Double.NaN`) with NULL in the 15 numeric accessors. `RST_SRID` is done separately (Task 2) because of its legitimate-`0` nuance. `RST_MetaData` is ALREADY compliant (its error path is `.orNull` at line 66; the `getOrElse(Map.empty)` calls are inside `execute()` for the real "no metadata" case) — do NOT touch it.

**Files (each: change the `eval` return type to boxed + swap the sentinel for `.orNull`):**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Width.scala:47` (`getOrElse(0)`)
- Modify: `.../accessors/RST_Height.scala:47` (`getOrElse(-1)`)
- Modify: `.../accessors/RST_NumBands.scala:46` (`getOrElse(-1)`)
- Modify: `.../accessors/RST_MemSize.scala:48` (`getOrElse(-1L)`)
- Modify: `.../accessors/RST_ScaleX.scala:47`, `RST_ScaleY.scala:47`, `RST_SkewX.scala:47`, `RST_SkewY.scala:47`, `RST_UpperLeftX.scala:47`, `RST_UpperLeftY.scala:47`, `RST_PixelWidth.scala:47`, `RST_PixelHeight.scala:48`, `RST_Rotation.scala:47` (all `getOrElse(Double.NaN)`)
- Modify: `.../accessors/RST_RasterToWorldCoordX.scala:49`, `RST_RasterToWorldCoordY.scala:49` (both `getOrElse(Double.NaN)`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsEvalTest.scala`

**Interfaces:**
- Produces: `gbx_rst_width`/`height`/`numbands`/`memsize` and the geo-transform doubles return NULL (not a sentinel) on a corrupt/unreadable tile. `dataType`/`nullable` are unchanged (already `nullable=true`); only the boxed eval-return + `.orNull` changes.

- [ ] **Step 1: Write the failing test (spike on RST_Width first)**

In `RST_AccessorsEvalTest.scala`, add a new test. The corrupt-row helper mirrors `RST_ErrorHandlerTest.minimalRowV2` — build a tile row whose raster bytes are garbage so `rowToDS`/`execute` throws and `safeEval` swallows to null:

```scala
test("RST_Width returns NULL (not 0) on a corrupt raster") {
    val sc = spark
    import com.databricks.labs.gbx.rasterx.functions._
    import sc.implicits._
    functions.register(spark)
    // Garbage bytes: not a valid raster -> GDAL open fails -> safeEval swallows.
    val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("raster")
    val res = df.select(rst_width(col("raster")).as("w")).collect()
    assert(res.head.get(0) == null, "corrupt raster width must be NULL, not the 0 sentinel")
}
```

- [ ] **Step 2: Run it, verify it FAILS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest' --log accessor-null-spike.log`
Expected: FAIL — `rst_width` currently returns `0`, so `get(0)` is `0`, not null.

- [ ] **Step 3: Fix RST_Width (the boxing spike)**

In `RST_Width.scala`, change the `eval(row, conf, dt)` return type from `Int` to `java.lang.Integer` and the final line from `.map(_.asInstanceOf[Int]).getOrElse(0)` to `.map(v => java.lang.Integer.valueOf(v.asInstanceOf[Int])).orNull`. Also change the 1-arg overload `def eval(row, conf): Int` to `java.lang.Integer`. Leave `execute(ds): Int` (the happy-path primitive) unchanged. Pattern mirrors `RST_Min.scala` (`java.lang.Double.valueOf(...)` + `.orNull`).

- [ ] **Step 4: Run the spike test, verify it PASSES**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest' --log accessor-null-spike.log`
Expected: PASS. If the `Invoke` mechanism rejects a boxed-null return (it should not — `returnNullable=true` and String accessors already `.orNull`), STOP and report; do not proceed to roll-out on a broken spike.

- [ ] **Step 5: Roll out to the other 14 numeric accessors**

Apply the identical transform to `RST_Height`, `RST_NumBands` (→ `java.lang.Integer`), `RST_MemSize` (→ `java.lang.Long`, `java.lang.Long.valueOf`), and the 11 `Double` ones (`RST_ScaleX/ScaleY/SkewX/SkewY/UpperLeftX/UpperLeftY/PixelWidth/PixelHeight/Rotation/RasterToWorldCoordX/RasterToWorldCoordY` → `java.lang.Double`, `java.lang.Double.valueOf`, `.orNull` replacing `getOrElse(Double.NaN)`). Add one assertion per group to the test (one Int, one Long, one Double) asserting NULL on corrupt input.

- [ ] **Step 6: Run the full accessor suite, verify PASS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest' --log accessor-null.log`
Expected: PASS (existing happy-path assertions + new NULL-on-corrupt assertions). Also run `RST_AccessorsExecuteTest` to confirm `execute()` (happy path) is unbroken: `--suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest'`.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Width.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Height.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_NumBands.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_MemSize.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_ScaleX.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_ScaleY.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SkewX.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SkewY.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_UpperLeftX.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_UpperLeftY.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_PixelWidth.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_PixelHeight.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Rotation.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_RasterToWorldCoordX.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_RasterToWorldCoordY.scala \
  src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsEvalTest.scala
git commit -m "fix(rasterx): heavy numeric accessors return NULL, not a sentinel

15 accessors (width/height/numbands/memsize + geo-transform doubles)
returned 0/-1/-1L/NaN on a corrupt raster via getOrElse. A corrupt
raster now yields NULL (boxed eval return + .orNull, mirroring
RST_Min). rst_srid is handled separately; rst_metadata already .orNull.

Co-authored-by: Isaac"
```

---

### Task 2: Heavy `RST_SRID` → NULL on error, keep legitimate `0`

`RST_SRID` is separate because its `execute()` returns `0` as a REAL answer for authority-less CRS. Only the swallow path becomes NULL.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SRID.scala:50` (the `getOrElse(0)` — the ERROR path, not the `case _ => 0` inside `execute`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsEvalTest.scala`

**Interfaces:**
- Produces: `gbx_rst_srid` returns NULL on a corrupt/unreadable tile, still returns `0` for a valid raster whose CRS has no EPSG authority.

- [ ] **Step 1: Write two failing tests**

```scala
test("RST_SRID returns NULL (not 0) on a corrupt raster") {
    val sc = spark
    import com.databricks.labs.gbx.rasterx.functions._
    import sc.implicits._
    functions.register(spark)
    val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("raster")
    val res = df.select(rst_srid(col("raster")).as("s")).collect()
    assert(res.head.get(0) == null, "corrupt raster SRID must be NULL, not 0")
}
```
Also add/keep a POSITIVE test proving a valid authority-less raster still returns `0` (guardrail). If no authority-less fixture exists in test resources, assert instead that a valid EPSG raster returns its real code (non-null, non-zero) — the point is that a successful `execute()` path is untouched.

- [ ] **Step 2: Run, verify the corrupt test FAILS** (currently returns `0`).

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest' --log srid-null.log`

- [ ] **Step 3: Fix RST_SRID**

Change the `eval(row, conf, rdt)` return type from `Int` to `java.lang.Integer`, the 1-arg overload likewise, and the final `.map(_.asInstanceOf[Int]).getOrElse(0)` to `.map(v => java.lang.Integer.valueOf(v.asInstanceOf[Int])).orNull`. Leave `execute(ds)` COMPLETELY unchanged — its internal `case _ => 0` for authority-less CRS is the legitimate answer that must survive.

- [ ] **Step 4: Run, verify both tests PASS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest' --log srid-null.log`
Expected: corrupt → NULL passes; valid → real/`0` passes.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SRID.scala \
  src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsEvalTest.scala
git commit -m "fix(rasterx): rst_srid returns NULL on corrupt tile, keeps 0 for no-EPSG

The error path (getOrElse(0)) collided with the legitimate 0 that
execute() returns for authority-less CRS (ESRI etc.), so a caller
could not tell 'no EPSG authority' from 'unreadable tile'. Error path
now NULL; execute()'s authority-less 0 is preserved. Ambiguity gone.

Co-authored-by: Isaac"
```

---

### Task 3: Heavy tile aggregators → skip corrupt member + record count

`RST_CombineAvgAgg`, `RST_MergeAgg`, `RST_DerivedBandAgg` already null-skip in `update()` and return NULL for an empty buffer, but a NON-null-but-corrupt member flows unguarded into `rowToTile` in `eval()` and kills the task (none route through `safeEval`).

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_CombineAvgAgg.scala` (the `eval` multi-tile branch, ~line 70: `buffer.map(row => ...rowToTile...)`)
- Modify: `.../agg/RST_MergeAgg.scala` (analogous `eval`)
- Modify: `.../agg/RST_DerivedBandAgg.scala` (analogous `eval`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AggEvalTest.scala`

**Interfaces:**
- Consumes: the empty-tile shape convention (Global Constraints) and `metadata.last_error` token format.
- Produces: `gbx_rst_combineavg_agg`/`merge_agg`/`derivedband_agg` skip a corrupt member instead of raising; the emitted aggregate tile's `metadata.last_error` records the dropped count when any were dropped; an all-corrupt/empty group returns the existing NULL.

- [ ] **Step 1: Write the failing test**

In `RST_AggEvalTest.scala`, add a test that groups a valid tile with a corrupt-bytes tile and asserts (a) `.collect()` does NOT throw, (b) the result tile is non-null (aggregate over the good member), (c) its metadata contains a `last_error` key mentioning the drop. Mirror the existing agg-test tile construction in that file.

```scala
test("RST_CombineAvgAgg skips a corrupt member and records the drop, does not raise") {
    // ... build a DataFrame with one valid tile + one Array[Byte](1,2,3) corrupt tile
    //     in the same group; agg with rst_combineavg_agg
    // assert noException should be thrownBy df....collect()
    // assert result tile non-null; assert metadata("last_error") contains "RST_CombineAvgAgg"
}
```

- [ ] **Step 2: Run, verify FAIL** (currently the corrupt `rowToTile` throws and kills the task).

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AggEvalTest' --log agg-skip.log`

- [ ] **Step 3: Implement skip + count in each `eval`**

In each aggregator's `eval` multi-tile branch, wrap the per-row `rowToTile` in a `Try`/try-catch: collect the successfully-parsed tiles into `tiles`, count failures into `dropped`. Compute the aggregate over `tiles` (unchanged logic). When building `resMtd`/the output row, if `dropped > 0` add `"last_error" -> s"RST_CombineAvgAgg: skipped $dropped corrupt input tile(s)"` (use each class's own name) to the metadata map before `tileToRow`. If ALL members failed (`tiles.isEmpty` after the guard), return `null` (reuse the empty-buffer branch semantics). Release datasets for the successfully-opened tiles only.

- [ ] **Step 4: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AggEvalTest' --log agg-skip.log`
Expected: PASS. Add the analogous mixed-group test for `merge_agg` and `derivedband_agg` and confirm all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_CombineAvgAgg.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala \
  src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_DerivedBandAgg.scala \
  src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AggEvalTest.scala
git commit -m "fix(rasterx): tile aggregators skip corrupt members instead of raising

RST_CombineAvgAgg/MergeAgg/DerivedBandAgg fed a corrupt member's bytes
straight into rowToTile in eval(), killing the whole task (none use
safeEval). They now skip a member that fails to parse, aggregate over
the good ones, and record the dropped count in metadata.last_error.
All-corrupt/empty group still returns NULL.

Co-authored-by: Isaac"
```

---

### Task 4: Light accessors → None on a corrupt (non-empty) tile

Light accessors funnel through `_header_accessor_udf` / `_pixel_accessor_udf`, which already return `None` for an EMPTY tile (`_tile_is_empty`). The gap: a corrupt-but-non-empty raster raises inside `core_fn(ds)`.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — the `_header_accessor_udf` and `_pixel_accessor_udf` factory bodies (`def _udf(tile): ...`), plus the `_header_accessor_udf2` 2-arg variant if it wraps the same core.
- Test: `python/geobrix/test/rasterx/test_rasterx_accessors.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (parallel-safe).
- Produces: light `rst_width`/`srid`/`scalex`/etc. return `None` on corrupt input, matching heavy Tasks 1–2. `rst_srid` still returns `None`/int per its core (`ds.crs.to_epsg()`); confirm parity with heavy's NULL-on-corrupt.

- [ ] **Step 1: Write the failing test**

In `test_rasterx_accessors.py` (reuse its spark/registration fixture), add a test passing a tile struct whose `raster` is garbage bytes to `rx.rst_width` / `rx.rst_srid` and assert the collected value is `None`, not an exception, and not a sentinel.

```python
def test_light_accessor_corrupt_tile_returns_none(spark):
    from databricks.labs.gbx.pyrx import functions as pf
    # build a tile struct with corrupt raster bytes (mirror how this file builds tiles)
    # df.select(pf.rst_width("tile")).collect()[0][0] is None
```

- [ ] **Step 2: Run, verify FAIL**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_rasterx_accessors.py --log light-accessor-none.log`
Expected: FAIL — `core_fn(ds)` raises on the garbage bytes (the UDF surfaces the error), so it does not return None.

- [ ] **Step 3: Guard the factory bodies**

In `_header_accessor_udf._udf` (and `_pixel_accessor_udf._udf`, `_header_accessor_udf2`), wrap the `open_header`/`_open` + `core_fn(ds)` block in `try/except Exception: return None` (keep the existing `_tile_is_empty` early-return). Keep it narrow — only the open+compute is guarded; the `_env.configure_gdal_env()` call stays outside if it must always run, or inside the try if a config failure should also degrade (prefer inside, so an env failure degrades rather than raises). Match the file's existing `# noqa: BLE001` convention for broad excepts.

- [ ] **Step 4: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_rasterx_accessors.py --log light-accessor-none.log`
Expected: PASS (corrupt → None; existing happy-path assertions still pass).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
  python/geobrix/test/rasterx/test_rasterx_accessors.py
git commit -m "fix(pyrx): light accessors return None on a corrupt raster

The header/pixel accessor UDF factories returned None only for an
EMPTY tile; a corrupt-but-non-empty raster raised inside core_fn(ds).
Guard the open+compute so corrupt input degrades to None, matching the
heavy accessors' NULL-on-corrupt behavior (cross-tier parity).

Co-authored-by: Isaac"
```

---

### Task 5: Light aggregators → skip corrupt member + record count

Mirror Task 3 on the light tier. `_merge_bytes` (`functions.py:605`), `_combineavg_bytes` (`:691`), and `_frombands_bytes` (`:783`) already drop empty/None members (`[t for t in tiles if t is not None and not _tile_is_empty(t)]` / `if t is None or _tile_is_empty(t): continue`) and return `None` for an empty group, but a corrupt-but-non-empty member raises during `_to_virtual_tile`/`materialize_to_bytes`/open, killing the group.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — `_merge_bytes` (~605), `_combineavg_bytes` (~691), `_frombands_bytes` (~783). These `*_bytes` helpers return raw bytes; the enclosing agg UDF/pandas-agg attaches metadata. Check whether the emitted aggregate carries a `metadata` map the drop-count can land on; if the `*_bytes` layer cannot carry metadata (returns bare bytes), record the drop via the enclosing aggregator's tile assembly (where `build_tile`/metadata is set), NOT in the bare-bytes helper.
- Test: `python/geobrix/test/pyrx/test_core_agg.py`

**Interfaces:**
- Consumes: `_tile_is_empty` (existing). Produces: light `rst_merge_agg`/`combineavg_agg`/`frombands_agg` skip a corrupt member instead of raising; where the return path carries a metadata map, `last_error` records the dropped count; all-corrupt/empty group returns `None` (existing).

- [ ] **Step 1: Write the failing test**

In `test_core_agg.py` (reuse its fixtures), pass a list mixing a valid tile + a corrupt-bytes tile to the merge/combineavg aggregation and assert (a) no exception, (b) a non-None aggregate over the good member. If the aggregate return path carries metadata, assert `last_error` mentions the drop; if it returns bare bytes with no carrier, assert only (a)+(b) and note the no-carrier limitation.

- [ ] **Step 2: Run, verify FAIL**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_agg.py --log light-agg-skip.log`
Expected: FAIL — the corrupt member raises during open/materialize.

- [ ] **Step 3: Implement per-member skip**

In each of the three helpers, wrap the per-member open/materialize in `try/except Exception: dropped += 1; continue` (keep the existing empty/None filter). Aggregate over survivors. If all members drop, return `None`. Where the enclosing aggregator assembles the output tile's metadata, add `"last_error": f"RST_Merge: skipped {dropped} corrupt input tile(s)"` (correct name per function) when `dropped > 0`. Match the file's `# noqa: BLE001` convention.

- [ ] **Step 4: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_agg.py --log light-agg-skip.log`
Expected: PASS. Add the analogous mixed-group assertion for combineavg + frombands.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
  python/geobrix/test/pyrx/test_core_agg.py
git commit -m "fix(pyrx): light aggregators skip corrupt members instead of raising

_merge_bytes/_combineavg_bytes/_frombands_bytes dropped empty/None
members but a corrupt-but-non-empty member raised during
open/materialize, killing the group. They now skip a member that fails
to open and record the dropped count in metadata.last_error where the
return path carries one, mirroring heavy Task 3.

Co-authored-by: Isaac"
```

---

### Task 6: Light error-tile builder + nullable-raster schema

Prerequisite for Task 7. The legacy `TILE_SCHEMA` has `raster` NON-nullable, so a UDTF cannot yield an error tile with `raster=None` today. Add a nullable-raster capability + a builder.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py` — flip `TILE_SCHEMA`'s `raster` field to `nullable=True`; add `build_error_tile(last_error: str, cellid: int = -1) -> Dict`.
- Test: `python/geobrix/test/pyrx/test_serde_error_tile.py` (new)

**Interfaces:**
- Produces: `build_error_tile(last_error, cellid=-1)` returns `{"cellid": cellid, "raster": None, "metadata": {"last_error": last_error}}` — the empty/error shape (both `raster` and, in v2, `path` are absent/None). `TILE_SCHEMA` accepts a None raster. Task 6 consumes `build_error_tile`.

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/pyrx/test_serde_error_tile.py`:

```python
from databricks.labs.gbx.pyrx import _serde


def test_build_error_tile_shape():
    t = _serde.build_error_tile("RST_ReTile: unreadable raster")
    assert t["raster"] is None
    assert t["cellid"] == -1
    assert t["metadata"]["last_error"] == "RST_ReTile: unreadable raster"


def test_tile_schema_raster_is_nullable():
    field = [f for f in _serde.TILE_SCHEMA.fields if f.name == "raster"][0]
    assert field.nullable is True
```

- [ ] **Step 2: Run, verify FAIL**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_serde_error_tile.py --log serde-error-tile.log`
Expected: FAIL — `build_error_tile` doesn't exist; `raster` is non-nullable.

- [ ] **Step 3: Implement**

In `_serde.py`: change `StructField("raster", BinaryType(), nullable=False)` to `nullable=True`. Add:

```python
def build_error_tile(last_error: str, cellid: int = -1) -> Dict:
    """Empty/error tile: raster None (and no path) + last_error in metadata.

    Signals a swallowed failure that stays diagnosable. Mirrors heavy's
    RST_ErrorHandler error-tile row (raster NULL + errorMetadata).
    """
    return {"cellid": int(cellid), "raster": None, "metadata": {"last_error": last_error}}
```

- [ ] **Step 4: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_serde_error_tile.py --log serde-error-tile.log`
Expected: PASS.

- [ ] **Step 5: Guard against regression — run the broader serde/ds consumers**

Flipping `raster` to nullable could affect readers that assumed non-null. Run the ds + core suites to confirm nothing relied on the non-nullable constraint:
Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log serde-consumers.log`
Expected: PASS. If any test fails because it depended on `raster` being non-nullable, that is in-scope to reconcile here (report it).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py \
  python/geobrix/test/pyrx/test_serde_error_tile.py
git commit -m "feat(pyrx): nullable-raster tile schema + build_error_tile helper

Prereq for UDTF error rows: the legacy TILE_SCHEMA had raster
non-nullable, so a UDTF could not yield an error tile with raster=None.
Make raster nullable and add build_error_tile(last_error, cellid),
mirroring heavy's error-tile row shape.

Co-authored-by: Isaac"
```

---

### Task 7: Light UDTFs → one error-tile row (the parity fix)

Light UDTFs currently `return` (yield nothing) on an empty/corrupt tile, while heavy generators emit ONE error-tile row. Make light yield exactly one `build_error_tile` row so row-counts match.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — the streaming UDTF `eval` methods: `_RstSeparateBandsUDTF` (~3297), `_RstRetileUDTF` (~3312), `_RstToOverlappingTilesUDTF` (~3329), `_RstMakeTilesUDTF` (~3348), `_RstH3TessellateUDTF` (~3374), `_RstQuadbinTessellateUDTF` (~3495), `_RstBngTessellateUDTF` (~3528). (XYZPyramid UDTF has a different row schema — see note.)
- Test: `python/geobrix/test/rasterx/test_udtf_error_row.py` (new)

**Interfaces:**
- Consumes: `_serde.build_error_tile` (Task 6) and the now-nullable `TILE_SCHEMA`.
- Produces: each listed UDTF yields exactly ONE error-tile row on empty/corrupt input (instead of zero rows), with `metadata.last_error` = `"<RST_FnName>: <cause>"`.

- [ ] **Step 1: Write the failing test**

Create `python/geobrix/test/rasterx/test_udtf_error_row.py` (reuse a JAR-free light spark fixture as in other rasterx light tests). Feed a corrupt tile through `rx.rst_separatebands` (or the SQL `gbx_rst_separatebands`) and assert the result has EXACTLY ONE row whose tile `raster` is None and whose `metadata.last_error` contains `"RST_SeparateBands"`.

```python
def test_separatebands_corrupt_yields_one_error_row(spark):
    # build a corrupt tile struct; lateral-explode via rst_separatebands
    rows = ...collect()
    assert len(rows) == 1
    assert rows[0]["...tile..."]["raster"] is None
    assert "RST_SeparateBands" in rows[0]["...tile..."]["metadata"]["last_error"]
```

- [ ] **Step 2: Run, verify FAIL** (currently zero rows).

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_udtf_error_row.py --log udtf-error-row.log`

- [ ] **Step 3: Change each UDTF's empty/corrupt path to yield one error tile**

In each listed UDTF `eval`, replace the bare `if _tile_is_empty(tile): return` with `if _tile_is_empty(tile): yield _serde.build_error_tile("<RST_FnName>: empty or unreadable tile"); return`, and wrap the open+iterate body in `try/except Exception as e: yield _serde.build_error_tile(f"<RST_FnName>: {e}"); return` so a corrupt-but-non-empty tile also yields one row. Use each function's canonical name in the token (`RST_SeparateBands`, `RST_ReTile`, `RST_ToOverlappingTiles`, `RST_MakeTiles`, `RST_H3_Tessellate`, `RST_Quadbin_Tessellate`, `RST_BNG_Tessellate`). Match the file's `# noqa: BLE001` convention.

- [ ] **Step 4: Run, verify PASS; add one more UDTF assertion**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_udtf_error_row.py --log udtf-error-row.log`
Add a second assertion for `rst_retile` (one error row on corrupt input) and confirm both pass.

- [ ] **Step 5: Note on XYZPyramid + Polygonize**

`_RstXyzPyramidUDTF` and `_RstPolygonizeUDTF` use DIFFERENT row schemas (`_XYZPYRAMID_ROW_SCHEMA` / `_POLYGONIZE_ROW_SCHEMA`), not `TILE_SCHEMA`, so `build_error_tile` does not fit them. LEAVE them yielding zero rows for now — they are out of the tile-row contract and would need their own error-row shape. State this explicitly in the report; do NOT force `build_error_tile` into a mismatched schema.

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
  python/geobrix/test/rasterx/test_udtf_error_row.py
git commit -m "fix(pyrx): tile UDTFs yield one error row, matching heavy generators

Light retile/separatebands/tessellate/maketiles UDTFs yielded ZERO
rows on an empty/corrupt tile while heavy generators emit one
error-tile row -- a silent cross-tier row-count divergence. They now
yield exactly one build_error_tile row with last_error. XYZPyramid and
Polygonize use different row schemas and are left as-is (noted).

Co-authored-by: Isaac"
```

---

### Task 8: Cross-tier parity + negative-guard tests

The spine of the spec: assert heavy and light produce the SAME signal for the same degenerate input, and that `crashExpressions=true` still raises.

**Files:**
- Test (heavy): `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_ErrorHandlingParityTest.scala` (new) — or extend `RST_AccessorsEvalTest` / `RST_AggEvalTest` if a new suite is heavier than warranted; a dedicated suite is preferred for the crashExpressions negative guard.
- Test (light): extend `python/geobrix/test/rasterx/test_rasterx_accessors.py` and `test_udtf_error_row.py` with the parity-value assertions.

**Interfaces:**
- Consumes: all of Tasks 1–7.

- [ ] **Step 1: Heavy — crashExpressions negative guard**

Add a test: with `spark.conf.set("spark.databricks.labs.gbx.expressions.crash.on.error", "true")`, a corrupt raster through `rst_width` (or any accessor) RAISES (assert `intercept[Exception]`), proving the dev escape hatch is intact. Reset the conf after (try/finally).

```scala
test("crashExpressions=true still raises on corrupt input (escape hatch intact)") {
    spark.conf.set("spark.databricks.labs.gbx.expressions.crash.on.error", "true")
    try {
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("raster")
        intercept[Exception] { df.select(rst_width(col("raster"))).collect() }
    } finally {
        spark.conf.set("spark.databricks.labs.gbx.expressions.crash.on.error", "false")
    }
}
```

- [ ] **Step 2: Run, verify PASS**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_ErrorHandlingParityTest' --log parity-negative.log` (or the suite you extended).

- [ ] **Step 3: Light — assert the same degrade values as heavy**

In the light tests, add explicit parity assertions matching heavy's Task 1/2 results: corrupt tile → `rst_width` is None, `rst_srid` is None, `rst_scalex` is None (not NaN). And confirm the UDTF error-row count is 1 (matching heavy's one-error-row) — the count-parity claim.

- [ ] **Step 4: Run the light parity assertions, verify PASS**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/ --log light-parity.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_ErrorHandlingParityTest.scala \
  python/geobrix/test/rasterx/test_rasterx_accessors.py \
  python/geobrix/test/rasterx/test_udtf_error_row.py
git commit -m "test(rasterx): cross-tier degrade parity + crashExpressions guard

Assert heavy and light produce the same NULL/None on corrupt accessor
input and the same one-error-row from UDTFs, and that
crashExpressions=true still raises (the dev escape hatch is intact).

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage (8 tasks):**
- Contract row "scalar accessors → NULL" → Task 1 (15 numeric, heavy), Task 2 (srid, heavy), Task 4 (light). ✓
- Contract row "tile ops → empty + reason" → already done in heavy (spec component 3, "confirm coverage only"); no code task needed — Task 8 parity implicitly exercises it. If Task 8 finds an unwrapped tile-op, that becomes an in-scope fix (noted in Task 8). ✓
- Contract row "aggregators → skip + count" → Task 3 (heavy), Task 5 (light). ✓
- Contract row "generators/UDTFs → one error row" → Task 6 (schema+builder prereq) + Task 7 (light UDTFs). Heavy generators already emit the error row (spec component 3). ✓
- `rst_srid=0` ambiguity → Task 2. ✓
- `crashExpressions` untouched + negative guard → Task 8. ✓
- Cross-tier parity spine → Task 8 + per-task parity assertions. ✓
- Spec component 5 (light aggregators) → Task 5 (its own task; the earlier gap where it had no home is resolved). ✓

**Placeholder scan:** no TBD/TODO. Every code step has real before/after (verbatim line numbers, the boxing idiom, the `build_error_tile` body). Two deliberately-open items are scoped OUT with reasons, not placeholders: XYZPyramid/Polygonize use different row schemas (Task 7 Step 5); and the light `*_bytes` aggregator layer may lack a metadata carrier for the drop-count (Task 5 records it at the enclosing tile-assembly layer where one exists, else notes the limitation). ✓

**Type consistency:** `build_error_tile(last_error, cellid=-1)` used identically in Tasks 6 and 7. `java.lang.Integer/Long/Double.valueOf(...) + .orNull` idiom consistent across Tasks 1–2. `last_error` token format `"<RST_FnName>: <cause>"` consistent across Tasks 3, 5, 6, 7, 8. ✓

**Ordering:** Task 6 (schema + builder) MUST precede Task 7 (UDTFs consume `build_error_tile`). Tasks 1–5 are independent (heavy 1/2/3, light 4/5). Task 8 depends on all. Per subagent-driven-development, run implementers sequentially, not in parallel, even though the tiers are independent.
