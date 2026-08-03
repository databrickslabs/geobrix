# Heavy-tier v2 tile handling — Implementation Plan (two phases)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** (Phase 1) Tear out the dead heavy-tier (Scala) machinery — the v1 String path-tile mechanism, the checkpoint machinery, and the `evalPath`/`evalBinary` split — collapsing to a single `eval` per expression, WITHOUT changing tile shape or behavior. (Phase 2) Move the heavy tier to the v2 tile struct: accept v1 (binary) and v2 tiles in, always emit v2 materialized tiles, and raise a clear guard error when a virtual tile reaches heavy.

**Architecture:** Heavy `rst_*` expressions are `InvokedExpression`s whose `replacement` dispatches through `RST_ExpressionUtil.rstInvoke` to a companion `evalPath` (StringType path-tile) or `evalBinary` (bytes). Path-tiles and checkpointing are dead (only `evalBinary` + the checkpoint-`StringType`-output branch are ever exercised on non-dead paths, and checkpoint output is opt-in-only). Phase 1 removes the dead half and collapses `evalBinary`→`eval`, deleting `rstInvoke`, `CheckpointManager`/`CheckpointCleaner`/`CleanupListener`, `RasterDriver.write`, and the checkpoint config — a behavior-preserving refactor (tile stays the v1 3-field binary struct). Phase 2 then makes the two serde chokepoints (`RasterSerializationUtil` deserialize/serialize + `RST_ExpressionUtil.tileDataType`) layout-aware: read v1 (3-field) or v2 (8-field) by `numFields`, guard virtual tiles, and always emit the v2 8-field struct matching light's `V2_TILE_SCHEMA`.

**Tech Stack:** Scala 2.13.16, Spark 4.0.0, GDAL Java bindings. Python heavy tier (`rasterx/functions.py`) is a pure passthrough — Phase 1 does not touch it; Phase 2 touches only three stale docstrings. Tests via `gbx:test:scala` (single: `--suite 'com...'`; multiple: `--suites 'A,B'`) inside the `geobrix-dev` Docker container; lint via `gbx:lint:scalastyle`. Both are long-running — dispatch to subagents, never inline.

## Global Constraints

- **Phase 1 is behavior-preserving.** The heavy tile stays the v1 3-field binary struct `(cellid: Long(nn), raster: Binary(nn), metadata: Map(null))`. No tile-shape change, no functional change — only dead-code removal + the `eval` collapse. Gate: all existing heavy suites pass (minus the deleted checkpoint tests).
- **Phase 2 is the functional change.** Heavy accepts v1 (binary) AND v2 tiles in; emits ONLY the v2 8-field materialized struct; a virtual tile (v2, raster null + path set) raises a clear materialize-first error.
- **The v2 struct heavy emits MUST byte-for-byte match light's `V2_TILE_SCHEMA`** (`python/geobrix/src/databricks/labs/gbx/pyrx/core/virtual_tile.py:34`): `cellid: Long(nn), raster: Binary(null), path: String(null), window: struct<col_off:Int(nn),row_off:Int(nn),width:Int(nn),height:Int(nn)>(null), clip_polygon: Binary(null), clip_crs: String(null), crs: String(null), metadata: Map(null)`.
- **Materialized-output pedigree = null** (`path`/`window`/`clip_polygon`/`clip_crs`/`crs` all null on heavy output): the raster bytes are self-describing (consistent with light's pixel-producers). Op-specific pedigree stamping is out of scope.
- **Path-tiles are removed, source-file paths are NOT.** A path-*tile* (a tile whose `raster` field holds a String path) is dead. A source-*file* path read by the data-source READER at ingest (`GDAL_Reader`, `NetCDF_Batch`, VRT open in `PixelCombineRasters`) or by `rst_fromfile` (reads a source file → produces a bytes-tile) STAYS. The distinction — "open a raster from a path stored in a tile" (DEAD) vs "read a source file from a path in the reader/constructor" (KEEP) — governs every deletion. Do not delete `RasterDriver.read(path)`, `copyToLocal`, `cleanPath`, `isLocal`, `NodeFileManager.readRemote`, or `HadoopUtils.copyToPath` — they have live reader callers.
- **No new registered functions;** binding parity unchanged. No change to any function's registered name or SQL signature. The Phase-2 output-schema change is not a signature change.
- **Layout detection is by `row.numFields()`:** 3 → v1, 8 → v2, else → clear error. Field names are fixed by the two canonical schemas, so ordinal-by-layout == by-name.
- **Config keys removed without deprecation cycle:** `spark.databricks.labs.gbx.raster.use.checkpoint` and `spark.databricks.labs.gbx.raster.checkpoint.dir` have no user-facing docs (internal planning specs only) and are read only into dead storage — safe to delete.

---

## File Structure

**Phase 1 (Scala only):**
- `src/main/scala/com/databricks/labs/gbx/expressions/InvokedExpression.scala` — delete `rstInvoke` (`:44-49`).
- ~103 expression files under `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/**` — rename companion `evalBinary`→`eval`, delete `evalPath` (138 defs), change `replacement` to `invoke(companion)`, drop now-unused per-expression `private def rasterType`. `RST_Convolve` (`:33-39`) collapses its 4 typed `evalPath*` + StringType match arms.
- `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` — `rowToTile`/`rowToDS` binary-only (delete StringType path-open arms `:28-37`,`:43-51`); `tileToRow` binary-only, delete StringType/checkpoint branch (`:72-101`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala` — simplify `rasterType` (`:21`) to reject StringType at analysis; drop `CheckpointManager.init` call in `init` (`:92`); prune now-unused imports.
- `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/CheckpointManager.scala` — DELETE whole file.
- `src/main/scala/com/databricks/labs/gbx/rasterx/util/CheckpointCleaner.scala` — DELETE whole file.
- `src/main/scala/com/databricks/labs/gbx/rasterx/util/CleanupListener.scala` — DELETE whole file.
- `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala` — remove `CheckpointManager.init` (`:40`) + `addSparkListener(new CleanupListener(...))` (`:41`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/RasterDriver.scala` — DELETE `write` method (`:113`).
- `src/main/scala/com/databricks/labs/gbx/expressions/ExpressionConfig.scala` — DELETE `useCheckpoint` (`:49-51`) + `getRasterCheckpointDir` (`:44-46`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/GDALManager.scala` — DELETE `checkpointPath`/`useCheckpoint` vars (`:28-29`) + their assignments (`:123-124`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala` — `contentKey` (`:125-126`) → `getBinary(1)` only; drop StringType import (`:13`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala` — `toBinaryTileRow` (`:64-82`) → binary arm only.
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/generators/RST_MakeTiles.scala` — stale checkpoint comment (`:16-19`).
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/dem/RST_DEMProcessingHelper.scala` — stale comment (`:37`).
- `src/test/scala/com/databricks/labs/gbx/expressions/CoreClassesTest.scala` — DELETE 3 checkpoint tests (`:60-80`).
- New: `src/test/scala/com/databricks/labs/gbx/rasterx/util/NoDeadTileCodeTest.scala` — dead-symbol grep guard.

**Phase 2 (Scala + Python docstrings):**
- `RasterSerializationUtil.scala` — layout-aware `rowToTile`/`rowToDS` + virtual guard; `tileToRow` → v2 8-field; `arrayToTiles` layout-aware.
- `RST_ExpressionUtil.scala` — `tileDataType` → v2 8-field `v2TileType`.
- `RST_ErrorHandler.scala` (`:64,74`), `RST_FromBandsAgg.scala` (`:126,153`) — layout-aware struct reads.
- `docs/docs/api/execution-tiers.mdx`, `docs/docs/_partials/_virtual-tile-overrides.mdx` — tier tile model.
- `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` (`:569,1219,1285`) — `source`→`cellid` + v2 fields in docstrings.
- `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py` (`:3`) — docstring: both tiers share v2 schema.
- Tests: extend `RST_ExpressionEvalTest`, `RST_FromBandsAggTest`; new `RST_V2RoundTripTest`, `RasterSerializationV2Test`.

---

# PHASE 1 — Teardown / streamline (behavior-preserving)

### Task 1: Collapse `evalPath`/`evalBinary` → single `eval`; delete `rstInvoke`

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/expressions/InvokedExpression.scala:44-49`
- Modify: ~103 expression files under `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/**` (every file with a `def evalPath`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_Convolve.scala` (typed variants)

**Interfaces:**
- Produces: each raster expression companion has a single `def eval(...)` entrypoint (its former `evalBinary` body); `evalPath` gone; `replacement` = `invoke(companion)`.
- Consumes: `InvokedExpression.invoke(companion, methodName="eval", nonFoldable=false)` (existing; `"eval"` is the default methodName).

- [ ] **Step 1: Establish the baseline (all heavy suites green before refactor)**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*' --log p1-baseline.log`
Expected: PASS. Record pass counts — Phase 1 must not change them (minus Task 3's deleted checkpoint tests).

- [ ] **Step 2: Sweep each expression companion**

For every file matching `grep -rl "def evalPath" src/main/scala`:
- Rename the companion's `def evalBinary(...)` to `def eval(...)` (keep its body verbatim — it already hardcodes `BinaryType` where it calls a shared private helper; leave that helper).
- Delete the `def evalPath(...)` method(s).
- In the case class, change `override def replacement: Expression = rstInvoke(<Companion>, rasterType)` to `override def replacement: Expression = invoke(<Companion>)`.
- Delete the now-unused `private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)` from that expression IF it is only referenced by the old `rstInvoke` call (it usually is; `dataType` uses `tileDataType(tileExpr)` which computes raster type internally). If `rasterType` is still referenced elsewhere in the file, leave it.

`RST_Convolve.scala` special-case: its `replacement` matches on `(rasterType, outputType)` and dispatches to `evalPathDouble/Int/Float/Long` vs `evalBinaryDouble/Int/Float/Long` (`:33-39`). Collapse: rename each `evalBinaryXxx`→`evalXxx`, delete each `evalPathXxx`, and simplify the match to dispatch on `outputType` only (no StringType arm) → `invoke(RST_Convolve, "eval" + typeSuffix)`.

- [ ] **Step 3: Delete `rstInvoke`**

In `InvokedExpression.scala`, delete the entire `rstInvoke` method (`:43-49`). Nothing should reference it after Step 2.

- [ ] **Step 4: Compile + full heavy suite (the correctness gate for a no-behavior-change sweep)**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*' --log p1-eval-collapse.log`
Expected: same pass count as Step 1 baseline. A compile error means a missed `evalPath`/`rstInvoke`/`rasterType` reference — fix and rerun. (No new tests here: this is a behavior-preserving rename; the existing suites ARE the test.)

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala/com/databricks/labs/gbx/
git commit -m "refactor(rasterx): collapse evalPath/evalBinary to a single eval

Path-tiles are dead, so rstInvoke only ever dispatched to evalBinary.
Rename each expression companion's evalBinary to eval, delete all
evalPath methods, point replacement at invoke(companion), and delete
rstInvoke. Behavior-preserving: the tile stays the v1 binary struct.

Co-authored-by: Isaac"
```

---

### Task 2: Strip StringType from the serde chokepoints; reject path-tiles at analysis

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala:24-103`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala:21`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_TileStructureTest.scala`

**Interfaces:**
- `rowToTile`/`rowToDS` open bytes only (binary raster field); `tileToRow` emits the v1 3-field binary row only (no checkpoint branch). Signatures unchanged (callers untouched).
- `RST_ExpressionUtil.rasterType(tileExpr)` returns `BinaryType`; throws a clear error if the raster field is `StringType` (path-tile deprecation).

- [ ] **Step 1: Write the failing test (analysis rejects a StringType raster field)**

Add to `RST_TileStructureTest.scala`:
```scala
test("rasterType rejects a v1 String path-tile schema") {
    val v1path = StructType(Seq(
        StructField("cellid", LongType, nullable = false),
        StructField("raster", StringType, nullable = false),
        StructField("metadata", MapType(StringType, StringType), nullable = true)))
    val ex = intercept[Exception](
        RST_ExpressionUtil.rasterType(BoundReference(0, v1path, nullable = true)))
    assert(ex.getMessage.toLowerCase.contains("path-tile") ||
           ex.getMessage.toLowerCase.contains("materialize"))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_TileStructureTest'`
Expected: FAIL — `rasterType` returns `fields(1).dataType` (StringType) without rejecting.

- [ ] **Step 3: Implement — binary-only serde + analysis rejection**

In `RST_ExpressionUtil.rasterType` (`:21`):
```scala
def rasterType(tileExpr: Expression): DataType = {
    val rdt = tileExpr.dataType.asInstanceOf[StructType].fields(1).dataType
    rdt match {
        case StringType => throw new IllegalArgumentException(
            "Raster path-tiles (raster field as a String path) are no longer supported by the " +
            "heavyweight tier. Materialize the raster to bytes in the lightweight tier " +
            "(materialize=True, or write + read back) before passing it to a heavyweight function.")
        case other => other
    }
}
```
Apply the same guard in `arrayOfTileRasterType` (`:47-63`) on its returned `fields(1).dataType`.

In `RasterSerializationUtil`: delete the `rasterDT match { case StringType => ... }` arms in `rowToTile` (`:28-37`) and `rowToDS` (`:43-51`) — keep only the `BinaryType` path (`getBinary(1)` → `RasterDriver.readFromBytes`). Delete the entire `case StringType =>` checkpoint branch of `tileToRow` (`:72-101`); `tileToRow` now always builds the 3-field binary row (the former `BinaryType` branch, `:57-71`). The `rasterDT`/`dataType` params stay in signatures but are no longer matched on; if scalastyle flags them unused, keep them (signature compatibility with ~80 callers) and add a `// retained for signature compat` note. Prune now-unused imports (`CheckpointManager`, `GDAL`, `TaskContext`, `UTF8String`, `Try`) if the deletions orphan them.

- [ ] **Step 4: Run to verify pass + no regression**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_TileStructureTest' --log p1-serde.log`
Then the eval suites: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest'`
Expected: PASS; eval suites unchanged (they use binary tiles).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_TileStructureTest.scala
git commit -m "refactor(rasterx): binary-only tile serde; reject path-tiles at analysis

rowToTile/rowToDS/tileToRow drop their StringType (path-tile /
checkpoint) branches; rasterType rejects a StringType raster field with
a clear materialize-first error. Tile stays the v1 3-field binary struct.

Co-authored-by: Isaac"
```

---

### Task 3: Delete the checkpoint machinery wholesale

**Files:**
- DELETE: `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/CheckpointManager.scala`
- DELETE: `src/main/scala/com/databricks/labs/gbx/rasterx/util/CheckpointCleaner.scala`
- DELETE: `src/main/scala/com/databricks/labs/gbx/rasterx/util/CleanupListener.scala`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala:40-41`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala:92`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/RasterDriver.scala:113` (delete `write`)
- Modify: `src/main/scala/com/databricks/labs/gbx/expressions/ExpressionConfig.scala:44-51`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/gdal/GDALManager.scala:28-29,123-124`
- Modify: `src/test/scala/com/databricks/labs/gbx/expressions/CoreClassesTest.scala:60-80`

**Interfaces:** none survive (all deletions). `functions.register()` no longer initializes checkpointing or registers the cleanup listener.

- [ ] **Step 1: Delete the three checkpoint files and their references**

Delete `CheckpointManager.scala`, `CheckpointCleaner.scala`, `CleanupListener.scala`. In `functions.scala`, remove the `CheckpointManager.init(...)` call (`:40`) and the `spark.sparkContext.addSparkListener(new CleanupListener(spark))` line (`:41`). In `RST_ExpressionUtil.init` (`:92`), remove the `CheckpointManager.init(exprConfig)` call. Remove the now-orphaned `CheckpointManager` import from `RST_ExpressionUtil.scala` (top-of-file imports).

- [ ] **Step 2: Delete `RasterDriver.write` and the checkpoint config**

In `RasterDriver.scala`, delete the `write` method (`:113`, its only caller was the deleted `tileToRow` StringType branch). In `ExpressionConfig.scala`, delete `getRasterCheckpointDir` (`:44-46`) and `useCheckpoint` (`:49-51`). In `GDALManager.scala`, delete the `checkpointPath`/`useCheckpoint` vars (`:28-29`) and their assignments in `configureGDAL` (`:123-124`).

- [ ] **Step 3: Delete the checkpoint tests**

In `CoreClassesTest.scala`, delete the three tests at `:60-80` (`"...raster checkpoint directory"`, `"...default checkpoint directory..."`, `"...parse useCheckpoint flag"`).

- [ ] **Step 4: Compile + affected suites**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.expressions.CoreClassesTest,com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest' --log p1-checkpoint.log`
Expected: PASS. A compile error = a missed reference to a deleted symbol — grep `CheckpointManager|CheckpointCleaner|CleanupListener|getRasterCheckpointDir|useCheckpoint|RasterDriver.write` in `src/` and fix.

- [ ] **Step 5: Commit**

```bash
git add -A src/main/scala/com/databricks/labs/gbx/ src/test/scala/com/databricks/labs/gbx/expressions/CoreClassesTest.scala
git commit -m "refactor(rasterx): delete dead checkpoint machinery

Remove CheckpointManager, CheckpointCleaner, CleanupListener,
RasterDriver.write, ExpressionConfig.useCheckpoint/getRasterCheckpointDir,
the GDALManager checkpoint vars, the functions.register listener
registration, and the checkpoint config keys. All were reachable only via
the removed path-tile/checkpoint-output branches. Deletes the 3
checkpoint tests in CoreClassesTest.

Co-authored-by: Isaac"
```

---

### Task 4: Collapse residual StringType arms + stale comments

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala:13,125-126`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala:64-82`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/generators/RST_MakeTiles.scala:16-19`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/dem/RST_DEMProcessingHelper.scala:37`
- Test: covered by existing `RST_AggEvalTest` / `RST_FromBandsAggTest`

**Interfaces:** internal simplifications; no signature change.

- [ ] **Step 1: Simplify `RST_MergeAgg.contentKey`**

`contentKey` (`:125-126`) currently matches raster type: `case StringType => row.getString(1)...; case BinaryType => row.getBinary(1)`. Raster is always binary now → collapse to `row.getBinary(1)`. Remove the `StringType` import (`:13`) if now unused.

- [ ] **Step 2: Simplify `RST_FromBandsAgg.toBinaryTileRow`**

`toBinaryTileRow` (`:64-82`) has a `case _ =>` arm that opens a non-BinaryType (path) tile and re-encodes to bytes. Raster is always binary now → the method collapses to the BinaryType arm (copy the tile row through). Keep behavior identical for binary input.

- [ ] **Step 3: Remove stale checkpoint comments**

`RST_MakeTiles.scala:16-19` (historical checkpoint comment) and `RST_DEMProcessingHelper.scala:37` ("byte payload or a checkpoint path") — update/remove so they describe binary-only behavior.

- [ ] **Step 4: Run the aggregation suites**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.RST_AggEvalTest,com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest' --log p1-aggs.log`
Expected: PASS (unchanged — binary path was always the live one).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/generators/RST_MakeTiles.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/dem/RST_DEMProcessingHelper.scala
git commit -m "refactor(rasterx): collapse residual StringType tile arms + stale comments

RST_MergeAgg.contentKey and RST_FromBandsAgg.toBinaryTileRow drop their
path-tile arms (raster is always binary); remove stale checkpoint/path
comments in RST_MakeTiles and RST_DEMProcessingHelper.

Co-authored-by: Isaac"
```

---

### Task 5: Dead-symbol guard + Phase-1 gate

**Files:**
- Test: new `src/test/scala/com/databricks/labs/gbx/rasterx/util/NoDeadTileCodeTest.scala`

**Interfaces:** none.

- [ ] **Step 1: Write the dead-symbol grep guard**

```scala
package com.databricks.labs.gbx.rasterx.util
import org.scalatest.funsuite.AnyFunSuite
class NoDeadTileCodeTest extends AnyFunSuite {
    private def scan(dir: String): String = {
        val root = new java.io.File(dir)
        def files(f: java.io.File): Seq[java.io.File] =
            if (f.isDirectory) f.listFiles.toSeq.flatMap(files)
            else if (f.getName.endsWith(".scala")) Seq(f) else Nil
        files(root).map(f => scala.io.Source.fromFile(f).mkString).mkString("\n")
    }
    test("no dead path-tile / checkpoint symbols remain in heavy src") {
        val src = scan("src/main/scala/com/databricks/labs/gbx")
        val banned = Seq("def evalPath", "rstInvoke", "CheckpointManager",
            "CheckpointCleaner", "CleanupListener", "getRasterCheckpointDir",
            "use.checkpoint")
        val hits = banned.filter(src.contains)
        assert(hits.isEmpty, s"Dead symbols still present: $hits")
    }
}
```
(`RasterDriver.write` is intentionally NOT in the banned list as a bare string — `write` appears in other contexts; the compile in Task 3 already proves it's gone. If a precise check is wanted, grep `def write\b` in `RasterDriver.scala` only.)

- [ ] **Step 2: Run the guard**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.NoDeadTileCodeTest'`
Expected: PASS. Any hit = an incomplete deletion from Tasks 1-4; fix before proceeding.

- [ ] **Step 3: Full heavy suite + scalastyle (Phase-1 gate)**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*' --log p1-gate.log`
Then: `gbx:lint:scalastyle`
Expected: full suite passes at the Task-1 baseline count minus the 3 deleted checkpoint tests; scalastyle clean (fix unused imports orphaned by the deletions).

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/util/NoDeadTileCodeTest.scala
git commit -m "test(rasterx): guard against dead path-tile/checkpoint symbols

Co-authored-by: Isaac"
```

---

# PHASE 2 — v2 tile support (functional change on clean code)

### Task 6: Layout-aware deserialize + virtual-tile guard

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` (`rowToTile`/`rowToDS`)
- Test: new `src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala`

**Interfaces:** `rowToTile`/`rowToDS` resolve layout from `row.numFields()` (3=v1, 8=v2), read metadata at the resolved ordinal, and throw a clear guard on a virtual tile. Signatures unchanged.

- [ ] **Step 1: Write the failing tests (AnyFunSuite + GDAL init, like RST_FromBandsAggTest)**

Create `RasterSerializationV2Test.scala` with a `/vsimem` GeoTIFF→bytes helper (copy `RST_FromBandsAggTest.makeSingleBandTileRow` pattern). Tests + row builders:
```scala
private def emptyMap = ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])
private def v1BinaryRow(cellid: Long, bytes: Array[Byte]) =
    new GenericInternalRow(Array[Any](cellid, bytes, emptyMap))
private def v2MaterializedRow(cellid: Long, bytes: Array[Byte], md: MapData) =
    new GenericInternalRow(Array[Any](cellid, bytes, null, null, null, null, null, md))
private def v2VirtualRow(cellid: Long, path: String) =
    new GenericInternalRow(Array[Any](cellid, null, UTF8String.fromString(path), null, null, null, null, emptyMap))

test("rowToTile reads a v1 (3-field) binary tile") {
    val (cell, ds, _) = RasterSerializationUtil.rowToTile(v1BinaryRow(7L, tinyGeotiff()), BinaryType)
    assert(cell == 7L && ds.GetRasterXSize() > 0); RasterDriver.releaseDataset(ds)
}
test("rowToTile reads a v2 (8-field) materialized tile; metadata at position 7") {
    val (cell, ds, meta) = RasterSerializationUtil.rowToTile(
        v2MaterializedRow(9L, tinyGeotiff(), toMapData(Map("k"->"v"))), BinaryType)
    assert(cell == 9L && meta.get("k").contains("v")); RasterDriver.releaseDataset(ds)
}
test("rowToTile on a VIRTUAL v2 tile throws the materialize-first guard") {
    val ex = intercept[IllegalArgumentException](
        RasterSerializationUtil.rowToTile(v2VirtualRow(1L, "/Volumes/x/y.tif"), BinaryType))
    assert(ex.getMessage.contains("virtual tile"))
    assert(ex.getMessage.toLowerCase.contains("materialize"))
    assert(ex.getMessage.toLowerCase.contains("lightweight"))
}
test("rowToTile on an unrecognized field count throws a clear error") {
    val ex = intercept[IllegalArgumentException](RasterSerializationUtil.rowToTile(
        new GenericInternalRow(Array[Any](1L, Array.emptyByteArray)), BinaryType))
    assert(ex.getMessage.contains("2"))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: FAIL — post-Phase-1 `rowToTile` reads metadata at fixed position 2 (crashes on the v2 row) and has no virtual guard.

- [ ] **Step 3: Implement layout resolution + guard**

Add to `RasterSerializationUtil`:
```scala
private case class TileLayout(cellid: Int, raster: Int, metadata: Int, path: Option[Int], isV2: Boolean)
private def tileLayout(row: InternalRow): TileLayout = row.numFields match {
    case 3 => TileLayout(0, 1, 2, None, isV2 = false)
    case 8 => TileLayout(0, 1, 7, Some(2), isV2 = true)
    case n => throw new IllegalArgumentException(
        s"Unrecognized raster tile struct: expected a v1 (3-field) or v2 (8-field) tile, got $n fields.")
}
private def guardMaterialized(row: InternalRow, lyt: TileLayout): Unit =
    if (lyt.isV2 && row.isNullAt(lyt.raster) && lyt.path.exists(p => !row.isNullAt(p))) {
        val path = lyt.path.map(row.getString).getOrElse("<unknown>")
        throw new IllegalArgumentException(
            s"Heavyweight rst_* received a VIRTUAL tile (raster is null, path=$path). The " +
            "heavyweight tier operates only on materialized (binary) tiles. Materialize it in the " +
            "lightweight tier first — call the lightweight rst_* with materialize=True, or write it " +
            "out and read it back — then pass the result to the heavyweight function. See Execution " +
            "Tiers: light→heavy bridge.")
    }
```
Rewrite `rowToTile`/`rowToDS` to `val lyt = tileLayout(row); guardMaterialized(row, lyt)`, then read `row.getLong(lyt.cellid)`, `row.getMap(lyt.metadata)`, `row.getBinary(lyt.raster)` → `RasterDriver.readFromBytes`.

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: PASS (4/4).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala
git commit -m "feat(rasterx): layout-aware tile deserialize (v1/v2) + virtual guard

rowToTile/rowToDS resolve v1 (3-field) vs v2 (8-field) by numFields and
read metadata at the resolved ordinal; a v2 virtual tile (raster null +
path) raises a clear materialize-first error.

Co-authored-by: Isaac"
```

---

### Task 7: Always emit the v2 tile struct

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala` (`tileDataType`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` (`tileToRow`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala`

**Interfaces:** `RST_ExpressionUtil.v2TileType: StructType` (canonical 8-field, matching light); `tileDataType(...)` returns it; `tileToRow(...)` emits an 8-field materialized row (raster bytes, null pedigree). Signatures unchanged.

> **Note:** the two edits land together — declaring `dataType`=8-field while `tileToRow` emits 3 fields is a broken intermediate that fails at row assembly.

- [ ] **Step 1: Write the failing tests**

Add to `RasterSerializationV2Test.scala`:
```scala
test("v2TileType matches the light V2 schema exactly") {
    val t = RST_ExpressionUtil.v2TileType
    assert(t.fieldNames.toSeq == Seq("cellid","raster","path","window","clip_polygon","clip_crs","crs","metadata"))
    assert(t("cellid").dataType == LongType && !t("cellid").nullable)
    assert(t("raster").dataType == BinaryType && t("raster").nullable)
    val w = t("window").dataType.asInstanceOf[StructType]
    assert(w.fieldNames.toSeq == Seq("col_off","row_off","width","height"))
    assert(t("metadata").dataType == MapType(StringType, StringType))
}
test("tileToRow emits an 8-field v2 materialized row with null pedigree") {
    val ds = openTinyGeotiff()
    val row = RasterSerializationUtil.tileToRow((5L, ds, Map("d"->"GTiff")), BinaryType, hconf)
    assert(row.numFields == 8 && row.getLong(0) == 5L && !row.isNullAt(1))
    assert(row.isNullAt(2) && row.isNullAt(3) && row.isNullAt(4) && row.isNullAt(5) && row.isNullAt(6))
    assert(!row.isNullAt(7)); RasterDriver.releaseDataset(ds)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: FAIL — `tileDataType` builds 3 fields; `tileToRow` emits 3.

- [ ] **Step 3: Implement v2 schema + v2 output**

In `RST_ExpressionUtil` add `windowType` + `v2TileType` (exact fields per Global Constraints; `org.apache.spark.sql.types._` already imported covers `IntegerType`), and make both `tileDataType` overloads `= v2TileType`. In `RasterSerializationUtil.tileToRow`, emit 8 fields:
```scala
InternalRow.fromSeq(Seq(tuple._1, bytes, null, null, null, null, null, metadata))
```
(raster bytes at 1, pedigree null, metadata at 7). `dataType`/`hconf` params retained for signature compat.

- [ ] **Step 4: Run to verify pass + eval suites still green**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test,com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest' --log p2-v2out.log`
Expected: PASS (eval suites now see v2 output; if any asserts the old 3-field shape, update it to v2).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala
git commit -m "feat(rasterx): heavy emits the v2 tile struct (null pedigree)

tileDataType returns the canonical 8-field v2 schema (matching the light
V2_TILE_SCHEMA); tileToRow emits an 8-field materialized row with raster
bytes and null pedigree (bytes self-describe).

Co-authored-by: Isaac"
```

---

### Task 8: Array + aggregator layout-aware reads

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` (`arrayToTiles`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/util/RST_ErrorHandler.scala:64,74`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala:126,153`
- Modify callers of `arrayToTiles`: `RST_CombineAvg.scala:44`, `RST_MapAlgebra.scala:44`, `RST_Merge.scala:44`, `constructor/RST_FromBands.scala:44`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAggTest.scala`, `RST_ErrorHandlerTest.scala`

**Interfaces:** array-of-tiles and agg-buffer struct reads use the element schema's field count (not hardcoded `3`).

- [ ] **Step 1: Write the failing tests**

In `RST_FromBandsAggTest`, add `makeSingleBandTileRowV2` (8-field) and a test that `update`/`merge`/`eval` assembles the correct multi-band result from v2 inputs (mirror the existing v1 assertions on band count/order). In `RST_ErrorHandlerTest`, add a v2 (8-field) array-of-tiles scan case asserting no truncation/crash.

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest,com.databricks.labs.gbx.rasterx.util.RST_ErrorHandlerTest'`
Expected: FAIL — `getStruct(_, 3)` truncates the 8-field element.

- [ ] **Step 3: Implement layout-aware struct reads**

Thread the element field count from the input `ArrayType(StructType(fields))` (available via `arrayOfTileRasterType`/the child `dataType`) into `arrayToTiles` and `RST_ErrorHandler` (replace literal `3` with `elementFields`). Update the 4 `arrayToTiles` callers to pass the element struct's field count. In `RST_FromBandsAgg`, replace `getStruct(1, 3)` (`:126,153`) with `getStruct(1, tileFieldCount)` where `tileFieldCount` derives from the agg's input tile element schema; `getBinary(1)` (`:184`) is layout-independent (raster at position 1 in both) — leave it.

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest,com.databricks.labs.gbx.rasterx.util.RST_ErrorHandlerTest,com.databricks.labs.gbx.rasterx.expressions.RST_AggEvalTest' --log p2-arrays.log`
Expected: PASS (v1 + new v2 cases).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/util/RST_ErrorHandler.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_CombineAvg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_MapAlgebra.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_Merge.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/constructor/RST_FromBands.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAggTest.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RST_ErrorHandlerTest.scala
git commit -m "feat(rasterx): array + aggregator tile reads are layout-aware (v1/v2)

arrayToTiles, RST_ErrorHandler, and RST_FromBandsAgg read each tile
element with the element schema's field count instead of a hardcoded 3.

Co-authored-by: Isaac"
```

---

### Task 9: End-to-end eval + light↔heavy parity

**Files:**
- Test: new `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_V2RoundTripTest.scala` (PlanTest with SilentSparkSession)

**Interfaces:** none (integration).

- [ ] **Step 1: Write the e2e tests**

```scala
test("heavy rst_* consumes a v1 binary tile and emits a v2 tile") {
    val df = spark.read.format("binaryFile").load(tifPath)
        .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
    val out = df.select(rst_clip(col("tile"), lit(wktPolygon)).as("t"))
    val s = out.schema("t").dataType.asInstanceOf[StructType]
    assert(s.fieldNames.toSeq == Seq("cellid","raster","path","window","clip_polygon","clip_crs","crs","metadata"))
    assert(out.head.getAs[Row]("t").getAs[Array[Byte]]("raster") != null)
}
test("heavy rst_* on a VIRTUAL tile raises the materialize-first error") {
    // build a v2 virtual tile DataFrame (raster null, path set) and run rst_boundingbox
    val ex = intercept[Exception](dfVirtual.select(rst_boundingbox(col("tile"))).collect())
    assert(ex.getMessage.toLowerCase.contains("materialize") ||
           Option(ex.getCause).exists(_.getMessage.toLowerCase.contains("materialize")))
}
test("heavy v2 output schema equals the light V2_TILE_SCHEMA field-for-field") {
    val heavyOut = /* schema of a heavy rst_* tile output */
    // assert names+types+nullability equal RST_ExpressionUtil.v2TileType
}
```
Add a parity check: a heavy-emitted v2 tile DataFrame `.unionByName` with a v2-shaped DataFrame succeeds (schemas compatible).

- [ ] **Step 2: Run**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.RST_V2RoundTripTest,com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest' --log p2-e2e.log`
Expected: PASS. (Virtual-tile error may surface wrapped in a `SparkException`; assert on the cause chain.)

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_V2RoundTripTest.scala
git commit -m "test(rasterx): heavy v1-in/v2-out, virtual-guard, light-heavy parity

Co-authored-by: Isaac"
```

---

### Task 10: Docs + Python docstring alignment + Phase-2 gate

**Files:**
- Modify: `docs/docs/api/execution-tiers.mdx`, `docs/docs/_partials/_virtual-tile-overrides.mdx`
- Modify: `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py:569,1219,1285`
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py:3`
- Test: extend `NoDeadTileCodeTest` (or a Phase-2 guard) with positional-read bans

- [ ] **Step 1: Docs — tier tile model**

In `execution-tiers.mdx` and `_virtual-tile-overrides.mdx`, state prominently (voice-clean): **"The lightweight tier is for light (virtual) raster tiles; the heavyweight tier is for heavy (binary) raster tiles."** Add: heavy accepts both v1 and v2 **materialized** tiles in and always emits the v2 tile struct; a **virtual** tile passed to a heavyweight function raises a clear error (materialize in the lightweight tier first — `materialize=True`, or write + read back); raster **path-tiles** are no longer supported by the heavyweight tier.

- [ ] **Step 2: Python docstrings**

In `rasterx/functions.py`, fix the return-type docstrings at `:569,1219,1285`: replace `source` with `cellid` and describe the v2 fields (`cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata`). In `pyrx/_serde.py:3`, update the "mirrors the heavyweight rasterx tile exactly: (cellid, raster, metadata)" docstring to note both tiers now share the v2 `V2_TILE_SCHEMA` (materialized = raster bytes; the extra fields are null for a plain materialized tile). Add a one-line note near `rst_fromfile` (`rasterx/functions.py:636`) clarifying its `path` arg is a **source file** path (read to produce a bytes-tile), NOT a path-tile — so it is unaffected by path-tile removal.

- [ ] **Step 3: Widen the dead-symbol guard with positional bans**

Extend `NoDeadTileCodeTest` (or add a case): assert no `getMap(2)` and no `getStruct(_, 3)` on tile rows survive outside `RasterSerializationUtil.scala`. (Scope the grep to the raster expression tree; if a legitimate non-tile `getStruct(_,3)` exists, refine the pattern to the tile context.)

- [ ] **Step 4: Voice guard + Phase-2 gate**

Run:
```bash
grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/ ; echo "exit:$?"
```
Expected: no matches.
Then the full gate: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*' --log p2-gate.log` and `gbx:lint:scalastyle`. Also run the Python lint if the docstring edits touch formatting: `gbx:lint:python --check`.
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add docs/docs/api/execution-tiers.mdx docs/docs/_partials/_virtual-tile-overrides.mdx python/geobrix/src/databricks/labs/gbx/rasterx/functions.py python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py src/test/scala/com/databricks/labs/gbx/rasterx/util/NoDeadTileCodeTest.scala
git commit -m "docs+test: document tier tile model; align Python docstrings; guard positional reads

The lightweight tier is for light (virtual) tiles; the heavyweight tier is
for heavy (binary) tiles — heavy takes v1/v2 materialized in, emits v2, and
raises a clear error on a virtual tile. Fixes stale rasterx docstrings
(source->cellid, v2 fields) and the _serde shared-schema note; widens the
guard test to ban positional tile reads outside the serde chokepoint.

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:**
- Full dead-code teardown (checkpoint machinery, path-tile, evalPath, rstInvoke, RasterDriver.write, config keys) → Phase 1 Tasks 1-4; guard Task 5. ✓
- `eval` collapse (user decision) → Task 1. ✓
- Two-phase structure (teardown first, then v2) (user decision) → Phase 1 / Phase 2. ✓
- Two chokepoints layout-aware + always-v2 → Tasks 6, 7. ✓
- Virtual guard with actionable message → Task 6 (+ e2e Task 9). ✓
- Accept v1+v2 in, emit v2 out → Tasks 6, 7, 9. ✓
- Array/aggregator positional sites → Task 8. ✓
- Pedigree null → Task 7. ✓
- Schema byte-match with light `V2_TILE_SCHEMA` → Task 7 + Task 9 parity. ✓
- Docs "light=virtual, heavy=binary" framing + Python docstring alignment + rst_fromfile note → Task 10. ✓
- KEEP vs DELETE discipline (source-file path reads stay) → Global Constraints + Task 2/3 anchors. ✓
- No new registered functions / binding parity → no task adds registrations. ✓

**Placeholder scan:** code steps carry real Scala/anchors; the eval-collapse sweep (Task 1) names the exact mechanical rule + the compile/guard as completeness proof; the "thread element field count from the input ArrayType" instruction (Task 8) names the exact source.

**Type consistency:** `v2TileType`/`windowType`/`TileLayout` names consistent; serde/`tileDataType`/`rowToTile` signatures preserved across both phases so the ~80 callers stay untouched. `IntegerType` covered by the existing `import org.apache.spark.sql.types._`.

**Sequencing:** Phase 1 is behavior-preserving (gate = existing suites still pass); Phase 2 builds on the cleaned code. Within Phase 1, Task 1 (eval collapse) → Task 2 (serde StringType strip) → Task 3 (checkpoint delete) → Task 4 (residual arms) → Task 5 (guard/gate). Within Phase 2, Task 6→7 must be reviewed as the core (7's two edits land together), 8 removes stragglers, 9 is e2e, 10 is docs+gate. Each task independently testable/reviewable.

**Phase boundary:** Phase 1 and Phase 2 are separable PRs if desired — Phase 1 leaves a fully-working v1 heavy tier; Phase 2 is the functional cutover. The plan runs them in sequence in one branch.
