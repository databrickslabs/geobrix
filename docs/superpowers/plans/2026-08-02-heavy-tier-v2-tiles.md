# Heavy-tier v2 tile handling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the heavy (Scala/rasterx) tier accept BOTH v1 and v2 tile inputs and always emit v2 materialized tiles, with a clear guard error when a virtual tile reaches heavy, via centralized serialize/deserialize chokepoints — and deprecate the v1 String path-tile + checkpoint output mechanism.

**Architecture:** All heavy `rst_*` route through four functions in `RasterSerializationUtil` (`rowToTile`, `rowToDS`, `tileToRow`, `arrayToTiles`) plus `RST_ExpressionUtil` (`rasterType`, `tileDataType`, `arrayOfTileRasterType`). Make those the chokepoints: layout-aware read (v1 3-field vs v2 8-field, detected by `InternalRow.numFields()`), central virtual-tile guard, always-v2 output. A handful of expressions read tile fields positionally outside these (`RST_ErrorHandler`, `RST_FromBandsAgg`, `RST_MergeAgg`, `arrayToTiles`); route them through the chokepoints too. Heavy never reads raster bytes from a path again.

**Tech Stack:** Scala 2.13.16, Spark 4.0.0, GDAL Java bindings. Tests via `gbx:test:scala` inside the `geobrix-dev` Docker container; lint via `gbx:lint:scalastyle`.

## Global Constraints

- **Heavy accepts v1 (binary) AND v2 tile inputs; emits ONLY v2 materialized tiles** (raster bytes present, 8-field struct).
- **Virtual tiles are lightweight-tier only.** A virtual tile (v2 with `raster` null + `path` set) reaching heavy raises a clear, actionable error naming the materialize-first remedy — never an opaque `ClassCastException`.
- **The v2 struct heavy emits MUST byte-for-byte match light's `V2_TILE_SCHEMA`** (field names, types, nullability), or DataFrames won't be union-compatible:
  ```
  cellid: LongType(nn), raster: BinaryType(null), path: StringType(null),
  window: struct<col_off:Int(nn),row_off:Int(nn),width:Int(nn),height:Int(nn)>(null),
  clip_polygon: BinaryType(null), clip_crs: StringType(null), crs: StringType(null),
  metadata: MapType(String,String)(null)
  ```
- **Materialized-output pedigree = null** (`path`/`window`/`clip_polygon`/`clip_crs`/`crs` all null on heavy output): the raster bytes are self-describing, consistent with light's pixel-producers. Op-specific pedigree stamping is out of scope (null is safe).
- **v1 String path-tile mechanism is deprecated/removed** (read notion #1 + checkpoint output notion #2). `metadata["path"]` (informational provenance) is kept.
- **No new registered functions;** binding parity unchanged. No change to any function's registered name or SQL signature.
- **Layout detection is by `row.numFields()`:** 3 → v1, 8 → v2, anything else → clear error. Field names are fixed by the two canonical schemas, so ordinal-by-layout == by-name here.

---

## File Structure

- `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala` — the deserialize/serialize chokepoints (`rowToTile`, `rowToDS`, `tileToRow`, `arrayToTiles`); virtual guard; always-v2 output; delete StringType/checkpoint branch.
- `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala` — `tileDataType` → v2 schema; `rasterType`/`arrayOfTileRasterType` reject v1 String path-tiles at analysis.
- `src/main/scala/com/databricks/labs/gbx/expressions/InvokedExpression.scala` — `rstInvoke` StringType branch → analysis error (path-tiles deprecated).
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/util/RST_ErrorHandler.scala` — `getStruct(i, 3)` → layout-aware.
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala` and `RST_MergeAgg.scala` — buffer/`contentKey` positional reads → layout-aware / binary-only.
- `src/main/scala/com/databricks/labs/gbx/rasterx/ds/gdal/GDAL_RowWriter.scala` — confirm tile sub-struct extraction handles v2.
- Tests: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RST_ErrorHandlerTest.scala`, `.../expressions/RST_TileStructureTest.scala`, `.../expressions/RST_ExpressionEvalTest.scala`, `.../expressions/agg/RST_FromBandsAggTest.scala`, plus a new `.../util/RasterSerializationV2Test.scala`.
- Docs: `docs/docs/api/execution-tiers.mdx`, `docs/docs/_partials/_virtual-tile-overrides.mdx`.

---

### Task 0: Confirm the checkpoint/StringType output branch is off the default path

**Files:** none modified (investigation + a note appended to the ledger/report).

**Interfaces:** none.

- [ ] **Step 1: Trace what sets a tile expression's `dataType`/`rasterType` to `StringType`**

Read `RST_ExpressionUtil.rasterType` (`RST_ExpressionUtil.scala:21`), `InvokedExpression.rstInvoke` (`InvokedExpression.scala:44-49`), `CheckpointManager.scala`, and `ExpressionConfig.useCheckpoint` (`ExpressionConfig.scala:49-51`). Confirm: (a) `useCheckpoint` defaults to `false`; (b) a StringType tile arises only when the INPUT tile's `fields(1).dataType` is StringType (i.e. a path-tile the user supplied), not from any default reader; (c) `rst_fromcontent` / `binaryFile` produce BinaryType tiles.

- [ ] **Step 2: Grep for any producer of a StringType tile on a default path**

Run:
```bash
grep -rn "StringType" src/main/scala/com/databricks/labs/gbx/rasterx/ds/ src/main/scala/com/databricks/labs/gbx/rasterx/expressions/constructor/
grep -rn "use.checkpoint\|useCheckpoint\|getCheckpointPath" src/main/scala/
```
Expected: the GDAL data source reader emits BinaryType tiles (`GDAL_Reader.scala` calls `tileToRow((-1L, tile._1, tile._2), BinaryType, hconf)`); no default path emits StringType.

- [ ] **Step 3: Record the finding**

If confirmed off-default (expected), record: "checkpoint/StringType output branch is opt-in only (requires a user-supplied StringType path-tile input; `useCheckpoint` default false); safe to remove." If it turns out to be on a default path, STOP and surface to the human before deletion (per spec). No commit (investigation only); the finding gates Task 3.

---

### Task 1: Central layout-aware deserialize + virtual guard (`RasterSerializationUtil.rowToTile` / `rowToDS`)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala:24-52`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala` (new)

**Interfaces:**
- Produces: private helpers `tileLayout(row: InternalRow): TileLayout` and the layout-aware field reads used by `rowToTile`/`rowToDS`. `TileLayout` resolves ordinals: v1 → `(cellid=0, raster=1, metadata=2)`; v2 → `(cellid=0, raster=1, path=2, window=3, clip_polygon=4, clip_crs=5, crs=6, metadata=7)`.
- Consumes: `InternalRow`, `RasterDriver.readFromBytes`.
- Signatures unchanged: `rowToTile(row, rasterDT)`, `rowToDS(row, rasterDT, shared)` keep their signatures (callers untouched) but internally resolve layout from `row.numFields()` and read metadata by the resolved ordinal.

- [ ] **Step 1: Write the failing unit tests (no Spark; AnyFunSuite + GDAL init, like RST_FromBandsAggTest)**

Create `RasterSerializationV2Test.scala`. Build tile rows with a small `/vsimem` GeoTIFF → bytes helper (copy the pattern from `RST_FromBandsAggTest.makeSingleBandTileRow`, lines 58-82). Tests:

```scala
test("rowToTile reads a v1 (3-field) binary tile") {
    val row = v1BinaryRow(cellid = 7L, bytes = tinyGeotiffBytes())
    val (cell, ds, meta) = RasterSerializationUtil.rowToTile(row, BinaryType)
    assert(cell == 7L)
    assert(ds != null && ds.GetRasterXSize() > 0)
    RasterDriver.releaseDataset(ds)
}

test("rowToTile reads a v2 (8-field) MATERIALIZED tile and finds metadata at position 7") {
    val row = v2MaterializedRow(cellid = 9L, bytes = tinyGeotiffBytes(),
                                metadata = Map("k" -> "v"))
    val (cell, ds, meta) = RasterSerializationUtil.rowToTile(row, BinaryType)
    assert(cell == 9L)
    assert(meta.get("k").contains("v"))   // metadata read from position 7, not 2
    RasterDriver.releaseDataset(ds)
}

test("rowToTile on a VIRTUAL v2 tile (raster null + path set) throws the guard error") {
    val row = v2VirtualRow(cellid = 1L, path = "/Volumes/x/y.tif")
    val ex = intercept[IllegalArgumentException](
        RasterSerializationUtil.rowToTile(row, BinaryType))
    assert(ex.getMessage.contains("virtual tile"))
    assert(ex.getMessage.toLowerCase.contains("materialize"))
    assert(ex.getMessage.contains("lightweight"))
}

test("rowToTile on an unrecognized field count throws a clear error") {
    val row = new GenericInternalRow(Array[Any](1L, Array.emptyByteArray))  // 2 fields
    val ex = intercept[IllegalArgumentException](
        RasterSerializationUtil.rowToTile(row, BinaryType))
    assert(ex.getMessage.contains("tile") && ex.getMessage.contains("2"))
}
```

Row builders in the test (exact):
```scala
private def v1BinaryRow(cellid: Long, bytes: Array[Byte]): InternalRow =
    new GenericInternalRow(Array[Any](cellid, bytes, emptyMap))
private def v2MaterializedRow(cellid: Long, bytes: Array[Byte], metadata: Map[String, String]): InternalRow =
    new GenericInternalRow(Array[Any](cellid, bytes, null, null, null, null, null, toMapData(metadata)))
private def v2VirtualRow(cellid: Long, path: String): InternalRow =
    new GenericInternalRow(Array[Any](cellid, null, UTF8String.fromString(path), null, null, null, null, emptyMap))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: FAIL — current `rowToTile` does `row.getMap(2)` (crashes on the v2 row: position 2 is `path`/null, not a map) and has no virtual guard.

- [ ] **Step 3: Implement the layout-aware read + guard**

In `RasterSerializationUtil.scala`, add above `rowToTile`:
```scala
private case class TileLayout(cellid: Int, raster: Int, metadata: Int,
                             path: Option[Int], isV2: Boolean)

private def tileLayout(row: InternalRow): TileLayout = row.numFields match {
    case 3 => TileLayout(0, 1, 2, None, isV2 = false)
    case 8 => TileLayout(0, 1, 7, Some(2), isV2 = true)
    case n => throw new IllegalArgumentException(
        s"Unrecognized raster tile struct: expected a v1 (3-field) or v2 (8-field) tile, got $n fields.")
}

/** Throws if this is a VIRTUAL tile (v2, raster null, path set) — heavy is materialized-only. */
private def guardMaterialized(row: InternalRow, lyt: TileLayout): Unit = {
    if (lyt.isV2 && row.isNullAt(lyt.raster) && lyt.path.exists(p => !row.isNullAt(p))) {
        val path = lyt.path.map(row.getString).getOrElse("<unknown>")
        throw new IllegalArgumentException(
            s"Heavyweight rst_* received a VIRTUAL tile (raster is null, path=$path). " +
            "The heavyweight tier operates only on materialized (binary) tiles. Materialize it " +
            "in the lightweight tier first — call the lightweight rst_* with materialize=True, or " +
            "write it out and read it back — then pass the result to the heavyweight function. " +
            "See Execution Tiers: light→heavy bridge.")
    }
}
```
Rewrite `rowToTile` and `rowToDS` to resolve `val lyt = tileLayout(row)`, call `guardMaterialized(row, lyt)`, read `cellID = row.getLong(lyt.cellid)`, `metadata` from `row.getMap(lyt.metadata)`, and open bytes from `row.getBinary(lyt.raster)`. **Remove the `rasterDT match { StringType => getString(1) … }` branch** — heavy opens bytes only (path-tile deprecation; the StringType input is rejected upstream at analysis in Task 4, but defensively, a StringType `rasterDT` here should also throw "path-tiles are no longer supported — materialize to bytes in the lightweight tier"). Keep the `rasterDT` parameter for signature compatibility; use it only to detect+reject the deprecated StringType case.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: PASS (4/4).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala
git commit -m "feat(rasterx): layout-aware tile deserialize + virtual-tile guard

rowToTile/rowToDS resolve v1 (3-field) vs v2 (8-field) layout from
numFields and read metadata by the resolved ordinal, so a v2 tile no
longer crashes at getMap(2). A virtual tile (v2, raster null, path set)
raises a clear materialize-first error. Path-tile (StringType raster)
reads are removed.

Co-authored-by: Isaac"
```

---

### Task 2: Always-v2 output schema (`RST_ExpressionUtil.tileDataType` + `RasterSerializationUtil.tileToRow`)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala:66-86`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala:55-103`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala`

**Interfaces:**
- Produces: `RST_ExpressionUtil.v2TileType: StructType` (the canonical 8-field schema) and `tileDataType(...)` now returns it. `tileToRow((cellid, ds, metadata), dataType, hconf)` now always builds an 8-field v2 row (raster bytes, pedigree null).
- Consumes: the schema must match light's `V2_TILE_SCHEMA` (see Global Constraints) exactly.

> **Note:** Tasks 2 and this schema change MUST land together — declaring `dataType` = 8-field while `tileToRow` emits 3 fields (or vice-versa) is a broken intermediate that fails at Spark row assembly. Both edits are in this one task.

- [ ] **Step 1: Write the failing test (schema + output shape)**

Add to `RasterSerializationV2Test.scala`:
```scala
test("v2TileType matches the light V2 schema exactly") {
    val t = RST_ExpressionUtil.v2TileType
    assert(t.fieldNames.toSeq ==
        Seq("cellid","raster","path","window","clip_polygon","clip_crs","crs","metadata"))
    assert(t("cellid").dataType == LongType && !t("cellid").nullable)
    assert(t("raster").dataType == BinaryType && t("raster").nullable)
    assert(t("path").dataType == StringType && t("path").nullable)
    val w = t("window").dataType.asInstanceOf[StructType]
    assert(w.fieldNames.toSeq == Seq("col_off","row_off","width","height"))
    assert(t("metadata").dataType == MapType(StringType, StringType))
}

test("tileToRow emits an 8-field v2 materialized row with null pedigree") {
    val ds = openTinyGeotiff()
    val row = RasterSerializationUtil.tileToRow((5L, ds, Map("d" -> "GTiff")), BinaryType, hconf)
    assert(row.numFields == 8)
    assert(row.getLong(0) == 5L)
    assert(!row.isNullAt(1))          // raster bytes present
    assert(row.isNullAt(2))           // path null
    assert(row.isNullAt(3))           // window null
    assert(row.isNullAt(4) && row.isNullAt(5) && row.isNullAt(6))  // clip/crs null
    assert(!row.isNullAt(7))          // metadata present
    RasterDriver.releaseDataset(ds)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: FAIL — `tileDataType` currently builds a 3-field struct; `tileToRow` emits 3 fields.

- [ ] **Step 3: Implement v2 schema + v2 output**

In `RST_ExpressionUtil.scala`, add the canonical schema and point both `tileDataType` overloads at it:
```scala
val windowType: StructType = StructType(Seq(
    StructField("col_off", IntegerType, nullable = false),
    StructField("row_off", IntegerType, nullable = false),
    StructField("width", IntegerType, nullable = false),
    StructField("height", IntegerType, nullable = false)))

val v2TileType: StructType = StructType(Seq(
    StructField("cellid", LongType, nullable = false),
    StructField("raster", BinaryType, nullable = true),
    StructField("path", StringType, nullable = true),
    StructField("window", windowType, nullable = true),
    StructField("clip_polygon", BinaryType, nullable = true),
    StructField("clip_crs", StringType, nullable = true),
    StructField("crs", StringType, nullable = true),
    StructField("metadata", MapType(StringType, StringType), nullable = true)))

def tileDataType(tileExpr: Expression): DataType = v2TileType
def tileDataType(rdt: DataType): DataType = v2TileType
```
(Keep both method signatures so callers are untouched; they now ignore the raster-type arg and always return v2. Add `IntegerType` to the imports if not present — `org.apache.spark.sql.types._` is already imported, so it is.)

In `RasterSerializationUtil.tileToRow`, **delete the `StringType` branch entirely** and make the function always emit 8 fields:
```scala
def tileToRow(tuple: (Long, Dataset, Map[String, String]), dataType: DataType,
              hconf: SerializableConfiguration): InternalRow = {
    val metadata = SerializationUtil.toMapData[String, String](tuple._3)
    val bytes = if (tuple._2 == null) Array.emptyByteArray
                else RasterDriver.writeToBytes(tuple._2, tuple._3)
    // v2 materialized: raster bytes present; pedigree (path/window/clip/crs) null —
    // the bytes are self-describing (matches the lightweight tier's pixel-producers).
    InternalRow.fromSeq(Seq(
        tuple._1,   // cellid
        bytes,      // raster
        null,       // path
        null,       // window
        null,       // clip_polygon
        null,       // clip_crs
        null,       // crs
        metadata    // metadata
    ))
}
```
The `dataType`/`hconf` params stay in the signature (callers untouched); `dataType` is now unused for branching (all output is v2 binary). Remove the `CheckpointManager`/`GDAL`/`TaskContext`/`UTF8String`/`Try` imports if they become unused (scalastyle unused-import). If `CheckpointManager` import is used elsewhere in the file, leave it.

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationV2Test.scala
git commit -m "feat(rasterx): heavy always emits the v2 tile struct (null pedigree)

tileDataType returns the canonical 8-field v2 schema (matching the light
V2_TILE_SCHEMA); tileToRow always emits an 8-field materialized row with
raster bytes and null pedigree (bytes self-describe). Deletes the
StringType/checkpoint output branch.

Co-authored-by: Isaac"
```

---

### Task 3: Reject v1 String path-tiles at analysis (`rasterType` / `arrayOfTileRasterType` / `rstInvoke`)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala:21,47-63`
- Modify: `src/main/scala/com/databricks/labs/gbx/expressions/InvokedExpression.scala:44-49`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_TileStructureTest.scala` (or the eval suite)

**Interfaces:**
- `rasterType(tileExpr)` returns `BinaryType` for a v1-binary or v2 tile; throws `AnalysisException` for a v1 String path-tile schema.
- `rstInvoke(companion, rdt)` no longer has a `StringType` case that dispatches to `evalPath`; a StringType raster type throws at analysis.

- [ ] **Step 1: Write the failing test**

Because a v2 tile's `fields(1)` is BinaryType, `rasterType` on a v2 schema must return BinaryType (so existing expressions dispatch to `evalBinary`). Add:
```scala
test("rasterType returns BinaryType for a v2 tile schema") {
    val v2 = RST_ExpressionUtil.v2TileType
    val litExpr = Literal.default(v2)   // an Expression whose dataType is the v2 struct
    assert(RST_ExpressionUtil.rasterType(litExpr) == BinaryType)
}

test("rasterType rejects a v1 String path-tile schema") {
    val v1path = StructType(Seq(
        StructField("cellid", LongType, false),
        StructField("raster", StringType, false),
        StructField("metadata", MapType(StringType, StringType), true)))
    val ex = intercept[Exception](RST_ExpressionUtil.rasterType(Literal.default(v1path)))
    assert(ex.getMessage.toLowerCase.contains("path-tile") ||
           ex.getMessage.toLowerCase.contains("materialize"))
}
```
(If `Literal.default(struct)` is awkward, use a tiny `Expression` stub whose `dataType` returns the struct — a `BoundReference(0, schema, true)` works.)

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_TileStructureTest'`
Expected: FAIL — current `rasterType` returns `fields(1).dataType` (StringType) without rejecting it.

- [ ] **Step 3: Implement the analysis-time rejection**

In `RST_ExpressionUtil.scala`:
```scala
def rasterType(tileExpr: Expression): DataType = {
    val rdt = tileExpr.dataType.asInstanceOf[StructType].fields(1).dataType
    rdt match {
        case StringType => throw new IllegalArgumentException(
            "Raster path-tiles (raster field as a String path) are no longer supported by the " +
            "heavyweight tier. Materialize the raster to bytes in the lightweight tier " +
            "(materialize=True, or write + read back) before passing it to a heavyweight function.")
        case other => other   // BinaryType for v1-binary and v2
    }
}
```
Apply the same guard inside `arrayOfTileRasterType` (its returned `fields(1).dataType` must also reject StringType). In `InvokedExpression.rstInvoke`, drop the `StringType` case (or make it throw the same message); keep only `case BinaryType => invoke(companion, "evalBinary")` with a `case _ => throw` fallback.

- [ ] **Step 4: Run to verify pass + no regression in the tile-structure suite**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_TileStructureTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RST_ExpressionUtil.scala src/main/scala/com/databricks/labs/gbx/expressions/InvokedExpression.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_TileStructureTest.scala
git commit -m "feat(rasterx): reject v1 String path-tiles at analysis time

rasterType/arrayOfTileRasterType and rstInvoke now reject a StringType
raster field with a clear materialize-first message instead of
dispatching to the deprecated evalPath/path-open branch.

Co-authored-by: Isaac"
```

---

### Task 4: Route the direct-positional array reads through the chokepoint (`arrayToTiles`, `RST_ErrorHandler`)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala:106-112` (`arrayToTiles`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/util/RST_ErrorHandler.scala:64,74`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/util/RST_ErrorHandlerTest.scala`

**Interfaces:**
- `arrayToTiles(array, dataType)` reads each element with a layout-aware field count (not hardcoded `3`).
- `RST_ErrorHandler.safeEval(...)` array-of-tiles scan reads structs with the layout-aware field count.

- [ ] **Step 1: Write the failing test**

In `RST_ErrorHandlerTest.scala` (AnyFunSuite, no Spark), add a v2 array-of-tiles error-scan case using its existing `minimalRow` pattern extended to 8 fields:
```scala
test("safeEval scans an array of v2 (8-field) tiles without truncating") {
    val ok = new GenericInternalRow(Array[Any](1L, Array[Byte](1,2), null, null, null, null, null, emptyMetadataMapData))
    val arr = ArrayData.toArrayData(Array[Any](ok))
    // a benign eval that reads the array; assert no ClassCastException / truncation
    val result = RST_ErrorHandler.safeEval(() => 42, arr, /* dt */ BinaryType)
    assert(result == 42)
}
```
(Match the exact `safeEval` overload signature in the file — the recon shows a 3-arg `(eval, rows, dt)` returning `Any` and a tile-returning overload. Use the one the array scan lives in.)

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RST_ErrorHandlerTest'`
Expected: FAIL — `getStruct(i, 3)` truncates the 8-field row (or mis-reads).

- [ ] **Step 3: Implement layout-aware struct reads**

Add a shared helper in `RasterSerializationUtil`:
```scala
/** Field count of a tile struct element in an array-of-tiles (v1=3, v2=8). */
def tileStructFields(array: ArrayData): Int =
    if (array.numElements() == 0) 3
    else array.getStruct(0, 8).numFields match { case n if n == 3 || n == 8 => n; case _ => 3 }
```
Simpler and safer: since `ArrayData.getStruct(i, numFields)` needs the count up front and the array's element schema is known at analysis from the expression's `ArrayType(StructType(...))`, thread the field count from the expression. In `RST_ErrorHandler` and `arrayToTiles`, replace the literal `3` with the element struct's field count derived from the input `ArrayType`. Where only the `ArrayData` is in scope, use `array.getStruct(i, expectedFields)` where `expectedFields` is passed in from the caller's `dataType` (the expression knows its `ArrayType(StructType(fields))` — pass `fields.length`).
Concretely: change `arrayToTiles(array, dataType)` to accept the element field count (or derive it from a passed `StructType`), and update its 4 callers (`RST_CombineAvg:44`, `RST_MapAlgebra:46,62`, `RST_Merge:44`, `RST_FromBands:44`) to pass the element struct from `arrayOfTileRasterType`/their input `ArrayType`. In `RST_ErrorHandler`, thread the element field count similarly (its array param's element `StructType`).

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.RST_ErrorHandlerTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/util/RasterSerializationUtil.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/util/RST_ErrorHandler.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_CombineAvg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_MapAlgebra.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/RST_Merge.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/constructor/RST_FromBands.scala src/test/scala/com/databricks/labs/gbx/rasterx/util/RST_ErrorHandlerTest.scala
git commit -m "feat(rasterx): array-of-tiles reads are layout-aware (v1/v2)

arrayToTiles and RST_ErrorHandler no longer hardcode a 3-field struct;
they read each tile element with the element schema's field count so a
v2 (8-field) array element is not truncated.

Co-authored-by: Isaac"
```

---

### Task 5: Aggregator buffer reads (`RST_FromBandsAgg`, `RST_MergeAgg`)

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala:126,153,184`
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala:125-126`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAggTest.scala`

**Interfaces:** internal agg buffer struct reads become layout-aware; `RST_MergeAgg.contentKey` is binary-only (StringType path-tile branch removed).

- [ ] **Step 1: Write the failing test**

`RST_FromBandsAggTest` already builds v1 3-field tile rows via `makeSingleBandTileRow`. Add a v2 variant `makeSingleBandTileRowV2` (8 fields, raster bytes at 1, metadata at 7, pedigree null) and a test that `update`/`merge`/`eval` produce a correct multi-band result from v2 inputs — mirroring the existing v1 test's assertions on band count/order:
```scala
test("FromBandsAgg assembles bands from v2 (8-field) tile rows") {
    val a = makeSingleBandTileRowV2("a", 10)
    val b = makeSingleBandTileRowV2("b", 20)
    val out = runAgg(Seq(a, b))          // reuse the suite's existing agg-drive helper
    // decode out (v2 row → bytes at field 1) and assert 2 bands, values 10 then 20
    ...
}
```

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest'`
Expected: FAIL — `getStruct(1, 3)` / `getBinary(1)` truncate or mis-read the 8-field element.

- [ ] **Step 3: Implement layout-aware agg reads**

In `RST_FromBandsAgg`: the buffer entry is `InternalRow(idx, tileRow)`; `getStruct(1, 3)` must become `getStruct(1, tileFieldCount)` where `tileFieldCount` is the input tile element's field count (from the agg's input `ArrayType(StructType)` — resolve once from `arrayOfTileRasterType`/the child's `dataType`). `serializeTileRow`'s `getBinary(1)` is fine (raster is at position 1 in both layouts) — confirm and leave. In `RST_MergeAgg.contentKey`: remove the `getString(1)` (StringType) branch — only `getBinary(1)` remains (path-tiles deprecated); the raster is at position 1 in both v1 and v2, so `getBinary(1)` is layout-independent. Verify no other positional read in these two files assumes 3 fields.

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest'`
Expected: PASS (both the pre-existing v1 tests and the new v2 test).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAgg.scala src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_FromBandsAggTest.scala
git commit -m "feat(rasterx): aggregator tile reads are layout-aware (v1/v2)

RST_FromBandsAgg buffer reads use the input tile element's field count
instead of a hardcoded 3; RST_MergeAgg.contentKey drops the StringType
path branch (raster bytes at position 1 in both layouts).

Co-authored-by: Isaac"
```

---

### Task 6: End-to-end eval + light↔heavy parity tests

**Files:**
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_ExpressionEvalTest.scala` (extend)
- Test: new `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_V2RoundTripTest.scala` (PlanTest with SilentSparkSession)

**Interfaces:** none (integration tests).

- [ ] **Step 1: Write the end-to-end tests**

In a `SilentSparkSession` suite, drive a real heavy expression (e.g. `rst_clip` or `rst_boundingbox`) over:
```scala
test("heavy rst_* consumes a v1 binary tile and emits a v2 tile") {
    val df = spark.read.format("binaryFile").load(tifPath)
        .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
    // tile here is v1 (3-field) from rst_fromcontent
    val out = df.select(rst_clip(col("tile"), lit(wktPolygon)).as("t"))
    val schema = out.schema("t").dataType.asInstanceOf[StructType]
    assert(schema.fieldNames.toSeq ==
        Seq("cellid","raster","path","window","clip_polygon","clip_crs","crs","metadata"))
    assert(out.head.getAs[Row]("t").getAs[Array[Byte]]("raster") != null)   // materialized
}

test("heavy rst_* consumes a v2 materialized tile (from a v2-shaped input)") {
    // Build a v2-materialized tile DataFrame (raster bytes + null pedigree) and run rst_boundingbox;
    // assert it returns a valid bbox (no ClassCastException on the old getMap(2)).
    ...
}

test("heavy rst_* on a VIRTUAL tile raises the materialize-first error") {
    val virt = /* a v2 tile Row with raster=null, path set */
    val ex = intercept[Exception](df.select(rst_boundingbox(col("tile"))).collect())
    assert(ex.getMessage.toLowerCase.contains("materialize"))
}
```
Add a **light↔heavy parity** check: a heavy-emitted v2 tile DataFrame `.unionByName` with a light-reader v2 DataFrame succeeds (schemas match) — or at minimum assert the heavy output schema equals the light `V2_TILE_SCHEMA` field-for-field.

- [ ] **Step 2: Run to verify (implementation from Tasks 1-5 should make these pass)**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest,com.databricks.labs.gbx.rasterx.expressions.RST_V2RoundTripTest'`
Expected: PASS. If the virtual-tile error surfaces as a Spark `SparkException` wrapping the `IllegalArgumentException`, assert on the cause chain / message substring accordingly.

- [ ] **Step 3: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_V2RoundTripTest.scala src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_ExpressionEvalTest.scala
git commit -m "test(rasterx): heavy v1/v2 in, v2 out, virtual-guard, light-heavy parity

Co-authored-by: Isaac"
```

---

### Task 7: Positional-access guard test + docs

**Files:**
- Test: new `src/test/scala/com/databricks/labs/gbx/rasterx/util/NoPositionalTileReadTest.scala` (AnyFunSuite; source-grep guard)
- Modify: `docs/docs/api/execution-tiers.mdx`, `docs/docs/_partials/_virtual-tile-overrides.mdx`

**Interfaces:** none.

- [ ] **Step 1: Write the positional-access guard test**

Mirror light's `test_no_v1_open_tile_pattern`. Grep the heavy expression source tree for banned positional tile reads outside `RasterSerializationUtil.scala`:
```scala
test("no expression reads tile fields positionally outside the serde chokepoint") {
    val root = new java.io.File("src/main/scala/com/databricks/labs/gbx/rasterx")
    val banned = Seq("getMap(2)", "getStruct(0, 3)", ".getStruct(i, 3)", "getStruct(1, 3)")
    val offenders = allScalaFiles(root)
        .filterNot(_.getName == "RasterSerializationUtil.scala")
        .flatMap { f => val s = readFile(f); banned.filter(s.contains).map(b => s"${f.getName}: $b") }
    assert(offenders.isEmpty, s"Positional tile reads must route through RasterSerializationUtil: $offenders")
}
```
(Adjust the banned list to the exact remaining literals after Tasks 4-5; the intent is that no `getStruct(_, 3)` / `getMap(2)` on a tile row survives outside the chokepoint. If a legitimate non-tile `getStruct(_, 3)` exists, scope the grep to tile-context files or refine the pattern.)

- [ ] **Step 2: Run to verify it passes (Tasks 1-5 removed the offenders)**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.util.NoPositionalTileReadTest'`
Expected: PASS. If it lists offenders, route those sites through the chokepoint before proceeding.

- [ ] **Step 3: Update the docs**

In `docs/docs/api/execution-tiers.mdx` and `_virtual-tile-overrides.mdx`, add/adjust prose to state plainly (voice-clean — no internal vocabulary):
- **"The lightweight tier is for light (virtual) raster tiles; the heavyweight tier is for heavy (binary) raster tiles."** (the user's framing — place it prominently in the tier-comparison / light↔heavy-bridge section, and echo it in the partial).
- Heavy accepts both v1 and v2 **materialized** tiles as input and always emits the v2 tile struct.
- A **virtual** tile passed to a heavyweight function raises a clear error; materialize it in the lightweight tier first (`materialize=True`, or write + read back).
- Raster **path-tiles** (raster field as a String path) are no longer supported by the heavyweight tier.

- [ ] **Step 4: Voice guard + commit**

Run:
```bash
grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/ ; echo "exit:$?"
```
Expected: no matches.
```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/util/NoPositionalTileReadTest.scala docs/docs/api/execution-tiers.mdx docs/docs/_partials/_virtual-tile-overrides.mdx
git commit -m "test+docs: guard against positional tile reads; document tier tile model

Adds a source-grep guard that all heavy tile-field reads route through
RasterSerializationUtil, and documents that the lightweight tier is for
light (virtual) tiles and the heavyweight tier is for heavy (binary)
tiles: heavy takes v1/v2 materialized in, emits v2, and raises a clear
error on a virtual tile (materialize in light first).

Co-authored-by: Isaac"
```

---

### Task 8: Full heavy suite + scalastyle (integration gate)

**Files:** none (verification).

- [ ] **Step 1: Run the full raster expression + serialization suites**

Run: `gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.util.RasterSerializationV2Test,com.databricks.labs.gbx.rasterx.util.RST_ErrorHandlerTest,com.databricks.labs.gbx.rasterx.expressions.RST_TileStructureTest,com.databricks.labs.gbx.rasterx.expressions.RST_ExpressionEvalTest,com.databricks.labs.gbx.rasterx.expressions.RST_AggEvalTest,com.databricks.labs.gbx.rasterx.expressions.agg.RST_FromBandsAggTest,com.databricks.labs.gbx.rasterx.expressions.RST_V2RoundTripTest,com.databricks.labs.gbx.rasterx.util.NoPositionalTileReadTest' --log heavy-v2.log`
Expected: all PASS. Also run the broader `gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.*'` to catch any expression not covered by the targeted suites (there are ~60 rst_* expressions; a regression in any that route through the chokepoints would surface here).

- [ ] **Step 2: Scalastyle**

Run: `gbx:lint:scalastyle`
Expected: no errors (fix unused imports left by the deleted StringType/checkpoint branch).

- [ ] **Step 3: Commit any lint fixes**

```bash
git add -A
git commit -m "style(rasterx): scalastyle clean after v2 tile changes

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:**
- Two centralized chokepoints (by-name/layout-aware read, always-v2 write) → Tasks 1, 2. ✓
- Virtual-tile guard with actionable message → Task 1 (+ e2e in Task 6). ✓
- Accept v1 + v2 in, emit v2 out → Tasks 1, 2, 6. ✓
- Deprecate v1 String path-tiles + checkpoint output → Tasks 0 (safety), 2 (output branch delete), 3 (analysis reject). ✓
- Route all positional sites through chokepoints → Tasks 4 (array/error-handler), 5 (aggregators); guard test Task 7. ✓
- Pedigree = null on materialized output → Task 2 (constraint + test). ✓
- Docs (incl. the "light tier = light/virtual tiles, heavy tier = heavy/binary tiles" framing) → Task 7. ✓
- Schema byte-for-byte match with light `V2_TILE_SCHEMA` → Task 2 (explicit schema test) + Task 6 (parity). ✓
- No new registered functions / binding parity → no task adds registrations; output schema change is not a signature change. ✓

**Placeholder scan:** every code step carries real Scala; test bodies are concrete; the one derive-field-count-from-input-ArrayType instruction (Task 4/5) names the exact source (`arrayOfTileRasterType` / child `dataType`) rather than hand-waving.

**Type consistency:** `v2TileType`/`windowType` names used consistently; `tileToRow`/`tileDataType`/`rowToTile` signatures preserved so the ~80 existing callers stay untouched (the leverage). `IntegerType` needed by `windowType` is covered by the existing `import org.apache.spark.sql.types._`.

**Sequencing risk:** Task 2 must land `tileDataType`=v2 and `tileToRow`=8-field together (a split would break row assembly) — called out in Task 2's note. Task 0 (checkpoint safety) gates the Task 2 deletion.

**Ordering:** Tasks 1→2→3 are the core; 4→5 remove the stragglers so the Task 7 guard test can pass; 6 is e2e; 8 is the full-suite gate. Each task is independently testable and independently reviewable.
