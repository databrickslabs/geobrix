# GridX Error Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring GridX (BNG + Quadbin + Custom, heavy Scala + light pygx) under the ratified GeoBrix error axis — bad/non-executable parameter raises, bad data degrades to NULL — retiring the `BNG.parse` stage-killer and reconciling coordinate handling across families.

**Architecture:** Heavy GridX expressions are `InvokedExpression` case classes whose companion `eval(...)` is invoked reflectively via `invoke(<Companion>)` (same rail as VectorX `ST_TransformCrs`). A new shared `GridErrorHandler.safeEval[T](nullValue)(body)` guard wraps only the DATA-touching work in each companion; parameter validation (resolution, grid spec, arity, type) stays OUTSIDE the guard so it still raises. Light pygx mirrors this with per-family never-error parse helpers returning `None` on bad data. Both tiers keep Quadbin's web-mercator latitude clamp as documented intended behavior.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / Java 17 (heavy); Python 3.12 + shapely/pyproj/quadbin (light). Tests run in the `geobrix-dev` Docker container via the `gbx:*` palette.

## Global Constraints

- **The axis:** bad/non-executable PARAMETER → raise; bad DATA → NULL. Applies to BNG + Quadbin + Custom, both tiers.
- **Parameter errors raise BEFORE the guarded body.** A `safeEval` that wraps resolution/grid-spec/arity/type validation would swallow a usage error into a silent all-NULL column. Resolve/validate parameters first, then wrap only cell-id parse, coordinate encode/decode, and geometry build.
- **Quadbin latitude clamp is intended behavior, NOT a degrade.** Keep the ±85.05112878° clamp (`Quadbin.scala:28` `LAT_MIN`/`LAT_MAX`, applied in `lonLatToTile:50`; also clamps lon ±180°). Document it in three places: the `error-handling.mdx` GridX section, that page's contract prose, and the `_quadbin.py` module docstring.
- **Per-shape degrade:** scalar (Long/Double/String) → NULL; geometry (WKB/WKT) → NULL; array (`Array[Long]`) → NULL (whole array); struct (tessellate) → NULL struct; aggregators (`CellUnionAgg`/`CellIntersectionAgg`) → skip bad member; explode UDTFs → zero rows for a bad input.
- **Scalar accessors returning primitives must box.** Heavy `BNG_CellArea` (`Double`) and `BNG_Distance` (`Long`) return primitives that cannot express NULL — switch their companion `eval` return types to `java.lang.Double`/`java.lang.Long`, mirroring the shipped RasterX accessor pattern (`RST_Width.scala`).
- **No new knob.** No `strict=`/crash parameter is added to GridX (deferred to the strict-mode workstream). The raise-on-bad-parameter path is the loud signal.
- **Docs are a required deliverable.** The implementation is NOT complete until `docs/docs/api/error-handling.mdx` has a GridX section and the page builds clean.
- **The RasterX `*_rasterize_agg` null-cellid bug is ALREADY FIXED** — re-run its tests (`test/pyrx/test_{h3,quadbin,bng}_rasterize_agg.py`, the `*_value_alignment` nodes), do NOT re-implement.
- **User-facing docs voice:** no internal planning vocabulary (QC `internals-leak` check). No `wave N` etc.
- **Commit hygiene:** subjects ≤72 chars, WHY in body, end with `Co-authored-by: Isaac`. Commit as a single plain command (never `-n`/`--no-verify`).
- **Cross-tier parity is the spine:** same degenerate input → same signal on both tiers.

## File Structure

**Heavy (Scala) — create:**
- `src/main/scala/com/databricks/labs/gbx/gridx/expressions/GridErrorHandler.scala` — the shared guard helper (sibling of `vectorx/expressions/CrsExpressionUtil.scala`).

**Heavy — modify (guard the data path, box primitives):**
- `gridx/grid/BNG.scala` — make `parse` degrade-friendly (a `parseOrNull` variant, or callers guard); leave `pointToCellID` NaN `require` (data → will be guarded by callers).
- `gridx/bng/BNG_AsWKB.scala`, `BNG_AsWKT.scala`, `BNG_Centroid.scala`, `BNG_CellArea.scala`, `BNG_Distance.scala`, `BNG_EuclideanDistance.scala`, `BNG_CellIntersection.scala`, `BNG_CellUnion.scala`, `BNG_PointAsCell.scala`, `BNG_EastNorthAsBNG.scala`, `BNG_KRing.scala`, `BNG_KLoop.scala`, `BNG_GeometryKRing.scala`, `BNG_GeometryKLoop.scala`, `BNG_Polyfill.scala`, `BNG_Tessellate.scala` — wrap data path in `safeEval`.
- `gridx/bng/agg/BNG_CellUnionAgg.scala:55`, `BNG_CellIntersectionAgg.scala:41` — guard per-member `BNG.parse` in `update`.
- `gridx/bng/generators/BNG_KRingExplode.scala:32`, `BNG_KLoopExplode.scala:32`, `BNG_TessellateExplode.scala`, `BNG_GeometryKRingExplode.scala`, `BNG_GeometryKLoopExplode.scala` — guard `eval` to return empty on bad input.
- `gridx/custom/Custom_PointAsCell.scala` + siblings (`Custom_AsWKB`, `Custom_AsWKT`, `Custom_Centroid`, `Custom_Polyfill`, `Custom_KRing`) — split parameter validation (resolution via `Custom_GridSpec.asInt`, grid spec) from data (NaN/out-of-bounds coords, cell decode) so data degrades to NULL while parameters raise.
- `gridx/grid/CustomGridSystem.scala` — provide a bounds/NaN-tolerant `pointToCellIdOrNull` path (or callers catch), keeping the parameter `require`s (resolution) raising.

**Light (pygx) — modify:**
- `pygx/_bng.py` — `parse()` retire `StopIteration` → return `None` on bad prefix (add `parse_safe`); keep `get_resolution` ValueError.
- `pygx/_custom.py` — split NaN/bounds (data → None) from resolution (param → ValueError) in `point_to_cell_id`.
- `pygx/_quadbin.py` — docstring clamp note; keep resolution/k ValueError.
- `pygx/functions.py` — scalar UDFs return `None` on bad data; aggregator UDFs skip bad member; `@udtf` classes yield nothing on bad input.

**Docs — modify:**
- `docs/docs/api/error-handling.mdx` — add GridX section (after VectorX) with the Quadbin clamp note.

**Tests — create/modify:**
- Heavy: `src/test/scala/com/databricks/labs/gbx/gridx/GridErrorHandlingParityTest.scala` (new) — per-family degenerate corpus.
- Light: `python/geobrix/test/pygx/test_gridx_error_handling.py` (new) — the same corpus, mirrored.

---

### Task 1: `GridErrorHandler.safeEval` guard helper

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/gridx/expressions/GridErrorHandler.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/expressions/GridErrorHandlerTest.scala`

**Interfaces:**
- Produces: `object GridErrorHandler { def safeEval[T](nullValue: T)(body: => T): T }` — runs `body`; on a `NonFatal` throw returns `nullValue`; fatal `Throwable` (OOM/StackOverflow) propagates. `nullValue` is the shape's null (`null` cast to the return type for reference types; a boxed `null` for numeric scalars).

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.expressions

import org.scalatest.funsuite.AnyFunSuite

class GridErrorHandlerTest extends AnyFunSuite {

    test("safeEval returns the body value when no exception") {
        assert(GridErrorHandler.safeEval("fallback")("ok") == "ok")
    }

    test("safeEval returns nullValue on a NonFatal throw") {
        val r: String = GridErrorHandler.safeEval[String](null)(throw new IllegalArgumentException("bad data"))
        assert(r == null)
    }

    test("safeEval returns boxed null for a numeric shape on throw") {
        val r: java.lang.Long = GridErrorHandler.safeEval[java.lang.Long](null)(throw new NumberFormatException("x"))
        assert(r == null)
    }

    test("safeEval rethrows a fatal error (StackOverflowError)") {
        assertThrows[StackOverflowError] {
            GridErrorHandler.safeEval[String](null)(throw new StackOverflowError())
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.expressions.GridErrorHandlerTest' --log gridx-guard.log`
Expected: FAIL — `GridErrorHandler` not defined (compile error).

- [ ] **Step 3: Write minimal implementation**

```scala
package com.databricks.labs.gbx.gridx.expressions

import scala.util.control.NonFatal

/** Shared degrade guard for GridX expression companions.
  *
  * GridX has no metadata carrier, so a bad-DATA condition degrades to NULL — the shape's
  * null. This helper wraps ONLY the data-touching work (cell-id parse, coordinate
  * encode/decode, geometry build); parameter validation (resolution range, grid spec,
  * arity, argument type) must be done and allowed to raise BEFORE calling `safeEval`, so a
  * usage error is never silently swallowed into an all-NULL column.
  *
  * `NonFatal`, not `Throwable`: OutOfMemoryError / StackOverflowError / InterruptedException
  * must propagate and fail the task. */
object GridErrorHandler {

    /** Run `body`; on a NonFatal exception return `nullValue` (the return type's null). */
    def safeEval[T](nullValue: T)(body: => T): T =
        try body
        catch { case NonFatal(_) => nullValue }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.expressions.GridErrorHandlerTest' --log gridx-guard.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/expressions/GridErrorHandler.scala src/test/scala/com/databricks/labs/gbx/gridx/expressions/GridErrorHandlerTest.scala
git commit -m "feat(gridx): shared safeEval degrade guard for GridX companions"
```

---

### Task 2: BNG `parseOrNull` + retire the parse stage-killer

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/gridx/grid/BNG.scala` (add `parseOrNull` near `parse` ~427)
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/grid/BNGParseTest.scala`

**Interfaces:**
- Produces: `def parseOrNull(cellID: String): java.lang.Long` — returns the parsed cell id, or `null` when the string is not a parseable BNG cell (bad 100km prefix, non-digit body). `parse` (raising) stays for callers that have already validated. Consumed by every BNG companion + aggregator + generator that takes a STRING cell id.

**Interfaces note:** `parseOrNull` returns `java.lang.Long` (nullable) — callers unbox only after a null check.

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite

class BNGParseTest extends AnyFunSuite {

    test("parseOrNull returns a cell id for a valid BNG string") {
        // TL is a valid 100km grid-square prefix (London area).
        assert(BNG.parseOrNull("TL") != null)
    }

    test("parseOrNull returns null for an unrecognised prefix (no throw)") {
        assert(BNG.parseOrNull("!!") == null)
    }

    test("parseOrNull returns null for a non-digit body (no NumberFormatException)") {
        // Valid prefix, garbage digits — parse() would throw NumberFormatException here.
        assert(BNG.parseOrNull("TLxy") == null)
    }

    test("parse still throws on a bad prefix (raising variant preserved)") {
        assertThrows[IllegalArgumentException](BNG.parse("!!"))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.grid.BNGParseTest' --log gridx-bngparse.log`
Expected: FAIL — `parseOrNull` not defined.

- [ ] **Step 3: Write minimal implementation**

Add to `BNG` (right after `parse`), reusing `GridErrorHandler`:

```scala
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler

/** Bad-DATA-tolerant [[parse]]: a malformed BNG cell string (unrecognised 100km prefix or
  * non-digit body) returns null rather than throwing, so one bad cell id in a column
  * degrades to NULL instead of killing the stage. Callers unbox only after a null check.
  * The raising [[parse]] is kept for internal callers that have already validated. */
def parseOrNull(cellID: String): java.lang.Long =
    GridErrorHandler.safeEval[java.lang.Long](null)(parse(cellID))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.grid.BNGParseTest' --log gridx-bngparse.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/grid/BNG.scala src/test/scala/com/databricks/labs/gbx/gridx/grid/BNGParseTest.scala
git commit -m "feat(gridx): BNG.parseOrNull degrades malformed cell ids to null"
```

---

### Task 3: BNG scalar accessors degrade to NULL (box primitives)

**Files:**
- Modify: `gridx/bng/BNG_AsWKB.scala`, `BNG_AsWKT.scala`, `BNG_Centroid.scala`, `BNG_CellArea.scala`, `BNG_Distance.scala`, `BNG_EuclideanDistance.scala`, `BNG_CellArea` and `BNG_Distance` return types → boxed.
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_AccessorNullTest.scala`

**Interfaces:**
- Consumes: `BNG.parseOrNull` (Task 2), `GridErrorHandler.safeEval` (Task 1).
- Produces: string-cellid `eval` overloads that return NULL (not throw) on a malformed cell id. `BNG_CellArea.eval(UTF8String): java.lang.Double`, `BNG_Distance.eval(UTF8String, UTF8String): java.lang.Long` (boxed).

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_AccessorNullTest extends AnyFunSuite {
    private def u(s: String) = UTF8String.fromString(s)

    test("BNG_AsWKB returns null for a malformed cell id") {
        assert(BNG_AsWKB.eval(u("!!")) == null)
    }
    test("BNG_CellArea returns null (boxed) for a malformed cell id") {
        assert(BNG_CellArea.eval(u("!!")) == null)
    }
    test("BNG_Distance returns null (boxed) when either cell id is malformed") {
        assert(BNG_Distance.eval(u("!!"), u("TL")) == null)
    }
    test("BNG_CellArea still computes a real value for a valid cell") {
        assert(BNG_CellArea.eval(u("TL")) != null)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_AccessorNullTest' --log gridx-bngacc.log`
Expected: FAIL — currently these throw (or won't compile against boxed types).

- [ ] **Step 3: Write minimal implementation**

For string-cellid accessors, parse via `parseOrNull` and short-circuit to null. Example `BNG_AsWKB` companion:

```scala
def eval(cellID: UTF8String): Array[Byte] = {
    val cid = BNG.parseOrNull(cellID.toString)
    if (cid == null) null else GridErrorHandler.safeEval[Array[Byte]](null)(execute(cid))
}
```

For `BNG_CellArea` — box the return type and the case-class `dataType` stays `DoubleType`:

```scala
// case class: dataType stays DoubleType, nullable = true (already true)
def eval(cellID: Long): java.lang.Double = GridErrorHandler.safeEval[java.lang.Double](null)(execute(cellID))
def eval(cellId: UTF8String): java.lang.Double = {
    val cid = BNG.parseOrNull(cellId.toString)
    if (cid == null) null else GridErrorHandler.safeEval[java.lang.Double](null)(execute(cid))
}
```

For `BNG_Distance` — box to `java.lang.Long`, null if either id fails:

```scala
def eval(c1: UTF8String, c2: UTF8String): java.lang.Long = {
    val a = BNG.parseOrNull(c1.toString); val b = BNG.parseOrNull(c2.toString)
    if (a == null || b == null) null else GridErrorHandler.safeEval[java.lang.Long](null)(execute(a, b))
}
```

Apply the analogous pattern to `BNG_AsWKT`, `BNG_Centroid`, `BNG_EuclideanDistance` (box its numeric return likewise). Add `import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler` to each.

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_AccessorNullTest' --log gridx-bngacc.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_AsWKB.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_AsWKT.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_Centroid.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_CellArea.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_Distance.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_EuclideanDistance.scala src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_AccessorNullTest.scala
git commit -m "fix(gridx): BNG scalar accessors degrade malformed cell ids to NULL"
```

---

### Task 4: BNG geometry / array / struct ops + PointAsCell degrade

**Files:**
- Modify: `gridx/bng/BNG_PointAsCell.scala`, `BNG_EastNorthAsBNG.scala`, `BNG_KRing.scala`, `BNG_KLoop.scala`, `BNG_GeometryKRing.scala`, `BNG_GeometryKLoop.scala`, `BNG_Polyfill.scala`, `BNG_Tessellate.scala`, `BNG_CellIntersection.scala`, `BNG_CellUnion.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_ShapeNullTest.scala`

**Interfaces:**
- Consumes: `GridErrorHandler.safeEval`, `BNG.parseOrNull`.
- Rule: PARAMETER validation (resolution via `BNG.resolutionMap`/`getResolution`, k as Int) stays OUTSIDE the guard and still raises; only geometry parse + coordinate/cell math is wrapped.

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_ShapeNullTest extends AnyFunSuite {
    private def u(s: String) = UTF8String.fromString(s)

    test("PointAsCell returns null for an unparseable geometry (data)") {
        assert(BNG_PointAsCell.eval(u("NOT WKT"), 3) == null)
    }
    test("PointAsCell still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_PointAsCell.eval(u("POINT (530000 180000)"), u("bogus-res")))
    }
    test("KRing returns null array for a malformed cell id (data)") {
        assert(BNG_KRing.eval(u("!!"), 1) == null)
    }
    test("Polyfill returns null for an unparseable geometry (data)") {
        assert(BNG_Polyfill.eval(u("NOT WKT"), 3) == null)
    }
    test("Tessellate returns null struct for an unparseable geometry (data)") {
        assert(BNG_Tessellate.eval(u("NOT WKT"), 3, true) == null)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_ShapeNullTest' --log gridx-bngshape.log`
Expected: FAIL — currently throws on bad geometry.

- [ ] **Step 3: Write minimal implementation**

The rule is uniform: resolve the resolution parameter FIRST (unchanged — this line may raise), then wrap the data body. For `BNG_PointAsCell` (string-resolution overload):

```scala
def eval(wkt: UTF8String, resolution: UTF8String): UTF8String = {
    val res = BNG.resolutionMap(resolution.toString)  // PARAMETER: raises on bad resolution
    GridErrorHandler.safeEval[UTF8String](null)(UTF8String.fromString(executeWKT(wkt.toString, res)))
}
def eval(wkt: UTF8String, resolution: Int): UTF8String =  // Int resolution already validated by builder
    GridErrorHandler.safeEval[UTF8String](null)(UTF8String.fromString(executeWKT(wkt.toString, resolution)))
```

- For array ops (`BNG_KRing`/`BNG_KLoop`, string-cellid overload): `parseOrNull` the cell id, null-short-circuit, then wrap the ring computation returning `null` (typed `Array[Long]`/`GenericArrayData` as the existing overload does).
- For geometry ops (`BNG_Polyfill`, `BNG_GeometryKRing`, `BNG_GeometryKLoop`): resolve resolution first (raises), wrap the geometry-parse + fill body → null on bad geometry.
- For `BNG_Tessellate`: resolve resolution first (raises), wrap the tessellation body → null struct on bad geometry.
- For `BNG_CellIntersection`/`BNG_CellUnion` (two string cell ids): `parseOrNull` both, null if either fails, wrap the set op.
- For `BNG_EastNorthAsBNG`: resolution first (raises); NaN eastings/northings is data → wrap so the `pointToCellID` NaN `require` degrades to null.

Add `import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler` to each modified file.

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_ShapeNullTest' --log gridx-bngshape.log`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_PointAsCell.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_EastNorthAsBNG.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_KRing.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_KLoop.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_GeometryKRing.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_GeometryKLoop.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_Polyfill.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_Tessellate.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_CellIntersection.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/BNG_CellUnion.scala src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_ShapeNullTest.scala
git commit -m "fix(gridx): BNG geometry/array/struct ops degrade bad data to NULL"
```

---

### Task 5: BNG aggregators skip bad members; generators emit zero rows

**Files:**
- Modify: `gridx/bng/agg/BNG_CellUnionAgg.scala` (update ~52-59), `gridx/bng/agg/BNG_CellIntersectionAgg.scala` (update ~38-47)
- Modify: `gridx/bng/generators/BNG_KRingExplode.scala` (eval ~24-40), `BNG_KLoopExplode.scala` (eval ~24-40), `BNG_TessellateExplode.scala`, `BNG_GeometryKRingExplode.scala`, `BNG_GeometryKLoopExplode.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_AggGenDegradeTest.scala`

**Interfaces:**
- Consumes: `BNG.parseOrNull`, `GridErrorHandler.safeEval`.
- Aggregator rule: a member whose STRING cell id fails `parseOrNull` is SKIPPED (not fatal); the aggregate continues over valid members.
- Generator rule: a bad input cell id / geometry yields an EMPTY iterator (zero rows).

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.bng

import com.databricks.labs.gbx.gridx.bng.generators.BNG_KRingExplode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_AggGenDegradeTest extends AnyFunSuite {

    test("KRingExplode yields zero rows for a malformed cell id (data)") {
        val gen = BNG_KRingExplode(Literal(UTF8String.fromString("!!"), StringType), Literal(1))
        val rows = gen.eval(InternalRow.empty).iterator.toList
        assert(rows.isEmpty)
    }

    test("KRingExplode still yields rows for a valid cell id") {
        val gen = BNG_KRingExplode(Literal(UTF8String.fromString("TL"), StringType), Literal(1))
        assert(gen.eval(InternalRow.empty).iterator.nonEmpty)
    }
}
```

(The aggregator skip-behavior is asserted end-to-end in the Task 8 parity suite, where a group with a bad member is unioned; unit-testing `TypedImperativeAggregate.update` in isolation needs a buffer-construction harness that the parity SQL test covers more cleanly.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_AggGenDegradeTest' --log gridx-agggen.log`
Expected: FAIL — `KRingExplode` currently throws on `"!!"`.

- [ ] **Step 3: Write minimal implementation**

Generators — guard the string-cellid branch. `BNG_KRingExplode.eval` (and `KLoopExplode` identically):

```scala
case s: UTF8String =>
    val cid = BNG.parseOrNull(s.toString)
    if (cid == null) Iterator.empty[InternalRow]
    else BNG.kRing(cid, kValue.asInstanceOf[Int])
        .map(c => InternalRow.fromSeq(Seq(UTF8String.fromString(BNG.format(c)))))
```

For the geometry generators (`TessellateExplode`, `GeometryKRingExplode`, `GeometryKLoopExplode`): resolve resolution first (raises), then wrap the geometry-parse + generation so a bad geometry returns `Seq.empty`:

```scala
val resolutionVal = ... // PARAMETER: unchanged, may raise
GridErrorHandler.safeEval[IterableOnce[InternalRow]](Iterator.empty)( /* existing parse+generate body */ )
```

Aggregators — skip a member whose string id fails to parse. `BNG_CellUnionAgg.update`:

```scala
override def update(b: UnionAcc, in: InternalRow): UnionAcc = {
    val r = child.eval(in).asInstanceOf[InternalRow]
    if (r == null) return b
    val cellId: java.lang.Long = idType match {
        case StringType => BNG.parseOrNull(r.getString(idFieldIndex))
        case LongType   => r.getLong(idFieldIndex)
    }
    if (cellId == null) return b  // skip a member with a malformed cell id
    b.update(cellId, r.getBoolean(coreFieldIndex), r.getBinary(wkbFieldIndex))
}
```

Apply the same skip pattern to `BNG_CellIntersectionAgg.update` (~41). Add the `GridErrorHandler` import where used.

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.bng.BNG_AggGenDegradeTest' --log gridx-agggen.log`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/bng/agg/BNG_CellUnionAgg.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/agg/BNG_CellIntersectionAgg.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/generators/BNG_KRingExplode.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/generators/BNG_KLoopExplode.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/generators/BNG_TessellateExplode.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/generators/BNG_GeometryKRingExplode.scala src/main/scala/com/databricks/labs/gbx/gridx/bng/generators/BNG_GeometryKLoopExplode.scala src/test/scala/com/databricks/labs/gbx/gridx/bng/BNG_AggGenDegradeTest.scala
git commit -m "fix(gridx): BNG aggregators skip bad members, generators emit zero rows"
```

---

### Task 6: Custom grid — split parameter validation from data degrade

**Files:**
- Modify: `gridx/grid/CustomGridSystem.scala` (add `pointToCellIdOrNull` ~249; guard `cellIdToCenter` decode ~332)
- Modify: `gridx/custom/Custom_PointAsCell.scala`, `Custom_AsWKB.scala`, `Custom_AsWKT.scala`, `Custom_Centroid.scala`, `Custom_Polyfill.scala`, `Custom_KRing.scala`
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/custom/Custom_DegradeTest.scala`

**Interfaces:**
- Produces: `CustomGridSystem.pointToCellIdOrNull(x, y, resolution): java.lang.Long` — keeps the resolution `require` (PARAMETER → raises) but returns `null` for NaN/out-of-bounds coordinates (DATA). The existing `pointToCellID` (all-raising) is unchanged for internal callers.
- **This is the crux:** today `CustomGridSystem.pointToCellID` bundles a parameter check (`resolution > maxResolution`) and data checks (NaN, out-of-bounds) behind one `IllegalStateException`. A blind `safeEval` wrap would swallow the resolution error. The resolution check must stay OUTSIDE the guard.

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.scalatest.funsuite.AnyFunSuite

class Custom_DegradeTest extends AnyFunSuite {
    // A 0..1e6 grid, maxResolution 3 (adjust ctor to the real GridConf factory).
    private def sys = CustomGridSystem(GridConf(0, 1000000, 0, 1000000, 100000, 10, 3))

    test("pointToCellIdOrNull returns null for out-of-bounds x (data)") {
        assert(sys.pointToCellIdOrNull(-5.0, 500000.0, 0) == null)
    }
    test("pointToCellIdOrNull returns null for NaN y (data)") {
        assert(sys.pointToCellIdOrNull(500000.0, Double.NaN, 0) == null)
    }
    test("pointToCellIdOrNull still RAISES for resolution over max (parameter)") {
        assertThrows[IllegalStateException](sys.pointToCellIdOrNull(500000.0, 500000.0, 99))
    }
    test("pointToCellIdOrNull returns a cell for an in-bounds point") {
        assert(sys.pointToCellIdOrNull(500000.0, 500000.0, 0) != null)
    }
}
```

(Adjust the `GridConf`/`CustomGridSystem` construction to the real factory signature found in `gridx/grid/GridConf.scala` — verify before writing.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.custom.Custom_DegradeTest' --log gridx-custom.log`
Expected: FAIL — `pointToCellIdOrNull` not defined.

- [ ] **Step 3: Write minimal implementation**

Add to `CustomGridSystem`:

```scala
import com.databricks.labs.gbx.gridx.expressions.GridErrorHandler

/** Bad-DATA-tolerant [[pointToCellID]]: the resolution PARAMETER check still raises, but a
  * NaN or out-of-bounds coordinate (DATA) returns null so one bad row degrades to NULL. */
def pointToCellIdOrNull(x: Double, y: Double, resolution: Int): java.lang.Long = {
    require(                              // PARAMETER: outside the guard, still raises
      resolution <= conf.maxResolution,
      throw new IllegalStateException(s"Resolution exceeds maximum resolution of ${conf.maxResolution}.")
    )
    GridErrorHandler.safeEval[java.lang.Long](null)(pointToCellID(x, y, resolution))  // DATA: NaN/bounds -> null
}
```

Then in `Custom_PointAsCell.eval`: keep `Custom_GridSpec.asInt(resolutionExpr..., "resolution")` and grid-spec decode (PARAMETER — raise) OUTSIDE, and call `pointToCellIdOrNull` for the point:

```scala
val res = Custom_GridSpec.asInt(resolutionExpr.eval(input), "resolution")  // PARAMETER
val c   = geom.getCoordinate
val cid = sys.pointToCellIdOrNull(c.x, c.y, res)  // DATA -> null
cid  // java.lang.Long; Catalyst reads null as SQL NULL
```

Wrap the geometry decode (`decodeGeom`) in `safeEval` too (unparseable bytes = data → null). For `Custom_AsWKB`/`AsWKT`/`Centroid`: guard `CustomGridSystem.cellIdToCenter`/geometry decode (add a `cellIdToGeometryOrNull` or wrap) so a cell id that decodes to an empty geometry (`:335` throw) returns null instead of raising. `Custom_Polyfill`/`Custom_KRing`: resolution/k stay raising; geometry/cell math wrapped.

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.custom.Custom_DegradeTest' --log gridx-custom.log`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/grid/CustomGridSystem.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_PointAsCell.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_AsWKB.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_AsWKT.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_Centroid.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_Polyfill.scala src/main/scala/com/databricks/labs/gbx/gridx/custom/Custom_KRing.scala src/test/scala/com/databricks/labs/gbx/gridx/custom/Custom_DegradeTest.scala
git commit -m "fix(gridx): Custom grid degrades bad coords to NULL, keeps param raises"
```

---

### Task 7: Quadbin — confirm clamp, no behavior change, document

**Files:**
- Modify: `gridx/grid/Quadbin.scala` (docstring only — confirm clamp, no logic change)
- Test: `src/test/scala/com/databricks/labs/gbx/gridx/quadbin/Quadbin_ClampTest.scala`

**Interfaces:**
- Consumes: nothing new. Quadbin's `lonLatToTile` already clamps (`:50-51`), `require`s resolution range (parameter → raise), and `Quadbin_CellUnionAgg.update` already null-guards (`:42`). Heavy Quadbin needs NO degrade change — this task PINS the clamp with a test and documents it.

- [ ] **Step 1: Write the failing test**

```scala
package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.scalatest.funsuite.AnyFunSuite

class Quadbin_ClampTest extends AnyFunSuite {

    test("out-of-range latitude is clamped, not NULL'd (documented behavior)") {
        // lat 89 is beyond the web-mercator limit; it must clamp to the +85.05 tile,
        // producing the SAME cell as lat 85.05112878, not an error or a different cell.
        val clamped = Quadbin.lonLatToTile(10.0, 89.0, 10)
        val atLimit = Quadbin.lonLatToTile(10.0, 85.05112878, 10)
        assert(clamped == atLimit)
    }

    test("resolution out of range still RAISES (parameter)") {
        assertThrows[IllegalArgumentException](Quadbin.pointToCell(10.0, 50.0, 99))
    }
}
```

(Verify `Quadbin.pointToCell`'s real name/signature before writing; adjust if it differs.)

- [ ] **Step 2: Run test to verify it fails/passes**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.quadbin.Quadbin_ClampTest' --log gridx-qbclamp.log`
Expected: PASS immediately for the clamp test (behavior already exists) — this test PINS it. The resolution test also passes. If either fails, the clamp/require assumption is wrong — stop and report.

- [ ] **Step 3: Document the clamp in the Scala docstring**

Add to `Quadbin.lonLatToTile`'s scaladoc: that out-of-range latitude is intentionally clamped to ±85.05112878° (web-mercator limit) and longitude to ±180°, matching every slippy-map tiler — a deliberate, documented behavior, NOT a degrade, and distinct from BNG/Custom which NULL an out-of-extent point.

- [ ] **Step 4: Re-run to confirm still green**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.quadbin.Quadbin_ClampTest' --log gridx-qbclamp.log`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/gridx/grid/Quadbin.scala src/test/scala/com/databricks/labs/gbx/gridx/quadbin/Quadbin_ClampTest.scala
git commit -m "test(gridx): pin Quadbin latitude clamp as documented behavior"
```

---

### Task 8: Light pygx core — never-error parse + data/parameter split

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/_bng.py` (add `parse_safe` ~300; keep `get_resolution` ValueError)
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/_custom.py` (split `point_to_cell_id` NaN/bounds → None from resolution → ValueError)
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/_quadbin.py` (docstring clamp note only)
- Test: `python/geobrix/test/pygx/test_gridx_error_core.py`

**Interfaces:**
- Produces: `_bng.parse_safe(cell_id: str) -> Optional[int]` (None on bad prefix/body, never raises); `_custom.point_to_cell_id_or_none(conf, x, y, resolution) -> Optional[int]` (resolution ValueError raises; NaN/bounds → None).

- [ ] **Step 1: Write the failing test**

```python
import math
import pytest
from databricks.labs.gbx.pygx import _bng, _custom


def test_bng_parse_safe_none_on_bad_prefix():
    assert _bng.parse_safe("!!") is None

def test_bng_parse_safe_none_on_bad_body():
    assert _bng.parse_safe("TLxy") is None

def test_bng_parse_safe_value_on_valid():
    assert _bng.parse_safe("TL") is not None

def test_bng_parse_still_raises():
    with pytest.raises(Exception):
        _bng.parse("!!")

def _conf():
    return _custom.CustomGridConf(0, 1_000_000, 0, 1_000_000, 100_000, 10, 3)

def test_custom_point_or_none_none_on_out_of_bounds():
    assert _custom.point_to_cell_id_or_none(_conf(), -5.0, 500_000.0, 0) is None

def test_custom_point_or_none_none_on_nan():
    assert _custom.point_to_cell_id_or_none(_conf(), 500_000.0, float("nan"), 0) is None

def test_custom_point_or_none_raises_on_bad_resolution():
    with pytest.raises(ValueError):
        _custom.point_to_cell_id_or_none(_conf(), 500_000.0, 500_000.0, 99)
```

(Verify `CustomGridConf`'s real constructor args before writing; adjust the `_conf()` factory.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:python --path python/geobrix/test/pygx/test_gridx_error_core.py`
Expected: FAIL — `parse_safe` / `point_to_cell_id_or_none` not defined.

- [ ] **Step 3: Write minimal implementation**

In `_bng.py`, add next to `parse`:

```python
def parse_safe(cell_id: str):
    """Bad-DATA-tolerant parse: a malformed BNG cell string (unrecognised prefix or
    non-digit body) returns None rather than raising StopIteration/ValueError, so one
    bad cell id in a column degrades to NULL instead of killing the stage."""
    try:
        return parse(cell_id)
    except Exception:  # noqa: BLE001 — StopIteration (bad prefix) or ValueError (bad body)
        return None
```

In `_custom.py`, add a resolution-first split:

```python
def point_to_cell_id_or_none(conf, x: float, y: float, resolution: int):
    """Resolution PARAMETER still raises ValueError; NaN / out-of-bounds coordinate DATA
    returns None so one bad row degrades to NULL."""
    if resolution > conf.max_resolution:                       # PARAMETER -> raise
        raise ValueError(
            f"gbx_custom: resolution ({resolution}) exceeds maximum "
            f"resolution of {conf.max_resolution}."
        )
    try:
        return point_to_cell_id(conf, x, y, resolution)        # DATA (NaN/bounds) -> None
    except ValueError:
        return None
```

In `_quadbin.py`, extend the module docstring with the clamp note (out-of-range latitude clamped to ±85.05112878°, longitude to ±180°; intended web-mercator behavior, not a degrade — distinct from BNG/Custom which return NULL out-of-extent).

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:python --path python/geobrix/test/pygx/test_gridx_error_core.py`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/_bng.py python/geobrix/src/databricks/labs/gbx/pygx/_custom.py python/geobrix/src/databricks/labs/gbx/pygx/_quadbin.py python/geobrix/test/pygx/test_gridx_error_core.py
git commit -m "feat(pygx): never-error BNG parse + Custom data/param split"
```

---

### Task 9: Light pygx UDFs — wire degrade into the registered surface

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/functions.py` (scalar UDFs → None on bad data; aggregator UDFs skip bad member; `@udtf` classes yield nothing on bad input)
- Test: `python/geobrix/test/pygx/test_gridx_error_udf.py`

**Interfaces:**
- Consumes: `_bng.parse_safe`, `_custom.point_to_cell_id_or_none` (Task 8).
- The registered SQL surface must mirror the heavy contract: bad data → NULL (scalar/geom/array/struct), skip bad member (agg), zero rows (udtf); bad parameter → raise.

- [ ] **Step 1: Write the failing test**

```python
import pytest
from pyspark.sql import SparkSession
from databricks.labs.gbx.pygx import functions as gx


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("pygx-err").getOrCreate()
    gx.register(s)
    yield s


def test_bng_aswkb_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_aswkb('!!') AS g").first()
    assert r["g"] is None

def test_bng_kringexplode_zero_rows_on_bad_cellid(spark):
    df = spark.sql("SELECT * FROM (SELECT '!!' AS c) LATERAL VIEW gbx_bng_kringexplode(c, 1) t AS cellid")
    assert df.count() == 0

def test_bng_pointascell_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql("SELECT gbx_bng_pointascell('POINT (530000 180000)', 'bogus')").first()
```

(Verify the exact `LATERAL VIEW`/`@udtf` invocation form the pygx udtfs register with — the recon showed `@udtf(returnType="cellid: string")`; use the registered SQL name and matching call syntax.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gbx:test:python --path python/geobrix/test/pygx/test_gridx_error_udf.py`
Expected: FAIL — scalar UDF currently raises / udtf currently errors on bad input.

- [ ] **Step 3: Write minimal implementation**

Route the scalar UDF callables through the safe helpers. Example `_bng_aswkb` (plain `@f.udf`):

```python
def _bng_aswkb(cellid):
    if cellid is None:
        return None
    cid = _bng.parse_safe(cellid)          # DATA -> None
    if cid is None:
        return None
    try:
        return _bng.cell_id_to_wkb(cid)    # (real accessor name)
    except Exception:  # noqa: BLE001
        return None
```

For `bng_pointascell` keep `_norm_res(r)` (PARAMETER — raises on bad resolution) OUTSIDE the try; wrap only the geometry+point math. For the `@udtf` explode classes, guard the body so a bad cell id yields nothing:

```python
@udtf(returnType="cellid: string")
class _BngKRingExplode:
    def eval(self, cellid, k):
        if cellid is None or k is None:
            return
        cid = _bng.parse_safe(cellid)      # DATA -> yield nothing
        if cid is None:
            return
        for c in _bng.k_ring_str_from_id(cid, int(k)):
            yield (c,)
```

For the aggregator UDFs (`_bng_cellunion_agg_udf` etc.), filter members whose cell id fails `parse_safe` from the fold input (skip bad member). For Custom scalar UDFs, use `point_to_cell_id_or_none`.

- [ ] **Step 4: Run test to verify it passes**

Run: `gbx:test:python --path python/geobrix/test/pygx/test_gridx_error_udf.py`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/functions.py python/geobrix/test/pygx/test_gridx_error_udf.py
git commit -m "fix(pygx): registered GridX UDFs degrade bad data, keep param raises"
```

---

### Task 10: Cross-tier parity suite (the spine)

**Files:**
- Create: `python/geobrix/test/pygx/test_gridx_error_parity.py` (marked `pytest.mark.integration` — heavy SQL against the staged JAR + light through the pygx UDFs)

**Interfaces:**
- Consumes: registered heavy `gbx_bng_*`/`gbx_quadbin_*`/`gbx_custom_*` (needs staged JAR) and the light pygx UDFs. Follows the `test/pyvx/test_crs_parity.py` harness (a `heavy` fixture on `spark_with_jar`, a `light` fixture importing the pygx module). Runs ONLY under the JAR-staging gate (`gbx:test:parity`), not a plain `gbx:test:python`.

- [ ] **Step 1: Write the failing test** (copy `heavy`/`light`/`_sql_lit` fixtures verbatim from `test_crs_parity.py`)

```python
import pytest
pytestmark = pytest.mark.integration

def test_bng_malformed_cellid_null_both_tiers(heavy, light):
    assert heavy.sql("SELECT gbx_bng_aswkb('!!') g").first()["g"] is None
    assert light.sql("SELECT gbx_bng_aswkb('!!') g").first()["g"] is None

def test_bng_bad_resolution_raises_both_tiers(heavy, light):
    with pytest.raises(Exception):
        heavy.sql("SELECT gbx_bng_pointascell('POINT (530000 180000)', 'bogus')").first()
    with pytest.raises(Exception):
        light.sql("SELECT gbx_bng_pointascell('POINT (530000 180000)', 'bogus')").first()

def test_quadbin_out_of_range_lat_clamped_not_null(heavy, light):
    hv = heavy.sql("SELECT gbx_quadbin_pointascell(10.0, 89.0, 10) c").first()["c"]
    lv = light.sql("SELECT gbx_quadbin_pointascell(10.0, 89.0, 10) c").first()["c"]
    assert hv is not None and lv is not None
    assert hv == lv

def test_bng_kringexplode_zero_rows_both_tiers(heavy, light):
    q = "SELECT * FROM (SELECT '!!' c) LATERAL VIEW gbx_bng_kringexplode(c,1) t AS cellid"
    assert heavy.sql(q).count() == 0
    assert light.sql(q).count() == 0
```

Verify each `gbx_*` function's real SQL name + arity before finalizing; adjust exprs to registered signatures.

- [ ] **Step 2: Run the gate to verify it fails** (assertions, not a missing-JAR error — the gate rebuilds+stages)

Run: `bash scripts/commands/gbx-test-parity.sh -k gridx_error --log gridx-parity.log`

- [ ] **Step 3: Implementation** — no new product code; if a case fails, fix the diverging tier in its owning task's file, not here.

- [ ] **Step 4: Run the full gate**

Run: `bash scripts/commands/gbx-test-parity.sh -k gridx_error --log gridx-parity.log`
Expected: PASS — every degenerate case agrees across tiers.

- [ ] **Step 5: Commit** — `git add` the new parity test, then commit with subject `test(gridx): cross-tier parity for the error-handling contract`.

---

### Task 11: Error Handling docs — GridX section (REQUIRED deliverable)

**Files:**
- Modify: `docs/docs/api/error-handling.mdx` (add a GridX section after the VectorX section, before "Catching degraded rows")

**Interfaces:** documents shipped behavior. The implementation is NOT complete until this lands and the page builds clean.

- [ ] **Step 1: Add the GridX section.** Content (fill the SQL fences with proper triple-backticks):
  - Lead: GridX (BNG, Quadbin, Custom) returns cell ids/geometries/arrays/structs — no metadata carrier — so `NULL` is the single bad-data degrade signal, same as VectorX.
  - **Bad cell-id or geometry data** → `NULL` (example: `SELECT gbx_bng_aswkb(cellid) ...` — malformed cellid rows produce NULL, query continues). Aggregators skip a corrupt member; generators emit zero rows for a bad input cell (mention `LEFT JOIN LATERAL` to see which inputs produced nothing).
  - **Bad resolution or grid argument** → raises (example: `gbx_bng_pointascell(geom, 99)` raises — parameter problem, not per-row data).
  - **Quadbin latitude clamp**: `gbx_quadbin_pointascell` follows the web-mercator convention — a latitude beyond ±85.05112878° is clamped to that limit (longitude to ±180°), returning a real cell rather than `NULL`. A point at lat 89° yields the same cell as one at 85.05112878°. Intentional; differs from BNG and Custom, which return `NULL` for a coordinate outside their valid extent.

- [ ] **Step 2: Build the docs**

Run: `cd docs && npm run build`
Expected: builds clean, no broken-link warnings.

- [ ] **Step 3: Verify no internals leak**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/error-handling.mdx`
Expected: no output.

- [ ] **Step 4: Commit** — `git add docs/docs/api/error-handling.mdx`, commit with subject `docs(error-handling): add GridX section + Quadbin clamp note`.

---

### Task 12: Re-confirm the rasterize_agg alignment tests (verify, do NOT re-implement)

**Files:** run only — `python/geobrix/test/pyrx/test_{h3,quadbin,bng}_rasterize_agg.py`

**Interfaces:** none. The null-cellid misalignment is ALREADY fixed (zip-then-filter + `pd.notna`); this proves it stayed fixed.

- [ ] **Step 1: Run the three alignment suites**

Run: `gbx:test:python --path python/geobrix/test/pyrx/test_h3_rasterize_agg.py --path python/geobrix/test/pyrx/test_quadbin_rasterize_agg.py --path python/geobrix/test/pyrx/test_bng_rasterize_agg.py --log gridx-rasterize-agg.log`
Expected: PASS — including the `*_null_cellid_value_alignment` nodes.

- [ ] **Step 2:** If green, no commit (nothing changed). If RED, STOP and report — a regression in already-shipped code is out of this plan's scope.

---

## Self-Review

**1. Spec coverage.** Every spec section maps to a task: guard mechanism → T1; BNG.parse stage-killer → T2; scalar/geometry/array/struct degrade → T3+T4; aggregators skip / generators zero-rows → T5; Custom parameter/data split → T6; Quadbin clamp preserved+documented → T7 (Scala) + T11 (docs) + T8 (light docstring); light pygx mirror → T8+T9; cross-tier parity spine → T10; docs required deliverable → T11; rasterize_agg verified-not-reworked → T12. No gap.

**2. Placeholder scan.** Each code step carries real code. Three tasks (T6, T7, T8) explicitly say "verify the real constructor/signature before writing" for `GridConf`/`CustomGridConf`/`Quadbin.pointToCell` — that is a real instruction to the implementer (the exact ctor args were not read this session), not a placeholder for logic.

**3. Type consistency.** `parseOrNull`/`parse_safe` return nullable (`java.lang.Long`/`Optional[int]`); callers null-check before unboxing. Boxed accessors (`BNG_CellArea`→`java.lang.Double`, `BNG_Distance`/`BNG_EuclideanDistance`→`java.lang.Long`) keep `dataType` `DoubleType`/`LongType` and `nullable=true`. `safeEval[T](nullValue: T)` is used consistently with the shape's null. Generator guards return `Iterator.empty`/`Seq.empty` matching each `eval`'s `IterableOnce[InternalRow]` return.

**Open item for the implementer (verify before coding):** the real factory signatures of `GridConf` / `CustomGridConf` / `CustomGridSystem` (T6, T8) and `Quadbin.pointToCell` name+arity (T7). These were not read this session; the test scaffolds assume plausible shapes and must be reconciled to the actual source.

---
