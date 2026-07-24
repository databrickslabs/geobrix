# Raster BNG + Quadbin (H3 parity) — Phase 1 (Heavy/Scala) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the 9 heavy-tier (Scala/GDAL) RasterX functions that bring the quadbin and BNG discrete-grid families to full parity with the H3 raster surface.

**Architecture:** Approach B from the spec — parallel per-grid expression families that clone the existing H3/quadbin shapes, sharing the raster→grid hot loop and reusing `Long`-keyed accumulators for all grids. BNG differs only by (a) reprojecting its input raster to EPSG:27700 before pixel→cell math and (b) rendering `String` cell ids at the output boundary via `BNG.format`. This plan is Phase 1 only (heavy tier); light-tier phases are separate plans (Phase 3 BNG is gated on pygx BNG phase 2).

**Tech Stack:** Scala 2.13.16, Spark 4.0.0, Java 17, GDAL Java bindings. All build/test runs inside the `geobrix-dev` Docker container (`gbx:docker:*`, `gbx:test:scala`).

**Spec:** `docs/superpowers/specs/2026-07-24-raster-bng-quadbin-h3-parity-design.md`

**Closes:** [databrickslabs/geobrix#49](https://github.com/databrickslabs/geobrix/issues/49) — customer request for Mosaic-style BNG tessellate on rasters (CV image tiling). The `gbx_rst_bng_tessellate` deliverable (Tasks 3–4) is that function; its #49 acceptance criteria are in spec §1.1 and repeated in the Task 3/4 briefs.

## Global Constraints

- **No aliases.** One canonical name per function; fix upstream, never add an alias.
- **Cross-language naming:** SQL name `gbx_<scala-api>`; Scala `override def name` is the SQL literal. Registered names go in `docs/tests-function-info/registered_functions.txt`.
- **Binding parity enforced:** every new function must appear as (1) a Scala `override def name` literal, (2) a Python `functions.py` binding, (3) a `function-info.json` key. `gbx:test:bindings` fails otherwise. (Python bindings + function-info are Tasks 8–9.)
- **BNG resolution contract:** integer indices ±1..±6 (1=100km … 6=1m; negatives=quadrants) or string keys from `BNG.resolutionMap` (`"1km"`, `"100m"`, …), resolved via `BNG.getResolution(res: Any): Int`. Never metres-as-Int.
- **CRS contract:** quadbin/H3 assume raster is EPSG:4326 (caller reprojects upstream — no change). BNG raster fns auto-reproject the input to EPSG:27700 internally via `RasterProject.project`, using **nearest-neighbour** resampling; pixels outside the GB extent are silently dropped.
- **Empty-cell / NoData semantics (spec §2.6, SAFETY-CRITICAL):**
  - `rastertogrid`: a cell is emitted only when ≥1 **valid** pixel lands in it (`maskBuf(idx) != 0`). Never emit a zero-valid-pixel cell; never substitute `0.0`/`NaN`/sentinel/NULL for an absent cell.
  - `rasterize_agg`: the output raster MUST be built through `VectorRasterBridge.buildEmptyRaster` (which calls `SetNoDataValue(-9999.0)` then `Fill`), so `-9999.0` is registered band NoData — never a bare pixel value.
- **GDAL thread-safety:** register GDAL/OGR only via `GDALManager` guards (`RST_ExpressionUtil.init` already does this in the eval paths). Never raw `gdal.AllRegister()` per task.
- **GDAL resource management:** release every `Dataset`/`Band` via `RasterDriver.releaseDataset(ds)` in `try/finally` (the shared eval helpers already do this).
- **Docker:** dispatch long Scala builds/tests as a Task subagent; never run inline. Give a progress line ~every 30s while a suite runs.

## File Structure

**Create (Scala main):**
- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGrid.scala` — shared BNG raster→grid object (String-output, Long-keyed accumulator, 27700 warp). Mirrors `RST_Quadbin_RasterToGrid`.
- `.../grid/RST_BNG_RasterToGridAvg.scala`, `…Count.scala`, `…Max.scala`, `…Min.scala`, `…Median.scala` — 5 reducer expressions.
- `.../generators/RST_Quadbin_Tessellate.scala`, `.../generators/RST_BNG_Tessellate.scala` — 2 tessellate generators.
- `.../agg/RST_Quadbin_RasterizeAgg.scala`, `.../agg/RST_BNG_RasterizeAgg.scala` — 2 UDAFs.

**Modify (Scala main):**
- `.../rasterx/operations/RasterTessellate.scala` — add `tessellateQuadbinIter` + `tessellateBngIter` (+ private covering/centroid helpers + a `getTileQuadbin`/`getTileBng`).
- `.../rasterx/functions.scala` — `rd.register(...)` × 9 and Scala `functions` column wrappers × 9.
- `docs/tests-function-info/registered_functions.txt` — +9 names.

**Modify (Python bindings):**
- `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py` — +9 `call_function` wrappers.

**Create/Modify (tests):**
- `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGridTest.scala`
- `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_Quadbin_TessellateTest.scala` (+ BNG tessellate cases)
- `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_GridRasterizeAggTest.scala`
- Doc-test SQL examples: `docs/tests/python/api/rasterx_functions_sql.py` (feeds function-info).

**Modify (docs):**
- `docs/docs/api/raster-functions.mdx`, `docs/docs/api/execution-tiers.mdx`, `docs/docs/api/performance.mdx`, `docs/docs/beta-release-notes.mdx`, `README.md` (badges).

---

## Task 1: BNG raster→grid shared object

**Files:**
- Create: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGrid.scala`
- Reference (read, do not modify): `.../grid/RST_Quadbin_RasterToGrid.scala`, `.../gridx/grid/BNG.scala`, `.../rasterx/operations/RasterProject.scala`

**Interfaces:**
- Consumes: `BNG.getResolution(res: Any): Int`, `BNG.pointToCellID(e: Double, n: Double, res: Int): Long`, `BNG.format(id: Long): String`; `RasterProject.project(ds, options, dstSR): (Dataset, Map[String,String])`.
- Produces: `RST_BNG_RasterToGrid.execute[T](ds: Dataset, resolution: Int, fAgg: mutable.ArrayBuffer[Double] => T): Array[Array[(String, T)]]` and `RST_BNG_RasterToGrid.eval[T](row, resolution, conf, rdt, execute): ArrayData` — note the cell id type is **String** (vs quadbin's Long).

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGridTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.test.RasterXTestBase // if a shared base exists; else mixin used by RST_Quadbin_RasterToGridTest
import org.gdal.gdal.gdal
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

class RST_BNG_RasterToGridTest extends AnyFunSuite {

    // A 2x2 EPSG:27700 raster centred on London (530000,180000), 100m pixels,
    // all pixels valid, values 1,2,3,4. Built in-memory via MEM driver.
    private def londonDs = {
        gdal.AllRegister() // test-only; production uses GDALManager
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 2, 2, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(530000.0, 100.0, 0.0, 180200.0, 0.0, -100.0))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(27700)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 2, 2, Array(1.0, 2.0, 3.0, 4.0))
        ds.FlushCache(); ds
    }

    test("bng rastertogrid: emits String cell ids and averages valid pixels") {
        val ds = londonDs
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out: Array[Array[(String, Double)]] =
            RST_BNG_RasterToGrid.execute(ds, resolution = 3, fAgg = meanF) // 3 = 1km
        RasterDriver.releaseDataset(ds)
        val cells = out.flatten
        assert(cells.nonEmpty)
        assert(cells.forall(_._1.matches("^[A-Z]{2}\\d*$"))) // BNG string form
        // all four pixels fall in the same 1km cell here -> mean 2.5
        assert(cells.map(_._1).distinct.length == 1)
        assert(math.abs(cells.head._2 - 2.5) < 1e-9)
    }

    test("bng rastertogrid: zero-valid-pixel cell is never emitted (spec 2.6)") {
        // Mask all pixels nodata -> no cells at all.
        val ds = londonDs
        ds.GetRasterBand(1).SetNoDataValue(1.0)
        ds.GetRasterBand(1).Fill(1.0)
        val meanF = (v: ArrayBuffer[Double]) => v.sum / v.length
        val out = RST_BNG_RasterToGrid.execute(ds, 3, meanF)
        RasterDriver.releaseDataset(ds)
        assert(out.flatten.isEmpty, "all-nodata raster must yield no cells")
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Dispatch a Task subagent to run in Docker:
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_BNG_RasterToGridTest' --log bng-r2g.log
```
Expected: FAIL — `RST_BNG_RasterToGrid` not found (does not compile / unresolved).

- [ ] **Step 3: Write the implementation**

Create `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGrid.scala`. This mirrors `RST_Quadbin_RasterToGrid` but: keys the accumulator on `Long` (BNG internal id), reprojects the dataset to 27700 up front, computes eastings/northings under the **warped** geotransform, and renders `BNG.format(cellId): String` into the output tuple.

```scala
package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterProject
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.gdal.osr.SpatialReference

import scala.collection.mutable

/** Shared helper for `RST_BNG_RasterToGrid*` expressions — mirrors `RST_Quadbin_RasterToGrid`
  * but delegates per-pixel cell math to [[BNG.pointToCellID]] (EPSG:27700 eastings/northings).
  *
  * Unlike the H3/quadbin families (whose input contract is EPSG:4326 lon/lat), BNG has no lon/lat
  * input path, so the raster is reprojected to EPSG:27700 up front via [[RasterProject]] using
  * nearest-neighbour resampling. Cell ids are `Long` internally and rendered to the user-facing
  * BNG `String` via [[BNG.format]] at the output boundary. Pixels outside the GB extent are dropped.
  */
object RST_BNG_RasterToGrid {

    /** Compute the BNG cell id for the centroid of pixel (x, y) under geotransform `gt` (EPSG:27700). */
    def cellPixel(gt: Array[Double], x: Int, y: Int, resolution: Int): Long = {
        val offset = 0.5
        val xOffset = offset + x
        val yOffset = offset + y
        val eGeo = gt(0) + xOffset * gt(1) + yOffset * gt(2)
        val nGeo = gt(3) + xOffset * gt(4) + yOffset * gt(5)
        BNG.pointToCellID(eGeo, nGeo, resolution)
    }

    def execute[T](
        ds: Dataset,
        resolution: Int,
        fAgg: mutable.ArrayBuffer[Double] => T
    ): Array[Array[(String, T)]] = {
        require(
          BNG.resolutions.contains(resolution),
          s"raster→bng: resolution must be one of ${BNG.resolutions.toSeq.sorted.mkString(", ")}; got $resolution"
        )

        // Reproject to EPSG:27700 (nearest-neighbour) unless already there.
        val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(27700)
        val srcWkt = ds.GetProjection()
        val (workDs, reprojected) =
            if (srcWkt != null && srcWkt.nonEmpty && {
                    val s = new SpatialReference(); s.ImportFromWkt(Array(srcWkt)); val same = s.IsSame(dstSR) == 1; s.delete(); same
                }) (ds, false)
            else {
                val (p, _) = RasterProject.project(ds, Map("resampling" -> "near"), dstSR)
                (p, true)
            }
        dstSR.delete()

        try {
            val gt = workDs.GetGeoTransform
            val xSize = workDs.getRasterXSize
            val ySize = workDs.getRasterYSize
            val nPix = xSize * ySize
            val bands = workDs.getRasterCount

            val bandBuf = new Array[Double](nPix)
            val maskBuf = new Array[Byte](nPix)

            (1 to bands).iterator.map { bi =>
                val b = workDs.GetRasterBand(bi)
                val m = b.GetMaskBand()
                b.ReadRaster(0, 0, xSize, ySize, bandBuf)
                m.ReadRaster(0, 0, xSize, ySize, maskBuf)

                var valid = 0; var i = 0
                while (i < nPix) { if (maskBuf(i) != 0) valid += 1; i += 1 }

                val acc = new mutable.LongMap[mutable.ArrayBuffer[Double]](valid)
                var y = 0; var idx = 0
                while (y < ySize) {
                    var x = 0
                    while (x < xSize) {
                        if (maskBuf(idx) != 0) {
                            val cell = cellPixel(gt, x, y, resolution) // Long id
                            val buf = acc.getOrElseUpdate(cell, new mutable.ArrayBuffer)
                            buf += bandBuf(idx)
                        }
                        idx += 1; x += 1
                    }
                    y += 1
                }

                val out = new Array[(String, T)](acc.size)
                var j = 0
                acc.foreach { case (cell, buf) => out(j) = (BNG.format(cell), fAgg(buf)); j += 1 }
                out
            }.toArray
        } finally {
            if (reprojected) RasterDriver.releaseDataset(workDs)
        }
    }

    def eval[T](
        row: InternalRow,
        resolution: Int,
        conf: UTF8String,
        rdt: DataType,
        execute: (Dataset, Int) => Array[Array[(String, T)]]
    ): ArrayData = {
        val exprConf = ExpressionConfig.fromB64(conf.toString)
        RST_ExpressionUtil.init(exprConf)
        val ds = RasterSerializationUtil.rowToDS(row, rdt)
        val result = execute(ds, resolution)
        RasterDriver.releaseDataset(ds)
        ArrayData.toArrayData(
          result.map(band =>
              ArrayData.toArrayData(
                band.map { case (cellId, measure) =>
                    InternalRow.fromSeq(Seq(UTF8String.fromString(cellId), measure))
                }
              )
          )
        )
    }
}
```

Note: verify `RasterProject.project`'s options key for resampling against its source (Step relies on it honouring `"resampling" -> "near"`; if the key differs, adapt to the actual signature — read `RasterProject.scala` and `RST_ToWebMercator.scala` which calls it). If `RasterProject` takes a gdalwarp command string instead, build `s"gdalwarp -t_srs EPSG:27700 -r near"` as `RST_ToWebMercator` does.

- [ ] **Step 4: Run the test to verify it passes**

Dispatch Task subagent:
```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_BNG_RasterToGridTest' --log bng-r2g.log
```
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGrid.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGridTest.scala
git commit -m "feat(rasterx): BNG raster-to-grid shared object (27700 warp, String cell ids)"
```

---

## Task 2: BNG rastertogrid reducer expressions (×5)

**Files:**
- Create: `.../grid/RST_BNG_RasterToGridAvg.scala`, `…Count.scala`, `…Max.scala`, `…Min.scala`, `…Median.scala`
- Reference: `.../grid/RST_Quadbin_RasterToGridAvg.scala` (and its Count/Max/Min/Median siblings)

**Interfaces:**
- Consumes: `RST_BNG_RasterToGrid.execute`/`.eval` (Task 1).
- Produces: 5 `WithExpressionInfo` objects with `name` = `gbx_rst_bng_rastertogrid{avg,count,max,min,median}`; each `dataType = ArrayType(ArrayType(StructType(cellID: StringType, measure: <Double|Int>)))`.

- [ ] **Step 1: Write the failing test**

Add to `RST_BNG_RasterToGridTest.scala`:

```scala
test("bng rastertogrid reducers: min/max/count/median on the london cell") {
    val ds = londonDs
    import scala.collection.mutable.ArrayBuffer
    val minF = (v: ArrayBuffer[Double]) => v.min
    val maxF = (v: ArrayBuffer[Double]) => v.max
    val cntF = (v: ArrayBuffer[Double]) => v.length
    val medF = (v: ArrayBuffer[Double]) => { val s = v.sorted; val m = s.length/2; if (s.length%2==0) (s(m-1)+s(m))/2.0 else s(m) }
    assert(RST_BNG_RasterToGrid.execute(ds, 3, minF).flatten.head._2 == 1.0)
    assert(RST_BNG_RasterToGrid.execute(ds, 3, maxF).flatten.head._2 == 4.0)
    assert(RST_BNG_RasterToGrid.execute(ds, 3, cntF).flatten.head._2 == 4)
    assert(math.abs(RST_BNG_RasterToGrid.execute(ds, 3, medF).flatten.head._2 - 2.5) < 1e-9)
    RasterDriver.releaseDataset(ds)
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_BNG_RasterToGridTest' --log bng-r2g.log
```
Expected: PASS actually (this test only exercises Task-1 `execute`). Its purpose is to lock reducer math; the reducer *expressions* are covered by the integration test in Task 7. If you prefer a failing gate here, assert on the expression objects' `name` instead:
```scala
test("bng reducer names are canonical") {
    assert(RST_BNG_RasterToGridAvg.name == "gbx_rst_bng_rastertogridavg")
    assert(RST_BNG_RasterToGridMedian.name == "gbx_rst_bng_rastertogridmedian")
}
```
Run: expected FAIL (objects undefined).

- [ ] **Step 3: Write the 5 reducer files**

`RST_BNG_RasterToGridAvg.scala` (clone the quadbin Avg verbatim, swapping `Quadbin`→`BNG`, `LongType`→`StringType` for `cellID`, and `Long` resolution handling; keep the `execute` reducer lambdas identical):

```scala
package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.gridx.grid.BNG
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import scala.collection.mutable.ArrayBuffer

/** Returns the average raster value within each BNG grid cell. */
case class RST_BNG_RasterToGridAvg(tileExpr: Expression, resolution: Expression) extends InvokedExpression {
    private def rasterType = RST_ExpressionUtil.rasterType(tileExpr)
    override def children: Seq[Expression] = Seq(tileExpr, resolution, ExpressionConfigExpr())
    override def dataType: DataType =
        ArrayType(ArrayType(StructType(Seq(StructField("cellID", StringType), StructField("measure", DoubleType)))))
    override def nullable: Boolean = true
    override def prettyName: String = RST_BNG_RasterToGridAvg.name
    override def replacement: Expression = rstInvoke(RST_BNG_RasterToGridAvg, rasterType)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))
}

object RST_BNG_RasterToGridAvg extends WithExpressionInfo {
    def evalPath(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, StringType)
    def evalBinary(row: InternalRow, resolution: Int, conf: UTF8String): ArrayData = doInvoke(row, resolution, conf, BinaryType)
    def evalPath(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = evalPath(row, resolution.toInt, conf)
    def evalBinary(row: InternalRow, resolution: Long, conf: UTF8String): ArrayData = evalBinary(row, resolution.toInt, conf)
    // BNG string-key resolution ("1km" etc.) — PySpark may send a UTF8String.
    def evalPath(row: InternalRow, resolution: UTF8String, conf: UTF8String): ArrayData = evalPath(row, BNG.getResolution(resolution), conf)
    def evalBinary(row: InternalRow, resolution: UTF8String, conf: UTF8String): ArrayData = evalBinary(row, BNG.getResolution(resolution), conf)

    private def doInvoke(row: InternalRow, resolution: Int, conf: UTF8String, rdt: DataType): ArrayData =
        Option(RST_ErrorHandler.safeEval(() => RST_BNG_RasterToGrid.eval[Double](row, resolution, conf, rdt, this.execute), row, rdt, conf))
            .map(_.asInstanceOf[ArrayData]).orNull

    def execute(ds: Dataset, resolution: Int): Array[Array[(String, Double)]] = {
        val meanF = (values: ArrayBuffer[Double]) => values.sum / values.length
        RST_BNG_RasterToGrid.execute(ds, resolution, meanF)
    }
    override def name: String = "gbx_rst_bng_rastertogridavg"
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_BNG_RasterToGridAvg(c(0), c(1))
}
```

Create the other four by the same clone, changing only: class/object name, `name` literal, the `execute` reducer lambda, and (for Count) the type param `[Int]` + `measure` field type `IntegerType`:
- `…Count`: `dataType` struct `measure: IntegerType`; `execute` returns `Array[Array[(String, Int)]]` with `(values) => values.length`, `eval[Int]`, `doInvoke` passes `eval[Int]`. Match `RST_Quadbin_RasterToGridCount` exactly for the Int plumbing.
- `…Max`: `(values) => values.max`. `…Min`: `(values) => values.min`.
- `…Median`: the median lambda from the test in Step 1.

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_BNG_RasterToGridTest' --log bng-r2g.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGrid{Avg,Count,Max,Min,Median}.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_BNG_RasterToGridTest.scala
git commit -m "feat(rasterx): BNG rastertogrid reducers (avg/count/max/min/median)"
```

---

## Task 3: Quadbin + BNG tessellate iterators in RasterTessellate

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/RasterTessellate.scala`
- Reference: same file's `tessellateH3Iter`, `getTile`, `tessellateH3CoveringIter`, `tessellateH3CentroidIter`; `Quadbin.cellToBoundary`/bbox helpers and `BNG.cellIdToGeometry` for cell polygons.

**Interfaces:**
- Consumes: `Quadbin` cell→geometry (verify method name in `Quadbin.scala`: bbox is `(lonMin,latMin,lonMax,latMax)`; build a JTS polygon from it), `BNG.cellIdToGeometry(cell: Long): Geometry`, `BNG.pointToCellID`, `BNG.format`, `Quadbin.pointToCell`.
- Produces: `RasterTessellate.tessellateQuadbinIter(ds, options, resolution, mode): Iterator[(Long, Dataset, Map[String,String])]` and `tessellateBngIter(ds, options, resolution, mode): Iterator[(String, Dataset, Map[String,String])]`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_Quadbin_TessellateTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.RasterTessellate
import org.gdal.gdal.gdal
import org.scalatest.funsuite.AnyFunSuite

class RST_Quadbin_TessellateTest extends AnyFunSuite {

    private def wgs84Ds = { // 4x4 EPSG:4326 raster over a small lon/lat box, all valid
        gdal.AllRegister()
        val drv = gdal.GetDriverByName("MEM")
        val ds = drv.Create("", 4, 4, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(-0.2, 0.1, 0.0, 51.6, 0.0, -0.1))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).Fill(7.0)
        ds.FlushCache(); ds
    }

    test("quadbin tessellate covering: yields >=1 chip, each tagged with its cell id") {
        val ds = wgs84Ds
        val it = RasterTessellate.tessellateQuadbinIter(ds, Map.empty, resolution = 10, mode = "covering")
        val chips = it.toList
        assert(chips.nonEmpty)
        chips.foreach { case (cell, d, _) => assert(cell != 0L); RasterDriver.releaseDataset(d) }
        RasterDriver.releaseDataset(ds)
    }

    test("quadbin tessellate rejects unknown mode") {
        val ds = wgs84Ds
        val ex = intercept[IllegalArgumentException] {
            RasterTessellate.tessellateQuadbinIter(ds, Map.empty, 10, "nonsense").toList
        }
        assert(ex.getMessage.toLowerCase.contains("mode"))
        RasterDriver.releaseDataset(ds)
    }
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_Quadbin_TessellateTest' --log tess.log
```
Expected: FAIL — `tessellateQuadbinIter` not a member of `RasterTessellate`.

- [ ] **Step 3: Implement the iterators**

In `RasterTessellate.scala`, add (mirroring the H3 trio). Reuse the existing covering/centroid structure; the only per-grid differences are (a) the cell-set enumeration for a bbox and (b) the cell→geometry function. Extract a private generic `getTileGeneric[C](ds, options, cell: C, cellGeom: Geometry, tagId: String)` if the H3 `getTile` body is easy to parameterize; otherwise add `getTileQuadbin`/`getTileBng` copies that differ only in the `RASTERX_CELL_ID` tag string (`cell.toString` for quadbin, `BNG.format(cell)` for BNG) and the geometry source.

```scala
// --- Quadbin ---
def tessellateQuadbinIter(
    ds: Dataset, options: Map[String, String], resolution: Int, mode: String = "covering"
): Iterator[(Long, Dataset, Map[String, String])] = {
    require(Modes.contains(mode), s"gbx_rst_quadbin_tessellate mode must be one of ${Modes.mkString(", ")}; got '$mode'")
    // Enumerate candidate cells covering the raster bbox via Quadbin.polyfill on the bbox
    // (verify Quadbin.polyfill signature: takes bbox tuple + zoom, returns Seq[Long]).
    // For covering: keep cells whose geometry intersects the raster bbox (as H3 covering does).
    // For centroid: single-assign each valid pixel to Quadbin.pointToCell(lon,lat,z) (as H3 centroid does).
    // Build cell geometry from Quadbin.cellToBBox -> JTS polygon.
    ... // structure copied from tessellateH3Iter / tessellateH3CoveringIter / tessellateH3CentroidIter
}

// --- BNG ---
def tessellateBngIter(
    ds: Dataset, options: Map[String, String], resolution: Int, mode: String = "covering"
): Iterator[(String, Dataset, Map[String, String])] = {
    require(Modes.contains(mode), s"gbx_rst_bng_tessellate mode must be one of ${Modes.mkString(", ")}; got '$mode'")
    // BNG: reproject ds to EPSG:27700 first (RasterProject, nearest). Cell geometry = BNG.cellIdToGeometry(cell:Long),
    // in EPSG:27700 (same CRS as the warped bbox). Enumerate via BNG.polyfill(bbox, resolution) if available,
    // else derive the easting/northing cell range from the bbox and resolution. Emit BNG.format(cell) as the id.
    ...
}
```

Because the H3 covering/centroid helpers are ~120 lines, the implementer MUST read `tessellateH3Iter`, `tessellateH3CoveringIter`, `tessellateH3CentroidIter`, and `getTile` in full and clone their control flow, substituting only the cell-enumeration + cell-geometry + id-tag. Do not invent a new tessellation algorithm. Keep `covering` = geometric-overlap keep-test (per the documented decision at the top of `getTile`), `centroid` = pixel-centroid single-assign.

**Issue #49 acceptance (BNG tessellate is the originating customer ask — spec §1.1):**
- The BNG iterator MUST build cell geometry directly from `BNG.cellIdToGeometry(cell: Long)` and enumerate cells for the raster bbox itself. It MUST NOT route through the vector `bng_tessellate` expression — that path carries inherited Mosaic bugs (mosaic#423 spurious POINT/LINESTRING chips; mosaic#434/#580 half-size cells) tracked separately for pygx phase 2. The customer explicitly hit "issues with the bng_tessellate function from GridX"; the raster path must sidestep them, not reuse them.
- `covering` mode yields one clipped chip per BNG cell overlapping the raster — uniform-size image tiles, which is the customer's CV use case.
- Add a Task-3 test asserting the BNG iterator emits ≥1 chip for a GB raster and each chip is tagged with its BNG `String` id (via `RASTERX_CELL_ID`), and that only areal geometry is used (no POINT/LINESTRING chip leakage).

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_Quadbin_TessellateTest' --log tess.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/operations/RasterTessellate.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_Quadbin_TessellateTest.scala
git commit -m "feat(rasterx): quadbin + BNG tessellate iterators in RasterTessellate"
```

---

## Task 4: Quadbin + BNG tessellate generator expressions

**Files:**
- Create: `.../generators/RST_Quadbin_Tessellate.scala`, `.../generators/RST_BNG_Tessellate.scala`
- Reference: `.../generators/RST_H3_Tessellate.scala`

**Interfaces:**
- Consumes: `RasterTessellate.tessellateQuadbinIter`/`tessellateBngIter` (Task 3).
- Produces: `RST_Quadbin_Tessellate` / `RST_BNG_Tessellate` case classes + companions; `name` = `gbx_rst_quadbin_tessellate` / `gbx_rst_bng_tessellate`; `elementSchema = StructType(Array(StructField("tile", tileType)))`; 2-or-3-arg builder, default `mode="covering"`.

- [ ] **Step 1: Write the failing test**

Add to `RST_Quadbin_TessellateTest.scala`:

```scala
test("generator names + default mode arity") {
    assert(RST_Quadbin_Tessellate.name == "gbx_rst_quadbin_tessellate")
    assert(RST_BNG_Tessellate.name == "gbx_rst_bng_tessellate")
    // 2-arg builder defaults mode to "covering"
    import org.apache.spark.sql.catalyst.expressions.Literal
    val e = RST_Quadbin_Tessellate.builder()(Seq(Literal("t"), Literal(10)))
    assert(e.isInstanceOf[RST_Quadbin_Tessellate])
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_Quadbin_TessellateTest' --log tess.log
```
Expected: FAIL — generators undefined.

- [ ] **Step 3: Implement generators**

Clone `RST_H3_Tessellate.scala` verbatim into each new file, changing: class/object name, `name` literal, the `RasterTessellate.tessellate*Iter` call, and the error message. `RST_BNG_Tessellate` uses `BNG.getResolution` on the resolution arg if it may arrive as a string; otherwise `resolutionExpr.eval(input).asInstanceOf[Int]` as H3 does. The generator body (CollectionGenerator, `elementSchema`, cleanup listener, row wrapping) is identical.

```scala
// RST_Quadbin_Tessellate.scala — identical structure to RST_H3_Tessellate, with:
//   name = "gbx_rst_quadbin_tessellate"
//   iter = RasterTessellate.tessellateQuadbinIter(ds, mtd, resolution, mode)  // cellId Long; tag handled inside iter
// RST_BNG_Tessellate.scala — same, with:
//   name = "gbx_rst_bng_tessellate"
//   iter = RasterTessellate.tessellateBngIter(ds, mtd, resolution, mode)      // cellId String
```

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.grid.RST_Quadbin_TessellateTest' --log tess.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/generators/RST_Quadbin_Tessellate.scala \
        src/main/scala/com/databricks/labs/gbx/rasterx/expressions/generators/RST_BNG_Tessellate.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/grid/RST_Quadbin_TessellateTest.scala
git commit -m "feat(rasterx): quadbin + BNG tessellate generator expressions"
```

---

## Task 5: Quadbin rasterize_agg UDAF

**Files:**
- Create: `.../agg/RST_Quadbin_RasterizeAgg.scala`
- Reference: `.../agg/RST_H3_RasterizeAgg.scala` (full), `.../util/VectorRasterBridge.scala` (`buildEmptyRaster`), `Quadbin.scala` (`resolution`, centroid, bbox).

**Interfaces:**
- Consumes: `VectorRasterBridge.buildEmptyRaster`, `Quadbin.resolution(cell: Long): Int`, `Quadbin.centroid`/bbox for gridspec sample points.
- Produces: `RST_Quadbin_RasterizeAgg` (TypedImperativeAggregate reusing a `Long`-keyed acc); `name` = `gbx_rst_quadbin_rasterize_agg`; same 12-arg signature as H3.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_GridRasterizeAggTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions.agg

import org.scalatest.funsuite.AnyFunSuite

class RST_GridRasterizeAggTest extends AnyFunSuite {
    test("quadbin rasterize_agg canonical name + 12-arg builder") {
        assert(RST_Quadbin_RasterizeAgg.name == "gbx_rst_quadbin_rasterize_agg")
        import org.apache.spark.sql.catalyst.expressions.Literal
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_Quadbin_RasterizeAgg.builder()(args).isInstanceOf[RST_Quadbin_RasterizeAgg])
    }
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_GridRasterizeAggTest' --log rasterize.log
```
Expected: FAIL — undefined.

- [ ] **Step 3: Implement the UDAF**

Clone `RST_H3_RasterizeAgg.scala`. Keep the `Long`-keyed accumulator + serde (rename `H3RasterizeAcc` → `QuadbinRasterizeAcc` or reuse a shared acc class — prefer a shared `LongCellRasterizeAcc` if you extract one, but a straight clone is acceptable for this task). Substitute the four documented per-grid points (spec §3.3):
1. `resolutionOf`: replace `H3Core.h3GetResolution(c)` with `Quadbin.resolution(c)`.
2. gridspec sample points: replace `H3.cellIdToCenter`/`cellIdToBoundary` with quadbin centroid / bbox-corner coordinates (WGS84 — quadbin bbox is already lon/lat).
3. default `pixel_size`: derive from zoom (web-mercator tile edge in the target srid) instead of `H3.edgeLength`.
4. projection: same pixel-centre → WGS84 → `Quadbin.pointToCell` mapping (quadbin input is 4326, so identical structure to H3's WGS84 hop).
`buildEmptyRaster` usage, NoData `-9999.0`, last-wins fold order, serde — unchanged.

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_GridRasterizeAggTest' --log rasterize.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_Quadbin_RasterizeAgg.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_GridRasterizeAggTest.scala
git commit -m "feat(rasterx): quadbin rasterize_agg UDAF"
```

---

## Task 6: BNG rasterize_agg UDAF

**Files:**
- Create: `.../agg/RST_BNG_RasterizeAgg.scala`
- Reference: `RST_H3_RasterizeAgg.scala`, `RST_Quadbin_RasterizeAgg.scala` (Task 5), `BNG.scala` (`parse`, `format`, `getResolution`, `cellIdToCenter`, `cellIdToBoundary`).

**Interfaces:**
- Consumes: `BNG.parse(cellID: String): Long`, `BNG.getResolution`, `BNG.cellIdToCenter(cell: Long): Coordinate`, `BNG.cellIdToBoundary(cell: Long): Seq[Coordinate]`, `VectorRasterBridge.buildEmptyRaster`.
- Produces: `RST_BNG_RasterizeAgg`; `name` = `gbx_rst_bng_rasterize_agg`; 12-arg signature; **cellid input is STRING** (parsed to internal Long on `update`).

- [ ] **Step 1: Write the failing test**

Add to `RST_GridRasterizeAggTest.scala`:

```scala
test("bng rasterize_agg canonical name + string cellid parse") {
    assert(RST_BNG_RasterizeAgg.name == "gbx_rst_bng_rasterize_agg")
    import org.apache.spark.sql.catalyst.expressions.Literal
    val args = (0 until 12).map(i => Literal(i)).toSeq
    assert(RST_BNG_RasterizeAgg.builder()(args).isInstanceOf[RST_BNG_RasterizeAgg])
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_GridRasterizeAggTest' --log rasterize.log
```
Expected: FAIL — `RST_BNG_RasterizeAgg` undefined.

- [ ] **Step 3: Implement the UDAF**

Clone the Task-5 quadbin UDAF. Per-grid substitutions:
1. **cellid input type STRING:** in `update`, the raw cellid is a `UTF8String`; convert to internal Long via `BNG.parse(raw.toString)`. Keep the accumulator `Long`-keyed (so serde is unchanged). Reject non-String cellids with a clear error.
2. `resolutionOf`: `BNG.getResolution` semantics — read each cell's resolution from its Long id (use the same helper BNG uses internally; if only `getResolution(res: Any)` exists for the *argument* form, derive resolution from `cellDigits`/the id as `BNG.format`/`cellIdToGeometry` do — read `BNG.scala` to find the id→resolution path). Error on mixed resolutions.
3. gridspec sample points: `BNG.cellIdToCenter` / `cellIdToBoundary` — these return **EPSG:27700** coordinates, so there is **no WGS84 hop**. Set `srcSR`/`dstSR` to 27700 (identity) — the sample points and the output raster are both in 27700. This is the key BNG divergence.
4. projection in the burn loop: pixel centre is already 27700 → `BNG.pointToCellID(e, n, resolution)` directly (no reproject to WGS84).
5. default `pixel_size`: BNG resolution → metre edge (1=100000, 2=10000, 3=1000, 4=100, 5=10, 6=1; negative resolutions per `BNG` base-50 quadrant sizes — derive from the same divisor math `pointToCellID` uses).
`buildEmptyRaster` (srid 27700), NoData `-9999.0`, last-wins — unchanged.

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.agg.RST_GridRasterizeAggTest' --log rasterize.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_BNG_RasterizeAgg.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/agg/RST_GridRasterizeAggTest.scala
git commit -m "feat(rasterx): BNG rasterize_agg UDAF (27700-native, String cellid)"
```

---

## Task 7: Register all 9 + Scala functions wrappers + integration test

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala`
- Modify: `docs/tests-function-info/registered_functions.txt`
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_GridIntegrationTest.scala` (create — end-to-end SQL registration + a covering→rastertogrid round-trip)

**Interfaces:**
- Consumes: all 9 expression objects (Tasks 2, 4, 5, 6) + the 5 BNG reducers.
- Produces: SQL-registered functions; Scala `functions` column wrappers.

- [ ] **Step 1: Write the failing test**

Create `RST_GridIntegrationTest.scala` — register functions on a `SparkSession`, assert all 9 resolve, and run one end-to-end assertion (spec §2.6 empty-cell + §5 round-trip). Use the existing `RST_H3IntegrationTest` as the pattern for session setup and sample-data loading.

```scala
// Mirror RST_H3IntegrationTest: create/borrow the test SparkSession, call rasterx functions.register(spark),
// then:
test("all 9 grid functions are registered") {
    val fns = spark.sessionState.functionRegistry.listFunction().map(_.funcName).toSet
    Seq("gbx_rst_bng_rastertogridavg","gbx_rst_bng_rastertogridcount","gbx_rst_bng_rastertogridmax",
        "gbx_rst_bng_rastertogridmin","gbx_rst_bng_rastertogridmedian","gbx_rst_bng_tessellate",
        "gbx_rst_quadbin_tessellate","gbx_rst_quadbin_rasterize_agg","gbx_rst_bng_rasterize_agg")
      .foreach(n => assert(fns.contains(n), s"$n not registered"))
}

test("rasterize_agg output declares -9999 NoData (spec 2.6)") {
    // build a tiny cell set, run gbx_rst_quadbin_rasterize_agg, read the tile band GetNoDataValue == -9999.0
    ...
}
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_GridIntegrationTest' --log grid-int.log
```
Expected: FAIL — functions not registered.

- [ ] **Step 3: Register + add wrappers**

In `functions.scala`:
- Add to the imports the new agg classes: `RST_Quadbin_RasterizeAgg, RST_BNG_RasterizeAgg` in the `expressions.agg` import; generators `RST_Quadbin_Tessellate, RST_BNG_Tessellate`; grid reducers `RST_BNG_RasterToGrid{Avg,Count,Max,Min,Median}`.
- Add `rd.register(...)` next to the existing siblings:
```scala
rd.register(RST_Quadbin_RasterizeAgg)   // near RST_H3_RasterizeAgg (line ~84)
rd.register(RST_BNG_RasterizeAgg)
rd.register(RST_Quadbin_Tessellate)     // near RST_H3_Tessellate (line ~95)
rd.register(RST_BNG_Tessellate)
rd.register(RST_BNG_RasterToGridAvg)    // after RST_Quadbin_RasterToGridMedian (line ~111)
rd.register(RST_BNG_RasterToGridCount)
rd.register(RST_BNG_RasterToGridMax)
rd.register(RST_BNG_RasterToGridMin)
rd.register(RST_BNG_RasterToGridMedian)
```
- Add Scala `functions` column wrappers mirroring `rst_h3_tessellate` / `rst_quadbin_rastertogridavg` (2-arg + 3-arg where the H3 sibling has them). Example:
```scala
def rst_bng_rastertogridavg(tileExpr: Column, resolution: Column): Column =
    ColumnAdapter(RST_BNG_RasterToGridAvg.name, Seq(tileExpr, resolution))
def rst_quadbin_tessellate(tileExpr: Column, resolution: Column): Column =
    ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tileExpr, resolution))
def rst_quadbin_tessellate(tileExpr: Column, resolution: Column, mode: String): Column =
    ColumnAdapter(RST_Quadbin_Tessellate.name, Seq(tileExpr, resolution, lit(mode)))
// ... and bng_tessellate (2+3 arg), the 4 other bng reducers, quadbin_rasterize_agg, bng_rasterize_agg
```
For `rasterize_agg`, mirror however `rst_h3_rasterize_agg`'s wrapper is exposed (grep it in `functions.scala`; if H3 has no column wrapper because it's SQL-only UDAF, follow that same pattern — SQL registration alone).

- Add the 9 names to `docs/tests-function-info/registered_functions.txt` (alphabetical within their prefix groups):
```
gbx_rst_bng_rastertogridavg
gbx_rst_bng_rastertogridcount
gbx_rst_bng_rastertogridmax
gbx_rst_bng_rastertogridmedian
gbx_rst_bng_rastertogridmin
gbx_rst_bng_rasterize_agg
gbx_rst_bng_tessellate
gbx_rst_quadbin_rasterize_agg
gbx_rst_quadbin_tessellate
```

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_GridIntegrationTest' --log grid-int.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala \
        docs/tests-function-info/registered_functions.txt \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_GridIntegrationTest.scala
git commit -m "feat(rasterx): register 9 BNG/quadbin raster-grid functions + wrappers"
```

---

## Task 8: Python bindings

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/rasterx/functions.py`
- Reference: the existing `rst_h3_tessellate`, `rst_quadbin_rastertogridavg`, `rst_h3_rasterize_agg` bindings in the same file.

**Interfaces:**
- Consumes: the 9 registered SQL names (Task 7).
- Produces: 9 Python `call_function` wrappers matching the Scala API names (`rst_bng_rastertogridavg`, …, `rst_quadbin_tessellate`, `rst_bng_tessellate`, `rst_quadbin_rasterize_agg`, `rst_bng_rasterize_agg`).

- [ ] **Step 1: Write the failing test**

Add to `python/geobrix/test/rasterx/` (heavy binding test dir; mirror an existing binding-presence test) — or if bindings are only asserted via `gbx:test:bindings`, write a light import/signature test:

```python
def test_bng_quadbin_raster_grid_bindings_exist():
    from databricks.labs.gbx.rasterx import functions as rx
    for name in [
        "rst_bng_rastertogridavg", "rst_bng_rastertogridcount", "rst_bng_rastertogridmax",
        "rst_bng_rastertogridmin", "rst_bng_rastertogridmedian", "rst_bng_tessellate",
        "rst_quadbin_tessellate", "rst_quadbin_rasterize_agg", "rst_bng_rasterize_agg",
    ]:
        assert hasattr(rx, name), f"missing binding {name}"
```

- [ ] **Step 2: Run to verify it fails**

```
gbx:test:python --path python/geobrix/test/rasterx/  # narrow to the new test node
```
Expected: FAIL — attributes missing.

- [ ] **Step 3: Implement bindings**

Clone the existing wrappers. Reducers (2-arg, like `rst_quadbin_rastertogridavg`):
```python
def rst_bng_rastertogridavg(tile: ColLike, resolution: ColLike) -> Column:
    """Average raster value within each BNG grid cell.

    Args:
        tile: Raster tile column.
        resolution: BNG resolution (±1..±6 or a resolution string like "1km").
    Returns:
        Column of array of (bng_cell STRING, measure DOUBLE).
    """
    return f.call_function("gbx_rst_bng_rastertogridavg", _col(tile), _col(resolution))
```
Repeat for count/max/min/median. Tessellate (mirror `rst_h3_tessellate`, 3-arg with `mode="covering"` default). `rasterize_agg` (mirror `rst_h3_rasterize_agg`'s wrapper — same 12 args; for BNG the cellid column is a STRING).

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:python --path python/geobrix/test/rasterx/
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/rasterx/functions.py python/geobrix/test/rasterx/
git commit -m "feat(rasterx): Python bindings for 9 BNG/quadbin raster-grid functions"
```

---

## Task 9: function-info SQL examples + regenerate + binding parity

**Files:**
- Modify: `docs/tests/python/api/rasterx_functions_sql.py` (add `*_sql_example()` for each of the 9)
- Regenerate: `src/main/resources/com/databricks/labs/gbx/function-info.json` (via `gbx:docs:function-info`)

**Interfaces:**
- Consumes: registered names (Task 7), Python bindings (Task 8).
- Produces: non-empty `function-info.json` entries; passing `gbx:test:bindings`.

- [ ] **Step 1: Write the failing check**

Run the parity gate first to see the 9 gaps:
```
gbx:test:bindings --log bindings.log
```
Expected: FAIL — 9 functions missing `function-info.json` examples.

- [ ] **Step 2: Add SQL examples**

For each of the 9, add a `*_sql_example()` in `docs/tests/python/api/rasterx_functions_sql.py` that executes **real** SQL against real sample data (BNG examples use British coordinates / a UK raster; quadbin/H3 use lon/lat). No placeholders, no empty usage — the coverage test asserts non-empty. Mirror the existing rasterx `*_sql_example()` functions in that file.

- [ ] **Step 3: Regenerate function-info**

```
gbx:docs:function-info
```
This runs `generate-function-info.py` and writes `function-info.json`. Dispatch in Docker if it needs the container.

- [ ] **Step 4: Run parity to verify it passes**

```
gbx:test:bindings --log bindings.log
```
Expected: PASS — all 9 present as Scala name + Python binding + function-info key.

- [ ] **Step 5: Commit**

```bash
git add docs/tests/python/api/rasterx_functions_sql.py \
        src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "feat(rasterx): function-info examples for 9 BNG/quadbin raster-grid functions"
```

---

## Task 10: Cross-cutting tests (reproject correctness, round-trip, seam-safety)

**Files:**
- Modify: `RST_BNG_RasterToGridTest.scala`, `RST_GridIntegrationTest.scala`
- These require real GDAL + sample data → run in Docker (integration).

- [ ] **Step 1: BNG reproject-correctness test**

Add: a fixture raster in EPSG:3857 (or 4326) and the same raster **pre-warped** to EPSG:27700 must yield identical BNG cell assignments + measures (within 1e-9). Proves the internal auto-warp matches an explicit upstream warp.

```scala
test("bng rastertogrid: internal warp matches explicit upstream warp") {
    val ds3857 = /* small raster over GB in EPSG:3857 */ ???
    val ds27700 = /* same raster warped to 27700 with gdalwarp -r near */ ???
    val meanF = (v: scala.collection.mutable.ArrayBuffer[Double]) => v.sum / v.length
    val a = RST_BNG_RasterToGrid.execute(ds3857, 3, meanF).flatten.toMap  // triggers internal warp
    val b = RST_BNG_RasterToGrid.execute(ds27700, 3, meanF).flatten.toMap // already 27700, no warp
    assert(a.keySet == b.keySet)
    a.foreach { case (cell, v) => assert(math.abs(v - b(cell)) < 1e-9) }
}
```

- [ ] **Step 2: rasterize_agg round-trip + NoData**

Add (per spec §5): `rastertogrid` then `rasterize_agg` on the same cell set recovers per-cell values; the output tile's band `GetNoDataValue == -9999.0`; feeding it back into a reducer excludes the filled pixels.

- [ ] **Step 3: Run the full grid suites in Docker**

```
gbx:test:scala --suites 'com.databricks.labs.gbx.rasterx.expressions.grid.*,com.databricks.labs.gbx.rasterx.expressions.agg.RST_GridRasterizeAggTest,com.databricks.labs.gbx.rasterx.expressions.RST_GridIntegrationTest' --log grid-all.log
```
Expected: PASS (all).

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/
git commit -m "test(rasterx): BNG reproject-correctness + rasterize round-trip + NoData regression"
```

---

## Task 11: Docs + README badges

**Files:**
- Modify: `docs/docs/api/raster-functions.mdx`, `docs/docs/api/execution-tiers.mdx`, `docs/docs/api/performance.mdx`, `docs/docs/beta-release-notes.mdx`, `README.md`

- [ ] **Step 1: raster-functions.mdx** — add reference entries for the 9 functions, grouped with their H3/quadbin siblings. BNG entries note the auto-reproject-to-27700 behaviour and the BNG resolution contract; all note the `-9999.0` NoData sentinel caveat that H3 already documents. No internal vocabulary (no wave numbers).

- [ ] **Step 2: execution-tiers.mdx** — mark the 9 as heavy-tier now, with light quadbin (Phase 2) and light BNG (Phase 3, gated on pygx BNG) as "planned". Match the existing tier-badge style.

- [ ] **Step 3: performance.mdx** — add the 9 to the existing execution-shape families (rastertogrid reducers, tessellate generator, rasterize UDAF) — classify into existing families, do not invent a new shape (per `performance-doc-update-on-new-function`).

- [ ] **Step 4: beta-release-notes.mdx** — a feature entry: quadbin and BNG now have the full H3 raster surface (rastertogrid reducers, tessellate, rasterize_agg) on the heavy tier. Call out `gbx_rst_bng_tessellate` for raster tiling by BNG scale (the CV image-tiling use case from issue #49). User-facing voice, no internal vocabulary (QC judge `internals-leak` enforces). The PR body / merge commit should reference "Closes #49".

- [ ] **Step 5: README.md badges** — RasterX 108 → 117, Functions 156 → 165. Update the two `img.shields.io` lines (the comment already documents the derivation).

```bash
git add docs/docs/api/raster-functions.mdx docs/docs/api/execution-tiers.mdx \
        docs/docs/api/performance.mdx docs/docs/beta-release-notes.mdx README.md
git commit -m "docs(rasterx): document 9 BNG/quadbin raster-grid functions + badge bump"
```

- [ ] **Step 6: Run doc tests + grep for internals leak**

```
gbx:test:docs --log docs.log
grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ ; echo "(should print nothing)"
```

---

## Task 12: Benchmark registration (heavy-tier, existing 20-node cluster)

**Files:**
- Modify: `src/test/scala/com/databricks/labs/gbx/bench/BenchDispatch.scala`
- Modify: `docs/docs/api/benchmarking.mdx`
- Test: `src/test/scala/com/databricks/labs/gbx/bench/BenchDispatchTest.scala`
- Reference: `BenchDispatch.scala` (the `shape` map, input-classification sets, aggregate-branch sets), `HeavyBenchSuite.scala`, `scripts/commands/gbx-bench-cluster.sh`, `scripts/commands/gbx-bench-heavyweight.sh`.

**Context — the 20-node config is reused, not redefined.** All cluster benches read the same cluster from `notebooks/tests/databricks_cluster_config.env` (via `CLUSTER_ID`, sourced by `gbx:bench:cluster`). "Same 20-node config as other benchmarks" means run the new functions through that existing config unchanged — do **not** author a new cluster spec. The only code change is registering the 9 functions in `BenchDispatch` so the existing harness discovers and dispatches them; the harness then benchmarks them on the same cluster with the same row ladder as every other `rst_*` function.

**Interfaces:**
- Consumes: the 9 registered SQL/Scala names (Task 7). Bench uses the Scala-API form without the `gbx_` prefix (e.g. `rst_bng_rastertogridavg`, matching how `rst_h3_rastertogridavg` appears in `BenchDispatch`).
- Produces: `BenchDispatch` entries so `BenchDispatch.all` and `--set full` include the 9; the 2 rasterize_aggs routed to the grid-aggregate branch (the `rst_h3_rasterize_agg` path).

- [ ] **Step 1: Write the failing test**

Add to `BenchDispatchTest.scala` (mirror its existing assertions about `rst_h3_*` membership):

```scala
test("BenchDispatch registers the 9 BNG/quadbin raster-grid functions") {
    val expected = Seq(
        "rst_bng_rastertogridavg", "rst_bng_rastertogridcount", "rst_bng_rastertogridmax",
        "rst_bng_rastertogridmin", "rst_bng_rastertogridmedian",
        "rst_bng_tessellate", "rst_quadbin_tessellate",
        "rst_quadbin_rasterize_agg", "rst_bng_rasterize_agg")
    expected.foreach(fn => assert(BenchDispatch.all.contains(fn), s"$fn not in BenchDispatch.all"))
    // shape classification: reducers + tessellate are DGGS; the aggs route to the grid-aggregate branch
    assert(BenchDispatch.shapeOf("rst_bng_rastertogridavg") == "DGGS")
    assert(BenchDispatch.aggregateBranchOf("rst_bng_rasterize_agg") == "grid_aggregate") // or whatever h3's branch label is
}
```
(Adapt `shapeOf`/`aggregateBranchOf` to the actual accessor names in `BenchDispatch` — read the file; if shape is a private `Map`, assert via the public dispatch entry point the test file already uses.)

- [ ] **Step 2: Run to verify it fails**

Dispatch a Task subagent (Docker):
```
gbx:test:scala --suite 'com.databricks.labs.gbx.bench.BenchDispatchTest' --log bench-dispatch.log
```
Expected: FAIL — the 9 names absent from `BenchDispatch.all`.

- [ ] **Step 3: Register the 9 in BenchDispatch**

In `BenchDispatch.scala`, add to the `shape` map next to the existing H3/quadbin grid entries (lines ~169–191):
```scala
// BNG raster→grid reducers (DGGS shape, same as H3/quadbin reducers)
"rst_bng_rastertogridavg" -> DGGS, "rst_bng_rastertogridcount" -> DGGS,
"rst_bng_rastertogridmax" -> DGGS, "rst_bng_rastertogridmedian" -> DGGS,
"rst_bng_rastertogridmin" -> DGGS,
// tessellate generators (DGGS, same as rst_h3_tessellate)
"rst_quadbin_tessellate" -> DGGS, "rst_bng_tessellate" -> DGGS,
// grid rasterize aggregators (DGGS; routed through the grid-aggregate branch like rst_h3_rasterize_agg)
"rst_quadbin_rasterize_agg" -> DGGS, "rst_bng_rasterize_agg" -> DGGS
```
Add the 2 rasterize_aggs to the same aggregate-branch set that holds `rst_h3_rasterize_agg` (the `h3Aggregate` set at line ~226 — rename to a grid-neutral `gridAggregate` if it now holds 3 grids, updating the `aggregateShape`/branch dispatch at line ~234 accordingly; keep the branch label stable or update the test in Step 1 to match). Ensure the pure-core/spark-path input classification for the new reducers matches the H3/quadbin reducers (they take a single tile + resolution — no special input set needed; verify they are NOT accidentally caught by `byteInput`/`tileArrayInput`/geometry-input sets).

**Parity contract for the aggs:** `rst_h3_rasterize_agg` uses a fixed deterministic cell set with an explicit grid (the PARITY CONTRACT block at line ~237). Add the analogous fixed cell set for quadbin (a fixed zoom + tile range) and BNG (a fixed resolution + a handful of GB cells), so the bench's light-vs-heavy parity check has a deterministic input. Mirror the `h3RaggRes`/`h3RaggCenterLat` constants with quadbin/BNG equivalents.

- [ ] **Step 4: Run to verify it passes**

```
gbx:test:scala --suite 'com.databricks.labs.gbx.bench.BenchDispatchTest' --log bench-dispatch.log
```
Expected: PASS.

- [ ] **Step 5: Document the bench coverage**

In `docs/docs/api/benchmarking.mdx`, add the 9 functions to the results narrative/tables alongside their siblings (the `rst_h3_rastertogrid*` / `rst_quadbin_rastertogrid*` rows already there ~lines 380–400, and `rst_h3_tessellate` ~line 370). Note that BNG reducers/tessellate include an internal EPSG:27700 reproject in their timing (unlike the 4326-native H3/quadbin), so a like-for-like comparison should account for the warp. No actual numbers yet — those come from a cluster run; add the rows as "pending first cluster run" or leave the table and add a one-line note that the new grid functions are covered by the harness. Per `bench-changes-update-docs`, any bench change is reflected here in the same stroke. No internal vocabulary.

- [ ] **Step 6: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/bench/BenchDispatch.scala \
        src/test/scala/com/databricks/labs/gbx/bench/BenchDispatchTest.scala \
        docs/docs/api/benchmarking.mdx
git commit -m "bench(rasterx): register 9 BNG/quadbin raster-grid functions in BenchDispatch"
```

- [ ] **Step 7: (Optional, user-gated) Run the actual cluster benchmark**

This runs on the shared 20-node cluster — **do not launch without user go-ahead** (bench cluster is shared; guard against duplicate runs; the cluster memory notes apply: poll libs to INSTALLED, give a summary.md link at the end). When approved:
```
gbx:bench:cluster --functions rst_bng_rastertogridavg,rst_bng_rastertogridcount,rst_bng_rastertogridmax,rst_bng_rastertogridmin,rst_bng_rastertogridmedian,rst_bng_tessellate,rst_quadbin_tessellate,rst_quadbin_rasterize_agg,rst_bng_rasterize_agg --row-counts 1000 --run-id bng-quadbin-parity
```
(1000-scale for the spark path per the bench policy.) Then backfill the real numbers into `benchmarking.mdx` and give the user the run's `summary.md` link.

---

## Final gate: Pre-push checks (after Task 12 — do NOT push, user batches pushes)

- [ ] Run and report results. Hold for user go-ahead before any push.

```
gbx:lint:scalastyle
gbx:lint:python --check
gbx:test:bindings
```

---

## Self-Review notes (author)

- **Spec §1 (+9 fns):** Tasks 2 (5 BNG reducers), 4 (2 tessellate), 5+6 (2 rasterize_agg) = 9. ✅ covered.
- **Spec §2.2 (Long ids, format/parse):** Task 1 uses `BNG.format` at output; Task 6 uses `BNG.parse` on input. ✅
- **Spec §2.3 (CRS split):** Task 1/3/6 reproject BNG to 27700; quadbin untouched (4326 contract). ✅
- **Spec §2.6 (empty-cell/NoData):** Task 1 Step-1 empty test; Task 10 Step-2 NoData/round-trip; global constraint restated. ✅
- **Spec §3.4 (BNG resolution):** Task 2 `UTF8String` overloads via `BNG.getResolution`; global constraint. ✅
- **Spec §4 (heavy-first):** this plan is Phase 1 only; light phases are separate plans. ✅
- **Spec §5 (tests):** Tasks 1,2,3,4,5,6,7,10 unit+integration; Task 10 reproject-correctness + round-trip. ✅
- **Spec §6 (surfaces):** registered_functions (T7), functions.scala (T7), Python (T8), function-info (T9), docs+README (T11), bindings (T9/T11), benchmark harness + benchmarking.mdx (T12). ✅
- **Benchmarking (user-requested addition):** Task 12 registers all 9 in `BenchDispatch` (reducers+tessellate→DGGS; aggs→grid-aggregate branch) and reuses the existing 20-node cluster config (`notebooks/tests/databricks_cluster_config.env`) — no new cluster spec. Actual cluster run is user-gated (Task 12 Step 7). ✅
- **Spec §7 risks:** scan-extraction is optional (Task 1 introduces BNG fresh, no H3 refactor — lowest-risk branch of §3.1/§8); nearest-neighbour warp pinned (global constraint + Task 10 test); GDALManager guard (global constraint).
- **Spec §8 open question (extract shared scan vs BNG-only helper):** plan chooses **BNG-only** (Task 1 writes a standalone object; no refactor of H3/quadbin reference), the lower-risk option. If a reviewer wants the shared `RasterGridScan` extraction, that is a follow-up refactor task, not a blocker.
