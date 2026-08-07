package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.Quadbin
import com.databricks.labs.gbx.gridx.quadbin.agg.{Quadbin_CellUnionAgg, QuadbinUnionAcc}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.LongType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for Quadbin_CellUnionAgg TypedImperativeAggregate. */
class Quadbin_CellUnionAggTest extends AnyFunSuite {

    // ~4 valid quadbin cells: center + 3 k=1 neighbours at z=8 near (0,0)
    private val baseCell: Long = Quadbin.pointToCell(0.0, 0.0, 8)
    private val testCells: Array[Long] = {
        val ring = Quadbin.kRing(baseCell, 1)
        ring.take(4)
    }

    private def freshAgg(): Quadbin_CellUnionAgg = {
        val child = Literal.create(testCells(0), LongType)
        Quadbin_CellUnionAgg(child)
    }

    test("agg result equals non-agg Quadbin_CellUnion.execute for same cells") {
        val agg = freshAgg()
        var buf = agg.createAggregationBuffer()

        testCells.foreach { cell =>
            val row = InternalRow(cell)
            val child = Literal.create(cell, LongType)
            val agg2 = agg.copy(cellid = child)
            buf = agg2.update(buf, row)
        }

        val aggResult = agg.eval(buf).asInstanceOf[Array[Byte]]
        val directResult = Quadbin_CellUnion.execute(testCells)

        aggResult should not be null
        directResult should not be null

        // Compare via JTS geometry equality (byte-level equality may differ by union order)
        val aggGeom = JTS.fromWKB(aggResult)
        val directGeom = JTS.fromWKB(directResult)

        aggGeom.getSRID shouldBe 4326
        directGeom.getSRID shouldBe 4326
        Seq("Polygon", "MultiPolygon") should contain (aggGeom.getGeometryType)

        // Topological equality: same area within tolerance
        math.abs(aggGeom.getArea - directGeom.getArea) should be < 1e-9
    }

    test("merge combines two partial buffers and eval equals Quadbin_CellUnion.execute on all cells") {
        val agg = freshAgg()

        // Split testCells into two halves to simulate distributed merge
        val (halfA, halfB) = testCells.splitAt(testCells.length / 2)

        // Build bufA from the first half
        var bufA = agg.createAggregationBuffer()
        halfA.foreach { cell =>
            val agg2 = agg.copy(cellid = Literal.create(cell, LongType))
            bufA = agg2.update(bufA, InternalRow(cell))
        }

        // Build bufB from the second half
        var bufB = agg.createAggregationBuffer()
        halfB.foreach { cell =>
            val agg2 = agg.copy(cellid = Literal.create(cell, LongType))
            bufB = agg2.update(bufB, InternalRow(cell))
        }

        // Merge: simulate what Spark does when combining partial aggregates
        val merged = agg.merge(bufA, bufB)

        val mergedResult = agg.eval(merged).asInstanceOf[Array[Byte]]
        val directResult = Quadbin_CellUnion.execute(testCells)

        mergedResult should not be null
        directResult should not be null

        val mergedGeom = JTS.fromWKB(mergedResult)
        val directGeom = JTS.fromWKB(directResult)

        mergedGeom.getSRID shouldBe 4326
        Seq("Polygon", "MultiPolygon") should contain (mergedGeom.getGeometryType)

        // Topological equality: same area within tolerance
        math.abs(mergedGeom.getArea - directGeom.getArea) should be < 1e-9
    }

    test("buffer serialize/deserialize roundtrip preserves cell list") {
        val child = Literal.create(testCells(0), LongType)
        val agg = Quadbin_CellUnionAgg(child)
        var buf = agg.createAggregationBuffer()

        testCells.foreach { cell =>
            val row = InternalRow(cell)
            val agg2 = agg.copy(cellid = Literal.create(cell, LongType))
            buf = agg2.update(buf, row)
        }

        val bytes = agg.serialize(buf)
        val restored = agg.deserialize(bytes)

        restored.cells.toArray shouldBe buf.cells.toArray
    }

}
