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
