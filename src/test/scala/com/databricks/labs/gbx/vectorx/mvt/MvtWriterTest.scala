package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

/** Direct unit tests for [[MvtWriter]] — happy path + bad-input resilience. */
class MvtWriterTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    test("encode should return empty Array[Byte] for an empty feature list") {
        val out = MvtWriter.encode("empty", 4096, Seq.empty)
        assert(out != null && out.isEmpty)
    }

    test("encode should skip null / empty / invalid WKB rows and still emit good ones") {
        val good = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        val features = Seq(
            (null.asInstanceOf[Array[Byte]], Map[String, Any]("name" -> "skip-null")),
            (Array.emptyByteArray, Map[String, Any]("name" -> "skip-empty")),
            (Array[Byte](0, 1, 2, 3), Map[String, Any]("name" -> "skip-invalid")),
            (good, Map[String, Any]("name" -> "ok"))
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
        assert(new String(out, "UTF-8").contains("layer1"))
    }

    test("encode keeps every feature of a tile past the driver's default size and feature caps") {
        // 40,000 points with an id each encode to about 900 KB, past the OGR MVT driver's default MAX_SIZE
        // (500,000 bytes): left at its default the driver coarsens the tile (extent 4096 -> 128) and drops
        // features, so the heavy tier answered a smaller tile than the light tier for the same group.
        val rnd = new scala.util.Random(31)
        val n = 40000
        val features = (0 until n).map { i =>
            val wkb = JTS.toWKB(gf.createPoint(new Coordinate(rnd.nextInt(4096).toDouble, rnd.nextInt(4096).toDouble)))
            (wkb, Map[String, Any]("id" -> i.toLong))
        }
        val out = MvtWriter.encode("big", 4096, features)
        assert(out.nonEmpty)
        val decoded = MvtDecoder.decode(out)
        assert(decoded.size == n, s"expected $n features after the round trip, got ${decoded.size}")
        // the layer keeps the requested extent (4096 = varint 0x80 0x20 after the extent tag 0x28)
        val extentTag = Array[Byte](0x28, 0x80.toByte, 0x20)
        assert(out.indices.exists(i => i + 3 <= out.length && out.slice(i, i + 3).sameElements(extentTag)))
    }

}
