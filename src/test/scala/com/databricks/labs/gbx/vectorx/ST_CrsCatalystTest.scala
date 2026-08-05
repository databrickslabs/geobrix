package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers._

/** Catalyst-level SQL tests for gbx_st_crs, gbx_st_setcrs, gbx_st_transformcrs.
  *
  * These tests run expressions through the full Spark SQL analysis path (parse → analyze
  * → optimize → execute) and catch issues that unit-level eval() tests miss, such as:
  * - propagateNull short-circuiting in RuntimeReplaceable (C1: 2-arg transformcrs → NULL)
  * - implicit casts not firing for integer CRS arguments (I2)
  * - expression registration gaps
  *
  * Requires GDAL native libs; run via gbx:test:scala or inside Docker.
  */
class ST_CrsCatalystTest extends PlanTest with SilentSparkSession with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        super.beforeAll()
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        functions.register(spark)
    }

    private val gf = new GeometryFactory()

    private def ewkbHex(srid: Int): String = {
        val g = gf.createPoint(new Coordinate(11.0, 42.0))
        g.setSRID(srid)
        JTS.toEWKB(g).map("%02x".format(_)).mkString
    }

    private def plainWkbHex: String = {
        val g = gf.createPoint(new Coordinate(11.0, 42.0))
        JTS.toWKB(g).map("%02x".format(_)).mkString
    }

    test("gbx_st_crs: EWKB EPSG:4326 via SQL") {
        val result = spark.sql(s"SELECT gbx_st_crs(unhex('${ewkbHex(4326)}'))").first().getString(0)
        result shouldBe "EPSG:4326"
    }

    test("gbx_st_setcrs: WKB + string CRS via SQL") {
        val result = spark.sql(
            s"SELECT gbx_st_setcrs(unhex('$plainWkbHex'), 'EPSG:32633')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "gbx_st_setcrs must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }

    test("gbx_st_setcrs: integer CRS coerces to string via SQL") {
        // Integer CRS should be implicitly cast to StringType via inputTypes override.
        val result = spark.sql(
            s"SELECT gbx_st_setcrs(unhex('$plainWkbHex'), 32633)"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "gbx_st_setcrs with integer CRS must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }

    test("gbx_st_transformcrs 2-arg: EWKB 4326 -> 32633 via SQL (Catalyst null propagation check)") {
        // This is the CRITICAL 1 regression test: the 2-arg form must not return NULL
        // through Catalyst's propagateNull even though there is no third argument.
        val hex = ewkbHex(4326)
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$hex'), 'EPSG:32633')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "2-arg gbx_st_transformcrs must not return NULL through Catalyst")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
    }

    test("gbx_st_transformcrs 3-arg: plain WKB + source CRS via SQL") {
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$plainWkbHex'), 'EPSG:32633', 'EPSG:4326')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "3-arg gbx_st_transformcrs must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }

    test("gbx_st_transformcrs: plain WKB no source -> returned unchanged via SQL") {
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$plainWkbHex'), 'EPSG:32633')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null)
        // No embedded SRID, no source arg → unchanged
        result shouldEqual JTS.toWKB(gf.createPoint(new Coordinate(11.0, 42.0)))
    }

    test("gbx_st_transformcrs: integer target CRS coerces to string via SQL") {
        val hex = ewkbHex(4326)
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$hex'), 32633)"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "gbx_st_transformcrs with integer CRS must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }
}
