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

    /** EWKB hex for POINT(15, 48) with given SRID.
      * lon=15 is inside EPSG:32633 (UTM Zone 33N, W=12 E=18) and other global-extent CRSes. */
    private def ewkbHex(srid: Int): String = {
        val g = gf.createPoint(new Coordinate(15.0, 48.0))
        g.setSRID(srid)
        JTS.toEWKB(g).map("%02x".format(_)).mkString
    }

    /** Plain WKB hex for POINT(15, 48) (no SRID). */
    private def plainWkbHex: String = {
        val g = gf.createPoint(new Coordinate(15.0, 48.0))
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
        // ewkbHex(4326) = POINT(15,48) SRID=4326, inside EPSG:32633 domain (W=12 E=18).
        val hex = ewkbHex(4326)
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$hex'), 'EPSG:32633')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "2-arg gbx_st_transformcrs must not return NULL through Catalyst")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (500000.0 +- 500000.0 * 1e-4)
    }

    test("gbx_st_transformcrs 3-arg: plain WKB + source CRS via SQL") {
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$plainWkbHex'), 'EPSG:32633', 'EPSG:4326')"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "3-arg gbx_st_transformcrs must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }

    test("gbx_st_transformcrs: plain WKB no source -> NULL via SQL (data: no source CRS)") {
        // Contract change: no source CRS is a data condition → NULL, not unchanged.
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$plainWkbHex'), 'EPSG:32633')"
        ).first().getAs[Array[Byte]](0)
        assert(result == null, "No source CRS must return NULL (data condition)")
    }

    test("gbx_st_transformcrs: integer target CRS coerces to string via SQL") {
        // ewkbHex(4326) = POINT(15,48) SRID=4326, inside EPSG:32633 domain
        val hex = ewkbHex(4326)
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$hex'), 32633)"
        ).first().getAs[Array[Byte]](0)
        assert(result != null, "gbx_st_transformcrs with integer CRS must not return NULL")
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
    }

    // ------------------------------------------------------------------
    // Always-BINARY SQL contract: a STRING geometry argument still yields BINARY
    // ------------------------------------------------------------------

    test("gbx_st_setcrs: STRING (EWKT) geometry input declares and returns BINARY") {
        // POINT(15 48) SRID=4326 — ST_SetCrs doesn't reproject, just stamps the SRID
        val df = spark.sql("SELECT gbx_st_setcrs('SRID=4326;POINT (15 48)', 'EPSG:32633') AS g")
        df.schema.fields(0).dataType shouldBe org.apache.spark.sql.types.BinaryType
        val g = JTS.fromWKB(df.first().getAs[Array[Byte]](0))
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (15.0 +- 1e-12)
    }

    test("gbx_st_transformcrs: STRING (EWKT) geometry input declares and returns BINARY") {
        // POINT(15 48) SRID=4326: inside EPSG:32633 domain (W=12 E=18)
        val df = spark.sql(
            "SELECT gbx_st_transformcrs('SRID=4326;POINT (15 48)', 'EPSG:32633') AS g")
        df.schema.fields(0).dataType shouldBe org.apache.spark.sql.types.BinaryType
        val g = JTS.fromWKB(df.first().getAs[Array[Byte]](0))
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)
    }

    test("gbx_st_transformcrs: BINARY and STRING geometry inputs agree via SQL") {
        // Both BINARY and STRING inputs with POINT(15,48) SRID=4326 — in domain for EPSG:32633
        val hex = ewkbHex(4326)
        val row = spark.sql(
            s"""SELECT gbx_st_transformcrs(unhex('$hex'), 'EPSG:32633') AS from_bin,
               |       gbx_st_transformcrs('SRID=4326;POINT (15 48)', 'EPSG:32633') AS from_txt
             """.stripMargin).first()
        val a = JTS.fromWKB(row.getAs[Array[Byte]]("from_bin"))
        val b = JTS.fromWKB(row.getAs[Array[Byte]]("from_txt"))
        a.getSRID shouldBe b.getSRID
        a.getCoordinate.getX shouldBe (b.getCoordinate.getX +- 1e-9)
        a.getCoordinate.getY shouldBe (b.getCoordinate.getY +- 1e-9)
    }

    test("gbx_st_transformcrs: PROJ4 target clears the SRID via SQL (authority-less)") {
        // PROJ4 strings have no area_of_use → domain check is skipped; ewkbHex = POINT(15,48)
        val hex = ewkbHex(4326)
        val result = spark.sql(
            s"SELECT gbx_st_transformcrs(unhex('$hex'), " +
            "'+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs')"
        ).first().getAs[Array[Byte]](0)
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 0
        g.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)
    }
}
