package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.expressions.{ST_Crs, ST_SetCrs, ST_TransformCrs}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for ST_Crs, ST_SetCrs, ST_TransformCrs — heavy Scala tier.
  *
  * Requires GDAL native libs; run via gbx:test:scala or inside Docker.
  *
  * NOTE: return type of eval is Any (both binary and string outputs); assertions
  * use assert/isInstanceOf rather than ScalaTest AnyRef matchers to avoid
  * "Cannot prove that Any <:< AnyRef" compile errors.
  */
class ST_CrsFamilyTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    }

    private val gf = new GeometryFactory()

    /** EWKB of POINT(11, 42) with given SRID. */
    private def ewkb(srid: Int): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(11.0, 42.0))
        g.setSRID(srid)
        JTS.toEWKB(g)
    }

    /** EWKT of POINT(11, 42) with given SRID. */
    private def ewkt(srid: Int): UTF8String =
        UTF8String.fromString(s"SRID=$srid;POINT (11 42)")

    /** Plain WKB of POINT(11, 42) (no SRID). */
    private def plainWkb(): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(11.0, 42.0))
        JTS.toWKB(g)
    }

    /** Plain WKT of POINT(11, 42). */
    private def plainWkt(): UTF8String = UTF8String.fromString("POINT (11 42)")

    private val _CUSTOM_TM_WKT =
        """PROJCS["Custom_TM",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["central_meridian",13.7],PARAMETER["scale_factor",0.9996],UNIT["metre",1]]"""
    private val _PROJ4_UTM33 = "+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs"

    // ------------------------------------------------------------------
    // ST_Crs
    // ------------------------------------------------------------------

    test("ST_Crs: EWKB EPSG:4326 -> 'EPSG:4326'") {
        ST_Crs.eval(ewkb(4326)) shouldBe UTF8String.fromString("EPSG:4326")
    }

    test("ST_Crs: EWKB ESRI:54008 -> 'ESRI:54008'") {
        ST_Crs.eval(ewkb(54008)) shouldBe UTF8String.fromString("ESRI:54008")
    }

    test("ST_Crs: plain WKB -> null") {
        ST_Crs.eval(plainWkb()) shouldBe null
    }

    test("ST_Crs: unresolvable SRID -> null (never-error)") {
        ST_Crs.eval(ewkb(999999)) shouldBe null
    }

    test("ST_Crs: EWKT EPSG:4326 -> 'EPSG:4326'") {
        ST_Crs.eval(ewkt(4326)) shouldBe UTF8String.fromString("EPSG:4326")
    }

    test("ST_Crs: EWKT ESRI:54008 -> 'ESRI:54008'") {
        ST_Crs.eval(ewkt(54008)) shouldBe UTF8String.fromString("ESRI:54008")
    }

    test("ST_Crs: plain WKT -> null") {
        ST_Crs.eval(plainWkt()) shouldBe null
    }

    test("ST_Crs: null input -> null") {
        ST_Crs.eval(null) shouldBe null
    }

    // ------------------------------------------------------------------
    // ST_SetCrs
    // ------------------------------------------------------------------

    test("ST_SetCrs: WKB in -> EWKB out with SRID 32633") {
        val result = ST_SetCrs.eval(plainWkb(), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 32633
    }

    test("ST_SetCrs: EWKB in -> EWKB out with SRID replaced to 32633") {
        val result = ST_SetCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 32633
    }

    test("ST_SetCrs: WKT in -> EWKT out starting 'SRID=54008;'") {
        val result = ST_SetCrs.eval(plainWkt(), UTF8String.fromString("ESRI:54008"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        result.asInstanceOf[UTF8String].toString should startWith("SRID=54008;")
    }

    test("ST_SetCrs: EWKT in -> EWKT out with SRID replaced") {
        val result = ST_SetCrs.eval(ewkt(4326), UTF8String.fromString("ESRI:54008"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        result.asInstanceOf[UTF8String].toString should startWith("SRID=54008;")
    }

    test("ST_SetCrs: coordinate preservation (high precision)") {
        val g = gf.createPoint(new Coordinate(11.123456789012345, 42.987654321098765))
        val wkb = JTS.toWKB(g)
        val result = ST_SetCrs.eval(wkb, UTF8String.fromString("EPSG:4326"))
        val decoded = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        decoded.getCoordinate.getX shouldBe (11.123456789012345 +- 1e-12)
        decoded.getCoordinate.getY shouldBe (42.987654321098765 +- 1e-12)
    }

    test("ST_SetCrs: null geom -> null") {
        assert(ST_SetCrs.eval(null, UTF8String.fromString("EPSG:4326")) == null)
    }

    test("ST_SetCrs: authority-less PROJ4 -> throws") {
        an[Exception] should be thrownBy
            ST_SetCrs.eval(plainWkb(), UTF8String.fromString(_PROJ4_UTM33))
    }

    test("ST_SetCrs: authority-less WKT -> throws") {
        an[Exception] should be thrownBy
            ST_SetCrs.eval(plainWkb(), UTF8String.fromString(_CUSTOM_TM_WKT))
    }

    // ------------------------------------------------------------------
    // ST_TransformCrs
    // ------------------------------------------------------------------

    test("ST_TransformCrs: EWKB + EPSG target -> EWKB with SRID=32633, coords reprojected") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
        g.getCoordinate.getY shouldBe (4657521.0 +- 4657521.0 * 1e-4)
    }

    test("ST_TransformCrs: EWKB + ESRI target -> EWKB with SRID=54008") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("ESRI:54008"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 54008
        g.getCoordinate.getX shouldBe (911358.0 +- 911358.0 * 1e-4)
    }

    test("ST_TransformCrs: EWKB + authority-less WKT target -> plain WKB, SRID=0") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString(_CUSTOM_TM_WKT))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 0
        // Should have been reprojected (x should differ from 11)
        math.abs(g.getCoordinate.getX - 11.0) should be > 1.0
    }

    test("ST_TransformCrs: EWKB + PROJ4 target (UTM33) -> reprojected, SRID=0 (authority-less in GDAL)") {
        // GDAL's GetAuthorityName/Code on a PROJ4-imported SpatialReference returns null
        // (PROJ4 strings carry no EPSG/ESRI authority code). The heavy tier therefore
        // treats PROJ4 the same as WKT: reprojects and clears SRID.
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString(_PROJ4_UTM33))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 0
        // Coordinates should be in UTM range (x ~168701 for lon=11, lat=42, zone 33)
        g.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
    }

    test("ST_TransformCrs: plain WKB + no source -> returned unchanged") {
        val input = plainWkb()
        val result = ST_TransformCrs.eval(input, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        result.asInstanceOf[Array[Byte]] shouldEqual input
    }

    test("ST_TransformCrs: plain WKB + explicit source_crs -> reprojected") {
        val result = ST_TransformCrs.eval(
            plainWkb(),
            UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("EPSG:4326")
        )
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
    }

    test("ST_TransformCrs: EWKT + EPSG target -> EWKT starting 'SRID=32633;'") {
        val result = ST_TransformCrs.eval(ewkt(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        result.asInstanceOf[UTF8String].toString should startWith("SRID=32633;")
    }

    test("ST_TransformCrs: EWKT + authority-less target -> plain WKT (no SRID prefix)") {
        val result = ST_TransformCrs.eval(ewkt(4326), UTF8String.fromString(_CUSTOM_TM_WKT))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should not startWith "SRID="
        wkt should startWith("POINT")
    }

    test("ST_TransformCrs: plain WKT + no source -> returned unchanged") {
        val input = plainWkt()
        val result = ST_TransformCrs.eval(input, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        result.asInstanceOf[UTF8String].toString shouldEqual input.toString
    }

    test("ST_TransformCrs: plain WKT + explicit source_crs -> reprojected WKT") {
        val result = ST_TransformCrs.eval(
            plainWkt(),
            UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("EPSG:4326")
        )
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        result.asInstanceOf[UTF8String].toString should startWith("SRID=32633;")
    }

    test("ST_TransformCrs: unresolvable embedded SRID (999999) -> returned unchanged") {
        val input = ewkb(999999)
        val result = ST_TransformCrs.eval(input, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        result.asInstanceOf[Array[Byte]] shouldEqual input
    }

    test("ST_TransformCrs: unresolvable explicit source_crs -> returned unchanged") {
        val input = plainWkb()
        val result = ST_TransformCrs.eval(
            input,
            UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ")
        )
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        result.asInstanceOf[Array[Byte]] shouldEqual input
    }

    test("ST_TransformCrs: unresolvable target -> throws") {
        an[Exception] should be thrownBy
            ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ"))
    }

    test("ST_TransformCrs: null target_crs -> null return") {
        assert(ST_TransformCrs.eval(ewkb(4326), null.asInstanceOf[UTF8String]) == null)
    }

    test("ST_TransformCrs: coordinate round-trip precision (reproject and back, < 1e-8 error)") {
        val xIn = 11.0
        val yIn = 42.0
        val g = gf.createPoint(new Coordinate(xIn, yIn))
        g.setSRID(4326)
        val wkb4326 = JTS.toEWKB(g)

        // Forward: 4326 -> 32633
        val wkb32633 = ST_TransformCrs.eval(wkb4326, UTF8String.fromString("EPSG:32633"))
        assert(wkb32633 != null)

        // Back: 32633 -> 4326
        val wkbBack = ST_TransformCrs.eval(wkb32633, UTF8String.fromString("EPSG:4326"))
        assert(wkbBack != null)
        val gBack = JTS.fromWKB(wkbBack.asInstanceOf[Array[Byte]])
        gBack.getCoordinate.getX shouldBe (xIn +- 1e-8)
        gBack.getCoordinate.getY shouldBe (yIn +- 1e-8)
    }

    // ------------------------------------------------------------------
    // IMPORTANT 1 — non-numeric authority codes (OGC:CRS84)
    // ------------------------------------------------------------------

    test("ST_SetCrs: OGC:CRS84 target -> throws (non-numeric authority code)") {
        // OGC:CRS84 resolves but GetAuthorityCode returns "CRS84" (non-numeric) → IllegalArgumentException
        an[Exception] should be thrownBy
            ST_SetCrs.eval(plainWkb(), UTF8String.fromString("OGC:CRS84"))
    }

    test("ST_TransformCrs: OGC:CRS84 target -> authority-less path (clear SRID)") {
        // OGC:CRS84 resolves but has non-numeric code "CRS84" — falls through to authority-less path
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("OGC:CRS84"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 0
    }

    // ------------------------------------------------------------------
    // IMPORTANT 4 — Z preservation
    // ------------------------------------------------------------------

    test("ST_SetCrs: binary medium preserves Z") {
        val g3d = gf.createPoint(new Coordinate(1.5, 2.5, 99.25))
        val wkb3d = JTS.toWKB3(g3d)
        val result = ST_SetCrs.eval(wkb3d, UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val decoded = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        decoded.getCoordinate.z shouldBe (99.25 +- 1e-9)
        decoded.getSRID shouldBe 4326
    }

    test("ST_SetCrs: text medium preserves Z") {
        val result = ST_SetCrs.eval(
            UTF8String.fromString("POINT Z (1.5 2.5 99.25)"),
            UTF8String.fromString("EPSG:4326")
        )
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should startWith("SRID=4326;")
        val body = wkt.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        g.getCoordinate.z shouldBe (99.25 +- 1e-9)
    }

    test("ST_TransformCrs: binary medium preserves Z through reproject") {
        val g3d = gf.createPoint(new Coordinate(11.0, 42.0, 500.0))
        g3d.setSRID(4326)
        val wkb3d = JTS.toEWKBAdaptive(g3d)
        val result = ST_TransformCrs.eval(wkb3d, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val decoded = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        assert(!decoded.getCoordinate.z.isNaN, "Z ordinate must be preserved through reproject")
    }

    test("ST_TransformCrs: 2D binary stays 2D (no NaN-Z injection)") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        // 2D EWKB is 25 bytes; 3D EWKB would be 33 bytes
        result.asInstanceOf[Array[Byte]].length shouldBe 25
    }

    test("ST_SetCrs: 2D binary stays 2D (no NaN-Z injection)") {
        val result = ST_SetCrs.eval(plainWkb(), UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        // 2D EWKB is 25 bytes; 3D EWKB would be 33 bytes
        result.asInstanceOf[Array[Byte]].length shouldBe 25
    }

    test("ST_TransformCrs: text medium preserves Z through reproject") {
        // POINT Z (11 42 500) with SRID=4326 in EWKT
        val ewktZ = UTF8String.fromString("SRID=4326;POINT Z (11 42 500)")
        val result = ST_TransformCrs.eval(ewktZ, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should startWith("SRID=32633;")
        val body = wkt.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        assert(!g.getCoordinate.z.isNaN, "Z ordinate must be preserved through reproject")
    }

    test("ST_SetCrs: 2D text stays 2D (no Z in output)") {
        val result = ST_SetCrs.eval(plainWkt(), UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should not include " Z "
        wkt should startWith("SRID=4326;POINT")
    }

    test("ST_TransformCrs: 2D text stays 2D after reproject") {
        val result = ST_TransformCrs.eval(plainWkt(), UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should not include " Z "
        wkt should startWith("SRID=32633;POINT")
    }

    // ------------------------------------------------------------------
    // IMPORTANT 5 — text-medium coordinate assertions for ST_TransformCrs
    // ------------------------------------------------------------------

    test("ST_TransformCrs: EWKT + EPSG target -> coordinate values correct") {
        val result = ST_TransformCrs.eval(ewkt(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        val body = wkt.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        g.getCoordinate.getX shouldBe (168701.0 +- 168701.0 * 1e-4)
        g.getCoordinate.getY shouldBe (4657521.0 +- 4657521.0 * 1e-4)
    }

    test("ST_TransformCrs: coordinate round-trip precision via text medium") {
        val ewktIn = ewkt(4326)
        val toUtm = ST_TransformCrs.eval(ewktIn, UTF8String.fromString("EPSG:32633"))
        assert(toUtm.isInstanceOf[UTF8String])
        val back = ST_TransformCrs.eval(toUtm, UTF8String.fromString("EPSG:4326"))
        assert(back.isInstanceOf[UTF8String])
        val body = back.asInstanceOf[UTF8String].toString.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        g.getCoordinate.getX shouldBe (11.0 +- 1e-8)
        g.getCoordinate.getY shouldBe (42.0 +- 1e-8)
    }

    // ------------------------------------------------------------------
    // Round 3 — mixed-Z never-error invariant
    // ------------------------------------------------------------------

    test("ST_TransformCrs: mixed-Z GEOMETRYCOLLECTION does not throw (never-error)") {
        // GEOMETRYCOLLECTION(POINT(0 0), POINT Z(1 1 5)) with SRID=4326.
        // First coord has NaN Z; toWKBAdaptive would produce 3D WKB that OGR rejects.
        // toWKBForOGR downcasts to 2D; transform succeeds (never-error invariant).
        val pt2d = gf.createPoint(new Coordinate(0.0, 0.0))       // Z = NaN
        val pt3d = gf.createPoint(new Coordinate(1.0, 1.0, 5.0))  // Z = 5
        val gc = gf.createGeometryCollection(Array[org.locationtech.jts.geom.Geometry](pt2d, pt3d))
        gc.setSRID(4326)
        val ewkbIn = JTS.toEWKBAdaptive(gc)  // 3D EWKB (any-coord rule) — has NaN Z for first point
        // This must NOT throw
        val result = ST_TransformCrs.eval(ewkbIn, UTF8String.fromString("EPSG:32633"))
        assert(result != null, "Mixed-Z input must not throw — never-error invariant")
        assert(result.isInstanceOf[Array[Byte]])
    }

    test("ST_TransformCrs: mixed-Z LINESTRING does not throw (never-error)") {
        // LINESTRING where first vertex has NaN Z but second has Z=5
        val ls = gf.createLineString(Array(
            new Coordinate(0.0, 0.0),       // Z = NaN
            new Coordinate(1.0, 1.0, 5.0)   // Z = 5
        ))
        ls.setSRID(4326)
        val ewkbIn = JTS.toEWKBAdaptive(ls)  // 3D flag because any coord has Z
        val result = ST_TransformCrs.eval(ewkbIn, UTF8String.fromString("EPSG:32633"))
        assert(result != null, "Mixed-Z LINESTRING must not throw")
        assert(result.isInstanceOf[Array[Byte]])
    }

    test("ST_SetCrs: mixed-Z GEOMETRYCOLLECTION preserves allZ (round-2 behavior intact)") {
        // ST_SetCrs never goes through OGR — it just stamps the SRID.
        // The any-coord rule for toEWKBAdaptive must still produce 3D EWKB with NaN Z for first point.
        val pt2d = gf.createPoint(new Coordinate(0.0, 0.0))       // Z = NaN
        val pt3d = gf.createPoint(new Coordinate(1.0, 1.0, 5.0))  // Z = 5
        val gc = gf.createGeometryCollection(Array[org.locationtech.jts.geom.Geometry](pt2d, pt3d))
        gc.setSRID(0)
        val ewkbIn = JTS.toEWKBAdaptive(gc)  // 3D because any coord has Z
        val result = ST_SetCrs.eval(ewkbIn, UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        val bytes = result.asInstanceOf[Array[Byte]]
        // 3D EWKB for a GEOMETRYCOLLECTION with two points must be larger than 2D version
        assert(bytes.length > 50, s"Expected 3D EWKB (>50 bytes) but got ${bytes.length}")
        // All Z values present (NaN for first point, 5.0 for second)
        val decoded = JTS.fromWKB(bytes)
        val allZ = decoded.getCoordinates.map(_.z)
        assert(allZ(0).isNaN, "First coord Z should remain NaN")
        allZ(1) shouldBe (5.0 +- 1e-9)
    }

    // ------------------------------------------------------------------
    // Round 4 — hot-path rewrite (cached CrsInfo + TransformPlan) guards
    // ------------------------------------------------------------------

    test("ST_TransformCrs: exact axis-order values for POINT(11 42) 4326 -> 32633") {
        // Exact expected values, matching the light tier bit-for-bit. An axis-order bug
        // silently yields x=3556703.20, y=1361574.16 — which the loose ±1e-4 relative
        // tolerance elsewhere in this suite would also catch, but pinning the exact value
        // guards the cached-plan path against picking up an authority-compliant CT.
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getCoordinate.getX shouldBe (168701.01508871152 +- 1e-6)
        g.getCoordinate.getY shouldBe (4657521.062149809 +- 1e-6)
    }

    test("ST_TransformCrs: identity 4326 -> 4326 preserves Z and stamps target SRID") {
        val g3d = gf.createPoint(new Coordinate(11.0, 42.0, 500.0))
        g3d.setSRID(4326)
        val result = ST_TransformCrs.eval(JTS.toEWKBAdaptive(g3d), UTF8String.fromString("EPSG:4326"))
        assert(result != null)
        val decoded = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        decoded.getSRID shouldBe 4326
        decoded.getCoordinate.z shouldBe (500.0 +- 1e-9)
        decoded.getCoordinate.getX shouldBe (11.0 +- 1e-12)
        decoded.getCoordinate.getY shouldBe (42.0 +- 1e-12)
    }

    test("ST_TransformCrs: bad source degrades repeatably (cache miss must not poison)") {
        // The CrsInfo cache stores nothing on a failed resolve, so an unresolvable source
        // must degrade to 'return input unchanged' on EVERY row, not just the first.
        val input = ewkb(999999)
        for (_ <- 0 until 3) {
            val result = ST_TransformCrs.eval(input, UTF8String.fromString("EPSG:32633"))
            result.asInstanceOf[Array[Byte]] shouldEqual input
        }
        val plain = plainWkb()
        for (_ <- 0 until 3) {
            val result = ST_TransformCrs.eval(plain, UTF8String.fromString("EPSG:32633"),
                UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ"))
            result.asInstanceOf[Array[Byte]] shouldEqual plain
        }
    }

    test("ST_TransformCrs: bad target raises repeatably (cache miss must not poison)") {
        for (_ <- 0 until 3) {
            an[Exception] should be thrownBy ST_TransformCrs.eval(
                ewkb(4326), UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ"))
        }
    }

    test("ST_TransformCrs: repeated rows stay stable across cached-plan reuse") {
        // Steady-state correctness guard: the cached CrsInfo / TransformPlan must give the
        // same answer on row 1000 as on row 1 (no mutated or released state behind the cache).
        val first = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
            .asInstanceOf[Array[Byte]]
        for (_ <- 0 until 1000) {
            ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
                .asInstanceOf[Array[Byte]] shouldEqual first
        }
    }

    test("ST_TransformCrs: interleaved CRS pairs do not cross-contaminate cached plans") {
        // Alternate two different targets so both plans stay live in the LRU; each must
        // keep producing its own correct coordinates.
        for (_ <- 0 until 50) {
            val utm33 = JTS.fromWKB(ST_TransformCrs.eval(
                ewkb(4326), UTF8String.fromString("EPSG:32633")).asInstanceOf[Array[Byte]])
            utm33.getSRID shouldBe 32633
            utm33.getCoordinate.getX shouldBe (168701.01508871152 +- 1e-6)

            val merc = JTS.fromWKB(ST_TransformCrs.eval(
                ewkb(4326), UTF8String.fromString("EPSG:3857")).asInstanceOf[Array[Byte]])
            merc.getSRID shouldBe 3857
            merc.getCoordinate.getX shouldBe (1224514.3987260093 +- 1e-2)
        }
    }
}
