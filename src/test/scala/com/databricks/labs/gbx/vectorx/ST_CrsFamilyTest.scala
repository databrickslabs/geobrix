package com.databricks.labs.gbx.vectorx

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.expressions.ST_TransformCrs
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for ST_TransformCrs — heavy Scala tier.
  *
  * Requires GDAL native libs; run via gbx:test:scala or inside Docker.
  *
  * Two entry points per function, tested separately:
  * - `eval` is the medium-preserving Scala core (binary in → binary out, text in → text out).
  * - `evalSql` is the registered SQL surface, which always returns BINARY.
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

    /** EWKB of POINT(15, 48) with given SRID.
      * lon=15 lat=48 is inside EPSG:32633 (UTM Zone 33N, W=12 E=18 S=0 N=84) and other
      * global-extent CRSes (ESRI:54008, EPSG:3857, OGC:CRS84). */
    private def ewkb(srid: Int): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(15.0, 48.0))
        g.setSRID(srid)
        JTS.toEWKB(g)
    }

    /** EWKT of POINT(15, 48) with given SRID. */
    private def ewkt(srid: Int): UTF8String =
        UTF8String.fromString(s"SRID=$srid;POINT (15 48)")

    /** Plain WKB of POINT(15, 48) (no SRID). */
    private def plainWkb(): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(15.0, 48.0))
        JTS.toWKB(g)
    }

    /** Plain WKT of POINT(15, 48). */
    private def plainWkt(): UTF8String = UTF8String.fromString("POINT (15 48)")

    private val _CUSTOM_TM_WKT =
        """PROJCS["Custom_TM",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["central_meridian",13.7],PARAMETER["scale_factor",0.9996],UNIT["metre",1]]"""
    private val _PROJ4_UTM33 = "+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs"

    // ------------------------------------------------------------------
    // ST_TransformCrs
    // ------------------------------------------------------------------

    test("ST_TransformCrs: EWKB + EPSG target -> EWKB with SRID=32633, coords reprojected") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (500000.0 +- 500000.0 * 1e-4)
        g.getCoordinate.getY shouldBe (5316300.0 +- 5316300.0 * 1e-4)
    }

    test("ST_TransformCrs: EWKB + ESRI target -> EWKB with SRID=54008") {
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("ESRI:54008"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 54008
        g.getCoordinate.getX shouldBe (1119380.0 +- 1119380.0 * 1e-4)
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
        // PROJ4 strings have no area_of_use, so the domain check is skipped.
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString(_PROJ4_UTM33))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getSRID shouldBe 0
        // Coordinates should be in UTM range (x ~500000 for lon=15, lat=48, zone 33)
        g.getCoordinate.getX shouldBe (500000.0 +- 500000.0 * 1e-4)
    }

    test("ST_TransformCrs: plain WKB + no source -> NULL (no source CRS = data condition)") {
        val result = ST_TransformCrs.eval(plainWkb(), UTF8String.fromString("EPSG:32633"))
        assert(result == null)
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
        g.getCoordinate.getX shouldBe (500000.0 +- 500000.0 * 1e-4)
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

    test("ST_TransformCrs: plain WKT + no source -> NULL (no source CRS = data condition)") {
        val result = ST_TransformCrs.eval(plainWkt(), UTF8String.fromString("EPSG:32633"))
        assert(result == null)
    }

    test("ST_TransformCrs: plain WKT + explicit source_crs -> reprojected WKT") {
        val result = ST_TransformCrs.eval(
            plainWkt(),
            UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("EPSG:4326")
        )
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        // POINT(15 48) 4326->32633 is in domain; should produce EWKT
        result.asInstanceOf[UTF8String].toString should startWith("SRID=32633;")
    }

    test("ST_TransformCrs: unresolvable embedded SRID (999999) -> NULL (data condition)") {
        val result = ST_TransformCrs.eval(ewkb(999999), UTF8String.fromString("EPSG:32633"))
        assert(result == null)
    }

    test("ST_TransformCrs: unresolvable explicit source_crs -> raises (parameter condition)") {
        an[Exception] should be thrownBy
            ST_TransformCrs.eval(
                plainWkb(),
                UTF8String.fromString("EPSG:32633"),
                UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ")
            )
    }

    test("ST_TransformCrs: unresolvable target -> throws") {
        an[Exception] should be thrownBy
            ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ"))
    }

    test("ST_TransformCrs: null target_crs -> null return") {
        assert(ST_TransformCrs.eval(ewkb(4326), null.asInstanceOf[UTF8String]) == null)
    }

    test("ST_TransformCrs: coordinate round-trip precision (reproject and back, < 1e-8 error)") {
        // POINT(15, 48) is inside EPSG:32633 (UTM Zone 33N, W=12 E=18)
        val xIn = 15.0
        val yIn = 48.0
        val g = gf.createPoint(new Coordinate(xIn, yIn))
        g.setSRID(4326)
        val wkb4326 = JTS.toEWKB(g)

        // Forward: 4326 -> 32633 (in domain: lon=15 inside W=12 E=18)
        val wkb32633 = ST_TransformCrs.eval(wkb4326, UTF8String.fromString("EPSG:32633"))
        assert(wkb32633 != null)

        // Back: 32633 -> 4326 (4326 has global domain; UTM coords are always in EPSG:4326 domain)
        val wkbBack = ST_TransformCrs.eval(wkb32633, UTF8String.fromString("EPSG:4326"))
        assert(wkbBack != null)
        val gBack = JTS.fromWKB(wkbBack.asInstanceOf[Array[Byte]])
        gBack.getCoordinate.getX shouldBe (xIn +- 1e-8)
        gBack.getCoordinate.getY shouldBe (yIn +- 1e-8)
    }

    // ------------------------------------------------------------------
    // IMPORTANT 1 — non-numeric authority codes (OGC:CRS84)
    // ------------------------------------------------------------------

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

    test("ST_TransformCrs: binary medium preserves Z through reproject") {
        // POINT(15, 48, 500) SRID=4326: lon=15 is inside EPSG:32633 domain (W=12 E=18)
        val g3d = gf.createPoint(new Coordinate(15.0, 48.0, 500.0))
        g3d.setSRID(4326)
        val wkb3d = JTS.toEWKBAdaptive(g3d)
        val result = ST_TransformCrs.eval(wkb3d, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        val decoded = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        assert(!decoded.getCoordinate.z.isNaN, "Z ordinate must be preserved through reproject")
    }

    test("ST_TransformCrs: 2D binary stays 2D (no NaN-Z injection)") {
        // ewkb(4326) is POINT(15,48) which is inside EPSG:32633 domain
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[Array[Byte]])
        // 2D EWKB is 25 bytes; 3D EWKB would be 33 bytes
        result.asInstanceOf[Array[Byte]].length shouldBe 25
    }

    test("ST_TransformCrs: text medium preserves Z through reproject") {
        // POINT Z (15 48 500) SRID=4326: lon=15 is inside EPSG:32633 domain (W=12 E=18)
        val ewktZ = UTF8String.fromString("SRID=4326;POINT Z (15 48 500)")
        val result = ST_TransformCrs.eval(ewktZ, UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        wkt should startWith("SRID=32633;")
        val body = wkt.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        assert(!g.getCoordinate.z.isNaN, "Z ordinate must be preserved through reproject")
    }

    test("ST_TransformCrs: 2D text stays 2D after reproject") {
        // plainWkt() = POINT(15 48), source=EPSG:4326, target=EPSG:32633 — in domain
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
        // ewkt(4326) = SRID=4326;POINT(15 48), inside EPSG:32633 domain
        val result = ST_TransformCrs.eval(ewkt(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        assert(result.isInstanceOf[UTF8String])
        val wkt = result.asInstanceOf[UTF8String].toString
        val body = wkt.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        g.getCoordinate.getX shouldBe (500000.0 +- 500000.0 * 1e-4)
        g.getCoordinate.getY shouldBe (5316300.0 +- 5316300.0 * 1e-4)
    }

    test("ST_TransformCrs: coordinate round-trip precision via text medium") {
        // ewkt(4326) = SRID=4326;POINT(15 48), inside EPSG:32633 domain
        val ewktIn = ewkt(4326)
        val toUtm = ST_TransformCrs.eval(ewktIn, UTF8String.fromString("EPSG:32633"))
        assert(toUtm.isInstanceOf[UTF8String])
        val back = ST_TransformCrs.eval(toUtm, UTF8String.fromString("EPSG:4326"))
        assert(back.isInstanceOf[UTF8String])
        val body = back.asInstanceOf[UTF8String].toString.split(";", 2)(1)
        val g = JTS.fromWKT(body)
        g.getCoordinate.getX shouldBe (15.0 +- 1e-8)
        g.getCoordinate.getY shouldBe (48.0 +- 1e-8)
    }

    // ------------------------------------------------------------------
    // Round 3 — mixed-Z never-error invariant
    // ------------------------------------------------------------------

    test("ST_TransformCrs: mixed-Z GEOMETRYCOLLECTION does not throw") {
        // GEOMETRYCOLLECTION(POINT(13 47), POINT Z(15 48 5)) with SRID=4326.
        // Both points are inside EPSG:32633 domain (W=12 E=18).
        // First coord has NaN Z; toWKBForOGR downcasts to 2D so OGR accepts.
        val pt2d = gf.createPoint(new Coordinate(13.0, 47.0))       // Z = NaN
        val pt3d = gf.createPoint(new Coordinate(15.0, 48.0, 5.0))  // Z = 5
        val gc = gf.createGeometryCollection(Array[org.locationtech.jts.geom.Geometry](pt2d, pt3d))
        gc.setSRID(4326)
        val ewkbIn = JTS.toEWKBAdaptive(gc)  // 3D EWKB (any-coord rule) — has NaN Z for first point
        val result = ST_TransformCrs.eval(ewkbIn, UTF8String.fromString("EPSG:32633"))
        assert(result != null, "Mixed-Z in-domain input must succeed")
        assert(result.isInstanceOf[Array[Byte]])
    }

    test("ST_TransformCrs: a partial-Z vertex takes X, Y and Z from the 3D transform (geocentric target)") {
        // A height-moving pair changes X and Y with Z; the vertex without a Z keeps a NaN Z.
        val ls = gf.createLineString(Array(new Coordinate(10.0, 50.0, 100.0), new Coordinate(11.0, 51.0)))
        ls.setSRID(4326)
        val result = ST_TransformCrs.eval(JTS.toEWKBAdaptive(ls), UTF8String.fromString("EPSG:4978"))
        assert(result != null)
        val cs = JTS.fromWKB(result.asInstanceOf[Array[Byte]]).getCoordinates
        cs(0).x shouldBe (4045519.707563044 +- 1e-3)
        cs(0).y shouldBe (713334.2754311836 +- 1e-3)
        cs(0).z shouldBe (4862865.642150745 +- 1e-3)
        cs(1).z.isNaN shouldBe true
    }

    test("ST_TransformCrs: mixed-Z LINESTRING does not throw") {
        // LINESTRING where first vertex has NaN Z but second has Z=5; both in EPSG:32633 domain
        val ls = gf.createLineString(Array(
            new Coordinate(13.0, 47.0),       // Z = NaN; lon=13 inside W=12 E=18
            new Coordinate(15.0, 48.0, 5.0)   // Z = 5
        ))
        ls.setSRID(4326)
        val ewkbIn = JTS.toEWKBAdaptive(ls)  // 3D flag because any coord has Z
        val result = ST_TransformCrs.eval(ewkbIn, UTF8String.fromString("EPSG:32633"))
        assert(result != null, "Mixed-Z in-domain LINESTRING must succeed")
        assert(result.isInstanceOf[Array[Byte]])
    }

    // ------------------------------------------------------------------
    // Round 4 — hot-path rewrite (cached CrsInfo + TransformPlan) guards
    // ------------------------------------------------------------------

    test("ST_TransformCrs: exact axis-order values for POINT(15 48) 4326 -> 32633") {
        // Exact expected values, matching the light tier bit-for-bit. An axis-order bug
        // silently yields x=3556703.20, y=1361574.16 — which the loose ±1e-4 relative
        // tolerance elsewhere in this suite would also catch, but pinning the exact value
        // guards the cached-plan path against picking up an authority-compliant CT.
        // POINT(15,48) is inside EPSG:32633 domain (UTM Zone 33N, W=12 E=18).
        val result = ST_TransformCrs.eval(ewkb(4326), UTF8String.fromString("EPSG:32633"))
        val g = JTS.fromWKB(result.asInstanceOf[Array[Byte]])
        g.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)
        g.getCoordinate.getY shouldBe (5316300.22445149533 +- 1e-6)
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

    test("ST_TransformCrs: bad embedded SRID returns NULL repeatably (cache miss must not poison)") {
        // The CrsInfo cache stores nothing on a failed resolve, so an unresolvable embedded
        // SRID must degrade to NULL on EVERY row, not just the first.
        val input = ewkb(999999)
        for (_ <- 0 until 3) {
            val result = ST_TransformCrs.eval(input, UTF8String.fromString("EPSG:32633"))
            assert(result == null, "Unresolvable embedded SRID must always return NULL")
        }
    }

    test("ST_TransformCrs: bad explicit source_crs raises repeatably (parameter, not cached)") {
        // An unresolvable explicit source_crs is a parameter error — raises on every call.
        for (_ <- 0 until 3) {
            an[Exception] should be thrownBy ST_TransformCrs.eval(
                plainWkb(),
                UTF8String.fromString("EPSG:32633"),
                UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ")
            )
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
        // ewkb(4326) = POINT(15,48) which is inside EPSG:32633 domain.
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
        // ewkb(4326) = POINT(15,48): inside EPSG:32633 (W=12 E=18) and EPSG:3857 (global).
        for (_ <- 0 until 50) {
            val utm33 = JTS.fromWKB(ST_TransformCrs.eval(
                ewkb(4326), UTF8String.fromString("EPSG:32633")).asInstanceOf[Array[Byte]])
            utm33.getSRID shouldBe 32633
            utm33.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)

            val merc = JTS.fromWKB(ST_TransformCrs.eval(
                ewkb(4326), UTF8String.fromString("EPSG:3857")).asInstanceOf[Array[Byte]])
            merc.getSRID shouldBe 3857
            merc.getCoordinate.getX shouldBe (1669792.36189910350 +- 1e-2)
        }
    }

    // ------------------------------------------------------------------
    // SQL surface (evalSql): always BINARY, whatever the input encoding
    // ------------------------------------------------------------------

    test("ST_TransformCrs.evalSql: TEXT input returns BINARY EWKB with the target SRID") {
        // ewkt(4326) = SRID=4326;POINT(15 48) — inside EPSG:32633 domain
        val result = ST_TransformCrs.evalSql(ewkt(4326), UTF8String.fromString("EPSG:32633"))
        assert(result != null)
        val g = JTS.fromWKB(result)
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)
    }

    test("ST_TransformCrs.evalSql: authority-less target returns plain WKB (SRID cleared)") {
        val g = JTS.fromWKB(
            ST_TransformCrs.evalSql(ewkt(4326), UTF8String.fromString(_CUSTOM_TM_WKT)))
        g.getSRID shouldBe 0
    }

    test("ST_TransformCrs.evalSql: 3-arg form with explicit source_crs returns BINARY") {
        // plainWkt() = POINT(15 48), source=EPSG:4326, target=EPSG:32633 — in domain
        val g = JTS.fromWKB(ST_TransformCrs.evalSql(
            plainWkt(), UTF8String.fromString("EPSG:32633"), UTF8String.fromString("EPSG:4326")))
        g.getSRID shouldBe 32633
        g.getCoordinate.getX shouldBe (500000.00000000116 +- 1e-6)
    }

    test("ST_TransformCrs.evalSql: unresolvable embedded SRID returns NULL (data condition)") {
        // Contract change: bad DATA (unresolvable embedded SRID) → NULL, not input-bytes.
        assert(ST_TransformCrs.evalSql(ewkb(999999), UTF8String.fromString("EPSG:32633")) == null)
    }

    test("ST_TransformCrs.evalSql: unresolvable explicit source_crs raises (parameter)") {
        // Plain WKT has no embedded SRID, so the explicit source_crs is the only source.
        // An unresolvable explicit source_crs is a parameter error — must raise.
        an[Exception] should be thrownBy ST_TransformCrs.evalSql(
            plainWkt(),
            UTF8String.fromString("EPSG:32633"),
            UTF8String.fromString("BOGUS_CRS_THAT_DOESNT_EXIST_XYZ")
        )
    }

    test("ST_TransformCrs.evalSql: plain WKT + no source returns NULL (data: no source CRS)") {
        assert(ST_TransformCrs.evalSql(plainWkt(), UTF8String.fromString("EPSG:32633")) == null)
    }

    test("ST_TransformCrs.evalSql: text-medium Z survives (no NaN token round-trip)") {
        // The always-BINARY surface skips WKT entirely, so a 3D EWKT input keeps its Z.
        // Through the old text path the WKT writer's literal `NaN` token was re-read as 2D.
        // POINT Z(15 48 500) SRID=4326: lon=15 is inside EPSG:32633 domain (W=12 E=18).
        val result = ST_TransformCrs.evalSql(
            UTF8String.fromString("SRID=4326;POINT Z (15 48 500)"),
            UTF8String.fromString("EPSG:32633"))
        assert(result != null, "In-domain 3D point must produce a result")
        val g = JTS.fromWKB(result)
        assert(!g.getCoordinate.z.isNaN, "Z must survive the text→binary SQL surface")
        g.getSRID shouldBe 32633
    }

    // ------------------------------------------------------------------
    // Task 3 — Heavy contract: data→NULL, parameter→raise, domain check
    // ------------------------------------------------------------------

    /** EWKB POINT(lon, lat) with given SRID. */
    private def ewkbPoint(lon: Double, lat: Double, srid: Int): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(lon, lat))
        g.setSRID(srid)
        JTS.toEWKB(g)
    }

    /** Plain WKB POINT(lon, lat) with no SRID. */
    private def wkbPoint(lon: Double, lat: Double): Array[Byte] = {
        val g = gf.createPoint(new Coordinate(lon, lat))
        JTS.toWKB(g)
    }

    test("transformcrs: unparseable data returns NULL (not unchanged)") {
        val out = ST_TransformCrs.evalSql(Array[Byte](1, 2, 3, 4), UTF8String.fromString("EPSG:3857"))
        assert(out == null, "unparseable geom must degrade to NULL")
    }

    test("transformcrs: out-of-domain point returns NULL") {
        // POINT(150 -80) SRID=4326 -> EPSG:27700, finite but far outside GB
        val g = ewkbPoint(150.0, -80.0, 4326)
        val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:27700"))
        assert(out == null, "out-of-domain reprojection must be NULL")
    }

    test("transformcrs: in-domain point succeeds") {
        val g = ewkbPoint(-0.13, 51.5, 4326)  // London
        val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:27700"))
        assert(out != null, "in-domain reprojection must produce a geometry")
    }

    test("transformcrs: unresolvable embedded SRID returns NULL (data)") {
        val g = ewkbPoint(1.0, 1.0, 99999)
        val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"))
        assert(out == null)
    }

    test("transformcrs: bad explicit source_crs raises (parameter)") {
        val g = wkbPoint(1.0, 1.0)  // plain, no SRID
        assertThrows[IllegalArgumentException] {
            ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"), UTF8String.fromString("EPSG:99999"))
        }
    }

    test("transformcrs: no source CRS returns NULL (data)") {
        val g = wkbPoint(1.0, 1.0)
        val out = ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:3857"))
        assert(out == null)
    }

    test("transformcrs: bad target raises") {
        val g = ewkbPoint(-0.13, 51.5, 4326)
        assertThrows[IllegalArgumentException] {
            ST_TransformCrs.evalSql(g, UTF8String.fromString("EPSG:99999"))
        }
    }

    test("ST_TransformCrs.evalSql: null geom and null crs -> null") {
        assert(ST_TransformCrs.evalSql(null, UTF8String.fromString("EPSG:4326")) == null)
        assert(ST_TransformCrs.evalSql(ewkb(4326), null.asInstanceOf[UTF8String]) == null)
    }

    // ------------------------------------------------------------------
    // Mixed-dimension text that is no collection: keep the Z that exists, NaN for the rest
    // ------------------------------------------------------------------

    private val mixedLine = UTF8String.fromString("SRID=4326;LINESTRING Z (15 51 5, 16 52)")
    private val mixedMulti = UTF8String.fromString("SRID=4326;MULTIPOINT Z ((15 51 5), (16 52))")

    private def zsOf(bytes: Array[Byte]): Array[Double] = JTS.fromWKB(bytes).getCoordinates.map(_.z)

    test("ST_TransformCrs.evalSql: mixed-dimension text moves X/Y, keeps the Z it has, NaN for the rest") {
        for (in <- Seq(mixedLine, mixedMulti)) {
            val out = ST_TransformCrs.evalSql(in, UTF8String.fromString("EPSG:32633"))
            assert(out != null, "mixed-dimension text must not read as no geometry")
            val g = JTS.fromWKB(out)
            g.getSRID shouldBe 32633
            val cs = g.getCoordinates
            cs(0).x shouldBe (500000.0 +- 1e-3)
            cs(0).y shouldBe (5649824.888 +- 1e-2)
            cs(0).z shouldBe (5.0 +- 1e-9)
            assert(!cs(1).x.isNaN && !cs(1).y.isNaN, "X/Y of the vertex without a Z must survive")
            assert(cs(1).z.isNaN)
        }
    }

    test("ST_TransformCrs.evalSql: a partial-Z binary geometry keeps the Z it has too") {
        val ls = gf.createLineString(Array(new Coordinate(15.0, 51.0), new Coordinate(16.0, 52.0, 6.0)))
        ls.setSRID(4326)
        val out = ST_TransformCrs.evalSql(JTS.toEWKBAdaptive(ls), UTF8String.fromString("EPSG:32633"))
        val zs = zsOf(out)
        assert(zs(0).isNaN)
        zs(1) shouldBe (6.0 +- 1e-9)
    }

    test("ST_TransformCrs.evalSql: a collection with a real Z and a missing one keeps the real Z") {
        val out = ST_TransformCrs.evalSql(
            UTF8String.fromString("SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT (16 52))"),
            UTF8String.fromString("EPSG:32633"))
        val zs = zsOf(out)
        zs(0) shouldBe 5.0
        assert(zs(1).isNaN)
    }

    // ------------------------------------------------------------------
    // EMPTY components: left EMPTY, never handed to the projection
    // ------------------------------------------------------------------

    private val emptyMember = UTF8String.fromString(
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT EMPTY)")
    private val emptyNested = UTF8String.fromString(
        "SRID=4326;GEOMETRYCOLLECTION (POINT (15 51), GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (15 51, 16 52)))")

    test("ST_TransformCrs.evalSql: a collection with an EMPTY point reprojects the rest and keeps the EMPTY one") {
        val out = ST_TransformCrs.evalSql(emptyMember, UTF8String.fromString("EPSG:32633"))
        assert(out != null, "an EMPTY member must not make the whole collection NULL")
        val g = JTS.fromWKB(out)
        g.getSRID shouldBe 32633
        g.getNumGeometries shouldBe 2
        val p0 = g.getGeometryN(0)
        p0.getCoordinate.x shouldBe (500000.0 +- 1e-3)
        p0.getCoordinate.z shouldBe (5.0 +- 1e-9)
        val p1 = g.getGeometryN(1)
        p1.getGeometryType shouldBe "Point"
        assert(p1.isEmpty)
    }

    test("ST_TransformCrs.evalSql: an EMPTY point nested in a collection stays EMPTY") {
        val out = ST_TransformCrs.evalSql(emptyNested, UTF8String.fromString("EPSG:32633"))
        assert(out != null)
        val g = JTS.fromWKB(out)
        val inner = g.getGeometryN(1)
        inner.getNumGeometries shouldBe 2
        inner.getGeometryN(0).getGeometryType shouldBe "Point"
        assert(inner.getGeometryN(0).isEmpty)
        inner.getGeometryN(1).getCoordinates.head.x shouldBe (500000.0 +- 1e-3)
    }

    test("ST_TransformCrs.evalSql: a MultiLineString and a MultiPolygon keep their type and an EMPTY part") {
        val cases = Seq(
            "MULTILINESTRING" -> "SRID=4326;MULTILINESTRING (EMPTY, (15 51, 16 52))",
            "MULTIPOLYGON" -> "SRID=4326;MULTIPOLYGON (EMPTY, ((15 51, 16 51, 16 52, 15 51)))"
        )
        for ((tpe, wkt) <- cases) {
            val out = ST_TransformCrs.evalSql(UTF8String.fromString(wkt), UTF8String.fromString("EPSG:32633"))
            assert(out != null, s"$tpe: an EMPTY part must not make the collection NULL")
            val g = JTS.fromWKB(out)
            g.getGeometryType.toUpperCase shouldBe tpe
            g.getSRID shouldBe 32633
            g.getNumGeometries shouldBe 2
            assert(g.getGeometryN(0).isEmpty, s"$tpe: the EMPTY part moved")
            g.getGeometryN(1).getCoordinates.head.x shouldBe (500000.0 +- 1e-3)
        }
    }

    test("ST_TransformCrs.evalSql: a bare EMPTY geometry stays EMPTY with the target SRID") {
        val out = ST_TransformCrs.evalSql(
            UTF8String.fromString("SRID=4326;POINT EMPTY"), UTF8String.fromString("EPSG:32633"))
        assert(out != null)
        val g = JTS.fromWKB(out)
        assert(g.isEmpty)
        g.getSRID shouldBe 32633
    }

    test("ST_TransformCrs.evalSql: partial Z with an EMPTY member keeps the EMPTY member and the real Z") {
        val out = ST_TransformCrs.evalSql(
            UTF8String.fromString(
                "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT (16 52), POINT EMPTY)"),
            UTF8String.fromString("EPSG:32633"))
        assert(out != null)
        val g = JTS.fromWKB(out)
        g.getSRID shouldBe 32633
        g.getNumGeometries shouldBe 3
        g.getGeometryN(0).getCoordinate.z shouldBe 5.0
        assert(g.getGeometryN(1).getCoordinate.z.isNaN)
        assert(g.getGeometryN(2).isEmpty)
    }

    test("ST_TransformCrs.evalSql: partial Z in a MultiPoint with an EMPTY part stays a MultiPoint of 3") {
        val out = ST_TransformCrs.evalSql(
            UTF8String.fromString("SRID=4326;MULTIPOINT Z ((15 51 5), (16 52), EMPTY)"),
            UTF8String.fromString("EPSG:32633"))
        assert(out != null)
        val g = JTS.fromWKB(out)
        g.getGeometryType shouldBe "MultiPoint"
        g.getNumGeometries shouldBe 3
        g.getGeometryN(0).getCoordinate.z shouldBe 5.0
        assert(g.getGeometryN(2).isEmpty)
    }

}
