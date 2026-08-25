package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.gdal
import org.gdal.osr.{SpatialReference, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class OSRTransformGeometryTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    test("transform should interpret JTS (x=lon, y=lat) WKT correctly even when srcSR uses authority-compliant axis order") {
        // This is the core regression guard for the "rst_clip all-black output" bug on Databricks:
        // GDAL 3+ defaults an EPSG:4326 SpatialReference to authority-compliant (lat, lon) axis
        // order. If OSRTransformGeometry.transform naively used that SR to interpret the input
        // WKB, JTS's (x=lon, y=lat) coordinates would be read as (lat, lon) and a clip cutline
        // for a Caribbean polygon (lon=-80, lat=14) would end up near the south pole.
        //
        // We force the source SR into authority-compliant mode so this test fails on any GDAL
        // build where the transform utility doesn't enforce traditional order internally — the
        // test is independent of whatever the GDAL default happens to be.
        val srcSR = new SpatialReference()
        srcSR.ImportFromEPSG(4326)
        srcSR.SetAxisMappingStrategy(osrConstants.OAMS_AUTHORITY_COMPLIANT)

        // Web Mercator destination — deterministic, metric, easy to assert against.
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(3857)
        dstSR.SetAxisMappingStrategy(osrConstants.OAMS_AUTHORITY_COMPLIANT)

        // Point in traditional (x=lon, y=lat) order: lon=-80°, lat=15° (Caribbean).
        // Expected EPSG:3857 projection: x ≈ -8,905,559 m, y ≈ 1,689,200 m.
        val point = JTS.fromWKT("POINT(-80 15)")

        val projected = OSRTransformGeometry.transform(point, srcSR, dstSR)

        val coord = projected.getCoordinate
        withClue(s"Expected Caribbean point to project near (-8.9e6, 1.69e6) Web Mercator; got (${coord.x}, ${coord.y}). " +
            "Far-off values indicate the axis-order guard in OSRTransformGeometry regressed.") {
            coord.x shouldBe (-8905559.0 +- 1000.0)
            coord.y shouldBe (  1689200.0 +- 1000.0)
        }

        srcSR.delete()
        dstSR.delete()
    }

    test("transform should not mutate the caller's SpatialReference axis mapping") {
        // We clone inside transform; caller-owned SRs must be left untouched so nothing
        // downstream observes a surprise axis-order flip.
        val srcSR = new SpatialReference()
        srcSR.ImportFromEPSG(4326)
        srcSR.SetAxisMappingStrategy(osrConstants.OAMS_AUTHORITY_COMPLIANT)
        val beforeMapping = srcSR.GetAxisMappingStrategy

        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(3857)

        val point = JTS.fromWKT("POINT(-80 15)")
        OSRTransformGeometry.transform(point, srcSR, dstSR)

        srcSR.GetAxisMappingStrategy shouldBe beforeMapping

        srcSR.delete()
        dstSR.delete()
    }

    test("transform should short-circuit when srcSR equals dstSR") {
        // IsSame returns 1 when the CRS definitions match (independent of axis mapping);
        // we skip the OGR round-trip entirely and return the original geometry untouched.
        val srcSR = new SpatialReference()
        srcSR.ImportFromEPSG(4326)

        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(4326)

        val point = JTS.fromWKT("POINT(-80 15)")
        val result = OSRTransformGeometry.transform(point, srcSR, dstSR)

        result should be theSameInstanceAs point

        srcSR.delete()
        dstSR.delete()
    }

    test("transform: mixed-Z polygon (some NaN Z) completes without throwing") {
        // A cutline with one NaN Z vertex and one finite Z vertex — ClipToGeom passes such
        // geometries from user WKB/WKT. Before the round-2 regression, toWKBAdaptive produced
        // 3D WKB with NaN Z → OGR 'General Error'. After the revert to toWKB (2D only), OGR
        // receives a clean 2D polygon and the transform succeeds.
        val gf = new org.locationtech.jts.geom.GeometryFactory()
        val ring = gf.createLinearRing(Array(
            new org.locationtech.jts.geom.Coordinate(0.0, 0.0),      // Z = NaN
            new org.locationtech.jts.geom.Coordinate(1.0, 0.0, 5.0), // Z = 5
            new org.locationtech.jts.geom.Coordinate(1.0, 1.0),      // Z = NaN
            new org.locationtech.jts.geom.Coordinate(0.0, 0.0)       // Z = NaN (close ring)
        ))
        val poly = gf.createPolygon(ring)

        val srcSR = new SpatialReference()
        srcSR.ImportFromEPSG(4326)
        val dstSR = new SpatialReference()
        dstSR.ImportFromEPSG(3857)

        // Must NOT throw (pre-fix: toWKBAdaptive + OGR = General Error)
        val result = OSRTransformGeometry.transform(poly, srcSR, dstSR)
        assert(result != null)

        srcSR.delete()
        dstSR.delete()
    }

}
