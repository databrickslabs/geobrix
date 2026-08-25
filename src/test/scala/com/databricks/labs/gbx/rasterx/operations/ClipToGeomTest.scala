package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ClipToGeomTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("ClipToGeom should clip raster to geometry") {
        // Create a polygon that intersects the raster
        // MODIS tile is in EPSG:32610 (UTM Zone 10N)
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultDs, metadata) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe ds.GetRasterCount
        // Clipped raster should be smaller than original
        resultDs.GetRasterXSize should be <= ds.GetRasterXSize
        resultDs.GetRasterYSize should be <= ds.GetRasterYSize

        metadata should not be null
        metadata should contain key "path"

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should handle small geometries") {
        // Create a very small polygon (smaller than pixel size)
        val wkt = "POLYGON((-8895604 2223901, -8895604 2223900, -8895603 2223900, -8895603 2223901, -8895604 2223901))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        // Should still produce a result even for small geometries (buffered)
        resultDs.GetRasterXSize should be > 0
        resultDs.GetRasterYSize should be > 0

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should respect cutlineAllTouched parameter") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (resultTrue, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)
        val (resultFalse, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = false)

        resultTrue should not be null
        resultFalse should not be null

        // Both should produce valid results
        resultTrue.GetRasterXSize should be > 0
        resultFalse.GetRasterXSize should be > 0

        resultTrue.delete()
        resultFalse.delete()
        geomSR.delete()
    }

    test("ClipToGeom should preserve band count") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val originalBandCount = ds.GetRasterCount

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs.GetRasterCount shouldBe originalBandCount

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should preserve spatial reference") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)
        
        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val originalSR = ds.GetSpatialRef
        val originalEPSG = originalSR.GetAuthorityCode(null)

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        val resultSR = resultDs.GetSpatialRef
        val resultEPSG = resultSR.GetAuthorityCode(null)

        resultEPSG shouldBe originalEPSG

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should handle geometry without SRID") {
        val wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
        val geom = JTS.fromWKT(wkt)

        // Pass null for geomSR - should use raster's SR
        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, null, cutlineAllTouched = true)

        resultDs should not be null
        resultDs.GetRasterXSize should be > 0
        resultDs.GetRasterYSize should be > 0

        resultDs.delete()
    }

    test("ClipToGeom should reproject geometry when geomSR differs from raster CRS") {
        // Raster is World Sinusoidal; build a WGS84 polygon inside the raster's WGS84 footprint
        // (lon [-85..-71], lat [10..20] — see gdalinfo). Polygon is a ~3° × 3° box around (-78, 15).
        // Coordinates are in traditional GIS order (x=lon, y=lat) — the JTS/WKB convention.
        // GDAL 3+ defaults EPSG:4326 to authority-compliant (lat, lon) order, so if we don't
        // flip the axis mapping, GDAL would interpret (-80, 14) as lat=-80 (near south pole),
        // the cutline would miss the raster, and the output would be an all-black TIFF.
        val wkt = "POLYGON((-80 14, -80 17, -77 17, -77 14, -80 14))"
        val geom = JTS.fromWKT(wkt)

        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(4326)

        val (resultDs, _) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        // The reprojected cutline should cover a chunk of the raster — many pixels each way.
        resultDs.GetRasterXSize should be > 100
        resultDs.GetRasterYSize should be > 100

        // Guard against axis-order bugs: a mis-transformed cutline can still leave a non-empty
        // bounding box (GDAL warp pads to the requested window) but fills it entirely with
        // NoData / zeros. Read a sample of pixels and require meaningful variability — the
        // MODIS surface-reflectance band is not uniformly zero over a 3° x 3° Caribbean box.
        val band = resultDs.GetRasterBand(1)
        val sampleW = math.min(64, resultDs.GetRasterXSize)
        val sampleH = math.min(64, resultDs.GetRasterYSize)
        val buf = new Array[Short](sampleW * sampleH)
        band.ReadRaster(0, 0, sampleW, sampleH, sampleW, sampleH, org.gdal.gdalconst.gdalconstConstants.GDT_Int16, buf)
        val nonZero = buf.count(_ != 0)
        withClue(s"clipped raster contains $nonZero non-zero pixels out of ${buf.length} — expected > 10% non-zero, indicates an all-black clip") {
            nonZero should be > (buf.length / 10)
        }

        resultDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should clip with a mixed-Z cutline (some NaN Z) without throwing") {
        // Regression guard for the shipped-rasterx break: OSRTransformGeometry must keep using
        // the always-2D JTS.toWKB for its OGR hop. If it is switched to a Z-aware writer
        // (any-coord rule), a user cutline where only SOME vertices carry Z produces 3D WKB
        // with NaN Z ordinates, OGR fails with 'General Error', and the whole tile is silently
        // error-flagged. ClipToGeom accepts user WKB/WKT cutlines, so mixed Z is reachable.
        val gf = new org.locationtech.jts.geom.GeometryFactory()
        val ring = gf.createLinearRing(Array(
            new org.locationtech.jts.geom.Coordinate(-8900000, 2220000),            // Z = NaN
            new org.locationtech.jts.geom.Coordinate(-8900000, 2200000, 123.5),     // Z = 123.5
            new org.locationtech.jts.geom.Coordinate(-8880000, 2200000),            // Z = NaN
            new org.locationtech.jts.geom.Coordinate(-8880000, 2220000),            // Z = NaN
            new org.locationtech.jts.geom.Coordinate(-8900000, 2220000)             // Z = NaN (close)
        ))
        val geom = gf.createPolygon(ring)

        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        // The 2D equivalent of the same footprint — Z is irrelevant to a cutline's 2D extent,
        // so the mixed-Z clip must produce a byte-identical window, not merely a non-empty one.
        val geom2d = JTS.fromWKT(
            "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, " +
            "-8880000 2220000, -8900000 2220000))")

        val (resultDs, metadata) = ClipToGeom.clip(ds, Map.empty, geom, geomSR, cutlineAllTouched = true)
        val (expectedDs, _) = ClipToGeom.clip(ds, Map.empty, geom2d, geomSR, cutlineAllTouched = true)

        resultDs should not be null
        resultDs.GetRasterCount shouldBe ds.GetRasterCount
        metadata should contain key "path"

        // Dimensions must match the 2D clip exactly. A weaker "> 0" assertion would still pass
        // if the mixed-Z cutline were mangled into a degenerate or wrongly-placed window.
        withClue("mixed-Z cutline must clip to the same window as its 2D equivalent: ") {
            resultDs.GetRasterXSize shouldBe expectedDs.GetRasterXSize
            resultDs.GetRasterYSize shouldBe expectedDs.GetRasterYSize
        }

        // And the pixels must actually match — same window filled with the same data, proving
        // the Z ordinates did not perturb the reprojected cutline geometry.
        val w = resultDs.GetRasterXSize
        val h = resultDs.GetRasterYSize
        val got = new Array[Short](w * h)
        val want = new Array[Short](w * h)
        val gt = org.gdal.gdalconst.gdalconstConstants.GDT_Int16
        resultDs.GetRasterBand(1).ReadRaster(0, 0, w, h, w, h, gt, got)
        expectedDs.GetRasterBand(1).ReadRaster(0, 0, w, h, w, h, gt, want)
        withClue(s"mixed-Z clip pixels must equal the 2D clip pixels (${w}x$h): ") {
            got shouldBe want
        }

        resultDs.delete()
        expectedDs.delete()
        geomSR.delete()
    }

    test("ClipToGeom should clip with a clean 3D cutline (all vertices have Z)") {
        // Companion to the mixed-Z case: an all-Z cutline must clip identically to its 2D
        // equivalent — Z is irrelevant to a 2D cutline footprint, and must not make OGR fail.
        val gf = new org.locationtech.jts.geom.GeometryFactory()
        val ring = gf.createLinearRing(Array(
            new org.locationtech.jts.geom.Coordinate(-8900000, 2220000, 10.0),
            new org.locationtech.jts.geom.Coordinate(-8900000, 2200000, 20.0),
            new org.locationtech.jts.geom.Coordinate(-8880000, 2200000, 30.0),
            new org.locationtech.jts.geom.Coordinate(-8880000, 2220000, 40.0),
            new org.locationtech.jts.geom.Coordinate(-8900000, 2220000, 10.0)
        ))
        val geom3d = gf.createPolygon(ring)
        val geom2d = JTS.fromWKT(
            "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, " +
            "-8880000 2220000, -8900000 2220000))")

        val geomSR = new SpatialReference()
        geomSR.ImportFromEPSG(32610)

        val (result3d, _) = ClipToGeom.clip(ds, Map.empty, geom3d, geomSR, cutlineAllTouched = true)
        val (result2d, _) = ClipToGeom.clip(ds, Map.empty, geom2d, geomSR, cutlineAllTouched = true)

        result3d should not be null
        result3d.GetRasterXSize shouldBe result2d.GetRasterXSize
        result3d.GetRasterYSize shouldBe result2d.GetRasterYSize
        result3d.GetRasterCount shouldBe ds.GetRasterCount

        result3d.delete()
        result2d.delete()
        geomSR.delete()
    }

}

