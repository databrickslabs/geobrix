package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_Crs
import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_SRID
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_SetCrs
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.osr.SpatialReference
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Direct-execute tests for the CRS-string ops (RST_Crs / RST_SetCrs / RST_TransformCrs)
  * plus the SpatialRefOps.resolveCrs / crsToCanonical helpers. The MODIS fixture is a
  * NON-EPSG raster (World Sinusoidal — the case that surfaced the need for CRS strings),
  * so it exercises the authority-else-WKT + non-EPSG warp paths that rst_srid (int/0)
  * cannot represent. Requires GDAL native libs (run in Docker).
  */
class RST_CrsOpsTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tif = this.getClass
            .getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
            .toString.replace("file:/", "/")
        ds = gdal.Open(tif)
    }

    override def afterAll(): Unit = if (ds != null) ds.delete()

    // --- SpatialRefOps.resolveCrs: the shared int-cast rule ---

    test("resolveCrs: int-castable string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs("4326")
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
        sr.delete()
    }

    test("resolveCrs: whitespace-padded int-string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs(" 32633 ")
        SpatialRefOps.getEPSGCode(sr) shouldBe 32633
        sr.delete()
    }

    test("resolveCrs: EPSG authority string -> EPSG") {
        val sr = SpatialRefOps.resolveCrs("EPSG:4326")
        SpatialRefOps.getEPSGCode(sr) shouldBe 4326
        sr.delete()
    }

    test("resolveCrs: non-EPSG ESRI authority string resolves (still valid, no EPSG code)") {
        val sr = SpatialRefOps.resolveCrs("ESRI:54008")
        sr should not be null
        SpatialRefOps.getEPSGCode(sr) shouldBe 0 // non-EPSG
        // Round-trips through crsToCanonical.
        SpatialRefOps.crsToCanonical(sr) shouldBe "ESRI:54008"
        sr.delete()
    }

    test("resolveCrs: WKT string resolves") {
        val ref = new SpatialReference()
        ref.ImportFromEPSG(4326)
        val wkt = ref.ExportToWkt()
        val sr = SpatialRefOps.resolveCrs(wkt)
        sr.IsSame(ref) shouldBe 1
        sr.delete(); ref.delete()
    }

    test("resolveCrs: garbage throws") {
        an[IllegalArgumentException] should be thrownBy SpatialRefOps.resolveCrs("not-a-crs-@@")
    }

    // --- crsToCanonical: authority-else-WKT ---

    test("crsToCanonical: EPSG SR -> 'EPSG:code'") {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(4326)
        SpatialRefOps.crsToCanonical(sr) shouldBe "EPSG:4326"
        sr.delete()
    }

    test("crsToCanonical: null SR -> null") {
        SpatialRefOps.crsToCanonical(null) shouldBe null
    }

    // --- RST_Crs accessor ---

    test("RST_Crs returns a non-null canonical string on the non-EPSG MODIS fixture") {
        val crs = RST_Crs.execute(ds)
        crs should not be null
        crs.nonEmpty shouldBe true
    }

    // --- Cross-tier CRS parity (T8): the non-EPSG MODIS fixture must report an
    // EQUIVALENT CRS on the heavy tier and the light tier. The light tier
    // (rasterio/pyproj) reads this exact fixture as the authority string
    // "ESRI:54008" (pinned in the pyrx suite test_crs_accessors.test_crs_esri_raster:
    // accessors.crs(ds) == "ESRI:54008"), because pyproj auto-identifies the
    // embedded World Sinusoidal WKT against the ESRI authority DB.
    //
    // The heavy tier reads the SAME fixture and, per the authority-else-WKT rule
    // (SpatialRefOps.crsToCanonical), emits the embedded WKT (PROJCS["World_Sinusoidal",
    // ...]) VERBATIM — GDAL does not auto-attach an ESRI AUTHORITY node to a raw
    // GeoTIFF whose header WKT carries none, so GetAuthorityName(null) is null and
    // the canonical form is the WKT. This is a canonical-STRING divergence between
    // the two CRS libraries, NOT a CRS divergence: both strings describe the same
    // World Sinusoidal / WGS84 CRS. The cross-tier contract is CRS-EQUIVALENCE
    // (spec: pixels+georeference+CRS identical across tiers), so we assert the heavy
    // canonical string resolves to the SAME SpatialReference as "ESRI:54008".
    //
    // (When a raster is EXPLICITLY tagged ESRI:54008 via RST_SetCrs — see the test
    // below — heavy DOES round-trip the "ESRI:54008" authority string, because
    // SetFromUserInput imports the ESRI authority node. The divergence here is
    // specific to a raw fixture whose embedded WKT lacks an authority node.)
    test("RST_Crs on the MODIS fixture is CRS-equivalent to the light tier's 'ESRI:54008' (cross-tier CRS parity)") {
        val heavyCanonical = RST_Crs.execute(ds)
        heavyCanonical should not be null
        // Heavy resolves this raw authority-less fixture to WKT (documented above).
        heavyCanonical should startWith("PROJCS")
        // CRS-EQUIVALENCE across tiers: the heavy canonical string and the light
        // tier's "ESRI:54008" resolve to the SAME CRS (IsSame == 1).
        val heavySr = SpatialRefOps.resolveCrs(heavyCanonical)
        val esriSr = SpatialRefOps.resolveCrs("ESRI:54008")
        try {
            heavySr.IsSame(esriSr) shouldBe 1
        } finally {
            heavySr.delete(); esriSr.delete()
        }
        // Non-EPSG on the heavy tier: srid is 0 (light reports None) — parity of the
        // "no EPSG code for this CRS" contract that motivated the CRS-string accessor.
        RST_SRID.execute(ds) shouldBe 0
    }

    test("RST_Crs and RST_SRID agree on an EPSG raster (relabelled to 4326)") {
        // Relabel the fixture to EPSG:4326 (header only), then read both accessors back.
        val (relabelled, _) = RST_SetCrs.execute(ds, Map.empty, "4326")
        try {
            RST_Crs.execute(relabelled) shouldBe "EPSG:4326"
            RST_SRID.execute(relabelled) shouldBe 4326
        } finally relabelled.delete()
    }

    // --- RST_SetCrs relabel (int-cast rule) ---

    test("RST_SetCrs with int-string == EPSG relabel (no reproject, pixels/size unchanged)") {
        val (out, _) = RST_SetCrs.execute(ds, Map.empty, "4326")
        try {
            RST_SRID.execute(out) shouldBe 4326
            out.GetRasterXSize shouldBe ds.GetRasterXSize
            out.GetRasterYSize shouldBe ds.GetRasterYSize
        } finally out.delete()
    }

    test("RST_SetCrs accepts a non-EPSG target (ESRI) without requiring a positive EPSG") {
        val (out, _) = RST_SetCrs.execute(ds, Map.empty, "ESRI:54008")
        try {
            RST_Crs.execute(out) shouldBe "ESRI:54008"
            out.GetRasterXSize shouldBe ds.GetRasterXSize
        } finally out.delete()
    }

    // --- RST_TransformCrs warp (accepts non-EPSG; RasterProject WKT fallback) ---

    test("RST_TransformCrs to EPSG:4326 reprojects (band count preserved, srid changes)") {
        val (out, _) = RST_TransformCrs.execute(ds, Map.empty, "EPSG:4326")
        try {
            out should not be null
            out.GetRasterCount shouldBe ds.GetRasterCount
            RST_SRID.execute(out) shouldBe 4326
        } finally out.delete()
    }

    test("RST_TransformCrs to a non-EPSG (ESRI) target warps via the WKT fallback") {
        // ESRI:54009 (World Mollweide) has no EPSG code -> RasterProject's WKT fallback
        // must kick in (authName:authCode would be broken). A bare int-EPSG path would
        // have thrown; this must produce a valid reprojected raster.
        val (out, _) = RST_TransformCrs.execute(ds, Map.empty, "ESRI:54009")
        try {
            out should not be null
            out.GetRasterCount shouldBe ds.GetRasterCount
            out.GetRasterXSize should be > 0
            out.GetRasterYSize should be > 0
            // Reprojected raster carries a valid CRS string (authority or WKT).
            RST_Crs.execute(out) should not be null
        } finally out.delete()
    }
}
