package com.databricks.labs.gbx.rasterx.expressions.web

import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

/** In-memory GTiff fixtures for RST_TileXYZ tests, placed over a known WebMercator
 *  z=8 tile so execute() produces a data-carrying tile (not the transparent fallback).
 *  Footprint: lon 10..12, lat 48..50 (EPSG:4326) -- mirrors the Python parity fixture.
 *
 *  Tile coordinates verified via the slippy-map formula:
 *    lon=11, lat=49 -> z=8 x=135 y=87
 *  (the plan draft had x=134 y=86 which does not overlap; corrected here). */
object TileXYZTestFixtures {
  // z=8 tile covering lon~11, lat~49 (the fixture midpoint). Computed via slippy-map formula.
  val z = 8; val x = 135; val y = 87

  private def wgs84Wkt: String = {
    val srs = new SpatialReference(); srs.ImportFromEPSG(4326); srs.ExportToWkt()
  }

  private def makeGeoTiff(nbands: Int, fill: (Int, Int, Int) => Int,
                          noDataBandVal: Option[Int] = None): Dataset = {
    val w = 64; val h = 64
    val mem = gdal.GetDriverByName("MEM").Create("", w, h, nbands, gdalconstConstants.GDT_Byte)
    // lon 10..12, lat 48..50 -> pixel size 2/64 in each axis (north-up).
    mem.SetGeoTransform(Array(10.0, 2.0 / w, 0.0, 50.0, 0.0, -2.0 / h))
    mem.SetProjection(wgs84Wkt)
    for (b <- 1 to nbands) {
      val buf = Array.tabulate(w * h)(i => fill(b, i % w, i / w).toByte)
      mem.GetRasterBand(b).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
      noDataBandVal.foreach(mem.GetRasterBand(b).SetNoDataValue(_))
    }
    mem
  }

  def singleBandOverTile(): Dataset =
    makeGeoTiff(1, (_, px, py) => (px + py) % 200 + 20)

  def threeBandOverTile(): Dataset =
    makeGeoTiff(3, (b, px, _) => (px * b) % 200 + 20)

  /** Single-band fixture with an internal NoData=0 hole. Extent is widened to
   *  lon [9, 13] / lat [47, 51] so tile z=8 x=135 y=87 is FULLY within the footprint
   *  (no outside-footprint alpha=0 strip -- the only source of alpha=0 is the hole).
   *  Hole at fixture pixels [17,32) x [23,33): covers the center of the tile. All 4
   *  tile corners map outside this range so corners render alpha=255. */
  def singleBandWithNoDataHole(): Dataset = {
    val w = 64; val h = 64
    val mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdalconstConstants.GDT_Byte)
    // Wider extent: lon [9,13], lat [47,51] -- 4 degrees per side, pixel size 0.0625 deg/px.
    mem.SetGeoTransform(Array(9.0, 4.0 / w, 0.0, 51.0, 0.0, -4.0 / h))
    mem.SetProjection(wgs84Wkt)
    val buf = Array.tabulate(w * h) { i =>
      val px = i % w; val py = i / w
      if (px >= 17 && px < 32 && py >= 23 && py < 33) 0 else (px + py) % 200 + 20
    }.map(_.toByte)
    mem.GetRasterBand(1).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
    mem.GetRasterBand(1).SetNoDataValue(0)
    mem
  }

  /** Two-band uint16 fixture placed over the tile. Band 1 values are in [100, 200];
   *  band 2 values are in [20000, 30000]. The wide gap between band ranges means "auto"
   *  rescale produces very different scale pairs per band -- required for the N==2
   *  grey R==G==B correctness test (FINDING 1 proof). */
  def twoBandUint16OverTile(): Dataset = {
    val w = 64; val h = 64
    val mem = gdal.GetDriverByName("MEM").Create("", w, h, 2, gdalconstConstants.GDT_UInt16)
    mem.SetGeoTransform(Array(10.0, 2.0 / w, 0.0, 50.0, 0.0, -2.0 / h))
    mem.SetProjection(wgs84Wkt)
    // Band 1: small range [100, 200]. "auto" rescale maps this to full [0, 255].
    val buf1 = Array.tabulate(w * h)(i => 100 + i % 101)
    mem.GetRasterBand(1).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Int32, buf1)
    // Band 2: large range [20000, 30000]. Entirely different scale from band 1.
    // For N==2, band 2 is used as the alpha source (rio-tiler mapping).
    // copyBandVerbatim reads it as Byte -> clamped to 255 for all values > 255.
    val buf2 = Array.tabulate(w * h)(i => 20000 + i % 10001)
    mem.GetRasterBand(2).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Int32, buf2)
    mem
  }

  /** Four-band uint8 fixture. Bands 1-3 carry colour data; band 4 is fully opaque (255)
   *  so N==4 alpha-preservation can be verified. */
  def fourBandOverTile(): Dataset =
    makeGeoTiff(4, (b, px, _) => if (b == 4) 255 else (px * b) % 200 + 20)
}
