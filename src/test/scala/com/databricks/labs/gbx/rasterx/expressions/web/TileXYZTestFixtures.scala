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

  /** Single band with a NoData value of 0 and a 0-filled square in the center. */
  def singleBandWithNoDataHole(): Dataset =
    makeGeoTiff(1, (_, px, py) => if (px >= 24 && px < 40 && py >= 24 && py < 40) 0 else (px + py) % 200 + 20,
      noDataBandVal = Some(0))
}
