package com.databricks.labs.gbx.bench

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// `role` (default "sweep") mirrors the Python TileEntry: "bng_gb" marks the
// Great-Britain-overlapping tile the BNG raster->grid / tessellate fns bench
// (they reproject to EPSG:27700 and drop out-of-GB pixels, so the NYC-ish sweep
// tiles bin an empty grid). Defaulted so older corpus.json (no role field) reads
// as "sweep". FAIL_ON_UNKNOWN_PROPERTIES is off, so a present field is honored.
case class TileEntry(path: String, cellid: Long, srid: Int, dtype: String,
                     bands: Int, tile_px: Int, nodata_frac: Double,
                     role: String = "sweep")
case class RowPool(tile_px: Int, bands: Int, dtype: String, tiles: Seq[TileEntry])
case class Corpus(seed: Long, size_sweep: Seq[TileEntry], row_pool: RowPool)

// --- geometry corpus (geometry.json) ----------------------------------------
// Geometry-input fns (rst_rasterize / rst_gridfrompoints / rst_dtmfromgeoms)
// read a deterministic, CRS-correct geometry set that BOTH engines read
// identically. WKB carries no CRS, so the srid is recorded on the set; geometry
// coords are in that CRS. WKB bytes are base64-encoded for JSON transport, so the
// heavy tier decodes the SAME bytes the pyrx tier wrote (write-once-read-both).
// Boxes / points are [base64Wkb, value] pairs; zpoints are bare base64 WKB.
case class GeometrySet(srid: Int, source_tile: String,
                       boxes: Seq[Seq[String]], points: Seq[Seq[String]],
                       zpoints: Seq[String]) {
  private def dec(b64: String): Array[Byte] = java.util.Base64.getDecoder.decode(b64)
  // (wkb, value) decoded pairs for boxes / points.
  def boxPairs: Seq[(Array[Byte], Double)] = boxes.map(p => (dec(p(0)), p(1).toDouble))
  def pointPairs: Seq[(Array[Byte], Double)] = points.map(p => (dec(p(0)), p(1).toDouble))
  def zpointWkbs: Seq[Array[Byte]] = zpoints.map(dec)
}
case class GeometryCorpus(seed: Long, srid: Int, source_tile: String,
                          sets: Map[String, GeometrySet]) {
  // The GeometrySet for a tile: by source_tile path, else by matching srid
  // (geometry is generated per CRS from a representative tile, so a tile whose own
  // path is not a geometry source still gets the in-extent set for its srid).
  def setFor(tilePath: String, tileSrid: Int): Option[GeometrySet] =
    sets.values.find(_.source_tile == tilePath)
      .orElse(sets.values.find(_.srid == tileSrid))
}

object BenchManifest {
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def read(path: String): Corpus = {
    // BenchIO.readBytes is FUSE-safe; corpus.json lives on a dbfs /Volumes mount
    // that rejects java.nio Files.readAllBytes ("Operation not permitted").
    mapper.readValue(BenchIO.readBytes(path), classOf[Corpus])
  }

  /** Read the geometry corpus written alongside corpus.json, or None if absent
    * (older corpora have no geometry.json, so non-geometry runs still work). The read
    * goes through Spark (BenchIO.readBytes); a missing path surfaces as a load failure,
    * which we treat as "absent". */
  def readGeometry(path: String): Option[GeometryCorpus] = {
    try Some(mapper.readValue(BenchIO.readBytes(path), classOf[GeometryCorpus]))
    catch { case _: Exception => None }
  }
}
