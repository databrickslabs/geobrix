package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.operations.BandAccessors
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.gdal.gdal.Dataset
import org.locationtech.jts.geom.Geometry

import java.security.MessageDigest

/** Output fingerprint matching python bench/fingerprint.py for cross-API consistency. */
object BenchFingerprint {
  private val mapper = new ObjectMapper()

  /** Timing-only sentinel: an empty fingerprint. The comparator treats an empty
    * fingerprint on either side as `na` (timed, not compared), so functions whose
    * output cannot be made cross-engine-comparable (maps/structs, CRS/render-
    * dependent bytes, on-disk-vs-in-memory sizes) emit this after executing for
    * real timing. */
  val empty: String = ""

  def ofScalar(v: Any): String = {
    val n = mapper.createObjectNode()
    n.put("kind", "scalar")
    v match {
      case i: Int    => n.put("value", i)
      case l: Long   => n.put("value", l)
      case d: Double => n.put("value", d)
      case other     => n.put("value", other.toString)
    }
    mapper.writeValueAsString(n)
  }

  def ofArray(values: Array[Double]): String = {
    val n = mapper.createObjectNode()
    n.put("kind", "scalar_list")
    val arr: ArrayNode = n.putArray("values")
    values.foreach(arr.add)
    mapper.writeValueAsString(n)
  }

  // Boxed variant for reducers that return NULL for all-nodata bands (RST_Max/Min/Avg/Median).
  // A null element is emitted as a JSON null — matching the python bench (None -> null), NOT NaN —
  // so cross-tier fingerprints agree on all-nodata inputs.
  def ofArray(values: Array[java.lang.Double]): String = {
    val n = mapper.createObjectNode()
    n.put("kind", "scalar_list")
    val arr: ArrayNode = n.putArray("values")
    values.foreach(v => if (v == null) arr.addNull() else arr.add(v.doubleValue()))
    mapper.writeValueAsString(n)
  }

  /** Fingerprint a COLLECTION of output tiles (bucket C, group C4 tiling fns).
    *
    * Mirrors python bench/fingerprint.py `fingerprint_collection`: records the tile
    * COUNT plus the agg stats POOLED over every tile's valid (non-nodata) pixels
    * across all bands. Pooling is ORDER-INDEPENDENT, so heavy and light may emit
    * tiles in any order and still agree; the comparator compares `count` exactly. */
  def ofCollection(tiles: Seq[Dataset]): String = {
    val pooled = scala.collection.mutable.ArrayBuffer.empty[Double]
    tiles.foreach { ds =>
      val w = ds.GetRasterXSize()
      val h = ds.GetRasterYSize()
      for (bi <- 1 to ds.GetRasterCount()) {
        val band = ds.GetRasterBand(bi)
        val buf = Array.ofDim[Double](w * h)
        band.ReadRaster(0, 0, w, h, buf)
        val nod = BandAccessors.getNoDataValue(band)
        val valid = if (nod.isNaN) buf else buf.filterNot(_ == nod)
        pooled ++= valid
        band.delete()
      }
    }
    val n = mapper.createObjectNode()
    n.put("kind", "raster_collection")
    n.put("count", tiles.length)
    val agg: ObjectNode = n.putObject("agg")
    if (pooled.isEmpty) {
      agg.putNull("min"); agg.putNull("max"); agg.putNull("mean"); agg.putNull("std")
    } else {
      val mean = pooled.sum / pooled.length
      val variance = pooled.map(x => (x - mean) * (x - mean)).sum / pooled.length
      agg.put("min", pooled.min)
      agg.put("max", pooled.max)
      agg.put("mean", mean)
      agg.put("std", math.sqrt(variance))
    }
    mapper.writeValueAsString(n)
  }

  /** Write min/max/mean/std (population) of `vals` into `node`; nulls when empty.
    * Shared by every stats-bearing kind so the JSON shape matches python `_stat`. */
  private def putStats(node: ObjectNode, vals: Seq[Double]): Unit = {
    if (vals.isEmpty) {
      node.putNull("min"); node.putNull("max"); node.putNull("mean"); node.putNull("std")
    } else {
      val mean = vals.sum / vals.length
      val variance = vals.map(x => (x - mean) * (x - mean)).sum / vals.length
      node.put("min", vals.min)
      node.put("max", vals.max)
      node.put("mean", mean)
      node.put("std", math.sqrt(variance))
    }
  }

  /** Fingerprint a discrete-global-grid output (bucket B grid fns).
    *
    * Mirrors python `fingerprint_dggs_grid`: `cells` is the per-band grid output
    * (`RST_{H3,Quadbin}_RasterToGrid*.execute` returns `Array[Array[(Long, T)]]`).
    * Records the cell COUNT, a sha256 over the SORTED (signed-int64) cell ids, the
    * sorted ids themselves, and order-independent agg stats over the measures.
    * H3/quadbin ids are signed Longs here and parity-comparable with light. */
  def ofDggsGrid(cells: Seq[Array[(Long, Double)]]): String = {
    val ids = scala.collection.mutable.ArrayBuffer.empty[Long]
    val vals = scala.collection.mutable.ArrayBuffer.empty[Double]
    cells.foreach(_.foreach { case (cid, measure) => ids += cid; vals += measure })
    dggsGridJson(ids.toSeq, Some(vals.toSeq))
  }

  /** Count-only dggs_grid fingerprint for tessellation (no per-cell measure).
    *
    * `RST_H3_Tessellate` yields one Dataset per cell with NO scalar measure, so
    * the fingerprint records only the cell COUNT + sorted-id hash and emits an
    * EMPTY `agg` object -- matching the python tessellate path, which fingerprints
    * `[(cellid, bytes)]` and produces `agg == {}` (no measures). Passing 0.0
    * measures instead would yield a non-empty heavy agg ({min:0,...}) that
    * disagrees with the light `{}`, so tessellation must use this id-only form. */
  def ofDggsGridIds(ids: Seq[Long]): String = dggsGridJson(ids, None)

  /** Shared dggs_grid JSON builder: cell COUNT, sha256 over SORTED signed-int64
    * cell ids, the sorted ids, and the agg. `vals == None` (tessellation) emits an
    * EMPTY agg object to mirror python's `agg == {}` when there are no measures;
    * `Some(vals)` runs `putStats` (which itself emits nulls only when vals empty). */
  private def dggsGridJson(ids: Seq[Long], vals: Option[Seq[Double]]): String = {
    val sorted = ids.sorted
    val joined = sorted.map(_.toString).mkString("\n")
    val digest = MessageDigest.getInstance("SHA-256").digest(joined.getBytes("UTF-8"))
    val hashHex = digest.map(b => f"${b & 0xff}%02x").mkString
    val n = mapper.createObjectNode()
    n.put("kind", "dggs_grid")
    n.put("count", sorted.length)
    n.put("cells_hash", hashHex)
    val idArr: ArrayNode = n.putArray("cell_ids")
    sorted.foreach(idArr.add)
    val agg: ObjectNode = n.putObject("agg")
    vals.foreach(v => putStats(agg, v))
    mapper.writeValueAsString(n)
  }

  /** Fingerprint a vector-feature output (bucket B vector fns: contour, polygonize).
    *
    * Mirrors python `fingerprint_vector`: `features` is a set of `(geometry, attr)`
    * pairs. Records the feature COUNT, the total `measure` (JTS `getLength` for line
    * geometries, else `getArea` for polygons), and order-independent agg stats over
    * the attributes. Summing the per-feature measure is ORDER-INDEPENDENT. */
  def ofVector(features: Seq[(Geometry, Double)]): String = {
    val geoms = features.map(_._1)
    val attrs = features.map(_._2)
    val isLines = geoms.exists { g =>
      val t = g.getGeometryType
      t == "LineString" || t == "MultiLineString"
    }
    val measure = if (isLines) geoms.map(_.getLength).sum else geoms.map(_.getArea).sum
    val n = mapper.createObjectNode()
    n.put("kind", "vector")
    n.put("count", geoms.length)
    n.put("measure", measure)
    val agg: ObjectNode = n.putObject("attr_agg")
    putStats(agg, attrs)
    mapper.writeValueAsString(n)
  }

  def ofDataset(ds: Dataset): String = {
    val w = ds.GetRasterXSize()
    val h = ds.GetRasterYSize()
    val n = mapper.createObjectNode()
    n.put("kind", "raster")
    val bands: ArrayNode = n.putArray("bands")
    for (bi <- 1 to ds.GetRasterCount()) {
      val band = ds.GetRasterBand(bi)
      val buf = Array.ofDim[Double](w * h)
      band.ReadRaster(0, 0, w, h, buf)
      val nod = BandAccessors.getNoDataValue(band)
      val valid = if (nod.isNaN) buf else buf.filterNot(_ == nod)
      val bn: ObjectNode = mapper.createObjectNode()
      val shape = bn.putArray("shape"); shape.add(h); shape.add(w)
      bn.put("dtype", BandAccessors.dataTypeHuman(band))
      bn.put("nodata_count", (buf.length - valid.length).toLong)
      if (valid.isEmpty) {
        bn.putNull("min"); bn.putNull("max"); bn.putNull("mean"); bn.putNull("std")
      } else {
        val mean = valid.sum / valid.length
        val variance = valid.map(x => (x - mean) * (x - mean)).sum / valid.length
        bn.put("min", valid.min)
        bn.put("max", valid.max)
        bn.put("mean", mean)
        bn.put("std", math.sqrt(variance))
      }
      bands.add(bn)
      band.delete()
    }
    mapper.writeValueAsString(n)
  }
}
