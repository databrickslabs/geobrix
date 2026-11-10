package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.fasterxml.jackson.databind.ObjectMapper
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class BenchDispatchTest extends AnyFunSuite with BeforeAndAfterAll {
  var ds: Dataset = _
  override def beforeAll(): Unit = {
    GDALManager.loadSharedObjects(Iterable.empty[String])
    GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
    gdal.AllRegister()
    val tif = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
      .toString.replace("file:/", "/")
    ds = gdal.Open(tif)
  }
  override def afterAll(): Unit = if (ds != null) ds.delete()

  test("registry covers the ds-in functions with categories + min_bands") {
    assert(BenchDispatch.all.toSet.contains("rst_width"))
    // 19 representative + 15 Task 2 scalar + 7 Task 3 coord + 6 Task 4 map/struct
    // + 13 Task 5 tile-out scalar-args + 10 Task 6 tile-out complex-args
    // + 5 bucket-C C1/C2 (readers + buildoverviews + subdataset fns; rst_fromfile
    //   dropped here — lightweight-only now, the JVM cannot read UC Volumes, issue #34)
    // + 3 bucket-C C3 (multi-tile: frombands/combineavg/merge)
    // + 5 bucket-C C4 (tiling: maketiles/retile/tooverlappingtiles/
    //   separatebands/xyzpyramid -> raster_collection fingerprint)
    // + 11 bucket-B B-grid (DGGS: h3_tessellate + 10 {h3,quadbin}
    //   rastertogrid{avg,count,max,median,min} -> dggs_grid fingerprint)
    // + 2 bucket-B B-vec (contour, polygonize -> vector fingerprint; both have left since)
    // + 3 bucket-D geometry-in (rasterize/gridfrompoints/dtmfromgeoms -> raster fp)
    // + 7 bucket-A aggregators (the 7 *_agg, Spark groupBy aggregate harness)
    // + 1 H3 rasterize aggregator (rst_h3_rasterize_agg, grid_aggregate kind)
    // + 9 BNG/quadbin raster-grid fns (5 rst_bng_rastertogrid{avg,count,max,min,
    //   median} + rst_{quadbin,bng}_tessellate + rst_{quadbin,bng}_rasterize_agg)
    // + 9 sum/variance/stddev reducers (rst_{h3,quadbin,bng}_rastertogrid{sum,
    //   variance,stddev})
    // + 3 CRS-string ops (rst_crs accessor, rst_setcrs relabel, rst_transformcrs
    //   warp) -- the string siblings of the benchmarked rst_srid/setsrid/transform.
    // + 3 cellfill grouped aggregators heavy-dispatched here ({h3,bng}_cellfill; quadbin_cellfill left with its companion;
    //   custom_cellfill needs a grid-struct arg the heavy dispatch can't stream, so it
    //   is a pyrx-tier timing-only fn, not counted here) -> 128 + 3 = 131
    // + 6 Stage-3 combine-stats fns (combinemin/max/median/sum/stddev/count) -> 131 + 6 = 137.
    // + 1 rst_align_to (timing-only two-tile warp, format category) -> 137 + 1 = 138.
    // + 4 LiDAR-DSM fns: rst_binpoints + rst_isoband (vector), rst_chm (format),
    //   rst_binpoints_agg (vector aggregator) -> 138 + 4 = 142.
    // + 1 rst_percentile_stretch (edit category) -> 142 + 1 = 143.
    // + 1 rst_h3_tessellate_centroid (a second rst_h3_tessellate case, the default
    //   centroid + complete assignment) -> 143 + 1 = 144.
    // The raster-to-grid reducers (24), rst_retile, rst_merge_agg, rst_rasterize, rst_dtmfromgeoms,
    // rst_gridfrompoints_agg and rst_dtmfromgeoms_agg left with their companions: 144 - 30 = 114.
    // gbx_quadbin_cellfill left with its companion: 111 - 1 = 110.
    // rst_transformcrs, rst_cog_convert, rst_align_to and rst_binpoints left with their companions: 110 - 4 = 106.
    // rst_gridfrompoints left with its companion: 106 - 1 = 105.
    // rst_combine{avg,count,max,median,min,stddev,sum} and rst_bng_tessellate left with their companions: 105 - 8 = 97.
    // the accessors and statistics that return an array, a map or a struct (avg, min, max, pixelcount, sample,
    // getnodata, type, georeference, bandmetadata, subdatasets and the two coordinate pairs) left with their companions: 97 - 12 = 85.
    // rst_buildoverviews left with its companion (light measured 2.2x faster): 85 - 1 = 84.
    // rst_percentile_stretch left with its companion (its JVM form took another percentile rule than light, unmeasured): 84 - 1 = 83.
    // rst_filter left with its companion (its JVM form left a different NoData mask than light, unmeasured): 83 - 1 = 82.
    // rst_slope left with its companion (the GDAL spec engine's JVM form measured no faster than light): 82 - 1 = 81.
    // rst_ndvi, ndwi, nbr, evi, savi, index, threshold, mapalgebra and chm (their light level is the GDAL engine) and
    // rst_contour (it runs the lightweight level only) left with their companions: 81 - 10 = 71.
    // rst_aspect, tpi, roughness and color_relief (measured under 1.2x of light) and the orphan rst_hillshade left with
    // their companions: 71 - 5 = 66.
    // rst_polygonize left with its companion (its light level is the GDAL spec engine's table function): 66 - 1 = 65.
    // rst_viewshed left with its expression (its light level is the GDAL spec engine): 65 - 1 = 64.
    // rst_clip left with its expression (it was never registered; its light level is lightweight only): 64 - 1 = 63.
    // rst_transform, resample, resample_to_res, fillnodata, proximity and summary left with their companions (measured under
    // 1.2x of light, or a different output): 63 - 6 = 57. rst_to_webmercator and rst_resample_to_size left too (0.98x): 57 - 2 = 55.
    // The JVM forms the level order never picks left: the tile and geometry aggregates (combineavg, frombands,
    // derivedband, rasterize, binpoints), the tiling and tessellate generators (maketiles, tooverlappingtiles,
    // separatebands, h3 and quadbin tessellate, and the h3 centroid case), isempty, tryopen, isoband and merge: 55 - 15 = 40.
    assert(BenchDispatch.all.size == 40)
    // rst_h3_rasterize_agg: grid_aggregate kind, dggs category, deterministic 331-cell set.
    assert(BenchDispatch.inputKind("rst_h3_rasterize_agg") == "grid_aggregate")
    assert(BenchDispatch.category("rst_h3_rasterize_agg") == "dggs")
    assert(BenchDispatch.h3RasterizeCells().size == BenchDispatch.h3RaggNCells)
    assert(BenchDispatch.h3RasterizeCells().toSet.size == BenchDispatch.h3RaggNCells)
    // cellfill grouped aggregators: grid_aggregate kind + dggs category (heavy-dispatched
    // {h3,bng}; custom is pyrx-tier timing-only, absent from cats/cellFill).
    for (fn <- Seq("h3_cellfill", "bng_cellfill")) {
      assert(BenchDispatch.inputKind(fn) == "grid_aggregate")
      assert(BenchDispatch.category(fn) == "dggs")
      assert(BenchDispatch.cellFill.contains(fn))
    }
    assert(!BenchDispatch.cellFill.contains("custom_cellfill"))
    // cellFillValues: deterministic NULL subset (a fill over an all-valid set is
    // vacuous) -- NULL every 4th cell (i % 4 == 0), else i.toDouble.
    val cfv8 = BenchDispatch.cellFillValues(8)
    assert(cfv8.length == 8)
    assert(cfv8(0) == null && cfv8(4) == null)
    assert(cfv8(1) == java.lang.Double.valueOf(1.0) && cfv8(7) == java.lang.Double.valueOf(7.0))
    assert(cfv8.count(_ == null) == 2)
    assert(BenchDispatch.cellFillLongCells("h3_cellfill").size == BenchDispatch.h3RaggNCells)
    assert(BenchDispatch.category("rst_xyzpyramid") == "format")
    assert(BenchDispatch.minBands("rst_band") == 2)
    assert(BenchDispatch.minBands("rst_width") == 1)
    assert(BenchDispatch.category("rst_tri") == "terrain")
    assert(BenchDispatch.category("rst_srid") == "accessor")
    // CRS-string ops: rst_crs is an accessor (like rst_srid); rst_setcrs is an edit/relabel (like rst_setsrid).
    assert(BenchDispatch.category("rst_crs") == "accessor")
    assert(BenchDispatch.category("rst_setcrs") == "edit")
    assert(BenchDispatch.inputKind("rst_crs") == "tile")
    assert(BenchDispatch.inputKind("rst_setcrs") == "tile")
  }

  test("BenchDispatch registers the BNG/quadbin raster-grid functions") {
    val aggs = Seq("rst_quadbin_rasterize_agg", "rst_bng_rasterize_agg")
    aggs.foreach { fn =>
      assert(BenchDispatch.all.contains(fn), s"$fn not in BenchDispatch.all")
      // the rasterize_aggs are DGGS shape (like H3).
      assert(BenchDispatch.category(fn) == "dggs", s"$fn should be dggs")
    }
    // The 2 rasterize_aggs route to the grid-aggregate branch (the same branch that
    // holds rst_h3_rasterize_agg), NOT the tile/geometry aggregate branches.
    Seq("rst_h3_rasterize_agg", "rst_quadbin_rasterize_agg", "rst_bng_rasterize_agg").foreach { fn =>
      assert(BenchDispatch.inputKind(fn) == "grid_aggregate", s"$fn should be grid_aggregate")
    }
    // Fixed-cell-set parity contracts (deterministic input for the light-vs-heavy check).
    assert(BenchDispatch.quadbinRasterizeCells().size == BenchDispatch.quadbinRaggNCells)
    assert(BenchDispatch.quadbinRasterizeCells().toSet.size == BenchDispatch.quadbinRaggNCells)
    assert(BenchDispatch.bngRasterizeCells().size == BenchDispatch.bngRaggNCells)
    assert(BenchDispatch.bngRasterizeCells().toSet.size == BenchDispatch.bngRaggNCells)
  }

  test("pureCore runs an accessor and a terrain op, returning a fingerprint") {
    val wfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_width", ds, Map.empty))
    assert(wfp.get("kind").asText() == "scalar")
    val sfp = new ObjectMapper().readTree(BenchDispatch.pureCore("rst_tri", ds, Map.empty))
    assert(sfp.get("kind").asText() == "raster")
  }

}
