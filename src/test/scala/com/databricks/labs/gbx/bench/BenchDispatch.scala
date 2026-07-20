package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.expressions.RST_IsEmpty
import com.databricks.labs.gbx.rasterx.expressions.RST_NDVI
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoord
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordY
import com.databricks.labs.gbx.rasterx.expressions.RST_Transform
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoord
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordY
import com.databricks.labs.gbx.rasterx.expressions.RST_AsFormat
import com.databricks.labs.gbx.rasterx.expressions.RST_Clip
import com.databricks.labs.gbx.rasterx.expressions.RST_Convolve
import com.databricks.labs.gbx.rasterx.expressions.RST_DerivedBand
import com.databricks.labs.gbx.rasterx.expressions.RST_Filter
import com.databricks.labs.gbx.rasterx.expressions.RST_InitNoData
import com.databricks.labs.gbx.rasterx.expressions.RST_MapAlgebra
import com.databricks.labs.gbx.rasterx.expressions.RST_Merge
import com.databricks.labs.gbx.rasterx.expressions.RST_CombineAvg
import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromBands
import com.databricks.labs.gbx.rasterx.expressions.RST_UpdateType
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_BuildOverviews
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_CogConvert
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Contour
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Proximity
import com.databricks.labs.gbx.rasterx.expressions.analysis.RST_Viewshed
import com.databricks.labs.gbx.rasterx.expressions.vector.RST_Polygonize
import com.databricks.labs.gbx.rasterx.expressions.vector.RST_Rasterize
import com.databricks.labs.gbx.rasterx.expressions.grid.RST_GridFromPoints
import com.databricks.labs.gbx.rasterx.expressions.RST_DTMFromGeoms
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.expressions.dem._
import com.databricks.labs.gbx.rasterx.expressions.spectral._
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Band
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_FillNodata
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Histogram
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Sample
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_SetSrid
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Threshold
import com.databricks.labs.gbx.rasterx.expressions.resample.RST_Resample
import com.databricks.labs.gbx.rasterx.expressions.resample.RST_ResampleToRes
import com.databricks.labs.gbx.rasterx.expressions.resample.RST_ResampleToSize
import com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZ
import com.databricks.labs.gbx.rasterx.expressions.web.RST_ToWebMercator
import com.databricks.labs.gbx.rasterx.expressions.grid._
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.{BalancedSubdivision, BoundingBox, OverlappingTiles, RasterTessellate, ReTile, SeparateBands}
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.util.NodeFilePathUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset
import org.locationtech.jts.geom.{Geometry, LineString}

import java.nio.file.Files

/** Maps a bench fn name to its pure-core call (-> fingerprint) and its Spark Column. */
object BenchDispatch {
  private def argS(a: Map[String, String], k: String, d: String) = a.getOrElse(k, d)
  private def argD(a: Map[String, String], k: String, d: Double) = a.get(k).map(_.toDouble).getOrElse(d)
  private def argI(a: Map[String, String], k: String, d: Int) = a.get(k).map(_.toInt).getOrElse(d)
  private def argB(a: Map[String, String], k: String, d: Boolean) = a.get(k).map(_.toBoolean).getOrElse(d)
  // ARRAY<INT> arg (e.g. buildoverviews levels) ridden as a comma/space-separated
  // string in the stringly-typed bench args map; falls back to `d` when absent.
  private def argIntArray(a: Map[String, String], k: String, d: Array[Int]): Array[Int] =
    a.get(k).map(_.split("[,\\s]+").filter(_.nonEmpty).map(_.trim.toInt)).getOrElse(d)
  // ARRAY<DOUBLE> arg (e.g. contour levels) ridden as a comma/space-separated
  // string in the stringly-typed bench args map; falls back to `d` when absent.
  private def argDoubleArray(a: Map[String, String], k: String, d: Array[Double]): Array[Double] =
    a.get(k).map(_.split("[,\\s]+").filter(_.nonEmpty).map(_.trim.toDouble)).getOrElse(d)

  private val ACC = "accessor"; private val TER = "terrain"
  private val BM = "band-math"; private val WARP = "warp"
  private val EDIT = "edit"; private val FEAT = "features"
  private val FOCAL = "focal"; private val FMT = "format"; private val RES = "resample"
  private val ANALYSIS = "analysis"; private val DGGS = "dggs"
  private val VECTOR = "vector"

  // Functions that need a >=2-band source even though they are not band-math
  // (rst_band selects band 2; rst_evi/savi/index read a second band on the
  // 2-band corpus).
  private val multiBand: Set[String] = Set("rst_band", "rst_evi", "rst_savi", "rst_index")

  // --- Task 6: complex-arg constants, hardcoded identically to the pyrx side ---
  // The band-map, the map-algebra expression, and the derived-band pixel function
  // cannot ride the stringly-typed bench args map, so they are fixed here exactly
  // as in spec.py (same policy as the convolve kernel).
  private val indexName = "ndvi"
  private val indexBandMap: Map[String, Int] = Map("red" -> 1, "nir" -> 2)
  // RST_MapAlgebra.execute takes a JSON spec (rasters A-Z + "calc"), NOT a bare
  // gdal_calc expression. With no explicit A_index the single input binds to A,
  // so {"calc":"A*2"} is the heavy-side equivalent of the pyrx "A*2" expression.
  private val mapAlgebraExpr = """{"calc":"A*2"}"""
  private val derivedBandFuncName = "mean_bands"
  private val derivedBandPyFunc =
    "import numpy as np\n" +
      "def mean_bands(in_ar, out_ar, xoff, yoff, xsize, ysize,\n" +
      "               raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n" +
      "    stack = np.array(in_ar, dtype='float64')\n" +
      "    out_ar[:] = stack.mean(axis=0)\n"
  // Small clip polygon (1000-unit box at the origin). A global-cover polygon
  // (±2e7) is NOT safe here: rst_clip falls back to the raster CRS when the
  // cutline carries no SRID, and gdalwarp -crop_to_cutline against a ±2e7-metre
  // cutline on a projected (metre) tile expands the output grid to ~1e8 px/side,
  // a native GDAL allocation abort that kills the JVM. A small finite box keeps
  // the timing-only call cheap and crash-free on every CRS (out-of-extent clips
  // are fine for timing). Same WKT as the WKB the pyrx side builds via
  // shapely.geometry.box(-500, -500, 500, 500).
  private val clipGeomWkt =
    "POLYGON ((-500 -500, 500 -500, 500 500, -500 500, -500 -500))"

  // Synthetic gdaldem color ramp for the timing-only color_relief call; written to
  // a temp file on first use (matches spec.py's _COLOR_RAMP_TEXT).
  private lazy val colorTablePath: String = {
    val p = Files.createTempFile("bench_color_", ".txt")
    Files.write(p, "nv 0 0 0\n0% 0 0 255\n50% 0 255 0\n100% 255 0 0\n".getBytes("UTF-8"))
    p.toString
  }

  private val cats: Map[String, String] = Map(
    "rst_width" -> ACC, "rst_height" -> ACC, "rst_numbands" -> ACC, "rst_avg" -> ACC,
    "rst_min" -> ACC, "rst_max" -> ACC, "rst_median" -> ACC, "rst_pixelcount" -> ACC,
    "rst_slope" -> TER, "rst_aspect" -> TER, "rst_hillshade" -> TER,
    "rst_tri" -> TER, "rst_tpi" -> TER, "rst_roughness" -> TER,
    "rst_ndvi" -> BM, "rst_ndwi" -> BM, "rst_nbr" -> BM,
    "rst_transform" -> WARP, "rst_to_webmercator" -> WARP,
    // scalar accessors (Task 2)
    "rst_srid" -> ACC, "rst_pixelwidth" -> ACC, "rst_pixelheight" -> ACC,
    "rst_upperleftx" -> ACC, "rst_upperlefty" -> ACC, "rst_scalex" -> ACC,
    "rst_scaley" -> ACC, "rst_skewx" -> ACC, "rst_skewy" -> ACC,
    "rst_rotation" -> ACC, "rst_isempty" -> ACC, "rst_getnodata" -> ACC,
    "rst_format" -> ACC, "rst_type" -> ACC, "rst_memsize" -> ACC,
    // coordinate / index accessors (Task 3)
    "rst_rastertoworldcoord" -> ACC, "rst_rastertoworldcoordx" -> ACC,
    "rst_rastertoworldcoordy" -> ACC, "rst_worldtorastercoord" -> ACC,
    "rst_worldtorastercoordx" -> ACC, "rst_worldtorastercoordy" -> ACC,
    "rst_tilexyz" -> ACC,
    // map / struct accessors (Task 4): timing-only
    "rst_metadata" -> ACC, "rst_bandmetadata" -> ACC, "rst_georeference" -> ACC,
    "rst_boundingbox" -> ACC, "rst_summary" -> ACC, "rst_histogram" -> ACC,
    // tile-out transforms with scalar / fixed args (Task 5)
    "rst_band" -> EDIT, "rst_threshold" -> EDIT, "rst_initnodata" -> EDIT,
    "rst_setsrid" -> EDIT, "rst_updatetype" -> EDIT, "rst_fillnodata" -> FEAT,
    "rst_filter" -> FOCAL, "rst_convolve" -> FOCAL,
    "rst_asformat" -> FMT, "rst_cog_convert" -> FMT,
    "rst_resample" -> RES, "rst_resample_to_res" -> RES, "rst_resample_to_size" -> RES,
    // tile-out transforms with geometry / expression / band-map / function args (Task 6)
    "rst_evi" -> BM, "rst_savi" -> BM, "rst_index" -> BM,
    "rst_mapalgebra" -> FMT, "rst_derivedband" -> FMT, "rst_proximity" -> ANALYSIS,
    "rst_clip" -> EDIT, "rst_color_relief" -> TER, "rst_viewshed" -> ANALYSIS,
    "rst_sample" -> FMT,
    // bucket C, group C1 (readers + buildoverviews) + C2 (subdataset fns).
    // tryopen / subdatasets / getsubdataset are accessor-flavored; the readers
    // and buildoverviews emit a tile -> format.
    "rst_tryopen" -> ACC, "rst_fromcontent" -> FMT,
    "rst_buildoverviews" -> FMT,
    "rst_subdatasets" -> ACC, "rst_getsubdataset" -> ACC,
    // bucket C, group C3: multi-tile-input fns (consume an ARRAY of tiles) -> format.
    "rst_frombands" -> FMT, "rst_combineavg" -> FMT, "rst_merge" -> FMT,
    // bucket C, group C4: tiling fns -> a COLLECTION of tiles -> format.
    "rst_maketiles" -> FMT, "rst_retile" -> FMT, "rst_tooverlappingtiles" -> FMT,
    "rst_separatebands" -> FMT, "rst_xyzpyramid" -> FMT,
    // bucket B, group B-grid: DGGS fns -> a set of grid cells (dggs_grid fp).
    "rst_h3_tessellate" -> DGGS,
    "rst_h3_rastertogridavg" -> DGGS, "rst_h3_rastertogridcount" -> DGGS,
    "rst_h3_rastertogridmax" -> DGGS, "rst_h3_rastertogridmedian" -> DGGS,
    "rst_h3_rastertogridmin" -> DGGS,
    "rst_quadbin_rastertogridavg" -> DGGS, "rst_quadbin_rastertogridcount" -> DGGS,
    "rst_quadbin_rastertogridmax" -> DGGS, "rst_quadbin_rastertogridmedian" -> DGGS,
    "rst_quadbin_rastertogridmin" -> DGGS,
    // bucket B, group B-vec: vector-out fns (contour LINES, polygonize POLYGONS)
    "rst_contour" -> VECTOR, "rst_polygonize" -> VECTOR,
    // bucket D: geometry-in constructors (burn/interpolate a geometry set into a
    // new raster). Categorized vector (they bridge vector geometry -> raster).
    "rst_rasterize" -> VECTOR, "rst_gridfrompoints" -> VECTOR,
    "rst_dtmfromgeoms" -> VECTOR,
    // bucket A: the 7 *_agg aggregators (Spark groupBy aggregate harness). The 4
    // tile aggregators reduce a group of tiles -> one tile (format); the 3 geometry
    // aggregators reduce a group of (geom, value) rows -> one tile (vector bridge).
    "rst_combineavg_agg" -> FMT, "rst_merge_agg" -> FMT,
    "rst_frombands_agg" -> FMT, "rst_derivedband_agg" -> FMT,
    "rst_rasterize_agg" -> VECTOR, "rst_gridfrompoints_agg" -> VECTOR,
    "rst_dtmfromgeoms_agg" -> VECTOR,
    // rst_h3_rasterize_agg: a GRID aggregator (cellid,value rows -> one tile,
    // pixel-centroid burn). dggs, like the other H3 fns.
    "rst_h3_rasterize_agg" -> DGGS
  )

  // input_kind adapter (mirrors FnSpec.input_kind): what the heavy dispatch is
  // fed for a function. "tile" (default) is an open Dataset; "bytes" is the raw
  // raster bytes (the dispatch opens them via vsimem); "path" is the corpus file
  // path (the dispatch opens it). Reader/constructor fns take content/path, not
  // an open ds, so they cannot use the shared ds-in pureCore path.
  private val byteInput: Set[String] = Set("rst_tryopen", "rst_fromcontent")
  // rst_fromfile is lightweight-only now (the JVM cannot read UC Volumes, issue #34),
  // so there is no heavy-tier dispatch for it -- the "path" input_kind is unused.
  private val pathInput: Set[String] = Set.empty
  // bucket C, group C3: multi-tile fns consume an ARRAY of tiles. The bench
  // synthesizes the multi-tile input from the corpus tile and writes it ONCE
  // (write-once-read-both); the heavy runner reads the SAME synthesized files.
  private val tileArrayInput: Set[String] = Set("rst_frombands", "rst_combineavg", "rst_merge")
  // bucket D: geometry-in fns are handed the open tile PLUS the tile's
  // GeometrySet (boxes/points/zpoints WKB, in the tile CRS) read from
  // geometry.json -- the SAME bytes the pyrx tier reads (write-once-read-both).
  private val geometryInput: Set[String] =
    Set("rst_rasterize", "rst_gridfrompoints", "rst_dtmfromgeoms")
  // bucket A: the 7 *_agg aggregators reduce a GROUP of rows to ONE tile via a real
  // df.groupBy(key).agg(...). The 4 tile aggregators build their fixed consistency
  // group from synthesized tiles (write-once-read-both); the 3 geometry aggregators
  // build it from the per-tile GeometrySet (geometry.json). Both are spark-path-only
  // (no single-row pure-core analogue of a UDAF) -- the heavy runner handles them in
  // a dedicated aggregate branch, not the column/pure-core paths.
  private val tileAggregate: Set[String] =
    Set("rst_combineavg_agg", "rst_merge_agg", "rst_frombands_agg", "rst_derivedband_agg")
  private val geometryAggregate: Set[String] =
    Set("rst_rasterize_agg", "rst_gridfrompoints_agg", "rst_dtmfromgeoms_agg")
  // rst_h3_rasterize_agg reduces a GROUP of (cellid LONG, value DOUBLE) rows to
  // ONE tile, burning each H3 cell's centroid pixel onto an EXPLICIT, hardcoded
  // grid. Its fixed consistency group is a deterministic H3 cell set generated
  // here (NOT from synth tiles or geometry.json), mirroring the pyrx tier.
  private val h3Aggregate: Set[String] = Set("rst_h3_rasterize_agg")
  def inputKind(fn: String): String =
    if (byteInput.contains(fn)) "bytes"
    else if (pathInput.contains(fn)) "path"
    else if (tileArrayInput.contains(fn)) "tile_array"
    else if (geometryInput.contains(fn)) "geometry"
    else if (tileAggregate.contains(fn)) "tile_aggregate"
    else if (geometryAggregate.contains(fn)) "geometry_aggregate"
    else if (h3Aggregate.contains(fn)) "h3_aggregate"
    else "tile"

  // --- rst_h3_rasterize_agg: fixed cell set + EXPLICIT grid (PARITY CONTRACT) ---
  // These MUST stay byte-for-byte in sync with the Python bench/spec.py block
  // (search "rst_h3_rasterize_agg: fixed deterministic cell set"):
  //   1. CELL SET RECIPE: the res-9 H3 cell at (lat, lng) = (40.7128, -74.0060)
  //      grid-disk'd (kRing) to k=10 -> 331 cells. The Scala com.uber.h3core lib
  //      and the Python h3 lib share the H3 C spec, so geoToH3 / latlng_to_cell
  //      + kRing / grid_disk yield the IDENTICAL cell-id set (order-independent).
  //   2. EXPLICIT GRID: hardcoded below (== compute_gridspec(cells, 4326,
  //      pixel_size=0.0025, kring_pad=1)). Both tiers receive these exact grid
  //      constants, so the burn lands on the identical canvas (masks compare).
  // If the recipe changes, recompute the grid (run cellraster.compute_gridspec)
  // and update BOTH this block and spec.py.
  private val h3RaggRes = 9
  private val h3RaggCenterLat = 40.7128
  private val h3RaggCenterLng = -74.0060
  private val h3RaggK = 10
  private val h3RaggSrid = 4326
  private val h3RaggPixelSize = 0.0025
  private val h3RaggKringPad = 1
  private val h3RaggMode = "centroids"
  private val h3RaggXmin = -74.055
  private val h3RaggYmin = 40.6825
  private val h3RaggXmax = -73.9575
  private val h3RaggYmax = 40.7425
  private val h3RaggWidth = 39
  private val h3RaggHeight = 24
  // Cross-tier sanity-check count (grid_disk(center, k=10)); asserted in tests.
  val h3RaggNCells: Int = 331

  /** The fixed deterministic H3 cell-id set the bench rasterizes (both tiers).
    * The res-9 NYC cell, kRing'd to k=10. com.uber.h3core returns signed Longs
    * (the unsigned 64-bit ids wrap negative past 2^63), which is exactly what the
    * Spark LONG column carries -- identical to the pyrx tier's signed ids. */
  def h3RasterizeCells(): Seq[Long] = {
    val center = H3.pointToCellID(h3RaggCenterLng, h3RaggCenterLat, h3RaggRes)
    H3.kRing(center, h3RaggK).toSeq.sorted
  }

  // bench.synth recipe name for a tile_array fn (mirrors spec.synth_recipe).
  def synthRecipe(fn: String): String = fn match {
    case "rst_frombands"  => "frombands"
    case "rst_combineavg" => "combineavg"
    case "rst_merge"      => "merge"
    case other            => throw new IllegalArgumentException(s"no synth recipe for: $other")
  }

  // bench.synth recipe whose tiles form a tile aggregator's fixed consistency group
  // (mirrors spec.agg_synth_recipe). combineavg over aligned copies, merge over
  // offset copies, frombands/derivedband over the per-band split.
  def aggSynthRecipe(fn: String): String = fn match {
    case "rst_combineavg_agg" => "combineavg"
    case "rst_merge_agg"      => "merge"
    case "rst_frombands_agg"  => "frombands"
    case "rst_derivedband_agg" => "frombands"
    case other => throw new IllegalArgumentException(s"no agg synth recipe for: $other")
  }

  def all: Seq[String] = cats.keys.toSeq.sorted
  def category(fn: String): String = cats(fn)
  def minBands(fn: String): Int = if (cats(fn) == BM || multiBand.contains(fn)) 2 else 1

  // Fixed 3x3 normalised mean kernel for rst_convolve — must match the
  // _CONVOLVE_KERNEL hardcoded on the pyrx side (1/9 in every cell).
  private val convolveKernel: Array[Array[Double]] =
    Array.fill(3, 3)(1.0 / 9.0)

  def pureCore(fn: String, ds: Dataset, a: Map[String, String]): String = {
    // gdal_calc/warp-backed functions write to NodeFilePathUtil.rootPath, a per-JVM scratch dir
    // that the Spark file-lock copy path normally mkdirs. Pure-core opens tiles directly via
    // gdal.Open and bypasses that path, so ensure the dir exists or gdal_calc fails with
    // "No such file or directory" and returns a null dataset.
    Files.createDirectories(NodeFilePathUtil.rootPath)
    fn match {
    case "rst_width"      => BenchFingerprint.ofScalar(RST_Width.execute(ds))
    case "rst_height"     => BenchFingerprint.ofScalar(RST_Height.execute(ds))
    case "rst_numbands"   => BenchFingerprint.ofScalar(RST_NumBands.execute(ds))
    case "rst_avg"        => BenchFingerprint.ofArray(RST_Avg.execute(ds))
    case "rst_min"        => BenchFingerprint.ofArray(RST_Min.execute(ds))
    case "rst_max"        => BenchFingerprint.ofArray(RST_Max.execute(ds))
    case "rst_median"     => BenchFingerprint.ofArray(RST_Median.execute(ds, Map.empty))
    case "rst_pixelcount" => BenchFingerprint.ofArray(RST_PixelCount.execute(ds).map(_.toDouble))
    case "rst_slope"      => fpDerived(RST_Slope.execute(ds, argS(a, "unit", "degrees"), argD(a, "scale", Double.NaN)))
    case "rst_aspect"     => fpDerived(RST_Aspect.execute(ds, argB(a, "trigonometric", false), argB(a, "zero_for_flat", false)))
    case "rst_hillshade"  => fpDerived(RST_Hillshade.execute(ds, argD(a, "azimuth", 315.0), argD(a, "altitude", 45.0), argD(a, "z_factor", 1.0)))
    case "rst_tri"        => fpDerived(RST_TRI.execute(ds))
    case "rst_tpi"        => fpDerived(RST_TPI.execute(ds))
    case "rst_roughness"  => fpDerived(RST_Roughness.execute(ds))
    case "rst_ndvi"       => fpDerived(RST_NDVI.execute(ds, argI(a, "red_band", 1), argI(a, "nir_band", 2), Map.empty))
    case "rst_ndwi"       => fpDerived(RST_NDWI.execute(ds, argI(a, "green_idx", 1), argI(a, "nir_idx", 2)))
    case "rst_nbr"        => fpDerived(RST_NBR.execute(ds, argI(a, "nir_idx", 1), argI(a, "swir_idx", 2)))
    case "rst_transform"  => fpDerived(RST_Transform.execute(ds, Map.empty, argI(a, "target_srid", 3857)))
    case "rst_to_webmercator" => fpDerived(RST_ToWebMercator.execute(ds, Map.empty, argS(a, "resampling", "bilinear")))
    // scalar accessors (Task 2)
    case "rst_srid"        => BenchFingerprint.ofScalar(RST_SRID.execute(ds))
    case "rst_pixelwidth"  => BenchFingerprint.ofScalar(RST_PixelWidth.execute(ds))
    case "rst_pixelheight" => BenchFingerprint.ofScalar(RST_PixelHeight.execute(ds))
    case "rst_upperleftx"  => BenchFingerprint.ofScalar(RST_UpperLeftX.execute(ds))
    case "rst_upperlefty"  => BenchFingerprint.ofScalar(RST_UpperLeftY.execute(ds))
    case "rst_scalex"      => BenchFingerprint.ofScalar(RST_ScaleX.execute(ds))
    case "rst_scaley"      => BenchFingerprint.ofScalar(RST_ScaleY.execute(ds))
    case "rst_skewx"       => BenchFingerprint.ofScalar(RST_SkewX.execute(ds))
    case "rst_skewy"       => BenchFingerprint.ofScalar(RST_SkewY.execute(ds))
    case "rst_rotation"    => BenchFingerprint.ofScalar(RST_Rotation.execute(ds))
    // bool -> 1.0/0.0 to match the pyrx core_fn's numeric coercion
    case "rst_isempty"     => BenchFingerprint.ofScalar(if (RST_IsEmpty.execute(ds)) 1.0 else 0.0)
    case "rst_getnodata"   => BenchFingerprint.ofArray(RST_GetNoData.execute(ds))
    case "rst_format"      => BenchFingerprint.ofScalar(RST_Format.execute(ds))
    // timing-only (per-band string array; no cross-engine fingerprint)
    case "rst_type"        => { RST_Type.execute(ds); BenchFingerprint.empty }
    // timing-only (file size vs in-memory size; not comparable)
    case "rst_memsize"     => { RST_MemSize.execute(ds); BenchFingerprint.empty }
    // coordinate / index accessors (Task 3).
    // raster->world: forward affine; X = pair._1, Y = pair._2; pair as scalar_list.
    case "rst_rastertoworldcoordx" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordX.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._1)
    case "rst_rastertoworldcoordy" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordY.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._2)
    case "rst_rastertoworldcoord" =>
      val p = RST_RasterToWorldCoord.execute(ds, argI(a, "x", 64), argI(a, "y", 64))
      BenchFingerprint.ofArray(Array(p._1, p._2))
    // world->raster: timing-only (CRS-dependent inverse-affine index; not comparable).
    case "rst_worldtorastercoordx" =>
      RST_WorldToRasterCoordX.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    case "rst_worldtorastercoordy" =>
      RST_WorldToRasterCoordY.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    case "rst_worldtorastercoord" =>
      RST_WorldToRasterCoord.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    // rst_tilexyz: timing-only (render/encode-dependent bytes; not comparable).
    case "rst_tilexyz" =>
      RST_TileXYZ.execute(ds, Map.empty, argI(a, "z", 12), argI(a, "x", 1205),
        argI(a, "y", 1539), argS(a, "format", "PNG"), argI(a, "size", 256), argS(a, "resampling", "bilinear"))
      BenchFingerprint.empty
    // map / struct accessors (Task 4): timing-only (maps, structs, CRS/JSON bytes).
    case "rst_metadata"      => { RST_MetaData.execute(ds); BenchFingerprint.empty }
    case "rst_bandmetadata"  => { RST_BandMetaData.execute(ds.GetRasterBand(1)); BenchFingerprint.empty }
    case "rst_georeference"  => { RST_GeoReference.execute(ds); BenchFingerprint.empty }
    case "rst_boundingbox"   => { RST_BoundingBox.execute(ds); BenchFingerprint.empty }
    case "rst_summary"       => { RST_Summary.execute(ds); BenchFingerprint.empty }
    case "rst_histogram"     => { RST_Histogram.execute(ds, 256, None, None, false); BenchFingerprint.empty }
    // tile-out transforms with scalar / fixed args (Task 5): all produce a raster
    // tile -> fpDerived raster fingerprint (same path as terrain).
    case "rst_band"        => fpDerived(RST_Band.execute(ds, Map.empty, argI(a, "band_index", 2)))
    case "rst_threshold"   => fpDerived(RST_Threshold.execute(ds, argS(a, "op", ">"), argD(a, "value", 0.5)))
    case "rst_initnodata"  => fpDerived(RST_InitNoData.execute(ds, Map.empty))
    case "rst_setsrid"     => fpDerived(RST_SetSrid.execute(ds, Map.empty, argI(a, "srid", 4326)))
    case "rst_updatetype"  => fpDerived(RST_UpdateType.execute(ds, Map.empty, argS(a, "new_type", "Float64")))
    case "rst_fillnodata"  =>
      fpDerived(RST_FillNodata.execute(ds, Map.empty, argD(a, "max_search_dist", 10.0), argI(a, "smoothing_iter", 0)))
    // KernelFilter accepts {avg, min, max, median, mode} -- NOT "mean" (which
    // throws "Invalid operation"). "median" is valid on BOTH engines (the light
    // focal.filt also accepts it), so the bench args carry "median".
    case "rst_filter"      => fpDerived(RST_Filter.execute(ds, argI(a, "kernel_size", 3), argS(a, "operation", "median")))
    case "rst_convolve"    => fpDerived(RST_Convolve.execute((0L, ds, Map.empty), convolveKernel))
    // RST_AsFormat is a no-op when newFormat == the input driver: execute returns
    // the *input* ds unchanged. fpDerived would then release that input ds, and
    // HeavyRunner reuses the same open ds across the warmup+measured loop, so the
    // next iteration calls GetDriver() on a freed dataset -> "GetDriver() null".
    // Guard the release: only release the result when it is a genuinely new
    // dataset (identity != input ds). The GTiff round-trip still times correctly
    // when newFormat differs; the no-op case times the cheap identity return.
    case "rst_asformat"    => fpAsFormat(RST_AsFormat.execute(ds, Map.empty, argS(a, "new_format", "GTiff")), ds)
    case "rst_cog_convert" =>
      fpDerived(RST_CogConvert.execute(ds, Map.empty, argS(a, "compression", "DEFLATE"),
        argI(a, "blocksize", 512), argS(a, "overview_resampling", "AVERAGE")))
    case "rst_resample" =>
      fpDerived(RST_Resample.execute(ds, Map.empty, argD(a, "factor", 2.0), argS(a, "algorithm", "bilinear")))
    case "rst_resample_to_size" =>
      fpDerived(RST_ResampleToSize.execute(ds, Map.empty, argI(a, "width_px", 128),
        argI(a, "height_px", 128), argS(a, "algorithm", "bilinear")))
    // rst_resample_to_res: pure-core-only, fingerprint suppressed (a single fixed
    // ground resolution is not sane across the multi-CRS corpus).
    case "rst_resample_to_res" =>
      val res = RST_ResampleToRes.execute(ds, Map.empty, argD(a, "x_res", 5.0),
        argD(a, "y_res", 5.0), argS(a, "algorithm", "bilinear"))
      RasterDriver.releaseDataset(res._1)
      BenchFingerprint.empty
    // tile-out transforms with geometry / expression / band-map / function args
    // (Task 6). SIX are full raster comparisons (same algorithm + CRS-independent
    // args); FOUR are timing-only (no in-extent geometry across the multi-CRS
    // corpus, and/or divergent interpolation/scan engines).
    case "rst_evi" =>
      fpDerived(RST_EVI.execute(ds, argI(a, "red_idx", 1), argI(a, "nir_idx", 2),
        argI(a, "blue_idx", 1), argD(a, "l", 1.0), argD(a, "c1", 6.0),
        argD(a, "c2", 7.5), argD(a, "g", 2.5)))
    case "rst_savi" =>
      fpDerived(RST_SAVI.execute(ds, argI(a, "red_idx", 1), argI(a, "nir_idx", 2), argD(a, "l", 0.5)))
    case "rst_index" =>
      fpDerived(RST_Index.execute(ds, indexName, indexBandMap))
    case "rst_mapalgebra" =>
      fpDerived(RST_MapAlgebra.execute(Seq(ds), Map.empty, mapAlgebraExpr))
    case "rst_derivedband" =>
      fpDerived(RST_DerivedBand.execute(Seq(ds), Map.empty, derivedBandPyFunc, derivedBandFuncName))
    case "rst_proximity" =>
      fpDerived(RST_Proximity.execute(ds, Map.empty,
        Some(argS(a, "target_values", "1")), argS(a, "distunits", "GEO"), None))
    // timing-only: clip needs an in-extent geom. A fixed box is out-of-extent on
    // some CRS (e.g. UTM), so derive the cutline per-tile from the dataset's
    // geotransform + size (bounds box shrunk 50% about the tile center, in the
    // tile's own CRS). Output is not compared; just must run on every tile.
    case "rst_clip" =>
      val res = RST_Clip.execute(ds, Map.empty, JTS.fromWKT(shrunkBoundsBoxWkt(ds)),
        argB(a, "cutline_all_touched", false))
      RasterDriver.releaseDataset(res._1)
      BenchFingerprint.empty
    // timing-only: color-relief reads a color table (synthetic) and the GDAL
    // DEMProcessing interpolation diverges from the pyrx np.interp path.
    case "rst_color_relief" =>
      val res = RST_ColorRelief.execute(ds, colorTablePath)
      RasterDriver.releaseDataset(res._1)
      BenchFingerprint.empty
    // timing-only: observer must be in-extent. A fixed (0,0) is outside the
    // UTM/3857 tile ranges, so derive the observer from the tile center (the
    // dataset geotransform + size), in the tile's own CRS. Output is not
    // compared (also an xrspatial-vs-GDAL parity divergence on the light side).
    case "rst_viewshed" =>
      val (ox, oy) = tileCenterXY(ds)
      val res = RST_Viewshed.execute(ds, Map.empty, ox, oy,
        argD(a, "observer_height", 2.0), argD(a, "target_height", 1.6), None)
      RasterDriver.releaseDataset(res._1)
      BenchFingerprint.empty
    // timing-only: sample at world (0,0) (no in-extent point across CRSs).
    case "rst_sample" =>
      RST_Sample.execute(ds, 0.0, 0.0); BenchFingerprint.empty
    // bucket C, group C1: rst_buildoverviews (tile-in). Internal overviews leave
    // the base band unchanged, so the raster fingerprint (full-resolution band)
    // is identical pre/post -> a full comparison.
    case "rst_buildoverviews" =>
      fpDerived(RST_BuildOverviews.execute(ds, Map.empty,
        argIntArray(a, "levels", Array(2, 4)), argS(a, "resampling", "average")))
    // bucket C, group C2: subdataset fns (timing-only). A plain GTiff corpus tile
    // has no subdatasets -> rst_subdatasets returns an empty map (timed, not
    // compared).
    case "rst_subdatasets" =>
      RST_Subdatasets.execute(ds); BenchFingerprint.empty
    // rst_getsubdataset on a plain GTiff: gdal.Open("GTiff:path:0") returns null
    // (no such subdataset). Guard the null so the timing call does not NPE; the
    // fingerprint is suppressed regardless. Mirrors the pyrx core swallowing its
    // "no subdataset" raise for a clean timing-only row.
    case "rst_getsubdataset" =>
      val sub = RST_GetSubdataset.execute(ds, argS(a, "name", "0"))
      if (sub != null) RasterDriver.releaseDataset(sub)
      BenchFingerprint.empty
    // bucket C, group C4: tiling fns -> a COLLECTION of tiles. Each reduces ONE
    // input tile to MANY output tiles; the output is fingerprinted as a
    // raster_collection (tile count + pooled, order-independent agg). The tiling
    // *Iter operations UNLINK the input ds when exhausted, but HeavyRunner reuses
    // the same open ds across its warmup+measured loop, so we feed each iterator a
    // CLONE (it destroys the clone, not the shared input). Each output tile is
    // released after pooling. xyzpyramid uses RST_TileXYZ.execute, which does NOT
    // close the source, so it runs directly on the shared ds.
    case "rst_maketiles" =>
      collectAndFingerprint(
        BalancedSubdivision.splitRasterIter(cloneDs(ds), Map.empty, argI(a, "size_in_mb", 1)))
    case "rst_retile" =>
      collectAndFingerprint(
        ReTile.reTileIter(cloneDs(ds), Map.empty, argI(a, "tile_width", 128), argI(a, "tile_height", 128)))
    // overlap default 25 mirrors the authoritative pyrx FnSpec (bench/spec.py:
    // rst_tooverlappingtiles args {overlap: 25}). The heavy runner passes an empty
    // args map, so this default IS the value compared cross-engine; a mismatched
    // default (was 32) produced different window positions -> a ~3% pooled-pixel
    // divergence even though the tile COUNT matched.
    case "rst_tooverlappingtiles" =>
      collectAndFingerprint(
        OverlappingTiles.reTileIter(cloneDs(ds), Map.empty,
          argI(a, "tile_width", 128), argI(a, "tile_height", 128), argI(a, "overlap", 25)))
    case "rst_separatebands" =>
      collectAndFingerprint(SeparateBands.separateIter(cloneDs(ds), Map.empty))
    case "rst_xyzpyramid" =>
      fpXyzPyramid(ds, argI(a, "min_z", 10), argI(a, "max_z", 11),
        argS(a, "format", "PNG"), argI(a, "size", 256), argS(a, "resampling", "bilinear"))
    // bucket B, group B-grid: DGGS fns. raster->grid emits one (cellId, measure)
    // set per band (Array[Array[(Long, T)]]) -> ofDggsGrid (count + sorted-id hash
    // + agg over the measures). Count returns an integral measure (H3 Int /
    // Quadbin Long), widened to Double so the agg matches the light float stats.
    // H3/quadbin cell ids are signed Longs here and parity-comparable with light.
    case "rst_h3_rastertogridavg" =>
      BenchFingerprint.ofDggsGrid(RST_H3_RasterToGridAvg.execute(ds, argI(a, "resolution", 7)).toSeq)
    case "rst_h3_rastertogridcount" =>
      BenchFingerprint.ofDggsGrid(
        RST_H3_RasterToGridCount.execute(ds, argI(a, "resolution", 7))
          .map(_.map { case (c, m) => (c, m.toDouble) }).toSeq)
    case "rst_h3_rastertogridmax" =>
      BenchFingerprint.ofDggsGrid(RST_H3_RasterToGridMax.execute(ds, argI(a, "resolution", 7)).toSeq)
    case "rst_h3_rastertogridmedian" =>
      BenchFingerprint.ofDggsGrid(RST_H3_RasterToGridMedian.execute(ds, argI(a, "resolution", 7)).toSeq)
    case "rst_h3_rastertogridmin" =>
      BenchFingerprint.ofDggsGrid(RST_H3_RasterToGridMin.execute(ds, argI(a, "resolution", 7)).toSeq)
    case "rst_quadbin_rastertogridavg" =>
      BenchFingerprint.ofDggsGrid(RST_Quadbin_RasterToGridAvg.execute(ds, argI(a, "resolution", 15)).toSeq)
    case "rst_quadbin_rastertogridcount" =>
      BenchFingerprint.ofDggsGrid(
        RST_Quadbin_RasterToGridCount.execute(ds, argI(a, "resolution", 15))
          .map(_.map { case (c, m) => (c, m.toDouble) }).toSeq)
    case "rst_quadbin_rastertogridmax" =>
      BenchFingerprint.ofDggsGrid(RST_Quadbin_RasterToGridMax.execute(ds, argI(a, "resolution", 15)).toSeq)
    case "rst_quadbin_rastertogridmedian" =>
      BenchFingerprint.ofDggsGrid(RST_Quadbin_RasterToGridMedian.execute(ds, argI(a, "resolution", 15)).toSeq)
    case "rst_quadbin_rastertogridmin" =>
      BenchFingerprint.ofDggsGrid(RST_Quadbin_RasterToGridMin.execute(ds, argI(a, "resolution", 15)).toSeq)
    // tessellate: one tile per overlapping H3 cell, NO scalar measure. Drain the
    // iterator (a CLONE of the shared input ds, since the iterator closes its
    // source on exhaustion), collect the cell ids, RELEASE each output tile, and
    // fingerprint the id-only dggs_grid (count + hash, empty agg) -- mirroring the
    // light tessellate path that emits agg == {}.
    case "rst_h3_tessellate" =>
      val iter = RasterTessellate.tessellateH3Iter(cloneDs(ds), Map.empty, argI(a, "resolution", 7))
      val ids = scala.collection.mutable.ArrayBuffer.empty[Long]
      iter.foreach { case (cell, resDs, _) =>
        ids += cell
        if (resDs != null) RasterDriver.releaseDataset(resDs)
      }
      BenchFingerprint.ofDggsGridIds(ids.toSeq)
    // bucket B, group B-vec: vector-out fns. RST_Contour / RST_Polygonize each
    // return an ArrayData of struct(geom_wkb BINARY, value DOUBLE); fpVector
    // decodes the WKB -> JTS and fingerprints (feature count + total measure
    // [line length for contour lines, polygon area for polygonize] + agg over
    // the per-feature value). The heavy runner passes an EMPTY args map, so the
    // defaults here ARE the cross-engine-compared values and MUST match the pyrx
    // spec: contour FIXED_LEVELS [0.2, 0.4, 0.6, 0.8] (interval/base unused with
    // non-empty levels), attr_field "elev"; polygonize band 1, connectedness 4.
    case "rst_contour" =>
      fpVector(RST_Contour.execute(
        ds, argDoubleArray(a, "levels", Array(0.2, 0.4, 0.6, 0.8)),
        argD(a, "interval", 0.0), argD(a, "base", 0.0), argS(a, "attr_field", "elev")))
    case "rst_polygonize" =>
      fpVector(RST_Polygonize.execute(
        ds, argI(a, "band", 1), argI(a, "connectedness", 4)))
    case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }

  /** Decode a vector-feature ArrayData (struct(geom_wkb BINARY, value DOUBLE))
    * into (JTS Geometry, attr) pairs and fingerprint via ofVector (feature count
    * + total measure [line length / polygon area] + agg over the attrs). The
    * WKB -> JTS parse mirrors the light side's shapely.wkb.loads. */
  private def fpVector(arr: ArrayData): String = {
    val features = (0 until arr.numElements()).map { i =>
      val row = arr.getStruct(i, 2)
      val wkb = row.getBinary(0)
      val v = row.getDouble(1)
      (JTS.fromWKB(wkb), v)
    }
    BenchFingerprint.ofVector(features)
  }

  /** Clone an open Dataset into an in-memory (MEM driver) copy. The tiling
    * iterators unlink their source ds on exhaustion; feed them a clone so the
    * HeavyRunner's reused input ds survives the warmup+measured loop. */
  private def cloneDs(ds: Dataset): Dataset =
    org.gdal.gdal.gdal.GetDriverByName("MEM").CreateCopy("", ds)

  /** Drain a (Dataset, metadata) tiling iterator into a raster_collection
    * fingerprint, pooling pixels across all output tiles, then release each
    * output tile. The iterator owns (and destroys) its CLONED source ds. */
  private def collectAndFingerprint(iter: Iterator[(Dataset, Map[String, String])]): String = {
    val tiles = iter.map(_._1).filter(_ != null).toSeq
    try BenchFingerprint.ofCollection(tiles)
    finally tiles.foreach(RasterDriver.releaseDataset)
  }

  /** Heavy xyzpyramid pure-core: enumerate intersecting (z, x, y) tiles across the
    * zoom range (WGS84 extent -> TileMath), render each via RST_TileXYZ.execute
    * (PNG/JPEG/WEBP bytes), open each render into a dataset, and fingerprint the
    * collection. RST_TileXYZ does not close the source, so `ds` stays valid. */
  private def fpXyzPyramid(ds: Dataset, minZ: Int, maxZ: Int,
      format: String, size: Int, resampling: String): String = {
    val env = BoundingBox.bbox(ds, GDAL.WSG84).getEnvelopeInternal
    val (lonMin, lonMax, latMin, latMax) = (env.getMinX, env.getMaxX, env.getMinY, env.getMaxY)
    val rendered = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var z = minZ
    while (z <= maxZ) {
      val tiles = TileMath.intersectingTiles(lonMin, latMin, lonMax, latMax, z)
      var i = 0
      while (i < tiles.length) {
        val (zz, xx, yy) = tiles(i)
        rendered += RST_TileXYZ.execute(ds, Map.empty, zz, xx, yy, format, size, resampling)
        i += 1
      }
      z += 1
    }
    val opened = rendered.map(b => RasterDriver.readFromBytes(b, Map.empty)).filter(_ != null).toSeq
    try BenchFingerprint.ofCollection(opened)
    finally opened.foreach(RasterDriver.releaseDataset)
  }

  /** Pure-core dispatch for `input_kind == "bytes"` reader/constructor fns: the
    * raw raster bytes are handed in instead of an open Dataset (the dispatch
    * opens them via vsimem). Mirrors the pyrx bytes adapter. */
  def pureCoreBytes(fn: String, bytes: Array[Byte], a: Map[String, String]): String = {
    Files.createDirectories(NodeFilePathUtil.rootPath)
    fn match {
      // rst_tryopen: "do the bytes open?" The Spark expression has no ds-in
      // execute (it succeeds iff the row deserializes), so replicate its work:
      // open + release, mapping success/failure to 1.0/0.0 to match the heavy
      // scalar fingerprint convention (same as rst_isempty).
      case "rst_tryopen" =>
        val ok =
          try {
            val d = RasterDriver.readFromBytes(bytes, Map.empty)
            val opened = d != null
            if (opened) RasterDriver.releaseDataset(d)
            opened
          } catch { case _: Throwable => false }
        BenchFingerprint.ofScalar(if (ok) 1.0 else 0.0)
      // rst_fromcontent: build a tile from bytes + driver. The comparable output
      // is the decoded raster grid, so open the bytes and fingerprint the
      // dataset (the pyrx side returns the same GTiff bytes -> raster fp).
      case "rst_fromcontent" =>
        val d = RasterDriver.readFromBytes(bytes, Map.empty)
        try BenchFingerprint.ofDataset(d)
        finally RasterDriver.releaseDataset(d)
      case other => throw new IllegalArgumentException(s"unknown bench bytes fn: $other")
    }
  }

  /** Pure-core dispatch for `input_kind == "path"` reader fns: the corpus tile's
    * file path is handed in instead of an open Dataset (the dispatch opens it).
    * Mirrors the pyrx path adapter. */
  def pureCorePath(fn: String, path: String, a: Map[String, String]): String = {
    Files.createDirectories(NodeFilePathUtil.rootPath)
    fn match {
      // rst_fromfile was the only "path" reader; it is lightweight-only now (issue #34),
      // so the heavy bench no longer dispatches any path-input fn.
      case other => throw new IllegalArgumentException(s"unknown bench path fn: $other")
    }
  }

  /** Pure-core dispatch for `input_kind == "tile_array"` multi-tile fns: a LIST of
    * open Datasets (the synthesized multi-tile input the bench wrote once and BOTH
    * engines read) is handed in. Each fn reduces the array to one raster tile, so
    * the output is fingerprinted as a dataset (full comparison). Mirrors the pyrx
    * tile_array adapter (core_fn(ds_list, args)). */
  def pureCoreTileArray(fn: String, dss: Array[Dataset], a: Map[String, String]): String = {
    Files.createDirectories(NodeFilePathUtil.rootPath)
    // Wrap each input ds as a (cellID, ds, metadata) tile; cellID 0, empty meta.
    val tiles: Seq[(Long, Dataset, Map[String, String])] =
      dss.toSeq.map(d => (0L, d, Map.empty[String, String]))
    fn match {
      // frombands: stack the N single-band tiles (array/band order preserved) into
      // one N-band tile. Fingerprint the stacked dataset.
      case "rst_frombands" =>
        val (out, _) = RST_FromBands.execute(tiles)
        try BenchFingerprint.ofDataset(out) finally RasterDriver.releaseDataset(out)
      // combineavg: NoData-aware per-pixel mean across the aligned copies.
      case "rst_combineavg" =>
        val (_, out, _) = RST_CombineAvg.execute(tiles)
        try BenchFingerprint.ofDataset(out) finally RasterDriver.releaseDataset(out)
      // merge: mosaic the offset-origin copies into their union extent.
      case "rst_merge" =>
        val (out, _) = RST_Merge.execute(dss, Map.empty[String, String])
        try BenchFingerprint.ofDataset(out) finally RasterDriver.releaseDataset(out)
      case other => throw new IllegalArgumentException(s"unknown bench tile_array fn: $other")
    }
  }

  // The b64-encoded empty ExpressionConfig RST_Rasterize.execute needs (it only
  // calls RST_ExpressionUtil.init; an empty config suffices, same as the
  // RST_RasterizeTest direct-execute helper). Built once.
  private lazy val encodedEmptyConf: UTF8String = {
    import com.databricks.labs.gbx.expressions.ExpressionConfig
    import org.apache.hadoop.conf.Configuration
    import org.apache.spark.util.SerializableConfiguration
    val cfg = new ExpressionConfig(Map.empty[String, String],
      new SerializableConfiguration(new Configuration()))
    val baos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(baos)
    oos.writeObject(cfg); oos.close()
    UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
  }

  // (xmin, ymin, xmax, ymax, widthPx, heightPx, srid) from an open Dataset --
  // the heavy analogue of the pyrx _tile_extent_size_srid(ds). Extent is the
  // dataset bounds (geotransform + size, rotation terms 0 for the corpus); srid
  // via RST_SRID.execute. The geometry-in constructors burn/interpolate into a
  // NEW raster at the SAME extent/size/srid as the tile the geometry was derived
  // from, so the heavy + light grids are pixel-comparable on every CRS.
  private def tileExtentSizeSrid(ds: Dataset): (Double, Double, Double, Double, Int, Int, Int) = {
    val gt = ds.GetGeoTransform()
    val w = ds.GetRasterXSize(); val h = ds.GetRasterYSize()
    val x0 = gt(0); val y0 = gt(3)
    val x1 = gt(0) + w * gt(1); val y1 = gt(3) + h * gt(5)
    val (xmin, xmax) = (math.min(x0, x1), math.max(x0, x1))
    val (ymin, ymax) = (math.min(y0, y1), math.max(y0, y1))
    (xmin, ymin, xmax, ymax, w, h, RST_SRID.execute(ds))
  }

  /** Pure-core dispatch for `input_kind == "geometry"` constructor fns: the open
    * tile PLUS the tile's GeometrySet (boxes/points/zpoints WKB, in the tile CRS,
    * read from geometry.json -- the SAME bytes the pyrx tier reads). Each fn burns
    * / interpolates the geometry into a NEW raster at the tile's own extent/size/
    * srid, so the heavy + light output grids are pixel-comparable. The expressions
    * return a tile InternalRow (cellid, raster bytes, metadata); we open the bytes
    * and fingerprint the dataset (same raster fp the pyrx GTiff bytes yield). */
  def pureCoreGeometry(fn: String, ds: Dataset, a: Map[String, String],
                       geom: GeometrySet): String = {
    Files.createDirectories(NodeFilePathUtil.rootPath)
    val (xmin, ymin, xmax, ymax, w, h, srid) = tileExtentSizeSrid(ds)
    val row: org.apache.spark.sql.catalyst.InternalRow = fn match {
      // SINGLE geometry: burn the FIRST corpus box (wkb, value) -- mirrors the
      // pyrx core_fn (features.rasterize_geom on g.boxes[0]).
      case "rst_rasterize" =>
        val (wkb, value) = geom.boxPairs.head
        RST_Rasterize.execute(wkb, value, xmin, ymin, xmax, ymax, w, h, srid, encodedEmptyConf)
      // ARRAY of points: IDW grid over all corpus points (wkb, value pairs).
      // max_pts default mirrors the pyrx FnSpec sentinel (>= any corpus point
      // count). gdal_grid `invdist` with no search radius ignores max_points and
      // uses ALL points; the lightweight idw_grid does a nearest-max_pts cKDTree
      // selection, so the bench feeds a large max_pts to force the light tier to
      // ALSO use all points -> the two tiers IDW over the identical point set.
      case "rst_gridfrompoints" =>
        RST_GridFromPoints.execute(geom.pointPairs, xmin, ymin, xmax, ymax, w, h, srid,
          argD(a, "power", 2.0), argI(a, "max_pts", 1000000))
      // ARRAY of 3D points: Delaunay DTM over all corpus zpoints; breaklines empty,
      // tolerances 0.0 (no scipy analogue on the light side either).
      case "rst_dtmfromgeoms" =>
        val pts: Seq[Geometry] = geom.zpointWkbs.map(JTS.fromWKB)
        RST_DTMFromGeoms.execute(pts, Seq.empty[LineString], 0.0, 0.0,
          xmin, ymin, xmax, ymax, w, h, srid, argD(a, "no_data", -9999.0))
      case other => throw new IllegalArgumentException(s"unknown bench geometry fn: $other")
    }
    if (row == null) return BenchFingerprint.empty
    val bytes = row.getBinary(1)
    val out = RasterDriver.readFromBytes(bytes, Map.empty)
    try BenchFingerprint.ofDataset(out)
    finally RasterDriver.releaseDataset(out)
  }

  private def fpDerived(res: (Dataset, Map[String, String])): String = {
    val out = res._1
    try BenchFingerprint.ofDataset(out)
    finally RasterDriver.releaseDataset(out)
  }

  // Fingerprint an AsFormat result without releasing the *input* ds. When
  // newFormat == the input driver, RST_AsFormat returns the input ds unchanged;
  // HeavyRunner reuses that open ds across its warmup+measured loop, so releasing
  // it here would invalidate the next iteration (GetDriver() on a freed dataset).
  // Only release a genuinely new result (identity != input).
  private def fpAsFormat(res: (Dataset, Map[String, String]), input: Dataset): String = {
    val out = res._1
    try BenchFingerprint.ofDataset(out)
    finally if (!(out eq input)) RasterDriver.releaseDataset(out)
  }

  // Tile center (x, y) in the dataset's own CRS, from the geotransform + size.
  // GT = (originX, pixelW, rowRotation, originY, colRotation, pixelH); pixelH<0
  // for north-up rasters. Center = origin + (size/2) along each axis.
  private def tileCenterXY(ds: Dataset): (Double, Double) = {
    val gt = ds.GetGeoTransform()
    val w = ds.GetRasterXSize().toDouble
    val h = ds.GetRasterYSize().toDouble
    val cx = gt(0) + (w / 2.0) * gt(1) + (h / 2.0) * gt(2)
    val cy = gt(3) + (w / 2.0) * gt(4) + (h / 2.0) * gt(5)
    (cx, cy)
  }

  // WKT of the tile's bounds box shrunk 50% about its center, in the tile's CRS.
  // Guarantees an in-extent cutline for the timing-only rst_clip on every CRS.
  private def shrunkBoundsBoxWkt(ds: Dataset): String = {
    val gt = ds.GetGeoTransform()
    val w = ds.GetRasterXSize().toDouble
    val h = ds.GetRasterYSize().toDouble
    // Two opposite corners (ignoring rotation terms, which are 0 for the corpus).
    val x0 = gt(0); val y0 = gt(3)
    val x1 = gt(0) + w * gt(1); val y1 = gt(3) + h * gt(5)
    val (cx, cy) = ((x0 + x1) / 2.0, (y0 + y1) / 2.0)
    val hw = math.abs(x1 - x0) / 4.0
    val hh = math.abs(y1 - y0) / 4.0
    val (minx, maxx) = (cx - hw, cx + hw)
    val (miny, maxy) = (cy - hh, cy + hh)
    s"POLYGON (($minx $miny, $maxx $miny, $maxx $maxy, $minx $maxy, $minx $miny))"
  }

  def column(fn: String, tile: Column, a: Map[String, String]): Column = {
    import functions._
    fn match {
      case "rst_width"      => rst_width(tile)
      case "rst_height"     => rst_height(tile)
      case "rst_numbands"   => rst_numbands(tile)
      case "rst_avg"        => rst_avg(tile)
      case "rst_min"        => rst_min(tile)
      case "rst_max"        => rst_max(tile)
      case "rst_median"     => rst_median(tile)
      case "rst_pixelcount" => rst_pixelcount(tile)
      case "rst_slope"      => rst_slope(tile, argS(a, "unit", "degrees"))
      case "rst_aspect"     => rst_aspect(tile)
      case "rst_hillshade"  => rst_hillshade(tile)
      case "rst_tri"        => rst_tri(tile)
      case "rst_tpi"        => rst_tpi(tile)
      case "rst_roughness"  => rst_roughness(tile)
      case "rst_ndvi"       => rst_ndvi(tile, argI(a, "red_band", 1), argI(a, "nir_band", 2))
      case "rst_ndwi"       => rst_ndwi(tile, argI(a, "green_idx", 1), argI(a, "nir_idx", 2))
      case "rst_nbr"        => rst_nbr(tile, argI(a, "nir_idx", 1), argI(a, "swir_idx", 2))
      case "rst_transform"  => rst_transform(tile, argI(a, "target_srid", 3857))
      case "rst_to_webmercator" => rst_to_webmercator(tile, argS(a, "resampling", "bilinear"))
      // scalar accessors (Task 2)
      case "rst_srid"        => rst_srid(tile)
      case "rst_pixelwidth"  => rst_pixelwidth(tile)
      case "rst_pixelheight" => rst_pixelheight(tile)
      case "rst_upperleftx"  => rst_upperleftx(tile)
      case "rst_upperlefty"  => rst_upperlefty(tile)
      case "rst_scalex"      => rst_scalex(tile)
      case "rst_scaley"      => rst_scaley(tile)
      case "rst_skewx"       => rst_skewx(tile)
      case "rst_skewy"       => rst_skewy(tile)
      case "rst_rotation"    => rst_rotation(tile)
      case "rst_isempty"     => rst_isempty(tile)
      case "rst_getnodata"   => rst_getnodata(tile)
      case "rst_format"      => rst_format(tile)
      case "rst_type"        => rst_type(tile)
      case "rst_memsize"     => rst_memsize(tile)
      // coordinate / index accessors (Task 3)
      case "rst_rastertoworldcoordx" => rst_rastertoworldcoordx(tile, argI(a, "x", 64), argI(a, "y", 64))
      case "rst_rastertoworldcoordy" => rst_rastertoworldcoordy(tile, argI(a, "x", 64), argI(a, "y", 64))
      case "rst_rastertoworldcoord"  => rst_rastertoworldcoord(tile, argI(a, "x", 64), argI(a, "y", 64))
      // world->raster + tilexyz are pure-core-only; the column form is here only
      // to keep the match exhaustive (the spark-path runner filters by modes).
      case "rst_worldtorastercoordx" => rst_worldtorastercoordx(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_worldtorastercoordy" => rst_worldtorastercoordy(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_worldtorastercoord"  => rst_worldtorastercoord(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_tilexyz"             => rst_tilexyz(tile, argI(a, "z", 12), argI(a, "x", 1205), argI(a, "y", 1539))
      // map / struct accessors (Task 4) are pure-core-only; the column form is here
      // only to keep the match exhaustive (the spark-path runner filters by modes).
      case "rst_metadata"      => rst_metadata(tile)
      case "rst_bandmetadata"  => rst_bandmetadata(tile, 1)
      case "rst_georeference"  => rst_georeference(tile)
      case "rst_boundingbox"   => rst_boundingbox(tile)
      case "rst_summary"       => rst_summary(tile)
      case "rst_histogram"     => rst_histogram(tile)
      // tile-out transforms with scalar / fixed args (Task 5)
      case "rst_band"        => rst_band(tile, argI(a, "band_index", 2))
      case "rst_threshold"   => rst_threshold(tile, argS(a, "op", ">"), argD(a, "value", 0.5))
      case "rst_initnodata"  => rst_initnodata(tile)
      case "rst_setsrid"     => rst_setsrid(tile, argI(a, "srid", 4326))
      case "rst_updatetype"  => rst_updatetype(tile, argS(a, "new_type", "Float64"))
      case "rst_fillnodata"  =>
        rst_fillnodata(tile, argD(a, "max_search_dist", 10.0), argI(a, "smoothing_iter", 0))
      case "rst_filter"      => rst_filter(tile, argI(a, "kernel_size", 3), argS(a, "operation", "median"))
      case "rst_convolve"    =>
        import org.apache.spark.sql.functions.{array, lit}
        val kernelCol = array(convolveKernel.map(row => array(row.map(lit): _*)): _*)
        rst_convolve(tile, kernelCol)
      case "rst_asformat"    => rst_asformat(tile, argS(a, "new_format", "GTiff"))
      case "rst_cog_convert" =>
        rst_cog_convert(tile, argS(a, "compression", "DEFLATE"),
          argI(a, "blocksize", 512), argS(a, "overview_resampling", "AVERAGE"))
      case "rst_resample" =>
        rst_resample(tile, argD(a, "factor", 2.0), argS(a, "algorithm", "bilinear"))
      case "rst_resample_to_size" =>
        rst_resample_to_size(tile, argI(a, "width_px", 128),
          argI(a, "height_px", 128), argS(a, "algorithm", "bilinear"))
      // rst_resample_to_res is pure-core-only; the column form is here only to keep
      // the match exhaustive (the spark-path runner filters by modes).
      case "rst_resample_to_res" =>
        rst_resample_to_res(tile, argD(a, "x_res", 5.0),
          argD(a, "y_res", 5.0), argS(a, "algorithm", "bilinear"))
      // tile-out transforms with geometry / expression / band-map / function args
      // (Task 6). The full-comparison six run in spark-path; the timing-only four
      // are pure-core-only and their column form is here only to keep the match
      // exhaustive (the spark-path runner filters by modes).
      case "rst_evi" =>
        rst_evi(tile, argI(a, "red_idx", 1), argI(a, "nir_idx", 2), argI(a, "blue_idx", 1),
          argD(a, "l", 1.0), argD(a, "c1", 6.0), argD(a, "c2", 7.5), argD(a, "g", 2.5))
      case "rst_savi" =>
        rst_savi(tile, argI(a, "red_idx", 1), argI(a, "nir_idx", 2), argD(a, "l", 0.5))
      case "rst_index" =>
        import org.apache.spark.sql.functions.{lit, map => sqlMap}
        val bandMapCol = sqlMap(indexBandMap.toSeq.flatMap { case (k, v) => Seq(lit(k), lit(v)) }: _*)
        rst_index(tile, indexName, bandMapCol)
      case "rst_mapalgebra" =>
        import org.apache.spark.sql.functions.array
        rst_mapalgebra(array(tile), mapAlgebraExpr)
      case "rst_derivedband" =>
        rst_derivedband(tile, derivedBandPyFunc, derivedBandFuncName)
      case "rst_proximity" =>
        import org.apache.spark.sql.functions.lit
        rst_proximity(tile, lit(argS(a, "target_values", "1")), lit(argS(a, "distunits", "GEO")))
      case "rst_clip" =>
        import org.apache.spark.sql.functions.lit
        rst_clip(tile, lit(clipGeomWkt), argB(a, "cutline_all_touched", false))
      case "rst_color_relief" =>
        rst_color_relief(tile, colorTablePath)
      case "rst_viewshed" =>
        import org.apache.spark.sql.functions.lit
        rst_viewshed(tile, lit("POINT (0 0)"), lit(argD(a, "observer_height", 2.0)),
          lit(argD(a, "target_height", 1.6)))
      case "rst_sample" =>
        import org.apache.spark.sql.functions.lit
        rst_sample(tile, lit("POINT (0 0)"))
      // bucket C, group C1/C2. tryopen takes the tile; fromcontent reads the
      // tile's raster (binary content) column; buildoverviews takes the tile +
      // an ARRAY<INT> levels literal. subdatasets / getsubdataset are
      // pure-core-only here; their column form exists only to keep the match
      // exhaustive (the spark-path runner filters by modes). rst_fromfile is
      // lightweight-only now (issue #34) and is not dispatched on the heavy tier.
      case "rst_tryopen"     => rst_tryopen(tile)
      case "rst_fromcontent" =>
        rst_fromcontent(tile.getField("raster"), argS(a, "driver", "GTiff"))
      case "rst_buildoverviews" =>
        rst_buildoverviews(tile, argIntArray(a, "levels", Array(2, 4)), argS(a, "resampling", "average"))
      case "rst_subdatasets"   => rst_subdatasets(tile)
      case "rst_getsubdataset" => rst_getsubdataset(tile, argS(a, "name", "0"))
      // bucket C, group C3: multi-tile fns. The `tile` Column passed here IS the
      // ARRAY<tile> column the runner built from the synthesized tiles (the same
      // files the pure-core path reads). Each binding takes a single array column.
      case "rst_frombands"  => rst_frombands(tile)
      case "rst_combineavg" => rst_combineavg(tile)
      case "rst_merge"      => rst_merge(tile)
      // bucket C, group C4: tiling fns -> ARRAY column (spark-path is timing-only,
      // not fingerprint-compared, so the args only need to be valid). The Scala
      // rst_maketiles wrapper takes (tileWidth, tileHeight) rather than the SQL
      // sizeInMB form, so the column path passes tile dimensions; the pure-core
      // path (which IS compared) uses the sizeInMB BalancedSubdivision directly.
      case "rst_maketiles"   => rst_maketiles(tile, argI(a, "tile_width", 128), argI(a, "tile_height", 128))
      case "rst_retile"      => rst_retile(tile, argI(a, "tile_width", 128), argI(a, "tile_height", 128))
      case "rst_tooverlappingtiles" =>
        rst_tooverlappingtiles(tile, argI(a, "tile_width", 128), argI(a, "tile_height", 128), argI(a, "overlap", 25))
      case "rst_separatebands" => rst_separatebands(tile)
      case "rst_xyzpyramid"  => rst_xyzpyramid(tile, argI(a, "min_z", 10), argI(a, "max_z", 11))
      // bucket B, group B-grid: DGGS fns -> ARRAY<...> column (spark-path is
      // timing-only, not fingerprint-compared, so the args only need to be valid).
      // The Scala wrappers take an Int resolution overload.
      case "rst_h3_tessellate"             => rst_h3_tessellate(tile, argI(a, "resolution", 7))
      case "rst_h3_rastertogridavg"        => rst_h3_rastertogridavg(tile, argI(a, "resolution", 7))
      case "rst_h3_rastertogridcount"      => rst_h3_rastertogridcount(tile, argI(a, "resolution", 7))
      case "rst_h3_rastertogridmax"        => rst_h3_rastertogridmax(tile, argI(a, "resolution", 7))
      case "rst_h3_rastertogridmedian"     => rst_h3_rastertogridmedian(tile, argI(a, "resolution", 7))
      case "rst_h3_rastertogridmin"        => rst_h3_rastertogridmin(tile, argI(a, "resolution", 7))
      case "rst_quadbin_rastertogridavg"   => rst_quadbin_rastertogridavg(tile, argI(a, "resolution", 15))
      case "rst_quadbin_rastertogridcount" => rst_quadbin_rastertogridcount(tile, argI(a, "resolution", 15))
      case "rst_quadbin_rastertogridmax"   => rst_quadbin_rastertogridmax(tile, argI(a, "resolution", 15))
      case "rst_quadbin_rastertogridmedian" => rst_quadbin_rastertogridmedian(tile, argI(a, "resolution", 15))
      case "rst_quadbin_rastertogridmin"   => rst_quadbin_rastertogridmin(tile, argI(a, "resolution", 15))
      // bucket B, group B-vec: vector-out fns -> ARRAY<struct> column (spark-path
      // is timing-only, not fingerprint-compared, so the args only need to be
      // valid). contour rides FIXED_LEVELS [0.2, 0.4, 0.6, 0.8] as an
      // ARRAY<DOUBLE> column; polygonize rides band 1 + connectedness 4.
      case "rst_contour" =>
        val levels = argDoubleArray(a, "levels", Array(0.2, 0.4, 0.6, 0.8))
        rst_contour(tile, org.apache.spark.sql.functions.array(
          levels.map(org.apache.spark.sql.functions.lit): _*))
      case "rst_polygonize" =>
        rst_polygonize(tile, org.apache.spark.sql.functions.lit(argI(a, "band", 1)),
          org.apache.spark.sql.functions.lit(argI(a, "connectedness", 4)))
      // bucket D: geometry-in constructors are pure-core-only (the spark-path tile
      // DataFrame carries no geometry column, and they are fingerprinted via the
      // geometry adapter, not the column path). The spark-path runner filters by
      // modes, so column() is never invoked for them; guard explicitly rather than
      // synthesize a meaningless geometry Column.
      case "rst_rasterize" | "rst_gridfrompoints" | "rst_dtmfromgeoms" =>
        throw new IllegalArgumentException(
          s"$fn is geometry-in / pure-core-only; no spark-path column form")
      // bucket A aggregators have no scalar column form; they are aggregate
      // expressions handled by aggregateColumn (the spark-path runner routes them
      // through the aggregate branch, not column()).
      case f if inputKind(f).endsWith("aggregate") =>
        throw new IllegalArgumentException(
          s"$f is an aggregator; use aggregateColumn, not column")
      case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }

  // bucket A: the aggregate Column for a *_agg fn over an already-keyed DataFrame.
  // The 4 tile aggregators consume the `tile` struct column (frombands also a
  // per-row `band_index` INT -- the ascending-sort key, supplied identically to the
  // pyrx side). The 3 geometry aggregators consume (geom_wkb BINARY, value DOUBLE)
  // plus the per-group extent constants (xmin,ymin,xmax,ymax,w,h,srid). derivedband
  // rides the SAME fixed mean-bands pyfunc as the non-agg rst_derivedband. dtmfrom-
  // geoms rides breaklines=NULL + tolerances 0.0 (unconstrained Delaunay, mirrored
  // on the light side). The result is a tile struct; the runner collects it and
  // fingerprints the raster bytes via BenchFingerprint.ofDataset.
  def aggregateColumn(fn: String, df: org.apache.spark.sql.DataFrame,
                      ext: (Double, Double, Double, Double, Int, Int, Int),
                      a: Map[String, String]): Column = {
    import functions._
    import org.apache.spark.sql.functions.{col, expr, lit}
    val (xmin, ymin, xmax, ymax, w, h, srid) = ext
    fn match {
      case "rst_combineavg_agg" => rst_combineavg_agg(col("tile"))
      case "rst_merge_agg"      => rst_merge_agg(col("tile"))
      // No Scala wrapper for the 2-arg frombands_agg; call the registered UDAF.
      // band_index is the per-row ascending-sort key (0,1,... supplied by both tiers).
      case "rst_frombands_agg"  => expr("gbx_rst_frombands_agg(tile, band_index)")
      case "rst_derivedband_agg" =>
        rst_derivedband_agg(col("tile"), derivedBandPyFunc, derivedBandFuncName)
      // Geometry aggregators: the extent/size/srid are per-group constants baked in
      // as SQL literals; geom_wkb/value are the streamed per-row columns.
      case "rst_rasterize_agg" =>
        expr(s"gbx_rst_rasterize_agg(geom_wkb, value, " +
          s"$xmin, $ymin, $xmax, $ymax, $w, $h, $srid)")
      // max_pts default mirrors the pyrx FnSpec sentinel: gdal_grid invdist (no
      // radius) ignores max_points and uses ALL points, so the lightweight tier is
      // fed a large max_pts to also use all points (parity over the same point set).
      case "rst_gridfrompoints_agg" =>
        rst_gridfrompoints_agg(col("geom_wkb"), col("value"),
          lit(xmin), lit(ymin), lit(xmax), lit(ymax), lit(w), lit(h), lit(srid),
          lit(argD(a, "power", 2.0)), lit(argI(a, "max_pts", 1000000)))
      // dtmfromgeoms_agg: breaklines NULL ARRAY<BINARY>; tolerances 0.0 (unconstrained
      // Delaunay, mirrored on the light side); no_data -9999.
      case "rst_dtmfromgeoms_agg" =>
        expr(s"gbx_rst_dtmfromgeoms_agg(geom_wkb, CAST(NULL AS ARRAY<BINARY>), " +
          s"${argD(a, "merge_tolerance", 0.0)}, ${argD(a, "snap_tolerance", 0.0)}, " +
          s"$xmin, $ymin, $xmax, $ymax, $w, $h, $srid, ${argD(a, "no_data", -9999.0)})")
      // rst_h3_rasterize_agg: stream (cellid, value) per row; the EXPLICIT grid
      // (xmin..height, srid, mode, kring_pad) rides as hardcoded SQL literals --
      // the SAME constants the pyrx tier passes (PARITY CONTRACT, see h3Ragg*
      // above). value is NULL on every row -> presence-mask burn (1.0). pixel_size
      // is supplied too (unused once the explicit extent is given, but keeps the
      // 12-arg signature). The HEAVY UDAF returns a tile STRUCT directly (dataType =
      // tileDataType(BinaryType); only the lightweight SQL form returns BINARY), so --
      // exactly like rst_rasterize_agg -- it is NOT wrapped in gbx_rst_fromcontent;
      // the consistency collect reads `raster` straight off the struct to fingerprint.
      case "rst_h3_rasterize_agg" =>
        expr(
          s"gbx_rst_h3_rasterize_agg(cellid, value, $h3RaggSrid, $h3RaggPixelSize, " +
            s"$h3RaggXmin, $h3RaggYmin, $h3RaggXmax, $h3RaggYmax, " +
            s"$h3RaggWidth, $h3RaggHeight, '$h3RaggMode', $h3RaggKringPad)")
      case other => throw new IllegalArgumentException(s"not an aggregator: $other")
    }
  }
}
