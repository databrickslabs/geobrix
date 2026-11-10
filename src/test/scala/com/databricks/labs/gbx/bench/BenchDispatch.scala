package com.databricks.labs.gbx.bench

import org.apache.spark.sql.adapters.{Column => ColumnAdapter}
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_RasterToWorldCoordY
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordX
import com.databricks.labs.gbx.rasterx.expressions.RST_WorldToRasterCoordY
import com.databricks.labs.gbx.rasterx.expressions.RST_AsFormat
import com.databricks.labs.gbx.rasterx.expressions.RST_Convolve
import com.databricks.labs.gbx.rasterx.expressions.RST_DerivedBand
import com.databricks.labs.gbx.rasterx.expressions.RST_InitNoData
import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromBands
import com.databricks.labs.gbx.rasterx.expressions.RST_UpdateType
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.gridx.grid.{BNG, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.expressions.dem._
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_Band
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_SetSrid
import com.databricks.labs.gbx.rasterx.expressions.pixel.RST_SetCrs
import com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZ
import com.databricks.labs.gbx.rasterx.expressions.grid._
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.BoundingBox
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.Column
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

import java.nio.file.Files

/** Maps a bench fn name to its pure-core call (-> fingerprint) and its Spark Column. */
object BenchDispatch {
  private def argS(a: Map[String, String], k: String, d: String) = a.getOrElse(k, d)
  private def argD(a: Map[String, String], k: String, d: Double) = a.get(k).map(_.toDouble).getOrElse(d)
  private def argI(a: Map[String, String], k: String, d: Int) = a.get(k).map(_.toInt).getOrElse(d)
  private def argB(a: Map[String, String], k: String, d: Boolean) = a.get(k).map(_.toBoolean).getOrElse(d)

  private val ACC = "accessor"; private val TER = "terrain"
  private val EDIT = "edit"
  private val FOCAL = "focal"; private val FMT = "format"
  private val DGGS = "dggs"
  private val VECTOR = "vector"

  // Functions that need a >=2-band source (rst_band selects band 2 on the 2-band corpus).
  private val multiBand: Set[String] = Set("rst_band")

  // --- complex-arg constants, hardcoded identically to the pyrx side ---
  // The derived-band pixel function cannot ride the stringly-typed bench args map,
  // so it is fixed here exactly as in spec.py (same policy as the convolve kernel).
  private val derivedBandFuncName = "mean_bands"
  private val derivedBandPyFunc =
    "import numpy as np\n" +
      "def mean_bands(in_ar, out_ar, xoff, yoff, xsize, ysize,\n" +
      "               raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n" +
      "    stack = np.array(in_ar, dtype='float64')\n" +
      "    out_ar[:] = stack.mean(axis=0)\n"

  private val cats: Map[String, String] = Map(
    "rst_width" -> ACC, "rst_height" -> ACC, "rst_numbands" -> ACC,
    "rst_tri" -> TER,
    // scalar accessors
    "rst_srid" -> ACC, "rst_crs" -> ACC, "rst_pixelwidth" -> ACC, "rst_pixelheight" -> ACC,
    "rst_upperleftx" -> ACC, "rst_upperlefty" -> ACC, "rst_scalex" -> ACC,
    "rst_scaley" -> ACC, "rst_skewx" -> ACC, "rst_skewy" -> ACC,
    "rst_rotation" -> ACC,
    "rst_format" -> ACC, "rst_memsize" -> ACC,
    // coordinate / index accessors
    "rst_rastertoworldcoordx" -> ACC,
    "rst_rastertoworldcoordy" -> ACC,
    "rst_worldtorastercoordx" -> ACC, "rst_worldtorastercoordy" -> ACC,
    "rst_tilexyz" -> ACC,
    // timing-only accessors
    "rst_boundingbox" -> ACC,
    // (the accessors and statistics that return an array, a map or a struct have no heavyweight level)
    // tile-out transforms with scalar / fixed args
    "rst_band" -> EDIT, "rst_initnodata" -> EDIT,
    "rst_setsrid" -> EDIT, "rst_setcrs" -> EDIT, "rst_updatetype" -> EDIT,
    "rst_convolve" -> FOCAL,
    "rst_asformat" -> FMT,
    // tile-out transforms with geometry / expression / band-map / function args
    "rst_derivedband" -> FMT,
    // bucket C, group C1 (readers) + C2 (subdataset fns).
    // getsubdataset is accessor-flavored; the readers emit a tile -> format.
    // (buildoverviews has no heavyweight level: it measured slower than light.)
    "rst_fromcontent" -> FMT,
    "rst_getsubdataset" -> ACC,
    // bucket C, group C3: multi-tile-input fns (consume an ARRAY of tiles) -> format.
    // (The combine family and merge have no heavyweight level: they measured slower than light.)
    "rst_frombands" -> FMT,
    // bucket C, group C4: the tiling fn with a heavyweight level -> a COLLECTION of tiles -> format.
    // (The other tiling, tessellate and isoband functions have no heavyweight level.)
    "rst_xyzpyramid" -> FMT,
    // grid rasterize aggregators (DGGS; routed through the grid-aggregate branch
    // like rst_h3_rasterize_agg -- see gridAggregate). quadbin streams LONG cell
    // ids; BNG streams STRING cell ids and burns onto a 27700-native grid.
    "rst_quadbin_rasterize_agg" -> DGGS, "rst_bng_rasterize_agg" -> DGGS,
    // bucket A: the grid *_agg aggregators (Spark groupBy aggregate harness). (The tile
    // and geometry aggregators have no heavyweight level.)
    // rst_h3_rasterize_agg: a GRID aggregator (cellid,value rows -> one tile,
    // pixel-centroid burn). dggs, like the other H3 fns.
    "rst_h3_rasterize_agg" -> DGGS,
    // gbx_<grid>_cellfill: GRID grouped aggregators that stream (cellid, value)
    // rows and return the NULL-filled cell set (heavy: ARRAY<STRUCT<cellid,value>>).
    // Routed through the grid-aggregate branch like the rasterize_aggs, but their
    // consistency fingerprint is the dggs cell-set (via ofCellFill/ofCellFillStr),
    // NOT a raster. dggs, like the other grid fns. custom_cellfill is NOT here: like
    // every custom-grid fn it needs a grid STRUCT arg the heavy dispatch can't yet
    // stream, so it is a pyrx-tier timing-only fn (fingerprint=False), not heavy-
    // dispatched -- exactly as rst_custom_rasterize_agg is absent from this map.
    "h3_cellfill" -> DGGS, "bng_cellfill" -> DGGS
  )

  // input_kind adapter (mirrors FnSpec.input_kind): what the heavy dispatch is
  // fed for a function. "tile" (default) is an open Dataset; "bytes" is the raw
  // raster bytes (the dispatch opens them via vsimem); "path" is the corpus file
  // path (the dispatch opens it). Reader/constructor fns take content/path, not
  // an open ds, so they cannot use the shared ds-in pureCore path.
  private val byteInput: Set[String] = Set("rst_fromcontent")
  // rst_fromfile is lightweight-only now (the JVM cannot read UC Volumes, issue #34),
  // so there is no heavy-tier dispatch for it -- the "path" input_kind is unused.
  private val pathInput: Set[String] = Set.empty
  // bucket C, group C3: multi-tile fns consume an ARRAY of tiles. The bench
  // synthesizes the multi-tile input from the corpus tile and writes it ONCE
  // (write-once-read-both); the heavy runner reads the SAME synthesized files.
  private val tileArrayInput: Set[String] = Set("rst_frombands")
  // bucket D: geometry-in fns are handed the open tile PLUS the tile's
  // GeometrySet (boxes/points/zpoints WKB, in the tile CRS) read from
  // geometry.json -- the SAME bytes the pyrx tier reads (write-once-read-both).
  // (No function takes this kind now: rst_gridfrompoints was the last, and it has no heavyweight level.)
  private val geometryInput: Set[String] = Set.empty
  // bucket A: the *_agg aggregators reduce a GROUP of rows to ONE result via a real
  // df.groupBy(key).agg(...). They are spark-path-only (no single-row pure-core
  // analogue of a UDAF) -- the heavy runner handles them in a dedicated aggregate
  // branch, not the column/pure-core paths. The tile and geometry aggregators have
  // no heavyweight level, so the grid aggregators below are the only kind.
  // The grid rasterize aggregators reduce a GROUP of (cellid, value) rows to ONE
  // tile, burning each grid cell's centroid pixel onto an EXPLICIT, hardcoded
  // grid. Their fixed consistency group is a deterministic cell set generated
  // here (NOT from synth tiles or geometry.json), mirroring the pyrx tier. H3 +
  // quadbin stream LONG cell ids; BNG streams STRING cell ids (OS grid refs).
  // Renamed from the H3-only h3Aggregate to hold all three grids' rasterize_aggs;
  // the branch behavior (fixed cell set -> explicit grid -> one tile) is identical.
  // The cellfill grouped aggregators also ride the grid-aggregate branch (they
  // stream (cellid, value) rows), but they RETURN the filled cell set (ARRAY<
  // STRUCT<cellid, value>>), not a burned tile -- so the HeavyRunner fingerprints
  // them via ofCellFill/ofCellFillStr (dggs cell-set), not the raster path. Their
  // input corpus REUSES the rasterize cell set but NULLs a deterministic subset of
  // the values (a fill over an all-valid set is vacuous), mirrored in the pyrx tier
  // (spec.*_cellfill_cells). BNG streams STRING cell ids; the rest stream LONG.
  // custom_cellfill is excluded (grid-struct arg not yet streamable -> pyrx-tier
  // timing-only, no heavy dispatch), matching rst_custom_rasterize_agg.
  val cellFill: Set[String] =
    Set("h3_cellfill", "bng_cellfill")
  private val gridAggregate: Set[String] =
    Set("rst_h3_rasterize_agg", "rst_quadbin_rasterize_agg", "rst_bng_rasterize_agg") ++ cellFill
  def inputKind(fn: String): String =
    if (byteInput.contains(fn)) "bytes"
    else if (pathInput.contains(fn)) "path"
    else if (tileArrayInput.contains(fn)) "tile_array"
    else if (geometryInput.contains(fn)) "geometry"
    else if (gridAggregate.contains(fn)) "grid_aggregate"
    else "tile"

  // Functions whose pure-core sweep must run on the Great-Britain-overlapping tile
  // (TileEntry.role == "bng_gb") rather than the ordinary NYC-ish sweep tiles: the
  // BNG raster->grid reducers + BNG tessellate reproject to EPSG:27700 internally
  // and drop out-of-GB pixels, so on a sweep tile they bin an EMPTY grid (a vacuous
  // both-empty cross-tier match). Mirrors the Python FnSpec.gb_tile flag exactly.
  // (rst_bng_tessellate was the last: it has no heavyweight level now.)
  private val gbTileFns: Set[String] = Set.empty
  def gbTile(fn: String): Boolean = gbTileFns.contains(fn)

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

  // --- rst_quadbin_rasterize_agg: fixed cell set (PARITY CONTRACT) ---
  // The zoom-15 quadbin cell at NYC (lat, lng) = (40.7128, -74.0060), kRing'd
  // (Chebyshev) to k=3 -> (2*3+1)^2 = 49 cells (a small deterministic set, "a few
  // cells"). Quadbin is 4326-native. The aggregator auto-derives the grid from the
  // cell set (deterministic given a fixed cell set), so the burn is reproducible
  // without a hardcoded canvas -- when the light tier adds this fn it mirrors this
  // recipe (center + zoom + k), the SAME parity anchor h3 uses for its cell set.
  private val quadbinRaggZoom = 15
  private val quadbinRaggCenterLat = 40.7128
  private val quadbinRaggCenterLng = -74.0060
  private val quadbinRaggK = 3
  private val quadbinRaggSrid = 4326
  // Cross-tier sanity-check count (kRing(center, k=3) = (2k+1)^2); asserted in tests.
  val quadbinRaggNCells: Int = (2 * quadbinRaggK + 1) * (2 * quadbinRaggK + 1)

  /** The fixed deterministic quadbin cell-id set the bench rasterizes (both tiers).
    * The zoom-15 NYC cell, kRing'd to k=3. Quadbin ids are signed Longs, exactly
    * what the Spark LONG column carries -- parity-comparable across engines. */
  def quadbinRasterizeCells(): Seq[Long] = {
    val center = Quadbin.pointToCell(quadbinRaggCenterLng, quadbinRaggCenterLat, quadbinRaggZoom)
    Quadbin.kRing(center, quadbinRaggK).toSeq.sorted
  }

  // --- rst_bng_rasterize_agg: fixed cell set (PARITY CONTRACT) ---
  // The res-3 (1km) BNG cell at British National Grid easting/northing
  // (530000, 180000) (central London -- BNG expects EPSG:27700 eastings/northings,
  // NOT WGS84 lon/lat), kRing'd to k=3 -> 49 cells (a handful of GB cells). BNG
  // cell ids are OS grid REFERENCE STRINGS (BNG.format of the internal Long id),
  // NOT Longs -- so the aggregator streams a STRING cellid column. BNG is
  // 27700-native (srid forced to 27700 by the expression); the aggregator
  // auto-derives the 27700 grid from the cell set (deterministic).
  private val bngRaggResolution = 3    // 1km (BNG resolution index 3, per BNG.resolutionMap)
  private val bngRaggEasting = 530000.0
  private val bngRaggNorthing = 180000.0
  private val bngRaggK = 3
  private val bngRaggSrid = 27700
  // Cross-tier sanity-check count (BNG.kRing(center, k=3) = (2k+1)^2); asserted in tests.
  val bngRaggNCells: Int = (2 * bngRaggK + 1) * (2 * bngRaggK + 1)

  /** The fixed deterministic BNG cell-id set the bench rasterizes (both tiers).
    * The res-3 (1km) central-London cell, kRing'd to k=3. Returns OS grid
    * REFERENCE STRINGS (BNG.format), the SAME string ids the light pygx `_bng`
    * tier emits -- so the string-keyed dggs fingerprints compare directly. */
  def bngRasterizeCells(): Seq[String] = {
    val center = BNG.pointToCellID(bngRaggEasting, bngRaggNorthing, bngRaggResolution)
    BNG.kRing(center, bngRaggK).map(BNG.format).toSeq.sorted
  }

  // --- cellfill: fixed (cellid, value) corpus (PARITY CONTRACT) ---
  // Mirrors the pyrx spec.py cellfill block byte-for-byte (search "cellfill: fixed
  // (cellid, value) corpus"). A cellfill over an ALL-VALID cell set is vacuous, so
  // the corpus REUSES each grid's rasterize cell set (a dense kRing disk -> every
  // cell has in-set neighbours) and NULLs a deterministic subset of the values:
  // cell i (in the recipe's sorted order) is NULL when i % cellFillNullStride == 0,
  // else carries value i.toDouble. NULL cells are ~1/4 of a dense disk, so each has
  // valid 1-ring neighbours and the default-k=1 fill produces real values. The sort
  // order matches the pyrx tier (signed Long for H3/quadbin, lexicographic String
  // for BNG), so the i-th (cellid, value) pair is identical across tiers.
  val cellFillNullStride: Int = 4

  /** Deterministic value column for a cellfill corpus of `n` cells: `null` (SQL
    * NULL, covered-but-missing) every `cellFillNullStride`-th cell, else `i.toDouble`.
    * Uses java.lang.Double so NULLs are representable. Mirrors spec._cellfill_values. */
  def cellFillValues(n: Int): Seq[java.lang.Double] =
    (0 until n).map(i =>
      if (i % cellFillNullStride == 0) null else java.lang.Double.valueOf(i.toDouble))

  /** Fixed cellfill cell-id list for a grid (the SAME sorted list the pyrx tier
    * zips values onto). h3_cellfill uses the H3 set; the rest the quadbin set. */
  def cellFillLongCells(fn: String): Seq[Long] =
    if (fn == "h3_cellfill") h3RasterizeCells() else quadbinRasterizeCells()

  // bench.synth recipe name for a tile_array fn (mirrors spec.synth_recipe).
  def synthRecipe(fn: String): String = fn match {
    case "rst_frombands"     => "frombands"
    case other               => throw new IllegalArgumentException(s"no synth recipe for: $other")
  }


  def all: Seq[String] = cats.keys.toSeq.sorted
  def category(fn: String): String = cats(fn)
  def minBands(fn: String): Int = if (multiBand.contains(fn)) 2 else 1

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
    case "rst_tri"        => fpDerived(RST_TRI.execute(ds))
    // scalar accessors
    case "rst_srid"        => BenchFingerprint.ofScalar(RST_SRID.execute(ds))
    // rst_crs: canonical CRS STRING (authority else WKT). ofScalar coerces the
    // String via toString -> a "scalar" fingerprint (CRS-string-comparable).
    case "rst_crs"         => BenchFingerprint.ofScalar(RST_Crs.execute(ds))
    case "rst_pixelwidth"  => BenchFingerprint.ofScalar(RST_PixelWidth.execute(ds))
    case "rst_pixelheight" => BenchFingerprint.ofScalar(RST_PixelHeight.execute(ds))
    case "rst_upperleftx"  => BenchFingerprint.ofScalar(RST_UpperLeftX.execute(ds))
    case "rst_upperlefty"  => BenchFingerprint.ofScalar(RST_UpperLeftY.execute(ds))
    case "rst_scalex"      => BenchFingerprint.ofScalar(RST_ScaleX.execute(ds))
    case "rst_scaley"      => BenchFingerprint.ofScalar(RST_ScaleY.execute(ds))
    case "rst_skewx"       => BenchFingerprint.ofScalar(RST_SkewX.execute(ds))
    case "rst_skewy"       => BenchFingerprint.ofScalar(RST_SkewY.execute(ds))
    case "rst_rotation"    => BenchFingerprint.ofScalar(RST_Rotation.execute(ds))
    case "rst_format"      => BenchFingerprint.ofScalar(RST_Format.execute(ds))
    // timing-only (per-band string array; no cross-engine fingerprint)
    // timing-only (file size vs in-memory size; not comparable)
    case "rst_memsize"     => { RST_MemSize.execute(ds); BenchFingerprint.empty }
    // coordinate / index accessors.
    // raster->world: forward affine; X = pair._1, Y = pair._2; pair as scalar_list.
    case "rst_rastertoworldcoordx" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordX.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._1)
    case "rst_rastertoworldcoordy" =>
      BenchFingerprint.ofScalar(RST_RasterToWorldCoordY.execute(ds, argI(a, "x", 64), argI(a, "y", 64))._2)
    // world->raster: timing-only (CRS-dependent inverse-affine index; not comparable).
    case "rst_worldtorastercoordx" =>
      RST_WorldToRasterCoordX.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    case "rst_worldtorastercoordy" =>
      RST_WorldToRasterCoordY.execute(ds, argD(a, "x", -73.985), argD(a, "y", 40.745)); BenchFingerprint.empty
    // rst_tilexyz: timing-only (render/encode-dependent bytes; not comparable).
    case "rst_tilexyz" =>
      RST_TileXYZ.execute(ds, Map.empty, argI(a, "z", 12), argI(a, "x", 1205),
        argI(a, "y", 1539), argS(a, "format", "PNG"), argI(a, "size", 256), argS(a, "resampling", "bilinear"))
      BenchFingerprint.empty
    // timing-only accessors (polygon, JSON bytes).
    case "rst_boundingbox"   => { RST_BoundingBox.execute(ds); BenchFingerprint.empty }
    // tile-out transforms with scalar / fixed args: all produce a raster
    // tile -> fpDerived raster fingerprint (same path as terrain).
    case "rst_band"        => fpDerived(RST_Band.execute(ds, Map.empty, argI(a, "band_index", 2)))
    case "rst_initnodata"  => fpDerived(RST_InitNoData.execute(ds, Map.empty))
    case "rst_setsrid"     => fpDerived(RST_SetSrid.execute(ds, Map.empty, argI(a, "srid", 4326)))
    // rst_setcrs: string-CRS relabel (int-cast rule); no reproject, pixels unchanged.
    case "rst_setcrs"      => fpDerived(RST_SetCrs.execute(ds, Map.empty, argS(a, "crs", "4326")))
    case "rst_updatetype"  => fpDerived(RST_UpdateType.execute(ds, Map.empty, argS(a, "new_type", "Float64")))
    case "rst_convolve"    => fpDerived(RST_Convolve.execute((0L, ds, Map.empty), convolveKernel))
    // RST_AsFormat is a no-op when newFormat == the input driver: execute returns
    // the *input* ds unchanged. fpDerived would then release that input ds, and
    // HeavyRunner reuses the same open ds across the warmup+measured loop, so the
    // next iteration calls GetDriver() on a freed dataset -> "GetDriver() null".
    // Guard the release: only release the result when it is a genuinely new
    // dataset (identity != input ds). The GTiff round-trip still times correctly
    // when newFormat differs; the no-op case times the cheap identity return.
    case "rst_asformat"    => fpAsFormat(RST_AsFormat.execute(ds, Map.empty, argS(a, "new_format", "GTiff")), ds)
    // tile-out transforms with geometry / expression / band-map / function args
    // SIX are full raster comparisons (same algorithm + CRS-independent
    // args); FOUR are timing-only (no in-extent geometry across the multi-CRS
    // corpus, and/or divergent interpolation/scan engines).
    case "rst_derivedband" =>
      fpDerived(RST_DerivedBand.execute(Seq(ds), Map.empty, derivedBandPyFunc, derivedBandFuncName))
    // bucket C, group C2: the subdataset function (timing-only).
    // rst_getsubdataset on a plain GTiff: gdal.Open("GTiff:path:0") returns null
    // (no such subdataset). Guard the null so the timing call does not NPE; the
    // fingerprint is suppressed regardless. Mirrors the pyrx core swallowing its
    // "no subdataset" raise for a clean timing-only row.
    case "rst_getsubdataset" =>
      val sub = RST_GetSubdataset.execute(ds, argS(a, "name", "0"))
      if (sub != null) RasterDriver.releaseDataset(sub)
      BenchFingerprint.empty
    // bucket C, group C4: the tiling fn -> a COLLECTION of tiles. It reduces ONE
    // input tile to MANY output tiles; the output is fingerprinted as a
    // raster_collection (tile count + pooled, order-independent agg). xyzpyramid uses
    // RST_TileXYZ.execute, which does NOT close the source, so it runs directly on
    // the shared ds.
    case "rst_xyzpyramid" =>
      fpXyzPyramid(ds, argI(a, "min_z", 10), argI(a, "max_z", 11),
        argS(a, "format", "PNG"), argI(a, "size", 256), argS(a, "resampling", "bilinear"))
    case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
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
      case other => throw new IllegalArgumentException(s"unknown bench tile_array fn: $other")
    }
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
      // no function has this input kind now (rst_gridfrompoints, the last, has no heavyweight level)
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

  def column(fn: String, tile: Column, a: Map[String, String]): Column = {
    import functions._
    fn match {
      case "rst_width"      => rst_width(tile)
      case "rst_height"     => rst_height(tile)
      case "rst_numbands"   => rst_numbands(tile)
      case "rst_tri"        => rst_tri(tile)
      // scalar accessors
      case "rst_srid"        => rst_srid(tile)
      case "rst_crs"         => rst_crs(tile)
      case "rst_pixelwidth"  => rst_pixelwidth(tile)
      case "rst_pixelheight" => rst_pixelheight(tile)
      case "rst_upperleftx"  => rst_upperleftx(tile)
      case "rst_upperlefty"  => rst_upperlefty(tile)
      case "rst_scalex"      => rst_scalex(tile)
      case "rst_scaley"      => rst_scaley(tile)
      case "rst_skewx"       => rst_skewx(tile)
      case "rst_skewy"       => rst_skewy(tile)
      case "rst_rotation"    => rst_rotation(tile)
      case "rst_format"      => rst_format(tile)
      case "rst_memsize"     => rst_memsize(tile)
      // coordinate / index accessors
      case "rst_rastertoworldcoordx" => rst_rastertoworldcoordx(tile, argI(a, "x", 64), argI(a, "y", 64))
      case "rst_rastertoworldcoordy" => rst_rastertoworldcoordy(tile, argI(a, "x", 64), argI(a, "y", 64))
      // world->raster + tilexyz are pure-core-only; the column form is here only
      // to keep the match exhaustive (the spark-path runner filters by modes).
      case "rst_worldtorastercoordx" => rst_worldtorastercoordx(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_worldtorastercoordy" => rst_worldtorastercoordy(tile, argD(a, "x", -73.985), argD(a, "y", 40.745))
      case "rst_tilexyz"             => rst_tilexyz(tile, argI(a, "z", 12), argI(a, "x", 1205), argI(a, "y", 1539))
      case "rst_boundingbox"   => rst_boundingbox(tile)
      // tile-out transforms with scalar / fixed args
      case "rst_band"        => rst_band(tile, argI(a, "band_index", 2))
      case "rst_initnodata"  => rst_initnodata(tile)
      case "rst_setsrid"     => rst_setsrid(tile, argI(a, "srid", 4326))
      case "rst_setcrs"      => rst_setcrs(tile, argS(a, "crs", "4326"))
      case "rst_updatetype"  => rst_updatetype(tile, argS(a, "new_type", "Float64"))
      case "rst_convolve"    =>
        import org.apache.spark.sql.functions.{array, lit}
        val kernelCol = array(convolveKernel.map(row => array(row.map(lit): _*)): _*)
        rst_convolve(tile, kernelCol)
      case "rst_asformat"    => rst_asformat(tile, argS(a, "new_format", "GTiff"))
      // tile-out transforms with geometry / function args
      case "rst_derivedband" =>
        rst_derivedband(tile, derivedBandPyFunc, derivedBandFuncName)
      // bucket C, group C1/C2. fromcontent reads the tile's raster (binary
      // content) column. getsubdataset is pure-core-only here; its column form
      // exists only to keep the match exhaustive (the spark-path runner filters
      // by modes). rst_fromfile is lightweight-only now (issue #34) and is not
      // dispatched on the heavy tier.
      case "rst_fromcontent" =>
        rst_fromcontent(tile.getField("raster"), argS(a, "driver", "GTiff"))
      case "rst_getsubdataset" => rst_getsubdataset(tile, argS(a, "name", "0"))
      // bucket C, group C3: multi-tile fns. The `tile` Column passed here IS the
      // ARRAY<tile> column the runner built from the synthesized tiles (the same
      // files the pure-core path reads). The binding takes a single array column.
      case "rst_frombands"     => rst_frombands(tile)
      // bucket C, group C4: the tiling fn -> ARRAY column (spark-path is timing-only,
      // not fingerprint-compared, so the args only need to be valid).
      case "rst_xyzpyramid"  => rst_xyzpyramid(tile, argI(a, "min_z", 10), argI(a, "max_z", 11))
      // bucket A aggregators have no scalar column form; they are aggregate
      // expressions handled by aggregateColumn (the spark-path runner routes them
      // through the aggregate branch, not column()).
      case f if inputKind(f).endsWith("aggregate") =>
        throw new IllegalArgumentException(
          s"$f is an aggregator; use aggregateColumn, not column")
      case other            => throw new IllegalArgumentException(s"unknown bench fn: $other")
    }
  }

  // bucket A: the aggregate Column for a grid *_agg fn over an already-keyed DataFrame.
  // The grid aggregators consume (cellid, value) rows; `ext` is unused (the grid rides
  // as literals or is derived from the cell set). The runner collects the result and
  // fingerprints it (the raster bytes, or the cell set of a cellfill).
  def aggregateColumn(fn: String, df: org.apache.spark.sql.DataFrame,
                      ext: (Double, Double, Double, Double, Int, Int, Int),
                      a: Map[String, String]): Column = {
    import functions._
    import org.apache.spark.sql.functions.{col, expr}
    fn match {
      // rst_h3_rasterize_agg: stream (cellid, value) per row; the EXPLICIT grid
      // (xmin..height, srid, mode, kring_pad) rides as hardcoded SQL literals --
      // the SAME constants the pyrx tier passes (PARITY CONTRACT, see h3Ragg*
      // above). value is NULL on every row -> presence-mask burn (1.0). pixel_size
      // is supplied too (unused once the explicit extent is given, but keeps the
      // 12-arg signature). The HEAVY UDAF returns a tile STRUCT directly (dataType =
      // tileDataType(BinaryType); only the lightweight SQL form returns BINARY), so
      // it is NOT wrapped in gbx_rst_fromcontent;
      // the consistency collect reads `raster` straight off the struct to fingerprint.
      case "rst_h3_rasterize_agg" =>
        expr(
          s"gbx_rst_h3_rasterize_agg(cellid, value, $h3RaggSrid, $h3RaggPixelSize, " +
            s"$h3RaggXmin, $h3RaggYmin, $h3RaggXmax, $h3RaggYmax, " +
            s"$h3RaggWidth, $h3RaggHeight, '$h3RaggMode', $h3RaggKringPad)")
      // rst_quadbin_rasterize_agg: stream (cellid LONG, value DOUBLE); value NULL on
      // every row -> presence-mask burn (1.0). The 2-arg overload auto-derives the
      // 4326 grid from the fixed deterministic quadbin cell set (quadbinRasterizeCells,
      // the SAME recipe the light tier will mirror), so the burn is reproducible. The
      // heavy UDAF returns a tile STRUCT directly (like rst_h3_rasterize_agg), so the
      // consistency collect reads `raster` straight off the struct.
      case "rst_quadbin_rasterize_agg" =>
        rst_quadbin_rasterize_agg(col("cellid"), col("value"))
      // rst_bng_rasterize_agg: stream (cellid STRING, value DOUBLE); value NULL on
      // every row -> presence-mask burn. cellid is an OS grid REFERENCE STRING (not a
      // Long). The 2-arg overload auto-derives the 27700-native grid from the fixed
      // deterministic BNG cell set (bngRasterizeCells). srid is forced to 27700 by the
      // expression regardless of the argument.
      case "rst_bng_rasterize_agg" =>
        rst_bng_rasterize_agg(col("cellid"), col("value"))
      // gbx_<grid>_cellfill: stream (cellid, value) rows -> the NULL-filled cell set
      // (ARRAY<STRUCT<cellid, value>>). k=1 / method='mean' / power=2.0 are passed
      // explicitly to match the light wrapper defaults (pygx.<grid>_cellfill). The
      // corpus (built by HeavyRunner from cellFillLongCells/bngRasterizeCells +
      // cellFillValues) CONTAINS NULLs so the fill does real work. The output is NOT
      // a tile -- HeavyRunner fingerprints the array via ofCellFill/ofCellFillStr.
      case "h3_cellfill" =>
        expr("gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0)")
      case "bng_cellfill" =>
        expr("gbx_bng_cellfill(cellid, value, 1, 'mean', 2.0)")
      case other => throw new IllegalArgumentException(s"not an aggregator: $other")
    }
  }
}
