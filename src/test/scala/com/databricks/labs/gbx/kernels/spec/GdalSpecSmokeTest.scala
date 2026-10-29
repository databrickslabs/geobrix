package com.databricks.labs.gbx.kernels.spec

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.kernels.KernelGdal
import com.databricks.labs.gbx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operator.OperatorOptions
import com.databricks.labs.gbx.rasterx.util.V2Tile
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** The one smoke of the GDAL spec engine's JVM side. The golden argv is the fixture the Python suite reads too;
  * the runs need the JVM on the manifest's GDAL (the env wheel payload's), and say so when it is not. */
class GdalSpecSmokeTest extends AnyFunSuite {

    private lazy val golden: JsonNode =
        new ObjectMapper().readTree(Files.readAllBytes(Paths.get("python/geobrix/test/engine/fixtures/argv_golden.json")))

    private def values(node: JsonNode): Map[String, Any] =
        node.fields.asScala.map { e =>
            val v = e.getValue
            e.getKey -> (if (v.isNull) null else if (v.isNumber) v.asDouble else if (v.isBoolean) v.asBoolean else v.asText)
        }.toMap

    private def props(node: JsonNode): Seq[RasterProps] =
        node.elements.asScala.map(p => RasterProps(p.get(0).asInt, p.get(1).asInt, p.get(2).asInt, p.get(3).asText)).toSeq

    // First, so the engine is this JVM's first GDAL user when the suite runs alone.
    test("a JVM whose first GDAL user is the engine sees the session's PROJ grid dirs") {
        assume(!GDALManager.isEnabled, "PRECONDITION MISSING: GDAL is already initialized in this JVM; run the suite alone")
        val dir = Files.createTempDirectory("gbx-se-grids").toString
        val conf = new ExpressionConfig(Map("spark.databricks.labs.gbx.gdal.PROJ_GRID_DIRS" -> dir),
          new SerializableConfiguration(new Configuration(false)))
        val expr = GdalSpecExpr("gbx_rst_slope", Seq(
          Literal(V2Tile.row(cellid = 7L, raster = Array[Byte](1, 2, 3, 4)), V2Tile.schema),
          Literal(UTF8String.fromString("degrees"), StringType), Literal(null, DoubleType), Literal(null, DoubleType),
          Literal(UTF8String.fromString(conf.toB64), StringType)
        ))
        assert(expr.eval(null) != null) // an error tile: the bytes are not a GeoTIFF, but GDAL was initialized first
        val projData = String.valueOf(gdal.GetConfigOption("PROJ_DATA"))
        assert(projData.split(":").head == dir, s"PROJ_DATA=$projData")
    }

    test("the canonical CRS resolver never reads a file a CRS value names") {
        KernelGdal.ensureInit()
        val dir = Files.createTempDirectory("gbx-se-crs")
        val file = dir.resolve("secret.prj")
        val wgs84 = new SpatialReference()
        wgs84.ImportFromEPSG(4326)
        Files.write(file, wgs84.ExportToWkt().getBytes("UTF-8"))
        wgs84.delete()
        try {
            for (v <- Seq(file.toString, s"+proj=longlat +datum=WGS84 +nadgrids=$dir/grid.gsb +no_defs")) {
                val e = intercept[IllegalArgumentException](SpatialRefOps.resolveCrs(v))
                assert(e.getMessage.contains("Fix:"), s"$v: ${e.getMessage}")
            }
            SpatialRefOps.resolveCrs("+proj=longlat +datum=WGS84 +nadgrids=@null +no_defs").delete() // no path: allowed
        } finally {
            Files.deleteIfExists(file)
            Files.deleteIfExists(dir)
        }
    }

    test("the manifest parser is closed: section keys, out keys, and a bound argument's type") {
        val m = new ObjectMapper()
        def root(section: String, block: String): JsonNode = m.readTree(
          s"""{"gdal": $section, "functions": [{"sql_name": "f", "min_args": 1, "params": [""" +
              """{"name": "tile", "session": ["TILE"]}, {"name": "unit", "session": ["STRING"]}], """ + s""""gdal": $block}]}"""
        )
        val section = """{"pin": "3.13.3", "deny": [], "allow": {}, "raster_drivers": {"allowed": [], "refused": []}}"""
        val block = """{"alg": "raster slope", "in": {"tile": "raster"}, "out": {"kind": "raster"}, "write": "tile", """ +
            """"args": [{"arg": "unit", "from": "unit", "type": "choice", "map": {"degrees": "degree"}}]}"""
        assert(GdalSpecs.parse(root(section, block))._2.contains("f")) // the control
        val refused = Seq(
          "section key" -> root(section.stripSuffix("}") + """, "allowlist": {}}""", block),
          "out key" -> root(section, block.replace(""""kind": "raster"}""", """"kind": "raster", "dtyp": "Float32"}""")),
          "needs a type" -> root(section, block.replace(""", "type": "choice"""", ""))
        )
        for ((what, r) <- refused) {
            val e = intercept[IllegalArgumentException](GdalSpecs.parse(r))
            assert(e.getMessage.contains(what), s"$what: ${e.getMessage}")
        }
    }

    test("the level knob, the virtual-tile policy and the engine's settings are the light tier's") {
        for (c <- golden.get("zstd_knob").elements.asScala) {
            val got = OperatorOptions.engineZstdLevel(c.get("decoded_bytes").asLong, Some(c.get("knob").asText))
            assert(got == c.get("level").asInt, s"GBX_ZSTD_LEVEL=${c.get("knob")}")
        }
        // The virtual-tile policy: the allowlist, and the refused drivers pinned as a set; the JVM skips exactly
        // those, so none is registered, and a nested source cannot open one.
        val section = GdalSpecs.section
        assert(section.allowedDrivers.contains("GTiff") && section.allowedDrivers.contains("VRT"), section.allowedDrivers)
        assert(!section.allowedDrivers.exists(section.refusedDrivers.contains), "a driver in both classes")
        assert(section.refusedDrivers.toSet == GDALManager.DefaultSkippedDrivers.split(" ").toSet, section.refusedDrivers)
        KernelGdal.ensureInit()
        for (name <- section.refusedDrivers) assert(gdal.GetDriverByName(name) == null, s"$name is registered")
        // The process-level settings of the driver policy are set at the first GDAL use, on the JVM too.
        if (sys.env.get(GDALManager.PolicyEnv).forall(!_.trim.equalsIgnoreCase("off"))) {
            assert(gdal.GetConfigOption("GDAL_VRT_ENABLE_RAWRASTERBAND") == "NO", "the raw-band setting is not applied")
        }
        // The engine's settings: the golden table, which the Python suite reads too.
        val settings = golden.get("engine_settings").fields.asScala.map(e => e.getKey -> e.getValue.asText).toMap
        assert(GdalSpecEngine.EngineSettings.toMap == settings, GdalSpecEngine.EngineSettings)
    }

    test("the argv, the write profile and the refusals are the golden ones") {
        KernelGdal.ensureInit() // the CRS binder parses with OSR
        for (c <- golden.get("argv").elements.asScala) {
            val got = GdalSpecArgv.build(GdalSpecs.function(c.get("fn").asText), values(c.get("values")), props(c.get("inputs")))
            assert(got.contains(c.get("argv").elements.asScala.map(_.asText).toSeq), c.get("id").asText)
        }
        for (c <- golden.get("creation_options").elements.asScala) {
            val got = OperatorOptions.engineCreationOptions(c.get("dtype").asText, c.get("decoded_bytes").asLong,
              c.get("width").asInt, c.get("height").asInt)
            assert(got == c.get("options").elements.asScala.map(_.asText).toSeq, c.get("id").asText)
        }
        for (c <- golden.get("refused").elements.asScala) {
            val fn = GdalSpecs.function(c.get("fn").asText)
            val e = intercept[Exception](GdalSpecArgv.build(fn, values(c.get("values")), props(c.get("inputs"))))
            val want = if (c.get("error").asText == "param") classOf[SpecParamException] else classOf[SpecDataException]
            assert(want.isInstance(e) && e.getMessage.contains(c.get("names").asText) && e.getMessage.contains(fn.sqlName),
              s"${c.get("id").asText}: $e")
        }
    }

    test("the bound argv is cached per (spec, values, inputs, section), and the cache is bounded (M7)") {
        val fn = GdalSpecs.function("gbx_rst_slope")
        // a key no other row binds: an input width of 641
        val props = Seq(RasterProps(641, 480, 1, "Float32"))
        val before = GdalSpecArgv.cachedTemplates
        for (i <- 0 until 3) {
            GdalSpecArgv.build(fn, Map[String, Any]("unit" -> "degrees", "xscale" -> 2.0, "yscale" -> 0.5), props,
              Seq(s"/vsimem/r$i/in0.tif"), s"/vsimem/r$i/out.tif")
        }
        assert(GdalSpecArgv.cachedTemplates == before + 1, "three binds of one literal set cache one template")
        for (i <- 0 until 600) {
            GdalSpecArgv.build(fn, Map[String, Any]("unit" -> "degrees", "xscale" -> (i + 1).toDouble, "yscale" -> 1.0), props)
        }
        assert(GdalSpecArgv.cachedTemplates <= 512, GdalSpecArgv.cachedTemplates)
    }

    /** A 64 x 64 Float32 GeoTIFF in EPSG:3857 (a slope of `k`), through the GTiff driver on /vsimem. */
    private def dem(k: Float): Array[Byte] = {
        val name = s"/vsimem/gbx-smoke-${java.util.UUID.randomUUID()}.tif"
        val ds = gdal.GetDriverByName("GTiff").Create(name, 64, 64, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array(400000.0, 10.0, 0.0, 5760000.0, 0.0, -10.0))
        val sr = new SpatialReference()
        sr.ImportFromEPSG(3857)
        ds.SetProjection(sr.ExportToWkt())
        sr.delete()
        ds.GetRasterBand(1).SetNoDataValue(-9999.0)
        ds.GetRasterBand(1).WriteRaster(0, 0, 64, 64, Array.tabulate(64 * 64)(i => 100f + k * (i % 64) + (i / 64)))
        ds.delete()
        try gdal.GetMemFileBuffer(name) finally gdal.Unlink(name)
    }

    private def left(): Int = Option(gdal.ReadDirRecursive(GdalSpecEngine.Prefix)).map(_.size).getOrElse(0)

    test("each spec runs, leaves /vsimem/gbx-spec empty, and holds on eight threads") {
        val probe = KernelGdal.probe()
        assume(
          probe == s"ok:${GdalSpecs.section.pin}",
          s"PRECONDITION MISSING: the JVM runs $probe, not GDAL ${GdalSpecs.section.pin} (the payload's); runs not checked"
        )
        val (a, b) = (dem(0.5f), dem(2f))
        val calls = Seq(
          "gbx_rst_slope" -> (Seq(a), Map[String, Any]("unit" -> "degrees", "xscale" -> null, "yscale" -> null)),
          "gbx_rst_transformcrs" -> (Seq(a), Map[String, Any]("target_crs" -> "EPSG:4326")),
          "gbx_rst_mapalgebra" -> (Seq(a, b), Map[String, Any]("expression" -> """{"calc": "norm_diff", "dialect": "builtin"}"""))
        )
        for ((name, (rasters, vals)) <- calls) {
            val res = GdalSpecEngine.run(GdalSpecs.function(name), rasters, vals).get
            assert(res.error == null && res.raster.length > 0 && res.command.startsWith(GdalSpecs.function(name).gdal.alg), name)
        }
        assert(left() == 0)
        val pool = Executors.newFixedThreadPool(8)
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
        try {
            val slope = GdalSpecs.function("gbx_rst_slope")
            val runs = (0 until 8).map { t =>
                Future((0 until 200).count { i =>
                    val r = GdalSpecEngine.run(slope, Seq(if ((t + i) % 2 == 0) a else b), calls.head._2._2).get
                    r.error == null && r.raster != null
                })
            }
            assert(Await.result(Future.sequence(runs), Duration(10, TimeUnit.MINUTES)) == Seq.fill(8)(200))
        } finally pool.shutdown()
        assert(left() == 0)
    }

}
