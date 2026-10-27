package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFileCacheProbe.{entries, withScratchDir}
import com.databricks.labs.gbx.util.{NodeCacheRetentionOff, NodeFileManager}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.StructType
import org.gdal.ogr.{DataSource, Feature, FieldDefn, ogr, ogrConstants}
import org.gdal.osr.SpatialReference
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Path

/** Which layer of a multi-layer OGR source a read opens. The fixture has two layers with different row
  * counts and different columns, so a read of the wrong layer shows in the schema, the count and the values.
  * `layerName` wins when it is given; otherwise `layerNumber` (default 0) picks the layer. Schema inference,
  * planning and the reader all follow that one rule. */
class OgrLayerSelectionTest extends PlanTest with SilentSparkSession with NodeCacheRetentionOff {

    private val roadIds = Seq(1, 2, 3)
    private val riverCodes = Seq(10, 11, 12, 13, 14)

    /** Creates `path` with `driver` holding layer 0 `roads` (3 rows: id, name) and layer 1 `rivers` (5 rows:
      * code, width, kind), then checks the driver lists them in that order. */
    private def writeTwoLayers(path: Path, driver: String): Unit = {
        GDALManager.initOgr()
        val ds = ogr.GetDriverByName(driver).CreateDataSource(path.toString)
        require(ds != null, s"$driver cannot create $path")
        try {
            val srs = new SpatialReference()
            srs.ImportFromEPSG(4326)
            addLayer(ds, srs, "roads", Seq("id" -> ogrConstants.OFTInteger, "name" -> ogrConstants.OFTString)) { (f, i) =>
                f.SetField("id", roadIds(i))
                f.SetField("name", s"road_$i")
            }(roadIds.size)
            addLayer(
              ds,
              srs,
              "rivers",
              Seq("code" -> ogrConstants.OFTInteger, "width" -> ogrConstants.OFTReal, "kind" -> ogrConstants.OFTString)
            ) { (f, i) =>
                f.SetField("code", riverCodes(i))
                f.SetField("width", 0.5 * i)
                f.SetField("kind", s"river_$i")
            }(riverCodes.size)
        } finally {
            ds.FlushCache()
            ds.delete()
        }
        val check = ogr.Open(path.toString, 0)
        try {
            (0 until check.GetLayerCount()).map(check.GetLayer(_).GetName()) shouldBe Seq("roads", "rivers")
        } finally check.delete()
    }

    private def addLayer(ds: DataSource, srs: SpatialReference, name: String, fields: Seq[(String, Int)])(
        fill: (Feature, Int) => Unit
    )(n: Int): Unit = {
        val layer = ds.CreateLayer(name, srs, ogrConstants.wkbPoint)
        fields.foreach { case (fname, ftype) => layer.CreateField(new FieldDefn(fname, ftype)) }
        val defn = layer.GetLayerDefn
        (0 until n).foreach { i =>
            val f = new Feature(defn)
            fill(f, i)
            f.SetGeometryDirectly(ogr.CreateGeometryFromWkt(s"POINT ($i 0)"))
            layer.CreateFeature(f)
            f.delete()
        }
    }

    private def withGpkg[T](body: Path => T): T = withScratchDir("gbx_ogr_layers_") { dir =>
        val gpkg = dir.resolve("two.gpkg")
        writeTwoLayers(gpkg, "GPKG")
        body(gpkg)
    }

    private def read(fmt: String, path: Path, options: (String, String)*) =
        spark.read.format(fmt).options(options.toMap).load(path.toString)

    private def readWith(fmt: String, path: Path, schema: StructType, options: (String, String)*) =
        spark.read.format(fmt).schema(schema).options(options.toMap).load(path.toString)

    private def ints(df: org.apache.spark.sql.DataFrame, column: String): Seq[Int] =
        df.select(column).collect().map(_.getInt(0)).toSeq.sorted

    /** The first `n` columns: a layer's attribute fields come first, then its geometry columns. */
    private def firstColumns(schema: StructType, n: Int): Seq[String] = schema.fieldNames.toSeq.take(n)

    private def failureOf(body: => Any): String =
        Iterator.iterate(intercept[Exception](body): Throwable)(_.getCause).takeWhile(_ != null).map(e => String.valueOf(e.getMessage)).mkString(" | ")

    // ---------------------------------------------------------------------------------------------
    // End to end: layerNumber picks the layer of the rows, not only of the schema and the counts
    // ---------------------------------------------------------------------------------------------

    test("L1 a GeoPackage read with layerNumber=1 and no layerName returns layer 1's schema and rows") {
        withGpkg { gpkg =>
            val df = read("gpkg_ogr", gpkg, "layerNumber" -> "1")
            firstColumns(df.schema, 3) shouldBe Seq("code", "width", "kind")
            df.count() shouldBe riverCodes.size.toLong
            ints(df, "code") shouldBe riverCodes
        }
    }

    test("L2 the same read with the schema passed in plans and reads layer 1 too") {
        withGpkg { gpkg =>
            val schema = read("gpkg_ogr", gpkg, "layerNumber" -> "1").schema
            val df = readWith("gpkg_ogr", gpkg, schema, "layerNumber" -> "1")
            df.count() shouldBe riverCodes.size.toLong
            ints(df, "code") shouldBe riverCodes
        }
    }

    test("L3 with no layer option, or layerNumber=0, the first layer is read") {
        withGpkg { gpkg =>
            Seq(Seq.empty[(String, String)], Seq("layerNumber" -> "0")).foreach { opts =>
                val df = read("gpkg_ogr", gpkg, opts: _*)
                firstColumns(df.schema, 2) shouldBe Seq("id", "name")
                ints(df, "id") shouldBe roadIds
            }
        }
    }

    test("L4 layerName wins over layerNumber, in either direction") {
        withGpkg { gpkg =>
            ints(read("gpkg_ogr", gpkg, "layerName" -> "roads", "layerNumber" -> "1"), "id") shouldBe roadIds
            ints(read("gpkg_ogr", gpkg, "layerName" -> "rivers", "layerNumber" -> "0"), "code") shouldBe riverCodes
        }
    }

    test("L5 a layerName is read whatever layerNumber says, even an index the file does not have") {
        withGpkg { gpkg =>
            ints(read("gpkg_ogr", gpkg, "layerName" -> "rivers", "layerNumber" -> "7"), "code") shouldBe riverCodes
        }
    }

    test("L6 a layerNumber the file does not have fails the read naming the index and the layers, inferred or not") {
        withGpkg { gpkg =>
            val schema = read("gpkg_ogr", gpkg).schema
            val inferred = failureOf(read("gpkg_ogr", gpkg, "layerNumber" -> "7"))
            val planned = failureOf(readWith("gpkg_ogr", gpkg, schema, "layerNumber" -> "7").count())
            Seq(inferred, planned).foreach { msg =>
                msg should include("7")
                msg should include("roads, rivers")
            }
            planned should include("two.gpkg")
        }
    }

    test("L7 every chunk of a layerNumber=1 read opens layer 1: five rows, each once, over three partitions") {
        withGpkg { gpkg =>
            val schema = read("gpkg_ogr", gpkg, "layerNumber" -> "1").schema
            val batch = new OGR_Batch(
              schema,
              Map("path" -> gpkg.toString, "driverName" -> "GPKG", "layerNumber" -> "1", "chunkSize" -> "2")
            )
            batch.planInputPartitions().map(_.asInstanceOf[OGR_Partition]).map(p => (p.start, p.end)).toSeq shouldBe
                Seq((0, 2), (2, 4), (4, 5))
            val df = read("gpkg_ogr", gpkg, "layerNumber" -> "1", "chunkSize" -> "2")
            df.rdd.getNumPartitions shouldBe 3
            ints(df, "code") shouldBe riverCodes
        }
    }

    test("L8 a FileGDB read with layerNumber=1 returns layer 1's schema and rows; layerName and the default agree") {
        withScratchDir("gbx_ogr_layers_") { dir =>
            val gdb = dir.resolve("two.gdb")
            writeTwoLayers(gdb, "OpenFileGDB")
            val byNumber = read("file_gdb_ogr", gdb, "layerNumber" -> "1")
            firstColumns(byNumber.schema, 3) shouldBe Seq("code", "width", "kind")
            byNumber.count() shouldBe riverCodes.size.toLong
            ints(byNumber, "code") shouldBe riverCodes
            ints(read("file_gdb_ogr", gdb, "layerName" -> "rivers"), "code") shouldBe riverCodes
            ints(read("file_gdb_ogr", gdb), "id") shouldBe roadIds
        }
    }

    // ---------------------------------------------------------------------------------------------
    // The shared rule, and the reader's use of it
    // ---------------------------------------------------------------------------------------------

    test("H1 the selection opens the layer by index or by name, a name wins, and a missing layer is null") {
        withGpkg { gpkg =>
            GDALManager.initOgr()
            val ds = ogr.Open(gpkg.toString, 0)
            try {
                OGR_LayerSelection(0, "").open(ds).GetName() shouldBe "roads"
                OGR_LayerSelection(1, "").open(ds).GetName() shouldBe "rivers"
                OGR_LayerSelection(1, "roads").open(ds).GetName() shouldBe "roads"
                OGR_LayerSelection(7, "rivers").open(ds).GetName() shouldBe "rivers"
                OGR_LayerSelection(7, "").open(ds) shouldBe null
                OGR_LayerSelection(-1, "").open(ds) shouldBe null
                OGR_LayerSelection(0, "nope").open(ds) shouldBe null
                OGR_LayerSelection(7, "").notFound(ds) shouldBe "layer '7' not found; layers: roads, rivers"
                OGR_LayerSelection(0, "nope").notFound(ds) shouldBe "layer 'nope' not found; layers: roads, rivers"
            } finally ds.delete()
        }
    }

    test("H2 the options give layerNumber 0 and no layerName by default") {
        OGR_LayerSelection.fromOptions(Map.empty) shouldBe OGR_LayerSelection(0, "")
        OGR_LayerSelection.fromOptions(Map("layerNumber" -> "2", "layerName" -> "x")) shouldBe OGR_LayerSelection(2, "x")
    }

    test("R1 a reader opens the layer its partition asks for by index, and one the file lacks fails freeing its copy") {
        withGpkg { gpkg =>
            val c = ExpressionConfig(spark)
            NodeFileManager.init(c.hConf)
            val path = gpkg.toString
            val schema = OGR_SchemaInference.inferSchemaImpl("GPKG", path, Map("layerNumber" -> "1")).get
            val before = entries()
            val reader = new OGR_Reader(OGR_Partition(path, "GPKG", "", true, schema, 0, Int.MaxValue, c, layerNumber = 1))
            val codes = Iterator.continually(reader.next()).takeWhile(identity).map(_ => reader.get().getInt(schema.fieldIndex("code"))).toList
            codes.sorted shouldBe riverCodes
            val e = intercept[IllegalArgumentException](
              new OGR_Reader(OGR_Partition(path, "GPKG", "", true, schema, 0, Int.MaxValue, c, layerNumber = 7))
            )
            e.getMessage should include("two.gpkg")
            e.getMessage should include("layer '7' not found; layers: roads, rivers")
            withClue("node-file cache entries left by the readers: ") { entries().diff(before) shouldBe empty }
        }
    }

}
