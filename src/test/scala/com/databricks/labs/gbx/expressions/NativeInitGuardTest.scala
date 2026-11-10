package com.databricks.labs.gbx.expressions

import com.databricks.labs.gbx.vectorx.expressions.{ST_Crs, ST_SetCrs}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.BinaryType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Every JVM expression that calls GDAL/OGR/OSR must start GDAL in its own eval path.
  *
  * A fresh executor JVM has not loaded the GDAL JNI. An expression whose first native call runs before
  * `GDALManager.init` fails with `UnsatisfiedLinkError`, a `LinkageError` that `Try` does not catch, so the
  * task fails. The test suite's JVM has GDAL loaded by earlier suites, so a behaviour test cannot see it;
  * this guard reads the sources instead. `gbx_st_crs` and `gbx_st_setcrs` shipped without the init and
  * failed on the first call on a cluster. */
class NativeInitGuardTest extends AnyFunSuite {

    private val root = new java.io.File("src/main/scala/com/databricks/labs/gbx")

    private def sources: Seq[(String, String)] = {
        def files(f: java.io.File): Seq[java.io.File] =
            if (f.isDirectory) f.listFiles.toSeq.flatMap(files)
            else if (f.getName.endsWith(".scala")) Seq(f) else Nil
        files(root).map { f =>
            val src = scala.io.Source.fromFile(f, "UTF-8")
            f.getPath -> code(try src.mkString finally src.close())
        }
    }

    /** A Catalyst expression, generator or aggregate defined in the file: an `extends` clause (comments stripped)
      * naming one. */
    private val definesExpression =
        """\bextends\s+[^{\n]*\b\w*(Expression|Generator|Aggregate|Replaceable)\w*\b""".r

    private def code(src: String): String =
        src.replaceAll("""(?s)/\*.*?\*/""", "").replaceAll("""//[^\n]*""", "")

    /** A direct native call, or a call into a helper that does not start GDAL itself. `MvtDecoder` and
      * `MvtWriter` are left out: both call `GDALManager.initOgr()` before their first OGR call. */
    private val nativeUse =
        """org\.gdal|SpatialRefOps|RasterDriver|TileBytes|GDALRasterize|OSRTransformGeometry|RasterAccessors|BandAccessors|GDALWarp|GDALTranslate|GDALBuildVRT|GDALInfo|VectorRasterBridge|KernelGdal""".r

    private val initCall = """GDALManager\.init|RST_ExpressionUtil\.init|GDALManager\.initOgr|initGdal\(""".r

    test("every expression that calls GDAL starts GDAL in its eval path") {
        val all = sources
        assert(all.size > 100, s"scan found ${all.size} sources: path resolution broken")
        val exprFiles = all.filter { case (_, s) =>
            definesExpression.findFirstIn(s).isDefined && nativeUse.findFirstIn(s).isDefined
        }
        // the scan must see the known native expressions, or it checks nothing
        val names = exprFiles.map(_._1)
        Seq("ST_TransformCrs.scala", "ST_Crs.scala", "ST_SetCrs.scala", "RST_Width.scala").foreach { n =>
            assert(names.exists(_.endsWith("/" + n)), s"$n not seen as a native expression: the guard is blind")
        }
        val missing = exprFiles.collect { case (p, s) if initCall.findFirstIn(s).isEmpty => p }
        assert(missing.isEmpty, s"expressions call GDAL with no GDALManager.init in the file: ${missing.mkString(", ")}")
    }

    private def invokeOf(e: org.apache.spark.sql.catalyst.expressions.Expression): Invoke =
        e.asInstanceOf[InvokedExpression].replacement match {
            case i: Invoke => i
            case other     => fail(s"replacement is not an Invoke: ${other.getClass.getName}")
        }

    test("gbx_st_crs carries the GDAL config and invokes the init path") {
        val expr = ST_Crs(Literal.create(Array[Byte](1), BinaryType))
        expr.children should have size 2
        expr.children.last shouldBe a[ExpressionConfigExpr]
        expr.withNewChildren(expr.children).children.count(_.isInstanceOf[ExpressionConfigExpr]) shouldBe 1
        invokeOf(expr).functionName shouldBe "evalSqlWithConf"
    }

    test("gbx_st_setcrs carries the GDAL config and invokes the init path") {
        val expr = ST_SetCrs(Literal.create(Array[Byte](1), BinaryType), Literal("EPSG:4326"))
        expr.children should have size 3
        expr.children.last shouldBe a[ExpressionConfigExpr]
        expr.withNewChildren(expr.children).children.count(_.isInstanceOf[ExpressionConfigExpr]) shouldBe 1
        invokeOf(expr).functionName shouldBe "evalSqlWithConf"
    }
}
