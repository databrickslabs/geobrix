package com.databricks.labs.gbx.expressions

import com.databricks.labs.gbx.vectorx.jts.legacy.expressions.ST_LegacyAsWKB
import org.apache.spark.sql.adapters.SparkHadoopUtils
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.must.Matchers.{an, be, noException, not}
import org.scalatest.matchers.should.Matchers._

import scala.language.postfixOps

class CoreClassesTest extends PlanTest with SilentSparkSession {

    // ========== ExpressionConfig Tests ==========

    test("ExpressionConfig should serialize and deserialize via Base64") {
        val config = ExpressionConfig(spark)
        val b64 = config.toB64
        
        b64 should not be null
        b64.length should be > 0
        
        val restored = ExpressionConfig.fromB64(b64)
        restored.configs should not be empty
    }

    test("ExpressionConfig should filter GDAL configs") {
        val configs = Map(
            "spark.databricks.labs.gbx.gdal.option1" -> "value1",
            "spark.gdal.option2" -> "value2",
            "spark.other.option" -> "value3"
        )
        val config = ExpressionConfig(configs, null)
        
        val gdalConfigs = config.getGDALConfig
        gdalConfigs.size shouldBe 2
        gdalConfigs should contain key "spark.databricks.labs.gbx.gdal.option1"
        gdalConfigs should contain key "spark.gdal.option2"
        gdalConfigs should not contain key("spark.other.option")
    }

    test("ExpressionConfig should filter shared object configs") {
        val configs = Map(
            "spark.databricks.labs.gbx.sharedobjects.lib1" -> "path1",
            "spark.sharedobjects.lib2" -> "path2",
            "spark.other.config" -> "value"
        )
        val config = ExpressionConfig(configs, null)
        
        val soConfigs = config.getSharedObjects
        soConfigs.size shouldBe 2
        soConfigs should contain key "spark.databricks.labs.gbx.sharedobjects.lib1"
        soConfigs should contain key "spark.sharedobjects.lib2"
    }

    test("ExpressionConfig should provide raster checkpoint directory") {
        val configs = Map("spark.databricks.labs.gbx.raster.checkpoint.dir" -> "/custom/checkpoint")
        val config = ExpressionConfig(configs, null)
        
        config.getRasterCheckpointDir shouldBe "/custom/checkpoint"
    }

    test("ExpressionConfig should use default checkpoint directory if not configured") {
        val config = ExpressionConfig(Map.empty, null)
        config.getRasterCheckpointDir shouldBe "/tmp/raster-checkpoint"
    }

    test("ExpressionConfig should parse useCheckpoint flag") {
        val configTrue = ExpressionConfig(Map("spark.databricks.labs.gbx.raster.use.checkpoint" -> "true"), null)
        configTrue.useCheckpoint shouldBe true
        
        val configFalse = ExpressionConfig(Map("spark.databricks.labs.gbx.raster.use.checkpoint" -> "false"), null)
        configFalse.useCheckpoint shouldBe false
        
        val configDefault = ExpressionConfig(Map.empty, null)
        configDefault.useCheckpoint shouldBe false
    }

    test("ExpressionConfig should parse crashExpressions flag") {
        val configTrue = ExpressionConfig(Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "true"), null)
        configTrue.crashExpressions shouldBe true
        
        val configFalse = ExpressionConfig(Map.empty, null)
        configFalse.crashExpressions shouldBe false
    }

    test("ExpressionConfig should deserialize from expression literal") {
        val config = ExpressionConfig(spark)
        val b64 = config.toB64
        val literal = Literal(UTF8String.fromString(b64), StringType)
        
        val restored = ExpressionConfig.fromExpr(literal)
        restored should not be null
        restored.configs should not be empty
    }

    // ========== WithExpressionInfo Tests ==========

    test("WithExpressionInfo should throw exception for unimplemented builder") {
        val testExpr = new WithExpressionInfo {
            override def name: String = "test"
            override def usageArgs: String = ""
            override def description: String = ""
            override def extendedUsageArgs: String = ""
            override def examples: String = ""
        }
        
        an[IllegalAccessException] should be thrownBy testExpr.builder()
    }

    // ========== RegistryDelegate Tests ==========

    test("RegistryDelegate should register function without prefix") {
        val delegate = RegistryDelegate(spark.sessionState.functionRegistry, None)
        
        // Create a test expression to avoid conflicts with pre-registered functions
        val testExpr = new WithExpressionInfo {
            override def name: String = "test_no_prefix"
            override def usageArgs: String = "arg"
            override def description: String = "Test"
            override def extendedUsageArgs: String = "arg: String"
            override def examples: String = "example"
            override def builder() = { _ => null }
        }
        
        delegate.register(testExpr)
        
        val registered = spark.sessionState.functionRegistry.lookupFunction(
            FunctionIdentifier("test_no_prefix")
        )
        registered should not be empty
    }

    test("RegistryDelegate should register function with prefix") {
        val delegate = RegistryDelegate(spark.sessionState.functionRegistry, Some("gbx"))
        
        val testExpr = new WithExpressionInfo {
            override def name: String = "test_prefixed"
            override def usageArgs: String = "arg"
            override def description: String = "Test"
            override def extendedUsageArgs: String = "arg: String"
            override def examples: String = "example"
            override def builder() = { _ => null }
        }
        
        delegate.register(testExpr)
        
        val registered = spark.sessionState.functionRegistry.lookupFunction(
            FunctionIdentifier("gbx_test_prefixed")
        )
        registered should not be empty
    }

    // ========== PrettyInvoke Tests ==========

    test("PrettyInvoke should format toString with short arguments") {
        val target = Literal.create("target", StringType)
        val args = Seq(Literal.create(1, IntegerType), Literal.create("short", StringType))
        
        val invoke = new PrettyInvoke(
            exprName = "test_func",
            targetObject = target,
            functionName = "eval",
            dataType = StringType,
            arguments = args
        )
        
        val str = invoke.toString()
        str should include("test_func")
        str should include("1")
        str should include("short")
    }

    test("PrettyInvoke should truncate long literal values in toString") {
        val target = Literal.create("target", StringType)
        val longValue = "a" * 50 // 50 character string
        val args = Seq(Literal.create(longValue, StringType))
        
        val invoke = new PrettyInvoke(
            exprName = "test_func",
            targetObject = target,
            functionName = "eval",
            dataType = StringType,
            arguments = args
        )
        
        val str = invoke.toString()
        str should include("literal(...)")
        str should not include longValue
    }

    test("PrettyInvoke should support child replacement") {
        val target = Literal.create("target", StringType)
        val args = Seq(Literal.create(1, IntegerType))
        
        val invoke = new PrettyInvoke(
            exprName = "test_func",
            targetObject = target,
            functionName = "eval",
            dataType = StringType,
            arguments = args
        )
        
        // Test via public withNewChildren method
        val newTarget = Literal.create("new_target", StringType)
        val newArgs = Seq(Literal.create(2, IntegerType))
        val newInvoke = invoke.withNewChildren(IndexedSeq(newTarget) ++ newArgs)
        
        newInvoke should not be null
    }

    // ========== ExpressionConfigExpr Tests ==========

    test("ExpressionConfigExpr should have correct properties") {
        val exprConf = ExpressionConfigExpr()
        
        exprConf.dataType shouldBe StringType
        exprConf.children shouldBe empty
        exprConf.prettyName shouldBe "expr_config_expr"
    }

    test("ExpressionConfigExpr should preserve itself when calling withNewChildrenInternal") {
        val exprConf = ExpressionConfigExpr()
        noException should be thrownBy exprConf.withNewChildren(Seq.empty)
    }

    test("ExpressionConfigExpr should generate base64 replacement literal") {
        val exprConf = ExpressionConfigExpr()
        val replacement = exprConf.replacement
        
        replacement should not be null
        replacement.dataType shouldBe StringType
    }

    // ========== InvokedExpression Tests ==========
    // Note: InvokedExpression is a trait tested through its implementations
    // (e.g., RST_* expressions). Direct instantiation requires implementing
    // RuntimeReplaceable methods which is complex for unit testing.

    // ========== Legacy Tests (preserved) ==========

    test("Core Expression Classes getters/constructors should not fail") {
        noException should be thrownBy ST_LegacyAsWKB.builder()
        noException should be thrownBy ST_LegacyAsWKB.info()
    }

    test("Should be able to get sdu instance") {
        val sdu = SparkHadoopUtils.sdu
        sdu should not be null
    }

}
