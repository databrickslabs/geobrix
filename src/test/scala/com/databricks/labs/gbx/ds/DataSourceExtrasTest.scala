package com.databricks.labs.gbx.ds

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class DataSourceExtrasTest extends AnyFunSuite {

    // Create a concrete implementation for testing
    class TestDataSource extends DataSourceExtras {
        override def dsExtraMap(checkMap: Map[String, String]): Map[String, String] = {
            Map("extra_key" -> "extra_value", "computed" -> s"processed_${checkMap.size}")
        }
    }

    test("extraJavaUtilMap should merge properties with extra options") {
        val dataSource = new TestDataSource()
        
        val inputMap = Map("key1" -> "value1", "key2" -> "value2").asJava
        val resultMap = dataSource.extraJavaUtilMap(inputMap)
        
        // Should contain original properties
        resultMap.get("key1") shouldBe "value1"
        resultMap.get("key2") shouldBe "value2"
        
        // Should contain extra properties
        resultMap.get("extra_key") shouldBe "extra_value"
        resultMap.get("computed") shouldBe "processed_2"
    }

    test("extraJavaUtilMap should handle empty properties") {
        val dataSource = new TestDataSource()
        
        val emptyMap = new java.util.HashMap[String, String]()
        val resultMap = dataSource.extraJavaUtilMap(emptyMap)
        
        // Should only contain extra properties
        resultMap.get("extra_key") shouldBe "extra_value"
        resultMap.get("computed") shouldBe "processed_0"
    }

    test("extraCaseInsensitiveStringMap should merge options with extra options") {
        val dataSource = new TestDataSource()
        
        val inputOptions = new CaseInsensitiveStringMap(Map("Key1" -> "value1", "KEY2" -> "value2").asJava)
        val resultOptions = dataSource.extraCaseInsensitiveStringMap(inputOptions)
        
        // Should contain original options (case insensitive)
        resultOptions.get("key1") shouldBe "value1"
        resultOptions.get("key2") shouldBe "value2"
        
        // Should contain extra options
        resultOptions.get("extra_key") shouldBe "extra_value"
        resultOptions.get("computed") shouldBe "processed_2"
    }

    test("extraCaseInsensitiveStringMap should handle empty options") {
        val dataSource = new TestDataSource()
        
        val emptyOptions = new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
        val resultOptions = dataSource.extraCaseInsensitiveStringMap(emptyOptions)
        
        // Should only contain extra options
        resultOptions.get("extra_key") shouldBe "extra_value"
        resultOptions.get("computed") shouldBe "processed_0"
    }

    test("extraCaseInsensitiveStringMap should preserve case insensitivity") {
        val dataSource = new TestDataSource()
        
        val inputOptions = new CaseInsensitiveStringMap(Map("MixedCase" -> "value").asJava)
        val resultOptions = dataSource.extraCaseInsensitiveStringMap(inputOptions)
        
        // Should be case insensitive
        resultOptions.get("mixedcase") shouldBe "value"
        resultOptions.get("MIXEDCASE") shouldBe "value"
        resultOptions.get("MixedCase") shouldBe "value"
    }

    test("extraJavaUtilMap should allow overriding existing keys") {
        class OverridingDataSource extends DataSourceExtras {
            override def dsExtraMap(checkMap: Map[String, String]): Map[String, String] = {
                Map("key1" -> "overridden")
            }
        }
        
        val dataSource = new OverridingDataSource()
        val inputMap = Map("key1" -> "original", "key2" -> "value2").asJava
        val resultMap = dataSource.extraJavaUtilMap(inputMap)
        
        // Extra map should override original
        resultMap.get("key1") shouldBe "overridden"
        resultMap.get("key2") shouldBe "value2"
    }

}
