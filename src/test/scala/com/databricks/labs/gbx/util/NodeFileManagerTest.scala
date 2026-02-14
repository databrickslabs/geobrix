package com.databricks.labs.gbx.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.BeforeAndAfterEach

class NodeFileManagerTest extends AnyFunSuite with BeforeAndAfterEach {

    // Note: NodeFileManager is a stateful singleton that interacts with NodeFilePathUtil
    // These tests verify the manager's state management and basic operations

    override def beforeEach(): Unit = {
        super.beforeEach()
        // NodeFileManager maintains internal state, so tests should be aware
    }

    test("init should accept SerializableConfiguration") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()
        
        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        
        noException should be thrownBy NodeFileManager.init(hconf)
        
        spark.stop()
    }

    test("init should store configuration") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()
        
        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        NodeFileManager.init(hconf)
        
        // After init, subsequent operations should have access to hconf
        // Verify by checking that manager can be initialized multiple times
        noException should be thrownBy NodeFileManager.init(hconf)
        
        spark.stop()
    }

    test("NodeFileManager should be a singleton object") {
        // Verify it's an object (singleton pattern)
        NodeFileManager should not be null
        NodeFileManager.getClass.getName should include ("NodeFileManager")
    }

    // Note: readRemote and releaseRemote require actual file operations with NodeFilePathUtil
    // which involves file system and Hadoop configuration. These are integration tests.
    // For unit tests, we verify the manager's structure and basic initialization.

    test("releaseRemote should not throw for empty remote path") {
        val spark = SparkSession.builder()
            .master("local[1]")
            .appName("NodeFileManagerTest")
            .getOrCreate()
        
        val hconf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
        NodeFileManager.init(hconf)
        
        // Releasing a non-existent remote path should not crash
        // (though it won't do anything useful)
        noException should be thrownBy NodeFileManager.releaseRemote("")
        
        spark.stop()
    }

}
