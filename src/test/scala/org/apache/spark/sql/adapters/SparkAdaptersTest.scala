package org.apache.spark.sql.adapters

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class SparkAdaptersTest extends AnyFunSuite {

    test("SparkHadoopUtils should provide SparkHadoopUtil instance") {
        val sdu = SparkHadoopUtils.sdu
        
        sdu should not be null
        // Verify it's the correct type  
        sdu.getClass.getSimpleName should include("SparkHadoopUtil")
    }

    test("Column adapter should create column from function name") {
        // Just verify the Column.apply method works (doesn't require execution)
        val addColumn = Column("add", Seq(col("a"), col("b")))
        
        addColumn should not be null
        // Verify it's a Spark Column
        addColumn.getClass.getName should include("Column")
    }

}
