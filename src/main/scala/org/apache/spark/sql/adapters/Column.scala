package org.apache.spark.sql.adapters

import org.apache.spark.sql.{Column => SparkColumn}

/** Adapter for building Spark Column from function name and args (Spark 3.x compatibility). */
object Column {

    /** Builds a Column as fnName(args). */
    def apply(fnName: String, args: Seq[SparkColumn]): SparkColumn = { SparkColumn.fn(fnName, args: _*) }

}
