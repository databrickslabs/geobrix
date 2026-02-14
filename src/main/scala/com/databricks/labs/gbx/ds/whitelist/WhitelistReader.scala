package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/** PartitionReader that yields a single row (did_read = true); used for registration/side-effect scans. */
class WhitelistReader extends PartitionReader[InternalRow] {

    private var hasNext = true

    /** Overrides PartitionReader.next: true until get() is called once. */
    override def next(): Boolean = hasNext

    /** Overrides PartitionReader.get: one row with did_read = true; sets hasNext to false. */
    override def get(): InternalRow = {
        hasNext = false
        val row = new GenericInternalRow(1)
        row.setBoolean(0, value = true)
        row
    }

    /** Overrides PartitionReader.close: no-op; no resources to release. */
    override def close(): Unit = {
        // No resources to close
    }

}
