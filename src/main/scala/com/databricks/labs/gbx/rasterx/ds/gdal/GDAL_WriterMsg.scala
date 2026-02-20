package com.databricks.labs.gbx.rasterx.ds.gdal

import org.apache.spark.sql.connector.write.WriterCommitMessage

/** Commit message carrying GDAL error strings from a GDAL_RowWriter (for diagnostics). */
final case class GDAL_WriterMsg(gdalErrors: Array[String]) extends WriterCommitMessage

