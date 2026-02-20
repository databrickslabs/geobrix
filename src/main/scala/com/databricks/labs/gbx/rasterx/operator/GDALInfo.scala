package com.databricks.labs.gbx.rasterx.operator

import org.gdal.gdal.{Dataset, InfoOptions, gdal}

/** Wrapper for GDAL Info: returns metadata string for a dataset via gdal.GDALInfo. */
object GDALInfo {

    /** Runs gdal.GDALInfo(ds, InfoOptions(parseOptions(command))); returns metadata string or error message. */
    def executeInfo(ds: Dataset, command: String): String = {
        require(command.startsWith("gdalinfo"), "Not a valid GDAL Info command.")

        val infoOptionsVec = OperatorOptions.parseOptions(command)
        val infoOptions = new InfoOptions(infoOptionsVec)
        val gdalInfo = gdal.GDALInfo(ds, infoOptions)

        if (gdalInfo == null) {
            s"""
               |GDAL Info failed.
               |Command: $command
               |Error: ${gdal.GetLastErrorMsg}
               |""".stripMargin
        } else {
            gdalInfo
        }
    }

}
