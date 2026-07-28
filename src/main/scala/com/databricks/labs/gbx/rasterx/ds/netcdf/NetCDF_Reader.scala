package com.databricks.labs.gbx.rasterx.ds.netcdf

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BalancedSubdivision
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

/** Reads one netcdf_gdal partition: opens its subdataset selector, splits into tiles, yields (source, tile). */
class NetCDF_Reader(partition: NetCDF_Partition) extends PartitionReader[InternalRow] {

    RST_ExpressionUtil.init(partition.expressionConfig)

    // Stage the .nc locally if remote (subdataset selectors are not plain paths, so
    // RasterDriver's own copyToLocal cannot recognize/stage them — do it explicitly).
    private val isLocal = partition.filePath.startsWith("/") &&
        !partition.filePath.startsWith("/Volumes/") && !partition.filePath.startsWith("/dbfs/")
    private val localPath = if (isLocal) partition.filePath else NodeFileManager.readRemote(partition.filePath)
    // Empty subdatasetName is the whole-file sentinel (single-variable .nc with NO SUBDATASETS
    // domain — NetCDF_Batch emits one partition for the whole file). Open the staged file
    // directly; a NETCDF:"file": selector with no trailing variable would be malformed.
    private val wholeFile = partition.subdatasetName == null || partition.subdatasetName.trim.isEmpty
    private val selector = if (wholeFile) localPath else s"""NETCDF:"$localPath":${partition.subdatasetName}"""

    // A subdataset selector is not a filesystem path, so open it directly via gdal.Open —
    // RasterDriver.read would treat the NETCDF:"..." string as a remote path and try to stage it.
    // The whole-file case also opens directly (selector is the plain local path).
    private val ds = {
        val opened = gdal.Open(selector, GA_ReadOnly)
        if (opened == null) {
            throw new RuntimeException(s"Failed to open subdataset: $selector; Error: ${gdal.GetLastErrorMsg}")
        }
        opened
    }
    // applyScale=true: decode CF scale_factor/add_offset to physical Float64 in WindowedExtract,
    // matching the light netcdf_gbx reader (xarray mask_and_scale=True). Heavy default is OFF;
    // only netcdf_gdal opts in, so other raster readers keep raw-copy behavior.
    private val tilesIter = BalancedSubdivision.splitRasterIter(ds, Map("applyScale" -> "true"), partition.sizeInMB)
    RST_ExpressionUtil.addCleanupListener(tilesIter)
    private val hconf = partition.expressionConfig.hConf
    // The result-facing source keeps the ORIGINAL (remote) path, not the local staging copy.
    // Whole-file case: use the bare original file path (no NETCDF:"...": wrapper) — it stays
    // a valid, re-openable path, whereas a variable-less selector would not parse.
    private val srcSelector =
        if (wholeFile) partition.filePath
        else s"""NETCDF:"${partition.filePath}":${partition.subdatasetName}"""

    override def next(): Boolean = tilesIter.hasNext

    override def get(): InternalRow = {
        val tile = tilesIter.next()
        val tileRow = RasterSerializationUtil.tileToRow((-1L, tile._1, tile._2), BinaryType, hconf)
        RasterDriver.releaseDataset(tile._1)
        InternalRow.fromSeq(Seq(UTF8String.fromString(srcSelector), tileRow))
    }

    override def close(): Unit = {
        if (!isLocal) NodeFileManager.releaseRemote(partition.filePath)
    }
}
