package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.CheckpointManager
import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.spark.util.SerializableConfiguration

/** Lists and deletes raster checkpoint stage directories under CheckpointManager path. */
object CheckpointCleaner {

    /** Returns (stageId, path) for each stage_* directory under the checkpoint path. */
    def getStageDirs(hconf: SerializableConfiguration): Seq[(Int, String)] = {
        val cpPath = CheckpointManager.getCheckpointPath
        HadoopUtils
            .listHadoopDirs(cpPath, hconf)
            .filter(path => path.contains("stage_"))
            .map { path =>
                val stagePart = path.split("/").last
                val stageId = stagePart.split("_")(1).toInt
                (stageId, path)
            }
    }

    /** Deletes checkpoint directories for the given stage IDs. */
    def deleteStages(stages: Seq[Int], hconf: SerializableConfiguration): Unit = {
        val existingStageDirs = getStageDirs(hconf)
        existingStageDirs.foreach { case (sid, path) =>
            if (stages.contains(sid)) {
                // Delete the directory
                HadoopUtils.deleteIfExists(path, hconf)
            }
        }
    }

}
