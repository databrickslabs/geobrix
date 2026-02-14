package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig

/** Holds the raster checkpoint directory and whether checkpointing is enabled (from ExpressionConfig). */
object CheckpointManager {

    private var checkpointPath: String = _
    private var useCheckpoint: Boolean = false

    /** Initializes path and useCheckpoint from ExpressionConfig. */
    def init(config: ExpressionConfig): Unit = {
        checkpointPath = config.getRasterCheckpointDir
        useCheckpoint = config.useCheckpoint
    }

    /** Sets the checkpoint directory (used for path-based tile output). */
    def setCheckpointPath(path: String): Unit = {
        checkpointPath = path
    }

    /** Returns the checkpoint directory; throws if not initialized. */
    def getCheckpointPath: String = {
        if (checkpointPath == null || checkpointPath.isEmpty) {
            throw new IllegalStateException("Checkpoint path is not set. Please initialize CheckpointManager first.")
        }
        checkpointPath
    }

    /** Enables or disables raster checkpointing. */
    def setUseCheckpoint(use: Boolean): Unit = {
        useCheckpoint = use
    }

    /** Returns true if raster checkpointing is enabled. */
    def isUseCheckpoint: Boolean = useCheckpoint

}
