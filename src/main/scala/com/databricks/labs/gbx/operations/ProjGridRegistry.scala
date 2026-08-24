package com.databricks.labs.gbx.operations

/** Driver-side registry of user-supplied PROJ grid dirs, folded into
  * ExpressionConfig at query-analysis time (see ExpressionConfig.apply). */
object ProjGridRegistry {
  private var dirs: Seq[String] = Seq.empty

  def set(newDirs: Seq[String], replace: Boolean): Seq[String] = synchronized {
    val base = if (replace) Seq.empty else dirs
    dirs = (base ++ newDirs).distinct
    dirs
  }

  def get: Seq[String] = synchronized { dirs }
}
