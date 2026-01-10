package com.databricks.labs.gbx.rasterx

/**
  * Global singleton that filters PROJ "crs not found" errors from System.err.
  * Installed once and used across all test suites.
  */
object ProjErrorFilter {

  val TOKEN = "proj_create_from_database: crs not found"
  private var installed = false

  def install(): Unit = synchronized {
    if (!installed) {
      ErrorTokenListener.addToken(TOKEN)
      installed = true
    }
  }

  def restore(): Unit = synchronized {
    if (installed) {
      ErrorTokenListener.removeToken(TOKEN)
      installed = false
    }
  }

  // Install at class load time
  install()
}

