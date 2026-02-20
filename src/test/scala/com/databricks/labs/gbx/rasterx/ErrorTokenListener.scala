package com.databricks.labs.gbx.rasterx

import java.io.{OutputStream, PrintStream}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

object ErrorTokenListener {
  private val tokens = ConcurrentHashMap.newKeySet[String]()
  @volatile private var originalErr: PrintStream = System.err

  private val interceptor = new OutputStream {
    private val lineBuffer = new StringBuilder()

    override def write(b: Int): Unit = synchronized {
      if (b == '\n'.toInt) {
        val line = lineBuffer.toString()
        lineBuffer.setLength(0)
        processLine(line)
      } else if (b != '\r'.toInt) {
        lineBuffer.append(b.toChar)
      }
    }
  }

  // Extracted logic so both Java and Native streams use it
  private def processLine(line: String): Unit = {
    val containsToken = tokens.asScala.exists(line.contains)
    if (!containsToken) {
      originalErr.println(line)
    }
  }

  /**
   * Tweak: Hook into Native GDAL/PROJ errors
   * - Native C++ code writes directly to File Descriptor 2 (stderr),
   *   bypassing System.setErr. Use GDAL Manager to control gdal configs further.
   */
  def install(token: String = null): Unit = synchronized {
    // 1. Install Java Interceptor
    if (!System.err.getClass.getName.contains("ErrorTokenInterceptor")) {
      originalErr = System.err
      System.setErr(new PrintStream(interceptor, true))
    }
    addToken(token)
  }

  /**
   * Restores the original error stream.
   */
  def uninstall(): Unit = synchronized {
    System.setErr(originalErr)
  }

  def addToken(token: String): Unit = if (token != null) tokens.add(token)
  def removeToken(token: String): Unit = tokens.remove(token)
  def removeAllTokens(): Unit = tokens.clear()
  def getTokens: Set[String] = tokens.asScala.toSet

  // Initial install
  install()
}