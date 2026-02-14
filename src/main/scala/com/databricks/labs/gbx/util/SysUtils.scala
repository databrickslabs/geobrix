package com.databricks.labs.gbx.util

import java.io.{BufferedReader, InputStream, InputStreamReader}

/** Runs external commands (Process/ProcessIO) and scripts; returns (stdout, stdout, stderr) or (exitValue, stdout, stderr). */
object SysUtils {

    import sys.process._

    /** Runs command with ProcessIO; blocks until exit; returns (stdout, stdout, stderr). */
    def runCommand(parts: Seq[String]): (String, String, String) = {
        val outSb = new StringBuilder
        val errSb = new StringBuilder

        def drain(is: InputStream, sb: StringBuilder): Unit = {
            val src = scala.io.Source.fromInputStream(is)
            try sb.appendAll(src.mkString) finally { src.close(); is.close() }
        }

        val io = new ProcessIO(
            in  => try in.close() catch { case _: Throwable => () },                // never leave stdin open
            out => drain(out, outSb),                                              // fully consume stdout
            err => drain(err, errSb)                                               // fully consume stderr
        )

        val p = Process(parts).run(io)                                           // start with custom IO
        p.exitValue()                                                            // blocks and reaps the direct child
        val stdout = outSb.toString
        (stdout, stdout, errSb.toString)                                         // keep your legacy tuple
    }

    /** Executes cmd via Runtime.exec; returns (exitValue string, stdout, stderr). */
    def runScript(cmd: Array[String]): (String, String, String) = {
        val p = Runtime.getRuntime.exec(cmd)
        val stdinStream = new BufferedReader(new InputStreamReader(p.getInputStream))
        val stderrStream = new BufferedReader(new InputStreamReader(p.getErrorStream))
        val exitValue =
            try {
                p.waitFor()
            } catch {
                case e: Exception => s"ERROR: ${e.getMessage}"
            }
        val stdinOutput = stdinStream.lines().toArray.mkString("\n")
        val stderrOutput = stderrStream.lines().toArray.mkString("\n")
        stdinStream.close()
        stderrStream.close()
        (s"$exitValue", stdinOutput, stderrOutput)
    }

    /** Returns the last line of the second element (stdout) of the (_, stdout, _) tuple. */
    def getLastOutputLine(prompt: (String, String, String)): String = {
        val (_, stdout, _) = prompt
        val lines = stdout.split("\n")
        lines.last
    }

}
