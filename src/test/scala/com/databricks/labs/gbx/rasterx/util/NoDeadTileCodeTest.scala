package com.databricks.labs.gbx.rasterx.util

import org.scalatest.funsuite.AnyFunSuite

class NoDeadTileCodeTest extends AnyFunSuite {

    private def scanSrc(dir: String): String = {
        val root = new java.io.File(dir)
        def files(f: java.io.File): Seq[java.io.File] =
            if (f.isDirectory) f.listFiles.toSeq.flatMap(files)
            else if (f.getName.endsWith(".scala")) Seq(f) else Nil
        files(root).map { f =>
            val src = scala.io.Source.fromFile(f, "UTF-8")
            try src.mkString finally src.close()
        }.mkString("\n")
    }

    test("no dead path-tile / checkpoint / eval-split symbols remain in heavy src") {
        val src = scanSrc("src/main/scala/com/databricks/labs/gbx")
        assert(src.nonEmpty, "scan found no sources — path resolution broken")
        val banned = Seq("def evalPath", "rstInvoke", "CheckpointManager",
            "CheckpointCleaner", "new CleanupListener", "getRasterCheckpointDir",
            "use.checkpoint")
        val hits = banned.filter(src.contains)
        assert(hits.isEmpty, s"Dead path-tile/checkpoint/eval symbols still present: $hits")
    }
}
