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

    /** Collect all rasterx sources EXCLUDING the serde chokepoint. */
    private def scanSrcExcludingSerde(): Map[String, String] = {
        val root = new java.io.File("src/main/scala/com/databricks/labs/gbx/rasterx")
        def files(f: java.io.File): Seq[java.io.File] =
            if (f.isDirectory) f.listFiles.toSeq.flatMap(files)
            else if (f.getName.endsWith(".scala")) Seq(f) else Nil
        files(root)
            .filterNot(_.getName == "RasterSerializationUtil.scala")
            .map { f =>
                val src = scala.io.Source.fromFile(f, "UTF-8")
                f.getName -> (try src.mkString finally src.close())
            }.toMap
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

    test("no positional tile-field reads survive outside the serde chokepoint") {
        // Ban two patterns that indicate un-threaded (positional) tile-field access:
        //   getMap(2)      — v1 read of metadata at hardcoded index 2 in the 3-field tile
        //   getStruct(n,3) — read of a struct field from a row with exactly 3 fields
        //                    (the old 3-field tile shape); after v2 all tile structs have 8 fields.
        // The only legitimate site for layout-aware tile reads is RasterSerializationUtil.scala,
        // which uses lyt.metadata / lyt.raster / elementFieldCount — those are explicitly excluded.
        val sources = scanSrcExcludingSerde()
        assert(sources.nonEmpty, "scan found no rasterx sources (excluding serde) — path resolution broken")

        val getMap2Hits = sources.collect { case (name, src) if src.contains("getMap(2)") => name }
        assert(getMap2Hits.isEmpty,
            s"Positional getMap(2) (v1 metadata-at-index-2) found outside serde chokepoint in: $getMap2Hits")

        // Match getStruct(anything, 3) — any whitespace variant — signalling the old 3-field tile shape.
        val getStruct3Re = """getStruct\s*\([^,)]+,\s*3\)""".r
        val getStruct3Hits = sources.collect {
            case (name, src) if getStruct3Re.findFirstIn(src).isDefined => name
        }
        assert(getStruct3Hits.isEmpty,
            s"Positional getStruct(_,3) (3-field tile shape) found outside serde chokepoint in: $getStruct3Hits")
    }
}
