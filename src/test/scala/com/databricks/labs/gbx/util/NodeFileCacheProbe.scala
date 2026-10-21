package com.databricks.labs.gbx.util

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

/** What the node-file cache holds on disk, for tests that check a read leaves no copy behind. */
object NodeFileCacheProbe {

    /** Every file and directory under `NodeFilePathUtil.rootPath`, relative to it. */
    def entries(): Set[String] = {
        val root = NodeFilePathUtil.rootPath
        if (!Files.exists(root)) Set.empty
        else {
            val s = Files.walk(root)
            try s.iterator().asScala.filter(_ != root).map(p => root.relativize(p).toString).toSet
            finally s.close()
        }
    }

    /** Bytes no GDAL or OGR driver recognises. */
    val garbage: Array[Byte] = ("not a dataset\n" * 64).getBytes(StandardCharsets.UTF_8)

    /** A fresh temp dir for `body`, deleted recursively afterwards. */
    def withScratchDir[T](prefix: String)(body: Path => T): T = {
        val dir = Files.createTempDirectory(prefix)
        try body(dir)
        finally deleteTree(dir)
    }

    /** A fresh temp dir holding a copy of the `tas_singlevar.nc` resource: `body(dir, nc)`. A fresh remote per
      * test, since the cache objects are JVM singletons. */
    def withNetCDFCopy[T](body: (Path, Path) => T): T = withScratchDir("gbx_nc_cache_") { dir =>
        val src = Paths.get(getClass.getResource("/binary/netcdf-singlevar/tas_singlevar.nc").toURI)
        val nc = Files.copy(src, dir.resolve("tas_singlevar.nc"))
        body(dir, nc)
    }

    def deleteTree(p: Path): Unit =
        if (Files.exists(p)) {
            val s = Files.walk(p)
            try s.sorted(java.util.Comparator.reverseOrder[Path]()).iterator().asScala.foreach(q => Files.deleteIfExists(q))
            finally s.close()
        }

}
