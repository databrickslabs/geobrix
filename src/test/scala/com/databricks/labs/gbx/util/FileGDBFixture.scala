package com.databricks.labs.gbx.util

import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipInputStream
import scala.jdk.CollectionConverters._

/** Unzipped FileGDB directories for tests that read a `.gdb` folder rather than a `.gdb.zip`. */
object FileGDBFixture {

    /** The NYC sample FileGDB (5 features) staged under the container's `/Volumes` mount; absent elsewhere. */
    val nycSampleZip: Path = Paths.get("/Volumes/main/default/test-data/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip")

    /** Unzips the bundled `bridges.gdb.zip` test resource; returns its `.gdb` directory. */
    def unzipBridges(): Path = unzipGdb(Paths.get(getClass.getResource("/binary/gdb/bridges.gdb.zip").toURI))

    /** Unzips `zip` into a fresh temp dir and returns the one top-level `.gdb` directory in it. */
    def unzipGdb(zip: Path): Path = {
        val out = Files.createTempDirectory("gbx_gdb_dir_")
        val in = new ZipInputStream(Files.newInputStream(zip))
        try {
            Iterator.continually(in.getNextEntry).takeWhile(_ != null).foreach { e =>
                val target = out.resolve(e.getName).normalize()
                require(target.startsWith(out), s"zip entry outside the target dir: ${e.getName}")
                if (e.isDirectory) Files.createDirectories(target)
                else {
                    Files.createDirectories(target.getParent)
                    Files.copy(in, target)
                }
            }
        } finally in.close()
        val gdbs = Files.list(out).iterator().asScala.filter(p => p.getFileName.toString.endsWith(".gdb")).toList
        require(gdbs.size == 1 && Files.isDirectory(gdbs.head), s"expected one .gdb directory in $zip, got $gdbs")
        gdbs.head
    }

}
