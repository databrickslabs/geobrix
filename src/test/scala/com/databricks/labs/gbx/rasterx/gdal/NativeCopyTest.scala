package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The node-local copy of a native payload ([[NativeCopy]]), over a small fake payload: no GDAL is loaded here (the
  * child-JVM row in [[GDALManagerNodeCopyTest]] is the end-to-end one). */
class NativeCopyTest extends AnyFunSuite {

    private val Jni = "libgdalalljni.so"
    private def sha(p: Path): String =
        Try(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(p)).map("%02x".format(_)).mkString).getOrElse(p.toString)

    /** A run's env wheel: `<root>/_native/x86_64/{lib,bin}` and `<root>/_native/share/{proj,gdal}`; the native dir. */
    private def payload(jniBytes: String = "jni-bytes"): (Path, Path, NativeChoice) = {
        val root = Files.createTempDirectory("gbx_wheel")
        val dir = Files.createDirectories(root.resolve("_native/x86_64"))
        Files.createDirectories(dir.resolve("lib/cmake"))
        Files.write(dir.resolve(s"lib/$Jni"), jniBytes.getBytes(StandardCharsets.UTF_8))
        Files.write(dir.resolve("lib/libgdal-0000.so.39"), "gdal".getBytes(StandardCharsets.UTF_8))
        Files.write(dir.resolve("lib/cmake/x.cmake"), "x".getBytes(StandardCharsets.UTF_8))
        dir.resolve(s"lib/$Jni").toFile.setExecutable(true)
        Files.createDirectories(dir.resolve("bin"))
        Files.write(dir.resolve("bin/projsync"), "bin".getBytes(StandardCharsets.UTF_8))
        Files.createDirectories(root.resolve("_native/share/proj"))
        Files.write(root.resolve("_native/share/proj/proj.db"), "db".getBytes(StandardCharsets.UTF_8))
        Files.createDirectories(root.resolve("_native/share/gdal"))
        Files.write(root.resolve("_native/share/gdal/bag_template.xml"), "<x/>".getBytes(StandardCharsets.UTF_8))
        val d = dir.toString
        (root, dir, NativeChoice(s"$d/lib/$Jni", Some(s"$d/../share/proj"), Some(s"$d/../share/gdal")))
    }

    private def delete(f: File): Unit = {
        Option(f.listFiles()).foreach(_.foreach(delete))
        f.delete()
        ()
    }

    private def entries(root: Path): Set[String] = {
        val s = Files.list(root)
        try s.iterator().asScala.map(_.getFileName.toString).toSet finally s.close()
    }

    test("the copy root: a setting, off, else the local disk's geobrix/native, else none") {
        assert(NativeCopy.rootFrom(Some("/x/copies"), None).contains(Paths.get("/x/copies")))
        assert(NativeCopy.rootFrom(Some(" OFF "), Some(Paths.get("/local_disk0"))).isEmpty)
        assert(NativeCopy.rootFrom(None, Some(Paths.get("/local_disk0"))).contains(Paths.get("/local_disk0/geobrix/native")))
        assert(NativeCopy.rootFrom(Some(""), None).isEmpty)
        assert(NativeCopy.keyFor("a" * 64, "0.6.0").contains("0.6.0-aaaaaaaaaaaaaaaa"))
        assert(NativeCopy.keyFor("/path/when/unreadable").isEmpty)
    }

    test("a payload is copied once, marked done, and its copy outlives the payload") {
        val (wheel, dir, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        val Right(copy) = NativeCopy.stabilize(c, root, id, sha)
        val key = NativeCopy.keyFor(id).get
        assert(copy.jni == s"$root/$key/x86_64/lib/$Jni")
        assert(copy.projData.contains(s"$root/$key/x86_64/../share/proj"))
        assert(Files.isRegularFile(root.resolve(s"$key/${NativeCopy.DoneMarker}")))
        assert(Files.isRegularFile(root.resolve(s"$key/x86_64/lib/cmake/x.cmake")))
        assert(Files.isRegularFile(root.resolve(s"$key/x86_64/bin/projsync")))
        assert(Files.isExecutable(Paths.get(copy.jni)), "the library lost its mode")
        assert(Files.isRegularFile(Paths.get(copy.projData.get, "proj.db")))
        assert(Files.isRegularFile(Paths.get(copy.gdalData.get, "bag_template.xml")))
        assert(entries(root) == Set(key, ".gbx-copy.lock"), "a temporary directory was left")
        // a later JVM reuses the done copy, also when the payload it came from is gone
        val stamp = Files.getLastModifiedTime(root.resolve(s"$key/${NativeCopy.DoneMarker}"))
        delete(wheel.toFile)
        assert(!Files.exists(dir))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(Files.getLastModifiedTime(root.resolve(s"$key/${NativeCopy.DoneMarker}")) == stamp)
    }

    test("an unfinished copy is made again, and a payload gone or changed while copied is refused with a Fix") {
        val (wheel, _, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        val key = NativeCopy.keyFor(id).get
        // a JVM killed mid-copy: the renamed directory has no marker, and a temporary one is left
        Files.createDirectories(root.resolve(s"$key/x86_64/lib"))
        Files.createDirectories(root.resolve(s".tmp-$key-dead"))
        val Right(copy) = NativeCopy.stabilize(c, root, id, sha)
        assert(Files.isRegularFile(Paths.get(copy.jni)) && Files.isRegularFile(root.resolve(s"$key/${NativeCopy.DoneMarker}")))
        assert(entries(root) == Set(key, ".gbx-copy.lock"))
        // another payload's bytes under the identity the JVM hashed: it changed between the hash and the copy
        val (_, _, other) = payload("other-bytes")
        val Left(changed) = NativeCopy.stabilize(other, Files.createTempDirectory("gbx_copies"), id, sha)
        assert(changed.contains("changed or was removed") && changed.contains("Fix:"), changed)
        // the payload is gone before any copy was made
        delete(wheel.toFile)
        val empty = Files.createTempDirectory("gbx_copies")
        val Left(gone) = NativeCopy.stabilize(c, empty, id, sha)
        assert(gone.contains("changed or was removed") && gone.contains("Fix:"), gone)
        assert(entries(empty) == Set(".gbx-copy.lock"), "a refused copy left a directory")
    }

    test("concurrent initialisers, threads and JVMs, make one copy") {
        val (_, dir, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        // two child JVMs (the processes of one node) and eight threads of this one, started together
        val java = new File(System.getProperty("java.home"), "bin/java").getPath
        val procs = (1 to 2).map { _ =>
            new ProcessBuilder(
              java, "-cp", System.getProperty("java.class.path"), "com.databricks.labs.gbx.rasterx.gdal.NativeCopyProbe",
              dir.toString, root.toString, id
            ).redirectErrorStream(true).start()
        }
        val pool = Executors.newFixedThreadPool(8)
        val threads = (1 to 8).map(_ => pool.submit(new Callable[Either[String, NativeChoice]] {
            override def call(): Either[String, NativeChoice] = NativeCopy.stabilize(c, root, id, sha)
        }))
        val results = threads.map(_.get(120, TimeUnit.SECONDS))
        pool.shutdown()
        val children = procs.map { p =>
            val src = Source.fromInputStream(p.getInputStream)
            val out = try src.getLines().toList finally src.close()
            p.waitFor(120, TimeUnit.SECONDS)
            out
        }
        val key = NativeCopy.keyFor(id).get
        val jni = s"$root/$key/x86_64/lib/$Jni"
        assert(results.forall(_.exists(_.jni == jni)), s"$results")
        children.foreach(out => assert(out.contains(s"JNI=$jni"), out.mkString(" | ")))
        assert(entries(root) == Set(key, ".gbx-copy.lock"), s"${entries(root)}")
    }
}

/** A JVM of [[NativeCopyTest]]'s race: copies the payload `args(0)` under `args(1)` (JNI sha256 `args(2)`) and prints
  * `JNI=<the copy's JNI>` or `ERROR=<why>`. */
object NativeCopyProbe {
    def main(args: Array[String]): Unit = {
        val Array(dir, root, id) = args
        val c = NativeChoice(s"$dir/lib/libgdalalljni.so", Some(s"$dir/../share/proj"), Some(s"$dir/../share/gdal"))
        val sha = (p: Path) =>
            Try(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(p)).map("%02x".format(_)).mkString).getOrElse(p.toString)
        NativeCopy.stabilize(c, Paths.get(root), id, sha) match {
            case Right(copy) => println(s"JNI=${copy.jni}")
            case Left(why)   => println(s"ERROR=$why")
        }
    }
}
