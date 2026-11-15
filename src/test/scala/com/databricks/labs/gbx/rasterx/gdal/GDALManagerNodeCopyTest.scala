package com.databricks.labs.gbx.rasterx.gdal

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitOption, FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption}
import java.util.EnumSet
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.io.Source

/** A JVM loads GDAL from the node-local copy of its payload ([[NativeCopy]]), so removing the run's Python environment
  * (the payload) while the JVM runs takes neither a library nor PROJ's database from under it.
  *
  * Runs [[NodeCopyNativeProbe]] in a child JVM (this one's GDAL is already loaded) over a wheel made of real copies of
  * the GDAL the native build stages (`build/native/out/<arch>/stage-native/gdal-native`); links into the staged tree
  * would leave the files there after the delete.
  */
class GDALManagerNodeCopyTest extends AnyFunSuite {

    private val Jni = "libgdalalljni.so"

    private def stagedPayload(): Option[Path] = {
        val arch = System.getProperty("os.arch") match {
            case "amd64" | "x86_64"  => "x86_64"
            case "aarch64" | "arm64" => "aarch64"
            case other               => other
        }
        Seq(Paths.get("build/native/out", arch, "stage-native/gdal-native"))
            .map(_.toAbsolutePath).find(p => Files.exists(p.resolve(s"lib/$Jni")))
    }

    private def copyTree(from: Path, to: Path): Unit =
        Files.walkFileTree(from, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Int.MaxValue, new SimpleFileVisitor[Path] {
            override def preVisitDirectory(d: Path, a: BasicFileAttributes): FileVisitResult = {
                Files.createDirectories(to.resolve(from.relativize(d).toString))
                FileVisitResult.CONTINUE
            }
            override def visitFile(f: Path, a: BasicFileAttributes): FileVisitResult = {
                Files.copy(f, to.resolve(from.relativize(f).toString), StandardCopyOption.COPY_ATTRIBUTES)
                FileVisitResult.CONTINUE
            }
        })

    private def delete(f: File): Unit = {
        if (!Files.isSymbolicLink(f.toPath)) Option(f.listFiles()).foreach(_.foreach(delete))
        f.delete()
        ()
    }

    test("GDAL and PROJ keep working from the node-local copy after the payload is removed") {
        val staged = stagedPayload()
        assume(staged.isDefined, "PRECONDITION MISSING: no staged GDAL (build/native/out/<arch>/stage-native); node-copy row not run")
        // the env wheel's layout: <root>/_native/<arch>/lib and <root>/_native/share/{proj,gdal}
        val wheel = Files.createTempDirectory("gbx_wheel")
        val dir = wheel.resolve("_native/x86_64")
        copyTree(staged.get.resolve("lib"), dir.resolve("lib"))
        Seq("proj", "gdal").foreach(n => copyTree(staged.get.resolve(s"share/$n"), wheel.resolve(s"_native/share/$n")))
        val copies = Files.createTempDirectory("gbx_copies")
        try {
            // the copy and its reuse on this payload, timed: a reuse checks every file's sha256 (the probe reuses it)
            val c = GDALManager.NativeChoice(s"$dir/lib/$Jni", Some(s"$dir/../share/proj"), Some(s"$dir/../share/gdal"))
            val id = NativeCopy.sha256(Paths.get(c.jni))
            val t0 = System.nanoTime()
            assert(NativeCopy.stabilize(c, copies, id, NativeCopy.sha256).isRight)
            val t1 = System.nanoTime()
            assert(NativeCopy.stabilize(c, copies, id, NativeCopy.sha256).isRight)
            val t2 = System.nanoTime()
            val timing = f"NODE_COPY copy ${(t1 - t0) / 1e6}%.0f ms, reuse (verify) ${(t2 - t1) / 1e6}%.0f ms"
            println(timing)
            info(timing)
            val java = new File(System.getProperty("java.home"), "bin/java").getPath
            // a library path with no GDAL JNI (the system's would be another GDAL): GDAL's Java classes bind the JNI the
            // manager loads, from the copy
            val noJni = Files.createTempDirectory("gbx_no_jni")
            val pb = new ProcessBuilder(
              java, s"-Djava.library.path=$noJni", "-cp", System.getProperty("java.class.path"), "com.databricks.labs.gbx.rasterx.gdal.NodeCopyNativeProbe",
              dir.toString, copies.toString, wheel.toString
            ).redirectErrorStream(true)
            pb.environment().remove("GBX_NATIVE_ROOT")
            pb.environment().remove(GDALManager.NativeCopyDirEnv)
            val proc = pb.start()
            val lines = mutable.ArrayBuffer.empty[String]
            val src = Source.fromInputStream(proc.getInputStream)
            try src.getLines().foreach(lines += _) finally src.close()
            proc.waitFor(300, TimeUnit.SECONDS)
            val out = lines.filter(_.contains("=")).map { l => val i = l.indexOf('='); l.take(i) -> l.drop(i + 1) }.toMap
            assert(!lines.exists(_.startsWith("INIT=ERROR")), s"the child JVM could not load GDAL: ${lines.takeRight(6).mkString(" | ")}")
            assert(lines.contains("DONE"), s"the probe did not finish (exit ${proc.exitValue()}): ${lines.takeRight(6).mkString(" | ")}")
            assert(out("JNI").startsWith(s"$copies/") && out("JNI").endsWith(s"/x86_64/lib/$Jni"), s"not loaded from the copy: ${out("JNI")}")
            assert(out("MARKER") == "true", "the copy has no done marker")
            assert(out("WHEEL_MAPPED") == "0", s"libraries mapped from the payload, not the copy: ${out("WHEEL_MAPPED")}")
            assert(out("PRE") == "27700", s"srid before the delete: ${out("PRE")}")
            assert(out("GONE") == "true", "the payload was not removed")
            // new threads (new PROJ contexts) read the CRS's authority code from the copy's proj.db
            assert(out("SRIDS") == "27700", s"srid after the payload is gone: ${out("SRIDS")}")
            assert(out("TRANSFORM").startsWith("ok "), s"PROJ cannot transform after the payload is gone: ${out("TRANSFORM")}")
            assert(out("PROJ_DATA").startsWith(s"$copies/"), s"PROJ_DATA does not name the copy: ${out("PROJ_DATA")}")
        } finally {
            delete(wheel.toFile)
            delete(copies.toFile)
        }
    }
}
