package com.databricks.labs.gbx.rasterx.gdal

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

/** On a dedicated cluster the executors outlive a notebook run, and a run's env wheel (its native payload) lives in
  * the run's own Python environment, which is removed when the run ends. The next run names its own copy of the same
  * payload: the JVM, which still has the first one mapped, must take it and not refuse it as "another GDAL".
  *
  * Runs [[StaleNativeDirProbe]] in a child JVM (a JVM cannot unload a GDAL, and this one's is already loaded) over the
  * GDAL the native build stages (`build/native/out/<arch>/stage-native/gdal-native`, what the env wheel packs); the
  * container's system GDAL is another version than the GDAL Java classes, so it cannot stand in.
  */
class GDALManagerStaleNativeDirTest extends AnyFunSuite {

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

    /** A wheel's `_native/<arch>` stand-in: `lib/` links to every file of the staged payload's `lib/`, `share/` to its data. */
    private def linkedPayload(staged: Path): Path = {
        val d = Files.createTempDirectory("gbx_native_dir")
        val lib = Files.createDirectories(d.resolve("lib"))
        val files = new File(staged.resolve("lib").toString).listFiles().filter(_.isFile)
        files.foreach(f => Files.createSymbolicLink(lib.resolve(f.getName), f.toPath.toRealPath()))
        Files.createSymbolicLink(d.resolve("share"), staged.resolve("share").toRealPath())
        d
    }

    /** The next run's copy of the payload: the same JNI bytes at another path. */
    private def copiedPayload(staged: Path): Path = {
        val d = Files.createTempDirectory("gbx_native_dir")
        Files.createDirectories(d.resolve("lib"))
        Files.copy(staged.resolve(s"lib/$Jni").toRealPath(), d.resolve(s"lib/$Jni"), StandardCopyOption.REPLACE_EXISTING)
        d
    }

    test("a later payload with the loaded JNI's bytes is taken when the loaded payload's directory is gone") {
        val staged = stagedPayload()
        assume(staged.isDefined, "PRECONDITION MISSING: no staged GDAL (build/native/out/<arch>/stage-native); stale-payload row not run")
        val first = linkedPayload(staged.get)
        val second = copiedPayload(staged.get)
        val java = new File(System.getProperty("java.home"), "bin/java").getPath
        // the payload's lib/ is the child's whole library path, as on a cluster with no other GDAL
        val pb = new ProcessBuilder(
          java, s"-Djava.library.path=$first/lib", "-cp", System.getProperty("java.class.path"),
          "com.databricks.labs.gbx.rasterx.gdal.StaleNativeDirProbe", first.toString, second.toString
        ).redirectErrorStream(true)
        val proc = pb.start()
        val lines = mutable.ArrayBuffer.empty[String]
        val src = Source.fromInputStream(proc.getInputStream)
        try src.getLines().foreach(lines += _) finally src.close()
        proc.waitFor(300, TimeUnit.SECONDS)
        val out = lines.filter(_.contains("=")).map { l => val i = l.indexOf('='); l.take(i) -> l.drop(i + 1) }.toMap
        assert(!lines.exists(_.startsWith("FIRST=ERROR")), s"the child JVM could not load the first payload: ${lines.mkString(" | ")}")
        assert(lines.contains("DONE"), s"the probe did not finish: ${lines.takeRight(4).mkString(" | ")}")
        assert(out("LATER") == "ok", s"the later payload was refused: ${out("LATER")}")
        assert(out("JNI") == s"$second/lib/$Jni")
        assert(out("PROJ_DATA").startsWith(s"$second/../share/proj"), s"PROJ_DATA did not move to the later payload: ${out("PROJ_DATA")}")
    }

    // laterNative is pure: these rows need no GDAL (the child-JVM row above is the end-to-end one)
    private def jniDir(bytes: Array[Byte]): Path = {
        val d = Files.createTempDirectory("gbx_native_dir")
        Files.createDirectories(d.resolve("lib"))
        Files.write(d.resolve(s"lib/$Jni"), bytes)
        d
    }

    private def later(loaded: Path, loadedBytes: Array[Byte], dir: Path) = {
        val c = GDALManager.NativeChoice(s"$loaded/lib/$Jni", Some(s"$loaded/../share/proj"), Some(s"$loaded/../share/gdal"))
        val sha = java.security.MessageDigest.getInstance("SHA-256").digest(loadedBytes).map("%02x".format(_)).mkString
        GDALManager.laterNative(
          c, Some(sha), dir.toString, p => Files.exists(Paths.get(p)), identity,
          p => Try(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(Paths.get(p))).map("%02x".format(_)).mkString).getOrElse(p)
        )
    }

    private val Bytes = Array[Byte](1, 2, 3, 4)

    test("a later dir naming a copy of the loaded JNI is the loaded GDAL while its file is there, and moves when it is gone") {
        val first = jniDir(Bytes)
        val copy = jniDir(Bytes.clone())
        assert(later(first, Bytes, copy) == Right(None))
        new File(s"$first/lib/$Jni").delete()
        val moved = later(first, Bytes, copy)
        assert(moved.exists(_.exists(_.jni == s"$copy/lib/$Jni")), s"$moved")
        assert(moved.exists(_.exists(_.projData.contains(s"$copy/../share/proj"))), s"$moved")
    }

    test("a later dir naming another JNI, or none, is refused whether or not the loaded file is still there") {
        val first = jniDir(Bytes)
        val other = jniDir(Array[Byte](9, 9, 9))
        val Left(another) = later(first, Bytes, other)
        assert(another.contains("a later configuration names") && another.contains(s"$other/lib/$Jni"), another)
        new File(s"$first/lib/$Jni").delete()
        assert(later(first, Bytes, other).isLeft)
        val Left(none) = later(first, Bytes, Files.createTempDirectory("gbx_native_dir_empty"))
        assert(none.contains("no GDAL JNI at"), none)
    }
}
