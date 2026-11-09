package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
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

    test("the copy roots: a setting alone, off, else the local disk's geobrix/native then the tmpdir's, else the tmpdir's") {
        val tmp = Paths.get("/tmp")
        assert(NativeCopy.rootsFrom(Some("/x/copies"), Some(Paths.get("/local_disk0")), tmp) == Seq(Paths.get("/x/copies")))
        assert(NativeCopy.rootsFrom(Some(" OFF "), Some(Paths.get("/local_disk0")), tmp).isEmpty)
        assert(NativeCopy.rootsFrom(None, Some(Paths.get("/local_disk0")), tmp) ==
            Seq(Paths.get("/local_disk0/geobrix/native"), Paths.get("/tmp/geobrix/native")))
        assert(NativeCopy.rootsFrom(Some(""), None, tmp) == Seq(Paths.get("/tmp/geobrix/native")))
        // java.io.tmpdir under the local disk (Databricks): /tmp is a third candidate; a base that repeats one is left out
        assert(NativeCopy.rootsFrom(None, Some(Paths.get("/local_disk0")), Paths.get("/local_disk0/tmp")) ==
            Seq("/local_disk0", "/local_disk0/tmp", "/tmp").map(b => Paths.get(b, "geobrix/native")))
        val real = Files.createTempDirectory("gbx_base")
        val link = Files.createSymbolicLink(Files.createTempDirectory("gbx_base").resolve("again"), real)
        assert(NativeCopy.rootsFrom(None, None, real, link) == Seq(real.resolve("geobrix/native")))
        assert(NativeCopy.keyFor("a" * 64, "0.6.0").contains("0.6.0-aaaaaaaaaaaaaaaa"))
        assert(NativeCopy.keyFor("/path/when/unreadable").isEmpty)
    }

    test("this JVM's uid is read from the first directory it can write, so an unwritable java.io.tmpdir is no refusal") {
        val missing = Paths.get("/gbx/no/such/dir")
        val writable = Files.createTempDirectory("gbx_uid_dir")
        val uid = NativeCopy.uidOfNewFileIn(Seq(writable))
        assert(NativeCopy.uidOfNewFileIn(Seq(missing, writable)) == uid)
        assert(entries(writable).isEmpty) // the probe file is removed
        assertThrows[IOException](NativeCopy.uidOfNewFileIn(Seq(missing)))
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

    test("a done copy with a planted or altered library, or a payload that changed, is made again") {
        val (_, dir, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        val Right(copy) = NativeCopy.stabilize(c, root, id, sha)
        val lib = Paths.get(copy.jni).getParent
        // a library planted next to the JNI, where its RUNPATH would find it
        Files.write(lib.resolve("libevil.so.1"), "evil".getBytes(StandardCharsets.UTF_8))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(!Files.exists(lib.resolve("libevil.so.1")), "a planted library was kept")
        // a library of the copy altered in place, its size kept
        Files.write(lib.resolve("libgdal-0000.so.39"), "evil".getBytes(StandardCharsets.UTF_8))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(new String(Files.readAllBytes(lib.resolve("libgdal-0000.so.39")), StandardCharsets.UTF_8) == "gdal")
        // the payload's data changed while the copy stood: the copy takes the new bytes
        Files.write(dir.resolve("../share/proj/proj.db"), "db2".getBytes(StandardCharsets.UTF_8))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(new String(Files.readAllBytes(Paths.get(copy.projData.get, "proj.db")), StandardCharsets.UTF_8) == "db2")
        // a link in the copy (none is made: the copy follows the payload's links)
        Files.createSymbolicLink(lib.resolve("liblink.so"), Paths.get("/etc/hostname"))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(!Files.exists(lib.resolve("liblink.so"), java.nio.file.LinkOption.NOFOLLOW_LINKS))
        assert(entries(root) == Set(NativeCopy.keyFor(id).get, ".gbx-copy.lock"))
    }

    test("a root another user could write is refused, so the JVM loads in place; a new root is owner-only") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        val open = Files.createTempDirectory("gbx_copies")
        Files.setPosixFilePermissions(open, PosixFilePermissions.fromString("rwxrwx---"))
        val e = intercept[NativeCopy.UnsafeCopyRoot](NativeCopy.stabilize(c, open, id, sha))
        assert(e.getMessage.contains("writable by its group or other users"), e.getMessage)
        assert(entries(open).isEmpty, "a refused root was written")
        // the caller (GDALManager) loads in place on any IOException
        assert(e.isInstanceOf[java.io.IOException])
        // a root under a directory other users can write (not sticky) is refused before it is made
        Files.setPosixFilePermissions(open, PosixFilePermissions.fromString("rwxrwxrwx"))
        intercept[NativeCopy.UnsafeCopyRoot](NativeCopy.stabilize(c, open.resolve("copies"), id, sha))
        assert(!Files.exists(open.resolve("copies")))
        // a parent replaced by a link to a directory other users can write: refused before anything is made there
        val base = Files.createTempDirectory("gbx_copies")
        Files.createSymbolicLink(base.resolve("parent"), open)
        intercept[NativeCopy.UnsafeCopyRoot](NativeCopy.stabilize(c, base.resolve("parent/copies"), id, sha))
        assert(!Files.exists(open.resolve("copies")), "a directory was made through the link")
        // a root that is this uid's but not owner-only is refused too
        val wide = Files.createTempDirectory("gbx_copies")
        Files.setPosixFilePermissions(wide, PosixFilePermissions.fromString("rwxr-xr-x"))
        intercept[NativeCopy.UnsafeCopyRoot](NativeCopy.stabilize(c, wide, id, sha))
        // a root made here is 0700, and so is each copy
        val made = Files.createTempDirectory("gbx_copies").resolve("a/b")
        val Right(copy) = NativeCopy.stabilize(c, made, id, sha)
        assert(PosixFilePermissions.toString(Files.getPosixFilePermissions(made)) == "rwx------")
        val key = made.resolve(NativeCopy.keyFor(id).get)
        assert(PosixFilePermissions.toString(Files.getPosixFilePermissions(key)) == "rwx------")
        assert(copy.jni.startsWith(key.toString))
    }

    test("an unsafe first candidate falls to the next; an explicit unsafe root does not fall through") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        val unsafe = Files.createTempDirectory("gbx_copies")
        Files.setPosixFilePermissions(unsafe, PosixFilePermissions.fromString("rwxrwx---")) // as another owner's would be
        val good = Files.createTempDirectory("gbx_copies").resolve("geobrix/native")
        val Right(copy) = NativeCopy.stabilizeFirst(c, Seq(unsafe, good), id, sha)
        assert(copy.jni.startsWith(good.toRealPath().toString), copy.jni)
        assert(entries(unsafe).isEmpty, "a refused root was written")
        // a root that is set is the only candidate: refused, the caller loads in place, no other root is made
        val other = Files.createTempDirectory("gbx_copies").resolve("geobrix/native")
        val roots = NativeCopy.rootsFrom(Some(unsafe.toString), Some(other.getParent.getParent), other.getParent.getParent)
        assert(roots == Seq(unsafe.toAbsolutePath.normalize()))
        val e = intercept[java.io.IOException](NativeCopy.stabilizeFirst(c, roots, id, sha))
        assert(e.getMessage.contains("no native copy root passed") && e.getMessage.contains("writable by its group"), e.getMessage)
        assert(!Files.exists(other), "an unset-default root was made after a set root was refused")
        // when every candidate is refused, the message names each with its reason
        val all = intercept[java.io.IOException](NativeCopy.stabilizeFirst(c, Seq(unsafe, unsafe.resolve("x")), id, sha))
        assert(all.getMessage.contains(unsafe.toString) && all.getMessage.contains(unsafe.resolve("x").toString), all.getMessage)
    }

    test("unsafe local-disk and tmpdir candidates fall through to the system /tmp") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        // a base another user could write, not sticky: its candidate is refused before anything is made under it
        def unsafeBase() = {
            val d = Files.createTempDirectory("gbx_base")
            Files.setPosixFilePermissions(d, PosixFilePermissions.fromString("rwxrwxrwx"))
            d
        }
        val (localDisk, javaTmp) = (unsafeBase(), unsafeBase())
        val systemTmp = Files.createTempDirectory("gbx_base")
        val roots = NativeCopy.rootsFrom(None, Some(localDisk), javaTmp, systemTmp)
        assert(roots.size == 3)
        val Right(copy) = NativeCopy.stabilizeFirst(c, roots, id, sha)
        assert(copy.jni.startsWith(systemTmp.toRealPath().resolve("geobrix/native").toString), copy.jni)
        assert(entries(localDisk).isEmpty && entries(javaTmp).isEmpty, "a refused base was written")
    }

    test("a candidate with less than 3x the payload free is skipped") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        val small = Files.createTempDirectory("gbx_copies").resolve("small")
        val big = Files.createTempDirectory("gbx_copies").resolve("big")
        val payloadSize = "jni-bytes".length + "gdal".length + "x".length + "bin".length + "db".length + "<x/>".length
        // `small` does not exist until secureRoot makes it: match it by prefix of its parent's real path
        val parentReal = small.getParent.toRealPath()
        val free = (p: Path) => if (p.startsWith(parentReal)) 3L * payloadSize - 1 else 3L * payloadSize
        val Right(copy) = NativeCopy.stabilizeFirst(c, Seq(small, big), id, sha, free)
        assert(copy.jni.startsWith(big.toRealPath().toString), copy.jni)
        assert(!Files.exists(small.resolve(NativeCopy.keyFor(id).get)), "the skipped candidate holds a copy")
        // exactly 3x is enough
        val Right(exact) = NativeCopy.stabilize(c, Files.createTempDirectory("gbx_copies"), id, sha, _ => 3L * payloadSize)
        assert(Files.isRegularFile(Paths.get(exact.jni)))
        // less is refused by stabilize itself, as an IOException the caller treats as "load in place"
        val e = intercept[NativeCopy.InsufficientSpace](
          NativeCopy.stabilize(c, Files.createTempDirectory("gbx_copies"), id, sha, _ => 3L * payloadSize - 1)
        )
        assert(e.getMessage.contains("bytes free"), e.getMessage)
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
