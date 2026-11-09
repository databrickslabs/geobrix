package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, IOException, UncheckedIOException}
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.nio.file.attribute.{AclEntry, AclEntryPermission, AclEntryType, AclFileAttributeView, PosixFilePermissions, UserPrincipal}
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

    /** The key of `c`'s copy: its JNI's sha256 and the sha256 of its manifest. */
    private def keyOf(c: NativeChoice, id: String): String =
        NativeCopy.keyFor(id, NativeCopy.sourceManifest(c, id, sha).toOption.get.hash).get

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
        assert(NativeCopy.keyFor("a" * 64, "b" * 64, "0.6.0").contains("0.6.0-aaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb"))
        assert(NativeCopy.keyFor("/path/when/unreadable", "b" * 64).isEmpty)
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
        val key = keyOf(c, id)
        assert(copy.jni == s"$root/$key/x86_64/lib/$Jni")
        assert(copy.projData.contains(s"$root/$key/x86_64/../share/proj"))
        assert(Files.isRegularFile(root.resolve(s"$key/${NativeCopy.DoneMarker}")))
        assert(Files.isRegularFile(root.resolve(s"$key/x86_64/lib/cmake/x.cmake")))
        assert(Files.isRegularFile(root.resolve(s"$key/x86_64/bin/projsync")))
        assert(Files.isExecutable(Paths.get(copy.jni)), "the library lost its mode")
        assert(Files.isRegularFile(Paths.get(copy.projData.get, "proj.db")))
        assert(Files.isRegularFile(Paths.get(copy.gdalData.get, "bag_template.xml")))
        assert(entries(root) == Set(key, ".gbx-copy.lock"), "a temporary directory was left")
        // a later JVM reuses the done copy
        val stamp = Files.getLastModifiedTime(root.resolve(s"$key/${NativeCopy.DoneMarker}"))
        assert(NativeCopy.stabilize(c, root, id, sha) == Right(copy))
        assert(Files.getLastModifiedTime(root.resolve(s"$key/${NativeCopy.DoneMarker}")) == stamp)
        // the payload gone: the key needs its files, so it is refused, and the copy a running JVM maps is not touched
        delete(wheel.toFile)
        assert(!Files.exists(dir))
        val Left(gone) = NativeCopy.stabilize(c, root, id, sha)
        assert(gone.contains("changed or was removed"), gone)
        assert(Files.isRegularFile(Paths.get(copy.jni)) && Files.getLastModifiedTime(root.resolve(s"$key/${NativeCopy.DoneMarker}")) == stamp)
    }

    test("an unfinished temporary copy is removed, and a payload gone or changed while copied is refused with a Fix") {
        val (wheel, _, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        val key = keyOf(c, id)
        // a JVM killed mid-copy left a temporary directory
        Files.createDirectories(root.resolve(s".tmp-$key-dead/x86_64/lib"))
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
        assert(entries(empty).isEmpty, "a refused copy left a directory")
    }

    test("a payload with the same JNI and other data is another copy; neither is removed") {
        val (_, dirA, a) = payload()
        val (_, dirB, b) = payload()
        Files.write(dirB.resolve("../share/proj/proj.db"), "db-other".getBytes(StandardCharsets.UTF_8)) // same JNI, other PROJ data
        val id = sha(Paths.get(a.jni))
        assert(id == sha(Paths.get(b.jni)))
        val root = Files.createTempDirectory("gbx_copies")
        val Right(copyA) = NativeCopy.stabilize(a, root, id, sha)
        val Right(copyB) = NativeCopy.stabilize(b, root, id, sha)
        assert(copyA.jni != copyB.jni && keyOf(a, id) != keyOf(b, id))
        val markerA = root.resolve(s"${keyOf(a, id)}/${NativeCopy.DoneMarker}")
        val stamp = Files.getLastModifiedTime(markerA)
        // each is reused by its own payload, in either order: neither copy is replaced by the other's
        assert(NativeCopy.stabilize(a, root, id, sha) == Right(copyA))
        assert(NativeCopy.stabilize(b, root, id, sha) == Right(copyB))
        assert(NativeCopy.stabilize(a, root, id, sha) == Right(copyA))
        assert(Files.getLastModifiedTime(markerA) == stamp)
        assert(new String(Files.readAllBytes(Paths.get(copyA.projData.get, "proj.db")), StandardCharsets.UTF_8) == "db")
        assert(new String(Files.readAllBytes(Paths.get(copyB.projData.get, "proj.db")), StandardCharsets.UTF_8) == "db-other")
        assert(entries(root) == Set(keyOf(a, id), keyOf(b, id), ".gbx-copy.lock"))
    }

    test("a done copy that fails its marker, manifest, mode or file check is left as it is and a sibling is made and reused") {
        def marker(copy: NativeChoice): Path = Paths.get(copy.jni).getParent.getParent.getParent.resolve(NativeCopy.DoneMarker)
        def rewrite(copy: NativeChoice)(f: Seq[String] => Seq[String]): Unit = {
            val m = marker(copy)
            val lines = new String(Files.readAllBytes(m), StandardCharsets.UTF_8).linesIterator.toList
            Files.write(m, f(lines).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
        }
        def lib(copy: NativeChoice) = Paths.get(copy.jni).getParent
        val other = "f" * 64
        val rejections: Seq[(String, NativeChoice => Unit)] = Seq(
          "marker names another JNI"      -> (c => rewrite(c)(l => other +: l.tail)),
          "marker names another manifest" -> (c => rewrite(c)(l => l.take(1) ++ Seq(other) ++ l.drop(2))),
          "manifest line is not a line"   -> (c => rewrite(c)(l => l :+ "not a manifest line")),
          "manifest hash differs"         -> (c => rewrite(c)(l => l.take(3) ++ l.drop(3).map(_.replaceFirst("^[0-9a-f]{64}", other)))),
          "manifest lacks a file"         -> (c => rewrite(c)(l => l.dropRight(1))),
          "group-writable directory"      -> (c => Files.setPosixFilePermissions(marker(c).getParent, PosixFilePermissions.fromString("rwxrwx---"))),
          "library planted"               -> (c => Files.write(lib(c).resolve("libevil.so.1"), "evil".getBytes(StandardCharsets.UTF_8))),
          "library altered, size kept"    -> (c => Files.write(lib(c).resolve("libgdal-0000.so.39"), "evil".getBytes(StandardCharsets.UTF_8))),
          "library gone"                  -> (c => Files.delete(lib(c).resolve("libgdal-0000.so.39"))),
          "link in the copy"              -> (c => Files.createSymbolicLink(lib(c).resolve("liblink.so"), Paths.get("/etc/hostname")))
        )
        rejections.foreach { case (why, damage) =>
            val (_, _, c) = payload()
            val root = Files.createTempDirectory("gbx_copies")
            val id = sha(Paths.get(c.jni))
            val Right(first) = NativeCopy.stabilize(c, root, id, sha)
            val key = keyOf(c, id)
            damage(first)
            val Right(second) = NativeCopy.stabilize(c, root, id, sha)
            withClue(s"$why: ") {
                assert(second.jni == s"$root/$key-1/x86_64/lib/$Jni", second.jni)
                assert(Files.isRegularFile(marker(first)), "the copy that failed its check was removed")
                // the sibling is reused by the next JVM; no third copy is made
                assert(NativeCopy.stabilize(c, root, id, sha) == Right(second))
                assert(entries(root) == Set(key, s"$key-1", ".gbx-copy.lock"), s"${entries(root)}")
            }
            Files.setPosixFilePermissions(marker(first).getParent, PosixFilePermissions.fromString("rwx------"))
        }
    }

    test("at most 8 copies of one key are made; the next is an IOException and the caller loads in place") {
        val (_, _, c) = payload()
        val root = Files.createTempDirectory("gbx_copies")
        val id = sha(Paths.get(c.jni))
        val key = keyOf(c, id)
        // a first copy exists; each later call finds the whole family damaged
        var last: NativeChoice = NativeCopy.stabilize(c, root, id, sha).toOption.get
        (1 until NativeCopy.MaxCopiesPerKey).foreach { _ =>
            Files.write(Paths.get(last.jni), "evil".getBytes(StandardCharsets.UTF_8))
            Files.setPosixFilePermissions(Paths.get(last.jni).getParent.getParent.getParent, PosixFilePermissions.fromString("rwxrwx---"))
            last = NativeCopy.stabilize(c, root, id, sha).toOption.get
        }
        assert(entries(root).count(_.startsWith(key)) == NativeCopy.MaxCopiesPerKey)
        Files.write(Paths.get(last.jni), "evil".getBytes(StandardCharsets.UTF_8))
        val e = intercept[IOException](NativeCopy.stabilize(c, root, id, sha))
        assert(e.getMessage.contains("none of them intact"), e.getMessage)
    }

    test("a failed read of the copied JNI is an IOException with its own message, not 'payload changed'") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        // the source reads fine; every read under the root fails, as an IOException or as a non-hash answer
        def failingUnder(root: Path, fail: Path => String): Path => String =
            p => if (p.startsWith(root.toRealPath())) fail(p) else sha(p)
        val root = Files.createTempDirectory("gbx_copies")
        val e = intercept[IOException](NativeCopy.stabilize(c, root, id, failingUnder(root, p => throw new IOException(s"EIO $p"))))
        assert(e.getMessage.contains("could not read") && !e.getMessage.contains("changed or was removed"), e.getMessage)
        val root2 = Files.createTempDirectory("gbx_copies")
        val e2 = intercept[IOException](NativeCopy.stabilize(c, root2, id, failingUnder(root2, _.toString)))
        assert(e2.getMessage.contains("could not read"), e2.getMessage)
        // no temporary directory is left behind, and the failure is no refusal to the caller
        assert(entries(root).forall(_.startsWith(".gbx")) && entries(root2).forall(_.startsWith(".gbx")))
        // an unreadable source file is the same failure
        val e3 = intercept[IOException](NativeCopy.stabilize(c, Files.createTempDirectory("gbx_copies"), id, _ => throw new IOException("EIO")))
        assert(e3.getMessage.contains("could not read"), e3.getMessage)
    }

    test("lockOf waits for a lock another channel of this JVM holds; past its limit stabilize is an IOException") {
        val dir = Files.createTempDirectory("gbx_lock")
        val file = dir.resolve("l")
        val holder = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        val waiter = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        try {
            val held = holder.lock()
            val releaser = new Thread(() => { Thread.sleep(200); held.release() })
            releaser.start()
            val t0 = System.nanoTime()
            val got = NativeCopy.lockOf(waiter, maxWaitMs = 10000, pollMs = 10)
            assert((System.nanoTime() - t0) / 1000000 >= 100, "the lock was not waited for")
            releaser.join()
            got.release()
            // held for good: the wait ends at its limit with the JDK's exception ...
            val held2 = holder.lock()
            intercept[OverlappingFileLockException](NativeCopy.lockOf(waiter, maxWaitMs = 100, pollMs = 10))
            // ... which the copy boundary turns into an IOException (the caller loads in place)
            val (_, _, c) = payload()
            val root = Files.createTempDirectory("gbx_copies")
            val rootLock = FileChannel.open(root.toRealPath().resolve(".gbx-copy.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
            val old = NativeCopy.lockWaitMs
            try {
                val rootHeld = rootLock.lock()
                NativeCopy.lockWaitMs = 100
                val e = intercept[IOException](NativeCopy.stabilize(c, root, sha(Paths.get(c.jni)), sha))
                assert(e.getMessage.contains("lock"), e.getMessage)
                rootHeld.release()
            } finally { NativeCopy.lockWaitMs = old; rootLock.close() }
            held2.release()
        } finally { holder.close(); waiter.close() }
    }

    test("asIO turns an OverlappingFileLockException and an UncheckedIOException into IOExceptions, and nothing else") {
        val listing = intercept[IOException](NativeCopy.asIO(throw new UncheckedIOException(new IOException("listing failed"))))
        assert(listing.getMessage == "listing failed")
        assert(intercept[IOException](NativeCopy.asIO(throw new OverlappingFileLockException())).getMessage.contains("lock"))
        intercept[IllegalStateException](NativeCopy.asIO(throw new IllegalStateException("other")))
        assert(NativeCopy.asIO(7) == 7)
    }

    test("an ACL that grants write to a principal other than the owner or root is writable by others") {
        def principal(n: String): UserPrincipal = new UserPrincipal { override def getName: String = n }
        def entry(n: String, t: AclEntryType, perms: AclEntryPermission*): AclEntry =
            AclEntry.newBuilder().setType(t).setPrincipal(principal(n)).setPermissions(perms.toSet.asJava).build()
        def writers(owner: String, es: AclEntry*) = NativeCopy.aclWriters(es.asJava, owner)
        import AclEntryPermission._
        import AclEntryType._
        assert(writers("me", entry("me", ALLOW, WRITE_DATA, DELETE)).isEmpty)
        assert(writers("me", entry("root", ALLOW, WRITE_DATA)).isEmpty && writers("me", entry("OWNER@", ALLOW, APPEND_DATA)).isEmpty)
        assert(writers("me", entry("alice", ALLOW, READ_DATA, EXECUTE, READ_ATTRIBUTES)).isEmpty)
        assert(writers("me", entry("alice", DENY, WRITE_DATA)).isEmpty)
        assert(writers("me", entry("alice", ALLOW, READ_DATA), entry("bob", ALLOW, APPEND_DATA)).exists(_.contains("bob")))
        Seq(WRITE_DATA, DELETE_CHILD, WRITE_ACL, WRITE_OWNER, DELETE).foreach { p =>
            assert(writers("me", entry("GROUP@", ALLOW, p)).exists(_.contains("GROUP@")), p.toString)
        }
        // the limit the class doc names: on Linux the JDK gives no ACL view, so a POSIX ACL is not read
        if (System.getProperty("os.name").toLowerCase.contains("linux")) {
            assert(Files.getFileAttributeView(Files.createTempDirectory("gbx_acl"), classOf[AclFileAttributeView]) == null)
        }
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
        val key = made.resolve(keyOf(c, id))
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
        assert(!Files.exists(small.resolve(keyOf(c, id))), "the skipped candidate holds a copy")
        // exactly 3x is enough
        val Right(exact) = NativeCopy.stabilize(c, Files.createTempDirectory("gbx_copies"), id, sha, _ => 3L * payloadSize)
        assert(Files.isRegularFile(Paths.get(exact.jni)))
        // less is refused by stabilize itself, as an IOException the caller treats as "load in place"
        val e = intercept[NativeCopy.InsufficientSpace](
          NativeCopy.stabilize(c, Files.createTempDirectory("gbx_copies"), id, sha, _ => 3L * payloadSize - 1)
        )
        assert(e.getMessage.contains("bytes free"), e.getMessage)
    }

    test("the copy directory setting is the caller's, then the session's, the cluster's and the environment's") {
        val unread = (name: String) => (throw new AssertionError(s"$name was read")): Option[String]
        // an earlier source that is set ends the search: the later ones are not read
        assert(GDALManager.copyDirSetting(Some("/caller"), unread("session"), unread("cluster"), unread("env")).contains("/caller"))
        assert(GDALManager.copyDirSetting(None, Some("/session"), unread("cluster"), unread("env")).contains("/session"))
        assert(GDALManager.copyDirSetting(None, None, Some("/cluster"), unread("env")).contains("/cluster"))
        assert(GDALManager.copyDirSetting(None, None, None, Some("/env")).contains("/env"))
        // a blank caller value is none
        assert(GDALManager.copyDirSetting(Some("  "), Some("/session"), None, None).contains("/session"))
        assert(GDALManager.copyDirSetting(None, None, None, None).isEmpty)
    }

    test("the loader copies unless the setting is off or the JNI is on the library path; it loads in place when no copy can be made") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        val tmp = Files.createTempDirectory("gbx_base")
        var warned = List.empty[String]
        val warn = (m: String) => warned ::= m
        def load(setting: Option[String], onPath: Option[Path], copy: Seq[Path] => Either[String, NativeChoice]) =
            GDALManager.nodeLocalWith(c, setting, None, tmp, onPath, copy, warn)
        val never = (_: Seq[Path]) => fail("a copy was made"): Either[String, NativeChoice]
        // off: in place, and no copy is tried
        assert(load(Some("off"), None, never) == c)
        // the JNI on the library path: GDAL's Java classes load that one by name, so a copy would map a second JNI
        assert(load(None, Some(Paths.get("/usr/lib/libgdalalljni.so")), never) == c)
        // a copy made is the choice, and the roots are the setting's alone
        val copied = c.copy(jni = "/copy/x86_64/lib/libgdalalljni.so")
        var tried = Seq.empty[Path]
        assert(load(Some("/set/root"), None, r => { tried = r; Right(copied) }) == copied)
        assert(tried == Seq(Paths.get("/set/root")))
        // the payload changed while it was copied: a refusal, not an in-place load
        val refused = intercept[IllegalStateException](load(None, None, _ => Left("geobrix: the GDAL payload changed")))
        assert(refused.getMessage.contains("changed"))
        // no root passed, a lock or a read failed: in place, with a warning that says why
        assert(warned.isEmpty)
        assert(load(None, None, _ => throw new IOException("no native copy root passed: ...")) == c)
        assert(warned.size == 1 && warned.head.contains("no node-local copy") && warned.head.contains(c.jni), warned.toString)
    }

    test("a copy directory that is set and not owner-only loads in place with a warning, and is not written") {
        val (_, _, c) = payload()
        val id = sha(Paths.get(c.jni))
        val set = Files.createTempDirectory("gbx_copies")
        Files.setPosixFilePermissions(set, PosixFilePermissions.fromString("rwxr-xr-x"))
        var warned = List.empty[String]
        val chosen = GDALManager.nodeLocalWith(
          c, Some(set.toString), None, Files.createTempDirectory("gbx_base"), None,
          roots => NativeCopy.stabilizeFirst(c, roots, id, sha), (m: String) => warned ::= m
        )
        assert(chosen == c)
        assert(warned.size == 1 && warned.head.contains("not owner-only") && warned.head.contains(set.toString), warned.toString)
        assert(entries(set).isEmpty, "a refused root was written")
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
        val key = keyOf(c, id)
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
