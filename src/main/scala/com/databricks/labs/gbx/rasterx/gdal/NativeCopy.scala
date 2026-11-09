package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.apache.spark.internal.Logging

import java.io.IOException
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}
import java.security.MessageDigest
import java.util.{EnumSet, Properties, UUID}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * A node-local copy of the native payload a JVM loads GDAL from.
  *
  * On a dedicated cluster the env wheel's payload lives in a notebook's Python environment, which is removed when its
  * run ends, while the executor JVM that loaded GDAL from it keeps running. A JVM whose PROJ or GDAL data directory is
  * removed under it loses PROJ's database, and a removed library can kill it. So each node copies the payload once,
  * to `<root>/<key>/`, and every JVM on the node loads from that copy: `<key>/<arch>/lib` (and `bin`, `hdf5-plugins`)
  * and `<key>/share/{proj,gdal}`, the payload's layout, so `$ORIGIN` RPATHs and the `<dir>/../share` data rule hold.
  *
  * The key is `<GeoBrix version>-<first 16 hex of the JNI's sha256>`. A copy is made in a temporary directory in the
  * root, marked done, then renamed into place, all under a lock file, so the JVMs of one node make one copy. A JVM that
  * finds a done copy whose JNI has the same sha256 reuses it.
  *
  * No copy is pruned: a node holds one copy per distinct payload its cluster ran (about 130 MB each), its clusters
  * restart onto fresh local disks, and a JVM's mapped libraries and data cannot be checked safely from another process.
  * Only an unfinished copy (a JVM killed while copying) is removed, by the next JVM that takes the lock.
  *
  * The JVM loads native code from the copy, so no other user may be able to write it. The root and each copy are made
  * owner-only (0700). A root that another user owns or can write, or that sits under a directory another user can
  * write (one that is not sticky, as `/tmp` is), is refused ([[UnsafeCopyRoot]]), and the JVM loads in place. The done
  * marker holds a manifest, the sha256 of every copied file. A JVM reuses a copy only when its files are exactly the
  * manifest's, each with its sha256, and each source file that is still there has the same sha256. Any other copy is
  * removed and made again.
  */
private[gdal] object NativeCopy extends Logging {

    /** In a copy: present once every file is in place, with the JNI's sha256 and the source on its lines. */
    val DoneMarker = ".gbx-copy-done"
    private val LockName = ".gbx-copy.lock"
    private val TmpPrefix = ".tmp-"
    private val Sha256 = "[0-9a-f]{64}".r
    /** The root when none is set: Databricks' local disk, when the node has one. */
    val DatabricksLocalDisk = "/local_disk0"
    val DefaultSubdir = "geobrix/native"
    private val Off = Set("off", "none", "false")

    // the JVM's own guard: a file lock is held per JVM, so two threads of one JVM must not both take it
    private val jvmLock = new Object

    /** A copy root another user could write: the JVM loads the payload in place instead (the caller's IOException). */
    final class UnsafeCopyRoot(msg: String) extends IOException(msg)

    private val OwnerOnly = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    private val GroupOrOtherWrite = 0x12 // 0o022
    private val Sticky = 0x200 // 0o1000

    /** This JVM's uid: the owner of a file it creates. */
    private lazy val myUid: Int = {
        val f = Files.createTempFile("gbx_uid", "")
        try uidOf(f) finally Files.deleteIfExists(f)
    }

    private def uidOf(p: Path): Int = Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS).asInstanceOf[Int]
    private def modeOf(p: Path): Int = Files.getAttribute(p, "unix:mode", LinkOption.NOFOLLOW_LINKS).asInstanceOf[Int]

    /** Why another user could write `d` (a real path), or None. `own`: the root or a copy, which this JVM's uid must own
      * and no group or other user may write. Else a directory above the root: this uid's or root's, and writable by no
      * other user unless sticky. */
    private def writableByOthers(d: Path, own: Boolean): Option[String] = {
        val (uid, mode) = (uidOf(d), modeOf(d))
        if (own && uid != myUid) Some(s"$d is owned by uid $uid, not this JVM's uid $myUid")
        else if (!own && uid != myUid && uid != 0) Some(s"$d is owned by uid $uid")
        else if ((mode & GroupOrOtherWrite) != 0 && (own || (mode & Sticky) == 0))
            Some(f"$d is writable by its group or other users (mode ${mode & 0xfff}%o)")
        else None
    }

    /**
      * The real path of `root`, made when missing, once no other user can write it or a directory above it; else an
      * [[UnsafeCopyRoot]]. Each missing directory is made by itself (0700, never through an existing one unchecked). An
      * existing one is checked before anything is made in it: a link must be this uid's or root's, and its target is
      * checked as a directory. Once made, every directory from the root's real path up to `/` is checked again: each
      * above it this uid's or root's, writable by no other user unless sticky; the root this uid's and 0700.
      */
    private[gdal] def secureRoot(root: Path): Path = {
        val unsafe = (why: String) =>
            new UnsafeCopyRoot(s"geobrix: the native copy root $root is not safe to load libraries from: $why")
        val nf = LinkOption.NOFOLLOW_LINKS
        val check = (d: Path, own: Boolean) => writableByOthers(d, own).foreach(w => throw unsafe(w))
        try {
            val abs = root.toAbsolutePath.normalize()
            val names = abs.iterator().asScala.toList
            names.indices.foldLeft(abs.getRoot) { (parent, i) =>
                val d = parent.resolve(names(i).toString)
                val isRoot = i == names.size - 1
                if (!Files.exists(d, nf)) {
                    try Files.createDirectory(d, OwnerOnly)
                    catch { case _: FileAlreadyExistsException => () } // made meanwhile: checked below as an existing one
                }
                if (Files.isSymbolicLink(d)) {
                    val uid = uidOf(d)
                    if (uid != myUid && uid != 0) throw unsafe(s"$d is a link owned by uid $uid")
                }
                if (!Files.isDirectory(d)) throw unsafe(s"$d is not a directory")
                check(d.toRealPath(), isRoot)
                d
            }
            val real = abs.toRealPath()
            Iterator.iterate(real.getParent)(_.getParent).takeWhile(_ != null).foreach(check(_, false))
            check(real, true)
            val mode = modeOf(real)
            if ((mode & 0x3f) != 0) throw unsafe(f"$real is not owner-only (mode ${mode & 0xfff}%o, not 700)")
            real
        } catch {
            case e: UnsafeCopyRoot                => throw e
            case e: UnsupportedOperationException => throw unsafe(s"its owner cannot be read ($e)")
            case e: IllegalArgumentException      => throw unsafe(s"its owner cannot be read ($e)")
        }
    }

    /** A file's sha256, streamed (a payload library is tens of MB). */
    private[gdal] def sha256(p: Path): String = {
        val md = MessageDigest.getInstance("SHA-256")
        val in = Files.newInputStream(p)
        try {
            val buf = new Array[Byte](1 << 20)
            Iterator.continually(in.read(buf)).takeWhile(_ >= 0).foreach(n => md.update(buf, 0, n))
        } finally in.close()
        md.digest().map("%02x".format(_)).mkString
    }

    /** The root a JVM copies into: `value` (the conf or environment setting) when set, `off` for none, else
      * `<local disk>/geobrix/native` when the node has a writable [[DatabricksLocalDisk]], else none (load in place). */
    def rootFrom(value: Option[String], localDisk: Option[Path]): Option[Path] =
        value.map(_.trim).filter(_.nonEmpty) match {
            case Some(v) if Off.contains(v.toLowerCase) => None
            case Some(v)                                => Some(Paths.get(v).toAbsolutePath.normalize())
            case None                                   => localDisk.map(_.resolve(DefaultSubdir))
        }

    /** [[DatabricksLocalDisk]] when it is a writable directory on this node. */
    def localDisk(): Option[Path] =
        Some(Paths.get(DatabricksLocalDisk)).filter(p => Files.isDirectory(p) && Files.isWritable(p))

    /** The GeoBrix JAR's version (its `pom.properties`), `dev` in a classes directory. */
    lazy val jarVersion: String =
        Try {
            val in = getClass.getResourceAsStream("/META-INF/maven/com.databricks.labs/geobrix/pom.properties")
            try {
                val p = new Properties()
                p.load(in)
                p.getProperty("version")
            } finally in.close()
        }.toOption.flatMap(Option(_)).map(_.replaceAll("[^A-Za-z0-9._-]", "_")).getOrElse("dev")

    /** The copy's directory name, or None when `identity` is no sha256 (the JNI could not be read). */
    def keyFor(identity: String, version: String = jarVersion): Option[String] =
        Some(identity).filter(Sha256.matches).map(s => s"$version-${s.take(16)}")

    /**
      * The node-local copy of `c` (a payload: `<dir>/lib/libgdalalljni.so`, data `<dir>/../share/{proj,gdal}`) under
      * `requested` (by its real path, once [[secureRoot]] has checked it), made now unless a done copy with the JNI `identity` (its sha256) is there. `sha` gives a file's sha256.
      *
      * Left: the refusal, when the payload is gone or changes while it is copied. An IOException for any other failure
      * (the root cannot be written): the caller loads in place.
      */
    def stabilize(c: NativeChoice, requested: Path, identity: String, sha: Path => String): Either[String, NativeChoice] = {
        val key = keyFor(identity).getOrElse(throw new IOException(s"the JNI at ${c.jni} could not be hashed"))
        val src = Paths.get(c.jni).getParent.getParent
        val arch = src.getFileName.toString
        // a copied file's source: `<arch>/...` under the payload's directory, `share/{proj,gdal}/...` under its data
        val sourceOf = (rel: String) => rel.split("/", 3) match {
            case Array(`arch`, sub, rest)  => Some(src.resolve(sub).resolve(rest))
            case Array("share", "proj", r) => c.projData.map(Paths.get(_).resolve(r))
            case Array("share", "gdal", r) => c.gdalData.map(Paths.get(_).resolve(r))
            case _                         => None
        }
        jvmLock.synchronized {
            // every later path (lock, copy, temp directory, rename, marker, the JNI loaded) is under the checked real root
            val root = secureRoot(requested)
            val dest = root.resolve(key)
            val choice = copyChoice(dest, arch)
            withNodeLock(root) {
                if (isDone(dest, arch, identity, sha, sourceOf)) {
                    logInfo(s"GDAL native payload: reusing the node-local copy $dest")
                    Right(choice)
                } else {
                    removeUnfinished(root)
                    if (Files.exists(dest)) {
                        // a copy with no done marker, or with other bytes: set aside and removed, then made again
                        val aside = root.resolve(s"$TmpPrefix$key-old-${UUID.randomUUID()}")
                        Files.move(dest, aside, StandardCopyOption.ATOMIC_MOVE)
                        deleteTree(aside)
                    }
                    val tmp = root.resolve(s"$TmpPrefix$key-${UUID.randomUUID()}")
                    try {
                        Files.createDirectory(tmp, OwnerOnly)
                        copyPayload(src, c, tmp, arch, root) match {
                            case Left(refusal) => Left(refusal)
                            case Right(())     =>
                                val got = sha(tmp.resolve(s"$arch/lib/${Paths.get(c.jni).getFileName}"))
                                if (got != identity) Left(changed(src, root, s"the copied JNI's sha256 is $got, not $identity"))
                                else {
                                    val manifest = filesOf(tmp).map(r => s"${sha256(tmp.resolve(r))}  $r")
                                    Files.write(
                                      tmp.resolve(DoneMarker),
                                      (Seq(identity, src.toString) ++ manifest).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8)
                                    )
                                    Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE)
                                    logInfo(s"GDAL native payload: copied $src to the node-local $dest")
                                    Right(choice)
                                }
                        }
                    } finally if (Files.exists(tmp)) deleteTree(tmp)
                }
            }
        }
    }

    private def copyChoice(dest: Path, arch: String): NativeChoice = {
        val d = dest.resolve(arch).toString
        NativeChoice(s"$d/lib/${GDALManager.JniName}", Some(s"$d/../share/proj"), Some(s"$d/../share/gdal"))
    }

    /** The regular files under `dir`, relative and `/`-separated, the done marker left out; None when `dir` holds
      * anything else (a link, a device), which no copy holds: the copy follows the payload's links. */
    private def filesIn(dir: Path): Option[Seq[String]] = {
        val out = Seq.newBuilder[String]
        var other = false
        Files.walkFileTree(dir, new SimpleFileVisitor[Path] {
            override def visitFile(f: Path, a: BasicFileAttributes): FileVisitResult = {
                if (a.isRegularFile) out += dir.relativize(f).iterator().asScala.mkString("/") else other = true
                FileVisitResult.CONTINUE
            }
        })
        if (other) None else Some(out.result().filterNot(_ == DoneMarker).sorted)
    }

    private def filesOf(dir: Path): Seq[String] =
        filesIn(dir).getOrElse(throw new IOException(s"$dir holds a file that is not a regular file"))

    /**
      * A done copy this JVM may load: its directory is this uid's and writable by no other user; its marker names
      * `identity` and the JNI has those bytes; its files are exactly the manifest's, each with the manifest's sha256;
      * and each file's source (`sourceOf`), where it is still there, has that sha256 too.
      */
    private def isDone(dest: Path, arch: String, identity: String, sha: Path => String, sourceOf: String => Option[Path]): Boolean = {
        val marker = dest.resolve(DoneMarker)
        val jni = dest.resolve(s"$arch/lib/${GDALManager.JniName}")
        val nf = LinkOption.NOFOLLOW_LINKS
        Files.isDirectory(dest, nf) && Files.isRegularFile(marker, nf) && Files.isRegularFile(jni, nf) && {
            val t0 = System.nanoTime()
            val lines = new String(Files.readAllBytes(marker), StandardCharsets.UTF_8).linesIterator.toList
            val manifest = lines.drop(2).map(_.split("  ", 2)).collect { case Array(h, r) if Sha256.matches(h) => r -> h }.toMap
            val why: Option[String] =
                writableByOthers(dest, own = true)
                    .orElse(Some("its done marker names another JNI").filterNot(_ => lines.headOption.map(_.trim).contains(identity)))
                    .orElse(Some(s"its manifest does not list every line").filter(_ => manifest.size != lines.size - 2))
                    .orElse(Some(s"its JNI's sha256 is not $identity").filter(_ => sha(jni) != identity))
                    .orElse(filesIn(dest) match {
                        case None                                       => Some("it holds a link or other non-regular file")
                        case Some(files) if files.toSet != manifest.keySet =>
                            val extra = files.filterNot(manifest.contains) ++ manifest.keys.filterNot(files.contains)
                            Some(s"its files are not its manifest's (${extra.take(3).mkString(", ")})")
                        case Some(files)                                =>
                            files.iterator.flatMap { r =>
                                val h = manifest(r)
                                if (sha256(dest.resolve(r)) != h) Some(s"$r does not match its manifest")
                                else sourceOf(r).filter(Files.isRegularFile(_)).filter(s => sha256(s) != h)
                                    .map(s => s"$r differs from its source $s")
                            }.nextOption()
                    })
            why match {
                case Some(w) =>
                    logWarning(s"GDAL native payload: not reusing the node-local copy $dest: $w; it is made again")
                    false
                case None    =>
                    logInfo(f"GDAL native payload: verified ${manifest.size} files of $dest in ${(System.nanoTime() - t0) / 1e6}%.0f ms")
                    true
            }
        }
    }

    /** `<src>/{lib,bin,hdf5-plugins}` to `<tmp>/<arch>`, and the choice's PROJ and GDAL data to `<tmp>/share`. */
    private def copyPayload(src: Path, c: NativeChoice, tmp: Path, arch: String, root: Path): Either[String, Unit] = {
        val share = tmp.resolve("share")
        try {
            if (!Files.isDirectory(src.resolve("lib"))) Left(changed(src, root, s"${src.resolve("lib")} is gone"))
            else {
                Seq("lib", "bin", "hdf5-plugins").map(src.resolve).filter(Files.isDirectory(_))
                    .foreach(d => copyTree(d, tmp.resolve(arch).resolve(d.getFileName.toString)))
                Seq("proj" -> c.projData, "gdal" -> c.gdalData).foreach { case (name, dir) =>
                    dir.map(Paths.get(_)).filter(Files.isDirectory(_)).foreach(d => copyTree(d, share.resolve(name)))
                }
                Right(())
            }
        } catch {
            // a source file that went while it was copied: the payload is being removed or replaced
            case e: NoSuchFileException if Option(e.getFile).exists(f => !f.startsWith(tmp.toString)) =>
                Left(changed(src, root, s"${e.getFile} is gone"))
        }
    }

    private def changed(src: Path, root: Path, why: String): String =
        s"geobrix: the GDAL payload at $src changed or was removed while this node copied it to $root ($why). " +
            "Fix: run the command again; if it fails again, reinstall the env wheel on the cluster or restart the cluster."

    /** `from` into `to`, links followed (a payload may link its files), modes kept (the libraries are executable). */
    private def copyTree(from: Path, to: Path): Unit =
        Files.walkFileTree(from, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Int.MaxValue, new SimpleFileVisitor[Path] {
            override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.createDirectories(to.resolve(from.relativize(dir).toString))
                FileVisitResult.CONTINUE
            }
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.copy(file, to.resolve(from.relativize(file).toString), StandardCopyOption.COPY_ATTRIBUTES)
                FileVisitResult.CONTINUE
            }
        })

    /** Unfinished copies in `root`: a JVM that held the lock while copying is gone (the caller holds the lock). */
    private def removeUnfinished(root: Path): Unit = {
        val s = Files.list(root)
        try s.iterator().asScala.filter(_.getFileName.toString.startsWith(TmpPrefix)).toList.foreach { p =>
            logInfo(s"GDAL native payload: removing the unfinished copy $p")
            Try(deleteTree(p))
        } finally s.close()
    }

    private def deleteTree(p: Path): Unit =
        Files.walkFileTree(p, new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.delete(file)
                FileVisitResult.CONTINUE
            }
            override def postVisitDirectory(dir: Path, e: IOException): FileVisitResult = {
                Files.delete(dir)
                FileVisitResult.CONTINUE
            }
        })

    /** `body` under `root`'s lock file, held by one JVM of the node at a time. */
    private def withNodeLock[T](root: Path)(body: => T): T = {
        val ch = FileChannel.open(root.resolve(LockName), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        try {
            val lock = lockOf(ch)
            try body finally lock.release()
        } finally ch.close()
    }

    /** The channel's lock; a lock another class loader of this JVM holds is waited for (one JVM holds one lock). */
    @annotation.tailrec
    private def lockOf(ch: FileChannel, waitedMs: Long = 0L): java.nio.channels.FileLock = {
        val got = try Some(ch.lock()) catch { case _: OverlappingFileLockException if waitedMs < 600000L => None }
        got match {
            case Some(l) => l
            case None    =>
                Thread.sleep(50)
                lockOf(ch, waitedMs + 50)
        }
    }

}
