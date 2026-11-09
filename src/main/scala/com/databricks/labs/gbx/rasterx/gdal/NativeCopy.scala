package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.apache.spark.internal.Logging

import java.io.IOException
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
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
      * `root`, made now unless a done copy with the JNI `identity` (its sha256) is there. `sha` gives a file's sha256.
      *
      * Left: the refusal, when the payload is gone or changes while it is copied. An IOException for any other failure
      * (the root cannot be written): the caller loads in place.
      */
    def stabilize(c: NativeChoice, root: Path, identity: String, sha: Path => String): Either[String, NativeChoice] = {
        val key = keyFor(identity).getOrElse(throw new IOException(s"the JNI at ${c.jni} could not be hashed"))
        val src = Paths.get(c.jni).getParent.getParent
        val arch = src.getFileName.toString
        val dest = root.resolve(key)
        val choice = copyChoice(dest, arch)
        jvmLock.synchronized {
            withNodeLock(root) {
                if (isDone(dest, arch, identity, sha)) {
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
                        copyPayload(src, c, tmp, arch, root) match {
                            case Left(refusal) => Left(refusal)
                            case Right(())     =>
                                val got = sha(tmp.resolve(s"$arch/lib/${Paths.get(c.jni).getFileName}"))
                                if (got != identity) Left(changed(src, root, s"the copied JNI's sha256 is $got, not $identity"))
                                else {
                                    Files.write(tmp.resolve(DoneMarker), s"$identity\n$src\n".getBytes(StandardCharsets.UTF_8))
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

    /** A done copy: its marker names `identity`, and its JNI has those bytes. */
    private def isDone(dest: Path, arch: String, identity: String, sha: Path => String): Boolean = {
        val marker = dest.resolve(DoneMarker)
        val jni = dest.resolve(s"$arch/lib/${GDALManager.JniName}")
        Files.isRegularFile(marker) && Files.isRegularFile(jni) &&
        Try(new String(Files.readAllBytes(marker), StandardCharsets.UTF_8).linesIterator.next().trim).toOption.contains(identity) &&
        sha(jni) == identity
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
        Files.createDirectories(root)
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
