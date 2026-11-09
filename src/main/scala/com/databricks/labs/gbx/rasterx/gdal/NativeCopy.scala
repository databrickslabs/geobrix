package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.rasterx.gdal.GDALManager.NativeChoice
import org.apache.spark.internal.Logging

import java.io.{IOException, UncheckedIOException}
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.{AclEntry, AclEntryPermission, AclEntryType, AclFileAttributeView, BasicFileAttributes, PosixFilePermissions}
import java.security.MessageDigest
import java.util.{EnumSet, Locale, Properties, UUID}
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
  * The key is `<GeoBrix version>-<first 16 hex of the JNI's sha256>-<first 16 hex of the manifest's sha256>`, the manifest
  * being the sha256 and copy-relative path of every file the payload gives (libraries, `bin`, `hdf5-plugins`, PROJ and GDAL
  * data). Two payloads with one JNI and other data are two copies. A copy is made in a temporary directory in the root,
  * marked done, then renamed into place, all under a lock file, so the JVMs of one node make one copy. A JVM that finds a
  * done copy with the same JNI and the same manifest, whose files each have the manifest's sha256, reuses it.
  *
  * No copy is pruned or removed: a node holds one copy per distinct payload its cluster ran (about 130 MB each), its
  * clusters restart onto fresh local disks, and a JVM's mapped libraries and data (and the PROJ database it opens by path
  * at any time) cannot be checked safely from another process. A done copy that fails the check (a file altered, planted
  * or gone) is left where it is, and a JVM makes a sibling `<key>-1`, `<key>-2`, ... (at most 8 copies of one key) or reuses
  * the first sibling that passes. Only an unfinished copy (a temporary directory: a JVM killed while copying) is removed,
  * by the next JVM that takes the lock.
  *
  * The JVM loads native code from the copy, so no other user may be able to write it. The root and each copy are made
  * owner-only (0700). A root that another user owns or can write, or that sits under a directory another user can
  * write (one that is not sticky, as `/tmp` is), is refused ([[UnsafeCopyRoot]]). With no root set, the JVM tries
  * `/local_disk0/geobrix/native`, then `<java.io.tmpdir>/geobrix/native`, then `/tmp/geobrix/native`, and uses the first
  * that passes (on Databricks Runtime 19, `/local_disk0` and so `java.io.tmpdir` belong to another uid, so the copy
  * lands in `/tmp`); a root that is set is the only candidate. A candidate whose filesystem has less than 3 times the
  * payload's size free is skipped too. When none passes, the JVM loads in place. Any failure of the copy that is not the
  * payload changing (a lock another class loader holds past its wait, a file that cannot be read or listed) also loads
  * in place, with a warning ([[asIO]]).
  *
  * Limit, NOT FIXABLE in Java: the checks read the mode bits, and an ACL where the filesystem gives one to Java
  * (`AclFileAttributeView`: NFSv4, Windows, Solaris), which must grant write to no principal but the owner and root. A
  * POSIX ACL on Linux (`setfacl` on ext4, xfs or overlay) is the `system.posix_acl_access` extended attribute: the JDK
  * has no ACL view on Linux and its user-attribute view reads only the `user.` namespace, so a POSIX ACL that grants
  * another user write is not seen. Reading it needs `getfacl` or a native call, and this code starts no process. On
  * Databricks nodes the roots are created here with mode 0700 and no ACL; a root an admin gave a POSIX ACL is outside
  * what the JVM can verify.
  */
private[gdal] object NativeCopy extends Logging {

    /** In a copy: present once every file is in place. Its lines: the JNI's sha256, the manifest's sha256, the source,
      * then the manifest (`<sha256>  <relative path>` per file). */
    val DoneMarker = ".gbx-copy-done"
    private val LockName = ".gbx-copy.lock"
    private val TmpPrefix = ".tmp-"
    private val Sha256 = "[0-9a-f]{64}".r
    /** The first candidate root when none is set: Databricks' local disk, when the node has one. */
    val DatabricksLocalDisk = "/local_disk0"
    /** A candidate needs this many times the payload's size free on its filesystem. */
    val FreeSpaceFactor = 3L
    val DefaultSubdir = "geobrix/native"
    /** The last candidate's base: root-owned and sticky on Linux, whatever `java.io.tmpdir` is. */
    val SystemTmp = "/tmp"
    private val Off = Set("off", "none", "false")
    /** A key's copies: `<key>`, then `<key>-1` to `<key>-7`, a sibling made when a done copy fails its check. */
    val MaxCopiesPerKey = 8
    /** How long a JVM waits for a lock another class loader of it holds, in ms (a test hook). */
    @volatile private[gdal] var lockWaitMs = 600000L

    // the JVM's own guard: a file lock is held per JVM, so two threads of one JVM must not both take it
    private val jvmLock = new Object

    /** A copy root another user could write: the JVM loads the payload in place instead (the caller's IOException). */
    final class UnsafeCopyRoot(msg: String) extends IOException(msg)

    /** A copy root whose filesystem has too little free space for the payload: the next candidate is tried. */
    final class InsufficientSpace(msg: String) extends IOException(msg)

    private val OwnerOnly = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    private val GroupOrOtherWrite = 0x12 // 0o022
    private val Sticky = 0x200 // 0o1000

    /** This JVM's uid: the owner of a file it creates. */
    private lazy val myUid: Int = uidOfNewFileIn(Seq(tmpDir(), Paths.get(SystemTmp)))

    /** The owner of a file this JVM makes in the first of `dirs` it can write: `java.io.tmpdir` may belong to another
      * user (`/local_disk0/tmp` on Databricks Runtime 19), and `/tmp` is the one directory any user can write. */
    private[gdal] def uidOfNewFileIn(dirs: Seq[Path]): Int = {
        val made = dirs.iterator.flatMap(d => Try(Files.createTempFile(d, "gbx_uid", "")).toOption).nextOption()
        val f = made.getOrElse(throw new IOException(s"no directory in ${dirs.mkString(", ")} can be written to find this JVM's uid"))
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
        else if ((mode & GroupOrOtherWrite) != 0 && (own || (mode & Sticky) == 0)) {
            Some(f"$d is writable by its group or other users (mode ${mode & 0xfff}%o)")
        } else if (own || (mode & Sticky) == 0) aclWritable(d)
        else None
    }

    /** What an ACL grants write to: these, to a principal that is neither the owner nor root. */
    private val WriteGrants = Set(
      AclEntryPermission.WRITE_DATA, AclEntryPermission.APPEND_DATA, AclEntryPermission.DELETE_CHILD,
      AclEntryPermission.DELETE, AclEntryPermission.WRITE_ACL, AclEntryPermission.WRITE_OWNER
    )
    private val SelfPrincipals = Set("root", "OWNER@")

    /** The first ALLOW entry of `acl` that grants write ([[WriteGrants]]) to a principal other than `owner`, root and
      * `OWNER@`, as the reason; None when there is none. A DENY entry is not weighed: an ACL with a write grant to another
      * principal is unsafe whatever else it holds. */
    private[gdal] def aclWriters(acl: java.util.List[AclEntry], owner: String): Option[String] =
        acl.asScala.find { e =>
            e.`type`() == AclEntryType.ALLOW && !SelfPrincipals.contains(e.principal().getName) &&
                e.principal().getName != owner && e.permissions().asScala.exists(WriteGrants.contains)
        }.map(e => s"its ACL grants write to ${e.principal().getName}")

    /** Why `d`'s ACL lets another principal write it, when the filesystem gives Java an ACL (see the class doc for the
      * Linux POSIX ACL it cannot read); an ACL that cannot be read is no proof of safety. */
    private def aclWritable(d: Path): Option[String] =
        Option(Files.getFileAttributeView(d, classOf[AclFileAttributeView], LinkOption.NOFOLLOW_LINKS)).flatMap { view =>
            try aclWriters(view.getAcl, view.getOwner.getName).map(w => s"$d: $w")
            catch { case e: IOException => Some(s"$d has an ACL that cannot be read ($e)") }
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

    private def sha256(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

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

    /** The roots a JVM tries to copy into, in order: `value` (the conf or environment setting) when set, alone, so a root
      * that is set and refused is not replaced by another; `off` for none; else `<local disk>/geobrix/native` when the
      * node has a writable [[DatabricksLocalDisk]], then `<tmpDir>/geobrix/native`, then `<systemTmp>/geobrix/native`.
      * On Databricks `java.io.tmpdir` is under `/local_disk0`, so `/tmp` (root-owned and sticky) is its own candidate. A
      * base directory with the same real path as an earlier one is left out. */
    def rootsFrom(value: Option[String], localDisk: Option[Path], tmpDir: Path, systemTmp: Path = Paths.get(SystemTmp)): Seq[Path] =
        value.map(_.trim).filter(_.nonEmpty) match {
            case Some(v) if Off.contains(v.toLowerCase(Locale.ROOT)) => Nil
            case Some(v)                                => Seq(Paths.get(v).toAbsolutePath.normalize())
            case None                                   =>
                val bases = (localDisk.toSeq ++ Seq(tmpDir, systemTmp)).map(_.toAbsolutePath.normalize())
                val seen = scala.collection.mutable.LinkedHashMap.empty[Path, Path]
                bases.foreach(b => seen.getOrElseUpdate(Try(b.toRealPath()).getOrElse(b), b))
                seen.values.toSeq.map(_.resolve(DefaultSubdir))
        }

    /** The JVM's temporary directory (`java.io.tmpdir`, usually `/tmp`). */
    def tmpDir(): Path = Paths.get(System.getProperty("java.io.tmpdir", "/tmp"))

    /** The free bytes on the filesystem that holds `dir`. */
    private def usableSpace(dir: Path): Long = Files.getFileStore(dir).getUsableSpace

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
    def keyFor(identity: String, manifestHash: String, version: String = jarVersion): Option[String] =
        Some(identity).filter(Sha256.matches).filter(_ => Sha256.matches(manifestHash))
            .map(s => s"$version-${s.take(16)}-${manifestHash.take(16)}")

    /** The files a payload gives, `<copy-relative path> -> sha256`, sorted, and the sha256 of that list. */
    final case class Manifest(files: Seq[(String, String)]) {
        lazy val lines: Seq[String] = files.map { case (r, h) => s"$h  $r" }
        lazy val hash: String = sha256(lines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
        def toMap: Map[String, String] = files.toMap
    }

    /** `sha(p)`, which must be a sha256: a file that cannot be read is an IOException with its own message (not a hash
      * to compare with, which would read as "the payload changed"); a file that is gone stays a NoSuchFileException. */
    private def hashed(sha: Path => String, p: Path): String = {
        val h = try sha(p) catch {
            case e: NoSuchFileException => throw e
            case e: IOException         => throw new IOException(s"geobrix: could not read $p to hash it ($e)", e)
        }
        if (Sha256.matches(h)) h else throw new IOException(s"geobrix: could not read $p to hash it (no sha256: $h)")
    }

    /** The payload's files as a copy holds them, `<copy-relative path> -> source file`: `<arch>/{lib,bin,hdf5-plugins}/...`
      * and `share/{proj,gdal}/...`, links followed. */
    private def sourceFiles(src: Path, c: NativeChoice, arch: String): Seq[(String, Path)] = {
        val trees = Seq("lib", "bin", "hdf5-plugins").map(n => s"$arch/$n" -> src.resolve(n)) ++
            Seq("proj" -> c.projData, "gdal" -> c.gdalData).flatMap { case (n, d) => d.map(x => s"share/$n" -> Paths.get(x)) }
        trees.filter(t => Files.isDirectory(t._2)).flatMap { case (prefix, dir) =>
            val out = Seq.newBuilder[(String, Path)]
            Files.walkFileTree(dir, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Int.MaxValue, new SimpleFileVisitor[Path] {
                override def visitFile(f: Path, a: BasicFileAttributes): FileVisitResult = {
                    if (a.isRegularFile) out += s"$prefix/${dir.relativize(f).iterator().asScala.mkString("/")}" -> f
                    FileVisitResult.CONTINUE
                }
            })
            out.result()
        }.sortBy(_._1)
    }

    /**
      * The manifest of the payload `c` (a payload: `<dir>/lib/libgdalalljni.so`, data `<dir>/../share/{proj,gdal}`): the
      * sha256 of every file a copy would hold. Left: the refusal, when the payload is gone, a file goes while it is read, or
      * its JNI is not the one with sha256 `identity` (it changed since the JVM hashed it). An IOException when a file
      * cannot be read.
      */
    def sourceManifest(c: NativeChoice, identity: String, sha: Path => String): Either[String, Manifest] = asIO {
        if (!Sha256.matches(identity)) throw new IOException(s"geobrix: the JNI at ${c.jni} could not be read to hash it")
        val src = Paths.get(c.jni).getParent.getParent
        val arch = src.getFileName.toString
        val jniRel = s"$arch/lib/${Paths.get(c.jni).getFileName}"
        try {
            if (!Files.isDirectory(src.resolve("lib"))) Left(changed(src, s"${src.resolve("lib")} is gone"))
            else {
                val m = Manifest(sourceFiles(src, c, arch).map { case (rel, f) => rel -> hashed(sha, f) })
                m.toMap.get(jniRel) match {
                    case Some(h) if h == identity => Right(m)
                    case other                    => Left(changed(src, s"the JNI's sha256 is ${other.getOrElse("absent")}, not $identity"))
                }
            }
        } catch {
            case e: NoSuchFileException => Left(changed(src, s"${e.getFile} is gone"))
        }
    }

    /**
      * The node-local copy of `c` (a payload: `<dir>/lib/libgdalalljni.so`, data `<dir>/../share/{proj,gdal}`) under
      * `requested` (by its real path, once [[secureRoot]] has checked it): the first done copy of its key that passes the
      * check ([[reusable]]), else a new one, made now. `identity` is the JNI's sha256 and `sha` gives a file's sha256 (a
      * read that fails throws an IOException). `manifest`: the payload's, when the caller has it ([[sourceManifest]]).
      *
      * Left: the refusal, when the payload is gone or changes while it is copied. An IOException for any other failure
      * (the root cannot be written, a lock is not given, a file cannot be read): the caller loads in place.
      */
    def stabilize(
        c: NativeChoice,
        requested: Path,
        identity: String,
        sha: Path => String,
        freeSpace: Path => Long = usableSpace,
        manifest: Option[Manifest] = None
    ): Either[String, NativeChoice] = asIO {
        val src = Paths.get(c.jni).getParent.getParent
        val arch = src.getFileName.toString
        manifest.map(Right(_)).getOrElse(sourceManifest(c, identity, sha)).flatMap { m =>
            val key = keyFor(identity, m.hash).getOrElse(throw new IOException(s"the JNI at ${c.jni} could not be hashed"))
            jvmLock.synchronized {
                // every later path (lock, copy, temp directory, rename, marker, the JNI loaded) is under the checked real root
                val root = secureRoot(requested)
                withNodeLock(root) {
                    val names = (key +: (1 until MaxCopiesPerKey).map(i => s"$key-$i")).toList
                    // the first done copy that passes; else the first name that is free
                    val firstFree = names.find(n => !Files.exists(root.resolve(n), LinkOption.NOFOLLOW_LINKS))
                    val existing = names.takeWhile(n => Files.exists(root.resolve(n), LinkOption.NOFOLLOW_LINKS))
                    existing.find(n => reusable(root.resolve(n), arch, identity, m, sha)) match {
                        case Some(n) =>
                            logInfo(s"GDAL native payload: reusing the node-local copy ${root.resolve(n)}")
                            Right(copyChoice(root.resolve(n), arch))
                        case None    =>
                            val name = firstFree.getOrElse(throw new IOException(
                              s"geobrix: the native copy root $root holds $MaxCopiesPerKey copies of $key, none of them intact"))
                            makeCopy(c, src, arch, root, key, root.resolve(name), identity, m, sha, freeSpace)
                    }
                }
            }
        }
    }

    /** A copy of the payload at `dest` (not yet there): made in a temporary directory, checked against `m`, marked done,
      * then renamed. Left when a file differs from the manifest or is gone (the payload changed meanwhile). */
    private def makeCopy(
        c: NativeChoice, src: Path, arch: String, root: Path, key: String, dest: Path, identity: String, m: Manifest,
        sha: Path => String, freeSpace: Path => Long
    ): Either[String, NativeChoice] = {
        removeUnfinished(root)
        val need = FreeSpaceFactor * payloadBytes(src, c)
        val free = freeSpace(root)
        if (free < need) {
            throw new InsufficientSpace(
              s"geobrix: the native copy root $root has $free bytes free, less than the $need (${FreeSpaceFactor}x the payload) it needs"
            )
        }
        val tmp = root.resolve(s"$TmpPrefix$key-${UUID.randomUUID()}")
        try {
            Files.createDirectory(tmp, OwnerOnly)
            copyPayload(src, c, tmp, arch, root) match {
                case Left(refusal) => Left(refusal)
                case Right(())     =>
                    // the copy is hashed once: it must be the manifest's files with the manifest's bytes
                    val got = filesOf(tmp).map(r => r -> hashed(sha, tmp.resolve(r)))
                    val expected = m.toMap
                    val differs = got.filter { case (r, h) => !expected.get(r).contains(h) }.map(_._1) ++
                        expected.keys.filterNot(r => got.exists(_._1 == r))
                    if (differs.nonEmpty) Left(changed(src, s"the copied ${differs.take(3).mkString(", ")} differ from the manifest"))
                    else {
                        Files.write(
                          tmp.resolve(DoneMarker),
                          (Seq(identity, m.hash, src.toString) ++ m.lines).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8)
                        )
                        Files.move(tmp, dest, StandardCopyOption.ATOMIC_MOVE)
                        logInfo(s"GDAL native payload: copied $src to the node-local $dest")
                        Right(copyChoice(dest, arch))
                    }
            }
        } finally if (Files.exists(tmp)) deleteTree(tmp)
    }

    /**
      * [[stabilize]] under the first of `roots` that passes its checks and has the space. A root that fails is logged at
      * INFO and the next is tried. Left: the refusal of [[stabilize]], as there. An IOException naming every root and why
      * it was refused when none passes: the caller loads in place. The payload is hashed once for all the roots.
      */
    def stabilizeFirst(
        c: NativeChoice,
        roots: Seq[Path],
        identity: String,
        sha: Path => String,
        freeSpace: Path => Long = usableSpace
    ): Either[String, NativeChoice] = asIO {
        sourceManifest(c, identity, sha).flatMap { m =>
            @annotation.tailrec
            def go(rest: List[Path], refused: Vector[String]): Either[String, NativeChoice] = rest match {
                case Nil          =>
                    throw new IOException(
                      if (refused.isEmpty) "no native copy root" else s"no native copy root passed: ${refused.mkString("; ")}"
                    )
                case root :: more =>
                    val tried: Either[IOException, Either[String, NativeChoice]] =
                        try Right(stabilize(c, root, identity, sha, freeSpace, Some(m)))
                        catch { case e: IOException => Left(e) }
                    tried match {
                        case Right(r) =>
                            logInfo(s"GDAL native payload: node-local copy root $root chosen")
                            r
                        case Left(e)  =>
                            logInfo(s"GDAL native payload: node-local copy root $root refused: ${e.getMessage}" +
                                (if (more.nonEmpty) s"; trying ${more.head}" else ""))
                            go(more, refused :+ s"$root (${e.getMessage})")
                    }
            }
            go(roots.toList, Vector.empty)
        }
    }

    /** The bytes `copyPayload` copies: the payload's `lib`, `bin`, `hdf5-plugins` and the PROJ and GDAL data. */
    private def payloadBytes(src: Path, c: NativeChoice): Long = {
        val dirs = Seq("lib", "bin", "hdf5-plugins").map(src.resolve) ++ (c.projData.toSeq ++ c.gdalData.toSeq).map(Paths.get(_))
        dirs.filter(Files.isDirectory(_)).map { d =>
            var total = 0L
            Try(Files.walkFileTree(d, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Int.MaxValue, new SimpleFileVisitor[Path] {
                override def visitFile(f: Path, a: BasicFileAttributes): FileVisitResult = {
                    total += a.size
                    FileVisitResult.CONTINUE
                }
                override def visitFileFailed(f: Path, e: IOException): FileVisitResult = FileVisitResult.CONTINUE
            }))
            total
        }.sum
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
      * `identity` and the manifest `m` (its hash, and every listed file the same); and its files are exactly the manifest's,
      * each with the manifest's sha256. A copy that fails is left as it is ([[stabilize]] makes a sibling).
      */
    private def reusable(dest: Path, arch: String, identity: String, m: Manifest, sha: Path => String): Boolean = {
        val marker = dest.resolve(DoneMarker)
        val jni = dest.resolve(s"$arch/lib/${GDALManager.JniName}")
        val nf = LinkOption.NOFOLLOW_LINKS
        Files.isDirectory(dest, nf) && Files.isRegularFile(marker, nf) && Files.isRegularFile(jni, nf) && {
            val t0 = System.nanoTime()
            val lines = new String(Files.readAllBytes(marker), StandardCharsets.UTF_8).linesIterator.toList
            val listed = lines.drop(3).map(_.split("  ", 2)).collect { case Array(h, r) if Sha256.matches(h) => r -> h }.toMap
            val why: Option[String] =
                writableByOthers(dest, own = true)
                    .orElse(Some("its done marker names another JNI").filterNot(_ => lines.headOption.map(_.trim).contains(identity)))
                    .orElse(Some("its done marker names another manifest").filterNot(_ => lines.lift(1).map(_.trim).contains(m.hash)))
                    .orElse(Some("its manifest does not list every line").filter(_ => listed.size != lines.size - 3))
                    .orElse(Some("its manifest is not the payload's").filter(_ => listed != m.toMap))
                    .orElse(filesIn(dest) match {
                        case None                                         => Some("it holds a link or other non-regular file")
                        case Some(files) if files.toSet != listed.keySet =>
                            val extra = files.filterNot(listed.contains) ++ listed.keys.filterNot(files.contains)
                            Some(s"its files are not its manifest's (${extra.take(3).mkString(", ")})")
                        case Some(files)                                  =>
                            files.iterator.filter(r => hashed(sha, dest.resolve(r)) != listed(r)).map(r => s"$r does not match its manifest").nextOption()
                    })
            why match {
                case Some(w) =>
                    logWarning(s"GDAL native payload: not reusing the node-local copy $dest: $w; it is left as it is")
                    false
                case None    =>
                    logInfo(f"GDAL native payload: verified ${listed.size} files of $dest in ${(System.nanoTime() - t0) / 1e6}%.0f ms")
                    true
            }
        }
    }

    /** `<src>/{lib,bin,hdf5-plugins}` to `<tmp>/<arch>`, and the choice's PROJ and GDAL data to `<tmp>/share`. */
    private def copyPayload(src: Path, c: NativeChoice, tmp: Path, arch: String, root: Path): Either[String, Unit] = {
        val share = tmp.resolve("share")
        try {
            if (!Files.isDirectory(src.resolve("lib"))) Left(changed(src, s"${src.resolve("lib")} is gone"))
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
                Left(changed(src, s"${e.getFile} is gone"))
        }
    }

    private def changed(src: Path, why: String): String =
        s"geobrix: the GDAL payload at $src changed or was removed while this node copied it ($why). " +
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

    /** The channel's lock; a lock another class loader of this JVM holds is waited for, `maxWaitMs` at most (one JVM
      * holds one lock): then the [[OverlappingFileLockException]] stands, which [[asIO]] turns into an IOException. */
    @annotation.tailrec
    private[gdal] def lockOf(ch: FileChannel, maxWaitMs: Long = lockWaitMs, pollMs: Long = 50L, waitedMs: Long = 0L): java.nio.channels.FileLock = {
        val got = try Some(ch.lock()) catch { case _: OverlappingFileLockException if waitedMs < maxWaitMs => None }
        got match {
            case Some(l) => l
            case None    =>
                Thread.sleep(pollMs)
                lockOf(ch, maxWaitMs, pollMs, waitedMs + pollMs)
        }
    }

    /**
      * `body`, with the two unchecked exceptions a copy can raise as the IOException the caller loads in place on: the
      * [[OverlappingFileLockException]] of a lock this JVM holds past [[lockWaitMs]], and the [[UncheckedIOException]] of
      * a directory listing (`Files.list`) that fails. No other exception is caught.
      */
    private[gdal] def asIO[T](body: => T): T =
        try body
        catch {
            case e: OverlappingFileLockException =>
                throw new IOException(s"geobrix: the native copy lock is held by this JVM and was not released within ${lockWaitMs / 1000} s", e)
            case e: UncheckedIOException         => throw e.getCause
        }

}
